from pathlib import Path
import binascii
import struct

import pytest
from fastapi.testclient import TestClient
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

from app.control import initialize_control_store
from app.main import create_app
from app.materializer import materialize
from app.settings import Settings


def _wal_record(payload: bytes) -> bytes:
    return struct.pack("<II", len(payload), binascii.crc32(payload) & 0xFFFFFFFF) + payload


def _sample_request() -> ExportTraceServiceRequest:
    request = ExportTraceServiceRequest()
    resource_spans = request.resource_spans.add()
    resource_spans.resource.attributes.add(key="service.name").value.string_value = "basic-rag-example"
    span = resource_spans.scope_spans.add().spans.add()
    span.trace_id = bytes.fromhex("ab" * 16)
    span.span_id = bytes.fromhex("cd" * 8)
    span.parent_span_id = bytes.fromhex("ef" * 8)
    span.name = "llm.generate"
    span.start_time_unix_nano = 10
    span.end_time_unix_nano = 20
    span.status.message = "ok"
    span.attributes.add(key="gen_ai.request.model").value.string_value = "example-model"
    span.attributes.add(key="gen_ai.usage.input_tokens").value.int_value = 100
    span.attributes.add(key="gen_ai.usage.output_tokens").value.int_value = 50
    return request


def _settings(tmp_path: Path) -> Settings:
    return Settings(tmp_path, tmp_path / "wal", tmp_path / "parquet", tmp_path / "control.db")


def test_health(tmp_path: Path):
    with TestClient(create_app(_settings(tmp_path))) as client:
        assert client.get("/healthz").json() == {"status": "ok"}


def test_crc32_matches_gateway_vector():
    # Must stay aligned with cpp/gateway WAL CRC-32 (IEEE / binascii).
    assert binascii.crc32(b"123456789") & 0xFFFFFFFF == 0xCBF43926


def test_create_project_rejects_duplicates(tmp_path: Path):
    with TestClient(create_app(_settings(tmp_path))) as client:
        created = client.post("/api/v1/projects", json={"id": "demo", "name": "Demo"})
        assert created.status_code == 201
        assert created.json() == {"id": "demo", "name": "Demo"}
        conflict = client.post("/api/v1/projects", json={"id": "demo", "name": "Other"})
        assert conflict.status_code == 409


def test_materialize_wal_record_once(tmp_path: Path):
    settings = _settings(tmp_path)
    settings.wal_dir.mkdir()
    initialize_control_store(settings)
    payload = _sample_request().SerializeToString()
    (settings.wal_dir / "active.wal").write_bytes(_wal_record(payload))
    assert materialize(settings) == 1
    assert materialize(settings) == 0
    assert len(list(settings.parquet_dir.glob("*.parquet"))) == 1


def test_materialize_rejects_checksum_mismatch(tmp_path: Path):
    settings = _settings(tmp_path)
    settings.wal_dir.mkdir()
    initialize_control_store(settings)
    payload = _sample_request().SerializeToString()
    (settings.wal_dir / "active.wal").write_bytes(
        struct.pack("<II", len(payload), 0xDEADBEEF) + payload
    )
    # The materializer now catches ValueError, logs it, and skips the corrupted file
    assert materialize(settings) == 0


def test_query_api_lists_and_fetches_materialized_trace(tmp_path: Path):
    settings = _settings(tmp_path)
    with TestClient(create_app(settings)) as client:
        assert client.get("/api/v1/traces").json() == []
        payload = _sample_request().SerializeToString()
        (settings.wal_dir / "active.wal").write_bytes(_wal_record(payload))
        assert client.post("/api/v1/materialize").json() == {"materialized_spans": 1}
        traces = client.get("/api/v1/traces").json()
        assert len(traces) == 1
        assert traces[0]["trace_id"] == "ab" * 16
        assert traces[0]["span_count"] == 1
        assert traces[0]["service_name"] == "basic-rag-example"
        assert traces[0]["aggregate_span_duration_ns"] == 10
        spans = client.get(f"/api/v1/traces/{traces[0]['trace_id']}").json()
        assert spans[0]["name"] == "llm.generate"
        assert spans[0]["parent_span_id"] == "ef" * 8
        assert client.get("/api/v1/traces/" + "00" * 16).status_code == 404


def test_analytics_endpoints_with_empty_and_populated_data(tmp_path: Path):
    settings = _settings(tmp_path)
    with TestClient(create_app(settings)) as client:
        # Empty data check
        overview = client.get("/api/v1/analytics/overview").json()
        assert overview["total_traces"] == 0
        assert overview["total_spans"] == 0
        assert overview["total_input_tokens"] == 0
        assert overview["unique_services"] == []
        assert client.get("/api/v1/analytics/models").json() == []

        # Populate with sample data
        payload = _sample_request().SerializeToString()
        (settings.wal_dir / "active.wal").write_bytes(_wal_record(payload))
        assert client.post("/api/v1/materialize").json() == {"materialized_spans": 1}

        # Query analytics overview
        overview_after = client.get("/api/v1/analytics/overview").json()
        assert overview_after["total_traces"] == 1
        assert overview_after["total_spans"] == 1
        assert overview_after["total_input_tokens"] == 100
        assert overview_after["total_output_tokens"] == 50
        assert "basic-rag-example" in overview_after["unique_services"]
        assert overview_after["error_rate"] == 0.0

        # Query model analytics
        models = client.get("/api/v1/analytics/models").json()
        assert len(models) == 1
        assert models[0]["model"] == "example-model"
        assert models[0]["request_count"] == 1
        assert models[0]["total_input_tokens"] == 100
        assert models[0]["total_output_tokens"] == 50
        assert models[0]["error_count"] == 0


def test_auto_materialize_in_background(tmp_path: Path):
    import time
    settings = Settings(
        tmp_path,
        tmp_path / "wal",
        tmp_path / "parquet",
        tmp_path / "control.db",
        materialize_interval_seconds=0.1,
    )
    with TestClient(create_app(settings)) as client:
        payload = _sample_request().SerializeToString()
        (settings.wal_dir / "active.wal").write_bytes(_wal_record(payload))
        # Wait up to 2 seconds for background worker to materialize
        deadline = time.time() + 2.0
        traces = []
        while time.time() < deadline:
            traces = client.get("/api/v1/traces").json()
            if len(traces) == 1:
                break
            time.sleep(0.05)
        assert len(traces) == 1
        assert traces[0]["trace_id"] == "ab" * 16
