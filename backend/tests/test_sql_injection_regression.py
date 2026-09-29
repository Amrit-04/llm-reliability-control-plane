"""
Tests for SQL Parameter Binding and Injection Prevention (P0 #6)
"""
from pathlib import Path
import struct
import binascii
import pytest
from fastapi.testclient import TestClient
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

from app.control import initialize_control_store
from app.main import create_app
from app.materializer import materialize
from app.settings import Settings


def _wal_record(payload: bytes) -> bytes:
    return struct.pack("<II", len(payload), binascii.crc32(payload) & 0xFFFFFFFF) + payload


def _make_span(project_id: str, service_name: str, trace_hex: str) -> ExportTraceServiceRequest:
    request = ExportTraceServiceRequest()
    rs = request.resource_spans.add()
    rs.resource.attributes.add(key="service.name").value.string_value = service_name
    rs.resource.attributes.add(key="project.id").value.string_value = project_id
    s = rs.scope_spans.add().spans.add()
    s.trace_id = bytes.fromhex(trace_hex)
    s.span_id = bytes.fromhex("01" * 8)
    s.name = "llm.query"
    s.start_time_unix_nano = 1000
    s.end_time_unix_nano = 2000
    s.attributes.add(key="gen_ai.request.model").value.string_value = "gpt-4"
    return request


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        tmp_path,
        tmp_path / "wal",
        tmp_path / "parquet",
        tmp_path / "control.db",
    )


def test_sql_injection_payloads_in_project_and_service_params(tmp_path: Path):
    """
    Test malicious SQL injection payloads in project_id, service_name, and trace_id.
    Ensure they are safely escaped via parameter binding (?) and do not leak records or corrupt queries.
    """
    settings = _settings(tmp_path)
    settings.wal_dir.mkdir(parents=True, exist_ok=True)
    initialize_control_store(settings)

    # Ingest legitimate trace
    p = _make_span("valid-proj", "valid-svc", "11" * 16).SerializeToString()
    (settings.wal_dir / "wal-0.wal").write_bytes(_wal_record(p))
    materialize(settings)

    with TestClient(create_app(settings)) as client:
        malicious_payloads = [
            "' OR '1'='1",
            "'; DROP TABLE projects; --",
            "valid-proj' UNION SELECT * FROM read_parquet(?) --",
            "1' OR 1=1 --",
            "admin' --",
            "' OR 'x'='x",
        ]

        for payload in malicious_payloads:
            # 1. Traces query with injection in project_id
            resp = client.get("/api/v1/traces", params={"project_id": payload})
            assert resp.status_code == 200
            assert resp.json() == []  # No records matched, injection failed to bypass filter

            # 2. Traces query with injection in service
            resp_svc = client.get("/api/v1/traces", params={"service": payload})
            assert resp_svc.status_code == 200
            assert resp_svc.json() == []

            # 3. Analytics overview with injection in project_id
            resp_ov = client.get("/api/v1/analytics/overview", params={"project_id": payload})
            assert resp_ov.status_code == 200
            ov = resp_ov.json()
            assert ov["total_traces"] == 0
            assert ov["total_spans"] == 0

            # 4. Model analytics with injection in project_id
            resp_models = client.get("/api/v1/analytics/models", params={"project_id": payload})
            assert resp_models.status_code == 200
            assert resp_models.json() == []

            # 5. Trace detail with injection in trace_id
            resp_detail = client.get(f"/api/v1/traces/{payload}")
            assert resp_detail.status_code == 404
