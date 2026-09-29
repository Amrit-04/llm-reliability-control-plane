"""
Tests for OpenTelemetry Span Status Semantics (P0 #3)

Verifies:
  - STATUS_CODE_UNSET = 0
  - STATUS_CODE_OK = 1
  - STATUS_CODE_ERROR = 2
Only status_code = 2 is counted as an error across trace lists, overview analytics, and model analytics.
"""
from pathlib import Path
import struct
import binascii
import pytest
from fastapi.testclient import TestClient
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.trace.v1.trace_pb2 import Status

from app.control import initialize_control_store
from app.main import create_app
from app.materializer import materialize
from app.settings import Settings


def _wal_record(payload: bytes) -> bytes:
    return struct.pack("<II", len(payload), binascii.crc32(payload) & 0xFFFFFFFF) + payload


def _settings(tmp_path: Path) -> Settings:
    return Settings(tmp_path, tmp_path / "wal", tmp_path / "parquet", tmp_path / "control.db")


def test_otel_status_codes_error_counting(tmp_path: Path):
    """
    Test 3 spans in the same trace:
      Span 0: Status UNSET (code 0)
      Span 1: Status OK (code 1)
      Span 2: Status ERROR (code 2)

    Assert:
      trace error_count == 1 (not 2 or 3)
      analytics overview error_spans == 1, error_rate == 1/3 (0.3333)
      model analytics error_count == 1
    """
    settings = _settings(tmp_path)
    settings.wal_dir.mkdir(parents=True, exist_ok=True)
    initialize_control_store(settings)

    request = ExportTraceServiceRequest()
    rs = request.resource_spans.add()
    rs.resource.attributes.add(key="service.name").value.string_value = "llm-gateway"
    scope = rs.scope_spans.add()

    trace_id = bytes.fromhex("99" * 16)

    # Span 0: UNSET
    s0 = scope.spans.add()
    s0.trace_id = trace_id
    s0.span_id = bytes.fromhex("01" * 8)
    s0.name = "query-embedding"
    s0.start_time_unix_nano = 1000
    s0.end_time_unix_nano = 2000
    s0.status.code = Status.STATUS_CODE_UNSET  # 0
    s0.attributes.add(key="gen_ai.request.model").value.string_value = "text-embedding-3-small"

    # Span 1: OK
    s1 = scope.spans.add()
    s1.trace_id = trace_id
    s1.span_id = bytes.fromhex("02" * 8)
    s1.name = "vector-search"
    s1.start_time_unix_nano = 2000
    s1.end_time_unix_nano = 3000
    s1.status.code = Status.STATUS_CODE_OK  # 1
    s1.attributes.add(key="gen_ai.request.model").value.string_value = "text-embedding-3-small"

    # Span 2: ERROR
    s2 = scope.spans.add()
    s2.trace_id = trace_id
    s2.span_id = bytes.fromhex("03" * 8)
    s2.name = "chat-completion"
    s2.start_time_unix_nano = 3000
    s2.end_time_unix_nano = 4000
    s2.status.code = Status.STATUS_CODE_ERROR  # 2
    s2.status.message = "Rate limit exceeded"
    s2.attributes.add(key="gen_ai.request.model").value.string_value = "text-embedding-3-small"

    (settings.wal_dir / "wal-0.wal").write_bytes(_wal_record(request.SerializeToString()))
    materialize(settings)

    with TestClient(create_app(settings)) as client:
        # 1. Traces list
        traces = client.get("/api/v1/traces").json()
        assert len(traces) == 1
        assert traces[0]["span_count"] == 3
        assert traces[0]["error_count"] == 1  # Only span 2 is an error

        # 2. Analytics overview
        overview = client.get("/api/v1/analytics/overview").json()
        assert overview["total_spans"] == 3
        assert overview["error_rate"] == round(1 / 3, 4)

        # 3. Model analytics
        models = client.get("/api/v1/analytics/models").json()
        assert len(models) == 1
        assert models[0]["request_count"] == 3
        assert models[0]["error_count"] == 1
        assert models[0]["error_rate"] == round(1 / 3, 4)
