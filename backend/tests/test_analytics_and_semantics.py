"""
Tests for Trace Semantics, Parallel Spans, Wall-Clock vs Aggregate Duration, and GenAI Metrics (Phases 10, 11, 16)
"""
from pathlib import Path
import struct
import binascii
import pytest
from fastapi.testclient import TestClient

from app.control import initialize_control_store
from app.main import create_app
from app.materializer import materialize
from app.settings import Settings
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest


def _wal_record(payload: bytes) -> bytes:
    return struct.pack("<II", len(payload), binascii.crc32(payload) & 0xFFFFFFFF) + payload


def _settings(tmp_path: Path) -> Settings:
    return Settings(tmp_path, tmp_path / "wal", tmp_path / "parquet", tmp_path / "control.db")


def test_parallel_spans_wall_clock_vs_aggregate_duration(tmp_path: Path):
    """
    Test scenario:
      Root span: 0 to 100 ns (duration 100 ns)
      Child A (parallel): 10 to 90 ns (duration 80 ns)
      Child B (parallel): 10 to 90 ns (duration 80 ns)

    Wall-clock duration: max(end) - min(start) = 100 - 0 = 100 ns
    Aggregate span duration: 100 + 80 + 80 = 260 ns
    """
    settings = _settings(tmp_path)
    settings.wal_dir.mkdir(parents=True, exist_ok=True)
    initialize_control_store(settings)

    request = ExportTraceServiceRequest()
    rs = request.resource_spans.add()
    rs.resource.attributes.add(key="service.name").value.string_value = "orchestrator"
    scope = rs.scope_spans.add()

    trace_id = bytes.fromhex("77" * 16)
    root_id = bytes.fromhex("01" * 8)
    child_a_id = bytes.fromhex("02" * 8)
    child_b_id = bytes.fromhex("03" * 8)

    # Root span
    root = scope.spans.add()
    root.trace_id = trace_id
    root.span_id = root_id
    root.name = "agent.invoke"
    root.start_time_unix_nano = 0
    root.end_time_unix_nano = 100

    # Child A (parallel)
    child_a = scope.spans.add()
    child_a.trace_id = trace_id
    child_a.span_id = child_a_id
    child_a.parent_span_id = root_id
    child_a.name = "tool.call_a"
    child_a.start_time_unix_nano = 10
    child_a.end_time_unix_nano = 90

    # Child B (parallel)
    child_b = scope.spans.add()
    child_b.trace_id = trace_id
    child_b.span_id = child_b_id
    child_b.parent_span_id = root_id
    child_b.name = "tool.call_b"
    child_b.start_time_unix_nano = 10
    child_b.end_time_unix_nano = 90

    (settings.wal_dir / "wal-0.wal").write_bytes(_wal_record(request.SerializeToString()))
    materialize(settings)

    with TestClient(create_app(settings)) as client:
        traces = client.get("/api/v1/traces").json()
        assert len(traces) == 1
        trace = traces[0]
        assert trace["span_count"] == 3
        assert trace["wall_clock_duration_ns"] == 100
        assert trace["aggregate_span_duration_ns"] == 260
        assert "orchestrator" in trace["services"]


def test_genai_metrics_with_missing_and_malformed_token_fields(tmp_path: Path):
    settings = _settings(tmp_path)
    settings.wal_dir.mkdir(parents=True, exist_ok=True)
    initialize_control_store(settings)

    request = ExportTraceServiceRequest()
    rs = request.resource_spans.add()
    rs.resource.attributes.add(key="service.name").value.string_value = "llm-gateway"
    scope = rs.scope_spans.add()

    # Span 1: Valid model and tokens
    span1 = scope.spans.add()
    span1.trace_id = bytes.fromhex("88" * 16)
    span1.span_id = bytes.fromhex("11" * 8)
    span1.name = "chat.completions"
    span1.start_time_unix_nano = 1_000_000_000
    span1.end_time_unix_nano = 2_000_000_000  # 1 second duration
    span1.attributes.add(key="gen_ai.request.model").value.string_value = "gpt-4o"
    span1.attributes.add(key="gen_ai.usage.input_tokens").value.int_value = 500
    span1.attributes.add(key="gen_ai.usage.output_tokens").value.int_value = 100

    # Span 2: Same model, string tokens / malformed numeric
    span2 = scope.spans.add()
    span2.trace_id = bytes.fromhex("88" * 16)
    span2.span_id = bytes.fromhex("22" * 8)
    span2.name = "chat.completions"
    span2.start_time_unix_nano = 2_000_000_000
    span2.end_time_unix_nano = 3_000_000_000
    span2.attributes.add(key="gen_ai.request.model").value.string_value = "gpt-4o"
    span2.attributes.add(key="gen_ai.usage.input_tokens").value.string_value = "300"
    span2.attributes.add(key="gen_ai.usage.output_tokens").value.string_value = "invalid_number"

    (settings.wal_dir / "wal-0.wal").write_bytes(_wal_record(request.SerializeToString()))
    materialize(settings)

    with TestClient(create_app(settings)) as client:
        overview = client.get("/api/v1/analytics/overview").json()
        assert overview["total_traces"] == 1
        assert overview["total_spans"] == 2
        assert overview["total_llm_requests"] == 2
        # 500 + 300 = 800
        assert overview["total_input_tokens"] == 800
        # 100 + 0 (invalid_number safely casted to null/0) = 100
        assert overview["total_output_tokens"] == 100

        models = client.get("/api/v1/analytics/models").json()
        assert len(models) == 1
        assert models[0]["model"] == "gpt-4o"
        assert models[0]["request_count"] == 2
        assert models[0]["total_input_tokens"] == 800
        assert models[0]["total_output_tokens"] == 100
