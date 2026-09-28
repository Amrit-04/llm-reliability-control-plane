"""
Tests for Materializer Concurrency and Locking (Phase 1)
"""
import concurrent.futures
from pathlib import Path
import struct
import binascii
import time

import pytest
from fastapi.testclient import TestClient
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

from app.control import initialize_control_store
from app.main import create_app
from app.materializer import materialize
from app.settings import Settings


def _wal_record(payload: bytes) -> bytes:
    return struct.pack("<II", len(payload), binascii.crc32(payload) & 0xFFFFFFFF) + payload


def _make_request(trace_id_hex: str, span_name: str = "test-span") -> ExportTraceServiceRequest:
    request = ExportTraceServiceRequest()
    rs = request.resource_spans.add()
    rs.resource.attributes.add(key="service.name").value.string_value = "concurrency-service"
    s = rs.scope_spans.add().spans.add()
    s.trace_id = bytes.fromhex(trace_id_hex)
    s.span_id = bytes.fromhex("11" * 8)
    s.name = span_name
    s.start_time_unix_nano = 100
    s.end_time_unix_nano = 200
    return request


def _settings(tmp_path: Path) -> Settings:
    return Settings(tmp_path, tmp_path / "wal", tmp_path / "parquet", tmp_path / "control.db")


def test_concurrent_materialize_calls_prevent_duplicates(tmp_path: Path):
    settings = _settings(tmp_path)
    settings.wal_dir.mkdir(parents=True, exist_ok=True)
    initialize_control_store(settings)

    # Write 5 WAL records
    for i in range(5):
        payload = _make_request(f"{i:02x}" * 16).SerializeToString()
        (settings.wal_dir / f"wal-{i}.wal").write_bytes(_wal_record(payload))

    # Run materialize concurrently across 10 threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(materialize, settings) for _ in range(10)]
        results = [f.result() for f in futures]

    # Total materialized spans across all threads must exactly equal 5
    assert sum(results) == 5

    # Repeated materialization must yield 0
    assert materialize(settings) == 0

    # Ensure no duplicates in trace API
    with TestClient(create_app(settings)) as client:
        traces = client.get("/api/v1/traces").json()
        assert len(traces) == 5


def test_manual_trigger_with_simultaneous_callers(tmp_path: Path):
    settings = _settings(tmp_path)
    settings.wal_dir.mkdir(parents=True, exist_ok=True)
    initialize_control_store(settings)

    payload = _make_request("aa" * 16).SerializeToString()
    (settings.wal_dir / "wal-0.wal").write_bytes(_wal_record(payload))

    with TestClient(create_app(settings)) as client:
        def trigger():
            return client.post("/api/v1/materialize").json()["materialized_spans"]

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(trigger) for _ in range(5)]
            counts = [f.result() for f in futures]

        assert sum(counts) == 1
        traces = client.get("/api/v1/traces").json()
        assert len(traces) == 1
