"""
Tests for Parquet Compaction Atomicity, Failure Injection, and Manifest Query Isolation (P0 #1)
"""
from pathlib import Path
import struct
import binascii
import pytest
import sqlite3
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from app.control import initialize_control_store, get_committed_parquet_files
from app.materializer import (
    materialize,
    compact_parquet_files,
    cleanup_orphaned_parquet_files,
    SPAN_ARROW_SCHEMA,
)
from app.main import create_app
from app.settings import Settings
from fastapi.testclient import TestClient
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest


def _wal_record(payload: bytes) -> bytes:
    return struct.pack("<II", len(payload), binascii.crc32(payload) & 0xFFFFFFFF) + payload


def _make_span_request(trace_hex: str, span_hex: str, span_name: str) -> ExportTraceServiceRequest:
    request = ExportTraceServiceRequest()
    rs = request.resource_spans.add()
    rs.resource.attributes.add(key="service.name").value.string_value = "order-service"
    s = rs.scope_spans.add().spans.add()
    s.trace_id = bytes.fromhex(trace_hex)
    s.span_id = bytes.fromhex(span_hex)
    s.name = span_name
    s.start_time_unix_nano = 1000
    s.end_time_unix_nano = 2000
    return request


def _settings(tmp_path: Path, batch_size: int = 2) -> Settings:
    return Settings(
        tmp_path,
        tmp_path / "wal",
        tmp_path / "parquet",
        tmp_path / "control.db",
        materialize_batch_size=batch_size,
    )


def test_crash_before_manifest_commit_leaves_uncommitted_file_unqueried(tmp_path: Path):
    """
    Failure injection: A .parquet.tmp or uncommitted .parquet file on disk
    is never returned by get_committed_parquet_files and is cleaned up on restart.
    """
    settings = _settings(tmp_path)
    settings.wal_dir.mkdir(parents=True, exist_ok=True)
    settings.parquet_dir.mkdir(parents=True, exist_ok=True)
    initialize_control_store(settings)

    # 1. Create a legitimate materialized span
    payload = _make_span_request("aa" * 16, "01" * 8, "span-1").SerializeToString()
    (settings.wal_dir / "wal-0.wal").write_bytes(_wal_record(payload))
    materialize(settings)

    committed_files_initial = get_committed_parquet_files(settings)
    assert len(committed_files_initial) == 1

    # 2. Simulate a crashed writer: orphaned .tmp and uncommitted .parquet
    dummy_table = pa.Table.from_pylist([{
        "trace_id": "bb" * 16,
        "span_id": "02" * 8,
        "parent_span_id": None,
        "name": "ghost-span",
        "kind": 1,
        "service_name": "ghost",
        "project_id": None,
        "start_time_unix_nano": 1000,
        "end_time_unix_nano": 2000,
        "duration_ns": 1000,
        "status_code": 1,
        "status_message": None,
        "scope_name": None,
        "scope_version": None,
        "trace_state": None,
        "flags": 0,
        "resource_attributes_json": "{}",
        "attributes_json": "{}",
        "events_json": "[]",
        "links_json": "[]",
        "dropped_attributes_count": 0,
        "dropped_events_count": 0,
        "dropped_links_count": 0,
        "source_file": "wal-ghost.wal",
        "source_offset": 0,
    }], schema=SPAN_ARROW_SCHEMA)

    tmp_parquet = settings.parquet_dir / "tmp-spans-crashed.parquet.tmp"
    pq.write_table(dummy_table, tmp_parquet)

    uncommitted_parquet = settings.parquet_dir / "spans-uncommitted.parquet"
    pq.write_table(dummy_table, uncommitted_parquet)

    # 3. Verify query layer (without lifespan auto-cleanup) ignores uncommitted files
    committed = get_committed_parquet_files(settings)
    assert len(committed) == 1
    assert committed[0].name == committed_files_initial[0].name

    # 4. Explicitly run orphan reconciliation cleanup
    deleted = cleanup_orphaned_parquet_files(settings)
    assert deleted == 2
    assert not tmp_parquet.exists()
    assert not uncommitted_parquet.exists()

    # 5. Starting the app after crash verifies clean state
    with TestClient(create_app(settings)) as client:
        traces = client.get("/api/v1/traces").json()
        assert len(traces) == 1
        assert traces[0]["trace_id"] == "aa" * 16


def test_crash_after_manifest_commit_before_old_file_unlink(tmp_path: Path):
    """
    Failure injection: Manifest marks old files COMPACTED and new file COMMITTED,
    but old physical files still exist on disk (crash before unlink).
    Queries must NOT return duplicate records.
    """
    settings = _settings(tmp_path, batch_size=2)
    settings.wal_dir.mkdir(parents=True, exist_ok=True)
    initialize_control_store(settings)

    # Ingest 4 spans across 2 files
    records = bytearray()
    for i in range(4):
        p = _make_span_request(f"{i:02x}" * 16, f"{i:02x}" * 8, f"span-{i}").SerializeToString()
        records.extend(_wal_record(p))
    (settings.wal_dir / "wal-0.wal").write_bytes(records)
    materialize(settings)

    files_before = get_committed_parquet_files(settings)
    assert len(files_before) == 2

    # Perform compaction
    compact_parquet_files(settings)

    files_after = get_committed_parquet_files(settings)
    assert len(files_after) == 1

    # Simulate an old file resurrected or un-deleted on disk
    old_file_resurrected = settings.parquet_dir / files_before[0].name
    dummy_table = pa.Table.from_pylist([{
        "trace_id": "00" * 16,
        "span_id": "00" * 8,
        "parent_span_id": None,
        "name": "span-0",
        "kind": 1,
        "service_name": "order-service",
        "project_id": None,
        "start_time_unix_nano": 1000,
        "end_time_unix_nano": 2000,
        "duration_ns": 1000,
        "status_code": 1,
        "status_message": None,
        "scope_name": None,
        "scope_version": None,
        "trace_state": None,
        "flags": 0,
        "resource_attributes_json": "{}",
        "attributes_json": "{}",
        "events_json": "[]",
        "links_json": "[]",
        "dropped_attributes_count": 0,
        "dropped_events_count": 0,
        "dropped_links_count": 0,
        "source_file": "wal-0.wal",
        "source_offset": 0,
    }], schema=SPAN_ARROW_SCHEMA)
    pq.write_table(dummy_table, old_file_resurrected)

    # SQLite manifest has it as COMPACTED, so get_committed_parquet_files excludes it
    with sqlite3.connect(settings.sqlite_path) as conn:
        status = conn.execute(
            "SELECT status FROM parquet_files WHERE filename = ?",
            (files_before[0].name,),
        ).fetchone()
        assert status[0] == "COMPACTED"

    committed = get_committed_parquet_files(settings)
    assert len(committed) == 1
    assert committed[0].name == files_after[0].name

    with TestClient(create_app(settings)) as client:
        traces = client.get("/api/v1/traces").json()
        assert len(traces) == 4
        # Verify no duplicate spans
        for t in traces:
            assert t["span_count"] == 1
