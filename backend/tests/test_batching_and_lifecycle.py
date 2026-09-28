"""
Tests for Bounded Memory Batching, Manifest Scalability, Compaction, and WAL Lifecycle (Phases 6, 7, 8, 9)
"""
from pathlib import Path
import struct
import binascii
import pytest
import sqlite3
import pyarrow.parquet as pq

from app.control import initialize_control_store
from app.materializer import (
    materialize,
    compact_parquet_files,
    cleanup_materialized_wal_files,
)
from app.settings import Settings
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest


def _wal_record(payload: bytes) -> bytes:
    return struct.pack("<II", len(payload), binascii.crc32(payload) & 0xFFFFFFFF) + payload


def _make_request(span_name: str) -> ExportTraceServiceRequest:
    request = ExportTraceServiceRequest()
    rs = request.resource_spans.add()
    rs.resource.attributes.add(key="service.name").value.string_value = "batch-service"
    s = rs.scope_spans.add().spans.add()
    s.trace_id = bytes.fromhex("55" * 16)
    s.span_id = bytes.fromhex("66" * 8)
    s.name = span_name
    s.start_time_unix_nano = 1000
    s.end_time_unix_nano = 2000
    return request


def _settings(tmp_path: Path, batch_size: int = 5) -> Settings:
    return Settings(
        tmp_path,
        tmp_path / "wal",
        tmp_path / "parquet",
        tmp_path / "control.db",
        materialize_batch_size=batch_size,
    )


def test_bounded_batching_writes_multiple_parquet_files(tmp_path: Path):
    settings = _settings(tmp_path, batch_size=3)
    settings.wal_dir.mkdir(parents=True, exist_ok=True)
    initialize_control_store(settings)

    # Write 10 records into a single WAL file
    records_bytes = bytearray()
    for i in range(10):
        payload = _make_request(f"span-{i}").SerializeToString()
        records_bytes.extend(_wal_record(payload))

    (settings.wal_dir / "wal-0.wal").write_bytes(records_bytes)

    # Materialize with batch size 3: should produce 4 parquet files (3 + 3 + 3 + 1)
    count = materialize(settings)
    assert count == 10

    parquet_files = list(settings.parquet_dir.glob("*.parquet"))
    assert len(parquet_files) == 4

    total_rows = sum(pq.read_table(f).num_rows for f in parquet_files)
    assert total_rows == 10


def test_parquet_compaction_merges_small_files(tmp_path: Path):
    settings = _settings(tmp_path, batch_size=2)
    settings.wal_dir.mkdir(parents=True, exist_ok=True)
    initialize_control_store(settings)

    records_bytes = bytearray()
    for i in range(6):
        payload = _make_request(f"compaction-span-{i}").SerializeToString()
        records_bytes.extend(_wal_record(payload))

    (settings.wal_dir / "wal-0.wal").write_bytes(records_bytes)
    materialize(settings)

    parquet_files_before = list(settings.parquet_dir.glob("*.parquet"))
    assert len(parquet_files_before) == 3

    # Run compaction
    compacted_rows = compact_parquet_files(settings, max_files_to_merge=10)
    assert compacted_rows == 6

    parquet_files_after = list(settings.parquet_dir.glob("*.parquet"))
    assert len(parquet_files_after) == 1

    table = pq.read_table(parquet_files_after[0])
    assert table.num_rows == 6


def test_wal_lifecycle_and_safe_cleanup(tmp_path: Path):
    settings = _settings(tmp_path)
    settings.wal_dir.mkdir(parents=True, exist_ok=True)
    initialize_control_store(settings)

    # Segment 1 (sealed)
    payload1 = _make_request("seg-1").SerializeToString()
    (settings.wal_dir / "wal-1.wal").write_bytes(_wal_record(payload1))

    # Segment 2 (active)
    payload2 = _make_request("seg-2").SerializeToString()
    (settings.wal_dir / "wal-2.wal").write_bytes(_wal_record(payload2))

    # Materialize all
    materialize(settings)

    # Clean up materialized WAL files
    deleted = cleanup_materialized_wal_files(settings)

    # wal-1.wal should be deleted, wal-2.wal must be retained because it's active
    assert "wal-1.wal" in deleted
    assert not (settings.wal_dir / "wal-1.wal").exists()
    assert (settings.wal_dir / "wal-2.wal").exists()

    # Check lifecycle database table
    with sqlite3.connect(settings.sqlite_path) as conn:
        status = conn.execute("SELECT status FROM wal_file_lifecycle WHERE filename = 'wal-1.wal'").fetchone()
        assert status[0] == "DELETED"
