"""
Tests for Cross-Process / Concurrent Storage Lifecycle Coordination (P0 #2)
"""
import concurrent.futures
from pathlib import Path
import struct
import binascii
import pytest
import sqlite3
import time

from app.control import initialize_control_store, get_committed_parquet_files
from app.materializer import (
    materialize,
    compact_parquet_files,
    cleanup_materialized_wal_files,
    storage_lifecycle_lock,
)
from app.settings import Settings
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest


def _wal_record(payload: bytes) -> bytes:
    return struct.pack("<II", len(payload), binascii.crc32(payload) & 0xFFFFFFFF) + payload


def _make_span(trace_hex: str, span_name: str) -> ExportTraceServiceRequest:
    request = ExportTraceServiceRequest()
    rs = request.resource_spans.add()
    rs.resource.attributes.add(key="service.name").value.string_value = "payment-svc"
    s = rs.scope_spans.add().spans.add()
    s.trace_id = bytes.fromhex(trace_hex)
    s.span_id = bytes.fromhex("11" * 8)
    s.name = span_name
    s.start_time_unix_nano = 1000
    s.end_time_unix_nano = 2000
    return request


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        tmp_path,
        tmp_path / "wal",
        tmp_path / "parquet",
        tmp_path / "control.db",
        materialize_batch_size=2,
        auto_cleanup_wal=False,
    )


def test_concurrent_materialize_compaction_and_cleanup_race(tmp_path: Path):
    """
    Execute materialization, compaction, and WAL cleanup concurrently across threads.
    Verify that storage_lifecycle_lock prevents race conditions and data corruption.
    """
    settings = _settings(tmp_path)
    settings.wal_dir.mkdir(parents=True, exist_ok=True)
    initialize_control_store(settings)

    # Populate 3 WAL files (2 sealed, 1 active)
    for seg_idx in range(3):
        records = bytearray()
        for i in range(4):
            trace_hex = f"{seg_idx:02x}{i:02x}" * 8
            payload = _make_span(trace_hex, f"span-{seg_idx}-{i}").SerializeToString()
            records.extend(_wal_record(payload))
        (settings.wal_dir / f"wal-000{seg_idx}-0.wal").write_bytes(records)

    errors = []

    def run_worker(action: str):
        try:
            for _ in range(5):
                if action == "materialize":
                    materialize(settings)
                elif action == "compact":
                    compact_parquet_files(settings, max_files_to_merge=5)
                elif action == "cleanup":
                    cleanup_materialized_wal_files(settings)
                time.sleep(0.01)
        except Exception as e:
            errors.append((action, e))

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        futures = [
            executor.submit(run_worker, "materialize"),
            executor.submit(run_worker, "materialize"),
            executor.submit(run_worker, "compact"),
            executor.submit(run_worker, "compact"),
            executor.submit(run_worker, "cleanup"),
            executor.submit(run_worker, "cleanup"),
        ]
        concurrent.futures.wait(futures)

    assert len(errors) == 0, f"Concurrent workers encountered errors: {errors}"

    # Final materialization pass to settle everything
    materialize(settings)

    # Check total spans in committed Parquet files
    committed = get_committed_parquet_files(settings)
    assert len(committed) >= 1
    total_spans = 0
    import pyarrow.parquet as pq
    for f in committed:
        total_spans += pq.read_table(f).num_rows

    assert total_spans == 12  # Exactly 3 segments * 4 spans, zero dropped or duplicated


def test_storage_lifecycle_lock_mutual_exclusion(tmp_path: Path):
    """
    Directly verify that storage_lifecycle_lock enforces exclusivity across simultaneous lockers.
    """
    settings = _settings(tmp_path)
    initialize_control_store(settings)

    results = []

    def locker_task(task_id: int):
        with storage_lifecycle_lock(settings, timeout_seconds=5.0):
            results.append((task_id, "enter"))
            time.sleep(0.05)
            results.append((task_id, "exit"))

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        f1 = executor.submit(locker_task, 1)
        f2 = executor.submit(locker_task, 2)
        concurrent.futures.wait([f1, f2])

    # Ensure no interleaved enter-enter without exit
    assert results == [
        (1, "enter"), (1, "exit"), (2, "enter"), (2, "exit")
    ] or results == [
        (2, "enter"), (2, "exit"), (1, "enter"), (1, "exit")
    ]
