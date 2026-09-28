"""
Tests for Materialization Crash Recovery and Idempotency (Phase 3)
"""
from pathlib import Path
import struct
import binascii
import pytest
import sqlite3
import pyarrow.parquet as pq

from app.control import initialize_control_store
from app.main import create_app
from app.materializer import materialize, cleanup_orphaned_parquet_files
from app.settings import Settings
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest


def _wal_record(payload: bytes) -> bytes:
    return struct.pack("<II", len(payload), binascii.crc32(payload) & 0xFFFFFFFF) + payload


def _make_request(trace_id_hex: str) -> ExportTraceServiceRequest:
    request = ExportTraceServiceRequest()
    rs = request.resource_spans.add()
    rs.resource.attributes.add(key="service.name").value.string_value = "crash-service"
    s = rs.scope_spans.add().spans.add()
    s.trace_id = bytes.fromhex(trace_id_hex)
    s.span_id = bytes.fromhex("22" * 8)
    s.name = "test-operation"
    s.start_time_unix_nano = 1000
    s.end_time_unix_nano = 2000
    return request


def _settings(tmp_path: Path) -> Settings:
    return Settings(tmp_path, tmp_path / "wal", tmp_path / "parquet", tmp_path / "control.db")


def test_orphaned_temporary_files_are_cleaned_safely(tmp_path: Path):
    settings = _settings(tmp_path)
    settings.parquet_dir.mkdir(parents=True, exist_ok=True)
    initialize_control_store(settings)

    # Create orphaned .tmp file
    orphan_tmp = settings.parquet_dir / "spans-abandoned.parquet.tmp"
    orphan_tmp.write_bytes(b"garbage-data")

    # Create uncommitted .parquet file
    orphan_parquet = settings.parquet_dir / "spans-uncommitted.parquet"
    orphan_parquet.write_bytes(b"uncommitted")

    # Run cleanup
    cleaned = cleanup_orphaned_parquet_files(settings)
    assert cleaned >= 2
    assert not orphan_tmp.exists()
    assert not orphan_parquet.exists()


def test_crash_before_manifest_commit_does_not_duplicate_on_restart(tmp_path: Path, monkeypatch):
    settings = _settings(tmp_path)
    settings.wal_dir.mkdir(parents=True, exist_ok=True)
    initialize_control_store(settings)

    payload = _make_request("bb" * 16).SerializeToString()
    (settings.wal_dir / "wal-0.wal").write_bytes(_wal_record(payload))

    original_connect = sqlite3.connect
    should_fail = True

    class FailingConnection:
        def __init__(self, real_conn):
            self._conn = real_conn
        def __getattr__(self, name):
            return getattr(self._conn, name)
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc_val, exc_tb):
            return self._conn.__exit__(exc_type, exc_val, exc_tb)
        def commit(self):
            nonlocal should_fail
            if should_fail:
                raise sqlite3.OperationalError("Simulated crash during SQLite commit")
            return self._conn.commit()

    def fake_connect(*args, **kwargs):
        real = original_connect(*args, **kwargs)
        if should_fail:
            return FailingConnection(real)
        return real

    monkeypatch.setattr(sqlite3, "connect", fake_connect)

    # First run fails during SQLite commit
    with pytest.raises(sqlite3.OperationalError):
        materialize(settings)

    # Turn off failure simulation (simulate restart)
    should_fail = False

    # Next run (restart) should clean orphans, re-process cleanly, and commit
    count = materialize(settings)
    assert count == 1

    # Ensure idempotency
    assert materialize(settings) == 0

    # Ensure exactly 1 span row exists
    parquet_files = list(settings.parquet_dir.glob("*.parquet"))
    assert len(parquet_files) == 1
    table = pq.read_table(parquet_files[0])
    assert table.num_rows == 1
