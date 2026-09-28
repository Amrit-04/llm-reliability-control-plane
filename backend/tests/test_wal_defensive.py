"""
Tests for Defensive WAL Parsing and Corruption Handling (Phase 4 & Phase 5)
"""
from pathlib import Path
import struct
import binascii
import pytest
import sqlite3

from app.control import initialize_control_store
from app.materializer import materialize, _wal_records_from
from app.settings import Settings
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest


def _wal_record(payload: bytes) -> bytes:
    return struct.pack("<II", len(payload), binascii.crc32(payload) & 0xFFFFFFFF) + payload


def _make_request(span_name: str) -> ExportTraceServiceRequest:
    request = ExportTraceServiceRequest()
    rs = request.resource_spans.add()
    rs.resource.attributes.add(key="service.name").value.string_value = "defensive-service"
    s = rs.scope_spans.add().spans.add()
    s.trace_id = bytes.fromhex("33" * 16)
    s.span_id = bytes.fromhex("44" * 8)
    s.name = span_name
    s.start_time_unix_nano = 100
    s.end_time_unix_nano = 200
    return request


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        tmp_path,
        tmp_path / "wal",
        tmp_path / "parquet",
        tmp_path / "control.db",
        max_wal_record_bytes=1024,  # Small limit for testing
    )


def test_reject_zero_length_record(tmp_path: Path):
    wal_path = tmp_path / "zero.wal"
    wal_path.write_bytes(struct.pack("<II", 0, 0))

    with pytest.raises(ValueError, match="invalid zero-length record"):
        list(_wal_records_from(wal_path, 0, max_record_bytes=1024))


def test_reject_oversized_record(tmp_path: Path):
    wal_path = tmp_path / "oversized.wal"
    wal_path.write_bytes(struct.pack("<II", 2048, 0x12345678))

    with pytest.raises(ValueError, match="oversized WAL record"):
        list(_wal_records_from(wal_path, 0, max_record_bytes=1024))


def test_reject_truncated_header(tmp_path: Path):
    wal_path = tmp_path / "trunc_header.wal"
    wal_path.write_bytes(b"\x05\x00")  # Only 2 bytes instead of 8

    with pytest.raises(ValueError, match="truncated WAL header"):
        list(_wal_records_from(wal_path, 0, max_record_bytes=1024))


def test_reject_truncated_payload(tmp_path: Path):
    wal_path = tmp_path / "trunc_payload.wal"
    # Header claims 50 bytes, but only 10 bytes present
    wal_path.write_bytes(struct.pack("<II", 50, 0x12345678) + b"0123456789")

    with pytest.raises(ValueError, match="truncated WAL payload"):
        list(_wal_records_from(wal_path, 0, max_record_bytes=1024))


def test_reject_crc_mismatch(tmp_path: Path):
    wal_path = tmp_path / "bad_crc.wal"
    payload = b"valid payload data"
    # Bad expected CRC
    wal_path.write_bytes(struct.pack("<II", len(payload), 0xDEADBEEF) + payload)

    with pytest.raises(ValueError, match="WAL checksum mismatch"):
        list(_wal_records_from(wal_path, 0, max_record_bytes=1024))


def test_corruption_in_middle_processes_valid_data_and_logs_event(tmp_path: Path):
    settings = _settings(tmp_path)
    settings.wal_dir.mkdir(parents=True, exist_ok=True)
    initialize_control_store(settings)

    # Record 1: Valid
    payload1 = _make_request("span-1").SerializeToString()
    rec1 = _wal_record(payload1)

    # Record 2: Corrupted CRC in middle
    payload2 = _make_request("span-2").SerializeToString()
    rec2 = struct.pack("<II", len(payload2), 0xBAADF00D) + payload2

    # Record 3: Valid (should not be processed due to framing untrustworthiness)
    payload3 = _make_request("span-3").SerializeToString()
    rec3 = _wal_record(payload3)

    wal_file = settings.wal_dir / "wal-mixed.wal"
    wal_file.write_bytes(rec1 + rec2 + rec3)

    # Materialization should safely salvage span-1 and record corruption event
    count = materialize(settings)
    assert count == 1

    # Check diagnostic corruption logging in SQLite
    with sqlite3.connect(settings.sqlite_path) as conn:
        events = conn.execute("SELECT source_file, reason FROM wal_corruption_events").fetchall()
        assert len(events) == 1
        assert events[0][0] == "wal-mixed.wal"
        assert "checksum mismatch" in events[0][1]

        # Lifecycle should show quarantined
        lifecycle = conn.execute("SELECT status FROM wal_file_lifecycle WHERE filename = 'wal-mixed.wal'").fetchone()
        assert lifecycle[0] == "QUARANTINED"
