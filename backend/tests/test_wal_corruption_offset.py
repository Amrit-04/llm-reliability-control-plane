"""
Tests for Exact WAL Corruption Offset Reporting and Diagnostics (P1 #11)
"""
from pathlib import Path
import struct
import binascii
import pytest
import sqlite3

from app.control import initialize_control_store
from app.materializer import materialize
from app.settings import Settings
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest


def _wal_record(payload: bytes) -> bytes:
    return struct.pack("<II", len(payload), binascii.crc32(payload) & 0xFFFFFFFF) + payload


def _make_span(span_name: str) -> ExportTraceServiceRequest:
    request = ExportTraceServiceRequest()
    rs = request.resource_spans.add()
    rs.resource.attributes.add(key="service.name").value.string_value = "diagnostic-svc"
    s = rs.scope_spans.add().spans.add()
    s.trace_id = bytes.fromhex("44" * 16)
    s.span_id = bytes.fromhex("55" * 8)
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
    )


def test_wal_corruption_offset_at_beginning_middle_and_end(tmp_path: Path):
    """
    Test WAL corruption at precise offsets and verify the exact byte offset
    is recorded in the wal_corruption_events database table.
    """
    settings = _settings(tmp_path)
    settings.wal_dir.mkdir(parents=True, exist_ok=True)
    initialize_control_store(settings)

    # 1. First segment: corrupted at offset 0 (invalid zero length)
    seg1 = settings.wal_dir / "wal-001-0.wal"
    seg1.write_bytes(struct.pack("<II", 0, 0))  # zero-length record at offset 0

    materialize(settings)

    with sqlite3.connect(settings.sqlite_path) as conn:
        events = conn.execute(
            "SELECT source_file, offset, reason FROM wal_corruption_events WHERE source_file = 'wal-001-0.wal'"
        ).fetchall()
        assert len(events) == 1
        assert events[0][0] == "wal-001-0.wal"
        assert events[0][1] == 0  # Exact offset 0
        assert "zero-length" in events[0][2]

    # 2. Second segment: 2 valid records, followed by CRC mismatch at precise offset
    valid1 = _wal_record(_make_span("span-1").SerializeToString())
    valid2 = _wal_record(_make_span("span-2").SerializeToString())
    corrupt_payload = b"corrupted bytes payload here"
    corrupt_rec = struct.pack("<II", len(corrupt_payload), 0xCAFEBABE) + corrupt_payload
    expected_offset = len(valid1) + len(valid2)

    seg2 = settings.wal_dir / "wal-002-0.wal"
    seg2.write_bytes(valid1 + valid2 + corrupt_rec)

    count = materialize(settings)
    assert count == 2  # Processed the 2 valid records before corruption

    with sqlite3.connect(settings.sqlite_path) as conn:
        events = conn.execute(
            "SELECT source_file, offset, reason FROM wal_corruption_events WHERE source_file = 'wal-002-0.wal'"
        ).fetchall()
        assert len(events) == 1
        assert events[0][0] == "wal-002-0.wal"
        assert events[0][1] == expected_offset  # Exact byte offset
        assert "checksum mismatch" in events[0][2]
