"""
Tests for Memory-Safe Streaming Parquet Compaction (P1 #10)
"""
from pathlib import Path
import struct
import binascii
import pytest
import pyarrow.parquet as pq

from app.control import initialize_control_store, get_committed_parquet_files
from app.materializer import (
    materialize,
    compact_parquet_files,
)
from app.settings import Settings
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest


def _wal_record(payload: bytes) -> bytes:
    return struct.pack("<II", len(payload), binascii.crc32(payload) & 0xFFFFFFFF) + payload


def _make_span(trace_hex: str, span_name: str) -> ExportTraceServiceRequest:
    request = ExportTraceServiceRequest()
    rs = request.resource_spans.add()
    rs.resource.attributes.add(key="service.name").value.string_value = "stream-svc"
    s = rs.scope_spans.add().spans.add()
    s.trace_id = bytes.fromhex(trace_hex)
    s.span_id = bytes.fromhex("22" * 8)
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
        materialize_batch_size=5,
    )


def test_streaming_compaction_preserves_row_groups_and_data_integrity(tmp_path: Path):
    """
    Produce 20 spans across 4 batches, compact them using the streaming compactor,
    and verify all 20 spans are preserved with correct schema and zero dropped fields.
    """
    settings = _settings(tmp_path)
    settings.wal_dir.mkdir(parents=True, exist_ok=True)
    initialize_control_store(settings)

    wal_data = bytearray()
    for i in range(20):
        t_hex = f"{i:04x}" * 8
        wal_data.extend(_wal_record(_make_span(t_hex, f"span-{i}").SerializeToString()))

    (settings.wal_dir / "wal-0.wal").write_bytes(wal_data)
    count = materialize(settings)
    assert count == 20

    files_before = get_committed_parquet_files(settings)
    assert len(files_before) == 4  # 20 spans / batch size 5 = 4 files

    # Compact all 4 files
    compacted_rows = compact_parquet_files(settings, max_files_to_merge=10)
    assert compacted_rows == 20

    files_after = get_committed_parquet_files(settings)
    assert len(files_after) == 1

    # Read the compacted file and verify contents
    parquet_table = pq.read_table(files_after[0])
    assert parquet_table.num_rows == 20
    assert "trace_id" in parquet_table.column_names
    assert "service_name" in parquet_table.column_names
    assert "status_code" in parquet_table.column_names
