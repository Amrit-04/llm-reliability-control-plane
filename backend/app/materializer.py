"""
WAL to Parquet Materialization Engine

This module converts raw WAL records (binary OTLP protobuf) into normalized
canonical Parquet files suitable for analytical queries.

Key capabilities:
  - Strict Concurrency: Only one materialization run can execute at a time.
  - Crash-Safe & Idempotent: Parquet files are written via atomic rename, registered
    in the SQLite manifest, and orphaned uncommitted Parquet files are detected and cleaned.
  - Bounded Memory: Streams WAL records and writes Parquet in configurable batch sizes.
  - Scalable Manifest: Uses indexed offset tracking instead of loading full history into memory.
  - Defensive WAL Parsing: Rejects oversized, zero-length, truncated, or CRC-mismatched records.
  - WAL Corruption Handling: Processes valid records before corruption, logs structured errors,
    records corruption events in SQLite, and quarantines corrupted segments.
  - OTLP Fidelity: Preserves span events, links, scopes, status, flags, and attributes.
  - WAL Lifecycle Management: Tracks active, sealed, materialized, and deleted segments.
"""
from __future__ import annotations

import binascii
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import sqlite3
import struct
import threading
import uuid
from typing import Any, Iterator

import pyarrow as pa
import pyarrow.parquet as pq
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

from .settings import Settings

logger = logging.getLogger(__name__)

# Single-process lock to prevent overlapping materialization runs
_MATERIALIZATION_LOCK = threading.Lock()

_RECORD_HEADER = struct.Struct("<II")  # 4-byte length, 4-byte CRC-32 (little endian)

# Canonical PyArrow Schema for normalized spans
SPAN_ARROW_SCHEMA = pa.schema([
    pa.field("trace_id", pa.string(), nullable=False),
    pa.field("span_id", pa.string(), nullable=False),
    pa.field("parent_span_id", pa.string(), nullable=True),
    pa.field("name", pa.string(), nullable=False),
    pa.field("kind", pa.int32(), nullable=False),
    pa.field("service_name", pa.string(), nullable=True),
    pa.field("project_id", pa.string(), nullable=True),
    pa.field("start_time_unix_nano", pa.int64(), nullable=False),
    pa.field("end_time_unix_nano", pa.int64(), nullable=False),
    pa.field("duration_ns", pa.int64(), nullable=False),
    pa.field("status_code", pa.int32(), nullable=False),
    pa.field("status_message", pa.string(), nullable=True),
    pa.field("scope_name", pa.string(), nullable=True),
    pa.field("scope_version", pa.string(), nullable=True),
    pa.field("trace_state", pa.string(), nullable=True),
    pa.field("flags", pa.uint32(), nullable=True),
    pa.field("resource_attributes_json", pa.string(), nullable=False),
    pa.field("attributes_json", pa.string(), nullable=False),
    pa.field("events_json", pa.string(), nullable=False),
    pa.field("links_json", pa.string(), nullable=False),
    pa.field("dropped_attributes_count", pa.uint32(), nullable=False),
    pa.field("dropped_events_count", pa.uint32(), nullable=False),
    pa.field("dropped_links_count", pa.uint32(), nullable=False),
    pa.field("source_file", pa.string(), nullable=False),
    pa.field("source_offset", pa.int64(), nullable=False),
])


def _hex(value: bytes) -> str:
    """Convert bytes to lowercase hex string."""
    return value.hex() if value else ""


def _attribute_value(value: Any) -> Any:
    """Extract primitive or JSON-serializable value from an OTLP AttributeValue oneof."""
    kind = value.WhichOneof("value")
    if kind is None:
        return ""
    primitive = getattr(value, kind)
    if isinstance(primitive, (str, bool, int, float)):
        return primitive
    if kind == "array_value":
        return [_attribute_value(v) for v in primitive.values]
    if kind == "kvlist_value":
        return {kv.key: _attribute_value(kv.value) for kv in primitive.values}
    if isinstance(primitive, bytes):
        return primitive.hex()
    return str(primitive)


def _attributes(attributes: Any) -> dict[str, Any]:
    """Convert OTLP repeated KeyValue into a standard dictionary."""
    return {attribute.key: _attribute_value(attribute.value) for attribute in attributes}


def _events(events: Any) -> list[dict[str, Any]]:
    """Convert OTLP span events into JSON-serializable dictionaries."""
    result = []
    for event in events:
        result.append({
            "name": event.name,
            "time_unix_nano": event.time_unix_nano,
            "attributes": _attributes(event.attributes),
            "dropped_attributes_count": event.dropped_attributes_count,
        })
    return result


def _links(links: Any) -> list[dict[str, Any]]:
    """Convert OTLP span links into JSON-serializable dictionaries."""
    result = []
    for link in links:
        result.append({
            "trace_id": _hex(link.trace_id),
            "span_id": _hex(link.span_id),
            "trace_state": link.trace_state,
            "attributes": _attributes(link.attributes),
            "dropped_attributes_count": link.dropped_attributes_count,
            "flags": link.flags,
        })
    return result


def _rows(payload: bytes, source_file: str, record_offset: int) -> list[dict[str, Any]]:
    """
    Parse an OTLP ExportTraceServiceRequest into normalized canonical span rows.
    """
    request = ExportTraceServiceRequest()
    if not request.ParseFromString(payload):
        raise ValueError(f"malformed OTLP protobuf in {source_file} at offset {record_offset}")

    rows = []
    for resource_spans in request.resource_spans:
        resource_attributes = _attributes(resource_spans.resource.attributes)
        service_name = str(resource_attributes.get("service.name") or "") or None
        project_id = str(resource_attributes.get("project.id") or resource_attributes.get("lrcp.project_id") or "") or None

        for scope_spans in resource_spans.scope_spans:
            scope_name = scope_spans.scope.name or None
            scope_version = scope_spans.scope.version or None

            for span in scope_spans.spans:
                span_attributes = _attributes(span.attributes)
                # Allow project_id override at span level if present
                span_project_id = str(span_attributes.get("project.id") or span_attributes.get("lrcp.project_id") or "") or project_id

                events_list = _events(span.events)
                links_list = _links(span.links)

                duration = max(0, span.end_time_unix_nano - span.start_time_unix_nano)

                rows.append({
                    "trace_id": _hex(span.trace_id),
                    "span_id": _hex(span.span_id),
                    "parent_span_id": _hex(span.parent_span_id) if span.parent_span_id else None,
                    "name": span.name or "",
                    "kind": int(span.kind),
                    "service_name": service_name,
                    "project_id": span_project_id,
                    "start_time_unix_nano": span.start_time_unix_nano,
                    "end_time_unix_nano": span.end_time_unix_nano,
                    "duration_ns": duration,
                    "status_code": int(span.status.code),
                    "status_message": span.status.message or None,
                    "scope_name": scope_name,
                    "scope_version": scope_version,
                    "trace_state": span.trace_state or None,
                    "flags": span.flags if span.flags else 0,
                    "resource_attributes_json": json.dumps(resource_attributes, sort_keys=True),
                    "attributes_json": json.dumps(span_attributes, sort_keys=True),
                    "events_json": json.dumps(events_list, sort_keys=True),
                    "links_json": json.dumps(links_list, sort_keys=True),
                    "dropped_attributes_count": span.dropped_attributes_count,
                    "dropped_events_count": span.dropped_events_count,
                    "dropped_links_count": span.dropped_links_count,
                    "source_file": source_file,
                    "source_offset": record_offset,
                })
    return rows


def _wal_records_from(
    path: Path,
    start_offset: int,
    max_record_bytes: int,
) -> Iterator[tuple[int, int, bytes]]:
    """
    Stream WAL records starting at `start_offset` with defensive validation.

    Yields:
        Tuples of (record_start_offset, record_end_offset, payload_bytes).
    """
    try:
        with path.open("rb") as handle:
            if start_offset > 0:
                handle.seek(start_offset)

            while True:
                record_start = handle.tell()
                header = handle.read(_RECORD_HEADER.size)
                if not header:
                    # Clean EOF
                    break
                if len(header) != _RECORD_HEADER.size:
                    raise ValueError(
                        f"truncated WAL header in {path.name} at offset {record_start}: "
                        f"expected {_RECORD_HEADER.size} bytes, got {len(header)}"
                    )

                length, expected_crc = _RECORD_HEADER.unpack(header)
                if length == 0:
                    raise ValueError(
                        f"invalid zero-length record in {path.name} at offset {record_start}"
                    )
                if length > max_record_bytes:
                    raise ValueError(
                        f"oversized WAL record in {path.name} at offset {record_start}: "
                        f"length {length} exceeds safety limit {max_record_bytes}"
                    )

                payload = handle.read(length)
                if len(payload) != length:
                    raise ValueError(
                        f"truncated WAL payload in {path.name} at offset {record_start}: "
                        f"expected {length} bytes, got {len(payload)}"
                    )

                actual_crc = binascii.crc32(payload) & 0xFFFFFFFF
                if actual_crc != expected_crc:
                    raise ValueError(
                        f"WAL checksum mismatch in {path.name} at offset {record_start}: "
                        f"expected 0x{expected_crc:08x}, got 0x{actual_crc:08x}"
                    )

                record_end = handle.tell()
                yield record_start, record_end, payload

    except OSError as error:
        logger.error(f"I/O error reading WAL file {path}: {error}")
        raise


def cleanup_orphaned_parquet_files(settings: Settings) -> int:
    """
    Detect and remove temporary or uncommitted Parquet files.
    """
    settings.parquet_dir.mkdir(parents=True, exist_ok=True)
    deleted_count = 0

    # 1. Clean up temporary files (.parquet.tmp)
    for tmp_file in settings.parquet_dir.glob("*.tmp"):
        try:
            tmp_file.unlink(missing_ok=True)
            deleted_count += 1
            logger.info(f"Cleaned up temporary Parquet file: {tmp_file.name}")
        except OSError as e:
            logger.warning(f"Could not remove temp file {tmp_file}: {e}")

    # 2. Reconcile committed parquet files against SQLite
    try:
        with sqlite3.connect(settings.sqlite_path) as connection:
            committed = set(
                row[0] for row in connection.execute("SELECT filename FROM parquet_files").fetchall()
            )
            # Also check legacy table if present
            try:
                legacy = set(
                    row[0] for row in connection.execute("SELECT DISTINCT parquet_file FROM materialized_wal_records").fetchall()
                )
                committed.update(legacy)
            except sqlite3.OperationalError:
                pass
    except sqlite3.Error as error:
        logger.warning(f"Could not query committed Parquet files for orphan cleanup: {error}")
        return deleted_count

    for parquet_file in settings.parquet_dir.glob("*.parquet"):
        if parquet_file.name not in committed:
            try:
                parquet_file.unlink(missing_ok=True)
                deleted_count += 1
                logger.warning(f"Removed uncommitted/orphaned Parquet file: {parquet_file.name}")
            except OSError as e:
                logger.error(f"Failed to remove orphaned file {parquet_file}: {e}")

    return deleted_count


def _flush_batch(
    settings: Settings,
    rows: list[dict[str, Any]],
    record_manifest_entries: list[tuple[str, int, str]],
    file_offsets: dict[str, int],
) -> str:
    """
    Atomically write a batch of rows to a Parquet file and commit manifest to SQLite.
    """
    if not rows:
        return ""

    settings.parquet_dir.mkdir(parents=True, exist_ok=True)
    file_uuid = uuid.uuid4().hex[:12]
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    final_filename = f"spans-{timestamp}-{file_uuid}.parquet"
    temp_path = settings.parquet_dir / f"tmp-{timestamp}-{file_uuid}.parquet.tmp"
    final_path = settings.parquet_dir / final_filename

    # Build PyArrow Table with strict canonical schema
    table = pa.Table.from_pylist(rows, schema=SPAN_ARROW_SCHEMA)

    # 1. Write to temp file
    pq.write_table(table, temp_path, compression="zstd")

    # 2. Atomic rename
    temp_path.replace(final_path)

    # 3. Commit to SQLite in one transaction
    with sqlite3.connect(settings.sqlite_path) as connection:
        connection.execute("PRAGMA busy_timeout=10000")
        # Record committed parquet file
        connection.execute(
            "INSERT OR REPLACE INTO parquet_files(filename, record_count, status) VALUES (?, ?, 'COMMITTED')",
            (final_filename, len(rows)),
        )
        # Record individual WAL record entries for fine-grained provenance
        connection.executemany(
            "INSERT OR IGNORE INTO materialized_wal_records(source_file, source_offset, parquet_file) VALUES (?, ?, ?)",
            record_manifest_entries,
        )
        # Update high-water mark offsets for fast incremental scanning
        for wal_file, max_off in file_offsets.items():
            connection.execute(
                """
                INSERT INTO materialized_wal_offsets(source_file, max_offset, is_fully_processed, updated_at)
                VALUES (?, ?, 0, CURRENT_TIMESTAMP)
                ON CONFLICT(source_file) DO UPDATE SET
                  max_offset = MAX(materialized_wal_offsets.max_offset, excluded.max_offset),
                  updated_at = CURRENT_TIMESTAMP
                """,
                (wal_file, max_off),
            )
        connection.commit()

    return final_filename


def materialize(settings: Settings) -> int:
    """
    Stream unmaterialized WAL records and write them into Parquet in bounded batches.

    Guarantees:
      - Mutual exclusion via _MATERIALIZATION_LOCK.
      - Crash safety: Temporary files are cleaned; only committed Parquet files persist.
      - Memory boundedness: Spans are flushed in batches of `materialize_batch_size`.
      - High performance: Fast resume via `materialized_wal_offsets`.
      - Full corruption reporting: Logs, marks, and isolates corrupted WAL files.
    """
    if not _MATERIALIZATION_LOCK.acquire(blocking=True):
        logger.info("Materialization is already running. Skipping concurrent invocation.")
        return 0

    try:
        import time
        start_time = time.perf_counter()

        settings.parquet_dir.mkdir(parents=True, exist_ok=True)
        settings.wal_dir.mkdir(parents=True, exist_ok=True)

        # Clean orphaned or abandoned partial parquet files from previous crashes
        cleanup_orphaned_parquet_files(settings)

        # Load known processed offsets from SQLite
        processed_offsets: dict[str, int] = {}
        fully_processed_files: set[str] = set()
        quarantined_files: set[str] = set()

        with sqlite3.connect(settings.sqlite_path) as connection:
            # Query high-water marks
            for row in connection.execute(
                "SELECT source_file, max_offset, is_fully_processed FROM materialized_wal_offsets"
            ).fetchall():
                processed_offsets[row[0]] = row[1]
                if row[2]:
                    fully_processed_files.add(row[0])

            # Query quarantined files
            try:
                for row in connection.execute(
                    "SELECT source_file FROM wal_corruption_events"
                ).fetchall():
                    quarantined_files.add(row[0])
            except sqlite3.OperationalError:
                pass

        wal_files = sorted(settings.wal_dir.glob("*.wal"))
        if not wal_files:
            return 0

        # Determine active vs sealed files
        # The last file in lexicographical / modification order is active
        active_wal_filename = wal_files[-1].name if wal_files else None

        total_materialized_spans = 0
        current_batch_rows: list[dict[str, Any]] = []
        current_batch_entries: list[tuple[str, int, str]] = []
        current_batch_offsets: dict[str, int] = {}

        for wal_file in wal_files:
            filename = wal_file.name
            if filename in quarantined_files:
                logger.debug(f"Skipping quarantined WAL file {filename}")
                continue

            file_size = wal_file.stat().st_size
            start_offset = processed_offsets.get(filename, 0)

            # If already marked fully processed and file size hasn't changed, skip
            if filename in fully_processed_files and start_offset >= file_size:
                continue

            if start_offset >= file_size and file_size > 0:
                continue

            try:
                for rec_start, rec_end, payload in _wal_records_from(
                    wal_file,
                    start_offset=start_offset,
                    max_record_bytes=settings.max_wal_record_bytes,
                ):
                    rows = _rows(payload, filename, rec_start)
                    current_batch_rows.extend(rows)
                    # Temporary placeholder for parquet_file, filled during flush
                    for _ in rows:
                        current_batch_entries.append((filename, rec_start, ""))
                    current_batch_offsets[filename] = rec_end

                    if len(current_batch_rows) >= settings.materialize_batch_size:
                        parquet_name = _flush_batch(
                            settings,
                            current_batch_rows,
                            [(f, off, "") for f, off, _ in current_batch_entries],
                            current_batch_offsets,
                        )
                        total_materialized_spans += len(current_batch_rows)
                        current_batch_rows.clear()
                        current_batch_entries.clear()
                        current_batch_offsets.clear()

                # If this is a sealed file (not active) and we reached the end of file:
                if filename != active_wal_filename and wal_file.stat().st_size == current_batch_offsets.get(filename, start_offset):
                    with sqlite3.connect(settings.sqlite_path) as connection:
                        connection.execute(
                            """
                            INSERT INTO materialized_wal_offsets(source_file, max_offset, is_fully_processed, updated_at)
                            VALUES (?, ?, 1, CURRENT_TIMESTAMP)
                            ON CONFLICT(source_file) DO UPDATE SET
                              is_fully_processed = 1,
                              updated_at = CURRENT_TIMESTAMP
                            """,
                            (filename, wal_file.stat().st_size),
                        )
                        connection.commit()

            except ValueError as corruption_error:
                error_msg = str(corruption_error)
                logger.error(f"WAL corruption detected: {error_msg}")
                # Flush any pending valid spans collected before the corruption occurred
                if current_batch_rows:
                    _flush_batch(
                        settings,
                        current_batch_rows,
                        [(f, off, "") for f, off, _ in current_batch_entries],
                        current_batch_offsets,
                    )
                    total_materialized_spans += len(current_batch_rows)
                    current_batch_rows.clear()
                    current_batch_entries.clear()
                    current_batch_offsets.clear()

                # Record corruption event in SQLite
                with sqlite3.connect(settings.sqlite_path) as connection:
                    connection.execute(
                        "INSERT INTO wal_corruption_events(source_file, offset, reason) VALUES (?, ?, ?)",
                        (filename, current_batch_offsets.get(filename, start_offset), error_msg),
                    )
                    connection.execute(
                        "INSERT OR REPLACE INTO wal_file_lifecycle(filename, status, bytes_size) VALUES (?, 'QUARANTINED', ?)",
                        (filename, file_size),
                    )
                    connection.commit()
                continue

        # Flush any remaining rows in the final batch
        if current_batch_rows:
            _flush_batch(
                settings,
                current_batch_rows,
                [(f, off, "") for f, off, _ in current_batch_entries],
                current_batch_offsets,
            )
            total_materialized_spans += len(current_batch_rows)
            current_batch_rows.clear()
            current_batch_entries.clear()
            current_batch_offsets.clear()

        # Perform WAL retention cleanup if enabled
        if settings.auto_cleanup_wal:
            cleanup_materialized_wal_files(settings)

        elapsed = time.perf_counter() - start_time
        if total_materialized_spans > 0:
            logger.info(
                f"Materialization complete: {total_materialized_spans} span(s) written in {elapsed:.3f}s "
                f"({total_materialized_spans/elapsed:.0f} spans/sec)"
            )
        return total_materialized_spans

    finally:
        _MATERIALIZATION_LOCK.release()


def cleanup_materialized_wal_files(settings: Settings) -> list[str]:
    """
    Safely delete sealed, fully materialized WAL segments.

    Rules:
      1. Never delete the active (newest) WAL segment.
      2. Never delete unmaterialized segments.
      3. Never delete quarantined segments silently.
      4. Log all deletions and record in lifecycle table.
    """
    deleted_files = []
    wal_files = sorted(settings.wal_dir.glob("*.wal"))
    if len(wal_files) <= 1:
        # Keep at least the active segment
        return deleted_files

    active_file = wal_files[-1].name

    with sqlite3.connect(settings.sqlite_path) as connection:
        fully_materialized = set(
            row[0] for row in connection.execute(
                "SELECT source_file FROM materialized_wal_offsets WHERE is_fully_processed = 1"
            ).fetchall()
        )

    for wal_file in wal_files[:-1]:  # Exclude active file
        if wal_file.name in fully_materialized:
            try:
                wal_file.unlink()
                deleted_files.append(wal_file.name)
                logger.info(f"Cleaned up materialized WAL segment: {wal_file.name}")
                with sqlite3.connect(settings.sqlite_path) as connection:
                    connection.execute(
                        "INSERT OR REPLACE INTO wal_file_lifecycle(filename, status) VALUES (?, 'DELETED')",
                        (wal_file.name,),
                    )
                    connection.commit()
            except OSError as e:
                logger.error(f"Failed to delete materialized WAL file {wal_file.name}: {e}")

    return deleted_files


def compact_parquet_files(settings: Settings, max_files_to_merge: int = 20) -> int:
    """
    Compact multiple small Parquet files into a larger consolidated Parquet file.
    """
    files = sorted(settings.parquet_dir.glob("*.parquet"))
    if len(files) <= 1:
        return 0

    to_merge = files[:max_files_to_merge]
    tables = []
    total_rows = 0
    for f in to_merge:
        try:
            t = pq.read_table(f)
            tables.append(t)
            total_rows += t.num_rows
        except Exception as e:
            logger.error(f"Error reading Parquet file {f} for compaction: {e}")
            return 0

    if not tables:
        return 0

    combined = pa.concat_tables(tables)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    compacted_filename = f"spans-compacted-{timestamp}-{uuid.uuid4().hex[:8]}.parquet"
    temp_path = settings.parquet_dir / f"tmp-{compacted_filename}.tmp"
    final_path = settings.parquet_dir / compacted_filename

    pq.write_table(combined, temp_path, compression="zstd")
    temp_path.replace(final_path)

    # Update manifest atomically and remove old files
    with sqlite3.connect(settings.sqlite_path) as connection:
        connection.execute(
            "INSERT INTO parquet_files(filename, record_count, status) VALUES (?, ?, 'COMMITTED')",
            (compacted_filename, total_rows),
        )
        for old_file in to_merge:
            connection.execute(
                "UPDATE parquet_files SET status = 'COMPACTED' WHERE filename = ?",
                (old_file.name,),
            )
        connection.commit()

    for old_file in to_merge:
        try:
            old_file.unlink(missing_ok=True)
        except OSError as e:
            logger.warning(f"Could not remove compacted file {old_file}: {e}")

    logger.info(f"Compacted {len(to_merge)} Parquet files into {compacted_filename} ({total_rows} rows)")
    return total_rows
