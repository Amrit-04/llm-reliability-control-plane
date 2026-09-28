"""
SQLite Control Store Initialization

This module manages the SQLite database schema for the LRCP control plane.
The database stores:
  - Projects: Tenant/organization containers for traces with API keys
  - Materialized WAL offsets: Track high-water mark for fast WAL scanning
  - Materialized WAL records: Provenance tracking
  - Parquet files: Committed Parquet files for orphan detection and compaction
  - WAL corruption events: Diagnostic tracking of corrupted WAL segments
  - WAL lifecycle: State tracking for WAL segments (ACTIVE, SEALED, MATERIALIZED, DELETED, QUARANTINED)

Schema design:
  - Simple normalized tables with indices
  - Timestamps use ISO 8601 text format (SQLite native)
  - Primary keys prevent duplicate inserts
  - All DDL is idempotent (CREATE IF NOT EXISTS)
"""
from __future__ import annotations

import logging
import sqlite3
from .settings import Settings

logger = logging.getLogger(__name__)


def initialize_control_store(settings: Settings) -> None:
    """
    Create SQLite schema if it doesn't exist and add required indexes.

    This function is idempotent: repeated calls are safe.

    Args:
        settings: Application settings with sqlite_path and data_dir.

    Raises:
        OSError: If directory creation fails.
        sqlite3.Error: If schema creation fails.
    """
    try:
        settings.data_dir.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Ensured data directory exists: {settings.data_dir}")
    except OSError as error:
        logger.error(f"Failed to create data directory {settings.data_dir}: {error}")
        raise

    try:
        with sqlite3.connect(settings.sqlite_path) as connection:
            # Enable WAL mode for SQLite itself to allow concurrent readers & writers
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA busy_timeout=5000")

            connection.execute("""
                CREATE TABLE IF NOT EXISTS projects (
                  id TEXT PRIMARY KEY,
                  name TEXT NOT NULL,
                  api_key TEXT,
                  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)

            connection.execute("""
                CREATE TABLE IF NOT EXISTS materialized_wal_offsets (
                  source_file TEXT PRIMARY KEY,
                  max_offset INTEGER NOT NULL,
                  is_fully_processed BOOLEAN NOT NULL DEFAULT 0,
                  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)

            connection.execute("""
                CREATE TABLE IF NOT EXISTS materialized_wal_records (
                  source_file TEXT NOT NULL,
                  source_offset INTEGER NOT NULL,
                  parquet_file TEXT NOT NULL,
                  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  PRIMARY KEY (source_file, source_offset)
                )
            """)

            connection.execute("""
                CREATE TABLE IF NOT EXISTS parquet_files (
                  filename TEXT PRIMARY KEY,
                  record_count INTEGER NOT NULL DEFAULT 0,
                  status TEXT NOT NULL DEFAULT 'COMMITTED',
                  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)

            connection.execute("""
                CREATE TABLE IF NOT EXISTS wal_corruption_events (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  source_file TEXT NOT NULL,
                  offset INTEGER,
                  reason TEXT NOT NULL,
                  detected_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)

            connection.execute("""
                CREATE TABLE IF NOT EXISTS wal_file_lifecycle (
                  filename TEXT PRIMARY KEY,
                  status TEXT NOT NULL DEFAULT 'ACTIVE',
                  bytes_size INTEGER NOT NULL DEFAULT 0,
                  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Add indices for fast lookups
            connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_wal_records_file ON materialized_wal_records (source_file)
            """)
            connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_wal_corruption_file ON wal_corruption_events (source_file)
            """)

            connection.commit()
        logger.info(f"Control store initialized at {settings.sqlite_path}")
    except sqlite3.Error as error:
        logger.error(f"Failed to initialize control store: {error}")
        raise
