"""
SQLite Control Store Initialization

This module manages the SQLite database schema for the LRCP control plane.
The database stores:
  - Projects: Tenant/organization containers for traces
  - Materialized WAL records: Deduplication manifest for WAL → Parquet conversion

Schema design:
  - Simple normalized tables, no foreign keys yet
  - Timestamps use ISO 8601 text format (SQLite native)
  - Primary keys prevent duplicate inserts
  - All DDL is idempotent (CREATE IF NOT EXISTS)

Threading:
  - This module is not thread-safe by itself
  - SQLite connections should not be shared across threads
  - FastAPI lifespan calls this once during startup
"""
from __future__ import annotations

import logging
import sqlite3
from .settings import Settings

logger = logging.getLogger(__name__)


def initialize_control_store(settings: Settings) -> None:
    """
    Create SQLite schema if it doesn't exist.

    This function is idempotent: repeated calls are safe. It creates:
      1. The data directory (if missing)
      2. Two tables: projects and materialized_wal_records

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
            connection.execute("""
                CREATE TABLE IF NOT EXISTS projects (
                  id TEXT PRIMARY KEY,
                  name TEXT NOT NULL,
                  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
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
            connection.commit()
        logger.info(f"Control store initialized at {settings.sqlite_path}")
    except sqlite3.Error as error:
        logger.error(f"Failed to initialize control store: {error}")
        raise
