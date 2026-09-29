"""
SQLite Control Store Initialization

This module manages the SQLite database schema for the LRCP control plane.
The database stores:
  - Projects: Tenant/organization containers for traces with hashed API keys
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

import hashlib
import hmac
import logging
from pathlib import Path
import secrets
import sqlite3
from .settings import Settings

logger = logging.getLogger(__name__)


def hash_api_key(raw_key: str) -> str:
    """Compute a deterministic SHA-256 hash of an API key for storage and lookup."""
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def generate_api_key() -> str:
    """Generate a cryptographically secure random API key."""
    return f"lrcp_{secrets.token_urlsafe(32)}"


def verify_project_credential(settings: Settings, token: str) -> str | None:
    """
    Verify if a token matches any project's hashed API key.

    Uses constant-time comparison to prevent timing attacks.

    Returns:
        The matched project_id, or None if invalid.
    """
    if not token or not token.strip():
        return None

    candidate_hash = hash_api_key(token.strip())
    try:
        with sqlite3.connect(settings.sqlite_path) as connection:
            rows = connection.execute(
                "SELECT id, api_key_hash FROM projects WHERE api_key_hash IS NOT NULL"
            ).fetchall()
            for project_id, stored_hash in rows:
                if stored_hash and hmac.compare_digest(candidate_hash, stored_hash):
                    return project_id
    except sqlite3.Error as error:
        logger.error(f"Failed to verify project credential: {error}")
    return None


def get_committed_parquet_files(settings: Settings) -> list[Path]:
    """
    Return physical paths for all Parquet files currently marked as 'COMMITTED'
    in the SQLite manifest and physically present on disk.

    This ensures queries never scan uncommitted temporary files or compacted-out files.
    """
    try:
        with sqlite3.connect(settings.sqlite_path) as connection:
            cursor = connection.execute(
                "SELECT filename FROM parquet_files WHERE status = 'COMMITTED' ORDER BY filename ASC"
            )
            filenames = [row[0] for row in cursor.fetchall()]

        valid_paths: list[Path] = []
        for filename in filenames:
            file_path = settings.parquet_dir / filename
            if file_path.exists():
                valid_paths.append(file_path)
        return valid_paths
    except Exception as error:
        logger.error(f"Failed to query committed Parquet files: {error}")
        return []


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
                  api_key_hash TEXT,
                  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Migration: Ensure api_key_hash column exists if table was created with old schema
            try:
                table_info = connection.execute("PRAGMA table_info(projects)").fetchall()
                col_names = [col[1] for col in table_info]
                if "api_key_hash" not in col_names and "api_key" in col_names:
                    connection.execute("ALTER TABLE projects ADD COLUMN api_key_hash TEXT")
            except Exception:
                pass

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
