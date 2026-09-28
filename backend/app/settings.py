"""
Application Configuration

This module provides a dataclass-based configuration system that reads from
environment variables with sensible defaults.

All paths are resolved to absolute paths during initialization to avoid
confusion with relative paths when the working directory changes.

Environment variables:
  - LRCP_DATA_DIR: Base directory for all persistent data (default: ./data)
  - LRCP_WAL_DIR: WAL file directory (default: $DATA_DIR/wal)
  - LRCP_PARQUET_DIR: Parquet output directory (default: $DATA_DIR/parquet)
  - LRCP_SQLITE_PATH: SQLite database path (default: $DATA_DIR/control.db)
  - LRCP_MATERIALIZE_INTERVAL_SECONDS: Auto-materialize interval (default: 2.0, 0 = disabled)
  - LRCP_MATERIALIZE_BATCH_SIZE: Max spans per Parquet batch (default: 5000)
  - LRCP_MAX_WAL_RECORD_BYTES: Safety ceiling for single WAL record (default: 64MB)
  - LRCP_API_KEY: Optional API key for authenticating HTTP API requests
  - LRCP_REQUIRE_AUTH: Whether to enforce API key authentication (default: False)
  - LRCP_AUTO_CLEANUP_WAL: Whether to automatically clean up materialized WAL segments (default: True)

Usage:
    settings = Settings.from_environment()
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


@dataclass(frozen=True)
class Settings:
    """
    Application settings with all configuration parameters.
    """
    data_dir: Path
    wal_dir: Path
    parquet_dir: Path
    sqlite_path: Path
    materialize_interval_seconds: float = 0.0
    materialize_batch_size: int = 5000
    max_wal_record_bytes: int = 64 * 1024 * 1024
    api_key: str | None = None
    require_auth: bool = False
    auto_cleanup_wal: bool = False

    @classmethod
    def from_environment(cls) -> "Settings":
        """
        Load settings from environment variables.

        Returns:
            Settings instance with all paths resolved to absolute paths.

        Raises:
            ValueError: If numeric settings are invalid.
        """
        data_dir = Path(os.environ.get("LRCP_DATA_DIR", "./data")).resolve()
        interval = float(os.environ.get("LRCP_MATERIALIZE_INTERVAL_SECONDS", "2.0"))
        batch_size = int(os.environ.get("LRCP_MATERIALIZE_BATCH_SIZE", "5000"))
        max_record_bytes = int(os.environ.get("LRCP_MAX_WAL_RECORD_BYTES", str(64 * 1024 * 1024)))
        api_key = os.environ.get("LRCP_API_KEY")
        require_auth = os.environ.get("LRCP_REQUIRE_AUTH", "false").lower() in ("true", "1", "yes")
        auto_cleanup = os.environ.get("LRCP_AUTO_CLEANUP_WAL", "false").lower() in ("true", "1", "yes")

        if interval < 0:
            raise ValueError("LRCP_MATERIALIZE_INTERVAL_SECONDS must be >= 0")
        if batch_size <= 0:
            raise ValueError("LRCP_MATERIALIZE_BATCH_SIZE must be > 0")
        if max_record_bytes <= 0:
            raise ValueError("LRCP_MAX_WAL_RECORD_BYTES must be > 0")

        return cls(
            data_dir=data_dir,
            wal_dir=Path(os.environ.get("LRCP_WAL_DIR", data_dir / "wal")).resolve(),
            parquet_dir=Path(os.environ.get("LRCP_PARQUET_DIR", data_dir / "parquet")).resolve(),
            sqlite_path=Path(os.environ.get("LRCP_SQLITE_PATH", data_dir / "control.db")).resolve(),
            materialize_interval_seconds=interval,
            materialize_batch_size=batch_size,
            max_wal_record_bytes=max_record_bytes,
            api_key=api_key,
            require_auth=require_auth,
            auto_cleanup_wal=auto_cleanup,
        )
