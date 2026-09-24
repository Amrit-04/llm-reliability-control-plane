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

Usage:
    settings = Settings.from_environment()
    # All paths are now absolute and ready to use
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


@dataclass(frozen=True)
class Settings:
    """
    Application settings with all configuration parameters.

    Attributes:
        data_dir: Root directory for all persistent data.
        wal_dir: Directory containing WAL files from the C++ gateway.
        parquet_dir: Directory for materialized Parquet files.
        sqlite_path: Path to the SQLite control database.
        materialize_interval_seconds: Interval for background materialization.
                                       0 disables automatic materialization.
    """
    data_dir: Path
    wal_dir: Path
    parquet_dir: Path
    sqlite_path: Path
    materialize_interval_seconds: float = 0.0

    @classmethod
    def from_environment(cls) -> "Settings":
        """
        Load settings from environment variables.

        Returns:
            Settings instance with all paths resolved to absolute paths.

        Raises:
            ValueError: If materialize_interval_seconds is negative.
        """
        data_dir = Path(os.environ.get("LRCP_DATA_DIR", "./data")).resolve()
        interval = float(os.environ.get("LRCP_MATERIALIZE_INTERVAL_SECONDS", "2.0"))

        if interval < 0:
            raise ValueError("LRCP_MATERIALIZE_INTERVAL_SECONDS must be >= 0")

        return cls(
            data_dir=data_dir,
            wal_dir=Path(os.environ.get("LRCP_WAL_DIR", data_dir / "wal")).resolve(),
            parquet_dir=Path(os.environ.get("LRCP_PARQUET_DIR", data_dir / "parquet")).resolve(),
            sqlite_path=Path(os.environ.get("LRCP_SQLITE_PATH", data_dir / "control.db")).resolve(),
            materialize_interval_seconds=interval,
        )
