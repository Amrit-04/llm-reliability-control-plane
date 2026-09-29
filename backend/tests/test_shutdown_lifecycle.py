"""
Tests for Application Worker Shutdown and In-Flight Protection (P1 #9)
"""
import asyncio
from pathlib import Path
import time
import pytest
from fastapi.testclient import TestClient

from app.control import initialize_control_store
from app.main import create_app
from app.settings import Settings


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        tmp_path,
        tmp_path / "wal",
        tmp_path / "parquet",
        tmp_path / "control.db",
        materialize_interval_seconds=0.05,
    )


def test_clean_shutdown_cancels_background_worker_without_errors(tmp_path: Path):
    """
    Verify application lifespan starts background worker and cleanly shuts down
    without hanging or leaking unhandled exceptions.
    """
    settings = _settings(tmp_path)
    settings.wal_dir.mkdir(parents=True, exist_ok=True)
    initialize_control_store(settings)

    app = create_app(settings)

    # Enter lifespan context (startup)
    with TestClient(app) as client:
        # App is running, worker is active
        res = client.get("/healthz")
        assert res.status_code == 200
        time.sleep(0.1)
    # Exit lifespan context (shutdown) - must complete cleanly
