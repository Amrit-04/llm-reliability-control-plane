"""
Tests for API Security and Project Isolation (Phases 17 & 18)
"""
from pathlib import Path
import pytest
from fastapi.testclient import TestClient

from app.control import initialize_control_store
from app.main import create_app
from app.settings import Settings


def _settings(tmp_path: Path, require_auth: bool = False, api_key: str | None = None) -> Settings:
    return Settings(
        tmp_path,
        tmp_path / "wal",
        tmp_path / "parquet",
        tmp_path / "control.db",
        require_auth=require_auth,
        api_key=api_key,
    )


def test_api_allows_requests_when_auth_disabled(tmp_path: Path):
    settings = _settings(tmp_path, require_auth=False)
    with TestClient(create_app(settings)) as client:
        # Public endpoints skip auth completely
        assert client.get("/healthz").status_code == 200
        # Data endpoints succeed without headers
        assert client.get("/api/v1/system/status").status_code == 200


def test_api_rejects_requests_missing_auth_header(tmp_path: Path):
    settings = _settings(tmp_path, require_auth=True, api_key="secret-token")
    with TestClient(create_app(settings)) as client:
        assert client.get("/healthz").status_code == 200
        assert client.get("/api/v1/system/status").status_code == 401
        assert client.post("/api/v1/materialize").status_code == 401


def test_api_accepts_valid_api_key_header(tmp_path: Path):
    settings = _settings(tmp_path, require_auth=True, api_key="secret-token")
    with TestClient(create_app(settings)) as client:
        # Test X-API-Key header
        resp = client.get("/api/v1/system/status", headers={"X-API-Key": "secret-token"})
        assert resp.status_code == 200

        # Test Authorization Bearer header
        resp2 = client.get("/api/v1/system/status", headers={"Authorization": "Bearer secret-token"})
        assert resp2.status_code == 200


def test_api_rejects_invalid_api_key(tmp_path: Path):
    settings = _settings(tmp_path, require_auth=True, api_key="secret-token")
    with TestClient(create_app(settings)) as client:
        resp = client.get("/api/v1/system/status", headers={"X-API-Key": "wrong-token"})
        assert resp.status_code == 401


def test_project_crud_and_auth(tmp_path: Path):
    settings = _settings(tmp_path, require_auth=True, api_key="admin-token")
    with TestClient(create_app(settings)) as client:
        headers = {"X-API-Key": "admin-token"}

        # Create project
        resp = client.post(
            "/api/v1/projects",
            json={"id": "tenant-auth", "name": "Tenant Auth DB", "api_key": "tenant-secret"},
            headers=headers
        )
        assert resp.status_code == 201

        # List project
        resp = client.get("/api/v1/projects", headers=headers)
        assert resp.status_code == 200
        assert len(resp.json()) == 1
        assert resp.json()[0]["id"] == "tenant-auth"
