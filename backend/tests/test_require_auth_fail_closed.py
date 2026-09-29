"""
Tests for Fail-Closed Authentication Configuration (P0 #5)
"""
import os
from pathlib import Path
import pytest

from app.settings import Settings


def test_require_auth_fails_closed_when_api_key_missing(monkeypatch, tmp_path: Path):
    """
    LRCP_REQUIRE_AUTH=true without LRCP_API_KEY must raise ValueError at startup.
    """
    monkeypatch.setenv("LRCP_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LRCP_REQUIRE_AUTH", "true")
    monkeypatch.delenv("LRCP_API_KEY", raising=False)

    with pytest.raises(ValueError, match="LRCP_REQUIRE_AUTH is true but LRCP_API_KEY is not configured or empty"):
        Settings.from_environment()


def test_require_auth_fails_closed_when_api_key_empty(monkeypatch, tmp_path: Path):
    """
    LRCP_REQUIRE_AUTH=true with empty or whitespace-only LRCP_API_KEY must raise ValueError.
    """
    monkeypatch.setenv("LRCP_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LRCP_REQUIRE_AUTH", "true")
    monkeypatch.setenv("LRCP_API_KEY", "   ")

    with pytest.raises(ValueError, match="LRCP_REQUIRE_AUTH is true but LRCP_API_KEY is not configured or empty"):
        Settings.from_environment()


def test_require_auth_succeeds_when_api_key_provided(monkeypatch, tmp_path: Path):
    """
    LRCP_REQUIRE_AUTH=true with valid LRCP_API_KEY loads successfully.
    """
    monkeypatch.setenv("LRCP_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LRCP_REQUIRE_AUTH", "true")
    monkeypatch.setenv("LRCP_API_KEY", "secure-admin-secret-key")

    settings = Settings.from_environment()
    assert settings.require_auth is True
    assert settings.api_key == "secure-admin-secret-key"


def test_auth_disabled_succeeds_without_key(monkeypatch, tmp_path: Path):
    """
    LRCP_REQUIRE_AUTH=false allows missing LRCP_API_KEY.
    """
    monkeypatch.setenv("LRCP_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LRCP_REQUIRE_AUTH", "false")
    monkeypatch.delenv("LRCP_API_KEY", raising=False)

    settings = Settings.from_environment()
    assert settings.require_auth is False
    assert settings.api_key is None
