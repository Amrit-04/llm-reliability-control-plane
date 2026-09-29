"""
Tests for Project Authentication, Tenant Isolation, and Key Management (P0 #4, P1 #7)
"""
from pathlib import Path
import struct
import binascii
import pytest
from fastapi.testclient import TestClient
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

from app.control import initialize_control_store, hash_api_key, verify_project_credential
from app.main import create_app
from app.materializer import materialize
from app.settings import Settings


def _wal_record(payload: bytes) -> bytes:
    return struct.pack("<II", len(payload), binascii.crc32(payload) & 0xFFFFFFFF) + payload


def _make_span(project_id: str, trace_hex: str, span_name: str) -> ExportTraceServiceRequest:
    request = ExportTraceServiceRequest()
    rs = request.resource_spans.add()
    rs.resource.attributes.add(key="service.name").value.string_value = "app"
    rs.resource.attributes.add(key="project.id").value.string_value = project_id
    s = rs.scope_spans.add().spans.add()
    s.trace_id = bytes.fromhex(trace_hex)
    s.span_id = bytes.fromhex("01" * 8)
    s.name = span_name
    s.start_time_unix_nano = 1000
    s.end_time_unix_nano = 2000
    return request


def _settings(tmp_path: Path, require_auth: bool = True) -> Settings:
    return Settings(
        tmp_path,
        tmp_path / "wal",
        tmp_path / "parquet",
        tmp_path / "control.db",
        require_auth=require_auth,
        api_key="global-admin-key",
    )


def test_project_key_hashing_and_verification(tmp_path: Path):
    """
    Ensure project API keys are hashed with SHA-256 and verified with constant-time comparison.
    """
    settings = _settings(tmp_path)
    initialize_control_store(settings)

    with TestClient(create_app(settings)) as client:
        # Admin creates project
        create_res = client.post(
            "/api/v1/projects",
            json={"id": "tenant-alpha", "name": "Alpha Corp"},
            headers={"X-API-Key": "global-admin-key"},
        )
        assert create_res.status_code == 201
        key_data = create_res.json()
        assert "api_key" in key_data
        raw_key = key_data["api_key"]
        assert raw_key.startswith("lrcp_")

        # Verify credential helper directly
        matched = verify_project_credential(settings, raw_key)
        assert matched == "tenant-alpha"

        # Invalid key returns None
        assert verify_project_credential(settings, "invalid-key") is None
        assert verify_project_credential(settings, "") is None


def test_project_tenant_isolation_in_queries(tmp_path: Path):
    """
    Test scenario:
      Tenant Alpha has Trace AA
      Tenant Beta has Trace BB
    Assert:
      - Tenant Alpha's API key can only query Trace AA, not Trace BB
      - Tenant Alpha querying with ?project_id=tenant-beta returns 403 Forbidden
      - Tenant Beta's API key can only query Trace BB
      - Admin key can query all or filter by project
    """
    settings = _settings(tmp_path)
    settings.wal_dir.mkdir(parents=True, exist_ok=True)
    initialize_control_store(settings)

    # Ingest spans for Alpha and Beta
    wal_bytes = bytearray()
    wal_bytes.extend(_wal_record(_make_span("tenant-alpha", "aa" * 16, "alpha-span").SerializeToString()))
    wal_bytes.extend(_wal_record(_make_span("tenant-beta", "bb" * 16, "beta-span").SerializeToString()))
    (settings.wal_dir / "wal-0.wal").write_bytes(wal_bytes)
    materialize(settings)

    with TestClient(create_app(settings)) as client:
        # Create both projects and get their keys
        admin_headers = {"X-API-Key": "global-admin-key"}
        alpha_key = client.post("/api/v1/projects", json={"id": "tenant-alpha", "name": "Alpha"}, headers=admin_headers).json()["api_key"]
        beta_key = client.post("/api/v1/projects", json={"id": "tenant-beta", "name": "Beta"}, headers=admin_headers).json()["api_key"]

        alpha_headers = {"X-API-Key": alpha_key}
        beta_headers = {"X-API-Key": beta_key}

        # 1. Tenant Alpha lists traces -> sees only Trace AA
        alpha_traces = client.get("/api/v1/traces", headers=alpha_headers).json()
        assert len(alpha_traces) == 1
        assert alpha_traces[0]["trace_id"] == "aa" * 16

        # 2. Tenant Alpha gets Trace AA -> 200 OK
        assert client.get(f"/api/v1/traces/{'aa' * 16}", headers=alpha_headers).status_code == 200

        # 3. Tenant Alpha attempts to get Trace BB -> 404 Not Found (scoped to tenant)
        assert client.get(f"/api/v1/traces/{'bb' * 16}", headers=alpha_headers).status_code == 404

        # 4. Tenant Alpha attempts cross-project parameter spoofing -> 403 Forbidden
        assert client.get("/api/v1/traces?project_id=tenant-beta", headers=alpha_headers).status_code == 403
        assert client.get(f"/api/v1/traces/{'aa' * 16}?project_id=tenant-beta", headers=alpha_headers).status_code == 403
        assert client.get("/api/v1/analytics/overview?project_id=tenant-beta", headers=alpha_headers).status_code == 403
        assert client.get("/api/v1/analytics/models?project_id=tenant-beta", headers=alpha_headers).status_code == 403

        # 5. Tenant Beta lists traces -> sees only Trace BB
        beta_traces = client.get("/api/v1/traces", headers=beta_headers).json()
        assert len(beta_traces) == 1
        assert beta_traces[0]["trace_id"] == "bb" * 16

        # 6. Admin lists traces without filter -> sees both AA and BB
        all_traces = client.get("/api/v1/traces", headers=admin_headers).json()
        assert len(all_traces) == 2

        # 7. Admin lists traces with filter -> sees requested project
        admin_alpha = client.get("/api/v1/traces?project_id=tenant-alpha", headers=admin_headers).json()
        assert len(admin_alpha) == 1
        assert admin_alpha[0]["trace_id"] == "aa" * 16


def test_project_list_does_not_leak_secrets(tmp_path: Path):
    """
    Ensure list_projects endpoint never includes raw api_key or api_key_hash in its response.
    """
    settings = _settings(tmp_path)
    initialize_control_store(settings)

    with TestClient(create_app(settings)) as client:
        admin_headers = {"X-API-Key": "global-admin-key"}
        client.post(
            "/api/v1/projects",
            json={"id": "tenant-gamma", "name": "Gamma Corp"},
            headers=admin_headers,
        )

        projects = client.get("/api/v1/projects", headers=admin_headers).json()
        assert len(projects) == 1
        assert "id" in projects[0]
        assert "name" in projects[0]
        assert "api_key" not in projects[0]
        assert "api_key_hash" not in projects[0]
