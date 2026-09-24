from __future__ import annotations

import subprocess
import time
import urllib.request
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

from app.main import create_app
from app.settings import Settings

_REPO_ROOT = Path(__file__).resolve().parents[2]
_GATEWAY_CANDIDATES = [
    _REPO_ROOT / "build" / "Debug" / "lrcp-gateway.exe",
    _REPO_ROOT / "build" / "Release" / "lrcp-gateway.exe",
    _REPO_ROOT / "build" / "lrcp-gateway",
    _REPO_ROOT / "build" / "lrcp-gateway.exe",
]


def _gateway_binary() -> Path | None:
    for path in _GATEWAY_CANDIDATES:
        if path.is_file():
            return path
    return None


@pytest.fixture
def gateway_binary() -> Path:
    binary = _gateway_binary()
    if binary is None:
        pytest.skip("C++ gateway binary is not built in this checkout")
    return binary


def test_gateway_wal_materializes_into_query_api(tmp_path: Path, gateway_binary: Path):
    wal_dir = tmp_path / "wal"
    wal_dir.mkdir()
    process = subprocess.Popen(
        [
            str(gateway_binary),
            "--address",
            "127.0.0.1",
            "--port",
            "0",
        ],
        cwd=tmp_path,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    # Port 0 is not currently advertised in stdout as a parsed value besides
    # "listening on host:port". Read that line with a timeout.
    try:
        assert process.stdout is not None
        line = ""
        deadline = time.time() + 5
        while time.time() < deadline:
            line = process.stdout.readline()
            if line.startswith("listening on"):
                break
        else:
            pytest.fail(f"gateway did not announce a listen address: {line!r}")
        port = int(line.rsplit(":", 1)[1].strip())
        payload_request = ExportTraceServiceRequest()
        resource_spans = payload_request.resource_spans.add()
        resource_spans.resource.attributes.add(key="service.name").value.string_value = "e2e"
        span = resource_spans.scope_spans.add().spans.add()
        span.trace_id = bytes.fromhex("11" * 16)
        span.span_id = bytes.fromhex("22" * 8)
        span.name = "llm.generate"
        span.start_time_unix_nano = 100
        span.end_time_unix_nano = 250
        payload = payload_request.SerializeToString()
        http_request = urllib.request.Request(
            f"http://127.0.0.1:{port}/v1/traces",
            data=payload,
            headers={"Content-Type": "application/x-protobuf"},
            method="POST",
        )
        with urllib.request.urlopen(http_request, timeout=2) as response:
            assert response.status == 200
    finally:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()

    settings = Settings(tmp_path, wal_dir, tmp_path / "parquet", tmp_path / "control.db")
    # The gateway default WAL directory is data/wal relative to cwd unless --wal-dir is set.
    # This test used --port 0 only, so WAL is at cwd/data/wal. Re-point after checking.
    default_wal = tmp_path / "data" / "wal"
    if default_wal.exists():
        settings = Settings(tmp_path, default_wal, tmp_path / "parquet", tmp_path / "control.db")
    else:
        pytest.fail(f"expected WAL directory at {default_wal}")

    with TestClient(create_app(settings)) as client:
        assert client.post("/api/v1/materialize").json() == {"materialized_spans": 1}
        traces = client.get("/api/v1/traces").json()
        assert traces[0]["trace_id"] == "11" * 16
        assert traces[0]["service_name"] == "e2e"
        spans = client.get(f"/api/v1/traces/{traces[0]['trace_id']}").json()
        assert spans[0]["name"] == "llm.generate"
        assert spans[0]["duration_ns"] == 150
