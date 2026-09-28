# Security Policy

## Current Scope & Capabilities

The LLM Reliability Control Plane implements strict defensive security and validation controls:

1. **Ingestion Authentication**:
   - The C++ telemetry gateway supports `--auth-token <TOKEN>` to require `Authorization: Bearer <TOKEN>` or `X-API-Key: <TOKEN>` on all incoming OTLP export requests.
   - The FastAPI backend supports API key authentication via `LRCP_API_KEY` and `LRCP_REQUIRE_AUTH=true`.

2. **Defensive Input Validation & Hardening**:
   - HTTP body size limits (`--max-body-bytes`, default 4 MiB) enforced at Beast parser level before memory allocation.
   - Protobuf parsing validation rejecting malformed or truncated payloads.
   - IEEE CRC-32 checksum framing on every WAL record to detect disk corruption or tampering.
   - Maximum WAL record size enforcement (`LRCP_MAX_WAL_RECORD_BYTES`) preventing memory exhaustion attacks.
   - DuckDB `TRY_CAST` defensive SQL queries preventing crashes on malformed GenAI attribute injection.

3. **Multi-Tenant / Project Isolation**:
   - Project registry in SQLite with unique API keys and project-scoped trace query filters (`?project_id=<id>`).

4. **Transport & Network Hardening**:
   - The gateway and backend are designed for local-first execution. When exposing across networks or in production clusters, place them behind TLS-terminating reverse proxies (e.g. Envoy, Nginx, or Cloudflare).

## Reporting a Vulnerability

If you discover a security issue or vulnerability in LRCP:
1. Do not disclose the vulnerability in a public GitHub issue.
2. Contact the maintainer privately via security report.
3. Provide full reproduction steps, affected commit hash, and assessment of impact.
