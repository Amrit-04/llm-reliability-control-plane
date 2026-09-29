# Security Policy

## Current Scope & Capabilities

The LLM Reliability Control Plane implements strict defensive security and validation controls:

1. **Ingestion & Backend Authentication**:
   - The C++ telemetry gateway supports `--auth-token <TOKEN>` to require `Authorization: Bearer <TOKEN>` or `X-API-Key: <TOKEN>` on all incoming OTLP export requests.
   - The FastAPI backend supports fail-closed API key authentication via `LRCP_API_KEY` and `LRCP_REQUIRE_AUTH=true`. If authentication is required but no key is configured, the backend fails closed immediately on startup.
   - Project API keys are cryptographically generated (`secrets.token_urlsafe(32)`), stored exclusively as SHA-256 hashes, and verified using constant-time `hmac.compare_digest` to prevent timing attacks. Raw secrets are returned only once upon creation (HTTP 201) and are excluded from all subsequent queries.
   - Next.js frontend Server Components securely forward credentials (`X-API-Key`) server-to-server, preventing API keys from being leaked into client-side JavaScript bundles.

2. **Defensive Input Validation & Hardening**:
   - HTTP body size limits (`--max-body-bytes`, default 4 MiB) enforced at Beast parser level before memory allocation.
   - Protobuf parsing validation rejecting malformed or truncated payloads.
   - IEEE CRC-32 checksum framing on every WAL record to detect disk corruption or tampering.
   - Defensive WAL error trapping with exact byte offset diagnostic recording in SQLite.
   - Maximum WAL record size enforcement (`LRCP_MAX_WAL_RECORD_BYTES`) preventing memory exhaustion attacks.
   - Complete parameterized SQL bindings across all DuckDB analytics and SQLite control queries, preventing SQL injection vulnerabilities.
   - DuckDB `TRY_CAST` defensive SQL queries preventing crashes on malformed GenAI attribute injection.

3. **Multi-Tenant / Project Isolation**:
   - Authenticated project requests are strictly scoped to their respective `project_id`.
   - Cross-project trace inspection or data leakage is rejected at the API boundary.

4. **Storage Lifecycle & Concurrency Locking**:
   - Cross-process SQLite exclusive locking on `$DATA_DIR/storage_lifecycle.lock.db` combined with in-process threading locks prevents concurrent race conditions across ingestion, materialization, compaction, and cleanup.

4. **Transport & Network Hardening**:
   - The gateway and backend are designed for local-first execution. When exposing across networks or in production clusters, place them behind TLS-terminating reverse proxies (e.g. Envoy, Nginx, or Cloudflare).

## Reporting a Vulnerability

If you discover a security issue or vulnerability in LRCP:
1. Do not disclose the vulnerability in a public GitHub issue.
2. Contact the maintainer privately via security report.
3. Provide full reproduction steps, affected commit hash, and assessment of impact.
