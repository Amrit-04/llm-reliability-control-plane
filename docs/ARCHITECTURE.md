# Architecture Deep Dive: LLM Reliability Control Plane

**Last updated:** 2026-09-24

This document explains every layer of the system — what each component does,
how to read the code, and how the pieces connect end-to-end.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Data Flow — End to End](#2-data-flow--end-to-end)
3. [C++ Telemetry Gateway](#3-c-telemetry-gateway)
4. [Write-Ahead Log (WAL)](#4-write-ahead-log-wal)
5. [Python Backend (FastAPI)](#5-python-backend-fastapi)
6. [Materializer — WAL to Parquet](#6-materializer--wal-to-parquet)
7. [Query Layer — DuckDB over Parquet](#7-query-layer--duckdb-over-parquet)
8. [SQLite Control Store](#8-sqlite-control-store)
9. [Next.js Trace Explorer](#9-nextjs-trace-explorer)
10. [Deployment](#10-deployment)
11. [Testing Strategy](#11-testing-strategy)
12. [Configuration Reference](#12-configuration-reference)
13. [Key Design Decisions](#13-key-design-decisions)
14. [Extending the System](#14-extending-the-system)

---

## 1. System Overview

The LLM Reliability Control Plane (LRCP) is a **local-first observability
platform** designed for developers building LLM applications, RAG pipelines,
and AI agents. It collects, stores, and queries OpenTelemetry traces without
any cloud dependency.

### Philosophy

- **Local-first**: runs on an ordinary developer machine. No cloud account,
  no external database, no internet required after the initial build.
- **Durable before acknowledgement**: the gateway confirms an OTLP request
  only after the data is CRC-checked and fsynced to the WAL.
- **Composable storage tiers**: WAL → Parquet → DuckDB today,
  with a clear path to NATS → ClickHouse for production scale.
- **No magic**: every component is explicit. No ORM, no framework auto-wiring,
  no hidden caches. You can reason about the data path by reading the code.

### Component Map

```text
┌──────────────────────┐     ┌────────────────────────┐     ┌──────────────────┐
│  Your LLM App        │     │  C++ Gateway            │     │  WAL (disk)      │
│  (OTel SDK)          │────▶│  POST /v1/traces        │────▶│  CRC-32 framed   │
│                      │     │  protobuf decode         │     │  fsynced records │
└──────────────────────┘     │  size/content validation │     └────────┬─────────┘
                             └────────────────────────┘              │
                                                                     ▼
┌──────────────────────┐     ┌────────────────────────┐     ┌──────────────────┐
│  Next.js UI          │     │  FastAPI Backend        │     │  Materializer    │
│  Trace explorer      │◀────│  DuckDB queries         │◀────│  WAL → Parquet   │
│  localhost:3000      │     │  localhost:8000          │     │  (background)    │
└──────────────────────┘     └────────────────────────┘     └──────────────────┘
                                       │
                                       ▼
                             ┌────────────────────────┐
                             │  SQLite                 │
                             │  Materialization manifest│
                             │  Project registry       │
                             └────────────────────────┘
```

---

## 2. Data Flow — End to End

Here is the journey of a single span from your application to the UI:

### Step 1: Instrumentation
Your Python app uses the OpenTelemetry SDK to create spans. The
`OTLPSpanExporter` serializes them as protobuf and sends an HTTP POST
to `http://127.0.0.1:4318/v1/traces`.

**File:** `examples/basic/python/app.py`

### Step 2: Gateway receives the request
The C++ HTTP server (Boost.Beast) accepts the connection. It validates:
- HTTP method is POST
- Path is `/v1/traces`
- Content-Type is `application/x-protobuf`
- Body size ≤ `max_body_bytes` (default 4 MiB)
- Body is valid `ExportTraceServiceRequest` protobuf

**File:** `cpp/gateway/src/http_server.cpp:120-174`

### Step 3: WAL append
The raw protobuf bytes are written to the WAL with an 8-byte header:
`[4 bytes: payload length (LE)] [4 bytes: CRC-32 (LE)] [N bytes: payload]`.
The write is fsynced before the HTTP 200 is returned.

**File:** `cpp/gateway/src/wal_writer.cpp:98-128`

### Step 4: WAL rotation
When the current WAL segment exceeds `max_wal_file_bytes` (default 64 MiB),
a new segment is opened with a unique name: `wal-<timestamp_ns>-<seq>.wal`.
The old segment is closed and becomes immutable.

### Step 5: Materialization
The FastAPI backend's background task (or a manual `POST /api/v1/materialize`)
reads every `.wal` file in the WAL directory, decodes each protobuf record,
extracts normalized span fields, and writes a Parquet file with Zstd
compression. The SQLite manifest tracks which `(file, offset)` pairs have
been materialized to prevent duplicate processing.

**File:** `backend/app/materializer.py:84-115`

### Step 6: Query
The `GET /api/v1/traces` endpoint uses DuckDB's `read_parquet()` to scan all
Parquet files, grouping by `trace_id` with aggregate metrics. Individual trace
details come from `GET /api/v1/traces/{trace_id}`.

**File:** `backend/app/main.py:70-101`

### Step 7: UI
The Next.js app (server components) fetches from the FastAPI backend and
renders a dark-themed trace table and span tree view.

**File:** `frontend/app/page.tsx` and `frontend/app/traces/[traceId]/page.tsx`

---

## 3. C++ Telemetry Gateway

### Why C++?

The gateway is the hot path — every span touches it. C++ provides:
- Predictable memory layout and zero-copy parsing.
- Direct control over `fsync` timing and file I/O.
- No GC pauses on the ingestion path.

### Architecture

The gateway uses **Boost.Beast** (an HTTP library built on Boost.Asio) with
a single `io_context`. It is single-threaded but async — multiple connections
are served concurrently via coroutine-style callbacks.

#### Key classes

| Class | File | Purpose |
|-------|------|---------|
| `HttpServer` | `http_server.h` | Public API. Owns a `SharedState` via `shared_ptr`. |
| `SharedState` | `http_server.cpp:44` | Holds the acceptor, WAL writer, config, and stats. |
| `Session` | `http_server.cpp:88` | One per TCP connection. Reads requests, dispatches handlers. |
| `WalWriter` | `wal_writer.h` | Thread-safe append-only WAL with CRC-32 framing. |
| `ServerConfig` | `http_server.h:14` | All tunable parameters with sensible defaults. |
| `GatewayStats` | `http_server.h:22` | Atomic counters for accepted/rejected requests and spans. |

#### Request lifecycle

```text
async_accept → Session::run() → Session::read()
  → parser with body_limit
  → on_read()
    → handle_request()
      → /healthz? → "ok\n"
      → POST /v1/traces?
        → check Content-Type
        → ParseFromString (protobuf validation)
        → wal_writer_.append() with fsync
        → count spans
        → serialize ExportTraceServiceResponse
        → write response
    → read() again (keep-alive) or close()
```

#### Error handling

| Condition | HTTP Status | Logged? |
|-----------|-------------|---------|
| Wrong method or path | 404 | No (counted as `rejected_requests`) |
| Wrong Content-Type | 415 | No |
| Malformed protobuf | 400 | No |
| Body exceeds limit | 413 | No |
| WAL write fails | 503 | Yes (stderr) |
| Response serialize fails | 500 | No |

#### Configuration (`ServerConfig`)

```cpp
struct ServerConfig {
  std::string address{"127.0.0.1"};       // Bind address
  std::uint16_t port{4318};               // OTLP standard port
  std::size_t max_body_bytes{4 * 1024 * 1024};  // 4 MiB body limit
  std::filesystem::path wal_directory{"data/wal"};
  std::size_t max_wal_file_bytes{64 * 1024 * 1024}; // 64 MiB rotation
};
```

All values are configurable via CLI flags: `--address`, `--port`,
`--max-body-bytes`, `--max-wal-file-bytes`, `--wal-dir`.

---

## 4. Write-Ahead Log (WAL)

### Record format

Each WAL record is a simple framed format:

```text
┌──────────────────┬──────────────────┬────────────────────────┐
│ payload_length   │ crc32_checksum   │ payload_bytes          │
│ uint32 LE        │ uint32 LE        │ variable length        │
│ (4 bytes)        │ (4 bytes)        │ (payload_length bytes) │
└──────────────────┴──────────────────┴────────────────────────┘
```

- **CRC-32**: IEEE polynomial (`0xEDB88320`), compatible with Python's
  `binascii.crc32()`. This is verified by a cross-language test.
- **Endianness**: little-endian, matching x86/ARM. The Python materializer
  unpacks with `struct.Struct("<II")`.
- **Sync**: `fsync()` (POSIX) or `_commit()` (Windows) after every record.

### Rotation

The WAL rotates when the current file would exceed `max_wal_file_bytes`.
Segment names include nanosecond timestamps and an atomic sequence counter
to guarantee uniqueness even at sub-nanosecond granularity:

```text
wal-1695571234567890123-0.wal
wal-1695571234567890456-1.wal
```

### Thread safety

`WalWriter::append()` holds a `std::mutex` (`std::scoped_lock`) for the
entire write-and-sync cycle. This serializes writes but guarantees
correctness. It is the "mutex serialization" noted in the status doc.

### Platform differences

```cpp
#ifdef _WIN32
  _open(..., _O_BINARY | _O_CREAT | _O_APPEND | _O_WRONLY, ...)
  _write(handle, data, size)
  _commit(handle)  // fsync equivalent
#else
  open(..., O_CREAT | O_APPEND | O_WRONLY, 0640)
  write(handle, data, size)
  fsync(handle)
#endif
```

---

## 5. Python Backend (FastAPI)

### Module structure

```text
backend/
├── app/
│   ├── __init__.py        # Package docstring
│   ├── main.py            # FastAPI app factory, all endpoints
│   ├── materializer.py    # WAL → Parquet conversion
│   ├── control.py         # SQLite schema initialization
│   └── settings.py        # Dataclass-based configuration
├── tests/
│   ├── test_api.py        # Unit + integration tests (8 tests)
│   └── test_gateway_e2e.py # Cross-process E2E test
├── pyproject.toml         # Dependencies and build config
└── Dockerfile             # Production image
```

### App factory pattern

`create_app(settings)` builds the `FastAPI` instance with all routes
registered as closures over the resolved `Settings`. This makes testing
trivial — each test can pass its own `Settings` pointing to a temp directory.

**File:** `backend/app/main.py:25`

### Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/healthz` | Liveness/readiness check |
| POST | `/api/v1/materialize` | Trigger WAL → Parquet conversion |
| GET | `/api/v1/traces` | List traces with aggregate metrics |
| GET | `/api/v1/traces/{trace_id}` | Get all spans for a trace |
| POST | `/api/v1/projects` | Create a named project |

### Background materializer

On startup, if `materialize_interval_seconds > 0` (default: 2.0s), an
`asyncio.Task` runs `materialize()` in a loop. This is a fire-and-forget
worker — errors are logged but don't crash the server.

```python
async def _materialize_loop():
    while True:
        try:
            materialize(resolved)
        except Exception as error:
            logger.warning("Auto-materialization error: %s", error)
        await asyncio.sleep(resolved.materialize_interval_seconds)
```

The task is cancelled cleanly during shutdown via the FastAPI lifespan
context manager.

### Settings

All configuration comes from environment variables with sensible defaults:

| Environment Variable | Default | Purpose |
|---------------------|---------|---------|
| `LRCP_DATA_DIR` | `./data` | Root data directory |
| `LRCP_WAL_DIR` | `$DATA_DIR/wal` | WAL file directory |
| `LRCP_PARQUET_DIR` | `$DATA_DIR/parquet` | Parquet output directory |
| `LRCP_SQLITE_PATH` | `$DATA_DIR/control.db` | SQLite database path |
| `LRCP_MATERIALIZE_INTERVAL_SECONDS` | `2.0` | Background materialization interval (0 = disabled) |

---

## 6. Materializer — WAL to Parquet

The materializer is the bridge between the raw WAL (binary protobuf) and
the queryable Parquet files.

### How it works

1. **Read the manifest**: query SQLite for all previously materialized
   `(source_file, source_offset)` pairs.
2. **Scan WAL files**: iterate every `*.wal` in the WAL directory (sorted).
3. **For each record**: verify the CRC-32, decode the protobuf, extract
   normalized span fields.
4. **Skip known records**: if `(filename, offset)` is in the manifest, skip.
5. **Write Parquet**: create one immutable Parquet file per materialization
   run: `spans-YYYYMMDDTHHMMSS.parquet` with Zstd compression.
6. **Update manifest**: insert all new `(file, offset, parquet_file)` rows
   into SQLite.

### Normalized span schema

Each span row contains:

| Field | Type | Source |
|-------|------|--------|
| `trace_id` | string | Hex-encoded 16-byte trace ID |
| `span_id` | string | Hex-encoded 8-byte span ID |
| `parent_span_id` | string | Hex-encoded parent span ID |
| `name` | string | Span operation name |
| `service_name` | string | From `service.name` resource attribute |
| `start_time_unix_nano` | int64 | Span start time |
| `end_time_unix_nano` | int64 | Span end time |
| `duration_ns` | int64 | `end - start`, clamped to ≥ 0 |
| `status_code` | int32 | OpenTelemetry status code |
| `status_message` | string | Status description |
| `resource_attributes_json` | string | JSON-encoded resource attributes |
| `attributes_json` | string | JSON-encoded span attributes |
| `source_file` | string | WAL filename for provenance |
| `source_offset` | int64 | Byte offset in WAL file |

### Known limitation: orphan Parquet files

If the process crashes after writing a Parquet file but before committing
the manifest rows to SQLite, the Parquet file will exist without manifest
coverage. The next materialization run won't re-process those records
(they're in the Parquet but not in SQLite), but duplicate data could appear
if the WAL is replayed. This is a known MVP limitation.

---

## 7. Query Layer — DuckDB over Parquet

DuckDB acts as an ephemeral analytical engine. Each query opens a fresh
connection (`duckdb.connect()`), uses `read_parquet(?)` with a list of all
Parquet files, and returns results as Python dicts.

### Trace list query

```sql
SELECT
  trace_id,
  min(start_time_unix_nano) AS start_time_unix_nano,
  sum(duration_ns) AS aggregate_span_duration_ns,
  count(*) AS span_count,
  max(service_name) AS service_name
FROM read_parquet(?)
GROUP BY trace_id
ORDER BY start_time_unix_nano DESC
LIMIT ?
```

### Trace detail query

```sql
SELECT *
FROM read_parquet(?)
WHERE trace_id = ?
ORDER BY start_time_unix_nano
```

### Why DuckDB?

- Zero configuration — no server process, no setup.
- Reads Parquet directly — no ETL step.
- Columnar analytics — efficient aggregation over wide schemas.
- Embeddable — just a Python import.

---

## 8. SQLite Control Store

SQLite stores two tables:

### `projects`
```sql
CREATE TABLE projects (
  id   TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```
Future: associates traces with projects for multi-tenant isolation.

### `materialized_wal_records`
```sql
CREATE TABLE materialized_wal_records (
  source_file   TEXT NOT NULL,
  source_offset INTEGER NOT NULL,
  parquet_file  TEXT NOT NULL,
  created_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (source_file, source_offset)
);
```
Prevents duplicate materialization across runs. Each row maps a WAL record
to the Parquet file it was written into.

---

## 9. Next.js Trace Explorer

### Architecture choice

The frontend uses **Next.js 15 with React 19 server components**. Pages
are server-rendered — no client-side state, no bundled API client. The
`fetch()` call runs server-side at request time (`cache: "no-store"`).

### Pages

| Route | Component | Purpose |
|-------|-----------|---------|
| `/` | `app/page.tsx` | Trace list with service name, span count, aggregate duration |
| `/traces/[traceId]` | `app/traces/[traceId]/page.tsx` | Span tree for one trace |

### Styling

A single `styles.css` provides a dark theme. The design is grid-based,
responsive (collapses to 2 columns on mobile), and uses system fonts with
Inter as primary.

### Backend URL

Set via `BACKEND_URL` environment variable (default: `http://127.0.0.1:8000`).
In Docker Compose, this is `http://backend:8000`.

---

## 10. Deployment

### Docker Compose (local)

```text
docker compose -f deploy/compose/compose.yaml up --build
```

Three services sharing a named volume (`lrcp-data`):
- **gateway** (port 4318): C++ binary built from source.
- **backend** (port 8000): uvicorn serving FastAPI.
- **frontend** (port 3000): Next.js standalone server.

The gateway Dockerfile uses a multi-stage build: Ubuntu 24.04 with build
tools → final Ubuntu 24.04 with only runtime libs. The frontend Dockerfile
uses Node 22 Alpine with `.next/standalone` output.

### Helm (experimental)

The Helm chart in `deploy/helm/lrcp/` defines only the gateway:
- Deployment with resource limits.
- Service exposing port 4318.
- PersistentVolumeClaim for `/data`.
- Readiness and liveness probes on `/healthz`.

Backend and frontend Helm templates are not yet defined.

### CI

GitHub Actions (`.github/workflows/ci.yml`) runs three parallel jobs:
- **cpp**: Build and test on Ubuntu with system packages.
- **python**: pip install and pytest.
- **frontend**: `npm ci && npm run build`.

---

## 11. Testing Strategy

### C++ tests (GTest)

7 tests in `cpp/gateway/tests/gateway_tests.cpp`:

| Test | What it verifies |
|------|-----------------|
| `Crc32Compatibility.MatchesPythonBinAsciiVector` | CRC-32 matches Python's `binascii.crc32` — ensures C++/Python WAL compatibility |
| `HealthEndpointReturnsOk` | `/healthz` returns 200 with `"ok\n"` |
| `AcceptsBinaryOtlpTraceRequest` | Valid protobuf → 200, WAL record created with correct CRC |
| `RejectsWrongContentType` | `application/json` → 415 |
| `RejectsMalformedProtobuf` | Bad bytes → 400 |
| `RejectsOversizePayload` | Body > limit → 413 |
| `RotatesWalWhenMaxFileBytesExceeded` | Two records exceeding limit → two separate WAL files |

Each test starts a real HTTP server on port 0 (OS-assigned) and makes actual
TCP connections. This is integration-level testing, not mocking.

### Python tests (pytest)

8 tests across two files:

**`test_api.py`** — 7 tests using `TestClient` (in-process ASGI):
- Health check
- CRC-32 cross-language compatibility
- Project creation and duplicate rejection
- Materialization of WAL records (once, no duplicates)
- Checksum mismatch rejection
- Full query pipeline (materialize → list → get)
- Background auto-materialization with polling

**`test_gateway_e2e.py`** — 1 test:
- Starts the actual C++ binary as a subprocess
- Sends an OTLP request via `urllib`
- Materializes the resulting WAL
- Queries the materialized trace

### Frontend

No unit tests. The CI verifies that `npm run build` succeeds (type-checking
and Next.js compilation).

---

## 12. Configuration Reference

### Gateway CLI

```
lrcp-gateway [OPTIONS]

  --address ADDR          Listen IP (default: 127.0.0.1)
  --port PORT             Listen port (default: 4318, use 0 for OS-assigned)
  --max-body-bytes N      Max HTTP body size in bytes (default: 4194304)
  --max-wal-file-bytes N  WAL rotation threshold (default: 67108864)
  --wal-dir PATH          WAL directory path (default: data/wal)
  --help                  Show usage and exit
```

### Backend environment

| Variable | Default | Purpose |
|----------|---------|---------|
| `LRCP_DATA_DIR` | `./data` | Base data directory |
| `LRCP_WAL_DIR` | `$DATA_DIR/wal` | WAL directory |
| `LRCP_PARQUET_DIR` | `$DATA_DIR/parquet` | Parquet output |
| `LRCP_SQLITE_PATH` | `$DATA_DIR/control.db` | SQLite path |
| `LRCP_MATERIALIZE_INTERVAL_SECONDS` | `2.0` | Auto-materialize interval (0=off) |

### Frontend environment

| Variable | Default | Purpose |
|----------|---------|---------|
| `BACKEND_URL` | `http://127.0.0.1:8000` | API server URL |

---

## 13. Key Design Decisions

### WAL before acknowledgement

The gateway does not return HTTP 200 until the WAL record is fsynced. This
means a successful response guarantees the data is on disk. The trade-off
is latency per request (one fsync per OTLP export).

### Protobuf stored as-is in WAL

The WAL stores the raw OTLP protobuf bytes rather than a normalized format.
This preserves full fidelity and allows re-materialization with new schemas.
The materializer handles normalization.

### Immutable Parquet parts

Each materialization run creates a new Parquet file rather than modifying
existing ones. This makes retention/deletion simple (just delete old files)
and avoids concurrent read/write issues.

### No ORM

The project uses raw SQL (SQLite via `sqlite3`, DuckDB via `duckdb`). This
keeps the data path transparent and avoids ORM overhead and abstraction
leaks. The schemas are simple enough that an ORM adds no value.

### Server components only

The Next.js frontend renders entirely on the server. There is no client-side
JavaScript for data fetching, no state management library, no loading
spinners. Each page load yields a complete HTML document. This is simpler
and faster for a developer tool.

---

## 14. Extending the System

### Adding a new OTLP signal (metrics, logs)

1. Add a new route in `http_server.cpp` (e.g., `POST /v1/metrics`).
2. Parse the corresponding protobuf type.
3. Write to a separate WAL or use a record type prefix.
4. Extend the materializer to handle the new record type.
5. Add DuckDB query endpoints.

### Adding a Python SDK

A Python SDK would wrap `OTLPSpanExporter` with LLM-specific convenience:

```python
from lrcp import LRCPTracer

tracer = LRCPTracer(service_name="my-rag-app")
with tracer.llm_call(model="gpt-4", prompt="...") as span:
    response = call_llm(...)
    span.set_response(response)
    span.set_token_usage(prompt_tokens=100, completion_tokens=50)
```

### Adding ClickHouse for production

Replace the DuckDB query layer with ClickHouse client calls. The Parquet
schema maps directly to a ClickHouse `MergeTree` table. The materializer
would `INSERT` directly instead of writing Parquet files.

### Adding authentication

1. Add a middleware to FastAPI that validates API keys or JWTs.
2. Associate API keys with projects in SQLite.
3. Filter queries by the authenticated project ID.
4. Add the same validation to the C++ gateway (or use a reverse proxy).

---

## Appendix: File Index

| File | Language | Lines | Purpose |
|------|----------|-------|---------|
| `cpp/gateway/src/main.cpp` | C++ | 64 | Gateway entry point, CLI parsing, signal handling |
| `cpp/gateway/src/http_server.cpp` | C++ | 228 | HTTP server, OTLP handler, session management |
| `cpp/gateway/src/wal_writer.cpp` | C++ | 130 | WAL: CRC-32, framing, rotation, fsync |
| `cpp/gateway/include/lrcp/gateway/http_server.h` | C++ | 48 | Public API for HttpServer, ServerConfig, GatewayStats |
| `cpp/gateway/include/lrcp/gateway/wal_writer.h` | C++ | 41 | Public API for WalWriter |
| `cpp/gateway/tests/gateway_tests.cpp` | C++ | 233 | GTest suite (7 tests) |
| `cpp/gateway/Dockerfile` | Docker | 23 | Multi-stage gateway image |
| `cpp/gateway/docker-entrypoint.sh` | Shell | 12 | WAL directory creation before exec |
| `backend/app/main.py` | Python | 119 | FastAPI app factory, all HTTP endpoints |
| `backend/app/materializer.py` | Python | 115 | WAL → Parquet conversion engine |
| `backend/app/control.py` | Python | 27 | SQLite schema initialization |
| `backend/app/settings.py` | Python | 26 | Dataclass configuration from environment |
| `backend/tests/test_api.py` | Python | 122 | Unit + integration tests (7 tests) |
| `backend/tests/test_gateway_e2e.py` | Python | 109 | Cross-process E2E test |
| `backend/pyproject.toml` | TOML | 27 | Python dependencies and build config |
| `backend/Dockerfile` | Docker | 8 | Backend production image |
| `frontend/app/page.tsx` | TypeScript | 60 | Trace list page |
| `frontend/app/traces/[traceId]/page.tsx` | TypeScript | 71 | Trace detail page |
| `frontend/app/layout.tsx` | TypeScript | 7 | Root layout with metadata |
| `frontend/app/styles.css` | CSS | 9 | Dark theme, responsive grid |
| `frontend/next.config.ts` | TypeScript | 3 | Standalone output mode |
| `frontend/Dockerfile` | Docker | 14 | Multi-stage frontend image |
| `deploy/compose/compose.yaml` | YAML | 42 | Docker Compose (gateway + backend + frontend) |
| `deploy/helm/lrcp/templates/gateway.yaml` | YAML | 37 | Helm: gateway Deployment + Service + PVC |
| `deploy/helm/lrcp/Chart.yaml` | YAML | 6 | Helm chart metadata |
| `deploy/helm/lrcp/values.yaml` | YAML | 10 | Helm default values |
| `examples/basic/python/app.py` | Python | 22 | Minimal OTLP trace exporter |
| `CMakeLists.txt` | CMake | 69 | Top-level C++ build system |
| `.github/workflows/ci.yml` | YAML | 34 | CI: C++ build+test, Python test, frontend build |
