# Implementation Plan: Code Quality Improvements

**Date:** 2026-09-24

## Overview

This document outlines the improvements being made to transform the LLM Reliability Control Plane from an MVP into a production-ready, maintainable codebase.

---

## Improvements Completed

### 1. Documentation ✅
- **ARCHITECTURE.md**: Complete deep-dive covering every component, data flows, and design decisions.
- **IMPLEMENTATION_PLAN.md**: This file.

### 2. Code Quality & Reliability Hardening ✅

#### A. Storage Lifecycle & Parquet Compaction (P0 #1, P0 #2, P1 #10)
- ✅ Parquet manifest isolation: `get_committed_parquet_files()` only queries files with `status = 'COMMITTED'`
- ✅ Crash-safe compaction lifecycle: `.tmp` write → atomic rename → atomic SQLite transaction → safe unlink
- ✅ Orphan file reconciliation: auto-cleanup of uncommitted `.tmp` and stale physical files on startup
- ✅ Memory-safe streaming compaction: PyArrow row-group streaming via `pq.ParquetWriter`
- ✅ Cross-process storage locking: SQLite `BEGIN EXCLUSIVE` lock on dedicated lockfile coordinating workers

#### B. OpenTelemetry Semantics & Analytics (P0 #3)
- ✅ Strict status code semantics: `status_code = 2` (STATUS_CODE_ERROR) for error counts and rates
- ✅ Status UNSET (0) and OK (1) accurately counted as non-errors
- ✅ Trace detail and waterfall error indicator alignment

#### C. Authentication & Tenant Isolation (P0 #4, P0 #5, P1 #7, P1 #8)
- ✅ Fail-closed startup validation: `LRCP_REQUIRE_AUTH=true` rejects missing or empty API keys
- ✅ SHA-256 API key hashing with constant-time `hmac.compare_digest` verification
- ✅ Project secrets masking: raw keys returned once at creation (HTTP 201) and excluded from listings
- ✅ Tenant query isolation: project-scoped identity automatically restricts all trace and analytics queries
- ✅ Frontend server-side auth: Next.js Server Components pass `X-API-Key` without leaking to client bundles

#### D. Defensive Ingestion & Diagnostics (P0 #6, P1 #9, P1 #11, P1 #12)
- ✅ SQL parameter binding: parameterized `?` placeholders across all DuckDB and SQLite statements
- ✅ Precise WAL corruption offset: structured `WalCorruptionError` persisting exact byte offsets to SQLite
- ✅ Active WAL segment ordering: timestamp-sequence sorted physical order invariant
- ✅ Graceful background worker shutdown: `asyncio.Event` coordination preventing in-flight task corruption

---

## Performance Targets

### C++
- **Modern C++20**: Use `std::optional`, `std::string_view`, structured bindings
- **RAII everywhere**: No manual memory management
- **Const correctness**: Mark everything const that can be const
- **Explicit over implicit**: Use `explicit` constructors, named casts
- **Thread safety**: Document all thread-safety guarantees in comments

### Python
- **Type hints**: Use `from __future__ import annotations` and full type coverage
- **Google-style docstrings**: Args, Returns, Raises sections
- **Dataclasses**: Prefer over plain dicts for structured data
- **Context managers**: Use `with` for all resources
- **Async-first**: FastAPI endpoints should be `async def` where I/O-bound

### TypeScript
- **Strict mode**: Enable all strict compiler options
- **Server components**: Prefer over client components for data fetching
- **Type imports**: Use `import type` for type-only imports
- **Const assertions**: Use `as const` for literal types
- **Discriminated unions**: For state machines and variants

---

## Performance Targets

### Gateway (C++)
- **Throughput**: ≥10,000 spans/sec on 4-core desktop
- **Latency**: p99 < 10ms for 1KB payloads
- **Memory**: < 100 MB RSS for 1M spans/hour workload
- **CPU**: < 50% utilization at target throughput

### Backend (Python)
- **Materialization**: Process 1M spans in < 30 seconds
- **Query latency**: p99 < 100ms for trace list, < 50ms for trace detail
- **Memory**: < 500 MB RSS during materialization
- **Concurrent requests**: Support 50 concurrent query clients

### Frontend (Next.js)
- **First load**: < 1 second on localhost
- **Time to interactive**: < 500ms
- **Bundle size**: < 200 KB gzipped
- **Accessibility**: WCAG 2.1 AA compliant

---

## Testing Strategy

### Unit Tests
- **Coverage target**: 80% line coverage, 90% critical path
- **Test isolation**: Each test uses temp directories, no shared state
- **Fast feedback**: Entire unit test suite runs in < 10 seconds

### Integration Tests
- **E2E pipeline**: Gateway → WAL → Materializer → Query → UI
- **Failure injection**: Crash recovery, checksum mismatch, disk full
- **Cross-platform**: Test on Linux, macOS, Windows

### Performance Tests
- **Benchmark suite**: Repeatable load tests with fixed workloads
- **Regression detection**: Automated comparison against baseline
- **Resource profiling**: Track CPU, memory, I/O over time

---

## Security Checklist

- ✅ Input validation on all HTTP endpoints
- ✅ CRC-32 verification on WAL reads
- ✅ Bounded request sizes (no unbounded allocations)
- ⚠️ No authentication yet (planned)
- ⚠️ No TLS support (use reverse proxy)
- ⚠️ No PII redaction (planned)
- ⚠️ No rate limiting (planned)
- ⚠️ No audit logging (planned)

---

## Next Steps

1. **Complete code improvements** (this PR)
2. **Benchmark suite** (measure current performance)
3. **Bounded queue** (remove mutex bottleneck)
4. **Authentication** (API keys in SQLite)
5. **Monitoring** (Prometheus exporter)
6. **Production deployment guide** (Kubernetes best practices)

---

## Contributing

When adding new features:
1. Update ARCHITECTURE.md with the new component
2. Add tests covering happy path and failure modes
3. Update configuration documentation
4. Add a changelog entry
5. Ensure CI passes on all platforms
