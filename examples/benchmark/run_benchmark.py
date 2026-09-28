"""
Reproducible Benchmark Suite for LLM Reliability Control Plane (LRCP)
Measures:
1. Gateway Ingestion Throughput & Latency (OTLP/HTTP -> WAL)
2. Materializer Throughput (WAL -> Parquet)
3. DuckDB Analytical Query Latency (Parquet -> DuckDB)
"""
from __future__ import annotations

import argparse
import os
import sys
import time
import urllib.request
import statistics
import concurrent.futures
from pathlib import Path
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest


def _build_payload(trace_idx: int, spans_per_req: int = 4) -> bytes:
    req = ExportTraceServiceRequest()
    rs = req.resource_spans.add()
    rs.resource.attributes.add(key="service.name").value.string_value = "benchmark-service"
    ss = rs.scope_spans.add()
    ss.scope.name = "benchmark-harness"
    ss.scope.version = "1.0.0"

    trace_id = bytes.fromhex(f"{trace_idx:032x}")
    parent_id = bytes.fromhex("11" * 8)

    # Root span
    root = ss.spans.add()
    root.trace_id = trace_id
    root.span_id = parent_id
    root.name = "agent.workflow"
    root.start_time_unix_nano = int(time.time_ns())
    root.end_time_unix_nano = root.start_time_unix_nano + 50_000_000

    # Child spans
    for s_idx in range(1, spans_per_req):
        s = ss.spans.add()
        s.trace_id = trace_id
        s.span_id = bytes.fromhex(f"{s_idx:016x}")
        s.parent_span_id = parent_id
        s.name = f"step.{s_idx}"
        s.start_time_unix_nano = root.start_time_unix_nano + (s_idx * 10_000_000)
        s.end_time_unix_nano = s.start_time_unix_nano + 8_000_000
        s.attributes.add(key="gen_ai.request.model").value.string_value = "benchmark-model"
        s.attributes.add(key="gen_ai.usage.prompt_tokens").value.int_value = 128
        s.attributes.add(key="gen_ai.usage.completion_tokens").value.int_value = 64

    return req.SerializeToString()


def benchmark_gateway_ingest(
    gateway_url: str,
    total_requests: int = 500,
    concurrency: int = 10,
    spans_per_req: int = 4,
    auth_token: str | None = None,
) -> dict:
    print(f"\n[1/3] Benchmarking Gateway Ingestion ({total_requests} requests, concurrency={concurrency})...")
    payloads = [_build_payload(i, spans_per_req) for i in range(total_requests)]
    headers = {"Content-Type": "application/x-protobuf"}
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    latencies_ms: list[float] = []

    def send_one(payload: bytes) -> float:
        req = urllib.request.Request(f"{gateway_url}/v1/traces", data=payload, headers=headers, method="POST")
        t0 = time.perf_counter()
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status != 200:
                raise RuntimeError(f"Unexpected status: {resp.status}")
        t1 = time.perf_counter()
        return (t1 - t0) * 1000.0

    start_wall = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(send_one, p) for p in payloads]
        for f in concurrent.futures.as_completed(futures):
            latencies_ms.append(f.result())
    end_wall = time.perf_counter()

    total_time_s = end_wall - start_wall
    total_spans = total_requests * spans_per_req
    reqs_per_sec = total_requests / total_time_s
    spans_per_sec = total_spans / total_time_s

    latencies_ms.sort()
    p50 = statistics.median(latencies_ms)
    p95 = latencies_ms[int(len(latencies_ms) * 0.95)]
    p99 = latencies_ms[int(len(latencies_ms) * 0.99)]

    print(f"  Total Spans Ingested: {total_spans:,}")
    print(f"  Wall Time:           {total_time_s:.2f} s")
    print(f"  Throughput:          {reqs_per_sec:.1f} req/s ({spans_per_sec:.1f} spans/s)")
    print(f"  Latency p50:         {p50:.2f} ms")
    print(f"  Latency p95:         {p95:.2f} ms")
    print(f"  Latency p99:         {p99:.2f} ms")

    return {
        "reqs_per_sec": reqs_per_sec,
        "spans_per_sec": spans_per_sec,
        "p50_ms": p50,
        "p95_ms": p95,
        "p99_ms": p99,
    }


def benchmark_backend_materialize(backend_url: str, auth_token: str | None = None) -> dict:
    print(f"\n[2/3] Benchmarking Backend Materialization...")
    headers = {}
    if auth_token:
        headers["X-API-Key"] = auth_token

    req = urllib.request.Request(f"{backend_url}/api/v1/materialize", data=b"", headers=headers, method="POST")
    t0 = time.perf_counter()
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = resp.read().decode()
    t1 = time.perf_counter()

    duration = t1 - t0
    print(f"  Response: {body}")
    print(f"  Materialization Latency: {duration*1000.0:.2f} ms")
    return {"materialize_time_s": duration}


def benchmark_queries(backend_url: str, query_count: int = 50, auth_token: str | None = None) -> dict:
    print(f"\n[3/3] Benchmarking DuckDB Analytical Query Latency ({query_count} iterations)...")
    headers = {}
    if auth_token:
        headers["X-API-Key"] = auth_token

    latencies_ms: list[float] = []
    for _ in range(query_count):
        req = urllib.request.Request(f"{backend_url}/api/v1/traces?limit=100", headers=headers, method="GET")
        t0 = time.perf_counter()
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
        t1 = time.perf_counter()
        latencies_ms.append((t1 - t0) * 1000.0)

    latencies_ms.sort()
    p50 = statistics.median(latencies_ms)
    p95 = latencies_ms[int(len(latencies_ms) * 0.95)]
    p99 = latencies_ms[int(len(latencies_ms) * 0.99)]

    print(f"  Query Latency p50: {p50:.2f} ms")
    print(f"  Query Latency p95: {p95:.2f} ms")
    print(f"  Query Latency p99: {p99:.2f} ms")

    return {"query_p50_ms": p50, "query_p95_ms": p95, "query_p99_ms": p99}


def main():
    parser = argparse.ArgumentParser(description="LRCP Performance Benchmark")
    parser.add_argument("--gateway-url", default="http://127.0.0.1:4318", help="Gateway endpoint")
    parser.add_argument("--backend-url", default="http://127.0.0.1:8000", help="FastAPI backend endpoint")
    parser.add_argument("--requests", type=int, default=1000, help="Number of trace export requests")
    parser.add_argument("--concurrency", type=int, default=16, help="Concurrent workers for ingestion")
    parser.add_argument("--spans-per-req", type=int, default=5, help="Spans per ExportTraceServiceRequest")
    parser.add_argument("--auth-token", default=None, help="Authentication token if configured")
    args = parser.parse_args()

    print("=" * 60)
    print("  LLM Reliability Control Plane - Benchmark")
    print("=" * 60)

    try:
        benchmark_gateway_ingest(
            gateway_url=args.gateway_url,
            total_requests=args.requests,
            concurrency=args.concurrency,
            spans_per_req=args.spans_per_req,
            auth_token=args.auth_token,
        )
        benchmark_backend_materialize(backend_url=args.backend_url, auth_token=args.auth_token)
        benchmark_queries(backend_url=args.backend_url, query_count=50, auth_token=args.auth_token)
    except Exception as e:
        print(f"\n[!] Benchmark aborted: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
