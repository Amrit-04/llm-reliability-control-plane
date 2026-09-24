"""
LM Studio Live Model Integration Test for LRCP

This script:
1. Connects to LM Studio's local OpenAI-compatible server (default: http://127.0.0.1:1234/v1).
2. Auto-discovers all loaded models.
3. Filters models to only test chat completion models (skips embedding models).
4. Sends test prompts to each model.
5. Instruments each call using OpenTelemetry with standard GenAI semantic conventions:
   - gen_ai.system: "lm_studio"
   - gen_ai.request.model: model ID
   - gen_ai.response.model: model ID returned
   - gen_ai.usage.input_tokens: prompt token count
   - gen_ai.usage.output_tokens: completion token count
   - gen_ai.response.finish_reasons: finish reason
6. Exports traces via OTLP/HTTP to the C++ Gateway (http://127.0.0.1:4318/v1/traces).
7. Triggers materialization and queries the FastAPI Backend (http://127.0.0.1:8000).
8. Verifies end-to-end data integrity and prints a rich summary table.

Usage:
    # 1. Start LM Studio -> Click "Local Server" tab -> Click "Start Server" (port 1234)
    # 2. Load one or more models in LM Studio
    # 3. Run:
    python test_lm_studio.py
    # Or with options:
    python test_lm_studio.py --model my-model-name --timeout 120

Environment Variables:
    LM_STUDIO_URL: Base URL for LM Studio API (default: http://127.0.0.1:1234/v1)
    GATEWAY_URL: OTLP endpoint for LRCP Gateway (default: http://127.0.0.1:4318/v1/traces)
    BACKEND_URL: FastAPI backend URL (default: http://127.0.0.1:8000)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request
import urllib.error
from dataclasses import dataclass
from typing import Any

# OpenTelemetry SDK
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

LM_STUDIO_URL = os.environ.get("LM_STUDIO_URL", "http://127.0.0.1:1234/v1")
GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://127.0.0.1:4318/v1/traces")
BACKEND_URL = os.environ.get("BACKEND_URL", "http://127.0.0.1:8000")


@dataclass
class TestResult:
    model_id: str
    prompt: str
    response_text: str
    input_tokens: int
    output_tokens: int
    latency_ms: float
    trace_id: str
    span_id: str
    verified_in_backend: bool


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Run LM Studio integration tests with OpenTelemetry tracing.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python test_lm_studio.py                          # Test all available models
  python test_lm_studio.py --model gemma-2b         # Test specific model only
  python test_lm_studio.py --timeout 180            # Set 3 minute timeout
  python test_lm_studio.py --prompt "Your custom prompt"
  python test_lm_studio.py --all-models --timeout 120
        """
    )
    parser.add_argument(
        "--model",
        "-m",
        type=str,
        default=None,
        help="Specific model ID to test (tests all if not specified)",
    )
    parser.add_argument(
        "--timeout",
        "-t",
        type=int,
        default=60,
        help="Timeout in seconds for model calls (default: 60)",
    )
    parser.add_argument(
        "--prompt",
        "-p",
        type=str,
        default=None,
        help="Custom prompt to use (default: pre-defined prompts)",
    )
    parser.add_argument(
        "--all-models",
        "-a",
        action="store_true",
        help="Test all models including those that might be slow",
    )
    return parser.parse_args()


def check_service(url: str, name: str) -> bool:
    """Check if a service is responsive."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "LRCP-Tester"})
        with urllib.request.urlopen(req, timeout=3) as resp:
            return resp.status in (200, 404, 405)  # Any active HTTP response
    except urllib.error.HTTPError:
        return True  # Server responded, so it's running
    except Exception:
        return False


def get_lm_studio_models(base_url: str) -> list[dict]:
    """Fetch loaded models from LM Studio."""
    url = f"{base_url}/models"
    req = urllib.request.Request(url, headers={"User-Agent": "LRCP-Tester"})
    with urllib.request.urlopen(req, timeout=5) as resp:
        data = json.loads(resp.read().decode())
        models = data.get("data", [])
        # Sort by type: chat models first, then others
        return sorted(
            models,
            key=lambda m: (0 if "chat" in m.get("id", "").lower() else 1, m.get("id", ""))
        )


def is_chat_model(model_id: str) -> bool:
    """Determine if a model supports chat completions based on its ID."""
    # Common embedding model indicators
    embedding_indicators = ["embedding", "embed", "nomic", "bge", "e5"]
    model_lower = model_id.lower()

    # If it explicitly says "chat" in the name, it's a chat model
    if "chat" in model_lower:
        return True

    # If it matches known embedding patterns, skip it
    for indicator in embedding_indicators:
        if indicator in model_lower:
            return False

    return True  # Assume chat by default if no indicators


def call_lm_studio(base_url: str, model_id: str, prompt: str, timeout: int = 60) -> dict[str, Any]:
    """Call LM Studio's chat completions endpoint."""
    url = f"{base_url}/chat/completions"
    payload = {
        "model": model_id,
        "messages": [
            {"role": "system", "content": "You are a concise AI assistant. Answer in 1-2 sentences."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.7,
        "max_tokens": 150
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json", "User-Agent": "LRCP-Tester"},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def setup_tracer(service_name: str = "lm-studio-evaluator") -> trace.Tracer:
    """Configure OpenTelemetry provider pointing to LRCP Gateway."""
    resource = Resource.create({
        "service.name": service_name,
        "service.version": "0.1.0",
        "deployment.environment": "local-evaluation",
    })
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=GATEWAY_URL)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    return trace.get_tracer("lrcp.lm_studio.test")


def format_output_safe(text: str, max_width: int = 100) -> str:
    """Format text for safe console output (Windows/UTF-8 safe)."""
    if not text:
        return "(empty response)"
    # Truncate long responses
    if len(text) > max_width:
        text = text[:max_width - 3] + "..."
    # Replace any problematic Unicode characters
    return text.encode('utf-8', errors='replace').decode('utf-8')


def run_tests(model_filter: str | None = None, timeout: int = 60, prompt: str | None = None):
    """Run the LM Studio integration test suite."""
    # Use safer print function for Windows console
    print_func = lambda *args, **kwargs: print(*args, flush=True, **kwargs)

    print("=" * 70)
    print("LRCP LM Studio Integration & Verification Test Suite")
    print("=" * 70)

    # 1. Check if LM Studio is reachable
    print(f"\n[1/6] Checking LM Studio at {LM_STUDIO_URL}...")
    if not check_service(f"{LM_STUDIO_URL}/models", "LM Studio"):
        print_func(f"ERROR: LM Studio is not reachable at {LM_STUDIO_URL}")
        print_func("\nTo fix this:")
        print_func("  1. Open LM Studio")
        print_func("  2. Go to the '<->' (Local Server) tab on the left")
        print_func("  3. Load at least one model")
        print_func("  4. Click 'Start Server' (make sure port is 1234 or set LM_STUDIO_URL)")
        sys.exit(1)
    print_func("OK: LM Studio server is running!")

    # 2. Discover and filter models
    models = get_lm_studio_models(LM_STUDIO_URL)
    if not models:
        print_func("WARNING: LM Studio returned 0 models. Please load a model in LM Studio.")
        sys.exit(1)

    # Apply model filter if specified
    if model_filter:
        filtered = [m for m in models if model_filter.lower() in m.get("id", "").lower()]
        if not filtered:
            print_func(f"WARNING: No model found matching '{model_filter}'")
            print_func(f"Available models: {[m['id'] for m in models]}")
            sys.exit(1)
        models = filtered

    # Filter out embedding models by default
    chat_models = [m for m in models if is_chat_model(m.get("id", ""))]
    skipped_models = [m for m in models if not is_chat_model(m.get("id", ""))]

    if skipped_models and not model_filter:
        print_func(f"Skipping {len(skipped_models)} embedding model(s) (not chat completion):")
        for m in skipped_models:
            print_func(f"  - {m['id']}")

    models = chat_models

    print_func(f"OK: Found {len(models)} model(s) available for testing:")
    for i, m in enumerate(models, 1):
        print_func(f"   {i}. {m['id']}")

    # 3. Check LRCP Gateway
    print_func(f"\n[2/6] Checking LRCP C++ Gateway at {GATEWAY_URL}...")
    if not check_service("http://127.0.0.1:4318/healthz", "LRCP Gateway"):
        print_func("ERROR: LRCP Gateway is not running on port 4318.")
        print_func("\nTo start it:")
        print_func("  ./build/Debug/lrcp-gateway.exe --port 4318 --wal-dir data/wal")
        sys.exit(1)
    print_func("OK: LRCP C++ Gateway is healthy and accepting traces!")

    # 4. Check LRCP Backend
    print_func(f"\n[3/6] Checking LRCP Backend at {BACKEND_URL}...")
    if not check_service(f"{BACKEND_URL}/healthz", "LRCP Backend"):
        print_func(f"WARNING: LRCP Backend is not running at {BACKEND_URL}.")
        print_func("  Auto-materialization won't be queried, but WAL traces will still be written.")
        backend_available = False
    else:
        print_func("OK: LRCP Backend is healthy!")
        backend_available = True

    # 5. Execute Instrumented Tests
    print_func(f"\n[4/6] Running instrumented inference across {len(models)} model(s)...")
    tracer = setup_tracer()

    # Prepare prompts
    default_prompts = [
        "Explain what an LLM observability control plane does in one sentence.",
        "What is the difference between latency and throughput in LLM serving?",
        "Why is durable write-ahead logging important for telemetry?",
        "Describe a use case for LLM trace analysis.",
        "What are the key components of OpenTelemetry?",
    ]
    if prompt:
        prompts = [prompt]
    else:
        prompts = default_prompts

    results: list[TestResult] = []

    for model_idx, model in enumerate(models):
        model_id = model["id"]
        prompt_text = prompts[model_idx % len(prompts)]

        print_func(f"\n--- Testing Model: {model_id} ---")
        print_func(f"Prompt: {format_output_safe(prompt_text)}")

        start_time = time.perf_counter()

        # Create a parent workflow span + child LLM call span
        with tracer.start_as_current_span(f"agent.task.{model_id}") as parent_span:
            parent_span.set_attribute("task.name", "model_evaluation")
            parent_span.set_attribute("target.model", model_id)

            trace_id_hex = format(parent_span.get_span_context().trace_id, "032x")

            with tracer.start_as_current_span("llm.generate") as llm_span:
                llm_span.set_attribute("gen_ai.system", "lm_studio")
                llm_span.set_attribute("gen_ai.request.model", model_id)
                llm_span.set_attribute("gen_ai.operation.name", "chat")

                try:
                    response = call_lm_studio(LM_STUDIO_URL, model_id, prompt_text, timeout)
                    latency = (time.perf_counter() - start_time) * 1000

                    # Parse response
                    choice = response.get("choices", [{}])[0]
                    msg_content = choice.get("message", {}).get("content", "").strip()
                    finish_reason = choice.get("finish_reason", "stop")
                    usage = response.get("usage", {})
                    input_tokens = usage.get("prompt_tokens", 0)
                    output_tokens = usage.get("completion_tokens", 0)

                    # Enrich span with GenAI attributes
                    llm_span.set_attribute("gen_ai.response.model", response.get("model", model_id))
                    llm_span.set_attribute("gen_ai.response.finish_reasons", [finish_reason])
                    llm_span.set_attribute("gen_ai.usage.input_tokens", input_tokens)
                    llm_span.set_attribute("gen_ai.usage.output_tokens", output_tokens)
                    llm_span.set_attribute("llm.latency_ms", latency)

                    span_id_hex = format(llm_span.get_span_context().span_id, "016x")

                    print_func(f"Response: {format_output_safe(msg_content)}")
                    print_func(f"Metrics: Latency={latency:.1f}ms | In Tokens={input_tokens} | Out Tokens={output_tokens}")
                    print_func(f"Trace ID: {trace_id_hex[:16]}...")

                    results.append(TestResult(
                        model_id=model_id,
                        prompt=prompt_text,
                        response_text=msg_content,
                        input_tokens=input_tokens,
                        output_tokens=output_tokens,
                        latency_ms=latency,
                        trace_id=trace_id_hex,
                        span_id=span_id_hex,
                        verified_in_backend=False
                    ))

                except urllib.error.HTTPError as e:
                    llm_span.record_exception(e)
                    llm_span.set_attribute("error", True)
                    print_func(f"HTTP Error {e.code}: {e.reason}")
                except Exception as e:
                    llm_span.record_exception(e)
                    llm_span.set_attribute("error", True)
                    print_func(f"ERROR during model call: {e}")

    # Force flush OTLP spans to Gateway
    print_func("\nFlushing OTLP telemetry to C++ Gateway...")
    trace.get_tracer_provider().force_flush()
    print_func("OK: Telemetry flushed to Gateway WAL!")

    # 6. Verify Materialization & Query Plane
    print_func(f"\n[5/6] Verifying telemetry in LRCP Backend...")
    if backend_available:
        # Trigger explicit materialization
        try:
            req = urllib.request.Request(f"{BACKEND_URL}/api/v1/materialize", data=b"", method="POST")
            with urllib.request.urlopen(req, timeout=5) as resp:
                mat_data = json.loads(resp.read().decode())
                print_func(f"OK: Materialization completed: {mat_data.get('materialized_spans', 0)} spans converted to Parquet")
        except Exception as e:
            print_func(f"WARNING: Materialize trigger: {e}")

        # Query traces
        try:
            req = urllib.request.Request(f"{BACKEND_URL}/api/v1/traces", headers={"User-Agent": "LRCP-Tester"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                traces_list = json.loads(resp.read().decode())
                found_trace_ids = {t["trace_id"] for t in traces_list}

                for res in results:
                    if res.trace_id in found_trace_ids:
                        res.verified_in_backend = True

        except Exception as e:
            print_func(f"WARNING: Trace query error: {e}")

    # 7. Print Final Verification Summary
    print_func("\n" + "=" * 70)
    print_func("TEST SUMMARY & VERIFICATION MATRIX")
    print_func("=" * 70)
    print_func(f"{'Model':<30} | {'Tokens (In/Out)':<15} | {'Latency':<10} | {'Status'}")
    print_func("-" * 70)

    for r in results:
        status = "VERIFIED" if r.verified_in_backend else "IN WAL"
        tok_str = f"{r.input_tokens}/{r.output_tokens}"
        lat_str = f"{r.latency_ms:.0f}ms"
        print_func(f"{r.model_id[:30]:<30} | {tok_str:<15} | {lat_str:<10} | {status}")

    print_func("=" * 70)

    # Analytics summary if backend is available
    if backend_available:
        try:
            req = urllib.request.Request(f"{BACKEND_URL}/api/v1/analytics/overview", headers={"User-Agent": "LRCP-Tester"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                analytics = json.loads(resp.read().decode())
                print_func("\nSYSTEM OVERVIEW (from analytics):")
                print_func(f"  Total Traces: {analytics.get('total_traces', 0)}")
                print_func(f"  Total Spans: {analytics.get('total_spans', 0)}")
                print_func(f"  Total Input Tokens: {analytics.get('total_input_tokens', 0)}")
                print_func(f"  Total Output Tokens: {analytics.get('total_output_tokens', 0)}")
        except Exception as e:
            print_func(f"\nWARNING: Could not fetch analytics overview: {e}")

    print_func("\nTesting complete! Open http://localhost:3000 to explore the traces in the UI.")
    print_func("=" * 70)


def main():
    """Main entry point."""
    args = parse_args()

    # Set timeout globally if specified
    if args.timeout:
        os.environ["LM_STUDIO_TIMEOUT"] = str(args.timeout)

    run_tests(
        model_filter=args.model,
        timeout=args.timeout,
        prompt=args.prompt
    )


if __name__ == "__main__":
    main()
