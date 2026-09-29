import { TraceViewer, Span } from "./TraceViewer";

async function loadTrace(traceId: string): Promise<{ spans: Span[]; error: string | null }> {
  const endpoint = process.env.BACKEND_URL ?? "http://127.0.0.1:8000";
  const apiKey = process.env.BACKEND_API_KEY ?? process.env.LRCP_API_KEY;
  const init: RequestInit = {
    cache: "no-store",
    ...(apiKey ? { headers: { "X-API-Key": apiKey } } : {}),
  };

  try {
    const response = await fetch(`${endpoint}/api/v1/traces/${traceId}`, init);
    if (response.status === 404) {
      return { spans: [], error: "Trace not found. Materialize WAL records first." };
    }
    if (!response.ok) {
      return { spans: [], error: `Backend returned HTTP ${response.status}` };
    }
    return { spans: await response.json(), error: null };
  } catch (cause) {
    const message = cause instanceof Error ? cause.message : "unknown error";
    return { spans: [], error: `Cannot reach control API at ${endpoint}: ${message}` };
  }
}

export default async function TraceDetailPage({ params }: { params: Promise<{ traceId: string }> }) {
  const { traceId } = await params;
  const { spans, error } = await loadTrace(traceId);

  return <TraceViewer traceId={traceId} initialSpans={spans} error={error} />;
}
