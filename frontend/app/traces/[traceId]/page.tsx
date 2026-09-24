type Span = {
  trace_id: string;
  span_id: string;
  parent_span_id: string;
  name: string;
  service_name: string | null;
  start_time_unix_nano: number;
  end_time_unix_nano: number;
  duration_ns: number;
  status_code: number;
  status_message: string;
  attributes_json: string;
};

async function loadTrace(traceId: string): Promise<{ spans: Span[]; error: string | null }> {
  const endpoint = process.env.BACKEND_URL ?? "http://127.0.0.1:8000";
  try {
    const response = await fetch(`${endpoint}/api/v1/traces/${traceId}`, { cache: "no-store" });
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

function milliseconds(nanoseconds: number) {
  return `${(nanoseconds / 1_000_000).toFixed(2)} ms`;
}

export default async function TraceDetail({ params }: { params: Promise<{ traceId: string }> }) {
  const { traceId } = await params;
  const { spans, error } = await loadTrace(traceId);
  return (
    <main>
      <header>
        <p className="eyebrow"><a href="/">← Traces</a></p>
        <h1>Span tree</h1>
        <p>Trace <code>{traceId}</code></p>
      </header>
      {error && <p className="empty" role="alert">{error}</p>}
      {!error && (
        <section className="table" aria-label="Spans">
          <div className="row heading">
            <span>Span</span>
            <span>Parent</span>
            <span>Status</span>
            <span>Duration</span>
          </div>
          {spans.map((span) => (
            <div className="row" key={span.span_id}>
              <span>
                <code title={span.span_id}>{span.name || span.span_id}</code>
                <br />
                <small>{span.service_name ?? "unknown"}</small>
              </span>
              <code title={span.parent_span_id}>{span.parent_span_id || "root"}</code>
              <span>{span.status_message || span.status_code}</span>
              <span>{milliseconds(span.duration_ns)}</span>
            </div>
          ))}
        </section>
      )}
    </main>
  );
}
