type Trace = {
  trace_id: string;
  start_time_unix_nano: number;
  aggregate_span_duration_ns: number;
  span_count: number;
  service_name: string | null;
};

async function traces(): Promise<{ items: Trace[]; error: string | null }> {
  const endpoint = process.env.BACKEND_URL ?? "http://127.0.0.1:8000";
  try {
    const response = await fetch(`${endpoint}/api/v1/traces`, { cache: "no-store" });
    if (!response.ok) {
      return { items: [], error: `Backend returned HTTP ${response.status}` };
    }
    return { items: await response.json(), error: null };
  } catch (cause) {
    const message = cause instanceof Error ? cause.message : "unknown error";
    return { items: [], error: `Cannot reach control API at ${endpoint}: ${message}` };
  }
}

function milliseconds(nanoseconds: number) {
  return `${(nanoseconds / 1_000_000).toFixed(2)} ms`;
}

export default async function TraceExplorer() {
  const { items, error } = await traces();
  return (
    <main>
      <header>
        <p className="eyebrow">LOCAL-FIRST · EXPERIMENTAL</p>
        <h1>Trace explorer</h1>
        <p>Traces materialized from the local WAL into Parquet. This list is not a live tail of the gateway.</p>
      </header>
      <section className="status">
        <strong>{items.length}</strong> trace{items.length === 1 ? "" : "s"} currently available.
        Run <code>POST /api/v1/materialize</code> after sending OTLP data.
      </section>
      {error && <p className="empty" role="alert">{error}</p>}
      <section className="table" aria-label="Traces">
        <div className="row heading">
          <span>Trace</span>
          <span>Service</span>
          <span>Spans</span>
          <span>Aggregate duration</span>
        </div>
        {items.map((trace) => (
          <a className="row" href={`/traces/${trace.trace_id}`} key={trace.trace_id}>
            <code title={trace.trace_id}>{trace.trace_id || "(missing trace id)"}</code>
            <span>{trace.service_name ?? "unknown"}</span>
            <span>{trace.span_count}</span>
            <span>{milliseconds(trace.aggregate_span_duration_ns)}</span>
          </a>
        ))}
        {!items.length && !error && <p className="empty">No materialized traces yet.</p>}
      </section>
    </main>
  );
}
