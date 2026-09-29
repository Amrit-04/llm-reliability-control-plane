type Trace = {
  trace_id: string;
  start_time_unix_nano: number;
  end_time_unix_nano?: number;
  wall_clock_duration_ns?: number;
  aggregate_span_duration_ns: number;
  span_count: number;
  service_name: string | null;
  services?: string[];
  project_id?: string | null;
  error_count?: number;
};

type HealthStatus = {
  status: string;
  wal_directory: string;
  parquet_directory: string;
  sqlite_database: string;
  wal_files_count: number;
  parquet_files_count: number;
  committed_parquet_files_count: number;
};

async function getTraces(): Promise<{ items: Trace[]; error: string | null }> {
  const endpoint = process.env.BACKEND_URL ?? "http://127.0.0.1:8000";
  const apiKey = process.env.BACKEND_API_KEY ?? process.env.LRCP_API_KEY;
  const init: RequestInit = {
    cache: "no-store",
    ...(apiKey ? { headers: { "X-API-Key": apiKey } } : {}),
  };

  try {
    const response = await fetch(`${endpoint}/api/v1/traces`, init);
    if (!response.ok) {
      return { items: [], error: `Backend returned HTTP ${response.status}` };
    }
    return { items: await response.json(), error: null };
  } catch (cause) {
    const message = cause instanceof Error ? cause.message : "unknown error";
    return { items: [], error: `Cannot reach control API at ${endpoint}: ${message}` };
  }
}

async function getHealth(): Promise<HealthStatus | null> {
  const endpoint = process.env.BACKEND_URL ?? "http://127.0.0.1:8000";
  const apiKey = process.env.BACKEND_API_KEY ?? process.env.LRCP_API_KEY;
  const init: RequestInit = {
    cache: "no-store",
    ...(apiKey ? { headers: { "X-API-Key": apiKey } } : {}),
  };

  try {
    const response = await fetch(`${endpoint}/api/v1/system/status`, init);
    if (!response.ok) {
      // Fallback to healthz if system/status fails (e.g. auth required but no key configured yet)
      const fallback = await fetch(`${endpoint}/healthz`, { cache: "no-store" });
      if (fallback.ok) return { status: "ok" } as HealthStatus;
      return null;
    }
    const data = await response.json();
    return {
      status: data.status,
      wal_directory: "",
      parquet_directory: "",
      sqlite_database: "",
      wal_files_count: data.wal?.segment_count ?? 0,
      parquet_files_count: data.parquet?.file_count ?? 0,
      committed_parquet_files_count: data.parquet?.file_count ?? 0,
    };
  } catch {
    return null;
  }
}

function formatDuration(nanoseconds?: number): string {
  if (nanoseconds === undefined || nanoseconds === null) return "0 ms";
  if (nanoseconds < 1_000) return `${nanoseconds} ns`;
  if (nanoseconds < 1_000_000) return `${(nanoseconds / 1_000).toFixed(2)} µs`;
  if (nanoseconds < 1_000_000_000) return `${(nanoseconds / 1_000_000).toFixed(2)} ms`;
  return `${(nanoseconds / 1_000_000_000).toFixed(3)} s`;
}

export default async function TraceExplorer() {
  const [{ items, error }, health] = await Promise.all([getTraces(), getHealth()]);

  const totalErrors = items.reduce((acc, t) => acc + (t.error_count ?? 0), 0);
  const totalSpans = items.reduce((acc, t) => acc + t.span_count, 0);

  return (
    <main className="dashboard-container">
      <header className="dashboard-header">
        <div className="header-top">
          <p className="eyebrow">LOCAL-FIRST TELEMETRY CONTROL PLANE</p>
          <div className="header-badges">
            <span className="badge-local">Local-First</span>
            {health && <span className="badge-online">System Ready</span>}
          </div>
        </div>
        <h1>LLM Reliability Control Plane</h1>
        <p className="dashboard-subtitle">
          Durable WAL ingestion via C++ gateway, zero-copy DuckDB analytics, and full OTLP fidelity.
        </p>
      </header>

      {/* Overview Stats */}
      <section className="metrics-grid">
        <div className="metric-card">
          <span className="metric-label">MATERIALIZED TRACES</span>
          <span className="metric-value">{items.length}</span>
        </div>
        <div className="metric-card">
          <span className="metric-label">TOTAL SPANS</span>
          <span className="metric-value">{totalSpans.toLocaleString()}</span>
        </div>
        <div className="metric-card">
          <span className="metric-label">PARQUET FILES</span>
          <span className="metric-value">{health?.committed_parquet_files_count ?? "-"}</span>
        </div>
        <div className="metric-card">
          <span className="metric-label">ACTIVE WAL FILES</span>
          <span className="metric-value">{health?.wal_files_count ?? "-"}</span>
        </div>
      </section>

      {error && <p className="empty" role="alert">{error}</p>}

      {/* Traces Table */}
      <section className="table-card" aria-label="Traces">
        <div className="table-card-header">
          <h2>Traces ({items.length})</h2>
          <span className="table-hint">Click a trace to view span hierarchy & waterfall timeline</span>
        </div>

        <div className="trace-table">
          <div className="table-row heading">
            <span>Trace ID</span>
            <span>Service</span>
            <span>Spans</span>
            <span>Wall-Clock Duration</span>
            <span>Status</span>
          </div>

          {items.map((trace) => {
            const hasError = (trace.error_count ?? 0) > 0;
            const services = trace.services?.length ? trace.services : [trace.service_name ?? "unknown"];

            return (
              <a className={`table-row ${hasError ? "row-error" : ""}`} href={`/traces/${trace.trace_id}`} key={trace.trace_id}>
                <div className="trace-id-cell">
                  <code title={trace.trace_id}>{trace.trace_id || "(missing trace id)"}</code>
                  {trace.project_id && <span className="project-tag">{trace.project_id}</span>}
                </div>
                <div className="services-cell">
                  {services.map((svc) => (
                    <span key={svc} className="service-tag">{svc}</span>
                  ))}
                </div>
                <span className="spans-cell">{trace.span_count}</span>
                <span className="duration-cell">{formatDuration(trace.wall_clock_duration_ns ?? trace.aggregate_span_duration_ns)}</span>
                <span className="status-cell">
                  {hasError ? (
                    <span className="badge-error-small">{trace.error_count} ERR</span>
                  ) : (
                    <span className="badge-ok-small">OK</span>
                  )}
                </span>
              </a>
            );
          })}

          {!items.length && !error && (
            <div className="empty-table-state">
              <p className="empty-title">No materialized traces yet</p>
              <p className="empty-desc">Send OTLP traces to the gateway and trigger materialization via <code>POST /api/v1/materialize</code>.</p>
            </div>
          )}
        </div>
      </section>
    </main>
  );
}
