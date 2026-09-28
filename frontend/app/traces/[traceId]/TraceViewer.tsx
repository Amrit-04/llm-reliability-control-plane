"use client";

import React, { useState, useMemo } from "react";

export type Span = {
  trace_id: string;
  span_id: string;
  parent_span_id?: string | null;
  name: string;
  kind?: number;
  service_name?: string | null;
  project_id?: string | null;
  start_time_unix_nano: number;
  end_time_unix_nano: number;
  duration_ns: number;
  status_code?: number;
  status_message?: string | null;
  scope_name?: string | null;
  scope_version?: string | null;
  trace_state?: string | null;
  flags?: number | null;
  resource_attributes_json?: string;
  attributes_json?: string;
  events_json?: string;
  links_json?: string;
  dropped_attributes_count?: number;
  dropped_events_count?: number;
  dropped_links_count?: number;
  source_file?: string;
  source_offset?: number;
};

type SpanNode = {
  span: Span;
  children: SpanNode[];
  depth: number;
};

function parseJsonSafe(raw: string | undefined): any {
  if (!raw) return {};
  try {
    return JSON.parse(raw);
  } catch {
    return { raw };
  }
}

function parseEventsSafe(raw: string | undefined): Array<{ time_unix_nano: number; name: string; attributes?: Record<string, any> }> {
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function formatDuration(nanoseconds: number): string {
  if (nanoseconds < 1_000) return `${nanoseconds} ns`;
  if (nanoseconds < 1_000_000) return `${(nanoseconds / 1_000).toFixed(2)} µs`;
  if (nanoseconds < 1_000_000_000) return `${(nanoseconds / 1_000_000).toFixed(2)} ms`;
  return `${(nanoseconds / 1_000_000_000).toFixed(3)} s`;
}

function spanKindLabel(kind?: number): string {
  switch (kind) {
    case 1: return "INTERNAL";
    case 2: return "SERVER";
    case 3: return "CLIENT";
    case 4: return "PRODUCER";
    case 5: return "CONSUMER";
    default: return "UNSPECIFIED";
  }
}

function extractGenAiInfo(attributes: Record<string, any>) {
  const model = attributes["gen_ai.request.model"] || attributes["gen_ai.response.model"] || attributes["llm.model"] || attributes["model"];
  const system = attributes["gen_ai.system"] || attributes["llm.system"];
  const promptTokens = attributes["gen_ai.usage.prompt_tokens"] ?? attributes["gen_ai.usage.input_tokens"] ?? attributes["llm.usage.prompt_tokens"];
  const completionTokens = attributes["gen_ai.usage.completion_tokens"] ?? attributes["gen_ai.usage.output_tokens"] ?? attributes["llm.usage.completion_tokens"];
  const totalTokens = attributes["gen_ai.usage.total_tokens"] ?? attributes["llm.usage.total_tokens"] ?? (promptTokens != null && completionTokens != null ? Number(promptTokens) + Number(completionTokens) : undefined);
  const temperature = attributes["gen_ai.request.temperature"] ?? attributes["llm.temperature"];
  const finishReason = attributes["gen_ai.response.finish_reasons"] ?? attributes["llm.finish_reason"];
  const isGenAi = Boolean(model || system || promptTokens !== undefined || attributes["gen_ai.prompt"] || attributes["llm.prompt"]);

  return {
    isGenAi,
    model: model ? String(model) : null,
    system: system ? String(system) : null,
    promptTokens: promptTokens !== undefined ? Number(promptTokens) : null,
    completionTokens: completionTokens !== undefined ? Number(completionTokens) : null,
    totalTokens: totalTokens !== undefined ? Number(totalTokens) : null,
    temperature: temperature !== undefined ? Number(temperature) : null,
    finishReason: finishReason ? String(finishReason) : null,
  };
}

export function TraceViewer({ traceId, initialSpans, error }: { traceId: string; initialSpans: Span[]; error: string | null }) {
  const [selectedSpanId, setSelectedSpanId] = useState<string | null>(initialSpans[0]?.span_id ?? null);
  const [collapsedSpans, setCollapsedSpans] = useState<Record<string, boolean>>({});
  const [activeTab, setActiveTab] = useState<"overview" | "attributes" | "resource" | "events" | "raw">("overview");

  // Trace bounds
  const { minStart, maxEnd, wallClockNs, aggregateNs, errorCount, rootServices, tree, spanMap } = useMemo(() => {
    if (!initialSpans.length) {
      return { minStart: 0, maxEnd: 0, wallClockNs: 0, aggregateNs: 0, errorCount: 0, rootServices: [], tree: [], spanMap: new Map<string, Span>() };
    }

    let min = initialSpans[0].start_time_unix_nano;
    let max = initialSpans[0].end_time_unix_nano;
    let agg = 0;
    let errors = 0;
    const services = new Set<string>();
    const map = new Map<string, Span>();

    for (const span of initialSpans) {
      map.set(span.span_id, span);
      if (span.start_time_unix_nano < min) min = span.start_time_unix_nano;
      if (span.end_time_unix_nano > max) max = span.end_time_unix_nano;
      agg += span.duration_ns;
      if (span.status_code && span.status_code !== 0 && span.status_code !== 1) errors += 1;
      if (span.service_name) services.add(span.service_name);
    }

    // Build hierarchy tree
    const childrenMap = new Map<string, string[]>();
    const rootIds: string[] = [];

    for (const span of initialSpans) {
      const parentId = span.parent_span_id;
      if (!parentId || !map.has(parentId)) {
        rootIds.push(span.span_id);
      } else {
        const existing = childrenMap.get(parentId) ?? [];
        existing.push(span.span_id);
        childrenMap.set(parentId, existing);
      }
    }

    function buildNode(id: string, depth: number): SpanNode {
      const span = map.get(id)!;
      const childIds = childrenMap.get(id) ?? [];
      // Sort children by start time
      childIds.sort((a, b) => (map.get(a)?.start_time_unix_nano ?? 0) - (map.get(b)?.start_time_unix_nano ?? 0));
      return {
        span,
        depth,
        children: childIds.map((cid) => buildNode(cid, depth + 1)),
      };
    }

    // Sort roots by start time
    rootIds.sort((a, b) => (map.get(a)?.start_time_unix_nano ?? 0) - (map.get(b)?.start_time_unix_nano ?? 0));
    const treeNodes = rootIds.map((rid) => buildNode(rid, 0));

    return {
      minStart: min,
      maxEnd: max,
      wallClockNs: Math.max(max - min, 1),
      aggregateNs: agg,
      errorCount: errors,
      rootServices: Array.from(services),
      tree: treeNodes,
      spanMap: map,
    };
  }, [initialSpans]);

  const selectedSpan = selectedSpanId ? spanMap.get(selectedSpanId) : initialSpans[0];
  const selectedAttrs = useMemo(() => parseJsonSafe(selectedSpan?.attributes_json), [selectedSpan]);
  const selectedResourceAttrs = useMemo(() => parseJsonSafe(selectedSpan?.resource_attributes_json), [selectedSpan]);
  const selectedEvents = useMemo(() => parseEventsSafe(selectedSpan?.events_json), [selectedSpan]);
  const selectedGenAi = useMemo(() => extractGenAiInfo(selectedAttrs), [selectedAttrs]);

  const toggleCollapse = (spanId: string, e: React.MouseEvent) => {
    e.stopPropagation();
    setCollapsedSpans((prev) => ({ ...prev, [spanId]: !prev[spanId] }));
  };

  // Flatten visible nodes
  const visibleNodes = useMemo(() => {
    const list: SpanNode[] = [];
    function traverse(node: SpanNode) {
      list.push(node);
      if (!collapsedSpans[node.span.span_id] && node.children.length > 0) {
        for (const child of node.children) {
          traverse(child);
        }
      }
    }
    for (const root of tree) {
      traverse(root);
    }
    return list;
  }, [tree, collapsedSpans]);

  if (error) {
    return (
      <main className="trace-container">
        <header className="trace-header">
          <p className="eyebrow"><a href="/">← Traces</a></p>
          <h1>Trace Not Found</h1>
          <p className="empty" role="alert">{error}</p>
        </header>
      </main>
    );
  }

  return (
    <main className="trace-container">
      <header className="trace-header">
        <div className="header-breadcrumbs">
          <a href="/" className="back-link">← All Traces</a>
          {selectedSpan?.project_id && <span className="project-badge">Project: {selectedSpan.project_id}</span>}
        </div>
        <div className="trace-title-row">
          <h1>Trace <span className="trace-id-highlight">{traceId}</span></h1>
          {errorCount > 0 ? (
            <span className="badge-error">{errorCount} Error{errorCount === 1 ? "" : "s"}</span>
          ) : (
            <span className="badge-success">Healthy</span>
          )}
        </div>

        {/* Trace metrics summary cards */}
        <div className="metrics-grid">
          <div className="metric-card">
            <span className="metric-label">WALL-CLOCK DURATION</span>
            <span className="metric-value">{formatDuration(wallClockNs)}</span>
          </div>
          <div className="metric-card">
            <span className="metric-label">AGGREGATE SPAN TIME</span>
            <span className="metric-value">{formatDuration(aggregateNs)}</span>
          </div>
          <div className="metric-card">
            <span className="metric-label">TOTAL SPANS</span>
            <span className="metric-value">{initialSpans.length}</span>
          </div>
          <div className="metric-card">
            <span className="metric-label">SERVICES</span>
            <span className="metric-value services-tag-list">
              {rootServices.map((s) => (
                <span key={s} className="service-tag">{s}</span>
              ))}
            </span>
          </div>
        </div>
      </header>

      {/* Main Split-View: Waterfall & Inspector */}
      <div className="trace-layout">
        {/* Waterfall Section */}
        <section className="waterfall-panel" aria-label="Trace Timeline and Hierarchy">
          <div className="panel-header">
            <span className="panel-title">Hierarchy & Timeline</span>
            <div className="timeline-scale">
              <span>0 ms</span>
              <span>{formatDuration(wallClockNs / 2)}</span>
              <span>{formatDuration(wallClockNs)}</span>
            </div>
          </div>

          <div className="waterfall-list">
            {visibleNodes.map(({ span, depth, children }) => {
              const isSelected = span.span_id === selectedSpanId;
              const hasChildren = children.length > 0;
              const isCollapsed = Boolean(collapsedSpans[span.span_id]);
              const offsetRatio = Math.max(0, Math.min(1, (span.start_time_unix_nano - minStart) / wallClockNs));
              const widthRatio = Math.max(0.005, Math.min(1 - offsetRatio, span.duration_ns / wallClockNs));
              const offsetPercent = (offsetRatio * 100).toFixed(2);
              const widthPercent = (widthRatio * 100).toFixed(2);
              const isError = span.status_code && span.status_code !== 0 && span.status_code !== 1;
              const attrs = parseJsonSafe(span.attributes_json);
              const genAi = extractGenAiInfo(attrs);

              return (
                <div
                  key={span.span_id}
                  className={`waterfall-row ${isSelected ? "selected" : ""} ${isError ? "has-error" : ""}`}
                  onClick={() => setSelectedSpanId(span.span_id)}
                >
                  {/* Left: Span Tree Column */}
                  <div className="span-tree-col" style={{ paddingLeft: `${depth * 18 + 12}px` }}>
                    {hasChildren ? (
                      <button
                        className="collapse-toggle"
                        onClick={(e) => toggleCollapse(span.span_id, e)}
                        aria-label={isCollapsed ? "Expand span children" : "Collapse span children"}
                      >
                        {isCollapsed ? "▶" : "▼"}
                      </button>
                    ) : (
                      <span className="tree-leaf-dot">•</span>
                    )}

                    <div className="span-meta">
                      <div className="span-name-line">
                        <span className="span-name" title={span.name}>{span.name}</span>
                        {genAi.isGenAi && <span className="badge-genai">GenAI</span>}
                        {genAi.model && <span className="badge-model">{genAi.model}</span>}
                        {isError && <span className="badge-error-small">ERR</span>}
                      </div>
                      <div className="span-service-sub">
                        <span>{span.service_name || "unknown"}</span>
                        <span className="span-id-muted">{span.span_id.slice(0, 8)}</span>
                      </div>
                    </div>
                  </div>

                  {/* Right: Waterfall Bar Column */}
                  <div className="waterfall-bar-col">
                    <div className="waterfall-track">
                      <div
                        className={`waterfall-bar ${isError ? "bar-error" : genAi.isGenAi ? "bar-genai" : "bar-default"}`}
                        style={{
                          left: `${offsetPercent}%`,
                          width: `${widthPercent}%`,
                        }}
                        title={`${span.name}: ${formatDuration(span.duration_ns)} (offset: ${formatDuration(span.start_time_unix_nano - minStart)})`}
                      />
                    </div>
                    <span className="duration-label">{formatDuration(span.duration_ns)}</span>
                  </div>
                </div>
              );
            })}
          </div>
        </section>

        {/* Right Details Inspector Panel */}
        {selectedSpan && (
          <aside className="inspector-panel" aria-label="Span Inspector">
            <div className="inspector-header">
              <h3>Span Details</h3>
              <span className="inspector-span-name">{selectedSpan.name}</span>
              <div className="inspector-badge-row">
                <span className="badge-kind">{spanKindLabel(selectedSpan.kind)}</span>
                <span className="badge-service">{selectedSpan.service_name || "unknown"}</span>
                {selectedGenAi.model && <span className="badge-model">{selectedGenAi.model}</span>}
              </div>
            </div>

            {/* Inspector Tabs */}
            <div className="inspector-tabs">
              <button
                className={`tab-btn ${activeTab === "overview" ? "active" : ""}`}
                onClick={() => setActiveTab("overview")}
              >
                Overview
              </button>
              <button
                className={`tab-btn ${activeTab === "attributes" ? "active" : ""}`}
                onClick={() => setActiveTab("attributes")}
              >
                Attributes ({Object.keys(selectedAttrs).length})
              </button>
              <button
                className={`tab-btn ${activeTab === "resource" ? "active" : ""}`}
                onClick={() => setActiveTab("resource")}
              >
                Resource ({Object.keys(selectedResourceAttrs).length})
              </button>
              <button
                className={`tab-btn ${activeTab === "events" ? "active" : ""}`}
                onClick={() => setActiveTab("events")}
              >
                Events ({selectedEvents.length})
              </button>
              <button
                className={`tab-btn ${activeTab === "raw" ? "active" : ""}`}
                onClick={() => setActiveTab("raw")}
              >
                Raw
              </button>
            </div>

            {/* Tab Content */}
            <div className="inspector-body">
              {activeTab === "overview" && (
                <div className="tab-pane">
                  {/* GenAI Card if applicable */}
                  {selectedGenAi.isGenAi && (
                    <div className="genai-card">
                      <div className="genai-card-title">🤖 GenAI / LLM Metrics</div>
                      <div className="kv-grid">
                        {selectedGenAi.model && (
                          <div className="kv-row"><span className="kv-key">Model</span><span className="kv-val highlight">{selectedGenAi.model}</span></div>
                        )}
                        {selectedGenAi.system && (
                          <div className="kv-row"><span className="kv-key">System</span><span className="kv-val">{selectedGenAi.system}</span></div>
                        )}
                        {selectedGenAi.promptTokens !== null && (
                          <div className="kv-row"><span className="kv-key">Prompt Tokens</span><span className="kv-val">{selectedGenAi.promptTokens.toLocaleString()}</span></div>
                        )}
                        {selectedGenAi.completionTokens !== null && (
                          <div className="kv-row"><span className="kv-key">Completion Tokens</span><span className="kv-val">{selectedGenAi.completionTokens.toLocaleString()}</span></div>
                        )}
                        {selectedGenAi.totalTokens !== null && (
                          <div className="kv-row"><span className="kv-key">Total Tokens</span><span className="kv-val highlight">{selectedGenAi.totalTokens.toLocaleString()}</span></div>
                        )}
                        {selectedGenAi.temperature !== null && (
                          <div className="kv-row"><span className="kv-key">Temperature</span><span className="kv-val">{selectedGenAi.temperature}</span></div>
                        )}
                        {selectedGenAi.finishReason && (
                          <div className="kv-row"><span className="kv-key">Finish Reason</span><span className="kv-val">{selectedGenAi.finishReason}</span></div>
                        )}
                      </div>
                    </div>
                  )}

                  <div className="section-title">Timing & Identity</div>
                  <div className="kv-grid">
                    <div className="kv-row"><span className="kv-key">Span ID</span><code className="kv-val">{selectedSpan.span_id}</code></div>
                    <div className="kv-row"><span className="kv-key">Parent Span ID</span><code className="kv-val">{selectedSpan.parent_span_id || "None (Root)"}</code></div>
                    <div className="kv-row"><span className="kv-key">Duration</span><span className="kv-val">{formatDuration(selectedSpan.duration_ns)}</span></div>
                    <div className="kv-row"><span className="kv-key">Relative Start</span><span className="kv-val">+{formatDuration(selectedSpan.start_time_unix_nano - minStart)}</span></div>
                    <div className="kv-row"><span className="kv-key">Start Time (ns)</span><span className="kv-val">{selectedSpan.start_time_unix_nano}</span></div>
                    <div className="kv-row"><span className="kv-key">End Time (ns)</span><span className="kv-val">{selectedSpan.end_time_unix_nano}</span></div>
                    <div className="kv-row"><span className="kv-key">Status Code</span><span className={`kv-val ${selectedSpan.status_code && selectedSpan.status_code !== 0 && selectedSpan.status_code !== 1 ? "val-error" : ""}`}>{selectedSpan.status_code ?? 0}</span></div>
                    {selectedSpan.status_message && (
                      <div className="kv-row"><span className="kv-key">Status Msg</span><span className="kv-val val-error">{selectedSpan.status_message}</span></div>
                    )}
                    {selectedSpan.scope_name && (
                      <div className="kv-row"><span className="kv-key">Instrumentation</span><span className="kv-val">{selectedSpan.scope_name} {selectedSpan.scope_version ? `v${selectedSpan.scope_version}` : ""}</span></div>
                    )}
                  </div>
                </div>
              )}

              {activeTab === "attributes" && (
                <div className="tab-pane">
                  {Object.keys(selectedAttrs).length === 0 ? (
                    <p className="empty-tab">No span attributes recorded.</p>
                  ) : (
                    <div className="kv-grid">
                      {Object.entries(selectedAttrs).map(([key, value]) => (
                        <div key={key} className="kv-row">
                          <span className="kv-key" title={key}>{key}</span>
                          <span className="kv-val">{typeof value === "object" ? JSON.stringify(value) : String(value)}</span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}

              {activeTab === "resource" && (
                <div className="tab-pane">
                  {Object.keys(selectedResourceAttrs).length === 0 ? (
                    <p className="empty-tab">No resource attributes recorded.</p>
                  ) : (
                    <div className="kv-grid">
                      {Object.entries(selectedResourceAttrs).map(([key, value]) => (
                        <div key={key} className="kv-row">
                          <span className="kv-key" title={key}>{key}</span>
                          <span className="kv-val">{typeof value === "object" ? JSON.stringify(value) : String(value)}</span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}

              {activeTab === "events" && (
                <div className="tab-pane">
                  {selectedEvents.length === 0 ? (
                    <p className="empty-tab">No events recorded on this span.</p>
                  ) : (
                    <div className="events-timeline">
                      {selectedEvents.map((evt, idx) => (
                        <div key={idx} className="event-item">
                          <div className="event-header">
                            <span className="event-name">{evt.name}</span>
                            <span className="event-time">+{formatDuration(evt.time_unix_nano - selectedSpan.start_time_unix_nano)}</span>
                          </div>
                          {evt.attributes && Object.keys(evt.attributes).length > 0 && (
                            <div className="event-attributes">
                              {Object.entries(evt.attributes).map(([k, v]) => (
                                <div key={k} className="kv-row-small">
                                  <span className="kv-key-small">{k}:</span>
                                  <span className="kv-val-small">{String(v)}</span>
                                </div>
                              ))}
                            </div>
                          )}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}

              {activeTab === "raw" && (
                <div className="tab-pane">
                  <pre className="raw-json-block">{JSON.stringify(selectedSpan, null, 2)}</pre>
                </div>
              )}
            </div>
          </aside>
        )}
      </div>
    </main>
  );
}
