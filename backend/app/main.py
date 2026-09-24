"""
FastAPI Application Factory for LRCP Backend

This module provides the main HTTP API for the LLM Reliability Control Plane.
It exposes:
  - Health checks
  - Manual and automatic materialization triggers
  - Trace and span query endpoints
  - Project management

The app uses a lifespan context manager to handle:
  - SQLite schema initialization
  - Background materialization worker startup/shutdown
  - Graceful cleanup on shutdown

Architecture:
    HTTP → FastAPI → DuckDB (read Parquet) OR materialize() (write Parquet)
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import logging
from typing import Annotated
import sqlite3

import duckdb
from fastapi import FastAPI, HTTPException, Query, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .control import initialize_control_store
from .materializer import materialize
from .settings import Settings

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("lrcp.backend")


class ProjectCreate(BaseModel):
    """Request schema for creating a new project."""
    id: str = Field(..., min_length=1, max_length=64, description="Unique project identifier")
    name: str = Field(..., min_length=1, max_length=200, description="Human-readable project name")


def create_app(settings: Settings | None = None) -> FastAPI:
    """
    Create and configure the FastAPI application.

    Args:
        settings: Optional Settings instance. If None, loads from environment.

    Returns:
        Configured FastAPI application with all routes and lifecycle hooks.
    """
    resolved = settings or Settings.from_environment()

    logger.info(
        f"Initializing LRCP backend with data_dir={resolved.data_dir}, "
        f"materialize_interval={resolved.materialize_interval_seconds}s"
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        """
        Application lifespan manager.

        Startup:
          - Initialize SQLite schema
          - Create necessary directories
          - Start background materialization worker (if enabled)

        Shutdown:
          - Cancel background worker
          - Wait for graceful cancellation
        """
        logger.info("Starting application lifespan")
        try:
            initialize_control_store(resolved)
            resolved.wal_dir.mkdir(parents=True, exist_ok=True)
            resolved.parquet_dir.mkdir(parents=True, exist_ok=True)
            logger.info("Control store initialized, directories created")
        except Exception as error:
            logger.error(f"Failed to initialize application: {error}")
            raise

        task = None
        if resolved.materialize_interval_seconds > 0:
            logger.info(
                f"Starting background materialization worker "
                f"(interval: {resolved.materialize_interval_seconds}s)"
            )

            async def _materialize_loop():
                """Background task: materialize WAL records on a schedule."""
                iteration = 0
                while True:
                    iteration += 1
                    try:
                        count = materialize(resolved)
                        if count > 0:
                            logger.debug(f"Background iteration {iteration}: materialized {count} spans")
                    except Exception as error:
                        logger.warning(f"Auto-materialization error (iteration {iteration}): {error}")
                    await asyncio.sleep(resolved.materialize_interval_seconds)

            task = asyncio.create_task(_materialize_loop())
        else:
            logger.info("Background materialization disabled (interval = 0)")

        try:
            logger.info("Application ready")
            yield
        finally:
            logger.info("Shutting down application")
            if task:
                logger.info("Cancelling background materialization worker")
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    logger.info("Background worker cancelled cleanly")
            logger.info("Application shutdown complete")

    application = FastAPI(
        title="LLM Reliability Control Plane",
        description="Local-first observability for LLM applications",
        version="0.1.0",
        lifespan=lifespan,
    )

    @application.exception_handler(Exception)
    async def global_exception_handler(request, exc: Exception):
        """Catch-all exception handler to prevent leaking internal errors."""
        logger.error(f"Unhandled exception on {request.method} {request.url.path}: {exc}", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Internal server error"},
        )

    @application.get("/healthz", tags=["Health"])
    def health() -> dict[str, str]:
        """
        Health check endpoint.

        Returns 200 OK if the service is running. Does not verify database
        connectivity or WAL availability — it's a liveness check only.

        Returns:
            {"status": "ok"}
        """
        return {"status": "ok"}

    @application.post("/api/v1/materialize", tags=["Materialization"])
    def run_materializer() -> dict[str, int]:
        """
        Manually trigger WAL → Parquet materialization.

        This endpoint is useful for:
          - On-demand materialization (when auto-materialize is disabled)
          - Testing and debugging
          - Forcing immediate consistency after sending OTLP data

        Returns:
            {"materialized_spans": <count>} where count is the number of
            span rows written to Parquet in this invocation (0 if nothing new).

        Raises:
            HTTPException 500: If materialization fails (corrupt WAL, disk full, etc.)
        """
        try:
            count = materialize(resolved)
            logger.info(f"Manual materialization: {count} spans processed")
            return {"materialized_spans": count}
        except Exception as error:
            logger.error(f"Manual materialization failed: {error}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Materialization failed: {str(error)}"
            )

    @application.get("/api/v1/traces", tags=["Traces"])
    def list_traces(limit: Annotated[int, Query(ge=1, le=1000)] = 100) -> list[dict]:
        """
        List recent traces with aggregate metrics.

        This endpoint scans all Parquet files and groups spans by trace_id,
        computing:
          - Earliest span start time (trace start)
          - Sum of all span durations (total time spent, not wall-clock)
          - Span count
          - Service name (max — arbitrary if multiple services)

        Args:
            limit: Maximum number of traces to return (default 100, max 1000).

        Returns:
            List of trace summary dicts, sorted by start time (newest first).
            Empty list if no materialized data exists.

        Raises:
            HTTPException 500: If DuckDB query fails.
        """
        try:
            files = list(resolved.parquet_dir.glob("*.parquet"))
            if not files:
                logger.debug("No Parquet files available for trace list query")
                return []

            logger.debug(f"Querying {len(files)} Parquet file(s) for trace list (limit={limit})")

            query = """
              SELECT trace_id, min(start_time_unix_nano) AS start_time_unix_nano,
                     sum(duration_ns) AS aggregate_span_duration_ns, count(*) AS span_count,
                     max(service_name) AS service_name
              FROM read_parquet(?)
              GROUP BY trace_id ORDER BY start_time_unix_nano DESC LIMIT ?
            """
            with duckdb.connect() as connection:
                cursor = connection.execute(query, [[str(file) for file in files], limit])
                names = [column[0] for column in cursor.description]
                results = [dict(zip(names, row)) for row in cursor.fetchall()]

            logger.debug(f"Trace list query returned {len(results)} trace(s)")
            return results

        except Exception as error:
            logger.error(f"Trace list query failed: {error}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to query traces"
            )

    @application.get("/api/v1/traces/{trace_id}", tags=["Traces"])
    def get_trace(trace_id: str) -> list[dict]:
        """
        Get all spans for a specific trace.

        Returns full span details for one trace, ordered by start time. This
        includes all normalized fields: span IDs, names, durations, status,
        attributes, etc.

        Args:
            trace_id: Hex-encoded trace ID (32 characters, lowercase).

        Returns:
            List of span dicts, ordered chronologically. Each dict contains
            all fields from the normalized span schema (see ARCHITECTURE.md).

        Raises:
            HTTPException 404: If the trace ID is not found in any Parquet file.
            HTTPException 500: If DuckDB query fails.
        """
        try:
            files = list(resolved.parquet_dir.glob("*.parquet"))
            if not files:
                logger.debug(f"No Parquet files available for trace {trace_id}")
                raise HTTPException(status_code=404, detail="trace not found")

            logger.debug(f"Querying {len(files)} Parquet file(s) for trace {trace_id}")

            with duckdb.connect() as connection:
                cursor = connection.execute(
                    "SELECT * FROM read_parquet(?) WHERE trace_id = ? ORDER BY start_time_unix_nano",
                    [[str(file) for file in files], trace_id],
                )
                names = [column[0] for column in cursor.description]
                rows = [dict(zip(names, row)) for row in cursor.fetchall()]

            if not rows:
                logger.debug(f"Trace {trace_id} not found in materialized data")
                raise HTTPException(status_code=404, detail="trace not found")

            logger.debug(f"Trace {trace_id}: found {len(rows)} span(s)")
            return rows

        except HTTPException:
            raise
        except Exception as error:
            logger.error(f"Trace detail query failed for {trace_id}: {error}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to query trace"
            )

    @application.post("/api/v1/projects", status_code=201, tags=["Projects"])
    def create_project(project: ProjectCreate) -> ProjectCreate:
        """
        Create a new project.

        Projects are logical containers for traces, enabling multi-tenant
        isolation and organization. Currently, project IDs are stored but not
        yet enforced on trace ingestion (planned for v0.2).

        Args:
            project: Project creation request with id and name.

        Returns:
            The created project (echoed back).

        Raises:
            HTTPException 409: If a project with this ID already exists.
            HTTPException 500: If database write fails.
        """
        try:
            logger.info(f"Creating project: id={project.id}, name={project.name}")
            with sqlite3.connect(resolved.sqlite_path) as connection:
                connection.execute(
                    "INSERT INTO projects(id, name) VALUES (?, ?)",
                    (project.id, project.name),
                )
                connection.commit()
            logger.info(f"Project {project.id} created successfully")
            return project
        except sqlite3.IntegrityError as error:
            logger.warning(f"Project creation failed: {project.id} already exists")
            raise HTTPException(status_code=409, detail="project already exists") from error
        except Exception as error:
            logger.error(f"Project creation failed for {project.id}: {error}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to create project"
            )

    @application.get("/api/v1/analytics/overview", tags=["Analytics"])
    def analytics_overview() -> dict:
        """
        Get high-level system observability metrics.

        Aggregates across all materialized traces to provide:
          - Total number of traces collected
          - Total number of spans
          - Total input and output tokens (from gen_ai.* attributes)
          - List of unique services
          - Overall error rate (spans with non-OK status)

        Returns:
            Dictionary with overview metrics. Empty-safe: returns sensible defaults
            if no Parquet files exist yet.

        Raises:
            HTTPException 500: If DuckDB query fails unexpectedly.
        """
        try:
            files = list(resolved.parquet_dir.glob("*.parquet"))
            if not files:
                logger.debug("No Parquet files available for analytics overview")
                return {
                    "total_traces": 0,
                    "total_spans": 0,
                    "total_input_tokens": 0,
                    "total_output_tokens": 0,
                    "unique_services": [],
                    "error_rate": 0.0,
                }

            logger.debug(f"Computing analytics overview from {len(files)} Parquet file(s)")

            with duckdb.connect() as connection:
                # Overall counts and error rate
                summary = connection.execute(
                    """
                    SELECT
                      COUNT(DISTINCT trace_id) AS total_traces,
                      COUNT(*) AS total_spans,
                      SUM(CAST(
                        json_extract_string(attributes_json, '$.\"gen_ai.usage.input_tokens\"')
                        AS BIGINT
                      )) AS total_input_tokens,
                      SUM(CAST(
                        json_extract_string(attributes_json, '$.\"gen_ai.usage.output_tokens\"')
                        AS BIGINT
                      )) AS total_output_tokens,
                      SUM(CASE WHEN status_code != 0 THEN 1 ELSE 0 END) AS error_spans
                    FROM read_parquet(?)
                    """,
                    [[str(file) for file in files]],
                ).fetchone()

                # Unique services
                services = connection.execute(
                    """
                    SELECT DISTINCT service_name FROM read_parquet(?)
                    WHERE service_name IS NOT NULL AND service_name != ''
                    """,
                    [[str(file) for file in files]],
                ).fetchall()

                total_spans = summary[1] or 0
                error_rate = (summary[4] or 0) / total_spans if total_spans > 0 else 0.0

                return {
                    "total_traces": summary[0] or 0,
                    "total_spans": total_spans,
                    "total_input_tokens": summary[2] or 0,
                    "total_output_tokens": summary[3] or 0,
                    "unique_services": [s[0] for s in services],
                    "error_rate": round(error_rate, 4),
                }

        except Exception as error:
            logger.error(f"Analytics overview query failed: {error}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to compute analytics overview"
            )

    @application.get("/api/v1/analytics/models", tags=["Analytics"])
    def analytics_models() -> list[dict]:
        """
        Get per-model observability metrics from GenAI semantic conventions.

        Aggregates metrics by gen_ai.request.model attribute, computing:
          - Request count (number of spans per model)
          - Input/output token totals
          - Average latency and p95 latency
          - Error count and error rate

        Returns:
            List of model metrics dicts sorted by request count (descending).
            Empty list if no Parquet files exist or no GenAI spans are found.

        Raises:
            HTTPException 500: If DuckDB query fails.
        """
        try:
            files = list(resolved.parquet_dir.glob("*.parquet"))
            if not files:
                logger.debug("No Parquet files available for model analytics")
                return []

            logger.debug(f"Computing model analytics from {len(files)} Parquet file(s)")

            with duckdb.connect() as connection:
                results = connection.execute(
                    """
                    SELECT
                      json_extract_string(attributes_json, '$.\"gen_ai.request.model\"') AS model,
                      COUNT(*) AS request_count,
                      SUM(CAST(
                        json_extract_string(attributes_json, '$.\"gen_ai.usage.input_tokens\"')
                        AS BIGINT
                      )) AS total_input_tokens,
                      SUM(CAST(
                        json_extract_string(attributes_json, '$.\"gen_ai.usage.output_tokens\"')
                        AS BIGINT
                      )) AS total_output_tokens,
                      AVG(duration_ns) AS avg_latency_ns,
                      approx_quantile(duration_ns, 0.95) AS p95_latency_ns,
                      SUM(CASE WHEN status_code != 0 THEN 1 ELSE 0 END) AS error_count
                    FROM read_parquet(?)
                    WHERE json_extract_string(attributes_json, '$.\"gen_ai.request.model\"') IS NOT NULL
                    GROUP BY model
                    ORDER BY request_count DESC
                    """,
                    [[str(file) for file in files]],
                ).fetchall()

                rows = []
                for row in results:
                    request_count = row[1] or 0
                    error_count = row[6] or 0
                    rows.append({
                        "model": row[0],
                        "request_count": request_count,
                        "total_input_tokens": row[2] or 0,
                        "total_output_tokens": row[3] or 0,
                        "avg_latency_ms": round((row[4] or 0) / 1_000_000, 2),
                        "p95_latency_ms": round((row[5] or 0) / 1_000_000, 2),
                        "error_count": error_count,
                        "error_rate": round(error_count / request_count, 4) if request_count > 0 else 0.0,
                    })

                return rows

        except Exception as error:
            logger.error(f"Model analytics query failed: {error}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to compute model analytics"
            )

    return application


app = create_app()
