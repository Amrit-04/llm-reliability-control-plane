"""
FastAPI Application Factory for LRCP Backend

This module provides the main HTTP API for the LLM Reliability Control Plane.
It exposes:
  - Health checks & system status / observability
  - Manual and automatic materialization triggers
  - Small-file Parquet compaction & WAL retention cleanup
  - Trace and span query endpoints with proper duration & service aggregation
  - Project management & tenant isolation
  - GenAI model analytics with robust token extraction and duration metrics
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import logging
from pathlib import Path
from typing import Annotated, Any
import sqlite3

import duckdb
from fastapi import FastAPI, Header, HTTPException, Query, Security, status, Depends
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field

from .control import initialize_control_store
from .materializer import (
    materialize,
    compact_parquet_files,
    cleanup_materialized_wal_files,
    cleanup_orphaned_parquet_files,
)
from .settings import Settings

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("lrcp.backend")

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


class ProjectCreate(BaseModel):
    """Request schema for creating a new project."""
    id: str = Field(..., min_length=1, max_length=64, description="Unique project identifier")
    name: str = Field(..., min_length=1, max_length=200, description="Human-readable project name")
    api_key: str | None = Field(None, max_length=128, description="Optional per-project API key")


class ProjectResponse(BaseModel):
    id: str
    name: str
    created_at: str | None = None


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
        f"materialize_interval={resolved.materialize_interval_seconds}s, "
        f"require_auth={resolved.require_auth}"
    )

    def verify_auth(
        x_api_key: str | None = Security(api_key_header),
        authorization: str | None = Header(None),
    ) -> None:
        """Enforce API authentication if require_auth or global api_key is configured."""
        if not resolved.require_auth and not resolved.api_key:
            return

        token = x_api_key
        if not token and authorization and authorization.startswith("Bearer "):
            token = authorization[7:].strip()

        expected = resolved.api_key
        if expected and token != expected:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing API key",
                headers={"WWW-Authenticate": "ApiKey"},
            )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        """
        Application lifespan manager.
        """
        logger.info("Starting application lifespan")
        try:
            initialize_control_store(resolved)
            resolved.wal_dir.mkdir(parents=True, exist_ok=True)
            resolved.parquet_dir.mkdir(parents=True, exist_ok=True)
            # Clean up orphaned files on startup
            cleanup_orphaned_parquet_files(resolved)
            logger.info("Control store initialized, directories prepared, orphans reconciled")
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
                """Background task: materialize WAL records on a schedule in thread worker."""
                iteration = 0
                while True:
                    iteration += 1
                    try:
                        count = await asyncio.to_thread(materialize, resolved)
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
        description="Local-first observability and control plane for LLM applications",
        version="0.2.0",
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
        """Health check liveness endpoint."""
        return {"status": "ok"}

    @application.get("/api/v1/system/status", tags=["System"], dependencies=[Depends(verify_auth)])
    def system_status() -> dict[str, Any]:
        """
        Get system health, WAL lifecycle metrics, Parquet statistics, and corruption diagnostics.
        """
        try:
            wal_files = list(resolved.wal_dir.glob("*.wal"))
            total_wal_bytes = sum(f.stat().st_size for f in wal_files)
            parquet_files = list(resolved.parquet_dir.glob("*.parquet"))
            total_parquet_bytes = sum(f.stat().st_size for f in parquet_files)

            with sqlite3.connect(resolved.sqlite_path) as connection:
                corruptions = [
                    {"source_file": row[0], "offset": row[1], "reason": row[2], "detected_at": row[3]}
                    for row in connection.execute(
                        "SELECT source_file, offset, reason, detected_at FROM wal_corruption_events ORDER BY id DESC LIMIT 10"
                    ).fetchall()
                ]
                total_corruptions = connection.execute(
                    "SELECT COUNT(*) FROM wal_corruption_events"
                ).fetchone()[0]

                materialized_offsets_count = connection.execute(
                    "SELECT COUNT(*) FROM materialized_wal_offsets WHERE is_fully_processed = 1"
                ).fetchone()[0]

                projects_count = connection.execute(
                    "SELECT COUNT(*) FROM projects"
                ).fetchone()[0]

            return {
                "status": "healthy" if total_corruptions == 0 else "degraded",
                "wal": {
                    "segment_count": len(wal_files),
                    "total_bytes": total_wal_bytes,
                    "fully_materialized_segments": materialized_offsets_count,
                },
                "parquet": {
                    "file_count": len(parquet_files),
                    "total_bytes": total_parquet_bytes,
                },
                "diagnostics": {
                    "total_corruption_events": total_corruptions,
                    "recent_corruptions": corruptions,
                },
                "projects_count": projects_count,
            }
        except Exception as error:
            logger.error(f"Failed to fetch system status: {error}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve system status",
            )

    @application.post("/api/v1/materialize", tags=["Materialization"], dependencies=[Depends(verify_auth)])
    async def run_materializer() -> dict[str, int]:
        """
        Manually trigger WAL → Parquet materialization off the event loop.
        """
        try:
            count = await asyncio.to_thread(materialize, resolved)
            logger.info(f"Manual materialization: {count} spans processed")
            return {"materialized_spans": count}
        except Exception as error:
            logger.error(f"Manual materialization failed: {error}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Materialization failed: {str(error)}"
            )

    @application.post("/api/v1/admin/compact", tags=["Admin"], dependencies=[Depends(verify_auth)])
    async def run_compaction() -> dict[str, Any]:
        """
        Compact small Parquet files into consolidated files.
        """
        try:
            compacted_rows = await asyncio.to_thread(compact_parquet_files, resolved)
            return {"compacted_rows": compacted_rows}
        except Exception as error:
            logger.error(f"Compaction failed: {error}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Compaction failed: {str(error)}"
            )

    @application.post("/api/v1/admin/wal-cleanup", tags=["Admin"], dependencies=[Depends(verify_auth)])
    async def run_wal_cleanup() -> dict[str, Any]:
        """
        Trigger safe cleanup of sealed and materialized WAL segments.
        """
        try:
            deleted = await asyncio.to_thread(cleanup_materialized_wal_files, resolved)
            return {"deleted_segments": deleted, "count": len(deleted)}
        except Exception as error:
            logger.error(f"WAL cleanup failed: {error}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"WAL cleanup failed: {str(error)}"
            )

    @application.get("/api/v1/traces", tags=["Traces"], dependencies=[Depends(verify_auth)])
    def list_traces(
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
        project_id: str | None = None,
        service: str | None = None,
    ) -> list[dict]:
        """
        List recent traces with distinct wall-clock duration, aggregate duration, and service hierarchy.
        """
        try:
            files = list(resolved.parquet_dir.glob("*.parquet"))
            if not files:
                logger.debug("No Parquet files available for trace list query")
                return []

            where_clauses = []
            params: list[Any] = [[str(file) for file in files]]

            if project_id:
                where_clauses.append("project_id = ?")
                params.append(project_id)
            if service:
                where_clauses.append("service_name = ?")
                params.append(service)

            where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
            params.append(limit)

            query = f"""
              SELECT
                trace_id,
                min(start_time_unix_nano) AS start_time_unix_nano,
                max(end_time_unix_nano) AS end_time_unix_nano,
                max(end_time_unix_nano) - min(start_time_unix_nano) AS wall_clock_duration_ns,
                sum(duration_ns) AS aggregate_span_duration_ns,
                count(*) AS span_count,
                max(service_name) AS service_name,
                list_distinct(list(service_name)) AS services,
                max(project_id) AS project_id,
                sum(CASE WHEN status_code != 0 THEN 1 ELSE 0 END) AS error_count
              FROM read_parquet(?)
              {where_sql}
              GROUP BY trace_id
              ORDER BY start_time_unix_nano DESC
              LIMIT ?
            """
            with duckdb.connect() as connection:
                cursor = connection.execute(query, params)
                names = [column[0] for column in cursor.description]
                results = []
                for row in cursor.fetchall():
                    d = dict(zip(names, row))
                    # Filter None from services list
                    if isinstance(d.get("services"), list):
                        d["services"] = [s for s in d["services"] if s]
                    results.append(d)

            return results

        except Exception as error:
            logger.error(f"Trace list query failed: {error}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to query traces"
            )

    @application.get("/api/v1/traces/{trace_id}", tags=["Traces"], dependencies=[Depends(verify_auth)])
    def get_trace(trace_id: str, project_id: str | None = None) -> list[dict]:
        """
        Get all spans for a specific trace with parent-child linkage.
        """
        try:
            files = list(resolved.parquet_dir.glob("*.parquet"))
            if not files:
                logger.debug(f"No Parquet files available for trace {trace_id}")
                raise HTTPException(status_code=404, detail="trace not found")

            where_clause = "trace_id = ?"
            params: list[Any] = [[str(file) for file in files], trace_id]

            if project_id:
                where_clause += " AND project_id = ?"
                params.append(project_id)

            query = f"""
                SELECT * FROM read_parquet(?)
                WHERE {where_clause}
                ORDER BY start_time_unix_nano ASC
            """

            with duckdb.connect() as connection:
                cursor = connection.execute(query, params)
                names = [column[0] for column in cursor.description]
                rows = [dict(zip(names, row)) for row in cursor.fetchall()]

            if not rows:
                raise HTTPException(status_code=404, detail="trace not found")

            return rows

        except HTTPException:
            raise
        except Exception as error:
            logger.error(f"Trace detail query failed for {trace_id}: {error}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to query trace"
            )

    @application.get("/api/v1/projects", tags=["Projects"], response_model=list[ProjectResponse], dependencies=[Depends(verify_auth)])
    def list_projects() -> list[dict]:
        """List registered projects."""
        try:
            with sqlite3.connect(resolved.sqlite_path) as connection:
                cursor = connection.execute("SELECT id, name, created_at FROM projects ORDER BY created_at ASC")
                return [{"id": row[0], "name": row[1], "created_at": row[2]} for row in cursor.fetchall()]
        except Exception as error:
            logger.error(f"Failed to list projects: {error}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to list projects")

    @application.post("/api/v1/projects", status_code=201, tags=["Projects"], dependencies=[Depends(verify_auth)])
    def create_project(project: ProjectCreate) -> dict[str, Any]:
        """Create a new project container."""
        try:
            logger.info(f"Creating project: id={project.id}, name={project.name}")
            with sqlite3.connect(resolved.sqlite_path) as connection:
                connection.execute(
                    "INSERT INTO projects(id, name, api_key) VALUES (?, ?, ?)",
                    (project.id, project.name, project.api_key),
                )
                connection.commit()
            return {k: v for k, v in project.model_dump().items() if v is not None}
        except sqlite3.IntegrityError as error:
            logger.warning(f"Project creation failed: {project.id} already exists")
            raise HTTPException(status_code=409, detail="project already exists") from error
        except Exception as error:
            logger.error(f"Project creation failed for {project.id}: {error}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to create project"
            )

    @application.get("/api/v1/analytics/overview", tags=["Analytics"], dependencies=[Depends(verify_auth)])
    def analytics_overview(project_id: str | None = None) -> dict:
        """
        Get system-wide or per-project observability overview metrics.
        """
        try:
            files = list(resolved.parquet_dir.glob("*.parquet"))
            if not files:
                return {
                    "total_traces": 0,
                    "total_spans": 0,
                    "total_llm_requests": 0,
                    "total_input_tokens": 0,
                    "total_output_tokens": 0,
                    "total_tokens": 0,
                    "unique_services": [],
                    "error_rate": 0.0,
                }

            where_sql = ""
            params: list[Any] = [[str(file) for file in files]]
            if project_id:
                where_sql = "WHERE project_id = ?"
                params.append(project_id)

            with duckdb.connect() as connection:
                summary = connection.execute(
                    f"""
                    SELECT
                      COUNT(DISTINCT trace_id) AS total_traces,
                      COUNT(*) AS total_spans,
                      SUM(CASE WHEN
                            json_extract_string(attributes_json, '$.\"gen_ai.request.model\"') IS NOT NULL
                            OR json_extract_string(attributes_json, '$.\"gen_ai.system\"') IS NOT NULL
                            OR name LIKE '%llm%' OR name LIKE '%generate%'
                          THEN 1 ELSE 0 END) AS total_llm_requests,
                      COALESCE(SUM(TRY_CAST(
                        json_extract_string(attributes_json, '$.\"gen_ai.usage.input_tokens\"')
                        AS BIGINT
                      )), 0) AS total_input_tokens,
                      COALESCE(SUM(TRY_CAST(
                        json_extract_string(attributes_json, '$.\"gen_ai.usage.output_tokens\"')
                        AS BIGINT
                      )), 0) AS total_output_tokens,
                      SUM(CASE WHEN status_code != 0 THEN 1 ELSE 0 END) AS error_spans
                    FROM read_parquet(?)
                    {where_sql}
                    """,
                    params,
                ).fetchone()

                services_query = f"""
                    SELECT DISTINCT service_name FROM read_parquet(?)
                    WHERE service_name IS NOT NULL AND service_name != ''
                    {f"AND project_id = '{project_id}'" if project_id else ""}
                """
                services = connection.execute(
                    services_query,
                    [[str(file) for file in files]],
                ).fetchall()

                total_spans = summary[1] or 0
                error_rate = (summary[5] or 0) / total_spans if total_spans > 0 else 0.0
                input_tokens = int(summary[3] or 0)
                output_tokens = int(summary[4] or 0)

                return {
                    "total_traces": summary[0] or 0,
                    "total_spans": total_spans,
                    "total_llm_requests": summary[2] or 0,
                    "total_input_tokens": input_tokens,
                    "total_output_tokens": output_tokens,
                    "total_tokens": input_tokens + output_tokens,
                    "unique_services": [s[0] for s in services if s[0]],
                    "error_rate": round(error_rate, 4),
                }

        except Exception as error:
            logger.error(f"Analytics overview query failed: {error}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to compute analytics overview"
            )

    @application.get("/api/v1/analytics/models", tags=["Analytics"], dependencies=[Depends(verify_auth)])
    def analytics_models(project_id: str | None = None) -> list[dict]:
        """
        Get per-model observability metrics with safe token parsing and latency statistics.
        """
        try:
            files = list(resolved.parquet_dir.glob("*.parquet"))
            if not files:
                return []

            where_clauses = ["json_extract_string(attributes_json, '$.\"gen_ai.request.model\"') IS NOT NULL"]
            params: list[Any] = [[str(file) for file in files]]
            if project_id:
                where_clauses.append("project_id = ?")
                params.append(project_id)

            where_sql = f"WHERE {' AND '.join(where_clauses)}"

            with duckdb.connect() as connection:
                results = connection.execute(
                    f"""
                    SELECT
                      json_extract_string(attributes_json, '$.\"gen_ai.request.model\"') AS model,
                      COUNT(*) AS request_count,
                      COALESCE(SUM(TRY_CAST(
                        json_extract_string(attributes_json, '$.\"gen_ai.usage.input_tokens\"')
                        AS BIGINT
                      )), 0) AS total_input_tokens,
                      COALESCE(SUM(TRY_CAST(
                        json_extract_string(attributes_json, '$.\"gen_ai.usage.output_tokens\"')
                        AS BIGINT
                      )), 0) AS total_output_tokens,
                      AVG(duration_ns) AS avg_latency_ns,
                      approx_quantile(duration_ns, 0.95) AS p95_latency_ns,
                      SUM(CASE WHEN status_code != 0 THEN 1 ELSE 0 END) AS error_count,
                      COALESCE(SUM(duration_ns), 0) AS total_duration_ns
                    FROM read_parquet(?)
                    {where_sql}
                    GROUP BY model
                    ORDER BY request_count DESC
                    """,
                    params,
                ).fetchall()

                rows = []
                for row in results:
                    request_count = row[1] or 0
                    input_tokens = int(row[2] or 0)
                    output_tokens = int(row[3] or 0)
                    total_tokens = input_tokens + output_tokens
                    error_count = row[6] or 0
                    total_duration_sec = (row[7] or 0) / 1_000_000_000.0

                    # Derive tokens per second if total duration > 0
                    tokens_per_sec = round(output_tokens / total_duration_sec, 2) if total_duration_sec > 0 else 0.0

                    rows.append({
                        "model": row[0],
                        "request_count": request_count,
                        "total_input_tokens": input_tokens,
                        "total_output_tokens": output_tokens,
                        "total_tokens": total_tokens,
                        "tokens_per_sec": tokens_per_sec,
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
