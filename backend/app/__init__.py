"""
LLM Reliability Control Plane Backend

This package provides the FastAPI-based control plane for the LRCP system.
It handles:
  - WAL materialization to Parquet files
  - DuckDB-based trace queries
  - Project and tenant management
  - Background auto-materialization worker

Architecture:
    WAL (from C++ gateway) → Materializer → Parquet → DuckDB → REST API

The application follows a functional core / imperative shell pattern:
  - Pure functions for WAL parsing, span normalization, and schema mapping
  - Side effects isolated to HTTP handlers, database I/O, and background tasks

For detailed architecture information, see docs/ARCHITECTURE.md.
"""

__version__ = "0.1.0"
