"""
Phase 2 — Step 5: Pydantic Request & Response Schemas for Unified FastAPI Interface.
Enforces strict input validation, bounded parameters, and `extra="forbid"` so callers
cannot inject unapproved fields such as `reset_db` or `db_name`.
"""
from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, ConfigDict, Field


class HealthResponse(BaseModel):
    """Response contract for GET /health."""

    status: str = Field(..., description="Overall API health status ('ok' or 'degraded').")
    database: str = Field(..., description="Configured target MongoDB database name.")
    mongodb: str = Field(..., description="MongoDB connectivity state ('connected' or 'disconnected').")


class IngestRequest(BaseModel):
    """
    Request body for POST /ingest.
    Note: `reset_db` and `db_name` are intentionally excluded and forbidden (`extra='forbid'`)
    so API callers can never drop collections or target arbitrary databases.
    """

    model_config = ConfigDict(extra="forbid")

    file_path: str = Field(
        ...,
        min_length=1,
        max_length=260,
        description="Relative or project-local path to a CSV file inside the 'data/' directory.",
    )
    custom_run_id: Optional[str] = Field(
        default=None,
        min_length=1,
        max_length=100,
        description="Optional custom run identifier for lineage tracking.",
    )
    batch_size: int = Field(
        default=10000,
        ge=100,
        le=50000,
        description="Streaming batch size (bounded between 100 and 50,000).",
    )


class RefreshMVRequest(BaseModel):
    """Request body for POST /refresh-mv (public API is incremental-only)."""

    model_config = ConfigDict(extra="forbid")

    mode: Literal["incremental"] = Field(
        default="incremental",
        description="Materialized view refresh mode ('incremental' only on public API).",
    )


class ErrorResponse(BaseModel):
    """Standardized sanitized JSON error response."""

    status: str = "error"
    error_type: str
    detail: str
