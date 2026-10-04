"""
Phase 2 — Step 5: Unified FastAPI Application Factory & Sanitized Exception Handlers (`src/api/app.py`).

Exposes `create_app()` and `app = create_app()` for Uvicorn and FastAPI `TestClient`.
Importing this module NEVER starts a live HTTP server or background scheduler loop.
"""
import logging
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pymongo.errors import PyMongoError

from src.api.routes import router

logger = logging.getLogger("src.api")


def create_app() -> FastAPI:
    """
    Create and configure the Unified FastAPI application for the Big Data E-Commerce
    Data Pipeline (Phase 1 Ingestion + Phase 2 Indexes, Queries, Aggregations,
    Materialized Views, and Scheduled Jobs).
    """
    application = FastAPI(
        title="Big Data E-Commerce Pipeline — Unified API",
        description=(
            "Unified REST API for Phase 1 CSV Ingestion and Phase 2 Analytical Indexes, "
            "Parameterized Queries, Aggregation Reports, Materialized Views, and Scheduled Jobs."
        ),
        version="2.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    @application.exception_handler(KeyError)
    async def key_error_handler(request: Request, exc: KeyError) -> JSONResponse:
        msg = exc.args[0] if exc.args else "Requested resource was not found."
        return JSONResponse(
            status_code=404,
            content={
                "status": "error",
                "error_type": "NotFound",
                "detail": str(msg),
            },
        )

    @application.exception_handler(FileNotFoundError)
    async def file_not_found_handler(request: Request, exc: FileNotFoundError) -> JSONResponse:
        return JSONResponse(
            status_code=404,
            content={
                "status": "error",
                "error_type": "FileNotFound",
                "detail": str(exc) or "Requested file was not found.",
            },
        )

    @application.exception_handler(ValueError)
    async def value_error_handler(request: Request, exc: ValueError) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content={
                "status": "error",
                "error_type": "ValidationError",
                "detail": str(exc) or "Invalid request parameter.",
            },
        )

    @application.exception_handler(PermissionError)
    async def permission_error_handler(request: Request, exc: PermissionError) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content={
                "status": "error",
                "error_type": "SecurityPolicyViolation",
                "detail": str(exc) or "Operation violates API security policy.",
            },
        )

    @application.exception_handler(PyMongoError)
    async def pymongo_error_handler(request: Request, exc: PyMongoError) -> JSONResponse:
        logger.error("MongoDB operation failed on %s: %s", request.url.path, exc)
        return JSONResponse(
            status_code=503,
            content={
                "status": "error",
                "error_type": "DatabaseUnavailable",
                "detail": "Database operation failed or MongoDB service is temporarily unavailable.",
            },
        )

    @application.exception_handler(Exception)
    async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.error("Unhandled internal error on %s: %s", request.url.path, exc)
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "error_type": "InternalServerError",
                "detail": "An unexpected internal server error occurred.",
            },
        )

    application.include_router(router)
    return application


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("src.api.app:app", host="127.0.0.1", port=8000, reload=False)
