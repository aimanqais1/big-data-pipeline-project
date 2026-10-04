"""
Phase 2 — Step 5: Unified FastAPI Package (`src/api`).
"""
from src.api.app import app, create_app
from src.api.routes import get_target_db_name, router

__all__ = [
    "app",
    "create_app",
    "get_target_db_name",
    "router",
]
