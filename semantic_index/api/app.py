"""FastAPI application factory for the Graph API."""

from __future__ import annotations

from fastapi import FastAPI

from semantic_index.api.routes import router


def create_app(db_path: str) -> FastAPI:
    """Create a FastAPI application wired to the given SQLite database.

    Args:
        db_path: Path to the SQLite graph database produced by the pipeline.
    """
    app = FastAPI(title="WXYC Semantic Graph API", version="0.1.0")
    app.state.db_path = db_path
    app.include_router(router)
    return app
