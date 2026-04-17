"""FastAPI application factory for the Graph API."""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from semantic_index.api.bio import bio_router
from semantic_index.api.narrative import narrative_router
from semantic_index.api.preview import preview_router
from semantic_index.api.routes import router

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
EXPLORER_DIR = PROJECT_ROOT / "explorer"
EXPLORER_HTML = EXPLORER_DIR / "index.html"


def create_app(
    db_path: str,
    anthropic_api_key: str | None = None,
    *,
    sync_enabled: bool = False,
    sync_hour_utc: int = 9,
    sync_dsn: str | None = None,
    sync_min_count: int = 2,
) -> FastAPI:
    """Create a FastAPI application wired to the given SQLite database.

    Args:
        db_path: Path to the SQLite graph database produced by the pipeline.
        anthropic_api_key: Anthropic API key for narrative generation. When None,
            the narrative endpoint returns 501.
        sync_enabled: Start the nightly sync scheduler on startup.
        sync_hour_utc: Hour (UTC) to run the daily sync.
        sync_dsn: PostgreSQL DSN for Backend-Service (required when sync_enabled).
        sync_min_count: Minimum co-occurrence count for DJ transition edges.
    """
    app = FastAPI(title="WXYC Semantic Graph API", version="0.1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["GET"],
        allow_headers=["*"],
    )
    app.state.db_path = db_path
    app.state.anthropic_api_key = anthropic_api_key
    app.state.anthropic_client = None
    app.include_router(router)
    app.include_router(narrative_router)
    app.include_router(bio_router)
    app.include_router(preview_router)

    @app.get("/health", include_in_schema=False)
    def health() -> JSONResponse:
        """Health check — verifies the SQLite database is readable."""
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            count = conn.execute("SELECT COUNT(*) FROM artist").fetchone()[0]
            conn.close()
            return JSONResponse({"status": "healthy", "artist_count": count})
        except Exception as exc:
            return JSONResponse(
                {"status": "unhealthy", "detail": str(exc)},
                status_code=503,
            )

    @app.get("/", include_in_schema=False)
    def root() -> FileResponse:
        """Serve the D3.js graph explorer."""
        return FileResponse(EXPLORER_HTML, media_type="text/html")

    app.mount("/", StaticFiles(directory=str(EXPLORER_DIR)), name="explorer")

    # Start nightly sync scheduler if enabled
    if sync_enabled:
        if not sync_dsn:
            logger.error("SYNC_ENABLED=true but DATABASE_URL_BACKEND not set — skipping scheduler")
        else:
            from semantic_index.api.sync_scheduler import start_scheduler

            start_scheduler(
                db_path=db_path,
                dsn=sync_dsn,
                min_count=sync_min_count,
                hour_utc=sync_hour_utc,
            )

    return app
