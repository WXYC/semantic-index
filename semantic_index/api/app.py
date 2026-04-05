"""FastAPI application factory for the Graph API."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from semantic_index.api.narrative import narrative_router
from semantic_index.api.routes import router

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
EXPLORER_DIR = PROJECT_ROOT / "explorer"
EXPLORER_HTML = EXPLORER_DIR / "index.html"


def create_app(db_path: str, anthropic_api_key: str | None = None) -> FastAPI:
    """Create a FastAPI application wired to the given SQLite database.

    Args:
        db_path: Path to the SQLite graph database produced by the pipeline.
        anthropic_api_key: Anthropic API key for narrative generation. When None,
            the narrative endpoint returns 501.
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

    return app
