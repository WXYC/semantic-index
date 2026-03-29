"""FastAPI application for the WXYC Graph API."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import aiosqlite
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from semantic_index.api.config import Settings
from semantic_index.api.database import connect

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

logger = logging.getLogger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    """Create and configure the FastAPI application.

    Args:
        settings: Optional settings override (useful for testing).
            Defaults to reading from environment variables.

    Returns:
        A configured FastAPI application.
    """
    if settings is None:
        settings = Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[dict[str, aiosqlite.Connection]]:
        db_path = settings.db_path
        if not Path(db_path).exists():
            logger.warning("Database not found at %s — starting without connection", db_path)
            app.state.db = None
            yield {}
        else:
            logger.info("Connecting to database at %s", db_path)
            db = await connect(db_path)
            app.state.db = db
            yield {}
            await db.close()
            logger.info("Database connection closed")

    app = FastAPI(
        title="WXYC Graph API",
        description="Query the WXYC semantic artist graph",
        lifespan=lifespan,
    )

    @app.get("/health")
    async def health() -> JSONResponse:
        """Check API health and database connectivity.

        Returns 200 with artist count if the database is accessible,
        or 503 if the database is missing or unreachable.
        """
        db: aiosqlite.Connection | None = app.state.db
        if db is None:
            return JSONResponse(
                status_code=503,
                content={"status": "error", "detail": "Database not available"},
            )
        try:
            async with db.execute("SELECT COUNT(*) FROM artist") as cursor:
                row = await cursor.fetchone()
                artist_count = row[0] if row else 0
            return JSONResponse(
                status_code=200,
                content={"status": "ok", "artist_count": artist_count},
            )
        except Exception as exc:
            logger.exception("Health check failed")
            return JSONResponse(
                status_code=503,
                content={"status": "error", "detail": str(exc)},
            )

    return app
