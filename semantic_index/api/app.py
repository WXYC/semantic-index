"""FastAPI application factory for the Graph API."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from wxyc_etl.logger import init_logger
from wxyc_fastapi.healthcheck import Check, readiness_router
from wxyc_fastapi.observability import init_sentry

from semantic_index.api.bio import bio_router
from semantic_index.api.narrative import narrative_router
from semantic_index.api.narrative_audit_routes import narrative_audit_router
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
    enrichment_top_k: int = 50,
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
        enrichment_top_k: Per-artist neighbor cap for shared_personnel and
            label_family applied on every nightly sync. 0 disables.
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
    app.include_router(narrative_audit_router)

    # Liveness/health: kept inline because semantic-index returns a custom
    # `artist_count` field in addition to `status`, which the shared
    # `wxyc_fastapi.healthcheck.liveness_router` does not surface. Adopting
    # `liveness_router` would drop the `artist_count` field that the WXYC
    # synthetic-DJ canary parses (see WXYC/wxyc-canary). Readiness, which
    # is shape-compatible with the shared router, *is* delegated below.
    # Tracked: WXYC/wxyc-fastapi#19 (extra-fields hook for liveness).
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

    def _probe_artist_count_sync() -> None:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            conn.execute("SELECT COUNT(*) FROM artist").fetchone()
        finally:
            conn.close()

    async def _probe_artist_count_query() -> str:
        """Readiness probe: confirms the SQLite graph DB is readable.

        Returns ``"ok"`` if a `SELECT COUNT(*) FROM artist` succeeds against
        the read-only sqlite URI; raises otherwise (the shared readiness
        router treats any exception as ``"unavailable"``).

        The sync `sqlite3` work is offloaded via `asyncio.to_thread` so the
        probe never blocks the event loop, even if the SQLite file is on slow
        storage or the OS file cache is cold.
        """
        await asyncio.to_thread(_probe_artist_count_sync)
        return "ok"

    app.include_router(
        readiness_router(
            checks=[
                Check(name="database", probe=_probe_artist_count_query, required=True),
            ],
        ),
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
                enrichment_top_k=enrichment_top_k,
            )

    return app


def _create_app_from_settings() -> FastAPI:
    """Create the app from environment-based settings for uvicorn multiworker mode."""
    from semantic_index.api.config import Settings

    settings = Settings()
    # Install the JSON-on-stderr handler before anything else so module loggers
    # under semantic_index.* (including the sync scheduler) are visible from
    # the first line of process lifetime. Pass sentry_dsn="" so init_logger
    # does not double-init Sentry — init_sentry below owns the full SDK config
    # (FastAPI + Httpx integrations, sample rates, service.name tag).
    init_logger(
        repo="semantic-index",
        tool="semantic-index api",
        sentry_dsn="",
    )
    init_sentry(
        dsn=settings.sentry_dsn,
        service_name="semantic-index",
        environment=settings.sentry_environment,
        release=settings.sentry_release,
    )
    return create_app(
        settings.db_path,
        anthropic_api_key=settings.anthropic_api_key,
        sync_enabled=settings.sync_enabled,
        sync_hour_utc=settings.sync_hour_utc,
        sync_dsn=settings.database_url_backend,
        sync_min_count=settings.sync_min_count,
        enrichment_top_k=settings.enrichment_top_k,
    )


app = _create_app_from_settings()
