"""Background scheduler for nightly sync within the API process.

Runs ``nightly_sync()`` once daily at a configurable hour (UTC).
The sync writes to a temp copy of the database and atomically swaps
it, so the API can continue serving requests during the rebuild.

Controlled by environment variables:
    SYNC_ENABLED=true           — enable the scheduler (default: false)
    SYNC_HOUR_UTC=9             — hour to run (default: 9 = 5am ET)
    DATABASE_URL_BACKEND=...    — Backend-Service PG DSN (required)
"""

from __future__ import annotations

import argparse
import logging
import threading
import time
from datetime import UTC, datetime

logger = logging.getLogger(__name__)


def _seconds_until_next_run(hour_utc: int) -> float:
    """Compute seconds from now until the next occurrence of *hour_utc*:00 UTC."""
    now = datetime.now(UTC)
    target = now.replace(hour=hour_utc, minute=0, second=0, microsecond=0)
    if target <= now:
        # Already past today's window — schedule for tomorrow
        target = target.replace(day=target.day + 1)
    delta = (target - now).total_seconds()
    return delta


def _run_sync(db_path: str, dsn: str, min_count: int) -> None:
    """Execute a single sync run, catching all exceptions."""
    try:
        from semantic_index.nightly_sync import nightly_sync

        args = argparse.Namespace(
            db_path=db_path,
            dsn=dsn,
            min_count=min_count,
            dry_run=False,
            verbose=False,
        )
        nightly_sync(args)
    except SystemExit:
        logger.error("Sync aborted (SystemExit)")
    except Exception:
        logger.exception("Sync failed with unexpected error")


def _scheduler_loop(db_path: str, dsn: str, min_count: int, hour_utc: int) -> None:
    """Sleep-and-run loop for the background thread."""
    while True:
        wait = _seconds_until_next_run(hour_utc)
        logger.info(
            "Next sync in %.1f hours (at %02d:00 UTC)",
            wait / 3600,
            hour_utc,
        )
        time.sleep(wait)

        logger.info("Starting scheduled sync...")
        _run_sync(db_path, dsn, min_count)


def start_scheduler(
    db_path: str,
    dsn: str,
    min_count: int = 2,
    hour_utc: int = 9,
) -> threading.Thread:
    """Start the sync scheduler as a daemon thread.

    Args:
        db_path: Path to the production SQLite database.
        dsn: PostgreSQL DSN for Backend-Service.
        min_count: Minimum co-occurrence count for DJ transition edges.
        hour_utc: Hour (UTC) to run the daily sync.

    Returns:
        The daemon thread (already started).
    """
    thread = threading.Thread(
        target=_scheduler_loop,
        args=(db_path, dsn, min_count, hour_utc),
        name="sync-scheduler",
        daemon=True,
    )
    thread.start()
    logger.info("Sync scheduler started (daily at %02d:00 UTC)", hour_utc)
    return thread
