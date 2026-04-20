"""SQLite database connection management for the Graph API."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager

from fastapi import Request


@contextmanager
def _open_db(db_path: str):
    """Open a read-only SQLite connection, ensuring creation and close happen in the same thread."""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    try:
        yield conn
    finally:
        conn.close()


def get_db(request: Request):
    """Yield a read-only SQLite connection scoped to a single request."""
    with _open_db(request.app.state.db_path) as conn:
        yield conn


def open_cache_db(db_path: str, suffix: str, schema: str) -> sqlite3.Connection:
    """Open a writable connection to a sidecar cache database.

    Creates ``{db_path}.{suffix}-cache.db`` with WAL mode and ``sqlite3.Row``
    row factory, then runs *schema* via ``executescript``.

    Args:
        db_path: Path to the main graph database.
        suffix: Cache name inserted into the sidecar filename.
        schema: SQL DDL to execute (idempotent ``CREATE TABLE IF NOT EXISTS``).
    """
    cache_path = db_path + f".{suffix}-cache.db"
    conn = sqlite3.connect(cache_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(schema)
    return conn
