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
