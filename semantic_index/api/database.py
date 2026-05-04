"""SQLite database connection management for the Graph API."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager

from fastapi import Request

_MMAP_BYTES = 1 << 30  # 1 GiB — covers the full graph database
_CACHE_PAGES = -65536  # negative → KiB; 65536 = 64 MiB per connection


@contextmanager
def _open_db(db_path: str):
    """Open a read-only SQLite connection, ensuring creation and close happen in the same thread.

    Tuning rationale:
    - ``mode=ro`` URI: true read-only open at the OS level. Lets SQLite skip
      lock files and lets the OS keep file pages clean.
    - ``mmap_size``: maps the database file. Mapped pages live in the OS page
      cache and survive the connection close, so subsequent requests reuse
      pages without disk I/O. Critical on memory-constrained hosts where the
      per-connection page cache is too small to matter.
    - ``cache_size``: bumps the per-connection page cache from 2 MiB to 64 MiB
      so a single request that hits multiple edge tables reuses pages instead
      of evicting them mid-query.
    - ``query_only``: belt-and-suspenders — even with ``mode=ro``, this
      guarantees no accidental writes from a misbehaving query.
    """
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA mmap_size = {_MMAP_BYTES}")
    conn.execute(f"PRAGMA cache_size = {_CACHE_PAGES}")
    conn.execute("PRAGMA query_only = ON")
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
