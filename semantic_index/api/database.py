"""aiosqlite connection management for the Graph API."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import aiosqlite

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

logger = logging.getLogger(__name__)


async def connect(db_path: str) -> aiosqlite.Connection:
    """Open an aiosqlite connection with row factory enabled.

    Args:
        db_path: Path to the SQLite database file.

    Returns:
        An open aiosqlite connection.
    """
    db = await aiosqlite.connect(db_path)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA foreign_keys=ON")
    return db


async def get_db(db: aiosqlite.Connection) -> AsyncIterator[aiosqlite.Connection]:
    """FastAPI dependency that yields the shared database connection.

    The connection is managed by the app lifespan — this dependency simply
    yields it for use in route handlers.

    Args:
        db: The shared aiosqlite connection from app state.

    Yields:
        The shared aiosqlite connection.
    """
    yield db
