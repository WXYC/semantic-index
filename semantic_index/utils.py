"""Shared utilities for the semantic index pipeline."""

from __future__ import annotations

import logging
import sqlite3

logger = logging.getLogger(__name__)

# ---- Schema migration ----


def ensure_columns(
    conn: sqlite3.Connection,
    table: str,
    columns: list[tuple[str, str]],
) -> list[str]:
    """Idempotently add columns to a table using PRAGMA inspection.

    Returns the list of column names that were actually added.
    """
    existing = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
    added = []
    for col_name, col_def in columns:
        if col_name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_def}")
            added.append(col_name)
            logger.info("Added column %s to %s table", col_name, table)
    return added
