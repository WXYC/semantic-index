"""Shared utilities for the semantic index pipeline."""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Iterator

logger = logging.getLogger(__name__)


def batched_with_log(
    items: list,
    batch_size: int = 1000,
    *,
    log_every: int = 5000,
    label: str = "items",
) -> Iterator[list]:
    """Yield batches of *items* with periodic progress logging.

    Args:
        items: Full list to iterate.
        batch_size: Number of items per batch.
        log_every: Log progress every this many items (must be a multiple of
            *batch_size* for consistent reporting).
        label: Prefix for the log message.
    """
    total = len(items)
    total_batches = (total + batch_size - 1) // batch_size
    for i in range(0, total, batch_size):
        yield items[i : i + batch_size]
        if (i + batch_size) % log_every == 0:
            logger.info(
                "  %s: %d/%d batches",
                label,
                i // batch_size + 1,
                total_batches,
            )


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
