"""Export facet tables for dynamic PMI computation.

Creates play-level data and pre-materialized single-dimension aggregate tables
so the API can compute filtered PMI at query time without combinatorial explosion.

Tables created:
- ``dj`` — DJ lookup (normalizes mixed int/str identifiers to integer PKs)
- ``play`` — one row per resolved flowsheet entry (~2M rows)
- ``artist_month_count`` — per-artist play count per month
- ``artist_dj_count`` — per-artist play count per DJ
- ``month_total`` — total plays and adjacency pairs per month
- ``dj_total`` — total plays and adjacency pairs per DJ
"""

from __future__ import annotations

import logging
import sqlite3
from collections import Counter
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from semantic_index.models import AdjacencyPair, ResolvedEntry

logger = logging.getLogger(__name__)

_FACET_SCHEMA = """
CREATE TABLE IF NOT EXISTS dj (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    original_id TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS play (
    id INTEGER PRIMARY KEY,
    artist_id INTEGER NOT NULL REFERENCES artist(id),
    show_id INTEGER NOT NULL,
    dj_id INTEGER REFERENCES dj(id),
    sequence INTEGER NOT NULL,
    month INTEGER NOT NULL,
    request_flag INTEGER NOT NULL DEFAULT 0,
    timestamp INTEGER
);

CREATE INDEX IF NOT EXISTS idx_play_show_seq ON play(show_id, sequence);
CREATE INDEX IF NOT EXISTS idx_play_artist ON play(artist_id);
CREATE INDEX IF NOT EXISTS idx_play_month ON play(month);
CREATE INDEX IF NOT EXISTS idx_play_dj ON play(dj_id);

CREATE TABLE IF NOT EXISTS artist_month_count (
    artist_id INTEGER NOT NULL REFERENCES artist(id),
    month INTEGER NOT NULL,
    play_count INTEGER NOT NULL,
    PRIMARY KEY (artist_id, month)
);

CREATE TABLE IF NOT EXISTS artist_dj_count (
    artist_id INTEGER NOT NULL REFERENCES artist(id),
    dj_id INTEGER NOT NULL REFERENCES dj(id),
    play_count INTEGER NOT NULL,
    PRIMARY KEY (artist_id, dj_id)
);

CREATE TABLE IF NOT EXISTS month_total (
    month INTEGER PRIMARY KEY,
    total_plays INTEGER NOT NULL,
    total_pairs INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS dj_total (
    dj_id INTEGER PRIMARY KEY REFERENCES dj(id),
    total_plays INTEGER NOT NULL,
    total_pairs INTEGER NOT NULL
);
"""


def _extract_month(start_time_ms: int | None) -> int:
    """Extract month (1-12) from a Unix-millisecond timestamp, or 0 if None."""
    if start_time_ms is None:
        return 0
    dt = datetime.fromtimestamp(start_time_ms / 1000, tz=UTC)
    return dt.month


def export_facet_tables(
    db_path: str,
    resolved_entries: list[ResolvedEntry],
    name_to_id: dict[str, int],
    show_to_dj: dict[int, int | str],
    show_dj_names: dict[int, str],
    adjacency_pairs: list[AdjacencyPair],
) -> None:
    """Export facet tables into an existing SQLite database.

    Args:
        db_path: Path to the SQLite database (must already contain the artist table).
        resolved_entries: All resolved flowsheet entries from the pipeline.
        name_to_id: Mapping of canonical artist name to artist.id in the database.
        show_to_dj: Mapping of show_id to DJ identifier (int DJ_ID or str DJ_NAME).
        show_dj_names: Mapping of show_id to DJ display name string.
        adjacency_pairs: All adjacency pairs extracted from the pipeline.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        conn.executescript(_FACET_SCHEMA)

        # Clear existing facet data (safe to re-run)
        for table in (
            "dj_total",
            "month_total",
            "artist_dj_count",
            "artist_month_count",
            "play",
            "dj",
        ):
            conn.execute(f"DELETE FROM {table}")  # noqa: S608

        # 1. Build DJ table
        dj_key_to_id = _insert_djs(conn, show_to_dj, show_dj_names)

        # 2. Build show_id -> dj table id mapping
        show_to_dj_table_id: dict[int, int] = {}
        for show_id, dj_key in show_to_dj.items():
            key = str(dj_key)
            if key in dj_key_to_id:
                show_to_dj_table_id[show_id] = dj_key_to_id[key]

        # 3. Insert play rows
        play_count = _insert_plays(conn, resolved_entries, name_to_id, show_to_dj_table_id)
        logger.info("  %d play rows inserted", play_count)

        # 4. Compute aggregate tables from play data
        _compute_artist_month_count(conn)
        _compute_artist_dj_count(conn)

        # 5. Compute totals (needs adjacency pair metadata)
        show_to_month = _build_show_to_month(conn)
        _compute_month_total(conn, adjacency_pairs, show_to_month)
        _compute_dj_total(conn, adjacency_pairs, show_to_dj_table_id)

        conn.commit()
    finally:
        conn.close()


def _insert_djs(
    conn: sqlite3.Connection,
    show_to_dj: dict[int, int | str],
    show_dj_names: dict[int, str],
) -> dict[str, int]:
    """Insert unique DJs and return a mapping of original_id (as str) -> dj.id."""
    # Collect unique DJ keys and their best display names
    dj_display: dict[str, str] = {}
    for show_id, dj_key in show_to_dj.items():
        key = str(dj_key)
        if key not in dj_display:
            # Prefer the display name from show_dj_names; fall back to the key itself
            name = show_dj_names.get(show_id, key)
            dj_display[key] = name

    # Also check other shows for better display names
    for show_id, dj_key in show_to_dj.items():
        key = str(dj_key)
        better_name = show_dj_names.get(show_id)
        if better_name and dj_display[key] == key:
            dj_display[key] = better_name

    conn.executemany(
        "INSERT OR IGNORE INTO dj (original_id, display_name) VALUES (?, ?)",
        list(dj_display.items()),
    )

    # Read back the assigned IDs
    rows = conn.execute("SELECT id, original_id FROM dj").fetchall()
    return {r["original_id"]: r["id"] for r in rows}


def _insert_plays(
    conn: sqlite3.Connection,
    resolved_entries: list[ResolvedEntry],
    name_to_id: dict[str, int],
    show_to_dj_table_id: dict[int, int],
) -> int:
    """Insert play rows. Returns the number of rows inserted."""
    batch: list[tuple] = []
    skipped = 0

    for entry in resolved_entries:
        artist_id = name_to_id.get(entry.canonical_name)
        if artist_id is None:
            skipped += 1
            continue

        fe = entry.entry
        month = _extract_month(fe.start_time)
        dj_id = show_to_dj_table_id.get(fe.show_id)

        batch.append(
            (
                fe.id,
                artist_id,
                fe.show_id,
                dj_id,
                fe.sequence,
                month,
                fe.request_flag,
                fe.start_time,
            )
        )

    if skipped:
        logger.debug("  Skipped %d entries with unknown artist names", skipped)

    conn.executemany(
        "INSERT OR IGNORE INTO play "
        "(id, artist_id, show_id, dj_id, sequence, month, request_flag, timestamp) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        batch,
    )
    return len(batch)


def _compute_artist_month_count(conn: sqlite3.Connection) -> None:
    conn.execute(
        "INSERT INTO artist_month_count (artist_id, month, play_count) "
        "SELECT artist_id, month, COUNT(*) "
        "FROM play WHERE month > 0 "
        "GROUP BY artist_id, month"
    )


def _compute_artist_dj_count(conn: sqlite3.Connection) -> None:
    conn.execute(
        "INSERT INTO artist_dj_count (artist_id, dj_id, play_count) "
        "SELECT artist_id, dj_id, COUNT(*) "
        "FROM play WHERE dj_id IS NOT NULL "
        "GROUP BY artist_id, dj_id"
    )


def _build_show_to_month(conn: sqlite3.Connection) -> dict[int, int]:
    """Build show_id -> month mapping from the first play entry in each show."""
    rows = conn.execute(
        "SELECT show_id, month FROM play WHERE month > 0 GROUP BY show_id"
    ).fetchall()
    return {r["show_id"]: r["month"] for r in rows}


def _compute_month_total(
    conn: sqlite3.Connection,
    adjacency_pairs: list[AdjacencyPair],
    show_to_month: dict[int, int],
) -> None:
    """Compute total plays and pairs per month."""
    # Total plays per month (from play table)
    play_rows = conn.execute(
        "SELECT month, COUNT(*) AS total_plays FROM play WHERE month > 0 GROUP BY month"
    ).fetchall()
    month_plays: dict[int, int] = {r["month"]: r["total_plays"] for r in play_rows}

    # Total pairs per month (from adjacency pairs + show metadata)
    month_pairs: Counter[int] = Counter()
    for pair in adjacency_pairs:
        month = show_to_month.get(pair.show_id, 0)
        if month > 0:
            month_pairs[month] += 1

    # Merge and insert
    all_months = set(month_plays) | set(month_pairs)
    conn.executemany(
        "INSERT INTO month_total (month, total_plays, total_pairs) VALUES (?, ?, ?)",
        [(m, month_plays.get(m, 0), month_pairs.get(m, 0)) for m in sorted(all_months)],
    )


def _compute_dj_total(
    conn: sqlite3.Connection,
    adjacency_pairs: list[AdjacencyPair],
    show_to_dj_table_id: dict[int, int],
) -> None:
    """Compute total plays and pairs per DJ."""
    # Total plays per DJ (from play table)
    play_rows = conn.execute(
        "SELECT dj_id, COUNT(*) AS total_plays FROM play WHERE dj_id IS NOT NULL GROUP BY dj_id"
    ).fetchall()
    dj_plays: dict[int, int] = {r["dj_id"]: r["total_plays"] for r in play_rows}

    # Total pairs per DJ (from adjacency pairs + show metadata)
    dj_pairs: Counter[int] = Counter()
    for pair in adjacency_pairs:
        dj_id = show_to_dj_table_id.get(pair.show_id)
        if dj_id is not None:
            dj_pairs[dj_id] += 1

    # Merge and insert
    all_djs = set(dj_plays) | set(dj_pairs)
    conn.executemany(
        "INSERT INTO dj_total (dj_id, total_plays, total_pairs) VALUES (?, ?, ?)",
        [(d, dj_plays.get(d, 0), dj_pairs.get(d, 0)) for d in sorted(all_djs)],
    )
