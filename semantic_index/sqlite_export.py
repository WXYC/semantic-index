"""Export the artist graph to a SQLite database.

Creates an artist table with node attributes, a dj_transition table for
PMI-weighted edges, and a cross_reference table for catalog cross-reference edges.
"""

import logging
import sqlite3

from semantic_index.models import ArtistStats, CrossReferenceEdge, PmiEdge

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS artist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_name TEXT NOT NULL UNIQUE,
    genre TEXT,
    total_plays INTEGER NOT NULL DEFAULT 0,
    active_first_year INTEGER,
    active_last_year INTEGER,
    dj_count INTEGER NOT NULL DEFAULT 0,
    request_ratio REAL NOT NULL DEFAULT 0.0,
    show_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS dj_transition (
    source_id INTEGER NOT NULL REFERENCES artist(id),
    target_id INTEGER NOT NULL REFERENCES artist(id),
    raw_count INTEGER NOT NULL,
    pmi REAL NOT NULL,
    PRIMARY KEY (source_id, target_id)
);

CREATE TABLE IF NOT EXISTS cross_reference (
    artist_a_id INTEGER NOT NULL REFERENCES artist(id),
    artist_b_id INTEGER NOT NULL REFERENCES artist(id),
    comment TEXT,
    source TEXT NOT NULL,
    PRIMARY KEY (artist_a_id, artist_b_id, source)
);

CREATE INDEX IF NOT EXISTS idx_transition_source ON dj_transition(source_id, pmi DESC);
CREATE INDEX IF NOT EXISTS idx_transition_target ON dj_transition(target_id, pmi DESC);
CREATE INDEX IF NOT EXISTS idx_xref_a ON cross_reference(artist_a_id);
CREATE INDEX IF NOT EXISTS idx_xref_b ON cross_reference(artist_b_id);
"""


def export_sqlite(
    path: str,
    artist_stats: dict[str, ArtistStats],
    pmi_edges: list[PmiEdge],
    xref_edges: list[CrossReferenceEdge],
    min_count: int = 2,
) -> None:
    """Export the artist graph to a SQLite database.

    Args:
        path: Output path for the SQLite database file.
        artist_stats: Per-artist statistics for the artist table.
        pmi_edges: PMI-weighted DJ transition edges.
        xref_edges: Cross-reference edges from the library catalog.
        min_count: Minimum raw co-occurrence count for DJ transition edges.
    """
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    conn.executescript(_SCHEMA)

    # Collect all artist names (from stats + cross-ref endpoints)
    all_names: set[str] = set(artist_stats.keys())
    for xref in xref_edges:
        all_names.add(xref.artist_a)
        all_names.add(xref.artist_b)

    # Insert artists
    artist_rows = []
    for name in sorted(all_names):
        stats = artist_stats.get(name)
        if stats:
            artist_rows.append(
                (
                    name,
                    stats.genre,
                    stats.total_plays,
                    stats.active_first_year,
                    stats.active_last_year,
                    stats.dj_count,
                    stats.request_ratio,
                    stats.show_count,
                )
            )
        else:
            # Catalog-only artist (from cross-references, not in flowsheet)
            artist_rows.append((name, None, 0, None, None, 0, 0.0, 0))

    conn.executemany(
        """
        INSERT INTO artist (
            canonical_name, genre, total_plays,
            active_first_year, active_last_year,
            dj_count, request_ratio, show_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        artist_rows,
    )

    # Build name → id mapping
    name_to_id: dict[str, int] = {}
    for row in conn.execute("SELECT id, canonical_name FROM artist"):
        name_to_id[row[1]] = row[0]

    # Insert DJ transition edges (filtered)
    transition_rows = []
    for edge in pmi_edges:
        if edge.raw_count < min_count or edge.pmi <= 0:
            continue
        source_id = name_to_id.get(edge.source)
        target_id = name_to_id.get(edge.target)
        if source_id is not None and target_id is not None:
            transition_rows.append((source_id, target_id, edge.raw_count, edge.pmi))

    conn.executemany(
        "INSERT INTO dj_transition (source_id, target_id, raw_count, pmi) VALUES (?, ?, ?, ?)",
        transition_rows,
    )

    # Insert cross-reference edges
    xref_rows = []
    for xref in xref_edges:
        a_id = name_to_id.get(xref.artist_a)
        b_id = name_to_id.get(xref.artist_b)
        if a_id is not None and b_id is not None:
            xref_rows.append((a_id, b_id, xref.comment, xref.source))

    conn.executemany(
        "INSERT INTO cross_reference (artist_a_id, artist_b_id, comment, source) VALUES (?, ?, ?, ?)",
        xref_rows,
    )

    conn.commit()

    artist_count = conn.execute("SELECT COUNT(*) FROM artist").fetchone()[0]
    transition_count = conn.execute("SELECT COUNT(*) FROM dj_transition").fetchone()[0]
    xref_count = conn.execute("SELECT COUNT(*) FROM cross_reference").fetchone()[0]
    logger.info(
        "SQLite export: %d artists, %d DJ transitions, %d cross-references",
        artist_count,
        transition_count,
        xref_count,
    )

    conn.close()
