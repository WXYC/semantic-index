"""Export the artist graph to a SQLite database.

Creates an artist table with node attributes, edge tables for DJ transitions,
cross-references, and Discogs-derived relationships (shared personnel, styles,
labels, compilations), plus enrichment tables for per-artist Discogs metadata.

When an ``EntityStore`` is provided, the artist table already exists (managed
by the entity store) and export skips artist creation, using the store for ID
mapping, stats updates, and style persistence instead.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from typing import TYPE_CHECKING

from semantic_index.models import ArtistStats, CrossReferenceEdge, PmiEdge

if TYPE_CHECKING:
    from semantic_index.entity_store import EntityStore
    from semantic_index.models import (
        ArtistEnrichment,
        CompilationEdge,
        LabelFamilyEdge,
        SharedPersonnelEdge,
        SharedStyleEdge,
    )

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
    show_count INTEGER NOT NULL DEFAULT 0,
    discogs_artist_id INTEGER
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

CREATE TABLE IF NOT EXISTS artist_style (
    artist_id INTEGER NOT NULL REFERENCES artist(id),
    style_tag TEXT NOT NULL,
    PRIMARY KEY (artist_id, style_tag)
);

CREATE TABLE IF NOT EXISTS artist_personnel (
    artist_id INTEGER NOT NULL REFERENCES artist(id),
    personnel_name TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS artist_label (
    artist_id INTEGER NOT NULL REFERENCES artist(id),
    label_name TEXT NOT NULL,
    label_id INTEGER,
    PRIMARY KEY (artist_id, label_name)
);

CREATE TABLE IF NOT EXISTS shared_personnel (
    artist_a_id INTEGER NOT NULL REFERENCES artist(id),
    artist_b_id INTEGER NOT NULL REFERENCES artist(id),
    shared_count INTEGER NOT NULL,
    shared_names TEXT NOT NULL,
    PRIMARY KEY (artist_a_id, artist_b_id)
);

CREATE TABLE IF NOT EXISTS shared_style (
    artist_a_id INTEGER NOT NULL REFERENCES artist(id),
    artist_b_id INTEGER NOT NULL REFERENCES artist(id),
    jaccard REAL NOT NULL,
    shared_tags TEXT NOT NULL,
    PRIMARY KEY (artist_a_id, artist_b_id)
);

CREATE TABLE IF NOT EXISTS label_family (
    artist_a_id INTEGER NOT NULL REFERENCES artist(id),
    artist_b_id INTEGER NOT NULL REFERENCES artist(id),
    shared_labels TEXT NOT NULL,
    PRIMARY KEY (artist_a_id, artist_b_id)
);

CREATE TABLE IF NOT EXISTS compilation (
    artist_a_id INTEGER NOT NULL REFERENCES artist(id),
    artist_b_id INTEGER NOT NULL REFERENCES artist(id),
    compilation_count INTEGER NOT NULL,
    compilation_titles TEXT NOT NULL,
    PRIMARY KEY (artist_a_id, artist_b_id)
);

CREATE INDEX IF NOT EXISTS idx_transition_source ON dj_transition(source_id, pmi DESC);
CREATE INDEX IF NOT EXISTS idx_transition_target ON dj_transition(target_id, pmi DESC);
CREATE INDEX IF NOT EXISTS idx_xref_a ON cross_reference(artist_a_id);
CREATE INDEX IF NOT EXISTS idx_xref_b ON cross_reference(artist_b_id);
CREATE INDEX IF NOT EXISTS idx_shared_personnel_a ON shared_personnel(artist_a_id);
CREATE INDEX IF NOT EXISTS idx_shared_personnel_b ON shared_personnel(artist_b_id);
CREATE INDEX IF NOT EXISTS idx_shared_style_a ON shared_style(artist_a_id);
CREATE INDEX IF NOT EXISTS idx_shared_style_b ON shared_style(artist_b_id);
CREATE INDEX IF NOT EXISTS idx_label_family_a ON label_family(artist_a_id);
CREATE INDEX IF NOT EXISTS idx_label_family_b ON label_family(artist_b_id);
CREATE INDEX IF NOT EXISTS idx_compilation_a ON compilation(artist_a_id);
CREATE INDEX IF NOT EXISTS idx_compilation_b ON compilation(artist_b_id);
"""

# Edge and enrichment tables only — used when entity_store manages the artist table.
# Excludes artist (managed by entity store) and artist_style (entity store uses
# column ``style`` rather than ``style_tag``).
_EDGE_ENRICHMENT_SCHEMA = """
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

CREATE TABLE IF NOT EXISTS artist_personnel (
    artist_id INTEGER NOT NULL REFERENCES artist(id),
    personnel_name TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS artist_label (
    artist_id INTEGER NOT NULL REFERENCES artist(id),
    label_name TEXT NOT NULL,
    label_id INTEGER,
    PRIMARY KEY (artist_id, label_name)
);

CREATE TABLE IF NOT EXISTS shared_personnel (
    artist_a_id INTEGER NOT NULL REFERENCES artist(id),
    artist_b_id INTEGER NOT NULL REFERENCES artist(id),
    shared_count INTEGER NOT NULL,
    shared_names TEXT NOT NULL,
    PRIMARY KEY (artist_a_id, artist_b_id)
);

CREATE TABLE IF NOT EXISTS shared_style (
    artist_a_id INTEGER NOT NULL REFERENCES artist(id),
    artist_b_id INTEGER NOT NULL REFERENCES artist(id),
    jaccard REAL NOT NULL,
    shared_tags TEXT NOT NULL,
    PRIMARY KEY (artist_a_id, artist_b_id)
);

CREATE TABLE IF NOT EXISTS label_family (
    artist_a_id INTEGER NOT NULL REFERENCES artist(id),
    artist_b_id INTEGER NOT NULL REFERENCES artist(id),
    shared_labels TEXT NOT NULL,
    PRIMARY KEY (artist_a_id, artist_b_id)
);

CREATE TABLE IF NOT EXISTS compilation (
    artist_a_id INTEGER NOT NULL REFERENCES artist(id),
    artist_b_id INTEGER NOT NULL REFERENCES artist(id),
    compilation_count INTEGER NOT NULL,
    compilation_titles TEXT NOT NULL,
    PRIMARY KEY (artist_a_id, artist_b_id)
);

CREATE INDEX IF NOT EXISTS idx_transition_source ON dj_transition(source_id, pmi DESC);
CREATE INDEX IF NOT EXISTS idx_transition_target ON dj_transition(target_id, pmi DESC);
CREATE INDEX IF NOT EXISTS idx_xref_a ON cross_reference(artist_a_id);
CREATE INDEX IF NOT EXISTS idx_xref_b ON cross_reference(artist_b_id);
CREATE INDEX IF NOT EXISTS idx_shared_personnel_a ON shared_personnel(artist_a_id);
CREATE INDEX IF NOT EXISTS idx_shared_personnel_b ON shared_personnel(artist_b_id);
CREATE INDEX IF NOT EXISTS idx_shared_style_a ON shared_style(artist_a_id);
CREATE INDEX IF NOT EXISTS idx_shared_style_b ON shared_style(artist_b_id);
CREATE INDEX IF NOT EXISTS idx_label_family_a ON label_family(artist_a_id);
CREATE INDEX IF NOT EXISTS idx_label_family_b ON label_family(artist_b_id);
CREATE INDEX IF NOT EXISTS idx_compilation_a ON compilation(artist_a_id);
CREATE INDEX IF NOT EXISTS idx_compilation_b ON compilation(artist_b_id);
"""


def export_sqlite(
    path: str,
    artist_stats: dict[str, ArtistStats],
    pmi_edges: list[PmiEdge],
    xref_edges: list[CrossReferenceEdge],
    min_count: int = 2,
    enrichments: dict[str, ArtistEnrichment] | None = None,
    shared_personnel_edges: list[SharedPersonnelEdge] | None = None,
    shared_style_edges: list[SharedStyleEdge] | None = None,
    label_family_edges: list[LabelFamilyEdge] | None = None,
    compilation_edges: list[CompilationEdge] | None = None,
    entity_store: EntityStore | None = None,
) -> None:
    """Export the artist graph to a SQLite database.

    Args:
        path: Output path for the SQLite database file.
        artist_stats: Per-artist statistics for the artist table.
        pmi_edges: PMI-weighted DJ transition edges.
        xref_edges: Cross-reference edges from the library catalog.
        min_count: Minimum raw co-occurrence count for DJ transition edges.
        enrichments: Optional Discogs enrichment data per artist.
        shared_personnel_edges: Optional shared personnel edges.
        shared_style_edges: Optional shared style edges.
        label_family_edges: Optional label family edges.
        compilation_edges: Optional compilation co-appearance edges.
        entity_store: Optional entity store. When provided, the artist table
            is assumed to already exist and be populated. Stats are updated
            via the store, and ID mapping comes from the store.
    """
    enrichments = enrichments or {}

    if entity_store is not None:
        _export_with_entity_store(
            path,
            entity_store,
            artist_stats,
            pmi_edges,
            xref_edges,
            min_count,
            enrichments,
            shared_personnel_edges or [],
            shared_style_edges or [],
            label_family_edges or [],
            compilation_edges or [],
        )
    else:
        _export_standalone(
            path,
            artist_stats,
            pmi_edges,
            xref_edges,
            min_count,
            enrichments,
            shared_personnel_edges or [],
            shared_style_edges or [],
            label_family_edges or [],
            compilation_edges or [],
        )


def _export_standalone(
    path: str,
    artist_stats: dict[str, ArtistStats],
    pmi_edges: list[PmiEdge],
    xref_edges: list[CrossReferenceEdge],
    min_count: int,
    enrichments: dict[str, ArtistEnrichment],
    shared_personnel_edges: list[SharedPersonnelEdge],
    shared_style_edges: list[SharedStyleEdge],
    label_family_edges: list[LabelFamilyEdge],
    compilation_edges: list[CompilationEdge],
) -> None:
    """Original export path: create all tables from scratch."""
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
        enrich = enrichments.get(name)
        discogs_id = enrich.discogs_artist_id if enrich else None
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
                    discogs_id,
                )
            )
        else:
            artist_rows.append((name, None, 0, None, None, 0, 0.0, 0, discogs_id))

    conn.executemany(
        """
        INSERT INTO artist (
            canonical_name, genre, total_plays,
            active_first_year, active_last_year,
            dj_count, request_ratio, show_count, discogs_artist_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        artist_rows,
    )

    # Build name → id mapping
    name_to_id: dict[str, int] = {}
    for row in conn.execute("SELECT id, canonical_name FROM artist"):
        name_to_id[row[1]] = row[0]

    # Insert edges and enrichments
    _insert_edges(conn, name_to_id, pmi_edges, xref_edges, min_count)
    _insert_enrichments(conn, enrichments, name_to_id)
    _insert_discogs_edges(
        conn,
        name_to_id,
        shared_personnel_edges,
        shared_style_edges,
        label_family_edges,
        compilation_edges,
    )

    conn.commit()
    _log_counts(conn)
    conn.close()


def _export_with_entity_store(
    path: str,
    entity_store: EntityStore,
    artist_stats: dict[str, ArtistStats],
    pmi_edges: list[PmiEdge],
    xref_edges: list[CrossReferenceEdge],
    min_count: int,
    enrichments: dict[str, ArtistEnrichment],
    shared_personnel_edges: list[SharedPersonnelEdge],
    shared_style_edges: list[SharedStyleEdge],
    label_family_edges: list[LabelFamilyEdge],
    compilation_edges: list[CompilationEdge],
) -> None:
    """Export path when an entity store manages the artist table."""
    conn = entity_store._conn

    # Create only edge and enrichment tables (artist + artist_style already exist)
    conn.executescript(_EDGE_ENRICHMENT_SCHEMA)

    # Ensure xref-only artists exist in the entity store
    xref_names: set[str] = set()
    for xref in xref_edges:
        xref_names.add(xref.artist_a)
        xref_names.add(xref.artist_b)

    existing = set(entity_store.get_name_to_id_mapping().keys())
    missing = xref_names - existing
    if missing:
        entity_store.bulk_upsert_artists(sorted(missing))

    # Update stats via entity store
    if artist_stats:
        entity_store.bulk_update_stats(artist_stats)

    # Get ID mapping from entity store
    name_to_id = entity_store.get_name_to_id_mapping()

    # Insert edges
    _insert_edges(conn, name_to_id, pmi_edges, xref_edges, min_count)

    # Insert enrichments — styles go through entity store, personnel/labels go to tables
    _insert_enrichments_with_store(conn, entity_store, enrichments, name_to_id)

    # Insert Discogs-derived edges
    _insert_discogs_edges(
        conn,
        name_to_id,
        shared_personnel_edges,
        shared_style_edges,
        label_family_edges,
        compilation_edges,
    )

    conn.commit()
    _log_counts(conn)


def _insert_edges(
    conn: sqlite3.Connection,
    name_to_id: dict[str, int],
    pmi_edges: list[PmiEdge],
    xref_edges: list[CrossReferenceEdge],
    min_count: int,
) -> None:
    """Insert DJ transition and cross-reference edges."""
    # DJ transitions (filtered)
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

    # Cross-references
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


def _insert_enrichments(
    conn: sqlite3.Connection,
    enrichments: dict[str, ArtistEnrichment],
    name_to_id: dict[str, int],
) -> None:
    """Insert per-artist enrichment data (styles, personnel, labels)."""
    style_rows = []
    personnel_rows = []
    label_rows = []

    for name, enrich in enrichments.items():
        artist_id = name_to_id.get(name)
        if artist_id is None:
            continue

        for style in enrich.styles:
            style_rows.append((artist_id, style))

        for credit in enrich.personnel:
            for role in credit.roles or [""]:
                personnel_rows.append((artist_id, credit.name, role or ""))

        for label in enrich.labels:
            label_rows.append((artist_id, label.name, label.label_id))

    if style_rows:
        conn.executemany(
            "INSERT OR IGNORE INTO artist_style (artist_id, style_tag) VALUES (?, ?)",
            style_rows,
        )
    if personnel_rows:
        conn.executemany(
            "INSERT OR IGNORE INTO artist_personnel (artist_id, personnel_name, role) VALUES (?, ?, ?)",
            personnel_rows,
        )
    if label_rows:
        conn.executemany(
            "INSERT OR IGNORE INTO artist_label (artist_id, label_name, label_id) VALUES (?, ?, ?)",
            label_rows,
        )


def _insert_enrichments_with_store(
    conn: sqlite3.Connection,
    entity_store: EntityStore,
    enrichments: dict[str, ArtistEnrichment],
    name_to_id: dict[str, int],
) -> None:
    """Insert enrichment data, routing styles through the entity store.

    The entity store's ``artist_style`` table uses column ``style`` (not
    ``style_tag``), so styles must go through ``persist_artist_styles()``.
    Personnel and labels are inserted into their own tables as usual.
    """
    personnel_rows = []
    label_rows = []

    for name, enrich in enrichments.items():
        artist_id = name_to_id.get(name)
        if artist_id is None:
            continue

        if enrich.styles:
            entity_store.persist_artist_styles(artist_id, enrich.styles)

        for credit in enrich.personnel:
            for role in credit.roles or [""]:
                personnel_rows.append((artist_id, credit.name, role or ""))

        for label in enrich.labels:
            label_rows.append((artist_id, label.name, label.label_id))

    if personnel_rows:
        conn.executemany(
            "INSERT OR IGNORE INTO artist_personnel (artist_id, personnel_name, role) VALUES (?, ?, ?)",
            personnel_rows,
        )
    if label_rows:
        conn.executemany(
            "INSERT OR IGNORE INTO artist_label (artist_id, label_name, label_id) VALUES (?, ?, ?)",
            label_rows,
        )


def _resolve_edge_rows(
    rows: list[tuple],
    name_to_id: dict[str, int],
) -> list[tuple]:
    """Map artist names in first two columns to IDs."""
    resolved = []
    for row in rows:
        a_id = name_to_id.get(row[0])
        b_id = name_to_id.get(row[1])
        if a_id is not None and b_id is not None:
            resolved.append((a_id, b_id, *row[2:]))
    return resolved


def _insert_discogs_edges(
    conn: sqlite3.Connection,
    name_to_id: dict[str, int],
    shared_personnel: list[SharedPersonnelEdge],
    shared_styles: list[SharedStyleEdge],
    label_family: list[LabelFamilyEdge],
    compilations: list[CompilationEdge],
) -> None:
    """Insert Discogs-derived edge tables."""
    rows = _resolve_edge_rows(
        [
            (e.artist_a, e.artist_b, e.shared_count, json.dumps(e.shared_names))
            for e in shared_personnel
        ],
        name_to_id,
    )
    if rows:
        conn.executemany(
            "INSERT OR IGNORE INTO shared_personnel (artist_a_id, artist_b_id, shared_count, shared_names) VALUES (?, ?, ?, ?)",
            rows,
        )

    rows = _resolve_edge_rows(
        [(e.artist_a, e.artist_b, e.jaccard, json.dumps(e.shared_tags)) for e in shared_styles],
        name_to_id,
    )
    if rows:
        conn.executemany(
            "INSERT OR IGNORE INTO shared_style (artist_a_id, artist_b_id, jaccard, shared_tags) VALUES (?, ?, ?, ?)",
            rows,
        )

    rows = _resolve_edge_rows(
        [(e.artist_a, e.artist_b, json.dumps(e.shared_labels)) for e in label_family],
        name_to_id,
    )
    if rows:
        conn.executemany(
            "INSERT OR IGNORE INTO label_family (artist_a_id, artist_b_id, shared_labels) VALUES (?, ?, ?)",
            rows,
        )

    rows = _resolve_edge_rows(
        [
            (e.artist_a, e.artist_b, e.compilation_count, json.dumps(e.compilation_titles))
            for e in compilations
        ],
        name_to_id,
    )
    if rows:
        conn.executemany(
            "INSERT OR IGNORE INTO compilation (artist_a_id, artist_b_id, compilation_count, compilation_titles) VALUES (?, ?, ?, ?)",
            rows,
        )


def _log_counts(conn: sqlite3.Connection) -> None:
    """Log summary counts for all tables."""
    artist_count = conn.execute("SELECT COUNT(*) FROM artist").fetchone()[0]
    transition_count = conn.execute("SELECT COUNT(*) FROM dj_transition").fetchone()[0]
    xref_count = conn.execute("SELECT COUNT(*) FROM cross_reference").fetchone()[0]
    logger.info(
        "SQLite export: %d artists, %d DJ transitions, %d cross-references",
        artist_count,
        transition_count,
        xref_count,
    )

    for table in ("shared_personnel", "shared_style", "label_family", "compilation"):
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608
        if count > 0:
            logger.info("  %s: %d edges", table, count)
