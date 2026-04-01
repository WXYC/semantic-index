"""Persistent entity store for reconciled artist identities.

Manages the entity store tables within a SQLite database: entity (Wikidata QID,
display name, type), artist migration (adds external ID columns to existing artist
tables), and Phase 2 placeholder tables (release, label, label_hierarchy).

Provides CRUD operations for entities, artist upsert with COALESCE semantics
(never overwrites populated fields with NULL), and bulk stat updates.

The ``initialize()`` method is fully idempotent — safe on a fresh database,
an old-schema database, or an already-migrated database.
"""

from __future__ import annotations

import logging
import sqlite3
from types import TracebackType

from semantic_index.models import ArtistStats, Entity, ReconciliationEvent

logger = logging.getLogger(__name__)

# Columns added to the artist table by _migrate_artist_table().
# Each entry is (column_name, alter_table_definition).
# ALTER TABLE ADD COLUMN requires constant defaults, so timestamps use NULL here
# and are backfilled with strftime() after all columns are added.
_NEW_ARTIST_COLUMNS = [
    ("entity_id", "INTEGER REFERENCES entity(id)"),
    ("musicbrainz_artist_id", "TEXT"),
    ("wxyc_library_code_id", "INTEGER"),
    ("reconciliation_status", "TEXT NOT NULL DEFAULT 'unreconciled'"),
    ("created_at", "TEXT"),
    ("updated_at", "TEXT"),
]

_ENTITY_STORE_SCHEMA = """
-- Real-world person, group, or organization.
CREATE TABLE IF NOT EXISTS entity (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wikidata_qid TEXT UNIQUE,
    name TEXT NOT NULL,
    entity_type TEXT NOT NULL DEFAULT 'artist',
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_entity_qid ON entity(wikidata_qid) WHERE wikidata_qid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_entity_type ON entity(entity_type);

-- Phase 2 tables (created now, populated later)

CREATE TABLE IF NOT EXISTS release (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    artist_id INTEGER NOT NULL REFERENCES artist(id),
    wxyc_library_release_id INTEGER,
    discogs_master_id INTEGER,
    discogs_release_id INTEGER,
    musicbrainz_release_group_id TEXT,
    year INTEGER,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS label (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    entity_id INTEGER REFERENCES entity(id),
    discogs_label_id INTEGER,
    musicbrainz_label_id TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS label_hierarchy (
    parent_label_id INTEGER NOT NULL REFERENCES label(id),
    child_label_id INTEGER NOT NULL REFERENCES label(id),
    source TEXT NOT NULL DEFAULT 'wikidata',
    PRIMARY KEY (parent_label_id, child_label_id)
);

-- Per-artist style tags from Discogs (persisted during reconciliation)

CREATE TABLE IF NOT EXISTS artist_style (
    artist_id INTEGER NOT NULL REFERENCES artist(id),
    style_tag TEXT NOT NULL,
    PRIMARY KEY (artist_id, style_tag)
);

-- Reconciliation audit trail

CREATE TABLE IF NOT EXISTS reconciliation_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    artist_id INTEGER NOT NULL REFERENCES artist(id),
    source TEXT NOT NULL,
    external_id TEXT NOT NULL,
    confidence REAL,
    method TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_reconciliation_artist ON reconciliation_log(artist_id);
"""

_ARTIST_TABLE_SCHEMA = """
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
    discogs_artist_id INTEGER,
    entity_id INTEGER REFERENCES entity(id),
    musicbrainz_artist_id TEXT,
    wxyc_library_code_id INTEGER,
    reconciliation_status TEXT NOT NULL DEFAULT 'unreconciled',
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
"""

_ARTIST_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_artist_entity ON artist(entity_id);
CREATE INDEX IF NOT EXISTS idx_artist_discogs ON artist(discogs_artist_id) WHERE discogs_artist_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_artist_musicbrainz ON artist(musicbrainz_artist_id) WHERE musicbrainz_artist_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_artist_library_code ON artist(wxyc_library_code_id) WHERE wxyc_library_code_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_artist_reconciliation ON artist(reconciliation_status);
"""


class EntityStore:
    """Manages entity store tables within a SQLite database.

    Args:
        db_path: Path to the SQLite database file.
    """

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")

    def initialize(self) -> None:
        """Create entity store tables and migrate the artist table if needed.

        This is fully idempotent — safe to call on a fresh database, an
        old-schema database, or an already-migrated database.
        """
        self._conn.executescript(_ENTITY_STORE_SCHEMA)
        self._migrate_artist_table()
        if self._has_table("artist"):
            self._conn.executescript(_ARTIST_INDEXES)
        self._conn.commit()
        logger.info("Entity store initialized: %s", self._db_path)

    def _has_table(self, name: str) -> bool:
        """Check whether a table exists in the database."""
        row = self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ).fetchone()
        return row is not None

    def _migrate_artist_table(self) -> None:
        """Create the artist table if missing, or add new columns to an existing one.

        On a fresh database, creates the full artist table with all entity
        store columns. On an old-schema database, uses PRAGMA table_info to
        detect which columns already exist and only adds the missing ones.
        """
        if not self._has_table("artist"):
            self._conn.executescript(_ARTIST_TABLE_SCHEMA)
            logger.info("Created artist table with full entity store schema")
            return

        existing = {r[1] for r in self._conn.execute("PRAGMA table_info(artist)")}
        added = []
        for col_name, col_def in _NEW_ARTIST_COLUMNS:
            if col_name not in existing:
                self._conn.execute(f"ALTER TABLE artist ADD COLUMN {col_name} {col_def}")
                added.append(col_name)
                logger.info("Migrated artist table: added column %s", col_name)

        # Backfill timestamps for existing rows (ALTER TABLE can't use strftime default)
        if "created_at" in added or "updated_at" in added:
            self._conn.execute(
                """UPDATE artist
                   SET created_at = COALESCE(created_at, strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                       updated_at = COALESCE(updated_at, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))"""
            )

    # ------------------------------------------------------------------
    # Entity CRUD
    # ------------------------------------------------------------------

    def get_or_create_entity(
        self,
        name: str,
        entity_type: str,
        wikidata_qid: str | None = None,
    ) -> Entity:
        """Return an existing entity by name+type, or create a new one.

        If the entity already exists, its wikidata_qid is not overwritten.

        Args:
            name: Display name of the entity.
            entity_type: One of 'artist', 'label', etc.
            wikidata_qid: Optional Wikidata QID to set on creation.

        Returns:
            The existing or newly created Entity.
        """
        row = self._conn.execute(
            "SELECT id, wikidata_qid, name, entity_type FROM entity WHERE name = ? AND entity_type = ?",
            (name, entity_type),
        ).fetchone()
        if row is not None:
            return Entity(id=row[0], wikidata_qid=row[1], name=row[2], entity_type=row[3])

        cur = self._conn.execute(
            "INSERT INTO entity (name, entity_type, wikidata_qid) VALUES (?, ?, ?)",
            (name, entity_type, wikidata_qid),
        )
        self._conn.commit()
        return Entity(
            id=cur.lastrowid,  # type: ignore[arg-type]
            name=name,
            entity_type=entity_type,
            wikidata_qid=wikidata_qid,
        )

    def update_entity_qid(self, entity_id: int, wikidata_qid: str) -> None:
        """Set the Wikidata QID for an entity.

        Args:
            entity_id: The entity's primary key.
            wikidata_qid: The Wikidata QID to assign.

        Raises:
            ValueError: If no entity with the given id exists.
        """
        cur = self._conn.execute(
            "UPDATE entity SET wikidata_qid = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?",
            (wikidata_qid, entity_id),
        )
        if cur.rowcount == 0:
            raise ValueError(f"No entity with id {entity_id}")
        self._conn.commit()

    def get_entity_by_qid(self, wikidata_qid: str) -> Entity | None:
        """Look up an entity by Wikidata QID.

        Args:
            wikidata_qid: The Wikidata QID to search for.

        Returns:
            The matching Entity, or None if not found.
        """
        row = self._conn.execute(
            "SELECT id, wikidata_qid, name, entity_type FROM entity WHERE wikidata_qid = ?",
            (wikidata_qid,),
        ).fetchone()
        if row is None:
            return None
        return Entity(id=row[0], wikidata_qid=row[1], name=row[2], entity_type=row[3])

    def merge_entities(self, keep_id: int, merge_id: int) -> None:
        """Merge two entities: re-parent artists from merge_id to keep_id, then delete merge_id.

        Args:
            keep_id: The entity ID to keep.
            merge_id: The entity ID to merge into keep_id and delete.

        Raises:
            ValueError: If keep_id == merge_id, or if either entity doesn't exist.
        """
        if keep_id == merge_id:
            raise ValueError("Cannot merge an entity into itself")

        for eid, label in [(keep_id, "keep"), (merge_id, "merge")]:
            row = self._conn.execute("SELECT id FROM entity WHERE id = ?", (eid,)).fetchone()
            if row is None:
                raise ValueError(f"No entity with id {eid} ({label})")

        self._conn.execute(
            "UPDATE artist SET entity_id = ? WHERE entity_id = ?", (keep_id, merge_id)
        )
        self._conn.execute("DELETE FROM entity WHERE id = ?", (merge_id,))
        self._conn.commit()

    # ------------------------------------------------------------------
    # Artist Upsert
    # ------------------------------------------------------------------

    def upsert_artist(
        self,
        canonical_name: str,
        *,
        genre: str | None = None,
        discogs_artist_id: int | None = None,
        entity_id: int | None = None,
        musicbrainz_artist_id: str | None = None,
        wxyc_library_code_id: int | None = None,
    ) -> int:
        """Insert or update an artist row using COALESCE semantics.

        On conflict (canonical_name), only updates fields where the new value
        is not NULL — existing populated fields are never overwritten with NULL.

        Args:
            canonical_name: The artist's canonical name (unique key).
            genre: Genre string.
            discogs_artist_id: Discogs artist ID.
            entity_id: FK to the entity table.
            musicbrainz_artist_id: MusicBrainz artist UUID.
            wxyc_library_code_id: WXYC library code ID.

        Returns:
            The artist row's integer primary key.
        """
        cur = self._conn.execute(
            """INSERT INTO artist (canonical_name, genre, discogs_artist_id, entity_id,
                                   musicbrainz_artist_id, wxyc_library_code_id,
                                   updated_at)
               VALUES (?, ?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
               ON CONFLICT(canonical_name) DO UPDATE SET
                   genre = COALESCE(excluded.genre, artist.genre),
                   discogs_artist_id = COALESCE(excluded.discogs_artist_id, artist.discogs_artist_id),
                   entity_id = COALESCE(excluded.entity_id, artist.entity_id),
                   musicbrainz_artist_id = COALESCE(excluded.musicbrainz_artist_id, artist.musicbrainz_artist_id),
                   wxyc_library_code_id = COALESCE(excluded.wxyc_library_code_id, artist.wxyc_library_code_id),
                   updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
               RETURNING id""",
            (
                canonical_name,
                genre,
                discogs_artist_id,
                entity_id,
                musicbrainz_artist_id,
                wxyc_library_code_id,
            ),
        )
        row = cur.fetchone()
        self._conn.commit()
        assert row is not None  # RETURNING always yields a row
        return int(row[0])

    def get_artist_by_name(self, canonical_name: str) -> dict[str, object] | None:
        """Look up an artist row by canonical name.

        Args:
            canonical_name: The artist's canonical name.

        Returns:
            A dict of column name -> value, or None if not found.
        """
        self._conn.row_factory = sqlite3.Row
        row = self._conn.execute(
            "SELECT * FROM artist WHERE canonical_name = ?", (canonical_name,)
        ).fetchone()
        self._conn.row_factory = None
        if row is None:
            return None
        return dict(row)

    def bulk_upsert_artists(self, names: list[str]) -> dict[str, int]:
        """Insert or retrieve artist rows for a list of canonical names.

        This is the main pipeline entry point for ensuring artist rows exist.
        Deduplicates the input list.

        Args:
            names: List of canonical artist names.

        Returns:
            Dict mapping canonical_name -> artist row id.
        """
        unique_names = list(dict.fromkeys(names))
        result: dict[str, int] = {}
        for name in unique_names:
            result[name] = self.upsert_artist(name)
        return result

    # ------------------------------------------------------------------
    # Reconciliation Log
    # ------------------------------------------------------------------

    def log_reconciliation(
        self,
        artist_id: int,
        source: str,
        external_id: str,
        confidence: float | None,
        method: str,
    ) -> None:
        """Record a reconciliation event for an artist.

        Args:
            artist_id: The artist row's primary key.
            source: External knowledge base ('discogs', 'musicbrainz', 'wikidata').
            external_id: Identifier in the external source.
            confidence: Match confidence score, or None if not applicable.
            method: Matching method ('exact', 'fuzzy', 'api_search', 'cache_lookup').

        Raises:
            ValueError: If no artist with the given id exists.
        """
        row = self._conn.execute("SELECT id FROM artist WHERE id = ?", (artist_id,)).fetchone()
        if row is None:
            raise ValueError(f"No artist with id {artist_id}")
        self._conn.execute(
            """INSERT INTO reconciliation_log (artist_id, source, external_id, confidence, method)
               VALUES (?, ?, ?, ?, ?)""",
            (artist_id, source, external_id, confidence, method),
        )
        self._conn.commit()

    def get_reconciliation_history(self, artist_id: int) -> list[ReconciliationEvent]:
        """Return all reconciliation events for an artist, ordered by insertion.

        Args:
            artist_id: The artist row's primary key.

        Returns:
            List of ReconciliationEvent instances, oldest first.
        """
        rows = self._conn.execute(
            """SELECT source, external_id, confidence, method
               FROM reconciliation_log
               WHERE artist_id = ?
               ORDER BY id""",
            (artist_id,),
        ).fetchall()
        return [
            ReconciliationEvent(
                source=row[0],
                external_id=row[1],
                confidence=row[2],
                method=row[3],
            )
            for row in rows
        ]

    # ------------------------------------------------------------------
    # Reconciliation Queries
    # ------------------------------------------------------------------

    def get_unreconciled_artists(self, limit: int | None = None) -> list[tuple[int, str]]:
        """Return (id, canonical_name) pairs for artists with status 'unreconciled'.

        Args:
            limit: Maximum number of artists to return. None for all.

        Returns:
            List of (artist_id, canonical_name) tuples.
        """
        sql = "SELECT id, canonical_name FROM artist WHERE reconciliation_status = 'unreconciled'"
        if limit is not None:
            sql += f" LIMIT {limit}"
        rows = self._conn.execute(sql).fetchall()
        return [(row[0], row[1]) for row in rows]

    def update_reconciliation_status(self, artist_id: int, status: str) -> None:
        """Update the reconciliation status for an artist.

        Args:
            artist_id: The artist row's primary key.
            status: New status ('reconciled', 'no_match', 'partial', etc.).

        Raises:
            ValueError: If no artist with the given id exists.
        """
        cur = self._conn.execute(
            """UPDATE artist SET reconciliation_status = ?,
                   updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
               WHERE id = ?""",
            (status, artist_id),
        )
        if cur.rowcount == 0:
            raise ValueError(f"No artist with id {artist_id}")
        self._conn.commit()

    # ------------------------------------------------------------------
    # Artist Styles
    # ------------------------------------------------------------------

    def persist_artist_styles(self, artist_id: int, styles: list[str]) -> None:
        """Persist Discogs style tags for an artist (idempotent).

        Uses INSERT OR IGNORE so overlapping styles from repeat calls
        do not raise errors or create duplicates.

        Args:
            artist_id: The artist row's primary key.
            styles: List of Discogs style strings.
        """
        if not styles:
            return
        self._conn.executemany(
            "INSERT OR IGNORE INTO artist_style (artist_id, style_tag) VALUES (?, ?)",
            [(artist_id, style) for style in styles],
        )
        self._conn.commit()

    def get_artist_styles(self, artist_id: int) -> list[str]:
        """Return all style tags for an artist, sorted alphabetically.

        Args:
            artist_id: The artist row's primary key.

        Returns:
            Sorted list of style strings.
        """
        rows = self._conn.execute(
            "SELECT style_tag FROM artist_style WHERE artist_id = ? ORDER BY style_tag",
            (artist_id,),
        ).fetchall()
        return [row[0] for row in rows]

    # ------------------------------------------------------------------
    # Name-to-ID Mapping
    # ------------------------------------------------------------------

    def get_name_to_id_mapping(self) -> dict[str, int]:
        """Return a mapping of canonical_name to artist row id.

        This replaces the inline dict-building pattern in sqlite_export.py.

        Returns:
            Dict mapping canonical_name -> artist row id.
        """
        rows = self._conn.execute("SELECT id, canonical_name FROM artist").fetchall()
        return {row[1]: row[0] for row in rows}

    # ------------------------------------------------------------------
    # Artist Stats
    # ------------------------------------------------------------------

    def update_artist_stats(self, canonical_name: str, stats: ArtistStats) -> None:
        """Update stats columns for a single artist.

        Args:
            canonical_name: The artist's canonical name.
            stats: The ArtistStats to apply.

        Raises:
            ValueError: If no artist with the given name exists.
        """
        cur = self._conn.execute(
            """UPDATE artist SET
                   total_plays = ?,
                   genre = COALESCE(?, genre),
                   active_first_year = ?,
                   active_last_year = ?,
                   dj_count = ?,
                   request_ratio = ?,
                   show_count = ?,
                   updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
               WHERE canonical_name = ?""",
            (
                stats.total_plays,
                stats.genre,
                stats.active_first_year,
                stats.active_last_year,
                stats.dj_count,
                stats.request_ratio,
                stats.show_count,
                canonical_name,
            ),
        )
        if cur.rowcount == 0:
            raise ValueError(f"No artist with canonical_name '{canonical_name}'")
        self._conn.commit()

    def bulk_update_stats(self, artist_stats: dict[str, ArtistStats]) -> None:
        """Update stats for multiple artists in a single transaction.

        Args:
            artist_stats: Dict mapping canonical_name -> ArtistStats.
        """
        for name, stats in artist_stats.items():
            self.update_artist_stats(name, stats)

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()

    def __enter__(self) -> EntityStore:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()
