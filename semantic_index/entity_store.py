"""Persistent entity store for reconciled artist identities.

Manages the entity store tables within a SQLite database: entity (Wikidata QID,
display name, type), artist migration (adds external ID columns to existing artist
tables), and Phase 2 placeholder tables (release, label, label_hierarchy).

The ``initialize()`` method is fully idempotent — safe on a fresh database,
an old-schema database, or an already-migrated database.
"""

from __future__ import annotations

import logging
import sqlite3
from types import TracebackType

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
        """Conditionally add new columns to an existing artist table.

        Uses PRAGMA table_info to detect which columns already exist and
        only adds the missing ones. No-op if the table doesn't exist or
        all columns are already present.
        """
        if not self._has_table("artist"):
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
