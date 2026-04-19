"""Pipeline database manager for the SQLite graph database.

Manages the SQLite schema (artist, entity, label, and edge tables),
artist CRUD with COALESCE upsert semantics, bulk stats updates, style
persistence, and entity deduplication. This is the slimmed-down successor
to entity_store.py, with all identity resolution code removed (now owned
by LML via the --entity-source=lml path).
"""

from __future__ import annotations

import logging
import sqlite3
from types import TracebackType

from semantic_index.models import ArtistStats, DeduplicationReport

logger = logging.getLogger(__name__)

# Columns added to the artist table by _migrate_artist_table().
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
    wikidata_qid TEXT,
    name TEXT NOT NULL,
    entity_type TEXT NOT NULL DEFAULT 'artist',
    spotify_artist_id TEXT,
    apple_music_artist_id TEXT,
    bandcamp_id TEXT,
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

# Symmetric edge tables that need re-keying during entity dedup.
# Each stores undirected edges as (artist_a_id, artist_b_id) composite PKs.
_SYMMETRIC_EDGE_TABLES = (
    "shared_personnel",
    "shared_style",
    "label_family",
    "compilation",
    "acoustic_similarity",
)

# Directed edge tables keyed by (source_id, target_id).
_DIRECTED_EDGE_TABLES = ("wikidata_influence",)

# Single-artist tables with artist_id as sole PK.
_SINGLE_ARTIST_PK_TABLES = ("audio_profile",)

# Single-artist tables with composite PK including artist_id.
# Tuples of (table_name, pk_column_besides_artist_id).
_SINGLE_ARTIST_COMPOSITE_PK_TABLES = (
    ("artist_style", "style_tag"),
    ("artist_label", "label_name"),
)

# Single-artist tables with no PK constraint (just an FK).
_SINGLE_ARTIST_FK_TABLES = ("artist_personnel",)

_ENTITY_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_entity_spotify ON entity(spotify_artist_id) WHERE spotify_artist_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_entity_apple_music ON entity(apple_music_artist_id) WHERE apple_music_artist_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_entity_bandcamp ON entity(bandcamp_id) WHERE bandcamp_id IS NOT NULL;
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


def _ensure_columns(
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


class PipelineDB:
    """Manages the SQLite graph database for the pipeline.

    Handles schema creation/migration, artist CRUD with COALESCE upsert
    semantics, bulk stats updates, style persistence, and entity
    deduplication by shared Wikidata QID.

    Args:
        db_path: Path to the SQLite database file.
    """

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")

    def initialize(self) -> None:
        """Create tables and migrate the artist table if needed.

        Fully idempotent -- safe on fresh, old-schema, or already-migrated databases.
        """
        self._migrate_entity_unique_qid()
        self._conn.executescript(_ENTITY_STORE_SCHEMA)
        self._migrate_entity_table()
        self._conn.executescript(_ENTITY_INDEXES)
        self._migrate_artist_table()
        if self._has_table("artist"):
            self._conn.executescript(_ARTIST_INDEXES)
        self._conn.commit()
        logger.info("Pipeline DB initialized: %s", self._db_path)

    def _migrate_entity_unique_qid(self) -> None:
        """Rebuild the entity table without the UNIQUE constraint on wikidata_qid."""
        if not self._has_table("entity"):
            return

        indexes = self._conn.execute("PRAGMA index_list(entity)").fetchall()
        has_unique_qid = False
        for idx in indexes:
            idx_name = idx[1]
            is_unique = bool(idx[2])
            if is_unique:
                cols = self._conn.execute(f"PRAGMA index_info('{idx_name}')").fetchall()
                if any(col[2] == "wikidata_qid" for col in cols):
                    has_unique_qid = True
                    break

        if not has_unique_qid:
            return

        logger.info("Rebuilding entity table to remove UNIQUE constraint on wikidata_qid")
        self._conn.execute("PRAGMA foreign_keys=OFF")
        self._conn.execute("ALTER TABLE entity RENAME TO _entity_old")
        self._conn.execute("""CREATE TABLE entity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wikidata_qid TEXT,
                name TEXT NOT NULL,
                entity_type TEXT NOT NULL DEFAULT 'artist',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            )""")
        self._conn.execute("INSERT INTO entity SELECT * FROM _entity_old")
        self._conn.execute("DROP TABLE _entity_old")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.commit()
        logger.info("Entity table rebuilt without UNIQUE constraint on wikidata_qid")

    _NEW_ENTITY_COLUMNS: list[tuple[str, str]] = [
        ("spotify_artist_id", "TEXT"),
        ("apple_music_artist_id", "TEXT"),
        ("bandcamp_id", "TEXT"),
    ]

    def _migrate_entity_table(self) -> None:
        """Add new columns to an existing entity table if they are missing."""
        if not self._has_table("entity"):
            return

        existing = {r[1] for r in self._conn.execute("PRAGMA table_info(entity)")}
        for col_name, col_def in self._NEW_ENTITY_COLUMNS:
            if col_name not in existing:
                self._conn.execute(f"ALTER TABLE entity ADD COLUMN {col_name} {col_def}")
                logger.info("Migrated entity table: added column %s", col_name)

    def _has_table(self, name: str) -> bool:
        """Check whether a table exists in the database."""
        row = self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ).fetchone()
        return row is not None

    def _migrate_artist_table(self) -> None:
        """Create the artist table if missing, or add new columns to an existing one."""
        if not self._has_table("artist"):
            self._conn.executescript(_ARTIST_TABLE_SCHEMA)
            logger.info("Created artist table with full schema")
            return

        added = _ensure_columns(self._conn, "artist", _NEW_ARTIST_COLUMNS)

        if "created_at" in added or "updated_at" in added:
            self._conn.execute("""UPDATE artist
                   SET created_at = COALESCE(created_at, strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                       updated_at = COALESCE(updated_at, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))""")

    # ------------------------------------------------------------------
    # Artist CRUD
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
        is not NULL -- existing populated fields are never overwritten with NULL.

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
        assert row is not None
        return int(row[0])

    def bulk_upsert_artists(self, names: list[str]) -> dict[str, int]:
        """Insert or retrieve artist rows for a list of canonical names.

        Returns:
            Dict mapping canonical_name -> artist row id.
        """
        unique_names = list(dict.fromkeys(names))
        result: dict[str, int] = {}
        for name in unique_names:
            result[name] = self.upsert_artist(name)
        return result

    def get_name_to_id_mapping(self) -> dict[str, int]:
        """Return a mapping of canonical_name to artist row id."""
        rows = self._conn.execute("SELECT id, canonical_name FROM artist").fetchall()
        return {row[1]: row[0] for row in rows}

    # ------------------------------------------------------------------
    # Artist Stats
    # ------------------------------------------------------------------

    def update_artist_stats(self, canonical_name: str, stats: ArtistStats) -> None:
        """Update stats columns for a single artist."""
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
        """Update stats for multiple artists in a single transaction."""
        for name, stats in artist_stats.items():
            self.update_artist_stats(name, stats)

    # ------------------------------------------------------------------
    # Artist Styles
    # ------------------------------------------------------------------

    def persist_artist_styles(self, artist_id: int, styles: list[str]) -> None:
        """Persist Discogs style tags for an artist (idempotent)."""
        if not styles:
            return
        self._conn.executemany(
            "INSERT OR IGNORE INTO artist_style (artist_id, style_tag) VALUES (?, ?)",
            [(artist_id, style) for style in styles],
        )
        self._conn.commit()

    # ------------------------------------------------------------------
    # Entity Deduplication
    # ------------------------------------------------------------------

    def find_duplicate_qid_groups(self) -> list[tuple[str, list[int]]]:
        """Find groups of entities sharing the same non-NULL Wikidata QID."""
        rows = self._conn.execute("""SELECT wikidata_qid, GROUP_CONCAT(id)
               FROM entity
               WHERE wikidata_qid IS NOT NULL
               GROUP BY wikidata_qid
               HAVING COUNT(*) > 1
               ORDER BY wikidata_qid""").fetchall()
        return [(row[0], sorted(int(x) for x in row[1].split(","))) for row in rows]

    def _rekey_symmetric_edges(self, keep_id: int, merge_id: int) -> int:
        """Re-key symmetric edge tables, replacing merge_id with keep_id.

        For each table in ``_SYMMETRIC_EDGE_TABLES``:
        1. Delete edges between merge_id and keep_id (would become self-referential).
        2. Delete edges that would create PK conflicts after re-keying (checks both
           ``(keep_id, X)`` and ``(X, keep_id)`` since tables are symmetric/unordered).
        3. UPDATE remaining edges to use keep_id.
        4. Delete any residual self-referential edges (defensive).

        Returns the total number of edge rows re-keyed (step 3 only).
        """
        total = 0
        for table in _SYMMETRIC_EDGE_TABLES:
            if not self._has_table(table):
                continue

            # 1. Delete edges between merge_id and keep_id (would become self-loops)
            self._conn.execute(
                f"DELETE FROM {table} WHERE "  # noqa: S608
                f"(artist_a_id = ? AND artist_b_id = ?) OR "
                f"(artist_a_id = ? AND artist_b_id = ?)",
                (merge_id, keep_id, keep_id, merge_id),
            )

            # 2a. Delete (merge_id, X) edges that would conflict with existing keep_id edges
            self._conn.execute(
                f"DELETE FROM {table} WHERE artist_a_id = ?1 AND ("  # noqa: S608
                f"  EXISTS (SELECT 1 FROM {table} t2"
                f"    WHERE t2.artist_a_id = ?2 AND t2.artist_b_id = {table}.artist_b_id)"
                f"  OR EXISTS (SELECT 1 FROM {table} t2"
                f"    WHERE t2.artist_a_id = {table}.artist_b_id AND t2.artist_b_id = ?2)"
                f")",
                (merge_id, keep_id),
            )

            # 2b. Delete (X, merge_id) edges that would conflict with existing keep_id edges
            self._conn.execute(
                f"DELETE FROM {table} WHERE artist_b_id = ?1 AND ("  # noqa: S608
                f"  EXISTS (SELECT 1 FROM {table} t2"
                f"    WHERE t2.artist_a_id = {table}.artist_a_id AND t2.artist_b_id = ?2)"
                f"  OR EXISTS (SELECT 1 FROM {table} t2"
                f"    WHERE t2.artist_a_id = ?2 AND t2.artist_b_id = {table}.artist_a_id)"
                f")",
                (merge_id, keep_id),
            )

            # 3. Re-key remaining edges
            cur = self._conn.execute(
                f"UPDATE {table} SET artist_a_id = ? WHERE artist_a_id = ?",  # noqa: S608
                (keep_id, merge_id),
            )
            total += cur.rowcount
            cur = self._conn.execute(
                f"UPDATE {table} SET artist_b_id = ? WHERE artist_b_id = ?",  # noqa: S608
                (keep_id, merge_id),
            )
            total += cur.rowcount

            # 4. Safety: delete any self-referential edges
            self._conn.execute(f"DELETE FROM {table} WHERE artist_a_id = artist_b_id")  # noqa: S608
        return total

    def _rekey_directed_edges(self, keep_id: int, merge_id: int) -> int:
        """Re-key directed edge tables, replacing merge_id with keep_id.

        For each table in ``_DIRECTED_EDGE_TABLES`` (keyed by ``source_id``/``target_id``):
        1. Delete edges between merge_id and keep_id (would become self-referential).
        2. Delete edges that would create PK conflicts after re-keying.
        3. UPDATE remaining edges to use keep_id.
        4. Delete any residual self-referential edges (defensive).

        Returns the total number of edge rows re-keyed (step 3 only).
        """
        total = 0
        for table in _DIRECTED_EDGE_TABLES:
            if not self._has_table(table):
                continue

            # 1. Delete edges between merge_id and keep_id
            self._conn.execute(
                f"DELETE FROM {table} WHERE "  # noqa: S608
                f"(source_id = ? AND target_id = ?) OR "
                f"(source_id = ? AND target_id = ?)",
                (merge_id, keep_id, keep_id, merge_id),
            )

            # 2a. Delete (merge_id, X) edges that conflict with existing (keep_id, X)
            self._conn.execute(
                f"DELETE FROM {table} WHERE source_id = ?1 AND "  # noqa: S608
                f"EXISTS (SELECT 1 FROM {table} t2"
                f"  WHERE t2.source_id = ?2 AND t2.target_id = {table}.target_id)",
                (merge_id, keep_id),
            )

            # 2b. Delete (X, merge_id) edges that conflict with existing (X, keep_id)
            self._conn.execute(
                f"DELETE FROM {table} WHERE target_id = ?1 AND "  # noqa: S608
                f"EXISTS (SELECT 1 FROM {table} t2"
                f"  WHERE t2.source_id = {table}.source_id AND t2.target_id = ?2)",
                (merge_id, keep_id),
            )

            # 3. Re-key remaining edges
            cur = self._conn.execute(
                f"UPDATE {table} SET source_id = ? WHERE source_id = ?",  # noqa: S608
                (keep_id, merge_id),
            )
            total += cur.rowcount
            cur = self._conn.execute(
                f"UPDATE {table} SET target_id = ? WHERE target_id = ?",  # noqa: S608
                (keep_id, merge_id),
            )
            total += cur.rowcount

            # 4. Safety: delete any self-referential edges
            self._conn.execute(f"DELETE FROM {table} WHERE source_id = target_id")  # noqa: S608
        return total

    def _rekey_single_artist_tables(self, keep_id: int, merge_id: int) -> int:
        """Re-key single-artist tables, replacing merge_id with keep_id.

        Handles three table categories:
        - PK tables (``artist_id`` is the sole PK): delete merge_id row if
          keep_id already exists, otherwise UPDATE.
        - Composite PK tables (PK includes ``artist_id`` + another column):
          delete duplicates, then UPDATE remaining.
        - FK-only tables (no PK constraint): simple UPDATE.

        Returns the total number of rows re-keyed.
        """
        total = 0

        # Tables where artist_id is the sole PK
        for table in _SINGLE_ARTIST_PK_TABLES:
            if not self._has_table(table):
                continue
            # Delete merge_id's row if keep_id already has one (PK conflict)
            self._conn.execute(
                f"DELETE FROM {table} WHERE artist_id = ?1 AND "  # noqa: S608
                f"EXISTS (SELECT 1 FROM {table} t2 WHERE t2.artist_id = ?2)",
                (merge_id, keep_id),
            )
            cur = self._conn.execute(
                f"UPDATE {table} SET artist_id = ? WHERE artist_id = ?",  # noqa: S608
                (keep_id, merge_id),
            )
            total += cur.rowcount

        # Tables with composite PK (artist_id, other_column)
        for table, other_col in _SINGLE_ARTIST_COMPOSITE_PK_TABLES:
            if not self._has_table(table):
                continue
            # Delete merge_id rows that would conflict with existing keep_id rows
            self._conn.execute(
                f"DELETE FROM {table} WHERE artist_id = ?1 AND "  # noqa: S608
                f"EXISTS (SELECT 1 FROM {table} t2"
                f"  WHERE t2.artist_id = ?2 AND t2.{other_col} = {table}.{other_col})",
                (merge_id, keep_id),
            )
            cur = self._conn.execute(
                f"UPDATE {table} SET artist_id = ? WHERE artist_id = ?",  # noqa: S608
                (keep_id, merge_id),
            )
            total += cur.rowcount

        # Tables with no PK constraint (just FK)
        for table in _SINGLE_ARTIST_FK_TABLES:
            if not self._has_table(table):
                continue
            cur = self._conn.execute(
                f"UPDATE {table} SET artist_id = ? WHERE artist_id = ?",  # noqa: S608
                (keep_id, merge_id),
            )
            total += cur.rowcount

        return total

    def _consolidate_entity_edges(self, entity_id: int) -> int:
        """Re-key all artist-referencing tables so aliases use one survivor.

        Picks the artist with the lowest id as the survivor. For each remaining
        alias artist, re-keys its edge and enrichment references to the survivor.

        Returns the total number of rows re-keyed.
        """
        rows = self._conn.execute(
            "SELECT id FROM artist WHERE entity_id = ? ORDER BY id", (entity_id,)
        ).fetchall()
        if len(rows) <= 1:
            return 0
        keep_artist_id = rows[0][0]
        total = 0
        for row in rows[1:]:
            merge_artist_id = row[0]
            total += self._rekey_symmetric_edges(keep_artist_id, merge_artist_id)
            total += self._rekey_directed_edges(keep_artist_id, merge_artist_id)
            total += self._rekey_single_artist_tables(keep_artist_id, merge_artist_id)
        return total

    def merge_entities(self, keep_id: int, merge_id: int) -> int:
        """Merge two entities: re-parent artists, re-key all references, delete merged entity.

        Returns the number of rows re-keyed across all artist-referencing tables.
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
        edges_rekeyed = self._consolidate_entity_edges(keep_id)
        self._conn.execute("DELETE FROM entity WHERE id = ?", (merge_id,))
        self._conn.commit()
        return edges_rekeyed

    def deduplicate_by_qid(self) -> DeduplicationReport:
        """Find entities sharing a Wikidata QID and merge duplicates."""
        groups = self.find_duplicate_qid_groups()
        entities_merged = 0
        artists_reassigned = 0
        edges_rekeyed = 0

        for qid, entity_ids in groups:
            keep_id = entity_ids[0]
            for merge_id in entity_ids[1:]:
                count = self._conn.execute(
                    "SELECT COUNT(*) FROM artist WHERE entity_id = ?", (merge_id,)
                ).fetchone()[0]
                artists_reassigned += count
                edges_rekeyed += self.merge_entities(keep_id, merge_id)
                entities_merged += 1
            logger.info(
                "Deduplicated QID %s: kept entity %d, merged %d entities",
                qid,
                keep_id,
                len(entity_ids) - 1,
            )

        if groups:
            logger.info(
                "Entity deduplication: %d groups, %d entities merged, "
                "%d artists reassigned, %d edges re-keyed",
                len(groups),
                entities_merged,
                artists_reassigned,
                edges_rekeyed,
            )
        else:
            logger.info("Entity deduplication: no duplicate QIDs found")

        return DeduplicationReport(
            groups_found=len(groups),
            entities_merged=entities_merged,
            artists_reassigned=artists_reassigned,
            edges_rekeyed=edges_rekeyed,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()

    def __enter__(self) -> PipelineDB:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()
