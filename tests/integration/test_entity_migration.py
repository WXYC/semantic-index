"""Integration tests for entity store migration failure recovery.

Verifies that when column creation fails during entity store migration
(e.g., due to disk full, permission error, or schema conflict), the
partial migration state is detected on the next run.
"""

from __future__ import annotations

import sqlite3
from unittest.mock import patch

import pytest
from semantic_index.entity_store import EntityStore

pytestmark = pytest.mark.integration


# The old artist schema without entity store columns
_OLD_ARTIST_SCHEMA = """
CREATE TABLE artist (
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
"""


def _get_column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {row[1] for row in rows}


def _get_table_names(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {row[0] for row in rows}


class TestPartialMigrationDetection:
    """Verify that a partially migrated database is correctly completed on next run."""

    def test_partial_artist_migration_completed_on_retry(self, tmp_path) -> None:
        """If only some artist columns were added, next initialize() adds the rest."""
        db_path = str(tmp_path / "test.db")

        # Create a database with the old artist schema
        conn = sqlite3.connect(db_path)
        conn.executescript(_OLD_ARTIST_SCHEMA)
        # Manually add only some of the new columns (simulating partial migration)
        conn.execute("ALTER TABLE artist ADD COLUMN entity_id INTEGER REFERENCES entity(id)")
        conn.execute("ALTER TABLE artist ADD COLUMN musicbrainz_artist_id TEXT")
        conn.commit()
        conn.close()

        # Verify partial state: entity_id and musicbrainz_artist_id exist, but not the rest
        conn = sqlite3.connect(db_path)
        cols = _get_column_names(conn, "artist")
        assert "entity_id" in cols
        assert "musicbrainz_artist_id" in cols
        assert "reconciliation_status" not in cols
        assert "wxyc_library_code_id" not in cols
        conn.close()

        # Run initialize() -- it should add the missing columns
        store = EntityStore(db_path)
        store.initialize()
        store.close()

        # Verify all columns now exist
        conn = sqlite3.connect(db_path)
        cols = _get_column_names(conn, "artist")
        assert "entity_id" in cols
        assert "musicbrainz_artist_id" in cols
        assert "wxyc_library_code_id" in cols
        assert "reconciliation_status" in cols
        assert "created_at" in cols
        assert "updated_at" in cols
        conn.close()

    def test_partial_entity_table_columns_completed(self, tmp_path) -> None:
        """If entity table exists but is missing streaming ID columns, they are added."""
        db_path = str(tmp_path / "test.db")

        # Create entity table without the streaming ID columns
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE entity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wikidata_qid TEXT,
                name TEXT NOT NULL,
                entity_type TEXT NOT NULL DEFAULT 'artist',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            )
        """)
        conn.commit()
        conn.close()

        # Verify streaming columns don't exist yet
        conn = sqlite3.connect(db_path)
        cols = _get_column_names(conn, "entity")
        assert "spotify_artist_id" not in cols
        conn.close()

        # Run initialize() -- should add the missing columns
        store = EntityStore(db_path)
        store.initialize()
        store.close()

        # Verify streaming columns now exist
        conn = sqlite3.connect(db_path)
        cols = _get_column_names(conn, "entity")
        assert "spotify_artist_id" in cols
        assert "apple_music_artist_id" in cols
        assert "bandcamp_id" in cols
        conn.close()

    def test_existing_data_preserved_after_migration(self, tmp_path) -> None:
        """Artist data from before migration is preserved with new columns defaulted."""
        db_path = str(tmp_path / "test.db")

        # Create old schema and insert test data
        conn = sqlite3.connect(db_path)
        conn.executescript(_OLD_ARTIST_SCHEMA)
        conn.execute(
            "INSERT INTO artist (canonical_name, genre, total_plays, discogs_artist_id) "
            "VALUES ('Autechre', 'Electronic', 50, 42)"
        )
        conn.execute(
            "INSERT INTO artist (canonical_name, genre, total_plays) "
            "VALUES ('Stereolab', 'Rock', 120)"
        )
        conn.commit()
        conn.close()

        # Run migration
        store = EntityStore(db_path)
        store.initialize()
        store.close()

        # Verify data is preserved
        conn = sqlite3.connect(db_path)
        row = conn.execute(
            "SELECT canonical_name, genre, total_plays, discogs_artist_id, "
            "reconciliation_status, entity_id FROM artist WHERE canonical_name = 'Autechre'"
        ).fetchone()
        assert row[0] == "Autechre"
        assert row[1] == "Electronic"
        assert row[2] == 50
        assert row[3] == 42
        assert row[4] == "unreconciled"  # default for new column
        assert row[5] is None  # entity_id not yet set
        conn.close()

    def test_idempotent_double_initialize(self, tmp_path) -> None:
        """Calling initialize() twice is safe and produces the same result."""
        db_path = str(tmp_path / "test.db")

        # Create old schema with data
        conn = sqlite3.connect(db_path)
        conn.executescript(_OLD_ARTIST_SCHEMA)
        conn.execute(
            "INSERT INTO artist (canonical_name, genre, total_plays) "
            "VALUES ('Cat Power', 'Rock', 75)"
        )
        conn.commit()
        conn.close()

        # First initialize
        store = EntityStore(db_path)
        store.initialize()
        store.close()

        # Get state after first init
        conn = sqlite3.connect(db_path)
        cols_after_first = _get_column_names(conn, "artist")
        tables_after_first = _get_table_names(conn)
        row_count = conn.execute("SELECT COUNT(*) FROM artist").fetchone()[0]
        conn.close()

        # Second initialize
        store = EntityStore(db_path)
        store.initialize()
        store.close()

        # Verify state is identical
        conn = sqlite3.connect(db_path)
        cols_after_second = _get_column_names(conn, "artist")
        tables_after_second = _get_table_names(conn)
        row_count_2 = conn.execute("SELECT COUNT(*) FROM artist").fetchone()[0]
        conn.close()

        assert cols_after_first == cols_after_second
        assert tables_after_first == tables_after_second
        assert row_count == row_count_2 == 1


class TestColumnCreationFailure:
    """Mock column creation failure during artist table migration."""

    def test_failed_alter_leaves_partial_state(self, tmp_path) -> None:
        """When ALTER TABLE fails mid-migration, some columns exist and others don't.

        On the next initialize() call, only the missing columns are added.
        """
        db_path = str(tmp_path / "test.db")

        # Create old schema
        conn = sqlite3.connect(db_path)
        conn.executescript(_OLD_ARTIST_SCHEMA)
        conn.execute(
            "INSERT INTO artist (canonical_name, total_plays) VALUES ('Jessica Pratt', 30)"
        )
        conn.commit()
        conn.close()

        # First pass: mock ensure_columns to only add the first 2 columns
        def partial_ensure_columns(conn, table, columns):
            """Add only the first 2 columns, simulating a failure after that."""
            from semantic_index.utils import ensure_columns as real_ensure

            partial = columns[:2]
            real_ensure(conn, table, partial)
            # Raise after partial work
            raise sqlite3.OperationalError("disk I/O error")

        with patch(
            "semantic_index.entity_store.ensure_columns",
            side_effect=partial_ensure_columns,
        ):
            store = EntityStore(db_path)
            with pytest.raises(sqlite3.OperationalError, match="disk I/O error"):
                store.initialize()
            store.close()

        # Verify partial state
        conn = sqlite3.connect(db_path)
        cols = _get_column_names(conn, "artist")
        assert "entity_id" in cols
        assert "musicbrainz_artist_id" in cols
        # Later columns may or may not be there depending on where the error hit
        conn.close()

        # Second pass: full initialize should complete the migration
        store = EntityStore(db_path)
        store.initialize()
        store.close()

        # Verify all columns now present
        conn = sqlite3.connect(db_path)
        cols = _get_column_names(conn, "artist")
        assert "entity_id" in cols
        assert "musicbrainz_artist_id" in cols
        assert "wxyc_library_code_id" in cols
        assert "reconciliation_status" in cols
        assert "created_at" in cols
        assert "updated_at" in cols

        # Verify data preserved
        row = conn.execute(
            "SELECT canonical_name, total_plays FROM artist WHERE canonical_name = 'Jessica Pratt'"
        ).fetchone()
        assert row is not None
        assert row[1] == 30
        conn.close()
