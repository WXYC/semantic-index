"""Tests for EntityStore schema and lifecycle."""

import sqlite3

import pytest

from semantic_index.entity_store import EntityStore

# Tables that EntityStore.initialize() must create
_EXPECTED_TABLES = {
    "entity",
    "release",
    "label",
    "label_hierarchy",
    "reconciliation_log",
}

# Columns added to artist table by _migrate_artist_table()
_NEW_ARTIST_COLUMNS = {
    "entity_id",
    "musicbrainz_artist_id",
    "wxyc_library_code_id",
    "reconciliation_status",
    "created_at",
    "updated_at",
}

# The old artist schema — matches the current sqlite_export._SCHEMA artist table
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


def _get_table_names(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {row[0] for row in rows}


def _get_column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {row[1] for row in rows}


def _get_index_names(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()
    return {row[0] for row in rows}


class TestFreshDatabase:
    """initialize() on a fresh database creates all expected tables and indexes."""

    def test_creates_entity_table(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        store = EntityStore(db_path)
        store.initialize()
        conn = sqlite3.connect(db_path)
        tables = _get_table_names(conn)
        assert "entity" in tables
        conn.close()
        store.close()

    def test_creates_all_new_tables(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        store = EntityStore(db_path)
        store.initialize()
        conn = sqlite3.connect(db_path)
        tables = _get_table_names(conn)
        for table in _EXPECTED_TABLES:
            assert table in tables, f"Missing table: {table}"
        conn.close()
        store.close()

    def test_entity_table_columns(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        store = EntityStore(db_path)
        store.initialize()
        conn = sqlite3.connect(db_path)
        columns = _get_column_names(conn, "entity")
        assert columns == {"id", "wikidata_qid", "name", "entity_type", "created_at", "updated_at"}
        conn.close()
        store.close()

    def test_creates_indexes(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        store = EntityStore(db_path)
        store.initialize()
        conn = sqlite3.connect(db_path)
        indexes = _get_index_names(conn)
        assert "idx_entity_qid" in indexes
        assert "idx_entity_type" in indexes
        assert "idx_reconciliation_artist" in indexes
        conn.close()
        store.close()

    def test_release_table_columns(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        store = EntityStore(db_path)
        store.initialize()
        conn = sqlite3.connect(db_path)
        columns = _get_column_names(conn, "release")
        expected = {
            "id",
            "title",
            "artist_id",
            "wxyc_library_release_id",
            "discogs_master_id",
            "discogs_release_id",
            "musicbrainz_release_group_id",
            "year",
            "created_at",
            "updated_at",
        }
        assert columns == expected
        conn.close()
        store.close()

    def test_label_table_columns(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        store = EntityStore(db_path)
        store.initialize()
        conn = sqlite3.connect(db_path)
        columns = _get_column_names(conn, "label")
        expected = {
            "id",
            "name",
            "entity_id",
            "discogs_label_id",
            "musicbrainz_label_id",
            "created_at",
            "updated_at",
        }
        assert columns == expected
        conn.close()
        store.close()

    def test_reconciliation_log_columns(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        store = EntityStore(db_path)
        store.initialize()
        conn = sqlite3.connect(db_path)
        columns = _get_column_names(conn, "reconciliation_log")
        expected = {
            "id",
            "artist_id",
            "source",
            "external_id",
            "confidence",
            "method",
            "created_at",
        }
        assert columns == expected
        conn.close()
        store.close()


class TestIdempotent:
    """Calling initialize() twice doesn't error."""

    def test_double_initialize(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        store = EntityStore(db_path)
        store.initialize()
        store.initialize()  # Should not raise
        conn = sqlite3.connect(db_path)
        tables = _get_table_names(conn)
        for table in _EXPECTED_TABLES:
            assert table in tables
        conn.close()
        store.close()

    def test_idempotent_preserves_data(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        store = EntityStore(db_path)
        store.initialize()

        # Insert a row into entity table
        store._conn.execute("INSERT INTO entity (name, entity_type) VALUES ('Autechre', 'artist')")
        store._conn.commit()

        # Re-initialize
        store.initialize()

        row = store._conn.execute("SELECT name FROM entity WHERE name = 'Autechre'").fetchone()
        assert row is not None
        assert row[0] == "Autechre"
        store.close()


class TestMigration:
    """_migrate_artist_table() adds new columns to an old-schema artist table."""

    def test_migration_adds_new_columns(self, tmp_path):
        db_path = str(tmp_path / "test.db")

        # Create old-schema artist table directly
        conn = sqlite3.connect(db_path)
        conn.executescript(_OLD_ARTIST_SCHEMA)
        conn.execute(
            """INSERT INTO artist (canonical_name, genre, total_plays, dj_count, request_ratio, show_count)
               VALUES ('Autechre', 'Electronic', 50, 15, 0.1, 40)"""
        )
        conn.commit()
        conn.close()

        # Now open EntityStore and initialize — should migrate
        store = EntityStore(db_path)
        store.initialize()

        columns = _get_column_names(store._conn, "artist")
        for col in _NEW_ARTIST_COLUMNS:
            assert col in columns, f"Migration did not add column: {col}"
        store.close()

    def test_migration_preserves_existing_data(self, tmp_path):
        db_path = str(tmp_path / "test.db")

        # Create old-schema with data
        conn = sqlite3.connect(db_path)
        conn.executescript(_OLD_ARTIST_SCHEMA)
        conn.execute(
            """INSERT INTO artist (canonical_name, genre, total_plays, active_first_year,
               active_last_year, dj_count, request_ratio, show_count, discogs_artist_id)
               VALUES ('Stereolab', 'Rock', 30, 1998, 2024, 10, 0.05, 25, 12345)"""
        )
        conn.commit()
        conn.close()

        store = EntityStore(db_path)
        store.initialize()

        row = store._conn.execute(
            "SELECT canonical_name, genre, total_plays, discogs_artist_id FROM artist WHERE canonical_name = 'Stereolab'"
        ).fetchone()
        assert row[0] == "Stereolab"
        assert row[1] == "Rock"
        assert row[2] == 30
        assert row[3] == 12345
        store.close()

    def test_migration_sets_defaults_for_new_columns(self, tmp_path):
        db_path = str(tmp_path / "test.db")

        conn = sqlite3.connect(db_path)
        conn.executescript(_OLD_ARTIST_SCHEMA)
        conn.execute(
            """INSERT INTO artist (canonical_name, total_plays, dj_count, request_ratio, show_count)
               VALUES ('Cat Power', 20, 8, 0.02, 15)"""
        )
        conn.commit()
        conn.close()

        store = EntityStore(db_path)
        store.initialize()

        row = store._conn.execute(
            "SELECT reconciliation_status, entity_id FROM artist WHERE canonical_name = 'Cat Power'"
        ).fetchone()
        assert row[0] == "unreconciled"
        assert row[1] is None  # entity_id defaults to NULL
        store.close()

    def test_migration_adds_artist_indexes(self, tmp_path):
        db_path = str(tmp_path / "test.db")

        conn = sqlite3.connect(db_path)
        conn.executescript(_OLD_ARTIST_SCHEMA)
        conn.close()

        store = EntityStore(db_path)
        store.initialize()

        indexes = _get_index_names(store._conn)
        assert "idx_artist_entity" in indexes
        assert "idx_artist_discogs" in indexes
        assert "idx_artist_musicbrainz" in indexes
        assert "idx_artist_library_code" in indexes
        assert "idx_artist_reconciliation" in indexes
        store.close()


class TestAlreadyMigrated:
    """initialize() on an already-migrated database is a no-op."""

    def test_already_migrated_is_noop(self, tmp_path):
        db_path = str(tmp_path / "test.db")

        # First: create old schema, migrate, insert data
        conn = sqlite3.connect(db_path)
        conn.executescript(_OLD_ARTIST_SCHEMA)
        conn.close()

        store = EntityStore(db_path)
        store.initialize()
        store._conn.execute(
            """INSERT INTO artist (canonical_name, total_plays, dj_count, request_ratio, show_count,
               reconciliation_status, musicbrainz_artist_id)
               VALUES ('Father John Misty', 25, 12, 0.08, 20, 'partial', 'mb-uuid-123')"""
        )
        store._conn.commit()

        # Grab column set before second initialize
        columns_before = _get_column_names(store._conn, "artist")

        # Re-initialize — should be a no-op
        store.initialize()

        columns_after = _get_column_names(store._conn, "artist")
        assert columns_before == columns_after

        # Data should be intact
        row = store._conn.execute(
            "SELECT reconciliation_status, musicbrainz_artist_id FROM artist WHERE canonical_name = 'Father John Misty'"
        ).fetchone()
        assert row[0] == "partial"
        assert row[1] == "mb-uuid-123"
        store.close()


class TestContextManager:
    """EntityStore supports the context manager protocol."""

    def test_context_manager_initializes(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        with EntityStore(db_path) as store:
            store.initialize()
            tables = _get_table_names(store._conn)
            assert "entity" in tables

    def test_context_manager_closes_connection(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        with EntityStore(db_path) as store:
            store.initialize()
            conn = store._conn

        # After exiting context, operations on the connection should fail
        with pytest.raises(sqlite3.ProgrammingError):
            conn.execute("SELECT 1")

    def test_context_manager_on_exception(self, tmp_path):
        """Connection is closed even if an exception occurs inside the with block."""
        db_path = str(tmp_path / "test.db")
        try:
            with EntityStore(db_path) as store:
                store.initialize()
                conn = store._conn
                raise ValueError("test error")
        except ValueError:
            pass

        with pytest.raises(sqlite3.ProgrammingError):
            conn.execute("SELECT 1")
