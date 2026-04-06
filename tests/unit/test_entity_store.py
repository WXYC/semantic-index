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
        assert columns == {
            "id", "wikidata_qid", "name", "entity_type",
            "spotify_artist_id", "apple_music_artist_id", "bandcamp_id",
            "created_at", "updated_at",
        }
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


class TestGetArtistsNeedingWikidata:
    """Tests for get_artists_needing_wikidata() query method."""

    def test_returns_artists_with_discogs_id_and_no_entity(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        store = EntityStore(db_path)
        store.initialize()
        store.upsert_artist("Autechre", discogs_artist_id=2774)
        store.upsert_artist("Stereolab", discogs_artist_id=10272)

        result = store.get_artists_needing_wikidata()
        names = {name for _, name, _ in result}
        assert names == {"Autechre", "Stereolab"}
        store.close()

    def test_returns_artists_with_entity_missing_qid(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        store = EntityStore(db_path)
        store.initialize()
        entity = store.get_or_create_entity("Autechre", "artist")
        store.upsert_artist("Autechre", discogs_artist_id=2774, entity_id=entity.id)

        result = store.get_artists_needing_wikidata()
        assert len(result) == 1
        assert result[0][1] == "Autechre"
        assert result[0][2] == 2774
        store.close()

    def test_excludes_artists_with_entity_that_has_qid(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        store = EntityStore(db_path)
        store.initialize()
        entity = store.get_or_create_entity("Autechre", "artist", wikidata_qid="Q2774")
        store.upsert_artist("Autechre", discogs_artist_id=2774, entity_id=entity.id)

        result = store.get_artists_needing_wikidata()
        assert result == []
        store.close()

    def test_excludes_artists_without_discogs_id(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        store = EntityStore(db_path)
        store.initialize()
        store.upsert_artist("Unknown Band")

        result = store.get_artists_needing_wikidata()
        assert result == []
        store.close()

    def test_returns_discogs_artist_id_in_tuple(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        store = EntityStore(db_path)
        store.initialize()
        aid = store.upsert_artist("Cat Power", discogs_artist_id=88)

        result = store.get_artists_needing_wikidata()
        assert len(result) == 1
        assert result[0] == (aid, "Cat Power", 88)
        store.close()

    def test_mixed_artists(self, tmp_path):
        """Only artists needing Wikidata are returned."""
        db_path = str(tmp_path / "test.db")
        store = EntityStore(db_path)
        store.initialize()
        # Has discogs ID, no entity -> needs wikidata
        store.upsert_artist("Autechre", discogs_artist_id=2774)
        # Has discogs ID, entity with QID -> already done
        entity = store.get_or_create_entity("Stereolab", "artist", wikidata_qid="Q650826")
        store.upsert_artist("Stereolab", discogs_artist_id=10272, entity_id=entity.id)
        # No discogs ID -> not eligible
        store.upsert_artist("Unknown Band")

        result = store.get_artists_needing_wikidata()
        names = {name for _, name, _ in result}
        assert names == {"Autechre"}
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


class TestEntityStreamingIds:
    """Tests for streaming ID columns on the entity table."""

    def test_fresh_db_has_streaming_columns(self, tmp_path):
        """A fresh database includes streaming ID columns on entity."""
        db_path = str(tmp_path / "test.db")
        store = EntityStore(db_path)
        store.initialize()
        columns = _get_column_names(store._conn, "entity")
        assert "spotify_artist_id" in columns
        assert "apple_music_artist_id" in columns
        assert "bandcamp_id" in columns
        store.close()

    def test_migration_adds_streaming_columns(self, tmp_path):
        """Migrating an old entity table (without streaming cols) adds them."""
        db_path = str(tmp_path / "test.db")
        conn = sqlite3.connect(db_path)
        # Create old-schema entity table without streaming columns
        conn.execute(
            """CREATE TABLE entity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wikidata_qid TEXT,
                name TEXT NOT NULL,
                entity_type TEXT NOT NULL DEFAULT 'artist',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            )"""
        )
        conn.execute("INSERT INTO entity (name, entity_type, wikidata_qid) VALUES ('Autechre', 'artist', 'Q2774')")
        conn.commit()
        conn.close()

        store = EntityStore(db_path)
        store.initialize()
        columns = _get_column_names(store._conn, "entity")
        assert "spotify_artist_id" in columns
        assert "apple_music_artist_id" in columns
        assert "bandcamp_id" in columns

        # Existing data preserved
        row = store._conn.execute("SELECT name, wikidata_qid FROM entity WHERE name = 'Autechre'").fetchone()
        assert row[0] == "Autechre"
        assert row[1] == "Q2774"
        store.close()

    def test_streaming_indexes_created(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        store = EntityStore(db_path)
        store.initialize()
        indexes = _get_index_names(store._conn)
        assert "idx_entity_spotify" in indexes
        assert "idx_entity_apple_music" in indexes
        assert "idx_entity_bandcamp" in indexes
        store.close()

    def test_update_entity_streaming_ids(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        store = EntityStore(db_path)
        store.initialize()
        entity = store.get_or_create_entity("Autechre", "artist", wikidata_qid="Q2774")

        store.update_entity_streaming_ids(
            entity.id,
            spotify="5bMqBjPbCOWGgWJpbAqdQq",
            apple_music="15821",
            bandcamp="autechre",
        )

        row = store._conn.execute(
            "SELECT spotify_artist_id, apple_music_artist_id, bandcamp_id FROM entity WHERE id = ?",
            (entity.id,),
        ).fetchone()
        assert row[0] == "5bMqBjPbCOWGgWJpbAqdQq"
        assert row[1] == "15821"
        assert row[2] == "autechre"
        store.close()

    def test_update_entity_streaming_ids_coalesce(self, tmp_path):
        """COALESCE semantics: don't overwrite non-null with null."""
        db_path = str(tmp_path / "test.db")
        store = EntityStore(db_path)
        store.initialize()
        entity = store.get_or_create_entity("Autechre", "artist", wikidata_qid="Q2774")

        # First update: set spotify
        store.update_entity_streaming_ids(entity.id, spotify="abc123", apple_music=None, bandcamp=None)
        # Second update: set bandcamp but pass None for spotify
        store.update_entity_streaming_ids(entity.id, spotify=None, apple_music=None, bandcamp="autechre")

        row = store._conn.execute(
            "SELECT spotify_artist_id, bandcamp_id FROM entity WHERE id = ?",
            (entity.id,),
        ).fetchone()
        assert row[0] == "abc123"  # preserved from first update
        assert row[1] == "autechre"
        store.close()

    def test_get_entities_needing_streaming_ids(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        store = EntityStore(db_path)
        store.initialize()

        # Entity with QID but no streaming IDs -> needs them
        store.get_or_create_entity("Autechre", "artist", wikidata_qid="Q2774")
        # Entity with QID and spotify -> already has some
        e2 = store.get_or_create_entity("Stereolab", "artist", wikidata_qid="Q650826")
        store.update_entity_streaming_ids(e2.id, spotify="xyz", apple_music=None, bandcamp=None)
        # Entity without QID -> not eligible
        store.get_or_create_entity("Unknown Band", "artist")

        result = store.get_entities_needing_streaming_ids()
        assert len(result) == 1
        assert result[0][1] == "Q2774"  # (entity_id, qid)
        store.close()
