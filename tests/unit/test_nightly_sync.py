"""Tests for nightly sync orchestrator helpers.

Tests the utility functions (_prepare_working_db, _clear_recomputed_tables,
_atomic_swap) and verifies enrichment data survives a pipeline re-run on
a copied database.
"""

import sqlite3
from pathlib import Path

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _create_test_db(path: Path, *, with_enrichment: bool = False) -> None:
    """Create a minimal SQLite database with pipeline tables.

    When *with_enrichment* is True, also creates enrichment tables and
    inserts sample data that should survive a nightly sync.
    """
    conn = sqlite3.connect(str(path))
    conn.executescript(
        """
        CREATE TABLE artist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            canonical_name TEXT NOT NULL UNIQUE,
            genre TEXT,
            total_plays INTEGER NOT NULL DEFAULT 0,
            dj_count INTEGER NOT NULL DEFAULT 0,
            request_ratio REAL NOT NULL DEFAULT 0.0,
            show_count INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE dj_transition (
            source_id INTEGER NOT NULL,
            target_id INTEGER NOT NULL,
            raw_count INTEGER NOT NULL,
            pmi REAL NOT NULL,
            PRIMARY KEY (source_id, target_id)
        );
        CREATE TABLE cross_reference (
            artist_a_id INTEGER NOT NULL,
            artist_b_id INTEGER NOT NULL,
            comment TEXT,
            source TEXT NOT NULL,
            PRIMARY KEY (artist_a_id, artist_b_id, source)
        );
        """
    )

    # Seed some edge data that should be cleared
    conn.execute("INSERT INTO artist (canonical_name, total_plays) VALUES ('Autechre', 500)")
    conn.execute("INSERT INTO artist (canonical_name, total_plays) VALUES ('Stereolab', 400)")
    conn.execute("INSERT INTO dj_transition VALUES (1, 2, 10, 3.5)")
    conn.execute("INSERT INTO cross_reference VALUES (1, 2, 'see also', 'library_code')")

    if with_enrichment:
        conn.executescript(
            """
            CREATE TABLE entity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wikidata_qid TEXT,
                name TEXT NOT NULL,
                entity_type TEXT NOT NULL DEFAULT 'artist',
                created_at TEXT NOT NULL DEFAULT '2025-01-01',
                updated_at TEXT NOT NULL DEFAULT '2025-01-01'
            );
            CREATE TABLE wikidata_influence (
                source_id INTEGER NOT NULL,
                target_id INTEGER NOT NULL,
                source_qid TEXT NOT NULL,
                target_qid TEXT NOT NULL,
                PRIMARY KEY (source_id, target_id)
            );
            CREATE TABLE audio_profile (
                artist_id INTEGER PRIMARY KEY,
                avg_danceability REAL,
                recording_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT '2025-01-01'
            );
            """
        )
        conn.execute("INSERT INTO entity (wikidata_qid, name) VALUES ('Q210513', 'Autechre')")
        conn.execute("INSERT INTO wikidata_influence VALUES (1, 2, 'Q210513', 'Q484464')")
        conn.execute("INSERT INTO audio_profile VALUES (1, 0.35, 42, '2025-01-01')")

    conn.commit()
    conn.close()


# ===========================================================================
# _validate_sqlite
# ===========================================================================


class TestValidateSqlite:
    def test_valid_sqlite_file(self, tmp_path):
        from semantic_index.nightly_sync import _validate_sqlite

        db = tmp_path / "test.db"
        conn = sqlite3.connect(str(db))
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.close()

        assert _validate_sqlite(db) is True

    def test_non_sqlite_file(self, tmp_path):
        from semantic_index.nightly_sync import _validate_sqlite

        f = tmp_path / "not_a_db.db"
        f.write_text("this is not sqlite")

        assert _validate_sqlite(f) is False

    def test_nonexistent_file(self, tmp_path):
        from semantic_index.nightly_sync import _validate_sqlite

        assert _validate_sqlite(tmp_path / "missing.db") is False


# ===========================================================================
# _prepare_working_db
# ===========================================================================


class TestPrepareWorkingDb:
    def test_copies_existing_db(self, tmp_path):
        from semantic_index.nightly_sync import _prepare_working_db

        prod = tmp_path / "prod.db"
        _create_test_db(prod)

        temp = _prepare_working_db(prod)

        assert temp.exists()
        assert temp != prod
        # Verify content was copied
        conn = sqlite3.connect(str(temp))
        count = conn.execute("SELECT COUNT(*) FROM artist").fetchone()[0]
        conn.close()
        assert count == 2

    def test_first_run_creates_empty_temp(self, tmp_path):
        from semantic_index.nightly_sync import _prepare_working_db

        prod = tmp_path / "nonexistent.db"
        temp = _prepare_working_db(prod)

        assert temp.exists()
        assert temp.stat().st_size == 0

    def test_temp_in_same_directory_as_production(self, tmp_path):
        from semantic_index.nightly_sync import _prepare_working_db

        prod = tmp_path / "data" / "graph.db"
        prod.parent.mkdir()
        _create_test_db(prod)

        temp = _prepare_working_db(prod)

        assert temp.parent == prod.parent


# ===========================================================================
# _clear_recomputed_tables
# ===========================================================================


class TestClearRecomputedTables:
    def test_clears_edge_tables(self, tmp_path):
        from semantic_index.nightly_sync import _clear_recomputed_tables

        db = tmp_path / "test.db"
        _create_test_db(db, with_enrichment=True)

        _clear_recomputed_tables(str(db))

        conn = sqlite3.connect(str(db))
        assert conn.execute("SELECT COUNT(*) FROM dj_transition").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM cross_reference").fetchone()[0] == 0
        conn.close()

    def test_preserves_enrichment_tables(self, tmp_path):
        from semantic_index.nightly_sync import _clear_recomputed_tables

        db = tmp_path / "test.db"
        _create_test_db(db, with_enrichment=True)

        _clear_recomputed_tables(str(db))

        conn = sqlite3.connect(str(db))
        assert conn.execute("SELECT COUNT(*) FROM entity").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM wikidata_influence").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM audio_profile").fetchone()[0] == 1
        conn.close()

    def test_handles_missing_tables_gracefully(self, tmp_path):
        """Works on a fresh DB that doesn't have edge tables yet."""
        from semantic_index.nightly_sync import _clear_recomputed_tables

        db = tmp_path / "empty.db"
        conn = sqlite3.connect(str(db))
        conn.execute("CREATE TABLE artist (id INTEGER PRIMARY KEY)")
        conn.close()

        # Should not raise
        _clear_recomputed_tables(str(db))


# ===========================================================================
# _atomic_swap
# ===========================================================================


class TestAtomicSwap:
    def test_replaces_production_with_temp(self, tmp_path):
        from semantic_index.nightly_sync import _atomic_swap

        prod = tmp_path / "prod.db"
        prod.write_bytes(b"old content")

        temp = tmp_path / "temp.db"
        temp.write_bytes(b"new content")

        _atomic_swap(temp, prod, dry_run=False)

        assert prod.read_bytes() == b"new content"
        assert not temp.exists()

    def test_dry_run_removes_temp_keeps_production(self, tmp_path):
        from semantic_index.nightly_sync import _atomic_swap

        prod = tmp_path / "prod.db"
        prod.write_bytes(b"old content")

        temp = tmp_path / "temp.db"
        temp.write_bytes(b"new content")

        _atomic_swap(temp, prod, dry_run=True)

        assert prod.read_bytes() == b"old content"
        assert not temp.exists()

    def test_first_run_no_existing_production(self, tmp_path):
        from semantic_index.nightly_sync import _atomic_swap

        prod = tmp_path / "prod.db"
        temp = tmp_path / "temp.db"
        temp.write_bytes(b"new content")

        _atomic_swap(temp, prod, dry_run=False)

        assert prod.read_bytes() == b"new content"

    def test_creates_parent_directory(self, tmp_path):
        from semantic_index.nightly_sync import _atomic_swap

        prod = tmp_path / "data" / "graph.db"
        temp = tmp_path / "data" / "temp.db"
        temp.parent.mkdir()
        temp.write_bytes(b"new content")

        _atomic_swap(temp, prod, dry_run=False)

        assert prod.exists()
