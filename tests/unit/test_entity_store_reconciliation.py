"""Tests for EntityStore reconciliation support: unreconciled queries, status updates, artist styles."""

import sqlite3

import pytest

from semantic_index.entity_store import EntityStore

# The old artist schema — matches sqlite_export._SCHEMA artist table
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


@pytest.fixture()
def store(tmp_path) -> EntityStore:
    """An initialized EntityStore with a pre-migrated artist table."""
    db_path = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_path)
    conn.executescript(_OLD_ARTIST_SCHEMA)
    conn.close()
    s = EntityStore(db_path)
    s.initialize()
    return s


# ---------------------------------------------------------------------------
# get_unreconciled_artists
# ---------------------------------------------------------------------------


class TestGetUnreconciledArtists:
    def test_returns_unreconciled(self, store: EntityStore):
        store.bulk_upsert_artists(["Autechre", "Stereolab"])
        unreconciled = store.get_unreconciled_artists()
        assert len(unreconciled) == 2
        names = {name for _, name in unreconciled}
        assert names == {"Autechre", "Stereolab"}

    def test_skips_reconciled(self, store: EntityStore):
        aid = store.upsert_artist("Autechre")
        store.update_reconciliation_status(aid, "reconciled")
        store.upsert_artist("Stereolab")
        unreconciled = store.get_unreconciled_artists()
        assert len(unreconciled) == 1
        assert unreconciled[0][1] == "Stereolab"

    def test_skips_no_match(self, store: EntityStore):
        aid = store.upsert_artist("Autechre")
        store.update_reconciliation_status(aid, "no_match")
        store.upsert_artist("Cat Power")
        unreconciled = store.get_unreconciled_artists()
        assert len(unreconciled) == 1
        assert unreconciled[0][1] == "Cat Power"

    def test_respects_limit(self, store: EntityStore):
        store.bulk_upsert_artists(["Autechre", "Stereolab", "Cat Power", "Buck Meek"])
        unreconciled = store.get_unreconciled_artists(limit=2)
        assert len(unreconciled) == 2

    def test_no_limit_returns_all(self, store: EntityStore):
        store.bulk_upsert_artists(["Autechre", "Stereolab", "Cat Power"])
        unreconciled = store.get_unreconciled_artists()
        assert len(unreconciled) == 3

    def test_empty_table(self, store: EntityStore):
        assert store.get_unreconciled_artists() == []

    def test_returns_id_and_name_tuples(self, store: EntityStore):
        expected_id = store.upsert_artist("Father John Misty")
        unreconciled = store.get_unreconciled_artists()
        assert unreconciled[0] == (expected_id, "Father John Misty")


# ---------------------------------------------------------------------------
# update_reconciliation_status
# ---------------------------------------------------------------------------


class TestUpdateReconciliationStatus:
    def test_updates_to_reconciled(self, store: EntityStore):
        aid = store.upsert_artist("Autechre")
        store.update_reconciliation_status(aid, "reconciled")
        row = store._conn.execute(
            "SELECT reconciliation_status FROM artist WHERE id = ?", (aid,)
        ).fetchone()
        assert row[0] == "reconciled"

    def test_updates_to_no_match(self, store: EntityStore):
        aid = store.upsert_artist("Stereolab")
        store.update_reconciliation_status(aid, "no_match")
        row = store._conn.execute(
            "SELECT reconciliation_status FROM artist WHERE id = ?", (aid,)
        ).fetchone()
        assert row[0] == "no_match"

    def test_nonexistent_artist_raises(self, store: EntityStore):
        with pytest.raises(ValueError, match="No artist with id"):
            store.update_reconciliation_status(9999, "reconciled")

    def test_updates_updated_at(self, store: EntityStore):
        aid = store.upsert_artist("Cat Power")
        before = store._conn.execute(
            "SELECT updated_at FROM artist WHERE id = ?", (aid,)
        ).fetchone()[0]
        store.update_reconciliation_status(aid, "reconciled")
        after = store._conn.execute(
            "SELECT updated_at FROM artist WHERE id = ?", (aid,)
        ).fetchone()[0]
        assert after is not None
        assert after >= before


# ---------------------------------------------------------------------------
# Artist styles
# ---------------------------------------------------------------------------


class TestArtistStyleTable:
    def test_table_created_on_initialize(self, store: EntityStore):
        row = store._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='artist_style'"
        ).fetchone()
        assert row is not None


class TestPersistArtistStyles:
    def test_persist_styles(self, store: EntityStore):
        aid = store.upsert_artist("Autechre")
        store.persist_artist_styles(aid, ["IDM", "Abstract", "Experimental"])
        styles = store.get_artist_styles(aid)
        assert set(styles) == {"IDM", "Abstract", "Experimental"}

    def test_persist_empty_list(self, store: EntityStore):
        aid = store.upsert_artist("Stereolab")
        store.persist_artist_styles(aid, [])
        assert store.get_artist_styles(aid) == []

    def test_persist_idempotent(self, store: EntityStore):
        """Persisting overlapping styles should not create duplicates."""
        aid = store.upsert_artist("Autechre")
        store.persist_artist_styles(aid, ["IDM", "Abstract"])
        store.persist_artist_styles(aid, ["IDM", "Experimental"])
        styles = store.get_artist_styles(aid)
        assert set(styles) == {"IDM", "Abstract", "Experimental"}

    def test_different_artists_independent(self, store: EntityStore):
        aid_a = store.upsert_artist("Autechre")
        aid_b = store.upsert_artist("Cat Power")
        store.persist_artist_styles(aid_a, ["IDM", "Abstract"])
        store.persist_artist_styles(aid_b, ["Indie Rock", "Lo-Fi"])
        assert set(store.get_artist_styles(aid_a)) == {"IDM", "Abstract"}
        assert set(store.get_artist_styles(aid_b)) == {"Indie Rock", "Lo-Fi"}


class TestGetArtistStyles:
    def test_empty_for_new_artist(self, store: EntityStore):
        aid = store.upsert_artist("Jessica Pratt")
        assert store.get_artist_styles(aid) == []

    def test_returns_sorted(self, store: EntityStore):
        aid = store.upsert_artist("Sessa")
        store.persist_artist_styles(aid, ["MPB", "Bossa Nova", "Art Pop"])
        styles = store.get_artist_styles(aid)
        assert styles == sorted(styles)
