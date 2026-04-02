"""Tests for EntityStore label and label_hierarchy CRUD operations."""

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


class TestGetOrCreateLabel:
    def test_creates_new_label(self, store: EntityStore):
        label_id = store.get_or_create_label("Warp Records")
        assert isinstance(label_id, int)
        row = store._conn.execute("SELECT name FROM label WHERE id = ?", (label_id,)).fetchone()
        assert row[0] == "Warp Records"

    def test_returns_existing_label(self, store: EntityStore):
        first = store.get_or_create_label("Warp Records")
        second = store.get_or_create_label("Warp Records")
        assert first == second

    def test_with_discogs_label_id(self, store: EntityStore):
        label_id = store.get_or_create_label("Warp Records", discogs_label_id=23528)
        row = store._conn.execute(
            "SELECT discogs_label_id FROM label WHERE id = ?", (label_id,)
        ).fetchone()
        assert row[0] == 23528

    def test_with_wikidata_qid_creates_entity(self, store: EntityStore):
        label_id = store.get_or_create_label(
            "Warp Records", discogs_label_id=23528, wikidata_qid="Q1312934"
        )
        row = store._conn.execute(
            "SELECT entity_id FROM label WHERE id = ?", (label_id,)
        ).fetchone()
        assert row[0] is not None
        # Entity should exist with the QID
        entity = store.get_entity_by_qid("Q1312934")
        assert entity is not None
        assert entity.name == "Warp Records"
        assert entity.entity_type == "label"

    def test_does_not_overwrite_existing_discogs_id(self, store: EntityStore):
        store.get_or_create_label("Warp Records", discogs_label_id=23528)
        store.get_or_create_label("Warp Records", discogs_label_id=99999)
        row = store._conn.execute(
            "SELECT discogs_label_id FROM label WHERE name = 'Warp Records'"
        ).fetchone()
        assert row[0] == 23528


class TestUpdateLabelQid:
    def test_updates_qid(self, store: EntityStore):
        label_id = store.get_or_create_label("Sub Pop")
        store.update_label_qid(label_id, "Q843988")
        row = store._conn.execute(
            "SELECT entity_id FROM label WHERE id = ?", (label_id,)
        ).fetchone()
        assert row[0] is not None
        entity = store.get_entity_by_qid("Q843988")
        assert entity is not None
        assert entity.entity_type == "label"

    def test_nonexistent_label_raises(self, store: EntityStore):
        with pytest.raises(ValueError, match="No label with id"):
            store.update_label_qid(9999, "Q12345")

    def test_idempotent_for_same_qid(self, store: EntityStore):
        label_id = store.get_or_create_label("Drag City")
        store.update_label_qid(label_id, "Q1254087")
        store.update_label_qid(label_id, "Q1254087")  # Should not raise
        entity = store.get_entity_by_qid("Q1254087")
        assert entity is not None


class TestInsertLabelHierarchy:
    def test_inserts_relationship(self, store: EntityStore):
        parent_id = store.get_or_create_label("Universal Music Group")
        child_id = store.get_or_create_label("Warp Records")
        store.insert_label_hierarchy(parent_id, child_id)
        row = store._conn.execute(
            "SELECT parent_label_id, child_label_id, source FROM label_hierarchy"
        ).fetchone()
        assert row[0] == parent_id
        assert row[1] == child_id
        assert row[2] == "wikidata"

    def test_custom_source(self, store: EntityStore):
        parent_id = store.get_or_create_label("Universal Music Group")
        child_id = store.get_or_create_label("Sub Pop")
        store.insert_label_hierarchy(parent_id, child_id, source="discogs")
        row = store._conn.execute(
            "SELECT source FROM label_hierarchy WHERE parent_label_id = ? AND child_label_id = ?",
            (parent_id, child_id),
        ).fetchone()
        assert row[0] == "discogs"

    def test_idempotent(self, store: EntityStore):
        parent_id = store.get_or_create_label("Universal Music Group")
        child_id = store.get_or_create_label("Warp Records")
        store.insert_label_hierarchy(parent_id, child_id)
        store.insert_label_hierarchy(parent_id, child_id)  # Should not raise
        count = store._conn.execute("SELECT COUNT(*) FROM label_hierarchy").fetchone()[0]
        assert count == 1

    def test_multiple_children(self, store: EntityStore):
        parent_id = store.get_or_create_label("Universal Music Group")
        child1 = store.get_or_create_label("Warp Records")
        child2 = store.get_or_create_label("Sub Pop")
        store.insert_label_hierarchy(parent_id, child1)
        store.insert_label_hierarchy(parent_id, child2)
        count = store._conn.execute(
            "SELECT COUNT(*) FROM label_hierarchy WHERE parent_label_id = ?",
            (parent_id,),
        ).fetchone()[0]
        assert count == 2


class TestGetLabelsWithDiscogsId:
    def test_returns_labels_with_discogs_ids(self, store: EntityStore):
        store.get_or_create_label("Warp Records", discogs_label_id=23528)
        store.get_or_create_label("Sub Pop", discogs_label_id=1594)
        store.get_or_create_label("Self-Released")  # No Discogs ID
        labels = store.get_labels_with_discogs_id()
        assert len(labels) == 2
        ids = {lid for lid, _, _ in labels}
        names = {name for _, name, _ in labels}
        assert names == {"Warp Records", "Sub Pop"}
        assert all(isinstance(lid, int) for lid in ids)

    def test_empty_when_no_labels(self, store: EntityStore):
        labels = store.get_labels_with_discogs_id()
        assert labels == []


class TestGetLabelByName:
    def test_found(self, store: EntityStore):
        store.get_or_create_label("Matador Records", discogs_label_id=2064)
        label = store.get_label_by_name("Matador Records")
        assert label is not None
        assert label["name"] == "Matador Records"
        assert label["discogs_label_id"] == 2064

    def test_not_found(self, store: EntityStore):
        label = store.get_label_by_name("Nonexistent Label")
        assert label is None
