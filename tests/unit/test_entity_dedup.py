"""Tests for entity deduplication by shared Wikidata QID."""

import sqlite3

import pytest

from semantic_index.entity_store import EntityStore
from semantic_index.models import DeduplicationReport

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
# find_duplicate_qid_groups
# ---------------------------------------------------------------------------


class TestFindDuplicateQidGroups:
    def test_no_entities_returns_empty(self, store: EntityStore):
        assert store.find_duplicate_qid_groups() == []

    def test_no_duplicates_returns_empty(self, store: EntityStore):
        store.get_or_create_entity("Autechre", "artist", wikidata_qid="Q207406")
        store.get_or_create_entity("Stereolab", "artist", wikidata_qid="Q483477")
        assert store.find_duplicate_qid_groups() == []

    def test_null_qids_excluded(self, store: EntityStore):
        store.get_or_create_entity("Autechre", "artist")
        store.get_or_create_entity("Stereolab", "artist")
        assert store.find_duplicate_qid_groups() == []

    def test_finds_single_duplicate_group(self, store: EntityStore):
        store._conn.execute(
            "INSERT INTO entity (name, entity_type, wikidata_qid) VALUES (?, ?, ?)",
            ("Autechre", "artist", "Q207406"),
        )
        store._conn.execute(
            "INSERT INTO entity (name, entity_type, wikidata_qid) VALUES (?, ?, ?)",
            ("Ae", "artist", "Q207406"),
        )
        store._conn.commit()

        groups = store.find_duplicate_qid_groups()
        assert len(groups) == 1
        qid, ids = groups[0]
        assert qid == "Q207406"
        assert len(ids) == 2

    def test_multiple_groups(self, store: EntityStore):
        for name, qid in [
            ("Autechre", "Q207406"),
            ("Ae", "Q207406"),
            ("Stereolab", "Q483477"),
            ("Stereo Lab", "Q483477"),
        ]:
            store._conn.execute(
                "INSERT INTO entity (name, entity_type, wikidata_qid) VALUES (?, ?, ?)",
                (name, "artist", qid),
            )
        store._conn.commit()

        groups = store.find_duplicate_qid_groups()
        assert len(groups) == 2
        qids = {g[0] for g in groups}
        assert qids == {"Q207406", "Q483477"}

    def test_entity_ids_sorted_ascending(self, store: EntityStore):
        store._conn.execute(
            "INSERT INTO entity (name, entity_type, wikidata_qid) VALUES (?, ?, ?)",
            ("Autechre", "artist", "Q207406"),
        )
        store._conn.execute(
            "INSERT INTO entity (name, entity_type, wikidata_qid) VALUES (?, ?, ?)",
            ("Ae", "artist", "Q207406"),
        )
        store._conn.commit()

        _, ids = store.find_duplicate_qid_groups()[0]
        assert ids == sorted(ids)

    def test_mixed_duplicate_and_unique_qids(self, store: EntityStore):
        """Only groups with 2+ entities are returned; unique QIDs are excluded."""
        store.get_or_create_entity("Cat Power", "artist", wikidata_qid="Q271362")
        store._conn.execute(
            "INSERT INTO entity (name, entity_type, wikidata_qid) VALUES (?, ?, ?)",
            ("Autechre", "artist", "Q207406"),
        )
        store._conn.execute(
            "INSERT INTO entity (name, entity_type, wikidata_qid) VALUES (?, ?, ?)",
            ("Ae", "artist", "Q207406"),
        )
        store._conn.commit()

        groups = store.find_duplicate_qid_groups()
        assert len(groups) == 1
        assert groups[0][0] == "Q207406"


# ---------------------------------------------------------------------------
# deduplicate_by_qid
# ---------------------------------------------------------------------------


class TestDeduplicateByQid:
    def test_no_duplicates_returns_zeros(self, store: EntityStore):
        report = store.deduplicate_by_qid()
        assert report == DeduplicationReport(
            groups_found=0, entities_merged=0, artists_reassigned=0
        )

    def test_merges_duplicate_entities(self, store: EntityStore):
        store._conn.execute(
            "INSERT INTO entity (name, entity_type, wikidata_qid) VALUES (?, ?, ?)",
            ("Autechre", "artist", "Q207406"),
        )
        store._conn.execute(
            "INSERT INTO entity (name, entity_type, wikidata_qid) VALUES (?, ?, ?)",
            ("Ae", "artist", "Q207406"),
        )
        store._conn.commit()

        e1_id = store._conn.execute("SELECT id FROM entity WHERE name='Autechre'").fetchone()[0]
        e2_id = store._conn.execute("SELECT id FROM entity WHERE name='Ae'").fetchone()[0]

        # Artist linked to the second (merge) entity
        store.upsert_artist("Ae", entity_id=e2_id)

        report = store.deduplicate_by_qid()
        assert report.groups_found == 1
        assert report.entities_merged == 1
        assert report.artists_reassigned == 1

        # Artist should be re-parented to the kept entity
        row = store.get_artist_by_name("Ae")
        assert row is not None
        assert row["entity_id"] == e1_id

        # Merged entity should be deleted
        assert store._conn.execute("SELECT id FROM entity WHERE id=?", (e2_id,)).fetchone() is None

    def test_keeps_lowest_id_entity(self, store: EntityStore):
        store._conn.execute(
            "INSERT INTO entity (name, entity_type, wikidata_qid) VALUES (?, ?, ?)",
            ("Autechre", "artist", "Q207406"),
        )
        store._conn.execute(
            "INSERT INTO entity (name, entity_type, wikidata_qid) VALUES (?, ?, ?)",
            ("Ae", "artist", "Q207406"),
        )
        store._conn.commit()

        e1_id = store._conn.execute("SELECT id FROM entity WHERE name='Autechre'").fetchone()[0]

        store.deduplicate_by_qid()

        # First (lowest ID) entity should survive
        assert (
            store._conn.execute("SELECT id FROM entity WHERE id=?", (e1_id,)).fetchone() is not None
        )
        # Only one entity should remain for this QID
        count = store._conn.execute(
            "SELECT COUNT(*) FROM entity WHERE wikidata_qid='Q207406'"
        ).fetchone()[0]
        assert count == 1

    def test_multiple_groups_merged(self, store: EntityStore):
        for name, qid in [
            ("Autechre", "Q207406"),
            ("Ae", "Q207406"),
            ("Stereolab", "Q483477"),
            ("Stereo Lab", "Q483477"),
        ]:
            store._conn.execute(
                "INSERT INTO entity (name, entity_type, wikidata_qid) VALUES (?, ?, ?)",
                (name, "artist", qid),
            )
        store._conn.commit()

        report = store.deduplicate_by_qid()
        assert report.groups_found == 2
        assert report.entities_merged == 2

        # Two entities should remain (one per QID)
        count = store._conn.execute("SELECT COUNT(*) FROM entity").fetchone()[0]
        assert count == 2

    def test_three_entities_with_same_qid(self, store: EntityStore):
        for name in ["Autechre", "Ae", "Autechre (UK)"]:
            store._conn.execute(
                "INSERT INTO entity (name, entity_type, wikidata_qid) VALUES (?, ?, ?)",
                (name, "artist", "Q207406"),
            )
        store._conn.commit()

        report = store.deduplicate_by_qid()
        assert report.groups_found == 1
        assert report.entities_merged == 2

        count = store._conn.execute("SELECT COUNT(*) FROM entity").fetchone()[0]
        assert count == 1

    def test_reassigns_artists_from_all_merged_entities(self, store: EntityStore):
        """Artists from all merged entities should point to the kept entity."""
        store._conn.execute(
            "INSERT INTO entity (name, entity_type, wikidata_qid) VALUES (?, ?, ?)",
            ("Autechre", "artist", "Q207406"),
        )
        store._conn.execute(
            "INSERT INTO entity (name, entity_type, wikidata_qid) VALUES (?, ?, ?)",
            ("Ae", "artist", "Q207406"),
        )
        store._conn.execute(
            "INSERT INTO entity (name, entity_type, wikidata_qid) VALUES (?, ?, ?)",
            ("Autechre (UK)", "artist", "Q207406"),
        )
        store._conn.commit()

        e1_id = store._conn.execute("SELECT id FROM entity WHERE name='Autechre'").fetchone()[0]
        e2_id = store._conn.execute("SELECT id FROM entity WHERE name='Ae'").fetchone()[0]
        e3_id = store._conn.execute("SELECT id FROM entity WHERE name='Autechre (UK)'").fetchone()[
            0
        ]

        store.upsert_artist("Ae", entity_id=e2_id)
        store.upsert_artist("Autechre (UK)", entity_id=e3_id)

        report = store.deduplicate_by_qid()
        assert report.artists_reassigned == 2

        for name in ["Ae", "Autechre (UK)"]:
            row = store.get_artist_by_name(name)
            assert row is not None
            assert row["entity_id"] == e1_id


# ---------------------------------------------------------------------------
# Migration: remove UNIQUE constraint on entity.wikidata_qid
# ---------------------------------------------------------------------------


class TestMigrateEntityUniqueQid:
    def test_rebuilds_table_without_unique(self, tmp_path):
        """Entity table with UNIQUE constraint should be rebuilt without it."""
        db_path = str(tmp_path / "test.db")
        conn = sqlite3.connect(db_path)
        conn.executescript(_OLD_ARTIST_SCHEMA)
        # Create entity table WITH the old UNIQUE constraint
        conn.executescript("""
            CREATE TABLE entity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wikidata_qid TEXT UNIQUE,
                name TEXT NOT NULL,
                entity_type TEXT NOT NULL DEFAULT 'artist',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            );
        """)
        conn.execute(
            "INSERT INTO entity (name, entity_type, wikidata_qid) "
            "VALUES ('Autechre', 'artist', 'Q207406')"
        )
        conn.commit()
        conn.close()

        store = EntityStore(db_path)
        store.initialize()

        # Should be able to insert duplicate QID after migration
        store._conn.execute(
            "INSERT INTO entity (name, entity_type, wikidata_qid) "
            "VALUES ('Ae', 'artist', 'Q207406')"
        )
        store._conn.commit()  # Should not raise

        rows = store._conn.execute(
            "SELECT name FROM entity WHERE wikidata_qid='Q207406' ORDER BY id"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0][0] == "Autechre"
        assert rows[1][0] == "Ae"

    def test_preserves_data_during_migration(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        conn = sqlite3.connect(db_path)
        conn.executescript(_OLD_ARTIST_SCHEMA)
        conn.executescript("""
            CREATE TABLE entity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wikidata_qid TEXT UNIQUE,
                name TEXT NOT NULL,
                entity_type TEXT NOT NULL DEFAULT 'artist',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            );
        """)
        conn.execute(
            "INSERT INTO entity (name, entity_type, wikidata_qid) "
            "VALUES ('Autechre', 'artist', 'Q207406')"
        )
        conn.execute(
            "INSERT INTO entity (name, entity_type, wikidata_qid) "
            "VALUES ('Stereolab', 'artist', 'Q483477')"
        )
        conn.commit()
        conn.close()

        store = EntityStore(db_path)
        store.initialize()

        rows = store._conn.execute("SELECT name, wikidata_qid FROM entity ORDER BY name").fetchall()
        assert len(rows) == 2
        assert rows[0] == ("Autechre", "Q207406")
        assert rows[1] == ("Stereolab", "Q483477")

    def test_no_migration_needed_when_already_without_unique(self, tmp_path):
        """If entity table already lacks UNIQUE, initialize is a no-op for migration."""
        db_path = str(tmp_path / "test.db")
        conn = sqlite3.connect(db_path)
        conn.executescript(_OLD_ARTIST_SCHEMA)
        # Entity table WITHOUT UNIQUE (already migrated)
        conn.executescript("""
            CREATE TABLE entity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wikidata_qid TEXT,
                name TEXT NOT NULL,
                entity_type TEXT NOT NULL DEFAULT 'artist',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            );
        """)
        conn.execute(
            "INSERT INTO entity (name, entity_type, wikidata_qid) "
            "VALUES ('Autechre', 'artist', 'Q207406')"
        )
        conn.commit()
        conn.close()

        store = EntityStore(db_path)
        store.initialize()

        # Data should be intact
        row = store._conn.execute("SELECT name FROM entity WHERE wikidata_qid='Q207406'").fetchone()
        assert row[0] == "Autechre"
