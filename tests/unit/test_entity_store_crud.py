"""Tests for EntityStore CRUD operations, artist upsert, and stats update."""

import sqlite3

import pytest

from semantic_index.entity_store import EntityStore
from semantic_index.models import ArtistStats

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
# Step 2: Entity CRUD
# ---------------------------------------------------------------------------


class TestGetOrCreateEntity:
    def test_creates_new_entity(self, store: EntityStore):
        entity = store.get_or_create_entity("Autechre", "artist")
        assert entity.name == "Autechre"
        assert entity.entity_type == "artist"
        assert entity.id is not None
        assert entity.wikidata_qid is None

    def test_returns_existing_entity(self, store: EntityStore):
        first = store.get_or_create_entity("Stereolab", "artist")
        second = store.get_or_create_entity("Stereolab", "artist")
        assert first.id == second.id

    def test_with_wikidata_qid(self, store: EntityStore):
        entity = store.get_or_create_entity("Autechre", "artist", wikidata_qid="Q207406")
        assert entity.wikidata_qid == "Q207406"

    def test_different_entity_types(self, store: EntityStore):
        artist = store.get_or_create_entity("Warp Records", "label")
        assert artist.entity_type == "label"

    def test_get_or_create_does_not_overwrite_existing_qid(self, store: EntityStore):
        """If an entity already has a QID, get_or_create should not overwrite it."""
        store.get_or_create_entity("Autechre", "artist", wikidata_qid="Q207406")
        again = store.get_or_create_entity("Autechre", "artist", wikidata_qid="Q999999")
        assert again.wikidata_qid == "Q207406"


class TestUpdateEntityQid:
    def test_update_qid(self, store: EntityStore):
        entity = store.get_or_create_entity("Cat Power", "artist")
        assert entity.wikidata_qid is None
        store.update_entity_qid(entity.id, "Q271362")
        updated = store.get_entity_by_qid("Q271362")
        assert updated is not None
        assert updated.name == "Cat Power"

    def test_update_qid_nonexistent_entity(self, store: EntityStore):
        with pytest.raises(ValueError, match="No entity with id"):
            store.update_entity_qid(9999, "Q123")


class TestGetEntityByQid:
    def test_found(self, store: EntityStore):
        store.get_or_create_entity("Autechre", "artist", wikidata_qid="Q207406")
        entity = store.get_entity_by_qid("Q207406")
        assert entity is not None
        assert entity.name == "Autechre"

    def test_not_found(self, store: EntityStore):
        result = store.get_entity_by_qid("Q000000")
        assert result is None


class TestMergeEntities:
    def test_merge_reparents_artists(self, store: EntityStore):
        keep = store.get_or_create_entity("Autechre", "artist")
        merge = store.get_or_create_entity("Autechre (UK)", "artist")

        # Create an artist row linked to the merge entity
        store._conn.execute(
            """INSERT INTO artist (canonical_name, total_plays, dj_count, request_ratio, show_count, entity_id)
               VALUES ('Autechre (UK)', 5, 2, 0.0, 3, ?)""",
            (merge.id,),
        )
        store._conn.commit()

        store.merge_entities(keep.id, merge.id)

        # Artist should now point to the keep entity
        row = store._conn.execute(
            "SELECT entity_id FROM artist WHERE canonical_name = 'Autechre (UK)'"
        ).fetchone()
        assert row[0] == keep.id

        # Merged entity should be deleted
        row = store._conn.execute("SELECT id FROM entity WHERE id = ?", (merge.id,)).fetchone()
        assert row is None

    def test_merge_nonexistent_raises(self, store: EntityStore):
        keep = store.get_or_create_entity("Autechre", "artist")
        with pytest.raises(ValueError, match="No entity with id"):
            store.merge_entities(keep.id, 9999)

    def test_merge_same_entity_raises(self, store: EntityStore):
        entity = store.get_or_create_entity("Autechre", "artist")
        with pytest.raises(ValueError, match="Cannot merge"):
            store.merge_entities(entity.id, entity.id)


# ---------------------------------------------------------------------------
# Step 3: Artist Upsert
# ---------------------------------------------------------------------------


class TestUpsertArtist:
    def test_insert_new_artist(self, store: EntityStore):
        artist_id = store.upsert_artist("Autechre")
        assert artist_id is not None
        row = store._conn.execute(
            "SELECT canonical_name FROM artist WHERE id = ?", (artist_id,)
        ).fetchone()
        assert row[0] == "Autechre"

    def test_upsert_existing_returns_same_id(self, store: EntityStore):
        first_id = store.upsert_artist("Stereolab")
        second_id = store.upsert_artist("Stereolab")
        assert first_id == second_id

    def test_upsert_with_genre(self, store: EntityStore):
        store.upsert_artist("Autechre", genre="Electronic")
        row = store._conn.execute(
            "SELECT genre FROM artist WHERE canonical_name = 'Autechre'"
        ).fetchone()
        assert row[0] == "Electronic"

    def test_upsert_does_not_overwrite_with_null(self, store: EntityStore):
        """COALESCE semantics: NULL arguments don't clobber existing data."""
        store.upsert_artist("Autechre", genre="Electronic")
        store.upsert_artist("Autechre")  # genre=None
        row = store._conn.execute(
            "SELECT genre FROM artist WHERE canonical_name = 'Autechre'"
        ).fetchone()
        assert row[0] == "Electronic"

    def test_upsert_updates_populated_field(self, store: EntityStore):
        """An explicit non-None value should update an existing field."""
        store.upsert_artist("Autechre", genre="Electronic")
        store.upsert_artist("Autechre", genre="IDM")
        row = store._conn.execute(
            "SELECT genre FROM artist WHERE canonical_name = 'Autechre'"
        ).fetchone()
        assert row[0] == "IDM"

    def test_upsert_with_discogs_artist_id(self, store: EntityStore):
        store.upsert_artist("Autechre", discogs_artist_id=42)
        row = store._conn.execute(
            "SELECT discogs_artist_id FROM artist WHERE canonical_name = 'Autechre'"
        ).fetchone()
        assert row[0] == 42

    def test_upsert_with_entity_id(self, store: EntityStore):
        entity = store.get_or_create_entity("Autechre", "artist")
        store.upsert_artist("Autechre", entity_id=entity.id)
        row = store._conn.execute(
            "SELECT entity_id FROM artist WHERE canonical_name = 'Autechre'"
        ).fetchone()
        assert row[0] == entity.id


class TestGetArtistByName:
    def test_found(self, store: EntityStore):
        store.upsert_artist("Father John Misty", genre="Rock")
        row = store.get_artist_by_name("Father John Misty")
        assert row is not None
        assert row["canonical_name"] == "Father John Misty"
        assert row["genre"] == "Rock"

    def test_not_found(self, store: EntityStore):
        result = store.get_artist_by_name("Nonexistent Artist")
        assert result is None


class TestBulkUpsertArtists:
    def test_bulk_upsert_creates_multiple(self, store: EntityStore):
        names = ["Autechre", "Stereolab", "Cat Power", "Buck Meek"]
        id_map = store.bulk_upsert_artists(names)
        assert len(id_map) == 4
        assert all(isinstance(v, int) for v in id_map.values())
        assert set(id_map.keys()) == set(names)

    def test_bulk_upsert_idempotent(self, store: EntityStore):
        names = ["Autechre", "Stereolab"]
        first = store.bulk_upsert_artists(names)
        second = store.bulk_upsert_artists(names)
        assert first == second

    def test_bulk_upsert_mixed_existing_and_new(self, store: EntityStore):
        store.upsert_artist("Autechre", genre="Electronic")
        names = ["Autechre", "Juana Molina"]
        id_map = store.bulk_upsert_artists(names)
        assert len(id_map) == 2
        # Original genre should be preserved
        row = store._conn.execute(
            "SELECT genre FROM artist WHERE canonical_name = 'Autechre'"
        ).fetchone()
        assert row[0] == "Electronic"

    def test_bulk_upsert_empty_list(self, store: EntityStore):
        id_map = store.bulk_upsert_artists([])
        assert id_map == {}

    def test_bulk_upsert_deduplicates_input(self, store: EntityStore):
        names = ["Autechre", "Autechre", "Stereolab"]
        id_map = store.bulk_upsert_artists(names)
        assert len(id_map) == 2


# ---------------------------------------------------------------------------
# Step 4: Artist Stats
# ---------------------------------------------------------------------------


class TestUpdateArtistStats:
    def test_updates_stats_for_existing_artist(self, store: EntityStore):
        store.upsert_artist("Autechre")
        stats = ArtistStats(
            canonical_name="Autechre",
            total_plays=50,
            genre="Electronic",
            active_first_year=1998,
            active_last_year=2024,
            dj_count=15,
            request_ratio=0.1,
            show_count=40,
        )
        store.update_artist_stats("Autechre", stats)

        row = store._conn.execute(
            """SELECT total_plays, genre, active_first_year, active_last_year,
                      dj_count, request_ratio, show_count
               FROM artist WHERE canonical_name = 'Autechre'"""
        ).fetchone()
        assert row[0] == 50
        assert row[1] == "Electronic"
        assert row[2] == 1998
        assert row[3] == 2024
        assert row[4] == 15
        assert row[5] == pytest.approx(0.1)
        assert row[6] == 40

    def test_updates_nonexistent_artist_raises(self, store: EntityStore):
        stats = ArtistStats(canonical_name="Nobody", total_plays=1)
        with pytest.raises(ValueError, match="No artist"):
            store.update_artist_stats("Nobody", stats)


class TestBulkUpdateStats:
    def test_bulk_update_multiple_artists(self, store: EntityStore):
        store.bulk_upsert_artists(["Autechre", "Stereolab", "Cat Power"])
        artist_stats = {
            "Autechre": ArtistStats(
                canonical_name="Autechre", total_plays=50, genre="Electronic", dj_count=15
            ),
            "Stereolab": ArtistStats(
                canonical_name="Stereolab", total_plays=30, genre="Rock", dj_count=10
            ),
            "Cat Power": ArtistStats(
                canonical_name="Cat Power", total_plays=20, genre="Rock", dj_count=8
            ),
        }
        store.bulk_update_stats(artist_stats)

        for name, stats in artist_stats.items():
            row = store._conn.execute(
                "SELECT total_plays, genre, dj_count FROM artist WHERE canonical_name = ?",
                (name,),
            ).fetchone()
            assert row[0] == stats.total_plays
            assert row[1] == stats.genre
            assert row[2] == stats.dj_count

    def test_bulk_update_empty_dict(self, store: EntityStore):
        store.bulk_update_stats({})  # Should not raise

    def test_bulk_update_preserves_other_fields(self, store: EntityStore):
        store.upsert_artist("Autechre", discogs_artist_id=42)
        stats = {
            "Autechre": ArtistStats(canonical_name="Autechre", total_plays=50, genre="Electronic"),
        }
        store.bulk_update_stats(stats)
        row = store._conn.execute(
            "SELECT discogs_artist_id FROM artist WHERE canonical_name = 'Autechre'"
        ).fetchone()
        assert row[0] == 42
