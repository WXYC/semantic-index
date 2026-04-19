"""Tests for PipelineDB entity deduplication with edge re-keying.

Verifies that deduplicate_by_qid() re-keys all artist-referencing tables
(Discogs edges, acoustic similarity, wikidata influence, audio profile,
artist style, artist personnel, artist label) when merging entities that
share a Wikidata QID, handling self-referential edges and PK conflicts.
"""

import json

import pytest

from semantic_index.pipeline_db import PipelineDB
from semantic_index.sqlite_export import (
    _EDGE_ENRICHMENT_SCHEMA,
)

# Schema for tables not included in _EDGE_ENRICHMENT_SCHEMA.
_AUDIO_TABLES_SCHEMA = """
CREATE TABLE IF NOT EXISTS audio_profile (
    artist_id INTEGER PRIMARY KEY REFERENCES artist(id),
    avg_danceability REAL,
    primary_genre TEXT,
    primary_genre_probability REAL,
    voice_instrumental_ratio REAL,
    feature_centroid TEXT,
    recording_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS acoustic_similarity (
    artist_a_id INTEGER NOT NULL REFERENCES artist(id),
    artist_b_id INTEGER NOT NULL REFERENCES artist(id),
    similarity REAL NOT NULL,
    PRIMARY KEY (artist_a_id, artist_b_id)
);
"""

# -- Fixtures ----------------------------------------------------------------


@pytest.fixture
def db(tmp_path):
    """PipelineDB with all edge and enrichment tables initialized."""
    pdb = PipelineDB(str(tmp_path / "test.db"))
    pdb.initialize()
    pdb._conn.executescript(_EDGE_ENRICHMENT_SCHEMA)
    pdb._conn.executescript(_AUDIO_TABLES_SCHEMA)
    return pdb


def _create_entity(db: PipelineDB, entity_id: int, name: str, qid: str) -> None:
    """Insert an entity row directly."""
    db._conn.execute(
        "INSERT INTO entity (id, name, entity_type, wikidata_qid, created_at, updated_at) "
        "VALUES (?, ?, 'artist', ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), "
        "strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))",
        (entity_id, name, qid),
    )
    db._conn.commit()


def _set_up_alias_pair(db: PipelineDB) -> dict[str, int]:
    """Create Sun Ra / Le Sony'r Ra as alias artists sharing QID Q312545.

    Also creates Autechre and Stereolab as third-party artists for edges.

    Returns:
        Dict mapping canonical names to artist IDs.
    """
    _create_entity(db, 1, "Sun Ra", "Q312545")
    _create_entity(db, 2, "Le Sony'r Ra", "Q312545")

    ids: dict[str, int] = {}
    ids["Sun Ra"] = db.upsert_artist("Sun Ra", entity_id=1)
    ids["Le Sony'r Ra"] = db.upsert_artist("Le Sony'r Ra", entity_id=2)
    ids["Autechre"] = db.upsert_artist("Autechre")
    ids["Stereolab"] = db.upsert_artist("Stereolab")
    return ids


def _get_edges(db: PipelineDB, table: str) -> list[tuple[int, int]]:
    """Return all (artist_a_id, artist_b_id) pairs from an edge table."""
    return db._conn.execute(f"SELECT artist_a_id, artist_b_id FROM {table}").fetchall()


# -- Tests -------------------------------------------------------------------


class TestRekeyUpdatesArtistIds:
    """Verify that re-keying replaces merge_id with keep_id in both columns."""

    def test_rekey_updates_artist_a_id(self, db):
        """Edge (merge_id, X) becomes (keep_id, X) after dedup."""
        ids = _set_up_alias_pair(db)
        sun_ra = ids["Sun Ra"]
        le_sonyr = ids["Le Sony'r Ra"]
        autechre = ids["Autechre"]

        db._conn.execute(
            "INSERT INTO shared_personnel VALUES (?, ?, 2, ?)",
            (le_sonyr, autechre, json.dumps(["Marshall Allen"])),
        )
        db._conn.commit()

        db.deduplicate_by_qid()

        edges = _get_edges(db, "shared_personnel")
        assert len(edges) == 1
        assert edges[0] == (sun_ra, autechre)

    def test_rekey_updates_artist_b_id(self, db):
        """Edge (X, merge_id) becomes (X, keep_id) after dedup."""
        ids = _set_up_alias_pair(db)
        sun_ra = ids["Sun Ra"]
        le_sonyr = ids["Le Sony'r Ra"]
        autechre = ids["Autechre"]

        db._conn.execute(
            "INSERT INTO shared_personnel VALUES (?, ?, 1, ?)",
            (autechre, le_sonyr, json.dumps(["Pat Patrick"])),
        )
        db._conn.commit()

        db.deduplicate_by_qid()

        edges = _get_edges(db, "shared_personnel")
        assert len(edges) == 1
        assert edges[0] == (autechre, sun_ra)


class TestSelfReferentialEdges:
    """Verify that edges between alias artists are removed (would become self-loops)."""

    def test_self_referential_deleted(self, db):
        """Edge between merge and keep is removed after dedup."""
        ids = _set_up_alias_pair(db)
        sun_ra = ids["Sun Ra"]
        le_sonyr = ids["Le Sony'r Ra"]

        db._conn.execute(
            "INSERT INTO shared_personnel VALUES (?, ?, 5, ?)",
            (sun_ra, le_sonyr, json.dumps(["Marshall Allen", "John Gilmore"])),
        )
        db._conn.commit()

        db.deduplicate_by_qid()

        edges = _get_edges(db, "shared_personnel")
        assert len(edges) == 0

    def test_self_referential_reverse_deleted(self, db):
        """Edge (merge, keep) in reverse direction is also removed."""
        ids = _set_up_alias_pair(db)
        sun_ra = ids["Sun Ra"]
        le_sonyr = ids["Le Sony'r Ra"]

        db._conn.execute(
            "INSERT INTO shared_style VALUES (?, ?, 0.95, ?)",
            (le_sonyr, sun_ra, json.dumps(["Free Jazz", "Avant-Garde"])),
        )
        db._conn.commit()

        db.deduplicate_by_qid()

        edges = _get_edges(db, "shared_style")
        assert len(edges) == 0


class TestDuplicatePkResolution:
    """Verify that PK conflicts after re-keying are resolved."""

    def test_duplicate_pk_resolved(self, db):
        """When both (merge_id, X) and (keep_id, X) exist, only one survives."""
        ids = _set_up_alias_pair(db)
        sun_ra = ids["Sun Ra"]
        le_sonyr = ids["Le Sony'r Ra"]
        autechre = ids["Autechre"]

        # Both Sun Ra and Le Sony'r Ra have shared_personnel edges to Autechre
        db._conn.execute(
            "INSERT INTO shared_personnel VALUES (?, ?, 3, ?)",
            (sun_ra, autechre, json.dumps(["Marshall Allen"])),
        )
        db._conn.execute(
            "INSERT INTO shared_personnel VALUES (?, ?, 1, ?)",
            (le_sonyr, autechre, json.dumps(["Pat Patrick"])),
        )
        db._conn.commit()

        db.deduplicate_by_qid()

        edges = _get_edges(db, "shared_personnel")
        assert len(edges) == 1
        a, b = edges[0]
        assert {a, b} == {sun_ra, autechre}

    def test_reverse_duplicate_resolved(self, db):
        """When (merge_id, X) and (X, keep_id) exist, only one survives."""
        ids = _set_up_alias_pair(db)
        sun_ra = ids["Sun Ra"]
        le_sonyr = ids["Le Sony'r Ra"]
        stereolab = ids["Stereolab"]

        # (Le Sony'r Ra, Stereolab) and (Stereolab, Sun Ra) — same edge after re-key
        db._conn.execute(
            "INSERT INTO label_family VALUES (?, ?, ?)",
            (le_sonyr, stereolab, json.dumps(["Evidence"])),
        )
        db._conn.execute(
            "INSERT INTO label_family VALUES (?, ?, ?)",
            (stereolab, sun_ra, json.dumps(["Evidence"])),
        )
        db._conn.commit()

        db.deduplicate_by_qid()

        edges = _get_edges(db, "label_family")
        assert len(edges) == 1
        a, b = edges[0]
        assert {a, b} == {sun_ra, stereolab}


class TestAllTablesRekeyed:
    """Verify that all artist-referencing tables are re-keyed."""

    def test_all_symmetric_tables_rekeyed(self, db):
        """Each symmetric edge table (Discogs + acoustic) is updated."""
        ids = _set_up_alias_pair(db)
        le_sonyr = ids["Le Sony'r Ra"]
        sun_ra = ids["Sun Ra"]
        autechre = ids["Autechre"]

        db._conn.execute(
            "INSERT INTO shared_personnel VALUES (?, ?, 1, ?)",
            (le_sonyr, autechre, json.dumps(["member"])),
        )
        db._conn.execute(
            "INSERT INTO shared_style VALUES (?, ?, 0.5, ?)",
            (le_sonyr, autechre, json.dumps(["Jazz"])),
        )
        db._conn.execute(
            "INSERT INTO label_family VALUES (?, ?, ?)",
            (le_sonyr, autechre, json.dumps(["Warp"])),
        )
        db._conn.execute(
            "INSERT INTO compilation VALUES (?, ?, 1, ?)",
            (le_sonyr, autechre, json.dumps(["Comp 1"])),
        )
        db._conn.execute(
            "INSERT INTO acoustic_similarity VALUES (?, ?, 0.95)",
            (le_sonyr, autechre),
        )
        db._conn.commit()

        db.deduplicate_by_qid()

        for table in (
            "shared_personnel",
            "shared_style",
            "label_family",
            "compilation",
            "acoustic_similarity",
        ):
            edges = _get_edges(db, table)
            assert len(edges) == 1, f"Expected 1 edge in {table}, got {len(edges)}"
            assert edges[0] == (sun_ra, autechre), f"Edge not re-keyed in {table}"

    def test_all_table_types_rekeyed_in_one_dedup(self, db):
        """All table types (symmetric, directed, single-artist) re-keyed in one pass."""
        ids = _set_up_alias_pair(db)
        le_sonyr = ids["Le Sony'r Ra"]
        sun_ra = ids["Sun Ra"]
        autechre = ids["Autechre"]

        # Symmetric edge
        db._conn.execute(
            "INSERT INTO shared_personnel VALUES (?, ?, 1, ?)",
            (le_sonyr, autechre, json.dumps(["member"])),
        )
        # Directed edge
        db._conn.execute(
            "INSERT INTO wikidata_influence VALUES (?, ?, 'Q312545', 'Q2774')",
            (le_sonyr, autechre),
        )
        # Single-artist PK
        db._conn.execute(
            "INSERT INTO audio_profile (artist_id, avg_danceability, recording_count) "
            "VALUES (?, 0.5, 10)",
            (le_sonyr,),
        )
        # Composite PK
        db._conn.execute("INSERT INTO artist_style VALUES (?, 'Free Jazz')", (le_sonyr,))
        # FK-only
        db._conn.execute(
            "INSERT INTO artist_personnel VALUES (?, 'Marshall Allen', 'alto sax')",
            (le_sonyr,),
        )
        # Composite PK
        db._conn.execute(
            "INSERT INTO artist_label VALUES (?, 'Saturn Records', NULL)",
            (le_sonyr,),
        )
        db._conn.commit()

        report = db.deduplicate_by_qid()

        # All rows re-keyed: 1 symmetric + 1 directed + 1 audio_profile
        # + 1 artist_style + 1 artist_personnel + 1 artist_label = 6
        assert report.edges_rekeyed == 6

        # Verify each table
        edges = _get_edges(db, "shared_personnel")
        assert edges[0] == (sun_ra, autechre)

        inf = _get_influence_edges(db)
        assert inf[0] == (sun_ra, autechre)

        ap = db._conn.execute("SELECT artist_id FROM audio_profile").fetchall()
        assert ap[0][0] == sun_ra

        styles = db._conn.execute("SELECT artist_id FROM artist_style").fetchall()
        assert styles[0][0] == sun_ra

        personnel = db._conn.execute("SELECT artist_id FROM artist_personnel").fetchall()
        assert personnel[0][0] == sun_ra

        labels = db._conn.execute("SELECT artist_id FROM artist_label").fetchall()
        assert labels[0][0] == sun_ra


class TestEdgeCases:
    """Verify graceful handling of edge cases."""

    def test_missing_tables_no_error(self, tmp_path):
        """Dedup works when edge tables don't exist (fresh DB)."""
        pdb = PipelineDB(str(tmp_path / "bare.db"))
        pdb.initialize()
        # No _EDGE_ENRICHMENT_SCHEMA — edge tables don't exist

        _create_entity(pdb, 1, "Sun Ra", "Q312545")
        _create_entity(pdb, 2, "Le Sony'r Ra", "Q312545")
        pdb.upsert_artist("Sun Ra", entity_id=1)
        pdb.upsert_artist("Le Sony'r Ra", entity_id=2)

        report = pdb.deduplicate_by_qid()
        assert report.groups_found == 1
        assert report.entities_merged == 1
        assert report.edges_rekeyed == 0

    def test_single_artist_per_entity_noop(self, db):
        """No re-keying when entity has only one artist row."""
        _create_entity(db, 10, "Autechre", "Q2774")
        _create_entity(db, 11, "Autechre (duplicate)", "Q2774")
        autechre_id = db.upsert_artist("Autechre", entity_id=10)
        stereolab_id = db.upsert_artist("Stereolab")
        # No second artist with entity_id=11

        db._conn.execute(
            "INSERT INTO shared_personnel VALUES (?, ?, 1, ?)",
            (autechre_id, stereolab_id, json.dumps(["member"])),
        )
        db._conn.commit()

        report = db.deduplicate_by_qid()
        assert report.edges_rekeyed == 0

        # Edge is unchanged
        edges = _get_edges(db, "shared_personnel")
        assert len(edges) == 1
        assert edges[0] == (autechre_id, stereolab_id)


class TestDeduplicationReport:
    """Verify the DeduplicationReport includes edges_rekeyed."""

    def test_dedup_report_edges_rekeyed(self, db):
        """Full deduplicate_by_qid() round-trip: report has correct edges_rekeyed."""
        ids = _set_up_alias_pair(db)
        le_sonyr = ids["Le Sony'r Ra"]
        autechre = ids["Autechre"]
        stereolab = ids["Stereolab"]

        # Two edges from Le Sony'r Ra (merge target)
        db._conn.execute(
            "INSERT INTO shared_personnel VALUES (?, ?, 2, ?)",
            (le_sonyr, autechre, json.dumps(["Marshall Allen"])),
        )
        db._conn.execute(
            "INSERT INTO shared_style VALUES (?, ?, 0.8, ?)",
            (stereolab, le_sonyr, json.dumps(["Free Jazz"])),
        )
        db._conn.commit()

        report = db.deduplicate_by_qid()

        assert report.groups_found == 1
        assert report.entities_merged == 1
        assert report.edges_rekeyed == 2

    def test_dedup_no_qids_noop(self, db):
        """Empty DB with no QIDs is a no-op (existing behavior preserved)."""
        db.upsert_artist("Autechre")
        db.upsert_artist("Stereolab")

        report = db.deduplicate_by_qid()

        assert report.groups_found == 0
        assert report.entities_merged == 0
        assert report.artists_reassigned == 0
        assert report.edges_rekeyed == 0


# -- Acoustic similarity re-keying ------------------------------------------


class TestAcousticSimilarityRekey:
    """Verify that acoustic_similarity edges are re-keyed during dedup."""

    def test_rekey_acoustic_similarity(self, db):
        """Acoustic similarity edge (merge_id, X) becomes (keep_id, X)."""
        ids = _set_up_alias_pair(db)
        sun_ra = ids["Sun Ra"]
        le_sonyr = ids["Le Sony'r Ra"]
        autechre = ids["Autechre"]

        db._conn.execute(
            "INSERT INTO acoustic_similarity VALUES (?, ?, 0.97)",
            (le_sonyr, autechre),
        )
        db._conn.commit()

        db.deduplicate_by_qid()

        edges = _get_edges(db, "acoustic_similarity")
        assert len(edges) == 1
        assert edges[0] == (sun_ra, autechre)

    def test_acoustic_similarity_self_loop_deleted(self, db):
        """Acoustic similarity between alias artists is removed."""
        ids = _set_up_alias_pair(db)
        sun_ra = ids["Sun Ra"]
        le_sonyr = ids["Le Sony'r Ra"]

        db._conn.execute(
            "INSERT INTO acoustic_similarity VALUES (?, ?, 0.99)",
            (sun_ra, le_sonyr),
        )
        db._conn.commit()

        db.deduplicate_by_qid()

        edges = _get_edges(db, "acoustic_similarity")
        assert len(edges) == 0

    def test_acoustic_similarity_pk_conflict(self, db):
        """When both aliases have similarity to same artist, only one survives."""
        ids = _set_up_alias_pair(db)
        sun_ra = ids["Sun Ra"]
        le_sonyr = ids["Le Sony'r Ra"]
        autechre = ids["Autechre"]

        db._conn.execute(
            "INSERT INTO acoustic_similarity VALUES (?, ?, 0.95)",
            (sun_ra, autechre),
        )
        db._conn.execute(
            "INSERT INTO acoustic_similarity VALUES (?, ?, 0.92)",
            (le_sonyr, autechre),
        )
        db._conn.commit()

        db.deduplicate_by_qid()

        edges = _get_edges(db, "acoustic_similarity")
        assert len(edges) == 1
        a, b = edges[0]
        assert {a, b} == {sun_ra, autechre}


# -- Wikidata influence re-keying -------------------------------------------


def _get_influence_edges(db: PipelineDB) -> list[tuple[int, int]]:
    """Return all (source_id, target_id) pairs from wikidata_influence."""
    return db._conn.execute("SELECT source_id, target_id FROM wikidata_influence").fetchall()


class TestWikidataInfluenceRekey:
    """Verify that wikidata_influence edges are re-keyed during dedup."""

    def test_rekey_influence_source(self, db):
        """Influence edge with merge_id as source becomes keep_id."""
        ids = _set_up_alias_pair(db)
        sun_ra = ids["Sun Ra"]
        le_sonyr = ids["Le Sony'r Ra"]
        autechre = ids["Autechre"]

        db._conn.execute(
            "INSERT INTO wikidata_influence VALUES (?, ?, 'Q312545', 'Q2774')",
            (le_sonyr, autechre),
        )
        db._conn.commit()

        db.deduplicate_by_qid()

        edges = _get_influence_edges(db)
        assert len(edges) == 1
        assert edges[0] == (sun_ra, autechre)

    def test_rekey_influence_target(self, db):
        """Influence edge with merge_id as target becomes keep_id."""
        ids = _set_up_alias_pair(db)
        sun_ra = ids["Sun Ra"]
        le_sonyr = ids["Le Sony'r Ra"]
        autechre = ids["Autechre"]

        db._conn.execute(
            "INSERT INTO wikidata_influence VALUES (?, ?, 'Q2774', 'Q312545')",
            (autechre, le_sonyr),
        )
        db._conn.commit()

        db.deduplicate_by_qid()

        edges = _get_influence_edges(db)
        assert len(edges) == 1
        assert edges[0] == (autechre, sun_ra)

    def test_influence_self_loop_deleted(self, db):
        """Influence edge between alias artists is removed."""
        ids = _set_up_alias_pair(db)
        sun_ra = ids["Sun Ra"]
        le_sonyr = ids["Le Sony'r Ra"]

        db._conn.execute(
            "INSERT INTO wikidata_influence VALUES (?, ?, 'Q312545', 'Q312545')",
            (sun_ra, le_sonyr),
        )
        db._conn.commit()

        db.deduplicate_by_qid()

        edges = _get_influence_edges(db)
        assert len(edges) == 0

    def test_influence_pk_conflict(self, db):
        """When both aliases have influence to same artist, only one survives."""
        ids = _set_up_alias_pair(db)
        sun_ra = ids["Sun Ra"]
        le_sonyr = ids["Le Sony'r Ra"]
        autechre = ids["Autechre"]

        db._conn.execute(
            "INSERT INTO wikidata_influence VALUES (?, ?, 'Q312545', 'Q2774')",
            (sun_ra, autechre),
        )
        db._conn.execute(
            "INSERT INTO wikidata_influence VALUES (?, ?, 'Q312545', 'Q2774')",
            (le_sonyr, autechre),
        )
        db._conn.commit()

        db.deduplicate_by_qid()

        edges = _get_influence_edges(db)
        assert len(edges) == 1
        assert edges[0] == (sun_ra, autechre)


# -- Audio profile re-keying ------------------------------------------------


class TestAudioProfileRekey:
    """Verify that audio_profile rows are re-keyed during dedup."""

    def test_rekey_audio_profile(self, db):
        """Audio profile for merge_id is reassigned to keep_id."""
        ids = _set_up_alias_pair(db)
        sun_ra = ids["Sun Ra"]
        le_sonyr = ids["Le Sony'r Ra"]

        db._conn.execute(
            "INSERT INTO audio_profile (artist_id, avg_danceability, recording_count) "
            "VALUES (?, 0.42, 15)",
            (le_sonyr,),
        )
        db._conn.commit()

        db.deduplicate_by_qid()

        rows = db._conn.execute("SELECT artist_id FROM audio_profile").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == sun_ra

    def test_audio_profile_pk_conflict_keeps_survivor(self, db):
        """When both aliases have audio profiles, keep_id's profile survives."""
        ids = _set_up_alias_pair(db)
        sun_ra = ids["Sun Ra"]
        le_sonyr = ids["Le Sony'r Ra"]

        db._conn.execute(
            "INSERT INTO audio_profile (artist_id, avg_danceability, recording_count) "
            "VALUES (?, 0.42, 15)",
            (sun_ra,),
        )
        db._conn.execute(
            "INSERT INTO audio_profile (artist_id, avg_danceability, recording_count) "
            "VALUES (?, 0.38, 5)",
            (le_sonyr,),
        )
        db._conn.commit()

        db.deduplicate_by_qid()

        rows = db._conn.execute("SELECT artist_id, recording_count FROM audio_profile").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == sun_ra
        # keep_id's profile (15 recordings) survives, merge_id's is deleted
        assert rows[0][1] == 15


# -- Artist style re-keying -------------------------------------------------


class TestArtistStyleRekey:
    """Verify that artist_style rows are re-keyed during dedup."""

    def test_rekey_artist_style(self, db):
        """Style tags for merge_id are reassigned to keep_id."""
        ids = _set_up_alias_pair(db)
        sun_ra = ids["Sun Ra"]
        le_sonyr = ids["Le Sony'r Ra"]

        db._conn.execute("INSERT INTO artist_style VALUES (?, 'Free Jazz')", (le_sonyr,))
        db._conn.commit()

        db.deduplicate_by_qid()

        rows = db._conn.execute("SELECT artist_id, style_tag FROM artist_style").fetchall()
        assert len(rows) == 1
        assert rows[0] == (sun_ra, "Free Jazz")

    def test_artist_style_pk_conflict(self, db):
        """When both aliases have the same style tag, only one row survives."""
        ids = _set_up_alias_pair(db)
        sun_ra = ids["Sun Ra"]
        le_sonyr = ids["Le Sony'r Ra"]

        db._conn.execute("INSERT INTO artist_style VALUES (?, 'Free Jazz')", (sun_ra,))
        db._conn.execute("INSERT INTO artist_style VALUES (?, 'Free Jazz')", (le_sonyr,))
        db._conn.commit()

        db.deduplicate_by_qid()

        rows = db._conn.execute("SELECT artist_id, style_tag FROM artist_style").fetchall()
        assert len(rows) == 1
        assert rows[0] == (sun_ra, "Free Jazz")


# -- Artist personnel re-keying ---------------------------------------------


class TestArtistPersonnelRekey:
    """Verify that artist_personnel rows are re-keyed during dedup."""

    def test_rekey_artist_personnel(self, db):
        """Personnel rows for merge_id are reassigned to keep_id."""
        ids = _set_up_alias_pair(db)
        sun_ra = ids["Sun Ra"]
        le_sonyr = ids["Le Sony'r Ra"]

        db._conn.execute(
            "INSERT INTO artist_personnel VALUES (?, 'Marshall Allen', 'alto sax')",
            (le_sonyr,),
        )
        db._conn.commit()

        db.deduplicate_by_qid()

        rows = db._conn.execute("SELECT artist_id, personnel_name FROM artist_personnel").fetchall()
        assert len(rows) == 1
        assert rows[0] == (sun_ra, "Marshall Allen")


# -- Artist label re-keying --------------------------------------------------


class TestArtistLabelRekey:
    """Verify that artist_label rows are re-keyed during dedup."""

    def test_rekey_artist_label(self, db):
        """Label rows for merge_id are reassigned to keep_id."""
        ids = _set_up_alias_pair(db)
        sun_ra = ids["Sun Ra"]
        le_sonyr = ids["Le Sony'r Ra"]

        db._conn.execute(
            "INSERT INTO artist_label VALUES (?, 'Saturn Records', NULL)",
            (le_sonyr,),
        )
        db._conn.commit()

        db.deduplicate_by_qid()

        rows = db._conn.execute("SELECT artist_id, label_name FROM artist_label").fetchall()
        assert len(rows) == 1
        assert rows[0] == (sun_ra, "Saturn Records")

    def test_artist_label_pk_conflict(self, db):
        """When both aliases have the same label, only one row survives."""
        ids = _set_up_alias_pair(db)
        sun_ra = ids["Sun Ra"]
        le_sonyr = ids["Le Sony'r Ra"]

        db._conn.execute(
            "INSERT INTO artist_label VALUES (?, 'Saturn Records', 100)",
            (sun_ra,),
        )
        db._conn.execute(
            "INSERT INTO artist_label VALUES (?, 'Saturn Records', 100)",
            (le_sonyr,),
        )
        db._conn.commit()

        db.deduplicate_by_qid()

        rows = db._conn.execute("SELECT artist_id, label_name FROM artist_label").fetchall()
        assert len(rows) == 1
        assert rows[0] == (sun_ra, "Saturn Records")
