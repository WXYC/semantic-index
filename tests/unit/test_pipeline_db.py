"""Tests for PipelineDB entity deduplication with Discogs edge re-keying.

Verifies that deduplicate_by_qid() re-keys the 4 Discogs edge tables
(shared_personnel, shared_style, label_family, compilation) when merging
entities that share a Wikidata QID, handling self-referential edges and
PK conflicts.
"""

import json

import pytest

from semantic_index.pipeline_db import PipelineDB
from semantic_index.sqlite_export import (
    _EDGE_ENRICHMENT_SCHEMA,
)

# -- Fixtures ----------------------------------------------------------------


@pytest.fixture
def db(tmp_path):
    """PipelineDB with Discogs edge tables initialized."""
    pdb = PipelineDB(str(tmp_path / "test.db"))
    pdb.initialize()
    pdb._conn.executescript(_EDGE_ENRICHMENT_SCHEMA)
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
    """Verify that all 4 Discogs edge tables are re-keyed."""

    def test_all_four_tables_rekeyed(self, db):
        """Each of shared_personnel, shared_style, label_family, compilation is updated."""
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
        db._conn.commit()

        db.deduplicate_by_qid()

        for table in ("shared_personnel", "shared_style", "label_family", "compilation"):
            edges = _get_edges(db, table)
            assert len(edges) == 1, f"Expected 1 edge in {table}, got {len(edges)}"
            assert edges[0] == (sun_ra, autechre), f"Edge not re-keyed in {table}"


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
