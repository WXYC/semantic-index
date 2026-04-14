"""Tests for Wikidata influence edge extraction."""

from __future__ import annotations

import sqlite3
import tempfile

from semantic_index.models import WikidataInfluence
from semantic_index.pipeline_db import PipelineDB
from semantic_index.wikidata_influence import extract_wikidata_influences


def _make_pipeline_db() -> PipelineDB:
    """Create a temporary pipeline DB with tables initialized."""
    path = tempfile.mktemp(suffix=".db")
    db = PipelineDB(path)
    db.initialize()
    return db


def _create_entity(conn: sqlite3.Connection, name: str, qid: str) -> int:
    """Create an entity row and return its ID."""
    cur = conn.execute(
        "INSERT INTO entity (name, entity_type, wikidata_qid) VALUES (?, 'artist', ?)",
        (name, qid),
    )
    conn.commit()
    return cur.lastrowid  # type: ignore[return-value]


def _set_up_artists(db: PipelineDB) -> dict[str, int]:
    """Set up test artists with Wikidata QIDs and return name-to-id mapping.

    Creates:
      - Autechre (Q2774, discogs 2774)
      - Stereolab (Q650826, discogs 10272)
      - Cat Power (Q218981, discogs 12345)
      - Father John Misty (no entity/QID)
    """
    entity_ae = _create_entity(db._conn, "Autechre", "Q2774")
    entity_sl = _create_entity(db._conn, "Stereolab", "Q650826")
    entity_cp = _create_entity(db._conn, "Cat Power", "Q218981")

    db.upsert_artist("Autechre", discogs_artist_id=2774, entity_id=entity_ae)
    db.upsert_artist("Stereolab", discogs_artist_id=10272, entity_id=entity_sl)
    db.upsert_artist("Cat Power", discogs_artist_id=12345, entity_id=entity_cp)
    db.upsert_artist("Father John Misty")  # no entity

    return db.get_name_to_id_mapping()


class TestExtractWikidataInfluences:
    """Tests for the extract_wikidata_influences function."""

    def test_returns_edges_between_known_artists(self):
        """Influences where both source and target are in the graph produce edges."""
        db = _make_pipeline_db()
        _set_up_artists(db)

        influences = [
            WikidataInfluence(source_qid="Q2774", target_qid="Q650826", target_name="Stereolab"),
        ]

        edges = extract_wikidata_influences(db._conn, influences)

        assert len(edges) == 1
        assert edges[0].source_artist == "Autechre"
        assert edges[0].target_artist == "Stereolab"
        assert edges[0].source_qid == "Q2774"
        assert edges[0].target_qid == "Q650826"
        db.close()

    def test_skips_target_not_in_graph(self):
        """Influences pointing to artists not in the graph are skipped."""
        db = _make_pipeline_db()
        _set_up_artists(db)

        influences = [
            WikidataInfluence(source_qid="Q2774", target_qid="Q484641", target_name="Kraftwerk"),
        ]

        edges = extract_wikidata_influences(db._conn, influences)

        assert len(edges) == 0
        db.close()

    def test_skips_source_not_in_graph(self):
        """Influences from unknown sources are skipped."""
        db = _make_pipeline_db()
        _set_up_artists(db)

        influences = [
            WikidataInfluence(source_qid="Q999999", target_qid="Q2774", target_name="Autechre"),
        ]

        edges = extract_wikidata_influences(db._conn, influences)

        assert len(edges) == 0
        db.close()

    def test_multiple_influences(self):
        """Multiple valid influence edges are all returned."""
        db = _make_pipeline_db()
        _set_up_artists(db)

        influences = [
            WikidataInfluence(source_qid="Q2774", target_qid="Q650826", target_name="Stereolab"),
            WikidataInfluence(source_qid="Q2774", target_qid="Q218981", target_name="Cat Power"),
            WikidataInfluence(source_qid="Q650826", target_qid="Q218981", target_name="Cat Power"),
        ]

        edges = extract_wikidata_influences(db._conn, influences)

        assert len(edges) == 3
        pairs = [(e.source_artist, e.target_artist) for e in edges]
        assert ("Autechre", "Stereolab") in pairs
        assert ("Autechre", "Cat Power") in pairs
        assert ("Stereolab", "Cat Power") in pairs
        db.close()

    def test_empty_influences(self):
        """Empty influence list produces no edges."""
        db = _make_pipeline_db()
        _set_up_artists(db)

        edges = extract_wikidata_influences(db._conn, [])

        assert edges == []
        db.close()

    def test_self_influence_skipped(self):
        """An artist listed as influencing itself is skipped."""
        db = _make_pipeline_db()
        _set_up_artists(db)

        influences = [
            WikidataInfluence(source_qid="Q2774", target_qid="Q2774", target_name="Autechre"),
        ]

        edges = extract_wikidata_influences(db._conn, influences)

        assert len(edges) == 0
        db.close()

    def test_deduplicates_edges(self):
        """Duplicate influence entries produce only one edge."""
        db = _make_pipeline_db()
        _set_up_artists(db)

        influences = [
            WikidataInfluence(source_qid="Q2774", target_qid="Q650826", target_name="Stereolab"),
            WikidataInfluence(source_qid="Q2774", target_qid="Q650826", target_name="Stereolab"),
        ]

        edges = extract_wikidata_influences(db._conn, influences)

        assert len(edges) == 1
        db.close()

    def test_deterministic_sort_order(self):
        """Output edges are sorted by (source_artist, target_artist) for determinism."""
        db = _make_pipeline_db()
        _set_up_artists(db)

        influences = [
            WikidataInfluence(source_qid="Q650826", target_qid="Q218981", target_name="Cat Power"),
            WikidataInfluence(source_qid="Q2774", target_qid="Q650826", target_name="Stereolab"),
            WikidataInfluence(source_qid="Q2774", target_qid="Q218981", target_name="Cat Power"),
        ]

        edges = extract_wikidata_influences(db._conn, influences)

        names = [(e.source_artist, e.target_artist) for e in edges]
        assert names == sorted(names)
        db.close()
