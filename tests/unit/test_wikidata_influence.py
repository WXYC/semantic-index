"""Tests for Wikidata influence edge extraction."""

from __future__ import annotations

import tempfile

from semantic_index.entity_store import EntityStore
from semantic_index.models import WikidataInfluence
from semantic_index.wikidata_influence import extract_wikidata_influences


def _make_entity_store() -> EntityStore:
    """Create an in-memory-style entity store with reconciled artists."""
    path = tempfile.mktemp(suffix=".db")
    store = EntityStore(path)
    store.initialize()
    return store


def _set_up_artists(store: EntityStore) -> dict[str, int]:
    """Set up test artists with Wikidata QIDs and return name-to-id mapping.

    Creates:
      - Autechre (Q2774, discogs 2774)
      - Stereolab (Q650826, discogs 10272)
      - Cat Power (Q218981, discogs 12345)
      - Father John Misty (no entity/QID)
    """
    # Create entities with QIDs
    entity_ae = store.get_or_create_entity("Autechre", "artist", wikidata_qid="Q2774")
    entity_sl = store.get_or_create_entity("Stereolab", "artist", wikidata_qid="Q650826")
    entity_cp = store.get_or_create_entity("Cat Power", "artist", wikidata_qid="Q218981")

    store.upsert_artist("Autechre", discogs_artist_id=2774, entity_id=entity_ae.id)
    store.upsert_artist("Stereolab", discogs_artist_id=10272, entity_id=entity_sl.id)
    store.upsert_artist("Cat Power", discogs_artist_id=12345, entity_id=entity_cp.id)
    store.upsert_artist("Father John Misty")  # no entity

    return store.get_name_to_id_mapping()


class TestExtractWikidataInfluences:
    """Tests for the extract_wikidata_influences function."""

    def test_returns_edges_between_known_artists(self):
        """Influences where both source and target are in the graph produce edges."""
        store = _make_entity_store()
        _set_up_artists(store)

        # Autechre influenced by Stereolab
        influences = [
            WikidataInfluence(source_qid="Q2774", target_qid="Q650826", target_name="Stereolab"),
        ]

        edges = extract_wikidata_influences(store, influences)

        assert len(edges) == 1
        assert edges[0].source_artist == "Autechre"
        assert edges[0].target_artist == "Stereolab"
        assert edges[0].source_qid == "Q2774"
        assert edges[0].target_qid == "Q650826"
        store.close()

    def test_skips_target_not_in_graph(self):
        """Influences pointing to artists not in the entity store are skipped."""
        store = _make_entity_store()
        _set_up_artists(store)

        # Autechre influenced by Kraftwerk (Q484641, not in our graph)
        influences = [
            WikidataInfluence(source_qid="Q2774", target_qid="Q484641", target_name="Kraftwerk"),
        ]

        edges = extract_wikidata_influences(store, influences)

        assert len(edges) == 0
        store.close()

    def test_skips_source_not_in_graph(self):
        """Influences from unknown sources are skipped."""
        store = _make_entity_store()
        _set_up_artists(store)

        # Unknown source influenced by Autechre
        influences = [
            WikidataInfluence(source_qid="Q999999", target_qid="Q2774", target_name="Autechre"),
        ]

        edges = extract_wikidata_influences(store, influences)

        assert len(edges) == 0
        store.close()

    def test_multiple_influences(self):
        """Multiple valid influence edges are all returned."""
        store = _make_entity_store()
        _set_up_artists(store)

        influences = [
            WikidataInfluence(source_qid="Q2774", target_qid="Q650826", target_name="Stereolab"),
            WikidataInfluence(source_qid="Q2774", target_qid="Q218981", target_name="Cat Power"),
            WikidataInfluence(source_qid="Q650826", target_qid="Q218981", target_name="Cat Power"),
        ]

        edges = extract_wikidata_influences(store, influences)

        assert len(edges) == 3
        pairs = [(e.source_artist, e.target_artist) for e in edges]
        assert ("Autechre", "Stereolab") in pairs
        assert ("Autechre", "Cat Power") in pairs
        assert ("Stereolab", "Cat Power") in pairs
        store.close()

    def test_empty_influences(self):
        """Empty influence list produces no edges."""
        store = _make_entity_store()
        _set_up_artists(store)

        edges = extract_wikidata_influences(store, [])

        assert edges == []
        store.close()

    def test_self_influence_skipped(self):
        """An artist listed as influencing itself is skipped."""
        store = _make_entity_store()
        _set_up_artists(store)

        influences = [
            WikidataInfluence(source_qid="Q2774", target_qid="Q2774", target_name="Autechre"),
        ]

        edges = extract_wikidata_influences(store, influences)

        assert len(edges) == 0
        store.close()

    def test_deduplicates_edges(self):
        """Duplicate influence entries produce only one edge."""
        store = _make_entity_store()
        _set_up_artists(store)

        influences = [
            WikidataInfluence(source_qid="Q2774", target_qid="Q650826", target_name="Stereolab"),
            WikidataInfluence(source_qid="Q2774", target_qid="Q650826", target_name="Stereolab"),
        ]

        edges = extract_wikidata_influences(store, influences)

        assert len(edges) == 1
        store.close()

    def test_deterministic_sort_order(self):
        """Output edges are sorted by (source_artist, target_artist) for determinism."""
        store = _make_entity_store()
        _set_up_artists(store)

        influences = [
            WikidataInfluence(source_qid="Q650826", target_qid="Q218981", target_name="Cat Power"),
            WikidataInfluence(source_qid="Q2774", target_qid="Q650826", target_name="Stereolab"),
            WikidataInfluence(source_qid="Q2774", target_qid="Q218981", target_name="Cat Power"),
        ]

        edges = extract_wikidata_influences(store, influences)

        names = [(e.source_artist, e.target_artist) for e in edges]
        assert names == sorted(names)
        store.close()
