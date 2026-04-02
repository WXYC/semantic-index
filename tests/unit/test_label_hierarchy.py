"""Tests for the label hierarchy population module."""

import sqlite3
from unittest.mock import MagicMock

import pytest

from semantic_index.entity_store import EntityStore
from semantic_index.label_hierarchy import populate_label_hierarchy
from semantic_index.models import (
    ArtistEnrichment,
    LabelInfo,
    WikidataEntity,
    WikidataLabelHierarchy,
)

# Old artist schema for EntityStore migration
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
    db_path = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_path)
    conn.executescript(_OLD_ARTIST_SCHEMA)
    conn.close()
    s = EntityStore(db_path)
    s.initialize()
    return s


def _make_enrichments(
    labels: dict[str, list[tuple[str, int | None]]],
) -> dict[str, ArtistEnrichment]:
    """Build enrichments dict. labels maps artist_name -> [(label_name, discogs_label_id), ...]."""
    result = {}
    for artist_name, label_list in labels.items():
        result[artist_name] = ArtistEnrichment(
            canonical_name=artist_name,
            labels=[LabelInfo(name=n, label_id=lid) for n, lid in label_list],
        )
    return result


class TestPopulateLabelHierarchy:
    def test_creates_labels_from_enrichments(self, store: EntityStore):
        enrichments = _make_enrichments(
            {
                "Autechre": [("Warp Records", 23528)],
                "Father John Misty": [("Sub Pop", 1594)],
            }
        )
        wikidata_client = MagicMock()
        wikidata_client.lookup_labels_by_discogs_ids.return_value = {}
        wikidata_client.get_label_hierarchy.return_value = []

        populate_label_hierarchy(store, enrichments, wikidata_client)

        warp = store.get_label_by_name("Warp Records")
        assert warp is not None
        assert warp["discogs_label_id"] == 23528

        subpop = store.get_label_by_name("Sub Pop")
        assert subpop is not None
        assert subpop["discogs_label_id"] == 1594

    def test_links_wikidata_qids_to_labels(self, store: EntityStore):
        enrichments = _make_enrichments({"Autechre": [("Warp Records", 23528)]})
        wikidata_client = MagicMock()
        wikidata_client.lookup_labels_by_discogs_ids.return_value = {
            23528: WikidataEntity(qid="Q1312934", name="Warp Records"),
        }
        wikidata_client.get_label_hierarchy.return_value = []

        populate_label_hierarchy(store, enrichments, wikidata_client)

        warp = store.get_label_by_name("Warp Records")
        assert warp is not None
        assert warp["entity_id"] is not None
        entity = store.get_entity_by_qid("Q1312934")
        assert entity is not None
        assert entity.name == "Warp Records"

    def test_populates_hierarchy(self, store: EntityStore):
        enrichments = _make_enrichments(
            {
                "Autechre": [("Warp Records", 23528)],
                "Father John Misty": [("Sub Pop", 1594)],
            }
        )
        wikidata_client = MagicMock()
        wikidata_client.lookup_labels_by_discogs_ids.return_value = {
            23528: WikidataEntity(qid="Q1312934", name="Warp Records"),
            1594: WikidataEntity(qid="Q843988", name="Sub Pop"),
        }
        wikidata_client.get_label_hierarchy.return_value = [
            WikidataLabelHierarchy(
                parent_qid="Q21077",
                parent_name="Universal Music Group",
                child_qid="Q1312934",
                child_name="Warp Records",
            ),
            WikidataLabelHierarchy(
                parent_qid="Q21077",
                parent_name="Universal Music Group",
                child_qid="Q843988",
                child_name="Sub Pop",
            ),
        ]

        populate_label_hierarchy(store, enrichments, wikidata_client)

        # Parent label should be auto-created
        umg = store.get_label_by_name("Universal Music Group")
        assert umg is not None

        # Hierarchy rows should exist
        rows = store._conn.execute("SELECT COUNT(*) FROM label_hierarchy").fetchone()
        assert rows[0] == 2

    def test_skips_labels_without_discogs_id(self, store: EntityStore):
        enrichments = _make_enrichments({"Autechre": [("Self-Released", None)]})
        wikidata_client = MagicMock()
        wikidata_client.lookup_labels_by_discogs_ids.return_value = {}
        wikidata_client.get_label_hierarchy.return_value = []

        populate_label_hierarchy(store, enrichments, wikidata_client)

        # Label should still be created but no Discogs lookup attempted
        label = store.get_label_by_name("Self-Released")
        assert label is not None
        assert label["discogs_label_id"] is None
        # Should have called with empty list (no Discogs IDs to look up)
        wikidata_client.lookup_labels_by_discogs_ids.assert_called_once_with([])

    def test_empty_enrichments(self, store: EntityStore):
        wikidata_client = MagicMock()
        populate_label_hierarchy(store, {}, wikidata_client)
        wikidata_client.lookup_labels_by_discogs_ids.assert_not_called()
        wikidata_client.get_label_hierarchy.assert_not_called()

    def test_deduplicates_labels_across_artists(self, store: EntityStore):
        enrichments = _make_enrichments(
            {
                "Autechre": [("Warp Records", 23528)],
                "Boards of Canada": [("Warp Records", 23528)],
            }
        )
        wikidata_client = MagicMock()
        wikidata_client.lookup_labels_by_discogs_ids.return_value = {}
        wikidata_client.get_label_hierarchy.return_value = []

        populate_label_hierarchy(store, enrichments, wikidata_client)

        count = store._conn.execute(
            "SELECT COUNT(*) FROM label WHERE name = 'Warp Records'"
        ).fetchone()[0]
        assert count == 1

    def test_hierarchy_parent_from_outside_enrichments(self, store: EntityStore):
        """Parent labels not in enrichments should still be created."""
        enrichments = _make_enrichments({"Autechre": [("Warp Records", 23528)]})
        wikidata_client = MagicMock()
        wikidata_client.lookup_labels_by_discogs_ids.return_value = {
            23528: WikidataEntity(qid="Q1312934", name="Warp Records"),
        }
        wikidata_client.get_label_hierarchy.return_value = [
            WikidataLabelHierarchy(
                parent_qid="Q21077",
                parent_name="Universal Music Group",
                child_qid="Q1312934",
                child_name="Warp Records",
            ),
        ]

        populate_label_hierarchy(store, enrichments, wikidata_client)

        umg = store.get_label_by_name("Universal Music Group")
        assert umg is not None
        entity = store.get_entity_by_qid("Q21077")
        assert entity is not None
        assert entity.entity_type == "label"

    def test_returns_report(self, store: EntityStore):
        enrichments = _make_enrichments(
            {
                "Autechre": [("Warp Records", 23528)],
                "Father John Misty": [("Sub Pop", 1594)],
                "Jessica Pratt": [("Drag City", 1218)],
            }
        )
        wikidata_client = MagicMock()
        wikidata_client.lookup_labels_by_discogs_ids.return_value = {
            23528: WikidataEntity(qid="Q1312934", name="Warp Records"),
            1594: WikidataEntity(qid="Q843988", name="Sub Pop"),
        }
        wikidata_client.get_label_hierarchy.return_value = [
            WikidataLabelHierarchy(
                parent_qid="Q21077",
                parent_name="Universal Music Group",
                child_qid="Q1312934",
                child_name="Warp Records",
            ),
        ]

        report = populate_label_hierarchy(store, enrichments, wikidata_client)

        assert report.labels_created == 3
        assert report.labels_matched == 2  # Warp + Sub Pop got QIDs
        assert report.hierarchy_edges == 1
