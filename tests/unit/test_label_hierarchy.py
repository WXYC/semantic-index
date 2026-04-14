"""Tests for the label hierarchy population module."""

import sqlite3
from unittest.mock import MagicMock

import pytest

from semantic_index.label_hierarchy import populate_label_hierarchy
from semantic_index.label_store import LabelStore
from semantic_index.models import (
    ArtistEnrichment,
    LabelInfo,
    WikidataEntity,
    WikidataLabelHierarchy,
)
from semantic_index.pipeline_db import PipelineDB

# Old artist schema for PipelineDB migration
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
def label_store(tmp_path) -> LabelStore:
    db_path = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_path)
    conn.executescript(_OLD_ARTIST_SCHEMA)
    conn.close()
    db = PipelineDB(db_path)
    db.initialize()
    return LabelStore(db._conn)


def _get_label_by_name(conn: sqlite3.Connection, name: str) -> dict | None:
    """Look up a label row by name."""
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM label WHERE name = ?", (name,)).fetchone()
    conn.row_factory = None
    if row is None:
        return None
    return dict(row)


def _get_entity_by_qid(conn: sqlite3.Connection, qid: str) -> dict | None:
    """Look up an entity by Wikidata QID."""
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM entity WHERE wikidata_qid = ?", (qid,)).fetchone()
    conn.row_factory = None
    if row is None:
        return None
    return dict(row)


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
    def test_creates_labels_from_enrichments(self, label_store: LabelStore):
        enrichments = _make_enrichments(
            {
                "Autechre": [("Warp Records", 23528)],
                "Father John Misty": [("Sub Pop", 1594)],
            }
        )
        wikidata_client = MagicMock()
        wikidata_client.lookup_labels_by_discogs_ids.return_value = {}
        wikidata_client.get_label_hierarchy.return_value = []

        populate_label_hierarchy(label_store, enrichments, wikidata_client)

        warp = _get_label_by_name(label_store._conn, "Warp Records")
        assert warp is not None
        assert warp["discogs_label_id"] == 23528

        subpop = _get_label_by_name(label_store._conn, "Sub Pop")
        assert subpop is not None
        assert subpop["discogs_label_id"] == 1594

    def test_links_wikidata_qids_to_labels(self, label_store: LabelStore):
        enrichments = _make_enrichments({"Autechre": [("Warp Records", 23528)]})
        wikidata_client = MagicMock()
        wikidata_client.lookup_labels_by_discogs_ids.return_value = {
            23528: WikidataEntity(qid="Q1312934", name="Warp Records"),
        }
        wikidata_client.get_label_hierarchy.return_value = []

        populate_label_hierarchy(label_store, enrichments, wikidata_client)

        warp = _get_label_by_name(label_store._conn, "Warp Records")
        assert warp is not None
        assert warp["entity_id"] is not None
        entity = _get_entity_by_qid(label_store._conn, "Q1312934")
        assert entity is not None
        assert entity["name"] == "Warp Records"

    def test_populates_hierarchy(self, label_store: LabelStore):
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

        populate_label_hierarchy(label_store, enrichments, wikidata_client)

        # Parent label should be auto-created
        umg = _get_label_by_name(label_store._conn, "Universal Music Group")
        assert umg is not None

        # Hierarchy rows should exist
        rows = label_store._conn.execute("SELECT COUNT(*) FROM label_hierarchy").fetchone()
        assert rows[0] == 2

    def test_skips_labels_without_discogs_id(self, label_store: LabelStore):
        enrichments = _make_enrichments({"Autechre": [("Self-Released", None)]})
        wikidata_client = MagicMock()
        wikidata_client.lookup_labels_by_discogs_ids.return_value = {}
        wikidata_client.get_label_hierarchy.return_value = []

        populate_label_hierarchy(label_store, enrichments, wikidata_client)

        label = _get_label_by_name(label_store._conn, "Self-Released")
        assert label is not None
        assert label["discogs_label_id"] is None
        wikidata_client.lookup_labels_by_discogs_ids.assert_called_once_with([])

    def test_empty_enrichments(self, label_store: LabelStore):
        wikidata_client = MagicMock()
        populate_label_hierarchy(label_store, {}, wikidata_client)
        wikidata_client.lookup_labels_by_discogs_ids.assert_not_called()
        wikidata_client.get_label_hierarchy.assert_not_called()

    def test_deduplicates_labels_across_artists(self, label_store: LabelStore):
        enrichments = _make_enrichments(
            {
                "Autechre": [("Warp Records", 23528)],
                "Boards of Canada": [("Warp Records", 23528)],
            }
        )
        wikidata_client = MagicMock()
        wikidata_client.lookup_labels_by_discogs_ids.return_value = {}
        wikidata_client.get_label_hierarchy.return_value = []

        populate_label_hierarchy(label_store, enrichments, wikidata_client)

        count = label_store._conn.execute(
            "SELECT COUNT(*) FROM label WHERE name = 'Warp Records'"
        ).fetchone()[0]
        assert count == 1

    def test_hierarchy_parent_from_outside_enrichments(self, label_store: LabelStore):
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

        populate_label_hierarchy(label_store, enrichments, wikidata_client)

        umg = _get_label_by_name(label_store._conn, "Universal Music Group")
        assert umg is not None
        entity = _get_entity_by_qid(label_store._conn, "Q21077")
        assert entity is not None
        assert entity["entity_type"] == "label"

    def test_returns_report(self, label_store: LabelStore):
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

        report = populate_label_hierarchy(label_store, enrichments, wikidata_client)

        assert report.labels_created == 3
        assert report.labels_matched == 2
        assert report.hierarchy_edges == 1
