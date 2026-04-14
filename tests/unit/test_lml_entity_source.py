"""Tests for --entity-source=lml flag and LML identity reading."""

import sqlite3
from unittest.mock import MagicMock

import pytest

from run_pipeline import parse_args
from semantic_index.entity_store import EntityStore
from semantic_index.lml_identity import import_lml_identities


class TestParseEntitySourceFlag:
    def test_entity_source_default_is_local(self):
        args = parse_args(["dump.sql"])
        assert args.entity_source == "local"

    def test_entity_source_local_explicit(self):
        args = parse_args(["dump.sql", "--entity-source", "local"])
        assert args.entity_source == "local"

    def test_entity_source_lml(self):
        args = parse_args(["dump.sql", "--entity-source", "lml"])
        assert args.entity_source == "lml"

    def test_entity_source_invalid_rejected(self):
        with pytest.raises(SystemExit):
            parse_args(["dump.sql", "--entity-source", "invalid"])

    def test_entity_source_lml_with_entity_store_path(self):
        args = parse_args(
            ["dump.sql", "--entity-source", "lml", "--entity-store-path", "/tmp/store.db"]
        )
        assert args.entity_source == "lml"
        assert args.entity_store_path == "/tmp/store.db"


class TestImportLmlIdentities:
    """Tests for import_lml_identities() which reads from entity.identity PG table."""

    def _make_entity_store(self, tmp_path) -> EntityStore:
        """Create a temporary EntityStore with an artist table."""
        db_path = str(tmp_path / "test.db")
        store = EntityStore(db_path)
        store.initialize()
        return store

    def _make_pg_source(self, rows: list[dict]) -> MagicMock:
        """Create a mock PG source that returns the given rows from fetchall()."""
        mock_pg = MagicMock()
        mock_pg.fetchall = MagicMock(return_value=rows)
        return mock_pg

    def test_populates_discogs_artist_id(self, tmp_path):
        """Artists matched in entity.identity get their discogs_artist_id populated."""
        store = self._make_entity_store(tmp_path)
        store.bulk_upsert_artists(["Stereolab", "Autechre"])

        mock_pg = self._make_pg_source(
            [
                {
                    "library_name": "Stereolab",
                    "discogs_artist_id": 2154,
                    "wikidata_qid": "Q484464",
                    "musicbrainz_artist_id": "d4133898-91ea-48ea-8820-1b85825901fe",
                    "spotify_artist_id": "1p6GVMFhLhSrRE7qgy8aAS",
                    "apple_music_artist_id": "5765873",
                    "bandcamp_id": None,
                    "reconciliation_status": "reconciled",
                },
                {
                    "library_name": "Autechre",
                    "discogs_artist_id": 12,
                    "wikidata_qid": "Q210513",
                    "musicbrainz_artist_id": "410c9baf-5469-44f6-9852-826524b80c61",
                    "spotify_artist_id": "6WH1V41LwGDGmlPUhSZLHO",
                    "apple_music_artist_id": None,
                    "bandcamp_id": None,
                    "reconciliation_status": "reconciled",
                },
            ]
        )

        report = import_lml_identities(store, mock_pg)

        artist = store.get_artist_by_name("Stereolab")
        assert artist is not None
        assert artist["discogs_artist_id"] == 2154
        assert artist["reconciliation_status"] == "reconciled"

        artist2 = store.get_artist_by_name("Autechre")
        assert artist2 is not None
        assert artist2["discogs_artist_id"] == 12

        assert report.matched == 2
        assert report.unmatched == 0

    def test_unmatched_artists_counted(self, tmp_path):
        """Artists in the local store but not in entity.identity are counted as unmatched."""
        store = self._make_entity_store(tmp_path)
        store.bulk_upsert_artists(["Stereolab", "Unknown Artist"])

        mock_pg = self._make_pg_source(
            [
                {
                    "library_name": "Stereolab",
                    "discogs_artist_id": 2154,
                    "wikidata_qid": None,
                    "musicbrainz_artist_id": None,
                    "spotify_artist_id": None,
                    "apple_music_artist_id": None,
                    "bandcamp_id": None,
                    "reconciliation_status": "reconciled",
                },
            ]
        )

        report = import_lml_identities(store, mock_pg)
        assert report.matched == 1
        assert report.unmatched == 1

    def test_empty_entity_identity_table(self, tmp_path):
        """When entity.identity returns no rows, all local artists are unmatched."""
        store = self._make_entity_store(tmp_path)
        store.bulk_upsert_artists(["Stereolab", "Autechre"])

        mock_pg = self._make_pg_source([])

        report = import_lml_identities(store, mock_pg)
        assert report.matched == 0
        assert report.unmatched == 2

    def test_creates_entity_with_qid(self, tmp_path):
        """When a QID is present, an entity row is created and linked to the artist."""
        store = self._make_entity_store(tmp_path)
        store.bulk_upsert_artists(["Stereolab"])

        mock_pg = self._make_pg_source(
            [
                {
                    "library_name": "Stereolab",
                    "discogs_artist_id": 2154,
                    "wikidata_qid": "Q484464",
                    "musicbrainz_artist_id": None,
                    "spotify_artist_id": "1p6GVMFhLhSrRE7qgy8aAS",
                    "apple_music_artist_id": "5765873",
                    "bandcamp_id": None,
                    "reconciliation_status": "reconciled",
                },
            ]
        )

        import_lml_identities(store, mock_pg)

        artist = store.get_artist_by_name("Stereolab")
        assert artist is not None
        assert artist["entity_id"] is not None

        # Check the entity row has the QID and streaming IDs
        conn = store._conn
        conn.row_factory = sqlite3.Row
        entity = conn.execute(
            "SELECT * FROM entity WHERE id = ?", (artist["entity_id"],)
        ).fetchone()
        conn.row_factory = None
        assert entity is not None
        assert entity["wikidata_qid"] == "Q484464"
        assert entity["spotify_artist_id"] == "1p6GVMFhLhSrRE7qgy8aAS"
        assert entity["apple_music_artist_id"] == "5765873"

    def test_musicbrainz_id_populated(self, tmp_path):
        """MusicBrainz artist ID from entity.identity is written to the artist row."""
        store = self._make_entity_store(tmp_path)
        store.bulk_upsert_artists(["Autechre"])

        mock_pg = self._make_pg_source(
            [
                {
                    "library_name": "Autechre",
                    "discogs_artist_id": 12,
                    "wikidata_qid": None,
                    "musicbrainz_artist_id": "410c9baf-5469-44f6-9852-826524b80c61",
                    "spotify_artist_id": None,
                    "apple_music_artist_id": None,
                    "bandcamp_id": None,
                    "reconciliation_status": "reconciled",
                },
            ]
        )

        import_lml_identities(store, mock_pg)

        artist = store.get_artist_by_name("Autechre")
        assert artist is not None
        assert artist["musicbrainz_artist_id"] == "410c9baf-5469-44f6-9852-826524b80c61"

    def test_pg_connection_failure_raises(self, tmp_path):
        """When PG connection fails, import_lml_identities raises an exception."""
        store = self._make_entity_store(tmp_path)
        store.bulk_upsert_artists(["Stereolab"])

        mock_pg = MagicMock()
        mock_pg.fetchall = MagicMock(side_effect=Exception("Connection refused"))

        with pytest.raises(Exception, match="Connection refused"):
            import_lml_identities(store, mock_pg)
