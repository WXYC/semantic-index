"""Tests for LML identity import into the pipeline database."""

import sqlite3
from unittest.mock import MagicMock

import pytest

from semantic_index.lml_identity import import_lml_identities
from semantic_index.pipeline_db import PipelineDB


def _get_artist_by_name(conn: sqlite3.Connection, name: str) -> dict | None:
    """Look up an artist row by canonical name."""
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM artist WHERE canonical_name = ?", (name,)).fetchone()
    conn.row_factory = None
    if row is None:
        return None
    return dict(row)


class TestImportLmlIdentities:
    """Tests for import_lml_identities() which reads from entity.identity PG table."""

    def _make_pipeline_db(self, tmp_path) -> PipelineDB:
        """Create a temporary PipelineDB with tables initialized."""
        db_path = str(tmp_path / "test.db")
        db = PipelineDB(db_path)
        db.initialize()
        return db

    def _make_pg_source(self, rows: list[dict]) -> MagicMock:
        """Create a mock PG source that returns the given rows from fetchall()."""
        mock_pg = MagicMock()
        mock_pg.fetchall = MagicMock(return_value=rows)
        return mock_pg

    def test_populates_discogs_artist_id(self, tmp_path):
        """Artists matched in entity.identity get their discogs_artist_id populated."""
        db = self._make_pipeline_db(tmp_path)
        db.bulk_upsert_artists(["Stereolab", "Autechre"])

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

        report = import_lml_identities(db, mock_pg)

        artist = _get_artist_by_name(db._conn, "Stereolab")
        assert artist is not None
        assert artist["discogs_artist_id"] == 2154
        assert artist["reconciliation_status"] == "reconciled"

        artist2 = _get_artist_by_name(db._conn, "Autechre")
        assert artist2 is not None
        assert artist2["discogs_artist_id"] == 12

        assert report.matched == 2
        assert report.unmatched == 0

    def test_unmatched_artists_counted(self, tmp_path):
        """Artists in the local store but not in entity.identity are counted as unmatched."""
        db = self._make_pipeline_db(tmp_path)
        db.bulk_upsert_artists(["Stereolab", "Unknown Artist"])

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

        report = import_lml_identities(db, mock_pg)
        assert report.matched == 1
        assert report.unmatched == 1

    def test_empty_entity_identity_table(self, tmp_path):
        """When entity.identity returns no rows, all local artists are unmatched."""
        db = self._make_pipeline_db(tmp_path)
        db.bulk_upsert_artists(["Stereolab", "Autechre"])

        mock_pg = self._make_pg_source([])

        report = import_lml_identities(db, mock_pg)
        assert report.matched == 0
        assert report.unmatched == 2

    def test_creates_entity_with_qid(self, tmp_path):
        """When a QID is present, an entity row is created and linked to the artist."""
        db = self._make_pipeline_db(tmp_path)
        db.bulk_upsert_artists(["Stereolab"])

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

        import_lml_identities(db, mock_pg)

        artist = _get_artist_by_name(db._conn, "Stereolab")
        assert artist is not None
        assert artist["entity_id"] is not None

        db._conn.row_factory = sqlite3.Row
        entity = db._conn.execute(
            "SELECT * FROM entity WHERE id = ?", (artist["entity_id"],)
        ).fetchone()
        db._conn.row_factory = None
        assert entity is not None
        assert entity["wikidata_qid"] == "Q484464"
        assert entity["spotify_artist_id"] == "1p6GVMFhLhSrRE7qgy8aAS"
        assert entity["apple_music_artist_id"] == "5765873"

    def test_musicbrainz_id_populated(self, tmp_path):
        """MusicBrainz artist ID from entity.identity is written to the artist row."""
        db = self._make_pipeline_db(tmp_path)
        db.bulk_upsert_artists(["Autechre"])

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

        import_lml_identities(db, mock_pg)

        artist = _get_artist_by_name(db._conn, "Autechre")
        assert artist is not None
        assert artist["musicbrainz_artist_id"] == "410c9baf-5469-44f6-9852-826524b80c61"

    def test_pg_connection_failure_raises(self, tmp_path):
        """When PG connection fails, import_lml_identities raises an exception."""
        db = self._make_pipeline_db(tmp_path)
        db.bulk_upsert_artists(["Stereolab"])

        mock_pg = MagicMock()
        mock_pg.fetchall = MagicMock(side_effect=Exception("Connection refused"))

        with pytest.raises(Exception, match="Connection refused"):
            import_lml_identities(db, mock_pg)
