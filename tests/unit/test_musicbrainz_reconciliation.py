"""Tests for MusicBrainz reconciliation via musicbrainz-cache."""

from unittest.mock import MagicMock

from semantic_index.musicbrainz_client import MusicBrainzClient


def _make_client(mock_conn: MagicMock) -> MusicBrainzClient:
    """Create a MusicBrainzClient with a mocked cache connection."""
    mock_conn.closed = False
    client = MusicBrainzClient.__new__(MusicBrainzClient)
    client._cache_dsn = "mock"
    client._cache_conn = mock_conn
    return client


class TestLookupByName:
    """Tests for artist name lookup."""

    def test_exact_match_returns_id(self):
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = (2914, "Autechre")

        client = _make_client(mock_conn)
        result = client.lookup_by_name("Autechre")

        assert result is not None
        assert result[0] == 2914
        assert result[1] == "Autechre"

    def test_no_match_returns_none(self):
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = None

        client = _make_client(mock_conn)
        result = client.lookup_by_name("zzz_nonexistent_12345")

        assert result is None

    def test_empty_name_returns_none(self):
        client = MusicBrainzClient(cache_dsn="mock")
        client._cache_conn = MagicMock()
        client._cache_conn.closed = False
        result = client.lookup_by_name("")

        assert result is None


class TestBatchLookup:
    """Tests for batch artist name lookup."""

    def test_batch_returns_matched(self):
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [
            ("autechre", 2914, "Autechre"),
            ("stereolab", 19808, "Stereolab"),
        ]

        client = _make_client(mock_conn)
        result = client.batch_lookup(["Autechre", "Stereolab", "Unknown Artist"])

        assert "autechre" in result
        assert result["autechre"] == (2914, "Autechre")
        assert "stereolab" in result
        assert "unknown artist" not in result

    def test_empty_list_returns_empty(self):
        mock_conn = MagicMock()
        client = _make_client(mock_conn)
        result = client.batch_lookup([])

        assert result == {}


class TestGetRecordingMbids:
    """Tests for recording MBID lookup via materialized view."""

    def test_returns_mbids_grouped_by_artist(self):
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [
            (2914, "a1b2c3d4-e5f6-7890-abcd-ef1234567890"),
            (2914, "a1b2c3d4-e5f6-7890-abcd-ef1234567891"),
            (19808, "b2c3d4e5-f6a7-8901-bcde-f12345678901"),
        ]

        client = _make_client(mock_conn)
        result = client.get_recording_mbids([2914, 19808])

        assert len(result) == 2
        assert len(result[2914]) == 2
        assert len(result[19808]) == 1
        assert "a1b2c3d4-e5f6-7890-abcd-ef1234567890" in result[2914]

    def test_empty_list_returns_empty(self):
        mock_conn = MagicMock()
        client = _make_client(mock_conn)
        result = client.get_recording_mbids([])

        assert result == {}

    def test_no_recordings_returns_empty_for_artist(self):
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []

        client = _make_client(mock_conn)
        result = client.get_recording_mbids([99999])

        assert result == {}
