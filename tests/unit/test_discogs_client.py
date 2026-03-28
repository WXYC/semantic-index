"""Tests for the two-tier Discogs client."""

from unittest.mock import MagicMock, patch

from semantic_index.discogs_client import DiscogsClient


class TestCacheQueries:
    """Tests for discogs-cache PostgreSQL queries."""

    def test_search_artist_from_cache(self):
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [
            (42, "Autechre"),
        ]
        with patch("psycopg.connect", return_value=mock_conn):
            client = DiscogsClient(cache_dsn="postgresql://test", api_base_url=None)
            result = client.search_artist("Autechre")

        assert result is not None
        assert result.artist_name == "Autechre"
        assert result.artist_id == 42

    def test_search_artist_cache_miss_returns_none(self):
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []
        with patch("psycopg.connect", return_value=mock_conn):
            client = DiscogsClient(cache_dsn="postgresql://test", api_base_url=None)
            result = client.search_artist("Nonexistent Artist")

        assert result is None

    def test_get_release_from_cache(self):
        mock_conn = MagicMock()
        # release row
        mock_conn.execute.return_value.fetchone.return_value = (12345, "Confield", 2001)
        # artist rows, label rows, track rows, track_artist rows
        mock_conn.execute.return_value.fetchall.side_effect = [
            [(42, "Autechre", 0, None)],  # release_artist
            [(100, "Warp Records", "WARPCD85")],  # release_label
            [("1", "VI Scose Poise", 1)],  # release_track
            [],  # release_track_artist
        ]
        with patch("psycopg.connect", return_value=mock_conn):
            client = DiscogsClient(cache_dsn="postgresql://test", api_base_url=None)
            release = client.get_release(12345)

        assert release is not None
        assert release.release_id == 12345
        assert release.title == "Confield"
        assert release.artist_name == "Autechre"
        assert len(release.labels) == 1
        assert release.labels[0].name == "Warp Records"

    def test_get_release_cache_miss(self):
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = None
        with patch("psycopg.connect", return_value=mock_conn):
            client = DiscogsClient(cache_dsn="postgresql://test", api_base_url=None)
            release = client.get_release(99999)

        assert release is None


class TestApiQueries:
    """Tests for library-metadata-lookup API fallback."""

    def test_search_artist_from_api(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [
                {
                    "artist": "Autechre",
                    "album": "Confield",
                    "release_id": 12345,
                    "release_url": "https://discogs.com/release/12345",
                    "confidence": 0.95,
                }
            ],
            "total": 1,
            "cached": False,
        }

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            client = DiscogsClient(cache_dsn=None, api_base_url="http://test")
            result = client.search_artist("Autechre")

        assert result is not None
        assert result.artist_name == "Autechre"
        assert result.release_id == 12345

    def test_search_artist_api_no_results(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": [], "total": 0, "cached": False}

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            client = DiscogsClient(cache_dsn=None, api_base_url="http://test")
            result = client.search_artist("Nonexistent")

        assert result is None

    def test_get_release_from_api(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "release_id": 12345,
            "title": "Confield",
            "artist": "Autechre",
            "artist_id": 42,
            "year": 2001,
            "genres": ["Electronic"],
            "styles": ["IDM", "Abstract"],
            "artists": [{"name": "Autechre", "artist_id": 42, "join": ""}],
            "extra_artists": [{"name": "Rob Brown", "role": "Written-By"}],
            "labels": [{"name": "Warp Records", "label_id": 100, "catno": "WARPCD85"}],
            "tracklist": [{"position": "1", "title": "VI Scose Poise", "artists": []}],
            "release_url": "https://discogs.com/release/12345",
            "cached": False,
        }

        mock_client = MagicMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            client = DiscogsClient(cache_dsn=None, api_base_url="http://test")
            release = client.get_release(12345)

        assert release is not None
        assert release.styles == ["IDM", "Abstract"]
        assert len(release.extra_artists) == 1
        assert release.extra_artists[0].name == "Rob Brown"
        assert release.extra_artists[0].role == "Written-By"


class TestFallbackBehavior:
    """Tests for two-tier fallback."""

    def test_cache_miss_falls_back_to_api(self):
        # Cache returns empty
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []

        # API returns a result
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [
                {
                    "artist": "Stereolab",
                    "release_id": 99999,
                    "release_url": "https://discogs.com/release/99999",
                    "confidence": 0.9,
                }
            ],
            "total": 1,
            "cached": False,
        }
        mock_http = MagicMock()
        mock_http.post.return_value = mock_response

        with (
            patch("psycopg.connect", return_value=mock_conn),
            patch("httpx.Client", return_value=mock_http),
        ):
            client = DiscogsClient(cache_dsn="postgresql://test", api_base_url="http://test")
            result = client.search_artist("Stereolab")

        assert result is not None
        assert result.artist_name == "Stereolab"

    def test_both_unavailable_returns_none(self):
        client = DiscogsClient(cache_dsn=None, api_base_url=None)
        result = client.search_artist("Anything")
        assert result is None

    def test_both_unavailable_get_release_returns_none(self):
        client = DiscogsClient(cache_dsn=None, api_base_url=None)
        result = client.get_release(12345)
        assert result is None


class TestGracefulDegradation:
    """Tests for error handling."""

    def test_cache_connection_error_falls_to_api(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [
                {
                    "artist": "Cat Power",
                    "release_id": 55555,
                    "release_url": "https://discogs.com/release/55555",
                    "confidence": 0.85,
                }
            ],
            "total": 1,
            "cached": False,
        }
        mock_http = MagicMock()
        mock_http.post.return_value = mock_response

        with (
            patch("psycopg.connect", side_effect=Exception("Connection refused")),
            patch("httpx.Client", return_value=mock_http),
        ):
            client = DiscogsClient(cache_dsn="postgresql://bad", api_base_url="http://test")
            result = client.search_artist("Cat Power")

        assert result is not None
        assert result.artist_name == "Cat Power"

    def test_api_error_returns_none(self):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = Exception("Server error")

        mock_http = MagicMock()
        mock_http.post.return_value = mock_response

        with patch("httpx.Client", return_value=mock_http):
            client = DiscogsClient(cache_dsn=None, api_base_url="http://test")
            result = client.search_artist("Autechre")

        assert result is None
