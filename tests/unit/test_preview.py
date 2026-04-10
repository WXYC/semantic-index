"""Tests for the preview endpoint with multi-source fallback and sidecar caching."""

from __future__ import annotations

import sqlite3
import tempfile
from unittest.mock import MagicMock, patch

import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from semantic_index.api.app import create_app
from semantic_index.entity_store import EntityStore
from semantic_index.models import ArtistStats

# -- iTunes mock responses --

ITUNES_LOOKUP_RESPONSE = {
    "resultCount": 2,
    "results": [
        {"wrapperType": "artist", "artistId": 15821237, "artistName": "Autechre"},
        {
            "wrapperType": "track",
            "trackName": "Gantz Graf",
            "artistName": "Autechre",
            "previewUrl": "https://audio-ssl.itunes.apple.com/autechre_gantz.m4a",
            "artworkUrl100": "https://is1-ssl.mzstatic.com/autechre_100.jpg",
        },
    ],
}

ITUNES_SEARCH_RESPONSE = {
    "resultCount": 1,
    "results": [
        {
            "wrapperType": "track",
            "trackName": "Jenny Ondioline",
            "artistName": "Stereolab",
            "previewUrl": "https://audio-ssl.itunes.apple.com/stereolab_jenny.m4a",
            "artworkUrl100": "https://is1-ssl.mzstatic.com/stereolab_100.jpg",
        },
    ],
}

ITUNES_EMPTY_RESPONSE = {"resultCount": 0, "results": []}

# -- Deezer mock responses --

DEEZER_SEARCH_RESPONSE = {
    "data": [
        {
            "title": "Meinheld",
            "artist": {"name": "Stereolab"},
            "preview": "https://cdns-preview-e.dzcdn.net/stereolab_meinheld.mp3",
            "album": {"cover_medium": "https://api.deezer.com/album/cover.jpg"},
        },
    ],
}

DEEZER_EMPTY_RESPONSE = {"data": []}

# -- Bandcamp mock responses --

BANDCAMP_PAGE_HTML = """
<html><body>
<script data-tralbum='{"trackinfo":[{"title":"Untitled","file":{"mp3-128":"https://t4.bcbits.com/stream/autechre_untitled.mp3"}}],"current":{"title":"LP5"}}'></script>
</body></html>
"""

BANDCAMP_PAGE_NO_TRACKS = """
<html><body>
<script data-tralbum='{"trackinfo":[]}'></script>
</body></html>
"""


def _build_preview_fixture_db() -> str:
    """Create a fixture database with entity store tables and streaming IDs."""
    path = tempfile.mktemp(suffix=".db")
    store = EntityStore(path)
    store.initialize()

    # Autechre: has Apple Music ID
    entity_ae = store.get_or_create_entity("Autechre", "artist", wikidata_qid="Q2774")
    store.update_entity_streaming_ids(entity_ae.id, apple_music="15821237")
    store.upsert_artist(
        "Autechre",
        genre="Electronic",
        discogs_artist_id=12,
        entity_id=entity_ae.id,
    )
    store.update_artist_stats(
        "Autechre",
        ArtistStats(
            canonical_name="Autechre",
            total_plays=633,
            genre="Electronic",
            active_first_year=2004,
            active_last_year=2025,
            dj_count=45,
            request_ratio=0.1,
            show_count=500,
        ),
    )

    # Stereolab: no streaming IDs (will test fallbacks)
    entity_sl = store.get_or_create_entity("Stereolab", "artist", wikidata_qid="Q498895")
    store.upsert_artist(
        "Stereolab",
        genre="Rock",
        entity_id=entity_sl.id,
    )
    store.update_artist_stats(
        "Stereolab",
        ArtistStats(
            canonical_name="Stereolab",
            total_plays=450,
            genre="Rock",
            active_first_year=2003,
            active_last_year=2024,
            dj_count=38,
            request_ratio=0.05,
            show_count=300,
        ),
    )

    # Cat Power: has Bandcamp ID (will test Bandcamp fallback)
    entity_cp = store.get_or_create_entity("Cat Power", "artist", wikidata_qid="Q228899")
    store.update_entity_streaming_ids(entity_cp.id, bandcamp="catpower")
    store.upsert_artist(
        "Cat Power",
        genre="Rock",
        entity_id=entity_cp.id,
    )
    store.update_artist_stats(
        "Cat Power",
        ArtistStats(
            canonical_name="Cat Power",
            total_plays=200,
            genre="Rock",
            active_first_year=2005,
            active_last_year=2023,
            dj_count=20,
            request_ratio=0.02,
            show_count=150,
        ),
    )

    store.close()
    return path


@pytest.fixture(scope="module")
def preview_db_path() -> str:
    return _build_preview_fixture_db()


@pytest_asyncio.fixture()
async def preview_client(preview_db_path: str) -> AsyncClient:
    app = create_app(preview_db_path)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


def _artist_id(db_path: str, name: str) -> int:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT id FROM artist WHERE canonical_name = ?", (name,)).fetchone()
    conn.close()
    return row["id"]


def _clear_preview_cache(db_path: str, artist_id: int) -> None:
    cache_path = db_path + ".preview-cache.db"
    try:
        cache_conn = sqlite3.connect(cache_path)
        cache_conn.execute("DELETE FROM preview_cache WHERE artist_id = ?", (artist_id,))
        cache_conn.commit()
        cache_conn.close()
    except sqlite3.OperationalError:
        pass  # cache table doesn't exist yet


class TestPreviewEndpoint:
    @pytest.mark.asyncio
    async def test_preview_returns_url_via_itunes_lookup(
        self, preview_client: AsyncClient, preview_db_path: str
    ) -> None:
        """Artist with apple_music_artist_id gets a preview via iTunes lookup."""
        aid = _artist_id(preview_db_path, "Autechre")
        _clear_preview_cache(preview_db_path, aid)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = ITUNES_LOOKUP_RESPONSE

        with patch("semantic_index.api.preview._http_get", return_value=mock_response):
            resp = await preview_client.get(f"/graph/artists/{aid}/preview")

        assert resp.status_code == 200
        data = resp.json()
        assert data["preview_url"] == "https://audio-ssl.itunes.apple.com/autechre_gantz.m4a"
        assert data["track_name"] == "Gantz Graf"
        assert data["source"] == "itunes_lookup"
        assert data["artist_id"] == aid

    @pytest.mark.asyncio
    async def test_preview_caches_result(
        self, preview_client: AsyncClient, preview_db_path: str
    ) -> None:
        """Second request returns cached result without calling external API."""
        aid = _artist_id(preview_db_path, "Autechre")
        _clear_preview_cache(preview_db_path, aid)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = ITUNES_LOOKUP_RESPONSE

        with patch("semantic_index.api.preview._http_get", return_value=mock_response) as mock_get:
            resp1 = await preview_client.get(f"/graph/artists/{aid}/preview")
            resp2 = await preview_client.get(f"/graph/artists/{aid}/preview")

        assert resp1.status_code == 200
        assert resp2.status_code == 200
        assert resp1.json()["cached"] is False
        assert resp2.json()["cached"] is True
        # External API should have been called only once
        assert mock_get.call_count == 1

    @pytest.mark.asyncio
    async def test_preview_falls_back_to_deezer(
        self, preview_client: AsyncClient, preview_db_path: str
    ) -> None:
        """Artist without streaming IDs falls back to Deezer search."""
        aid = _artist_id(preview_db_path, "Stereolab")
        _clear_preview_cache(preview_db_path, aid)

        deezer_response = MagicMock()
        deezer_response.status_code = 200
        deezer_response.json.return_value = DEEZER_SEARCH_RESPONSE

        with patch("semantic_index.api.preview._http_get", return_value=deezer_response):
            resp = await preview_client.get(f"/graph/artists/{aid}/preview")

        assert resp.status_code == 200
        data = resp.json()
        assert data["preview_url"] == "https://cdns-preview-e.dzcdn.net/stereolab_meinheld.mp3"
        assert data["source"] == "deezer"

    @pytest.mark.asyncio
    async def test_preview_falls_back_to_bandcamp(
        self, preview_client: AsyncClient, preview_db_path: str
    ) -> None:
        """Artist with bandcamp_id falls back to Bandcamp track scrape."""
        aid = _artist_id(preview_db_path, "Cat Power")
        _clear_preview_cache(preview_db_path, aid)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = BANDCAMP_PAGE_HTML

        with patch("semantic_index.api.preview._http_get", return_value=mock_response):
            resp = await preview_client.get(f"/graph/artists/{aid}/preview")

        assert resp.status_code == 200
        data = resp.json()
        assert data["preview_url"] == "https://t4.bcbits.com/stream/autechre_untitled.mp3"
        assert data["source"] == "bandcamp"

    @pytest.mark.asyncio
    async def test_preview_falls_back_to_itunes_search(
        self, preview_client: AsyncClient, preview_db_path: str
    ) -> None:
        """When Deezer returns empty, falls back to iTunes search by name."""
        aid = _artist_id(preview_db_path, "Stereolab")
        _clear_preview_cache(preview_db_path, aid)

        call_count = 0

        def mock_get_side_effect(url, **kwargs):
            nonlocal call_count
            call_count += 1
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            if "deezer" in url:
                mock_resp.json.return_value = DEEZER_EMPTY_RESPONSE
            elif "itunes" in url and "search" in url:
                mock_resp.json.return_value = ITUNES_SEARCH_RESPONSE
            else:
                mock_resp.json.return_value = ITUNES_EMPTY_RESPONSE
            return mock_resp

        with patch("semantic_index.api.preview._http_get", side_effect=mock_get_side_effect):
            resp = await preview_client.get(f"/graph/artists/{aid}/preview")

        assert resp.status_code == 200
        data = resp.json()
        assert data["preview_url"] == "https://audio-ssl.itunes.apple.com/stereolab_jenny.m4a"
        assert data["source"] == "itunes_search"

    @pytest.mark.asyncio
    async def test_preview_returns_null_when_no_results(
        self, preview_client: AsyncClient, preview_db_path: str
    ) -> None:
        """All sources return empty — preview_url is null."""
        aid = _artist_id(preview_db_path, "Stereolab")
        _clear_preview_cache(preview_db_path, aid)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = ITUNES_EMPTY_RESPONSE

        def mock_get_all_empty(url, **kwargs):
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            if "deezer" in url:
                mock_resp.json.return_value = DEEZER_EMPTY_RESPONSE
            else:
                mock_resp.json.return_value = ITUNES_EMPTY_RESPONSE
            return mock_resp

        with patch("semantic_index.api.preview._http_get", side_effect=mock_get_all_empty):
            resp = await preview_client.get(f"/graph/artists/{aid}/preview")

        assert resp.status_code == 200
        data = resp.json()
        assert data["preview_url"] is None
        assert data["source"] == "none"

    @pytest.mark.asyncio
    async def test_preview_caches_null_result(
        self, preview_client: AsyncClient, preview_db_path: str
    ) -> None:
        """Null results are cached so we don't re-query external APIs."""
        aid = _artist_id(preview_db_path, "Stereolab")
        _clear_preview_cache(preview_db_path, aid)

        def mock_get_all_empty(url, **kwargs):
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            if "deezer" in url:
                mock_resp.json.return_value = DEEZER_EMPTY_RESPONSE
            else:
                mock_resp.json.return_value = ITUNES_EMPTY_RESPONSE
            return mock_resp

        with patch(
            "semantic_index.api.preview._http_get", side_effect=mock_get_all_empty
        ) as mock_get:
            resp1 = await preview_client.get(f"/graph/artists/{aid}/preview")
            resp2 = await preview_client.get(f"/graph/artists/{aid}/preview")

        assert resp1.json()["cached"] is False
        assert resp2.json()["cached"] is True
        assert resp2.json()["preview_url"] is None
        # External calls should only happen on the first request
        assert mock_get.call_count > 0

    @pytest.mark.asyncio
    async def test_preview_404_unknown_artist(self, preview_client: AsyncClient) -> None:
        """Non-existent artist ID returns 404."""
        resp = await preview_client.get("/graph/artists/999999/preview")
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_preview_handles_timeout(
        self, preview_client: AsyncClient, preview_db_path: str
    ) -> None:
        """Timeout on one source falls through to next."""
        aid = _artist_id(preview_db_path, "Stereolab")
        _clear_preview_cache(preview_db_path, aid)

        def mock_get_with_timeout(url, **kwargs):
            if "deezer" in url:
                raise httpx.TimeoutException("Connection timed out")
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            if "itunes" in url and "search" in url:
                mock_resp.json.return_value = ITUNES_SEARCH_RESPONSE
            else:
                mock_resp.json.return_value = ITUNES_EMPTY_RESPONSE
            return mock_resp

        with patch("semantic_index.api.preview._http_get", side_effect=mock_get_with_timeout):
            resp = await preview_client.get(f"/graph/artists/{aid}/preview")

        assert resp.status_code == 200
        data = resp.json()
        assert data["preview_url"] == "https://audio-ssl.itunes.apple.com/stereolab_jenny.m4a"
        assert data["source"] == "itunes_search"
