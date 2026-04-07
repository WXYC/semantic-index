"""Tests for the artist bio endpoint with sidecar caching."""

from __future__ import annotations

import sqlite3
import tempfile
from unittest.mock import patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from semantic_index.api.app import create_app
from semantic_index.api.bio import _generated_summary, parse_discogs_markup
from semantic_index.entity_store import EntityStore
from semantic_index.models import ArtistStats


def _build_bio_fixture_db() -> str:
    """Create a fixture database with entity store tables."""
    path = tempfile.mktemp(suffix=".db")
    store = EntityStore(path)
    store.initialize()

    entity_ae = store.get_or_create_entity("Autechre", "artist", wikidata_qid="Q2774")
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

    # Artist with no external IDs
    store.upsert_artist("Unknown Band", genre="Rock")
    store.update_artist_stats(
        "Unknown Band",
        ArtistStats(
            canonical_name="Unknown Band",
            total_plays=5,
            genre="Rock",
            active_first_year=2020,
            active_last_year=2021,
            dj_count=2,
            request_ratio=0.0,
            show_count=3,
        ),
    )

    store.close()
    return path


@pytest.fixture(scope="module")
def bio_db_path() -> str:
    return _build_bio_fixture_db()


@pytest_asyncio.fixture()
async def bio_client(bio_db_path: str) -> AsyncClient:
    app = create_app(bio_db_path)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


class TestParseDiscogsMarkup:
    def test_artist_link_with_disambiguation(self):
        result = parse_discogs_markup("[a=Rob Brown (3)]")
        assert "<a " in result
        assert "Rob Brown</a>" in result  # display name has disambiguation stripped
        assert "discogs.com" in result

    def test_artist_link_no_disambiguation(self):
        result = parse_discogs_markup("[a=Sean Booth]")
        assert "Sean Booth</a>" in result

    def test_label_plain_text(self):
        result = parse_discogs_markup("[l=Warp Records]")
        assert result == "Warp Records"
        assert "<a" not in result

    def test_url_link(self):
        result = parse_discogs_markup("[url=http://example.com]Example[/url]")
        assert '<a href="http://example.com"' in result
        assert "Example</a>" in result

    def test_bold(self):
        assert parse_discogs_markup("[b]bold text[/b]") == "<b>bold text</b>"

    def test_italic(self):
        assert parse_discogs_markup("[i]italic text[/i]") == "<i>italic text</i>"

    def test_full_bio(self):
        bio = (
            "An English electronic music duo formed in 1987 in Rochdale, "
            "Greater Manchester, UK by [a=Rob Brown (3)] and [a=Sean Booth]. "
            "They are also heavily involved with the [a=Gescom] collective."
        )
        result = parse_discogs_markup(bio)
        assert "Rob Brown</a>" in result
        assert "Sean Booth</a>" in result
        assert "Gescom</a>" in result
        assert "[a=" not in result
        assert "[/a]" not in result

    def test_plain_text_unchanged(self):
        text = "Just a plain bio with no markup."
        assert parse_discogs_markup(text) == text

    def test_html_escaping(self):
        result = parse_discogs_markup("Tom & Jerry <rock>")
        assert "&amp;" in result
        assert "&lt;rock&gt;" in result


class TestGeneratedSummary:
    def test_format(self):
        detail = {
            "canonical_name": "Autechre",
            "genre": "Electronic",
            "total_plays": 633,
            "dj_count": 45,
            "active_first_year": 2004,
            "active_last_year": 2025,
        }
        summary = _generated_summary(detail)
        assert "Electronic" in summary
        assert "633" in summary
        assert "45" in summary
        assert "2004" in summary
        assert "2025" in summary

    def test_no_years(self):
        detail = {
            "canonical_name": "Unknown",
            "genre": "Rock",
            "total_plays": 5,
            "dj_count": 2,
            "active_first_year": None,
            "active_last_year": None,
        }
        summary = _generated_summary(detail)
        assert "Rock" in summary
        assert "5" in summary


class TestBioEndpoint:
    @pytest.mark.asyncio
    async def test_returns_generated_for_unknown_artist(
        self, bio_client: AsyncClient, bio_db_path: str
    ) -> None:
        """Artist with no external IDs gets a generated summary."""
        # Find the Unknown Band ID
        conn = sqlite3.connect(bio_db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT id FROM artist WHERE canonical_name = 'Unknown Band'").fetchone()
        conn.close()

        with patch("semantic_index.api.bio._fetch_wikipedia") as mock_wiki:
            mock_wiki.return_value = None
            resp = await bio_client.get(f"/graph/artists/{row['id']}/bio")

        assert resp.status_code == 200
        data = resp.json()
        assert data["source"] == "generated"
        assert "Rock" in data["bio"]

    @pytest.mark.asyncio
    async def test_caches_result(self, bio_client: AsyncClient, bio_db_path: str) -> None:
        """Second request returns cached result."""
        conn = sqlite3.connect(bio_db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT id FROM artist WHERE canonical_name = 'Unknown Band'").fetchone()
        conn.close()
        aid = row["id"]

        # Clear cache from previous tests
        cache_path = bio_db_path + ".bio-cache.db"
        cache_conn = sqlite3.connect(cache_path)
        cache_conn.execute("DELETE FROM bio_cache WHERE artist_id = ?", (aid,))
        cache_conn.commit()
        cache_conn.close()

        with patch("semantic_index.api.bio._fetch_wikipedia") as mock_wiki:
            mock_wiki.return_value = None
            resp1 = await bio_client.get(f"/graph/artists/{aid}/bio")
            resp2 = await bio_client.get(f"/graph/artists/{aid}/bio")

        assert resp1.json()["cached"] is False
        assert resp2.json()["cached"] is True

    @pytest.mark.asyncio
    async def test_wikipedia_preferred(self, bio_client: AsyncClient, bio_db_path: str) -> None:
        """Wikipedia extract is used when available."""
        conn = sqlite3.connect(bio_db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT id FROM artist WHERE canonical_name = 'Autechre'").fetchone()
        conn.close()

        # Clear any cached bio for this artist
        cache_path = bio_db_path + ".bio-cache.db"
        cache_conn = sqlite3.connect(cache_path)
        cache_conn.execute("DELETE FROM bio_cache WHERE artist_id = ?", (row["id"],))
        cache_conn.commit()
        cache_conn.close()

        with (
            patch("semantic_index.api.bio._fetch_wikipedia") as mock_wiki,
            patch("semantic_index.api.bio._fetch_discogs_profile") as mock_discogs,
        ):
            mock_wiki.return_value = "Autechre are a British electronic music duo."
            mock_discogs.return_value = "Short."
            resp = await bio_client.get(f"/graph/artists/{row['id']}/bio")

        assert resp.status_code == 200
        data = resp.json()
        assert data["source"] == "wikipedia"
        assert "British electronic music duo" in data["bio"]

    @pytest.mark.asyncio
    async def test_discogs_used_when_longer(
        self, bio_client: AsyncClient, bio_db_path: str
    ) -> None:
        """Discogs profile used when longer than Wikipedia."""
        conn = sqlite3.connect(bio_db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT id FROM artist WHERE canonical_name = 'Autechre'").fetchone()
        conn.close()

        cache_path = bio_db_path + ".bio-cache.db"
        cache_conn = sqlite3.connect(cache_path)
        cache_conn.execute("DELETE FROM bio_cache WHERE artist_id = ?", (row["id"],))
        cache_conn.commit()
        cache_conn.close()

        with (
            patch("semantic_index.api.bio._fetch_wikipedia") as mock_wiki,
            patch("semantic_index.api.bio._fetch_discogs_profile") as mock_discogs,
        ):
            mock_wiki.return_value = "Short."
            mock_discogs.return_value = (
                "Autechre are a British electronic music duo consisting of "
                "Rob Brown and Sean Booth, both from Rochdale, Greater Manchester."
            )
            resp = await bio_client.get(f"/graph/artists/{row['id']}/bio")

        data = resp.json()
        assert data["source"] == "discogs"
        assert "Rob Brown" in data["bio"]

    @pytest.mark.asyncio
    async def test_404_unknown_id(self, bio_client: AsyncClient) -> None:
        resp = await bio_client.get("/graph/artists/99999/bio")
        assert resp.status_code == 404
