"""Tests for /graph/communities and /graph/discovery API endpoints, and community_id on ArtistSummary."""

from __future__ import annotations

import sqlite3
import tempfile

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from semantic_index.api.app import create_app
from semantic_index.graph_metrics import compute_and_persist
from semantic_index.models import ArtistStats, PmiEdge
from semantic_index.sqlite_export import export_sqlite


def _build_metrics_fixture_db() -> str:
    """Build a fixture DB with graph metrics populated."""
    path = tempfile.mktemp(suffix=".db")
    stats = {
        "Autechre": ArtistStats(
            canonical_name="Autechre",
            total_plays=50,
            genre="Electronic",
            dj_count=15,
            show_count=40,
        ),
        "Boards of Canada": ArtistStats(
            canonical_name="Boards of Canada",
            total_plays=40,
            genre="Electronic",
            dj_count=12,
            show_count=30,
        ),
        "Aphex Twin": ArtistStats(
            canonical_name="Aphex Twin",
            total_plays=45,
            genre="Electronic",
            dj_count=14,
            show_count=35,
        ),
        "Pavement": ArtistStats(
            canonical_name="Pavement",
            total_plays=35,
            genre="Rock",
            dj_count=10,
            show_count=25,
        ),
        "Guided by Voices": ArtistStats(
            canonical_name="Guided by Voices",
            total_plays=30,
            genre="Rock",
            dj_count=8,
            show_count=20,
        ),
        "Yo La Tengo": ArtistStats(
            canonical_name="Yo La Tengo",
            total_plays=55,
            genre="Rock",
            dj_count=18,
            show_count=45,
        ),
        "Stereolab": ArtistStats(
            canonical_name="Stereolab",
            total_plays=38,
            genre="Rock",
            dj_count=12,
            show_count=28,
        ),
    }
    pmi_edges = [
        PmiEdge(source="Autechre", target="Boards of Canada", raw_count=10, pmi=4.0),
        PmiEdge(source="Autechre", target="Aphex Twin", raw_count=8, pmi=3.5),
        PmiEdge(source="Boards of Canada", target="Aphex Twin", raw_count=7, pmi=3.0),
        PmiEdge(source="Boards of Canada", target="Autechre", raw_count=9, pmi=3.8),
        PmiEdge(source="Aphex Twin", target="Autechre", raw_count=6, pmi=2.8),
        PmiEdge(source="Aphex Twin", target="Boards of Canada", raw_count=5, pmi=2.5),
        PmiEdge(source="Pavement", target="Guided by Voices", raw_count=9, pmi=3.8),
        PmiEdge(source="Pavement", target="Yo La Tengo", raw_count=6, pmi=2.5),
        PmiEdge(source="Guided by Voices", target="Yo La Tengo", raw_count=5, pmi=2.0),
        PmiEdge(source="Guided by Voices", target="Pavement", raw_count=8, pmi=3.5),
        PmiEdge(source="Yo La Tengo", target="Pavement", raw_count=5, pmi=2.2),
        PmiEdge(source="Yo La Tengo", target="Guided by Voices", raw_count=4, pmi=1.8),
        PmiEdge(source="Stereolab", target="Autechre", raw_count=4, pmi=2.0),
        PmiEdge(source="Stereolab", target="Pavement", raw_count=3, pmi=1.5),
        PmiEdge(source="Autechre", target="Stereolab", raw_count=3, pmi=1.8),
        PmiEdge(source="Pavement", target="Stereolab", raw_count=2, pmi=1.2),
    ]
    export_sqlite(path, artist_stats=stats, pmi_edges=pmi_edges, xref_edges=[], min_count=2)

    # Add acoustic similarity for discovery scores
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS acoustic_similarity (
            artist_a_id INTEGER NOT NULL, artist_b_id INTEGER NOT NULL,
            similarity REAL NOT NULL, PRIMARY KEY (artist_a_id, artist_b_id)
        );
    """)
    ids = {r[1]: r[0] for r in conn.execute("SELECT id, canonical_name FROM artist")}
    conn.executemany(
        "INSERT INTO acoustic_similarity VALUES (?, ?, ?)",
        [
            (ids["Autechre"], ids["Boards of Canada"], 0.97),
            (ids["Autechre"], ids["Aphex Twin"], 0.97),
            (ids["Boards of Canada"], ids["Aphex Twin"], 0.97),
        ],
    )
    conn.commit()
    conn.close()

    compute_and_persist(path)
    return path


def _build_plain_fixture_db() -> str:
    """Build a fixture DB without graph metrics (old schema)."""
    path = tempfile.mktemp(suffix=".db")
    stats = {
        "Autechre": ArtistStats(
            canonical_name="Autechre",
            total_plays=50,
            genre="Electronic",
        ),
    }
    export_sqlite(path, artist_stats=stats, pmi_edges=[], xref_edges=[], min_count=2)
    return path


@pytest_asyncio.fixture
async def client():
    """AsyncClient for the metrics-populated DB."""
    import semantic_index.api.routes as routes_mod

    routes_mod._HAS_METRICS = None  # reset cached flag
    path = _build_metrics_fixture_db()
    app = create_app(path)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    routes_mod._HAS_METRICS = None


@pytest_asyncio.fixture
async def plain_client():
    """AsyncClient for a DB without graph metrics columns."""
    import semantic_index.api.routes as routes_mod

    routes_mod._HAS_METRICS = None
    path = _build_plain_fixture_db()
    app = create_app(path)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    routes_mod._HAS_METRICS = None


class TestCommunities:
    @pytest.mark.asyncio
    async def test_returns_communities(self, client):
        resp = await client.get("/graph/communities", params={"min_size": 1})
        assert resp.status_code == 200
        data = resp.json()
        assert "communities" in data
        assert len(data["communities"]) >= 2

    @pytest.mark.asyncio
    async def test_community_has_metadata(self, client):
        resp = await client.get("/graph/communities", params={"min_size": 1})
        data = resp.json()
        comm = data["communities"][0]
        assert "id" in comm
        assert "size" in comm
        assert comm["size"] >= 1
        assert "label" in comm

    @pytest.mark.asyncio
    async def test_min_size_filter(self, client):
        resp = await client.get("/graph/communities", params={"min_size": 100})
        assert resp.status_code == 200
        data = resp.json()
        assert data["communities"] == []

    @pytest.mark.asyncio
    async def test_graceful_on_old_schema(self, plain_client):
        resp = await plain_client.get("/graph/communities")
        assert resp.status_code == 200
        assert resp.json()["communities"] == []


class TestDiscovery:
    @pytest.mark.asyncio
    async def test_returns_scored_artists(self, client):
        resp = await client.get("/graph/discovery", params={"limit": 10})
        assert resp.status_code == 200
        data = resp.json()
        assert "results" in data
        assert len(data["results"]) > 0
        # Scores should be descending
        scores = [r["discovery_score"] for r in data["results"]]
        assert scores == sorted(scores, reverse=True)

    @pytest.mark.asyncio
    async def test_entry_has_expected_fields(self, client):
        resp = await client.get("/graph/discovery", params={"limit": 1})
        entry = resp.json()["results"][0]
        assert "artist" in entry
        assert "discovery_score" in entry
        assert "dj_edge_count" in entry
        assert "acoustic_neighbor_count" in entry
        assert entry["discovery_score"] > 0

    @pytest.mark.asyncio
    async def test_community_filter(self, client):
        resp = await client.get("/graph/discovery", params={"community_id": 9999})
        assert resp.status_code == 200
        assert resp.json()["results"] == []

    @pytest.mark.asyncio
    async def test_graceful_on_old_schema(self, plain_client):
        resp = await plain_client.get("/graph/discovery")
        assert resp.status_code == 200
        assert resp.json()["results"] == []


class TestArtistSummaryIncludesMetrics:
    @pytest.mark.asyncio
    async def test_search_includes_community_id(self, client):
        resp = await client.get("/graph/artists/search", params={"q": "Autechre"})
        assert resp.status_code == 200
        artist = resp.json()["results"][0]
        assert "community_id" in artist
        assert artist["community_id"] is not None

    @pytest.mark.asyncio
    async def test_search_includes_pagerank(self, client):
        resp = await client.get("/graph/artists/search", params={"q": "Autechre"})
        artist = resp.json()["results"][0]
        assert "pagerank" in artist
        assert artist["pagerank"] is not None
        assert artist["pagerank"] > 0

    @pytest.mark.asyncio
    async def test_neighbors_include_community_id(self, client):
        # Get Autechre's ID
        search_resp = await client.get("/graph/artists/search", params={"q": "Autechre"})
        artist_id = search_resp.json()["results"][0]["id"]
        resp = await client.get(
            f"/graph/artists/{artist_id}/neighbors",
            params={"type": "djTransition", "limit": 5},
        )
        assert resp.status_code == 200
        data = resp.json()
        # Center artist should have community_id
        assert data["artist"]["community_id"] is not None
        # Neighbors should also have community_id
        for n in data["neighbors"]:
            assert "community_id" in n["artist"]

    @pytest.mark.asyncio
    async def test_old_schema_returns_null_metrics(self, plain_client):
        resp = await plain_client.get("/graph/artists/search", params={"q": "Autechre"})
        artist = resp.json()["results"][0]
        assert artist["community_id"] is None
        assert artist["pagerank"] is None
