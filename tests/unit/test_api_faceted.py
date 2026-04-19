"""Tests for faceted PMI API endpoints: /graph/facets and filtered neighbors."""

from __future__ import annotations

import sqlite3
import tempfile

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from semantic_index.api.app import create_app
from semantic_index.facet_export import export_facet_tables
from semantic_index.models import ArtistStats, PmiEdge
from semantic_index.sqlite_export import export_sqlite
from tests.conftest import make_adjacency_pair, make_resolved_entry

# -- Timestamps (Unix ms) --
# Show 1: January 2024
JAN_15_2024 = 1705276800000
# Show 2: July 2024
JUL_10_2024 = 1720569600000
# Show 3: January 2024 (different DJ)
JAN_20_2024 = 1705708800000


def _build_faceted_fixture_db() -> str:
    """Build a fixture DB with both pre-computed edges and facet tables.

    Layout:
    - Show 1 (DJ Cool, January): Autechre -> Stereolab -> Cat Power
    - Show 2 (DJ Cool, July):    Autechre -> Cat Power
    - Show 3 (DJ Sunshine, January): Stereolab -> Autechre
    """
    path = tempfile.mktemp(suffix=".db")

    stats = {
        "Autechre": ArtistStats(
            canonical_name="Autechre",
            total_plays=4,
            genre="Electronic",
        ),
        "Stereolab": ArtistStats(
            canonical_name="Stereolab",
            total_plays=3,
            genre="Rock",
        ),
        "Cat Power": ArtistStats(
            canonical_name="Cat Power",
            total_plays=2,
            genre="Rock",
        ),
    }
    pmi_edges = [
        PmiEdge(source="Autechre", target="Stereolab", raw_count=3, pmi=2.5),
        PmiEdge(source="Autechre", target="Cat Power", raw_count=2, pmi=1.8),
        PmiEdge(source="Stereolab", target="Cat Power", raw_count=1, pmi=0.5),
    ]
    export_sqlite(path, artist_stats=stats, pmi_edges=pmi_edges, xref_edges=[], min_count=1)

    # Read name_to_id
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    name_to_id = {
        r["canonical_name"]: r["id"]
        for r in conn.execute("SELECT id, canonical_name FROM artist").fetchall()
    }
    conn.close()

    # Resolved entries for all three shows
    resolved_entries = [
        # Show 1: January, DJ Cool
        make_resolved_entry(
            id=1, canonical_name="Autechre", show_id=1, sequence=1, start_time=JAN_15_2024
        ),
        make_resolved_entry(
            id=2, canonical_name="Stereolab", show_id=1, sequence=2, start_time=JAN_15_2024
        ),
        make_resolved_entry(
            id=3, canonical_name="Cat Power", show_id=1, sequence=3, start_time=JAN_15_2024
        ),
        # Show 2: July, DJ Cool
        make_resolved_entry(
            id=4, canonical_name="Autechre", show_id=2, sequence=1, start_time=JUL_10_2024
        ),
        make_resolved_entry(
            id=5, canonical_name="Cat Power", show_id=2, sequence=2, start_time=JUL_10_2024
        ),
        # Show 3: January, DJ Sunshine
        make_resolved_entry(
            id=6, canonical_name="Stereolab", show_id=3, sequence=1, start_time=JAN_20_2024
        ),
        make_resolved_entry(
            id=7, canonical_name="Autechre", show_id=3, sequence=2, start_time=JAN_20_2024
        ),
    ]
    pairs = [
        # Show 1
        make_adjacency_pair(source="Autechre", target="Stereolab", show_id=1),
        make_adjacency_pair(source="Stereolab", target="Cat Power", show_id=1),
        # Show 2
        make_adjacency_pair(source="Autechre", target="Cat Power", show_id=2),
        # Show 3
        make_adjacency_pair(source="Stereolab", target="Autechre", show_id=3),
    ]
    show_to_dj = {1: 42, 2: 42, 3: 99}
    show_dj_names = {1: "DJ Cool", 2: "DJ Cool", 3: "DJ Sunshine"}

    export_facet_tables(
        db_path=path,
        resolved_entries=resolved_entries,
        name_to_id=name_to_id,
        show_to_dj=show_to_dj,
        show_dj_names=show_dj_names,
        adjacency_pairs=pairs,
    )
    return path


def _build_old_schema_db() -> str:
    """Build a fixture DB without facet tables (old schema)."""
    path = tempfile.mktemp(suffix=".db")
    stats = {
        "Autechre": ArtistStats(canonical_name="Autechre", total_plays=10, genre="Electronic"),
    }
    export_sqlite(path, artist_stats=stats, pmi_edges=[], xref_edges=[], min_count=1)
    return path


@pytest.fixture(scope="module")
def faceted_db_path() -> str:
    return _build_faceted_fixture_db()


@pytest.fixture(scope="module")
def faceted_artist_ids(faceted_db_path: str) -> dict[str, int]:
    conn = sqlite3.connect(faceted_db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT id, canonical_name FROM artist").fetchall()
    conn.close()
    return {r["canonical_name"]: r["id"] for r in rows}


@pytest.fixture(scope="module")
def faceted_dj_ids(faceted_db_path: str) -> dict[str, int]:
    conn = sqlite3.connect(faceted_db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT id, display_name FROM dj").fetchall()
    conn.close()
    return {r["display_name"]: r["id"] for r in rows}


@pytest_asyncio.fixture
async def client(faceted_db_path: str) -> AsyncClient:
    app = create_app(faceted_db_path)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest_asyncio.fixture
async def old_client() -> AsyncClient:
    path = _build_old_schema_db()
    app = create_app(path)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


class TestFacetsEndpoint:
    @pytest.mark.asyncio
    async def test_returns_months_without_djs_by_default(self, client: AsyncClient) -> None:
        resp = await client.get("/graph/facets")
        assert resp.status_code == 200
        data = resp.json()
        assert 1 in data["months"]
        assert 7 in data["months"]
        assert data["djs"] == []

    @pytest.mark.asyncio
    async def test_returns_djs_when_requested(self, client: AsyncClient) -> None:
        resp = await client.get("/graph/facets?include_djs=true")
        assert resp.status_code == 200
        data = resp.json()
        dj_names = {d["display_name"] for d in data["djs"]}
        assert "DJ Cool" in dj_names
        assert "DJ Sunshine" in dj_names

    @pytest.mark.asyncio
    async def test_graceful_on_old_db(self, old_client: AsyncClient) -> None:
        resp = await old_client.get("/graph/facets")
        assert resp.status_code == 200
        data = resp.json()
        assert data["months"] == []
        assert data["djs"] == []


class TestNeighborsNoFacet:
    @pytest.mark.asyncio
    async def test_uses_precomputed_table(
        self, client: AsyncClient, faceted_artist_ids: dict[str, int]
    ) -> None:
        """Without facet params, neighbors come from the pre-computed dj_transition table."""
        ae_id = faceted_artist_ids["Autechre"]
        resp = await client.get(
            f"/graph/artists/{ae_id}/neighbors",
            params={"type": "djTransition", "limit": 10},
        )
        assert resp.status_code == 200
        data = resp.json()
        names = {n["artist"]["canonical_name"] for n in data["neighbors"]}
        assert "Stereolab" in names
        assert "Cat Power" in names


class TestNeighborsMonthFilter:
    @pytest.mark.asyncio
    async def test_january_neighbors(
        self, client: AsyncClient, faceted_artist_ids: dict[str, int]
    ) -> None:
        """In January, Autechre's neighbors are Stereolab (show 1 + show 3)."""
        ae_id = faceted_artist_ids["Autechre"]
        resp = await client.get(
            f"/graph/artists/{ae_id}/neighbors",
            params={"type": "djTransition", "limit": 10, "month": 1},
        )
        assert resp.status_code == 200
        data = resp.json()
        names = {n["artist"]["canonical_name"] for n in data["neighbors"]}
        # Autechre->Stereolab in show 1, Stereolab->Autechre in show 3
        assert "Stereolab" in names

    @pytest.mark.asyncio
    async def test_july_neighbors(
        self, client: AsyncClient, faceted_artist_ids: dict[str, int]
    ) -> None:
        """In July, Autechre's only neighbor is Cat Power (show 2)."""
        ae_id = faceted_artist_ids["Autechre"]
        resp = await client.get(
            f"/graph/artists/{ae_id}/neighbors",
            params={"type": "djTransition", "limit": 10, "month": 7},
        )
        assert resp.status_code == 200
        data = resp.json()
        names = {n["artist"]["canonical_name"] for n in data["neighbors"]}
        assert "Cat Power" in names
        # Stereolab only appears in January shows, not July
        assert "Stereolab" not in names

    @pytest.mark.asyncio
    async def test_returns_pmi_in_detail(
        self, client: AsyncClient, faceted_artist_ids: dict[str, int]
    ) -> None:
        ae_id = faceted_artist_ids["Autechre"]
        resp = await client.get(
            f"/graph/artists/{ae_id}/neighbors",
            params={"type": "djTransition", "limit": 10, "month": 1},
        )
        data = resp.json()
        assert len(data["neighbors"]) > 0
        neighbor = data["neighbors"][0]
        assert "pmi" in neighbor["detail"]
        assert "raw_count" in neighbor["detail"]
        assert neighbor["detail"]["raw_count"] >= 1


class TestNeighborsDjFilter:
    @pytest.mark.asyncio
    async def test_dj_cool_neighbors(
        self,
        client: AsyncClient,
        faceted_artist_ids: dict[str, int],
        faceted_dj_ids: dict[str, int],
    ) -> None:
        """DJ Cool plays Autechre in show 1 (→ Stereolab) and show 2 (→ Cat Power)."""
        ae_id = faceted_artist_ids["Autechre"]
        cool_id = faceted_dj_ids["DJ Cool"]
        resp = await client.get(
            f"/graph/artists/{ae_id}/neighbors",
            params={"type": "djTransition", "limit": 10, "dj_id": cool_id},
        )
        assert resp.status_code == 200
        names = {n["artist"]["canonical_name"] for n in resp.json()["neighbors"]}
        assert "Stereolab" in names
        assert "Cat Power" in names

    @pytest.mark.asyncio
    async def test_dj_sunshine_neighbors(
        self,
        client: AsyncClient,
        faceted_artist_ids: dict[str, int],
        faceted_dj_ids: dict[str, int],
    ) -> None:
        """DJ Sunshine only has show 3: Stereolab → Autechre."""
        ae_id = faceted_artist_ids["Autechre"]
        sunshine_id = faceted_dj_ids["DJ Sunshine"]
        resp = await client.get(
            f"/graph/artists/{ae_id}/neighbors",
            params={"type": "djTransition", "limit": 10, "dj_id": sunshine_id},
        )
        assert resp.status_code == 200
        names = {n["artist"]["canonical_name"] for n in resp.json()["neighbors"]}
        assert "Stereolab" in names
        # Cat Power never appears in DJ Sunshine's shows
        assert "Cat Power" not in names


class TestNeighborsBothFacets:
    @pytest.mark.asyncio
    async def test_month_and_dj_combined(
        self,
        client: AsyncClient,
        faceted_artist_ids: dict[str, int],
        faceted_dj_ids: dict[str, int],
    ) -> None:
        """DJ Cool in January = only show 1: Autechre -> Stereolab -> Cat Power."""
        ae_id = faceted_artist_ids["Autechre"]
        cool_id = faceted_dj_ids["DJ Cool"]
        resp = await client.get(
            f"/graph/artists/{ae_id}/neighbors",
            params={"type": "djTransition", "limit": 10, "month": 1, "dj_id": cool_id},
        )
        assert resp.status_code == 200
        names = {n["artist"]["canonical_name"] for n in resp.json()["neighbors"]}
        # In January show 1 with DJ Cool, Autechre -> Stereolab
        assert "Stereolab" in names
        # Cat Power is NOT an Autechre neighbor (Cat Power follows Stereolab, not Autechre)
        assert "Cat Power" not in names


class TestHeatPrecomputed:
    @pytest.mark.asyncio
    async def test_heat_cool_ranks_by_raw_count(
        self, client: AsyncClient, faceted_artist_ids: dict[str, int]
    ) -> None:
        """heat=0 (cool) ranks by raw_count; Stereolab (raw_count=3) should rank above Cat Power (raw_count=2)."""
        ae_id = faceted_artist_ids["Autechre"]
        resp = await client.get(
            f"/graph/artists/{ae_id}/neighbors",
            params={"type": "djTransition", "limit": 10, "heat": 0.0},
        )
        assert resp.status_code == 200
        neighbors = resp.json()["neighbors"]
        assert len(neighbors) >= 2
        assert neighbors[0]["detail"]["raw_count"] >= neighbors[-1]["detail"]["raw_count"]


class TestHeatFaceted:
    @pytest.mark.asyncio
    async def test_heat_with_month_filter(
        self,
        client: AsyncClient,
        faceted_artist_ids: dict[str, int],
    ) -> None:
        """heat parameter works with month facet filter."""
        ae_id = faceted_artist_ids["Autechre"]
        resp = await client.get(
            f"/graph/artists/{ae_id}/neighbors",
            params={"type": "djTransition", "limit": 10, "month": 1, "heat": 0.0},
        )
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_heat_with_dj_filter(
        self,
        client: AsyncClient,
        faceted_artist_ids: dict[str, int],
        faceted_dj_ids: dict[str, int],
    ) -> None:
        """heat parameter works with DJ facet filter."""
        ae_id = faceted_artist_ids["Autechre"]
        sunshine_id = faceted_dj_ids["DJ Sunshine"]
        resp = await client.get(
            f"/graph/artists/{ae_id}/neighbors",
            params={"type": "djTransition", "limit": 10, "dj_id": sunshine_id, "heat": 1.0},
        )
        assert resp.status_code == 200


class TestFacetIgnoredForNonDjEdges:
    @pytest.mark.asyncio
    async def test_shared_style_ignores_month(
        self, client: AsyncClient, faceted_artist_ids: dict[str, int]
    ) -> None:
        ae_id = faceted_artist_ids["Autechre"]
        resp = await client.get(
            f"/graph/artists/{ae_id}/neighbors",
            params={"type": "sharedStyle", "limit": 10, "month": 1},
        )
        # Should work (not error), facet is simply ignored
        assert resp.status_code == 200
