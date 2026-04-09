"""Tests for Graph API query endpoints: search, neighbors, explain, artist detail, entity artists."""

from __future__ import annotations

import sqlite3
import tempfile

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from semantic_index.api.app import create_app
from semantic_index.entity_store import EntityStore
from semantic_index.models import (
    ArtistStats,
    CrossReferenceEdge,
    PmiEdge,
    SharedPersonnelEdge,
    SharedStyleEdge,
    WikidataInfluenceEdge,
)
from semantic_index.sqlite_export import export_sqlite


def _build_fixture_db() -> str:
    """Create a fixture SQLite database and return its path."""
    path = tempfile.mktemp(suffix=".db")
    stats = {
        "Autechre": ArtistStats(
            canonical_name="Autechre",
            total_plays=50,
            genre="Electronic",
            active_first_year=2004,
            active_last_year=2025,
            dj_count=15,
            request_ratio=0.1,
            show_count=40,
        ),
        "Stereolab": ArtistStats(
            canonical_name="Stereolab",
            total_plays=30,
            genre="Rock",
            active_first_year=2003,
            active_last_year=2024,
            dj_count=10,
            request_ratio=0.05,
            show_count=25,
        ),
        "Cat Power": ArtistStats(
            canonical_name="Cat Power",
            total_plays=20,
            genre="Rock",
            active_first_year=2005,
            active_last_year=2023,
            dj_count=8,
            request_ratio=0.02,
            show_count=15,
        ),
        "Father John Misty": ArtistStats(
            canonical_name="Father John Misty",
            total_plays=15,
            genre="Rock",
            active_first_year=2013,
            active_last_year=2025,
            dj_count=5,
            request_ratio=0.0,
            show_count=10,
        ),
        "Broadcast": ArtistStats(
            canonical_name="Broadcast",
            total_plays=25,
            genre="Electronic",
            active_first_year=2004,
            active_last_year=2020,
            dj_count=8,
            request_ratio=0.0,
            show_count=18,
        ),
        "Ata Kak": ArtistStats(
            canonical_name="Ata Kak",
            total_plays=3,
            genre="Electronic",
            active_first_year=2015,
            active_last_year=2019,
            dj_count=2,
            request_ratio=0.0,
            show_count=2,
        ),
    }
    pmi_edges = [
        PmiEdge(source="Autechre", target="Stereolab", raw_count=5, pmi=3.0),
        PmiEdge(source="Autechre", target="Cat Power", raw_count=3, pmi=1.5),
        PmiEdge(source="Stereolab", target="Cat Power", raw_count=2, pmi=0.8),
    ]
    xref_edges = [
        CrossReferenceEdge(
            artist_a="Stereolab",
            artist_b="Cat Power",
            comment="See also",
            source="library_code",
        ),
    ]
    shared_personnel = [
        SharedPersonnelEdge(
            artist_a="Autechre",
            artist_b="Stereolab",
            shared_count=2,
            shared_names=["Jim O'Rourke", "John McEntire"],
        ),
    ]
    shared_styles = [
        SharedStyleEdge(
            artist_a="Autechre",
            artist_b="Stereolab",
            jaccard=0.5,
            shared_tags=["Electronic", "Experimental"],
        ),
    ]
    influence_edges = [
        WikidataInfluenceEdge(
            source_artist="Autechre",
            target_artist="Stereolab",
            source_qid="Q2774",
            target_qid="Q650826",
        ),
        WikidataInfluenceEdge(
            source_artist="Cat Power",
            target_artist="Stereolab",
            source_qid="Q218981",
            target_qid="Q650826",
        ),
    ]
    export_sqlite(
        path,
        artist_stats=stats,
        pmi_edges=pmi_edges,
        xref_edges=xref_edges,
        min_count=1,
        shared_personnel_edges=shared_personnel,
        shared_style_edges=shared_styles,
        wikidata_influence_edges=influence_edges,
    )
    return path


@pytest.fixture(scope="module")
def db_path() -> str:
    return _build_fixture_db()


@pytest.fixture(scope="module")
def artist_ids(db_path: str) -> dict[str, int]:
    """Return a mapping of canonical_name -> id from the fixture database."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT id, canonical_name FROM artist").fetchall()
    conn.close()
    return {r["canonical_name"]: r["id"] for r in rows}


@pytest_asyncio.fixture
async def client(db_path: str) -> AsyncClient:
    app = create_app(db_path)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


class TestSearch:
    @pytest.mark.asyncio
    async def test_search_by_exact_name(self, client: AsyncClient) -> None:
        resp = await client.get("/graph/artists/search", params={"q": "Autechre"})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["results"]) == 1
        assert data["results"][0]["canonical_name"] == "Autechre"
        assert data["results"][0]["genre"] == "Electronic"
        assert data["results"][0]["total_plays"] == 50

    @pytest.mark.asyncio
    async def test_search_case_insensitive(self, client: AsyncClient) -> None:
        resp = await client.get("/graph/artists/search", params={"q": "autechre"})
        assert resp.status_code == 200
        assert len(resp.json()["results"]) == 1
        assert resp.json()["results"][0]["canonical_name"] == "Autechre"

    @pytest.mark.asyncio
    async def test_search_partial_match(self, client: AsyncClient) -> None:
        resp = await client.get("/graph/artists/search", params={"q": "stereo"})
        assert resp.status_code == 200
        assert len(resp.json()["results"]) == 1
        assert resp.json()["results"][0]["canonical_name"] == "Stereolab"

    @pytest.mark.asyncio
    async def test_search_no_results(self, client: AsyncClient) -> None:
        resp = await client.get("/graph/artists/search", params={"q": "Nonexistent"})
        assert resp.status_code == 200
        assert len(resp.json()["results"]) == 0

    @pytest.mark.asyncio
    async def test_search_ordered_by_total_plays(self, client: AsyncClient) -> None:
        """Multiple Rock artists should come back ordered by total_plays."""
        resp = await client.get("/graph/artists/search", params={"q": "Power"})
        assert resp.status_code == 200
        results = resp.json()["results"]
        assert len(results) == 1
        assert results[0]["canonical_name"] == "Cat Power"

    @pytest.mark.asyncio
    async def test_search_prefix_matches_ranked_first(self, client: AsyncClient) -> None:
        """Prefix matches should rank above substring matches regardless of play count."""
        resp = await client.get("/graph/artists/search", params={"q": "at"})
        assert resp.status_code == 200
        results = resp.json()["results"]
        names = [r["canonical_name"] for r in results]
        # Ata Kak (3 plays, prefix) should rank above Cat Power (20 plays, substring)
        assert names[0] == "Ata Kak"
        assert "Cat Power" in names

    @pytest.mark.asyncio
    async def test_search_limit(self, client: AsyncClient) -> None:
        # All four artists contain a space or letter — search broadly
        resp = await client.get("/graph/artists/search", params={"q": "e", "limit": 2})
        assert resp.status_code == 200
        assert len(resp.json()["results"]) <= 2

    @pytest.mark.asyncio
    async def test_search_empty_query_rejected(self, client: AsyncClient) -> None:
        resp = await client.get("/graph/artists/search", params={"q": ""})
        assert resp.status_code == 422


class TestRandom:
    @pytest.mark.asyncio
    async def test_random_returns_valid_artist(self, client: AsyncClient) -> None:
        resp = await client.get("/graph/artists/random")
        assert resp.status_code == 200
        data = resp.json()
        assert "id" in data
        assert "canonical_name" in data
        assert "genre" in data
        assert "total_plays" in data

    @pytest.mark.asyncio
    async def test_random_only_returns_connected_artists(self, client: AsyncClient) -> None:
        """The random endpoint should only return artists that have DJ transition edges.

        Father John Misty has no DJ transition edges in the fixture, so should never appear.
        """
        connected = {"Autechre", "Stereolab", "Cat Power"}
        for _ in range(20):
            resp = await client.get("/graph/artists/random")
            assert resp.json()["canonical_name"] in connected


class TestNeighbors:
    @pytest.mark.asyncio
    async def test_dj_transition_neighbors(
        self, client: AsyncClient, artist_ids: dict[str, int]
    ) -> None:
        aid = artist_ids["Autechre"]
        resp = await client.get(f"/graph/artists/{aid}/neighbors", params={"type": "djTransition"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["artist"]["canonical_name"] == "Autechre"
        assert data["edge_type"] == "djTransition"
        names = [n["artist"]["canonical_name"] for n in data["neighbors"]]
        assert "Stereolab" in names
        assert "Cat Power" in names
        # Ordered by PMI descending — Stereolab (3.0) before Cat Power (1.5)
        assert names.index("Stereolab") < names.index("Cat Power")
        # Check detail fields
        stereo_entry = next(
            n for n in data["neighbors"] if n["artist"]["canonical_name"] == "Stereolab"
        )
        assert stereo_entry["weight"] == 3.0
        assert stereo_entry["detail"]["raw_count"] == 5
        assert stereo_entry["detail"]["pmi"] == 3.0

    @pytest.mark.asyncio
    async def test_shared_personnel_neighbors(
        self, client: AsyncClient, artist_ids: dict[str, int]
    ) -> None:
        aid = artist_ids["Autechre"]
        resp = await client.get(
            f"/graph/artists/{aid}/neighbors", params={"type": "sharedPersonnel"}
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["neighbors"]) == 1
        assert data["neighbors"][0]["artist"]["canonical_name"] == "Stereolab"
        assert data["neighbors"][0]["detail"]["shared_count"] == 2
        assert "Jim O'Rourke" in data["neighbors"][0]["detail"]["shared_names"]

    @pytest.mark.asyncio
    async def test_shared_style_neighbors(
        self, client: AsyncClient, artist_ids: dict[str, int]
    ) -> None:
        aid = artist_ids["Autechre"]
        resp = await client.get(f"/graph/artists/{aid}/neighbors", params={"type": "sharedStyle"})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["neighbors"]) == 1
        assert data["neighbors"][0]["weight"] == 0.5
        assert "Electronic" in data["neighbors"][0]["detail"]["shared_tags"]

    @pytest.mark.asyncio
    async def test_cross_reference_neighbors(
        self, client: AsyncClient, artist_ids: dict[str, int]
    ) -> None:
        aid = artist_ids["Stereolab"]
        resp = await client.get(
            f"/graph/artists/{aid}/neighbors", params={"type": "crossReference"}
        )
        assert resp.status_code == 200
        data = resp.json()
        names = [n["artist"]["canonical_name"] for n in data["neighbors"]]
        assert "Cat Power" in names

    @pytest.mark.asyncio
    async def test_neighbors_default_type_is_dj_transition(
        self, client: AsyncClient, artist_ids: dict[str, int]
    ) -> None:
        aid = artist_ids["Autechre"]
        resp = await client.get(f"/graph/artists/{aid}/neighbors")
        assert resp.status_code == 200
        assert resp.json()["edge_type"] == "djTransition"

    @pytest.mark.asyncio
    async def test_neighbors_limit(self, client: AsyncClient, artist_ids: dict[str, int]) -> None:
        aid = artist_ids["Autechre"]
        resp = await client.get(
            f"/graph/artists/{aid}/neighbors", params={"type": "djTransition", "limit": 1}
        )
        assert resp.status_code == 200
        assert len(resp.json()["neighbors"]) == 1

    @pytest.mark.asyncio
    async def test_neighbors_unknown_artist_404(self, client: AsyncClient) -> None:
        resp = await client.get("/graph/artists/99999/neighbors")
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_neighbors_no_edges(
        self, client: AsyncClient, artist_ids: dict[str, int]
    ) -> None:
        """Father John Misty has no shared personnel edges."""
        aid = artist_ids["Father John Misty"]
        resp = await client.get(
            f"/graph/artists/{aid}/neighbors", params={"type": "sharedPersonnel"}
        )
        assert resp.status_code == 200
        assert len(resp.json()["neighbors"]) == 0

    @pytest.mark.asyncio
    async def test_min_raw_count_default_returns_all(
        self, client: AsyncClient, artist_ids: dict[str, int]
    ) -> None:
        """Default min_raw_count=1 returns all DJ transition neighbors."""
        aid = artist_ids["Autechre"]
        resp = await client.get(f"/graph/artists/{aid}/neighbors", params={"type": "djTransition"})
        assert resp.status_code == 200
        names = [n["artist"]["canonical_name"] for n in resp.json()["neighbors"]]
        assert "Stereolab" in names  # raw_count=5
        assert "Cat Power" in names  # raw_count=3

    @pytest.mark.asyncio
    async def test_min_raw_count_filters_low_count_edges(
        self, client: AsyncClient, artist_ids: dict[str, int]
    ) -> None:
        """min_raw_count=4 should exclude Cat Power (raw_count=3) but keep Stereolab (raw_count=5)."""
        aid = artist_ids["Autechre"]
        resp = await client.get(
            f"/graph/artists/{aid}/neighbors",
            params={"type": "djTransition", "min_raw_count": 4},
        )
        assert resp.status_code == 200
        names = [n["artist"]["canonical_name"] for n in resp.json()["neighbors"]]
        assert "Stereolab" in names
        assert "Cat Power" not in names

    @pytest.mark.asyncio
    async def test_min_raw_count_filters_all(
        self, client: AsyncClient, artist_ids: dict[str, int]
    ) -> None:
        """min_raw_count higher than any raw_count returns no neighbors."""
        aid = artist_ids["Autechre"]
        resp = await client.get(
            f"/graph/artists/{aid}/neighbors",
            params={"type": "djTransition", "min_raw_count": 100},
        )
        assert resp.status_code == 200
        assert len(resp.json()["neighbors"]) == 0

    @pytest.mark.asyncio
    async def test_min_raw_count_ignored_for_non_dj_edges(
        self, client: AsyncClient, artist_ids: dict[str, int]
    ) -> None:
        """min_raw_count has no effect on non-djTransition edge types."""
        aid = artist_ids["Autechre"]
        resp = await client.get(
            f"/graph/artists/{aid}/neighbors",
            params={"type": "sharedPersonnel", "min_raw_count": 100},
        )
        assert resp.status_code == 200
        # shared personnel edge still returned (min_raw_count is irrelevant)
        assert len(resp.json()["neighbors"]) == 1

    @pytest.mark.asyncio
    async def test_min_raw_count_validation(
        self, client: AsyncClient, artist_ids: dict[str, int]
    ) -> None:
        """min_raw_count must be >= 1."""
        aid = artist_ids["Autechre"]
        resp = await client.get(
            f"/graph/artists/{aid}/neighbors",
            params={"type": "djTransition", "min_raw_count": 0},
        )
        assert resp.status_code == 422


class TestWikidataInfluenceNeighbors:
    @pytest.mark.asyncio
    async def test_influence_neighbors_outbound(
        self, client: AsyncClient, artist_ids: dict[str, int]
    ) -> None:
        """Autechre is influenced by Stereolab — Stereolab appears as neighbor."""
        aid = artist_ids["Autechre"]
        resp = await client.get(
            f"/graph/artists/{aid}/neighbors", params={"type": "wikidataInfluence"}
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["edge_type"] == "wikidataInfluence"
        names = [n["artist"]["canonical_name"] for n in data["neighbors"]]
        assert "Stereolab" in names

    @pytest.mark.asyncio
    async def test_influence_neighbors_inbound(
        self, client: AsyncClient, artist_ids: dict[str, int]
    ) -> None:
        """Stereolab is a target of influence edges — Autechre and Cat Power appear."""
        aid = artist_ids["Stereolab"]
        resp = await client.get(
            f"/graph/artists/{aid}/neighbors", params={"type": "wikidataInfluence"}
        )
        assert resp.status_code == 200
        names = [n["artist"]["canonical_name"] for n in resp.json()["neighbors"]]
        assert "Autechre" in names
        assert "Cat Power" in names

    @pytest.mark.asyncio
    async def test_influence_neighbors_empty(
        self, client: AsyncClient, artist_ids: dict[str, int]
    ) -> None:
        """Father John Misty has no influence edges."""
        aid = artist_ids["Father John Misty"]
        resp = await client.get(
            f"/graph/artists/{aid}/neighbors", params={"type": "wikidataInfluence"}
        )
        assert resp.status_code == 200
        assert len(resp.json()["neighbors"]) == 0

    @pytest.mark.asyncio
    async def test_influence_neighbors_detail_has_qids(
        self, client: AsyncClient, artist_ids: dict[str, int]
    ) -> None:
        """Influence neighbor detail includes source and target QIDs."""
        aid = artist_ids["Autechre"]
        resp = await client.get(
            f"/graph/artists/{aid}/neighbors", params={"type": "wikidataInfluence"}
        )
        data = resp.json()
        stereo_entry = next(
            n for n in data["neighbors"] if n["artist"]["canonical_name"] == "Stereolab"
        )
        assert stereo_entry["detail"]["source_qid"] == "Q2774"
        assert stereo_entry["detail"]["target_qid"] == "Q650826"
        assert stereo_entry["weight"] == 1.0


class TestExplain:
    @pytest.mark.asyncio
    async def test_explain_multiple_edge_types(
        self, client: AsyncClient, artist_ids: dict[str, int]
    ) -> None:
        src = artist_ids["Autechre"]
        tgt = artist_ids["Stereolab"]
        resp = await client.get(f"/graph/artists/{src}/explain/{tgt}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["source"]["canonical_name"] == "Autechre"
        assert data["target"]["canonical_name"] == "Stereolab"
        types = {r["type"] for r in data["relationships"]}
        assert "djTransition" in types
        assert "sharedPersonnel" in types
        assert "sharedStyle" in types

    @pytest.mark.asyncio
    async def test_explain_dj_transition_detail(
        self, client: AsyncClient, artist_ids: dict[str, int]
    ) -> None:
        src = artist_ids["Autechre"]
        tgt = artist_ids["Stereolab"]
        resp = await client.get(f"/graph/artists/{src}/explain/{tgt}")
        data = resp.json()
        dj_rel = next(r for r in data["relationships"] if r["type"] == "djTransition")
        assert dj_rel["weight"] == 3.0
        assert dj_rel["detail"]["raw_count"] == 5

    @pytest.mark.asyncio
    async def test_explain_no_relationship(
        self, client: AsyncClient, artist_ids: dict[str, int]
    ) -> None:
        """Autechre and Father John Misty have no direct edges."""
        src = artist_ids["Autechre"]
        tgt = artist_ids["Father John Misty"]
        resp = await client.get(f"/graph/artists/{src}/explain/{tgt}")
        assert resp.status_code == 200
        assert len(resp.json()["relationships"]) == 0

    @pytest.mark.asyncio
    async def test_explain_unknown_source_404(
        self, client: AsyncClient, artist_ids: dict[str, int]
    ) -> None:
        tgt = artist_ids["Autechre"]
        resp = await client.get(f"/graph/artists/99999/explain/{tgt}")
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_explain_unknown_target_404(
        self, client: AsyncClient, artist_ids: dict[str, int]
    ) -> None:
        src = artist_ids["Autechre"]
        resp = await client.get(f"/graph/artists/{src}/explain/99999")
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_explain_cross_reference(
        self, client: AsyncClient, artist_ids: dict[str, int]
    ) -> None:
        src = artist_ids["Stereolab"]
        tgt = artist_ids["Cat Power"]
        resp = await client.get(f"/graph/artists/{src}/explain/{tgt}")
        data = resp.json()
        types = {r["type"] for r in data["relationships"]}
        assert "crossReference" in types
        xref_rel = next(r for r in data["relationships"] if r["type"] == "crossReference")
        assert xref_rel["detail"]["comment"] == "See also"

    @pytest.mark.asyncio
    async def test_explain_wikidata_influence(
        self, client: AsyncClient, artist_ids: dict[str, int]
    ) -> None:
        """Autechre is influenced by Stereolab — explain should include wikidataInfluence."""
        src = artist_ids["Autechre"]
        tgt = artist_ids["Stereolab"]
        resp = await client.get(f"/graph/artists/{src}/explain/{tgt}")
        data = resp.json()
        types = {r["type"] for r in data["relationships"]}
        assert "wikidataInfluence" in types
        inf_rel = next(r for r in data["relationships"] if r["type"] == "wikidataInfluence")
        assert inf_rel["detail"]["source_qid"] == "Q2774"
        assert inf_rel["detail"]["target_qid"] == "Q650826"
        assert inf_rel["weight"] == 1.0


def _build_entity_store_fixture_db() -> str:
    """Create a fixture SQLite database with entity store tables and return its path."""
    path = tempfile.mktemp(suffix=".db")
    store = EntityStore(path)
    store.initialize()

    # Create entities
    entity_ae = store.get_or_create_entity("Autechre", "artist")
    store.update_entity_qid(entity_ae.id, "Q375855")
    entity_sl = store.get_or_create_entity("Stereolab", "artist")

    # Upsert artists linked to entities
    store.upsert_artist(
        "Autechre",
        genre="Electronic",
        discogs_artist_id=1240,
        entity_id=entity_ae.id,
        musicbrainz_artist_id="410c9baf-5469-44f6-9852-826524b80c61",
    )
    store.upsert_artist(
        "Stereolab",
        genre="Rock",
        entity_id=entity_sl.id,
    )
    # An alias pointing to the same entity as Autechre
    store.upsert_artist(
        "Ae",
        genre="Electronic",
        entity_id=entity_ae.id,
    )
    # An artist with no entity
    store.upsert_artist("Cat Power", genre="Rock")

    # Update stats
    store.update_artist_stats(
        "Autechre",
        ArtistStats(
            canonical_name="Autechre",
            total_plays=50,
            genre="Electronic",
            active_first_year=2004,
            active_last_year=2025,
            dj_count=15,
            request_ratio=0.1,
            show_count=40,
        ),
    )
    store.update_artist_stats(
        "Stereolab",
        ArtistStats(
            canonical_name="Stereolab",
            total_plays=30,
            genre="Rock",
            active_first_year=2003,
            active_last_year=2024,
            dj_count=10,
            request_ratio=0.05,
            show_count=25,
        ),
    )
    store.update_artist_stats(
        "Ae",
        ArtistStats(
            canonical_name="Ae",
            total_plays=5,
            genre="Electronic",
            active_first_year=2015,
            active_last_year=2020,
            dj_count=2,
            request_ratio=0.0,
            show_count=3,
        ),
    )
    store.update_artist_stats(
        "Cat Power",
        ArtistStats(
            canonical_name="Cat Power",
            total_plays=20,
            genre="Rock",
            active_first_year=2005,
            active_last_year=2023,
            dj_count=8,
            request_ratio=0.02,
            show_count=15,
        ),
    )

    # Mark reconciliation status for Autechre
    store.update_reconciliation_status(
        store.get_artist_by_name("Autechre")["id"],
        "reconciled",  # type: ignore[index]
    )

    # Set streaming IDs on Autechre's entity
    store.update_entity_streaming_ids(
        entity_ae.id,
        spotify="5bMqBjPbCOWGgWJpbAqdQq",
        apple_music="15821",
        bandcamp="autechre",
    )

    store.close()
    return path


@pytest.fixture(scope="module")
def entity_db_path() -> str:
    return _build_entity_store_fixture_db()


@pytest.fixture(scope="module")
def entity_artist_ids(entity_db_path: str) -> dict[str, int]:
    """Return a mapping of canonical_name -> id from the entity store database."""
    conn = sqlite3.connect(entity_db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT id, canonical_name FROM artist").fetchall()
    conn.close()
    return {r["canonical_name"]: r["id"] for r in rows}


@pytest.fixture(scope="module")
def entity_ids(entity_db_path: str) -> dict[str, int]:
    """Return a mapping of entity name -> entity id."""
    conn = sqlite3.connect(entity_db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT id, name FROM entity").fetchall()
    conn.close()
    return {r["name"]: r["id"] for r in rows}


@pytest_asyncio.fixture
async def entity_client(entity_db_path: str) -> AsyncClient:
    app = create_app(entity_db_path)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


class TestArtistDetail:
    @pytest.mark.asyncio
    async def test_artist_detail_with_entity(
        self, entity_client: AsyncClient, entity_artist_ids: dict[str, int]
    ) -> None:
        """Artist detail returns all fields including external IDs from joined entity."""
        aid = entity_artist_ids["Autechre"]
        resp = await entity_client.get(f"/graph/artists/{aid}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["canonical_name"] == "Autechre"
        assert data["genre"] == "Electronic"
        assert data["total_plays"] == 50
        assert data["active_first_year"] == 2004
        assert data["active_last_year"] == 2025
        assert data["dj_count"] == 15
        assert data["request_ratio"] == 0.1
        assert data["show_count"] == 40
        assert data["discogs_artist_id"] == 1240
        assert data["musicbrainz_artist_id"] == "410c9baf-5469-44f6-9852-826524b80c61"
        assert data["wikidata_qid"] == "Q375855"
        assert data["reconciliation_status"] == "reconciled"
        assert data["spotify_artist_id"] == "5bMqBjPbCOWGgWJpbAqdQq"
        assert data["apple_music_artist_id"] == "15821"
        assert data["bandcamp_id"] == "autechre"

    @pytest.mark.asyncio
    async def test_artist_detail_no_entity(
        self, entity_client: AsyncClient, entity_artist_ids: dict[str, int]
    ) -> None:
        """Artist with no entity_id returns None for entity fields."""
        aid = entity_artist_ids["Cat Power"]
        resp = await entity_client.get(f"/graph/artists/{aid}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["canonical_name"] == "Cat Power"
        assert data["entity_id"] is None
        assert data["discogs_artist_id"] is None
        assert data["musicbrainz_artist_id"] is None
        assert data["wikidata_qid"] is None
        assert data["reconciliation_status"] == "unreconciled"
        assert data["spotify_artist_id"] is None
        assert data["apple_music_artist_id"] is None
        assert data["bandcamp_id"] is None

    @pytest.mark.asyncio
    async def test_artist_detail_404(self, entity_client: AsyncClient) -> None:
        """Unknown artist ID returns 404."""
        resp = await entity_client.get("/graph/artists/99999")
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_artist_detail_old_schema(
        self, client: AsyncClient, artist_ids: dict[str, int]
    ) -> None:
        """Databases without entity columns gracefully return None for entity fields."""
        aid = artist_ids["Autechre"]
        resp = await client.get(f"/graph/artists/{aid}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["canonical_name"] == "Autechre"
        assert data["entity_id"] is None
        assert data["discogs_artist_id"] is None
        assert data["musicbrainz_artist_id"] is None
        assert data["wikidata_qid"] is None
        assert data["reconciliation_status"] == "unreconciled"
        assert data["spotify_artist_id"] is None
        assert data["apple_music_artist_id"] is None
        assert data["bandcamp_id"] is None


class TestEntityArtists:
    @pytest.mark.asyncio
    async def test_entity_artists_grouped(
        self,
        entity_client: AsyncClient,
        entity_ids: dict[str, int],
    ) -> None:
        """Entity artists endpoint returns all artists sharing the entity."""
        eid = entity_ids["Autechre"]
        resp = await entity_client.get(f"/graph/entities/{eid}/artists")
        assert resp.status_code == 200
        data = resp.json()
        assert data["entity_id"] == eid
        assert data["entity_name"] == "Autechre"
        assert data["wikidata_qid"] == "Q375855"
        names = {a["canonical_name"] for a in data["artists"]}
        assert names == {"Autechre", "Ae"}

    @pytest.mark.asyncio
    async def test_entity_artists_single(
        self,
        entity_client: AsyncClient,
        entity_ids: dict[str, int],
    ) -> None:
        """Entity with a single artist returns a list of one."""
        eid = entity_ids["Stereolab"]
        resp = await entity_client.get(f"/graph/entities/{eid}/artists")
        assert resp.status_code == 200
        data = resp.json()
        assert data["entity_id"] == eid
        assert data["entity_name"] == "Stereolab"
        assert len(data["artists"]) == 1
        assert data["artists"][0]["canonical_name"] == "Stereolab"

    @pytest.mark.asyncio
    async def test_entity_artists_404(self, entity_client: AsyncClient) -> None:
        """Unknown entity ID returns 404."""
        resp = await entity_client.get("/graph/entities/99999/artists")
        assert resp.status_code == 404
