"""Tests for POST /graph/library-artists/neighbors/batch (On Tour R3b, #354).

The endpoint answers "artists similar to X" in the Backend catalog library-artist
id keyspace (``SimilarArtist.artist_id``), reading the ``artist.wxyc_library_code_id``
mapping populated by the nightly sync (#358). Graph ids never leak into the
response — every id in and out is a library code id.

Fixtures assign ``wxyc_library_code_id`` with a direct UPDATE after export, since
``export_sqlite`` (SQL-dump mode) leaves the column present-but-NULL by design.
Library code ids are >= 1000 while graph ids autoincrement from 1, so a test can
never pass by accidentally conflating the two keyspaces.
"""

from __future__ import annotations

import sqlite3

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from semantic_index.api.app import create_app
from semantic_index.models import PmiEdge
from semantic_index.pipeline_db import PipelineDB
from semantic_index.sqlite_export import export_sqlite
from tests.conftest import make_artist_stats

ENDPOINT = "/graph/library-artists/neighbors/batch"

# Autechre is the source under test. Its djTransition neighbors, in raw_count
# order, are Stereolab (60), Broadcast (50), Boards of Canada (40), Oval (30),
# Mouse on Mars (20), Pole (10). At heat=0.0 the affinity rank is exactly this
# raw_count order. Stereolab and Broadcast are deliberately left UNMAPPED so the
# top two affinity ranks drop out — the over-fetch test hinges on this.
_CODE_MAP = {
    "Autechre": 1000,
    "Boards of Canada": 1002,
    "Oval": 1003,
    "Mouse on Mars": 1004,
    "Pole": 1005,
    "Cat Power": 2000,
    "Jessica Pratt": 2001,
}
# Stereolab, Broadcast: intentionally absent from _CODE_MAP (unmapped / NULL).


def _build_library_fixture_db(path: str) -> tuple[str, dict[str, int]]:
    """Build a fixture DB and map a subset of artists to library code ids.

    Returns ``(db_path, name_to_graph_id)``. Names absent from ``_CODE_MAP`` are
    left NULL, modeling homonym/raw names #358 excludes from the mapping.
    """
    names = [
        "Autechre",
        "Stereolab",
        "Broadcast",
        "Boards of Canada",
        "Oval",
        "Mouse on Mars",
        "Pole",
        "Cat Power",
        "Jessica Pratt",
    ]
    stats = {n: make_artist_stats(n, total_plays=50, genre="Electronic") for n in names}
    pmi_edges = [
        PmiEdge(source="Autechre", target="Stereolab", raw_count=60, pmi=6.0),
        PmiEdge(source="Autechre", target="Broadcast", raw_count=50, pmi=5.0),
        PmiEdge(source="Autechre", target="Boards of Canada", raw_count=40, pmi=4.0),
        PmiEdge(source="Autechre", target="Oval", raw_count=30, pmi=3.0),
        PmiEdge(source="Autechre", target="Mouse on Mars", raw_count=20, pmi=2.0),
        PmiEdge(source="Autechre", target="Pole", raw_count=10, pmi=1.0),
        PmiEdge(source="Cat Power", target="Jessica Pratt", raw_count=5, pmi=1.5),
    ]
    export_sqlite(path, artist_stats=stats, pmi_edges=pmi_edges, xref_edges=[], min_count=1)

    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    name_to_graph = {
        r["canonical_name"]: r["id"] for r in conn.execute("SELECT id, canonical_name FROM artist")
    }
    for name, code in _CODE_MAP.items():
        conn.execute(
            "UPDATE artist SET wxyc_library_code_id = ? WHERE canonical_name = ?",
            (code, name),
        )
    conn.commit()
    conn.close()
    return path, name_to_graph


@pytest.fixture(scope="module")
def _fixture(tmp_path_factory: pytest.TempPathFactory) -> tuple[str, dict[str, int]]:
    path = str(tmp_path_factory.mktemp("library_neighbors") / "graph.db")
    return _build_library_fixture_db(path)


@pytest.fixture(scope="module")
def db_path(_fixture: tuple[str, dict[str, int]]) -> str:
    return _fixture[0]


@pytest.fixture(scope="module")
def name_to_graph(_fixture: tuple[str, dict[str, int]]) -> dict[str, int]:
    return _fixture[1]


@pytest_asyncio.fixture
async def client(db_path: str) -> AsyncClient:
    app = create_app(db_path)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


class TestInputDirection:
    """Requested library ids resolve to graph neighbors; unmapped ids → empty."""

    @pytest.mark.asyncio
    async def test_mapped_source_returns_neighbors(self, client: AsyncClient) -> None:
        resp = await client.post(
            ENDPOINT, json={"library_artist_ids": [2000], "limit": 20, "heat": 0.5}
        )
        assert resp.status_code == 200
        results = resp.json()["results"]
        assert "2000" in results
        # Cat Power (2000) -> Jessica Pratt (2001), the only mapped neighbor.
        assert [n["artist_id"] for n in results["2000"]] == [2001]
        assert results["2000"][0]["weight"] > 0

    @pytest.mark.asyncio
    async def test_unknown_library_id_present_but_empty(self, client: AsyncClient) -> None:
        resp = await client.post(ENDPOINT, json={"library_artist_ids": [999999], "limit": 20})
        assert resp.status_code == 200
        # Present in results (never dropped), but empty — unknown to the mapping.
        assert resp.json()["results"] == {"999999": []}

    @pytest.mark.asyncio
    async def test_ambiguous_and_unmapped_ids_return_empty(self, client: AsyncClient) -> None:
        # 4999 models a homonym code #358 excluded from the mapping (no graph
        # artist carries it); it behaves identically to an unknown id.
        resp = await client.post(ENDPOINT, json={"library_artist_ids": [2000, 4999], "limit": 20})
        assert resp.status_code == 200
        results = resp.json()["results"]
        assert results["4999"] == []
        assert results["2000"]  # the mapped one still resolves
        # Every requested id is present in results.
        assert set(results.keys()) == {"2000", "4999"}


class TestOutputDirection:
    """Neighbor graph ids translate to library ids; unmapped neighbors drop."""

    @pytest.mark.asyncio
    async def test_neighbors_are_library_ids_never_graph_ids(
        self, client: AsyncClient, name_to_graph: dict[str, int]
    ) -> None:
        resp = await client.post(
            ENDPOINT, json={"library_artist_ids": [1000], "limit": 20, "heat": 0.0}
        )
        assert resp.status_code == 200
        got = [n["artist_id"] for n in resp.json()["results"]["1000"]]
        # Stereolab and Broadcast are unmapped and drop out entirely; the rest
        # translate to their library code ids in weight order.
        assert got == [1002, 1003, 1004, 1005]
        # The unmapped neighbors' GRAPH ids must never leak into the response.
        assert name_to_graph["Stereolab"] not in got
        assert name_to_graph["Broadcast"] not in got

    @pytest.mark.asyncio
    async def test_over_fetch_refills_past_unmapped_top_neighbors(
        self, client: AsyncClient
    ) -> None:
        """The 2x over-fetch is what keeps a short list full after the drop.

        Autechre's top-2 affinity neighbors (Stereolab, Broadcast) are unmapped.
        With limit=3 and NO over-fetch, only rank-3 (Boards) survives the drop
        for a length-1 list. The 2x over-fetch pulls ranks 1-6, drops the two
        unmapped, and refills to a full 3. This is the test that fails if someone
        simplifies the fixed 2x away.
        """
        resp = await client.post(
            ENDPOINT, json={"library_artist_ids": [1000], "limit": 3, "heat": 0.0}
        )
        assert resp.status_code == 200
        got = [n["artist_id"] for n in resp.json()["results"]["1000"]]
        assert got == [1002, 1003, 1004]

    @pytest.mark.asyncio
    async def test_weights_descending(self, client: AsyncClient) -> None:
        resp = await client.post(
            ENDPOINT, json={"library_artist_ids": [1000], "limit": 20, "heat": 0.0}
        )
        weights = [n["weight"] for n in resp.json()["results"]["1000"]]
        assert weights == sorted(weights, reverse=True)


class TestEnvelope:
    """Batch cap, empty input, dedup, and bound enforcement."""

    @pytest.mark.asyncio
    async def test_over_100_ids_returns_422(self, client: AsyncClient) -> None:
        resp = await client.post(
            ENDPOINT, json={"library_artist_ids": list(range(101)), "limit": 20}
        )
        assert resp.status_code == 422  # structured overflow, never silent truncation

    @pytest.mark.asyncio
    async def test_exactly_100_ids_ok(self, client: AsyncClient) -> None:
        resp = await client.post(
            ENDPOINT, json={"library_artist_ids": list(range(1, 101)), "limit": 5}
        )
        assert resp.status_code == 200
        assert len(resp.json()["results"]) == 100

    @pytest.mark.asyncio
    async def test_empty_input_returns_empty_results(self, client: AsyncClient) -> None:
        resp = await client.post(ENDPOINT, json={"library_artist_ids": [], "limit": 20})
        assert resp.status_code == 200
        assert resp.json()["results"] == {}

    @pytest.mark.asyncio
    async def test_duplicate_ids_collapse(self, client: AsyncClient) -> None:
        resp = await client.post(
            ENDPOINT, json={"library_artist_ids": [2000, 2000, 2000], "limit": 20}
        )
        assert resp.status_code == 200
        assert list(resp.json()["results"].keys()) == ["2000"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("limit", [0, 101])
    async def test_limit_out_of_bounds_422(self, client: AsyncClient, limit: int) -> None:
        resp = await client.post(ENDPOINT, json={"library_artist_ids": [2000], "limit": limit})
        assert resp.status_code == 422

    @pytest.mark.asyncio
    @pytest.mark.parametrize("heat", [-0.1, 1.1])
    async def test_heat_out_of_bounds_422(self, client: AsyncClient, heat: float) -> None:
        resp = await client.post(
            ENDPOINT, json={"library_artist_ids": [2000], "limit": 20, "heat": heat}
        )
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_heat_defaults_when_omitted(self, client: AsyncClient) -> None:
        resp = await client.post(ENDPOINT, json={"library_artist_ids": [2000]})
        assert resp.status_code == 200
        assert resp.json()["results"]["2000"]  # default heat still resolves neighbors


class TestSimilarArtistContract:
    """Response items are the shared SimilarArtist schema, verbatim."""

    @pytest.mark.asyncio
    async def test_response_items_have_exactly_similar_artist_fields(
        self, client: AsyncClient
    ) -> None:
        resp = await client.post(
            ENDPOINT, json={"library_artist_ids": [1000], "limit": 3, "heat": 0.0}
        )
        neighbors = resp.json()["results"]["1000"]
        assert neighbors  # non-empty, so the assertion below actually runs
        for item in neighbors:
            assert set(item.keys()) == {"artist_id", "weight"}
            assert isinstance(item["artist_id"], int)
            assert isinstance(item["weight"], int | float)

    def test_generated_similar_artist_field_names(self) -> None:
        # Pins the contract here so drift fails in this repo, not in the
        # Backend-Service consumer. Fails if a regen renames/drops a field.
        from generated.api_models import SimilarArtist

        assert set(SimilarArtist.model_fields.keys()) == {"artist_id", "weight"}


def _build_entity_primary_path_db(path: str) -> int:
    """Build a pipeline-DB fixture whose entity table makes ``_get_artist_detail``
    take its PRIMARY (entity-JOIN) branch — the path production's served DB uses —
    with ``wxyc_library_code_id`` set, so the primary read is covered. The SQL-dump
    fixtures above have no entity table and always fall back to
    ``_fetch_library_code_id`` instead. Returns Autechre's graph id.
    """
    db = PipelineDB(path)
    db.initialize()
    db.upsert_artist("Autechre", genre="Electronic", wxyc_library_code_id=1000)
    aid = db._conn.execute("SELECT id FROM artist WHERE canonical_name = 'Autechre'").fetchone()[0]
    db.close()
    return int(aid)


class TestArtistDetailLibraryCodeId:
    """ArtistDetail exposes wxyc_library_code_id for browser spot-checks."""

    @pytest.mark.asyncio
    async def test_mapped_artist_detail_reports_code_id(
        self, client: AsyncClient, name_to_graph: dict[str, int]
    ) -> None:
        resp = await client.get(f"/graph/artists/{name_to_graph['Autechre']}")
        assert resp.status_code == 200
        assert resp.json()["wxyc_library_code_id"] == 1000

    @pytest.mark.asyncio
    async def test_unmapped_artist_detail_reports_null(
        self, client: AsyncClient, name_to_graph: dict[str, int]
    ) -> None:
        resp = await client.get(f"/graph/artists/{name_to_graph['Stereolab']}")
        assert resp.status_code == 200
        assert resp.json()["wxyc_library_code_id"] is None

    @pytest.mark.asyncio
    async def test_primary_path_reports_code_id(self, tmp_path) -> None:
        """The entity-JOIN branch reads ``a.wxyc_library_code_id`` directly — the
        production served DB has an entity table, so this, not the fallback the other
        two tests hit, is the live read path.
        """
        path = str(tmp_path / "entity_primary.db")
        aid = _build_entity_primary_path_db(path)
        app = create_app(path)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get(f"/graph/artists/{aid}")
        assert resp.status_code == 200
        assert resp.json()["wxyc_library_code_id"] == 1000


def _build_pre_358_db(path: str) -> int:
    """Export a fixture DB, then drop the #358 column and its index to model a served
    DB that predates the mapping. Returns Autechre's graph id.
    """
    export_sqlite(
        path,
        artist_stats={"Autechre": make_artist_stats("Autechre")},
        pmi_edges=[],
        xref_edges=[],
        min_count=1,
    )
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    aid = conn.execute("SELECT id FROM artist WHERE canonical_name = 'Autechre'").fetchone()["id"]
    conn.execute("DROP INDEX IF EXISTS idx_artist_library_code")
    conn.execute("ALTER TABLE artist DROP COLUMN wxyc_library_code_id")
    conn.commit()
    conn.close()
    return int(aid)


class TestDeployOrderDegradation:
    """A pre-#358 served DB has no wxyc_library_code_id column yet."""

    @pytest.mark.asyncio
    async def test_missing_column_returns_empty_not_500(self, tmp_path) -> None:
        path = str(tmp_path / "old_schema.db")
        _build_pre_358_db(path)

        app = create_app(path)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post(ENDPOINT, json={"library_artist_ids": [1000], "limit": 5})
        assert resp.status_code == 200
        assert resp.json()["results"] == {"1000": []}
        # The degrade branch yields no station-play signal either (#369).
        assert resp.json()["source_plays"] == {}

    @pytest.mark.asyncio
    async def test_artist_detail_survives_missing_column(self, tmp_path) -> None:
        """GET /graph/artists/{id} degrades wxyc_library_code_id to null rather than
        500 when the served DB predates #358 — the _fetch_library_code_id guard.
        """
        path = str(tmp_path / "old_schema.db")
        aid = _build_pre_358_db(path)

        app = create_app(path)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get(f"/graph/artists/{aid}")
        assert resp.status_code == 200
        assert resp.json()["wxyc_library_code_id"] is None


def _build_source_plays_db(path: str) -> None:
    """Build a fixture whose mapped source artists carry DISTINCT total_plays, so
    a source_plays value can't pass by coincidentally matching a shared count.
    Father John Misty (code 3000) has 812 plays, Stereolab (code 3001) has 337.
    """
    stats = {
        "Father John Misty": make_artist_stats("Father John Misty", total_plays=812),
        "Stereolab": make_artist_stats("Stereolab", total_plays=337),
    }
    export_sqlite(path, artist_stats=stats, pmi_edges=[], xref_edges=[], min_count=1)

    conn = sqlite3.connect(path)
    conn.execute(
        "UPDATE artist SET wxyc_library_code_id = 3000 WHERE canonical_name = 'Father John Misty'"
    )
    conn.execute("UPDATE artist SET wxyc_library_code_id = 3001 WHERE canonical_name = 'Stereolab'")
    conn.commit()
    conn.close()


class TestSourcePlays:
    """source_plays carries each mapped source artist's own total_plays (#369)."""

    @pytest_asyncio.fixture
    async def sp_client(self, tmp_path) -> AsyncClient:
        path = str(tmp_path / "source_plays.db")
        _build_source_plays_db(path)
        app = create_app(path)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac

    @pytest.mark.asyncio
    async def test_mapped_ids_carry_total_plays_unmapped_absent(
        self, sp_client: AsyncClient
    ) -> None:
        # 3000/3001 map to graph artists; 999999 is unknown to the mapping.
        resp = await sp_client.post(
            ENDPOINT, json={"library_artist_ids": [3000, 3001, 999999], "limit": 20}
        )
        assert resp.status_code == 200
        body = resp.json()
        # Each mapped id carries its OWN total_plays, keyed by the request string.
        assert body["source_plays"] == {"3000": 812, "3001": 337}
        # Unknown id: present in results (empty list), ABSENT from source_plays —
        # the two maps diverge on unmapped ids by design.
        assert body["results"]["999999"] == []
        assert "999999" not in body["source_plays"]

    @pytest.mark.asyncio
    async def test_results_shape_unchanged_by_source_plays(self, sp_client: AsyncClient) -> None:
        # source_plays is additive: results still carries every requested id.
        resp = await sp_client.post(
            ENDPOINT, json={"library_artist_ids": [3000, 3001, 999999], "limit": 20}
        )
        assert resp.status_code == 200
        assert set(resp.json()["results"].keys()) == {"3000", "3001", "999999"}

    @pytest.mark.asyncio
    async def test_empty_input_yields_empty_source_plays(self, sp_client: AsyncClient) -> None:
        resp = await sp_client.post(ENDPOINT, json={"library_artist_ids": [], "limit": 20})
        assert resp.status_code == 200
        assert resp.json()["source_plays"] == {}
