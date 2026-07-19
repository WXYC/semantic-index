"""Tests for POST /graph/discogs-artists/neighbors/batch (On Tour, #367).

Sibling of ``test_api_library_neighbors`` (#354): the SOURCE headliner is keyed by
its external ``artist.discogs_artist_id`` instead of the library code, but the
NEIGHBORS returned are still in the Backend catalog library-artist id keyspace
(``SimilarArtist.artist_id``, the ``wxyc_library_code_id`` mapping). So a fixture
sets ``discogs_artist_id`` on the sources and ``wxyc_library_code_id`` on the
neighbors, via direct UPDATE after ``export_sqlite`` (SQL-dump mode leaves both
columns present-but-NULL by design).

The one real divergence from the library sibling: ``idx_artist_discogs`` is a
PLAIN partial index, not UNIQUE like ``idx_artist_library_code``. Homonym-collapse
can therefore put >=2 graph nodes on one Discogs id, and the endpoint must DROP
such an id (return ``[]``), NOT collapse to an arbitrary winner. ``Homonym One`` /
``Homonym Two`` share Discogs id 900009 to pin that contract.

Keyspaces never collide by accident: Discogs source ids are >= 900000, library
code ids >= 1000, graph ids autoincrement from 1.
"""

from __future__ import annotations

import sqlite3

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from semantic_index.api.app import create_app
from semantic_index.models import PmiEdge
from semantic_index.sqlite_export import export_sqlite
from tests.conftest import make_artist_stats

ENDPOINT = "/graph/discogs-artists/neighbors/batch"

# Autechre (Discogs 900000) is the source under test. Its djTransition neighbors,
# in raw_count order, are Stereolab (60), Broadcast (50), Boards of Canada (40),
# Oval (30), Mouse on Mars (20), Pole (10). At heat=0.0 the affinity rank is
# exactly this raw_count order. Stereolab and Broadcast are deliberately left
# UNMAPPED (no library code) so the top two affinity ranks drop out — the
# over-fetch test hinges on this.
_DISCOGS_MAP = {
    "Autechre": 900000,
    "Cat Power": 900001,
    "Homonym One": 900009,
    "Homonym Two": 900009,  # SAME id as Homonym One → ambiguous, must return []
}
_CODE_MAP = {
    "Boards of Canada": 1002,
    "Oval": 1003,
    "Mouse on Mars": 1004,
    "Pole": 1005,
    "Jessica Pratt": 2001,
}
# Stereolab, Broadcast: intentionally in neither map (unmapped neighbors / NULL).


def _build_discogs_fixture_db(path: str) -> tuple[str, dict[str, int]]:
    """Build a fixture DB, keying sources by Discogs id and neighbors by code id.

    Returns ``(db_path, name_to_graph_id)``.
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
        "Homonym One",
        "Homonym Two",
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
        # Both nodes sharing the ambiguous Discogs id 900009 carry a DISTINCT mapped
        # neighbor (Homonym One -> Boards of Canada 1002, Homonym Two -> Oval 1003),
        # so a naive "pick one node" impl returns a non-empty list whichever node it
        # lands on — only the correct ambiguity DROP yields []. Giving Homonym Two a
        # neighbor is load-bearing: without it, the rowid-order dict-overwrite would
        # land on the edgeless Homonym Two and return [] anyway, letting a regression
        # that deletes the `GROUP BY ... HAVING COUNT(*) = 1` guard pass unnoticed.
        PmiEdge(source="Homonym One", target="Boards of Canada", raw_count=15, pmi=2.5),
        PmiEdge(source="Homonym Two", target="Oval", raw_count=15, pmi=2.5),
    ]
    export_sqlite(path, artist_stats=stats, pmi_edges=pmi_edges, xref_edges=[], min_count=1)

    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    name_to_graph = {
        r["canonical_name"]: r["id"] for r in conn.execute("SELECT id, canonical_name FROM artist")
    }
    for name, discogs_id in _DISCOGS_MAP.items():
        conn.execute(
            "UPDATE artist SET discogs_artist_id = ? WHERE canonical_name = ?",
            (discogs_id, name),
        )
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
    path = str(tmp_path_factory.mktemp("discogs_neighbors") / "graph.db")
    return _build_discogs_fixture_db(path)


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
    """Requested Discogs ids resolve to graph neighbors; unknown ids → empty."""

    @pytest.mark.asyncio
    async def test_mapped_source_returns_neighbors(self, client: AsyncClient) -> None:
        resp = await client.post(
            ENDPOINT, json={"discogs_artist_ids": [900001], "limit": 20, "heat": 0.5}
        )
        assert resp.status_code == 200
        results = resp.json()["results"]
        assert "900001" in results
        # Cat Power (Discogs 900001) -> Jessica Pratt (code 2001), the only mapped neighbor.
        assert [n["artist_id"] for n in results["900001"]] == [2001]
        assert results["900001"][0]["weight"] > 0

    @pytest.mark.asyncio
    async def test_unknown_discogs_id_present_but_empty(self, client: AsyncClient) -> None:
        resp = await client.post(ENDPOINT, json={"discogs_artist_ids": [999999], "limit": 20})
        assert resp.status_code == 200
        # Present in results (never dropped), but empty — no graph node carries it.
        assert resp.json()["results"] == {"999999": []}


class TestAmbiguousDiscogsId:
    """A Discogs id borne by >=2 graph nodes is dropped, not collapsed."""

    @pytest.mark.asyncio
    async def test_ambiguous_discogs_id_returns_empty(self, client: AsyncClient) -> None:
        # 900009 is carried by both Homonym One and Homonym Two, each with a distinct
        # mapped neighbor, so a naive "pick one node" impl would return [1002] or
        # [1003] whichever node it lands on. The contract requires [] — ambiguity is
        # unresolvable, so the id is dropped rather than collapsed to a winner.
        resp = await client.post(ENDPOINT, json={"discogs_artist_ids": [900009], "limit": 20})
        assert resp.status_code == 200
        results = resp.json()["results"]
        assert results == {"900009": []}

    @pytest.mark.asyncio
    async def test_ambiguous_id_does_not_poison_sibling_ids(self, client: AsyncClient) -> None:
        # An ambiguous id in the batch must not affect the unambiguous ones.
        resp = await client.post(
            ENDPOINT, json={"discogs_artist_ids": [900009, 900001], "limit": 20}
        )
        assert resp.status_code == 200
        results = resp.json()["results"]
        assert results["900009"] == []
        assert [n["artist_id"] for n in results["900001"]] == [2001]
        assert set(results.keys()) == {"900009", "900001"}


class TestOutputDirection:
    """Neighbor graph ids translate to library ids; unmapped neighbors drop."""

    @pytest.mark.asyncio
    async def test_neighbors_are_library_ids_never_graph_ids(
        self, client: AsyncClient, name_to_graph: dict[str, int]
    ) -> None:
        resp = await client.post(
            ENDPOINT, json={"discogs_artist_ids": [900000], "limit": 20, "heat": 0.0}
        )
        assert resp.status_code == 200
        got = [n["artist_id"] for n in resp.json()["results"]["900000"]]
        # Stereolab and Broadcast are unmapped and drop out entirely; the rest
        # translate to their library code ids in weight order.
        assert got == [1002, 1003, 1004, 1005]
        # The unmapped neighbors' GRAPH ids must never leak into the response.
        assert name_to_graph["Stereolab"] not in got
        assert name_to_graph["Broadcast"] not in got
        # Nor may the SOURCE's own Discogs id ever appear as a neighbor id.
        assert 900000 not in got

    @pytest.mark.asyncio
    async def test_over_fetch_refills_past_unmapped_top_neighbors(
        self, client: AsyncClient
    ) -> None:
        """The 2x over-fetch keeps a short list full after the unmapped-top drop.

        Autechre's top-2 affinity neighbors (Stereolab, Broadcast) are unmapped.
        With limit=3 and NO over-fetch, only rank-3 (Boards) survives for a
        length-1 list. The 2x over-fetch pulls ranks 1-6, drops the two unmapped,
        and refills to a full 3. Fails if someone simplifies the fixed 2x away.
        """
        resp = await client.post(
            ENDPOINT, json={"discogs_artist_ids": [900000], "limit": 3, "heat": 0.0}
        )
        assert resp.status_code == 200
        got = [n["artist_id"] for n in resp.json()["results"]["900000"]]
        assert got == [1002, 1003, 1004]

    @pytest.mark.asyncio
    async def test_weights_descending(self, client: AsyncClient) -> None:
        resp = await client.post(
            ENDPOINT, json={"discogs_artist_ids": [900000], "limit": 20, "heat": 0.0}
        )
        weights = [n["weight"] for n in resp.json()["results"]["900000"]]
        assert weights == sorted(weights, reverse=True)


class TestEnvelope:
    """Batch cap, empty input, dedup, and bound enforcement."""

    @pytest.mark.asyncio
    async def test_over_100_ids_returns_422(self, client: AsyncClient) -> None:
        resp = await client.post(
            ENDPOINT, json={"discogs_artist_ids": list(range(101)), "limit": 20}
        )
        assert resp.status_code == 422  # structured overflow, never silent truncation

    @pytest.mark.asyncio
    async def test_exactly_100_ids_ok(self, client: AsyncClient) -> None:
        resp = await client.post(
            ENDPOINT, json={"discogs_artist_ids": list(range(1, 101)), "limit": 5}
        )
        assert resp.status_code == 200
        assert len(resp.json()["results"]) == 100

    @pytest.mark.asyncio
    async def test_empty_input_returns_empty_results(self, client: AsyncClient) -> None:
        resp = await client.post(ENDPOINT, json={"discogs_artist_ids": [], "limit": 20})
        assert resp.status_code == 200
        assert resp.json()["results"] == {}

    @pytest.mark.asyncio
    async def test_duplicate_ids_collapse(self, client: AsyncClient) -> None:
        resp = await client.post(
            ENDPOINT, json={"discogs_artist_ids": [900001, 900001, 900001], "limit": 20}
        )
        assert resp.status_code == 200
        assert list(resp.json()["results"].keys()) == ["900001"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("limit", [0, 101])
    async def test_limit_out_of_bounds_422(self, client: AsyncClient, limit: int) -> None:
        resp = await client.post(ENDPOINT, json={"discogs_artist_ids": [900001], "limit": limit})
        assert resp.status_code == 422

    @pytest.mark.asyncio
    @pytest.mark.parametrize("heat", [-0.1, 1.1])
    async def test_heat_out_of_bounds_422(self, client: AsyncClient, heat: float) -> None:
        resp = await client.post(
            ENDPOINT, json={"discogs_artist_ids": [900001], "limit": 20, "heat": heat}
        )
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_heat_defaults_when_omitted(self, client: AsyncClient) -> None:
        resp = await client.post(ENDPOINT, json={"discogs_artist_ids": [900001]})
        assert resp.status_code == 200
        assert resp.json()["results"]["900001"]  # default heat still resolves neighbors


class TestSimilarArtistContract:
    """Response items are the shared SimilarArtist schema, verbatim."""

    @pytest.mark.asyncio
    async def test_response_items_have_exactly_similar_artist_fields(
        self, client: AsyncClient
    ) -> None:
        resp = await client.post(
            ENDPOINT, json={"discogs_artist_ids": [900000], "limit": 3, "heat": 0.0}
        )
        neighbors = resp.json()["results"]["900000"]
        assert neighbors  # non-empty, so the assertion below actually runs
        for item in neighbors:
            assert set(item.keys()) == {"artist_id", "weight"}
            assert isinstance(item["artist_id"], int)
            assert isinstance(item["weight"], int | float)


def _build_pre_358_db(path: str) -> None:
    """Export a fixture with a Discogs-keyed source, then drop the #358 mapping
    column + index to model a served DB that predates library-code translation.
    The source Discogs lookup still resolves, but no neighbor can be translated.
    """
    stats = {
        "Autechre": make_artist_stats("Autechre", genre="Electronic"),
        "Boards of Canada": make_artist_stats("Boards of Canada", genre="Electronic"),
    }
    export_sqlite(
        path,
        artist_stats=stats,
        pmi_edges=[PmiEdge(source="Autechre", target="Boards of Canada", raw_count=40, pmi=4.0)],
        xref_edges=[],
        min_count=1,
    )
    conn = sqlite3.connect(path)
    conn.execute("UPDATE artist SET discogs_artist_id = 900000 WHERE canonical_name = 'Autechre'")
    conn.execute("DROP INDEX IF EXISTS idx_artist_library_code")
    conn.execute("ALTER TABLE artist DROP COLUMN wxyc_library_code_id")
    conn.commit()
    conn.close()


def _build_pre_discogs_column_db(path: str) -> None:
    """Export a fixture, then drop the discogs_artist_id column + its index to model a
    served DB so old it predates the Discogs reverse-lookup KEY itself. The SOURCE
    lookup can't run, so every requested id degrades to empty — exercising the FIRST
    OperationalError guard (the reverse discogs query), which ``_build_pre_358_db``
    leaves untouched because it keeps discogs_artist_id and only drops the mapping.
    """
    stats = {
        "Autechre": make_artist_stats("Autechre", genre="Electronic"),
        "Boards of Canada": make_artist_stats("Boards of Canada", genre="Electronic"),
    }
    export_sqlite(
        path,
        artist_stats=stats,
        pmi_edges=[PmiEdge(source="Autechre", target="Boards of Canada", raw_count=40, pmi=4.0)],
        xref_edges=[],
        min_count=1,
    )
    conn = sqlite3.connect(path)
    conn.execute("DROP INDEX IF EXISTS idx_artist_discogs")
    conn.execute("ALTER TABLE artist DROP COLUMN discogs_artist_id")
    conn.commit()
    conn.close()


class TestDeployOrderDegradation:
    """A pre-#358 served DB has no wxyc_library_code_id column to translate into."""

    @pytest.mark.asyncio
    async def test_missing_mapping_column_returns_empty_not_500(self, tmp_path) -> None:
        path = str(tmp_path / "old_schema.db")
        _build_pre_358_db(path)

        app = create_app(path)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post(ENDPOINT, json={"discogs_artist_ids": [900000], "limit": 5})
        # Source resolves via discogs_artist_id, but neighbor translation cannot
        # run without wxyc_library_code_id — degrade to empty, never 500.
        assert resp.status_code == 200
        assert resp.json()["results"] == {"900000": []}

    @pytest.mark.asyncio
    async def test_missing_discogs_column_returns_empty_not_500(self, tmp_path) -> None:
        path = str(tmp_path / "pre_discogs.db")
        _build_pre_discogs_column_db(path)

        app = create_app(path)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post(ENDPOINT, json={"discogs_artist_ids": [900000], "limit": 5})
        # The reverse discogs_artist_id lookup can't run on a DB without the column;
        # the source-query OperationalError guard degrades it to empty, never 500.
        assert resp.status_code == 200
        assert resp.json()["results"] == {"900000": []}
