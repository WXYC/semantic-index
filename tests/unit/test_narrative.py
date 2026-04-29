"""Tests for the narrative endpoint — LLM-generated edge explanations."""

from __future__ import annotations

import json
import math
import sqlite3
import tempfile
from unittest.mock import MagicMock

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from semantic_index.api.app import create_app
from semantic_index.api.narrative import (
    _INSUFFICIENT_SIGNAL_NARRATIVE,
    _SHARED_NEIGHBORS_TOP_K,
    _STYLES_TOP_N,
    _rank_shared_neighbors_by_aa,
)
from semantic_index.facet_export import export_facet_tables
from semantic_index.models import ArtistStats, PmiEdge, SharedPersonnelEdge, SharedStyleEdge
from semantic_index.sqlite_export import export_sqlite
from tests.conftest import make_adjacency_pair, make_resolved_entry

# -- Timestamps (Unix ms) --
JAN_15_2024 = 1705276800000
JUL_10_2024 = 1720569600000
JAN_20_2024 = 1705708800000

MOCK_NARRATIVE = "WXYC DJs frequently pair Autechre with Stereolab, reflecting their shared experimental electronic roots."


def _mock_anthropic_client(response_text: str = MOCK_NARRATIVE) -> MagicMock:
    """Create a mock Anthropic client that returns a fixed narrative."""
    client = MagicMock()
    mock_message = MagicMock()
    mock_block = MagicMock()
    mock_block.text = response_text
    mock_message.content = [mock_block]
    client.messages.create.return_value = mock_message
    return client


def _build_narrative_fixture_db() -> str:
    """Build a fixture DB with base tables, edges, and facet tables."""
    path = tempfile.mktemp(suffix=".db")
    stats = {
        "Autechre": ArtistStats(
            canonical_name="Autechre",
            total_plays=50,
            genre="Electronic",
        ),
        "Stereolab": ArtistStats(
            canonical_name="Stereolab",
            total_plays=30,
            genre="Rock",
        ),
        "Cat Power": ArtistStats(
            canonical_name="Cat Power",
            total_plays=20,
            genre="Rock",
        ),
    }
    pmi_edges = [
        PmiEdge(source="Autechre", target="Stereolab", raw_count=5, pmi=3.0),
        PmiEdge(source="Autechre", target="Cat Power", raw_count=2, pmi=1.5),
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
    export_sqlite(
        path,
        artist_stats=stats,
        pmi_edges=pmi_edges,
        xref_edges=[],
        min_count=1,
        shared_personnel_edges=shared_personnel,
        shared_style_edges=shared_styles,
    )

    # Add artist style tags (Discogs-derived)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    name_to_id_for_styles = {
        r["canonical_name"]: r["id"]
        for r in conn.execute("SELECT id, canonical_name FROM artist").fetchall()
    }
    style_data = [
        (name_to_id_for_styles["Autechre"], "IDM"),
        (name_to_id_for_styles["Autechre"], "Abstract"),
        (name_to_id_for_styles["Stereolab"], "Post-Rock"),
        (name_to_id_for_styles["Stereolab"], "Krautrock"),
    ]
    conn.executemany(
        "INSERT OR IGNORE INTO artist_style (artist_id, style_tag) VALUES (?, ?)",
        style_data,
    )
    conn.commit()
    conn.close()

    # Add facet tables
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    name_to_id = {
        r["canonical_name"]: r["id"]
        for r in conn.execute("SELECT id, canonical_name FROM artist").fetchall()
    }
    conn.close()

    resolved_entries = [
        make_resolved_entry(
            id=1, canonical_name="Autechre", show_id=1, sequence=1, start_time=JAN_15_2024
        ),
        make_resolved_entry(
            id=2, canonical_name="Stereolab", show_id=1, sequence=2, start_time=JAN_15_2024
        ),
        make_resolved_entry(
            id=3, canonical_name="Autechre", show_id=2, sequence=1, start_time=JUL_10_2024
        ),
        make_resolved_entry(
            id=4, canonical_name="Cat Power", show_id=2, sequence=2, start_time=JUL_10_2024
        ),
    ]
    pairs = [
        make_adjacency_pair(source="Autechre", target="Stereolab", show_id=1),
        make_adjacency_pair(source="Autechre", target="Cat Power", show_id=2),
    ]
    export_facet_tables(
        db_path=path,
        resolved_entries=resolved_entries,
        name_to_id=name_to_id,
        show_to_dj={1: 42, 2: 42},
        show_dj_names={1: "DJ Cool", 2: "DJ Cool"},
        adjacency_pairs=pairs,
    )
    return path


@pytest.fixture(scope="module")
def narrative_db_path() -> str:
    return _build_narrative_fixture_db()


@pytest.fixture(scope="module")
def narrative_artist_ids(narrative_db_path: str) -> dict[str, int]:
    conn = sqlite3.connect(narrative_db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT id, canonical_name FROM artist").fetchall()
    conn.close()
    return {r["canonical_name"]: r["id"] for r in rows}


def _clear_narrative_cache(db_path: str) -> None:
    """Delete the sidecar cache DB so each test starts fresh."""
    import os

    cache_path = db_path + ".narrative-cache.db"
    for suffix in ("", "-wal", "-shm"):
        try:
            os.unlink(cache_path + suffix)
        except FileNotFoundError:
            pass


@pytest.fixture(autouse=True)
def _disable_aa_threshold(monkeypatch: pytest.MonkeyPatch) -> None:
    """Disable the AA-score threshold for default narrative tests.

    The shared fixture only has two adjacency pairs and no shared neighbors,
    so every pair would otherwise short-circuit on ``insufficient_signal`` and
    skip the mock LLM. Tests in ``TestAaThreshold`` re-set the env var to
    exercise the threshold logic explicitly.
    """
    monkeypatch.setenv("NARRATIVE_MIN_AA_SCORE", "0")


@pytest_asyncio.fixture
async def client(narrative_db_path: str) -> AsyncClient:
    _clear_narrative_cache(narrative_db_path)
    mock_client = _mock_anthropic_client()
    app = create_app(narrative_db_path, anthropic_api_key="test-key")
    app.state.anthropic_client = mock_client
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest_asyncio.fixture
async def client_no_key(narrative_db_path: str) -> AsyncClient:
    _clear_narrative_cache(narrative_db_path)
    app = create_app(narrative_db_path)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


class TestNarrativeCaching:
    @pytest.mark.asyncio
    async def test_cache_miss_calls_llm(
        self, client: AsyncClient, narrative_artist_ids: dict[str, int]
    ) -> None:
        ae_id = narrative_artist_ids["Autechre"]
        sl_id = narrative_artist_ids["Stereolab"]
        resp = await client.get(f"/graph/artists/{ae_id}/explain/{sl_id}/narrative")
        assert resp.status_code == 200
        data = resp.json()
        assert data["narrative"] == MOCK_NARRATIVE
        assert data["cached"] is False

    @pytest.mark.asyncio
    async def test_cache_hit_skips_llm(
        self, client: AsyncClient, narrative_artist_ids: dict[str, int]
    ) -> None:
        ae_id = narrative_artist_ids["Autechre"]
        sl_id = narrative_artist_ids["Stereolab"]
        # First call populates cache
        resp1 = await client.get(f"/graph/artists/{ae_id}/explain/{sl_id}/narrative")
        assert resp1.status_code == 200
        # Second call should be cached
        resp2 = await client.get(f"/graph/artists/{ae_id}/explain/{sl_id}/narrative")
        assert resp2.status_code == 200
        data = resp2.json()
        assert data["narrative"] == MOCK_NARRATIVE
        assert data["cached"] is True

    @pytest.mark.asyncio
    async def test_faceted_cache_separate_from_global(
        self, client: AsyncClient, narrative_artist_ids: dict[str, int]
    ) -> None:
        ae_id = narrative_artist_ids["Autechre"]
        sl_id = narrative_artist_ids["Stereolab"]
        # Global request
        resp_global = await client.get(f"/graph/artists/{ae_id}/explain/{sl_id}/narrative")
        assert resp_global.json()["cached"] is False
        # Faceted request with month=1 — should NOT be cached
        resp_faceted = await client.get(
            f"/graph/artists/{ae_id}/explain/{sl_id}/narrative", params={"month": 1}
        )
        assert resp_faceted.status_code == 200
        assert resp_faceted.json()["cached"] is False


class TestEdgeTypeFiltering:
    @pytest.mark.asyncio
    async def test_edge_type_scopes_prompt_to_single_type(
        self, client: AsyncClient, narrative_artist_ids: dict[str, int]
    ) -> None:
        """When edge_type is provided, only that relationship type appears in the prompt."""
        ae_id = narrative_artist_ids["Autechre"]
        sl_id = narrative_artist_ids["Stereolab"]
        resp = await client.get(
            f"/graph/artists/{ae_id}/explain/{sl_id}/narrative",
            params={"edge_type": "sharedPersonnel"},
        )
        assert resp.status_code == 200

        mock_client = client._transport.app.state.anthropic_client  # type: ignore[union-attr]
        last_call = mock_client.messages.create.call_args
        messages = last_call.kwargs.get("messages") or last_call[1].get("messages", [])
        prompt_data = json.loads(messages[0]["content"])

        rel_types = [r["type"] for r in prompt_data["relationships"]]
        assert rel_types == ["sharedPersonnel"]

    @pytest.mark.asyncio
    async def test_edge_type_cache_separate_from_global(
        self, client: AsyncClient, narrative_artist_ids: dict[str, int]
    ) -> None:
        """Narratives for different edge types are cached separately."""
        ae_id = narrative_artist_ids["Autechre"]
        sl_id = narrative_artist_ids["Stereolab"]
        # Global narrative (no edge_type)
        resp1 = await client.get(f"/graph/artists/{ae_id}/explain/{sl_id}/narrative")
        assert resp1.json()["cached"] is False
        # sharedPersonnel narrative — separate cache entry
        resp2 = await client.get(
            f"/graph/artists/{ae_id}/explain/{sl_id}/narrative",
            params={"edge_type": "sharedPersonnel"},
        )
        assert resp2.json()["cached"] is False
        # sharedStyle narrative — also separate
        resp3 = await client.get(
            f"/graph/artists/{ae_id}/explain/{sl_id}/narrative",
            params={"edge_type": "sharedStyle"},
        )
        assert resp3.json()["cached"] is False
        # Repeat sharedPersonnel — should hit cache
        resp4 = await client.get(
            f"/graph/artists/{ae_id}/explain/{sl_id}/narrative",
            params={"edge_type": "sharedPersonnel"},
        )
        assert resp4.json()["cached"] is True

    @pytest.mark.asyncio
    async def test_unknown_edge_type_falls_back_to_all(
        self, client: AsyncClient, narrative_artist_ids: dict[str, int]
    ) -> None:
        """An unrecognized edge_type queries all relationship types."""
        ae_id = narrative_artist_ids["Autechre"]
        sl_id = narrative_artist_ids["Stereolab"]
        resp = await client.get(
            f"/graph/artists/{ae_id}/explain/{sl_id}/narrative",
            params={"edge_type": "notARealType"},
        )
        assert resp.status_code == 200

        mock_client = client._transport.app.state.anthropic_client  # type: ignore[union-attr]
        last_call = mock_client.messages.create.call_args
        messages = last_call.kwargs.get("messages") or last_call[1].get("messages", [])
        prompt_data = json.loads(messages[0]["content"])

        rel_types = {r["type"] for r in prompt_data["relationships"]}
        assert "djTransition" in rel_types
        assert "sharedPersonnel" in rel_types


class TestPairNormalization:
    @pytest.mark.asyncio
    async def test_reversed_pair_shares_cache(
        self, client: AsyncClient, narrative_artist_ids: dict[str, int]
    ) -> None:
        ae_id = narrative_artist_ids["Autechre"]
        sl_id = narrative_artist_ids["Stereolab"]
        # Request A -> B
        resp1 = await client.get(f"/graph/artists/{ae_id}/explain/{sl_id}/narrative")
        assert resp1.json()["cached"] is False
        # Request B -> A — should hit cache
        resp2 = await client.get(f"/graph/artists/{sl_id}/explain/{ae_id}/narrative")
        assert resp2.json()["cached"] is True


class TestGracefulDegradation:
    @pytest.mark.asyncio
    async def test_no_api_key_returns_501(
        self, client_no_key: AsyncClient, narrative_artist_ids: dict[str, int]
    ) -> None:
        ae_id = narrative_artist_ids["Autechre"]
        sl_id = narrative_artist_ids["Stereolab"]
        resp = await client_no_key.get(f"/graph/artists/{ae_id}/explain/{sl_id}/narrative")
        assert resp.status_code == 501

    @pytest.mark.asyncio
    async def test_unknown_artist_returns_404(self, client: AsyncClient) -> None:
        resp = await client.get("/graph/artists/99999/explain/99998/narrative")
        assert resp.status_code == 404


class TestPromptContent:
    @pytest.mark.asyncio
    async def test_prompt_contains_artist_names(
        self, client: AsyncClient, narrative_artist_ids: dict[str, int]
    ) -> None:
        ae_id = narrative_artist_ids["Autechre"]
        sl_id = narrative_artist_ids["Stereolab"]
        resp = await client.get(f"/graph/artists/{ae_id}/explain/{sl_id}/narrative")
        assert resp.status_code == 200
        assert resp.json()["cached"] is False

        # Inspect the mock's call args
        mock_client = client._transport.app.state.anthropic_client  # type: ignore[union-attr]
        assert mock_client.messages.create.call_count == 1
        call_kwargs = mock_client.messages.create.call_args
        messages = call_kwargs.kwargs.get("messages") or call_kwargs[1].get("messages", [])
        user_message = messages[0]["content"]
        assert "Autechre" in user_message
        assert "Stereolab" in user_message

    @pytest.mark.asyncio
    async def test_prompt_includes_facet_context(
        self, client: AsyncClient, narrative_artist_ids: dict[str, int]
    ) -> None:
        ae_id = narrative_artist_ids["Autechre"]
        sl_id = narrative_artist_ids["Stereolab"]
        resp = await client.get(
            f"/graph/artists/{ae_id}/explain/{sl_id}/narrative", params={"month": 1}
        )
        assert resp.status_code == 200

        mock_client = client._transport.app.state.anthropic_client  # type: ignore[union-attr]
        # Find the faceted call (may be second if first test ran unfaceted)
        last_call = mock_client.messages.create.call_args
        messages = last_call.kwargs.get("messages") or last_call[1].get("messages", [])
        user_message = messages[0]["content"]
        assert "January" in user_message

    @pytest.mark.asyncio
    async def test_prompt_includes_artist_metadata(
        self, client: AsyncClient, narrative_artist_ids: dict[str, int]
    ) -> None:
        """Prompt should contain genre, styles, and play counts for both artists."""
        ae_id = narrative_artist_ids["Autechre"]
        sl_id = narrative_artist_ids["Stereolab"]
        resp = await client.get(f"/graph/artists/{ae_id}/explain/{sl_id}/narrative")
        assert resp.status_code == 200

        mock_client = client._transport.app.state.anthropic_client  # type: ignore[union-attr]
        last_call = mock_client.messages.create.call_args
        messages = last_call.kwargs.get("messages") or last_call[1].get("messages", [])
        user_message = messages[0]["content"]
        prompt_data = json.loads(user_message)

        # Source artist metadata
        assert prompt_data["source"]["name"] == "Autechre"
        assert prompt_data["source"]["genre"] == "Electronic"
        assert prompt_data["source"]["total_plays"] == 50
        assert set(prompt_data["source"]["styles"]) == {"IDM", "Abstract"}

        # Target artist metadata
        assert prompt_data["target"]["name"] == "Stereolab"
        assert prompt_data["target"]["genre"] == "Rock"
        assert prompt_data["target"]["total_plays"] == 30
        assert set(prompt_data["target"]["styles"]) == {"Post-Rock", "Krautrock"}

    @pytest.mark.asyncio
    async def test_prompt_metadata_omits_empty_styles(
        self, client: AsyncClient, narrative_artist_ids: dict[str, int]
    ) -> None:
        """Artists with no style tags get the ``styles`` key omitted entirely.

        Empty fields invite the model to invent — it doesn't conclude "this is
        a data gap," it concludes "I should fill this." So drop the key.
        """
        ae_id = narrative_artist_ids["Autechre"]
        cp_id = narrative_artist_ids["Cat Power"]
        resp = await client.get(f"/graph/artists/{ae_id}/explain/{cp_id}/narrative")
        assert resp.status_code == 200

        mock_client = client._transport.app.state.anthropic_client  # type: ignore[union-attr]
        last_call = mock_client.messages.create.call_args
        messages = last_call.kwargs.get("messages") or last_call[1].get("messages", [])
        prompt_data = json.loads(messages[0]["content"])

        assert prompt_data["target"]["name"] == "Cat Power"
        assert "styles" not in prompt_data["target"]
        assert prompt_data["target"]["total_plays"] == 20


class TestAudioProfileEnrichment:
    @pytest.mark.asyncio
    async def test_prompt_includes_audio_profile(
        self, narrative_db_path: str, narrative_artist_ids: dict[str, int]
    ) -> None:
        """When an audio profile exists, the prompt carries qualitative descriptors only.

        Verifies no raw decimals reach the prompt and that extreme values get
        their human-readable labels.
        """
        _clear_narrative_cache(narrative_db_path)

        # Insert an audio profile for Autechre with extreme values across the
        # board so every descriptor has something to render.
        ae_id = narrative_artist_ids["Autechre"]
        conn = sqlite3.connect(narrative_db_path)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS audio_profile ("
            "artist_id INTEGER PRIMARY KEY, avg_danceability REAL, "
            "primary_genre TEXT, primary_genre_probability REAL, "
            "voice_instrumental_ratio REAL, feature_centroid TEXT, "
            "recording_count INTEGER NOT NULL DEFAULT 0, "
            "created_at TEXT NOT NULL DEFAULT '')"
        )
        centroid = [0.0] * 59
        # Moods at indices 9-15: acoustic=0.2, aggressive=0.6, electronic=0.8,
        # happy=0.1, party=0.3, relaxed=0.1, sad=0.2
        centroid[9:16] = [0.2, 0.6, 0.8, 0.1, 0.3, 0.1, 0.2]
        conn.execute(
            "INSERT OR REPLACE INTO audio_profile "
            "(artist_id, avg_danceability, primary_genre, primary_genre_probability, "
            "voice_instrumental_ratio, feature_centroid, recording_count, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                ae_id,
                0.15,  # < 0.3 → "minimal pulse"
                "electronic",
                0.85,  # > 0.7 → "clearly electronic"
                0.10,  # < 0.2 → "instrumental"
                json.dumps(centroid),
                12,
                "2026-01-01T00:00:00Z",
            ),
        )
        conn.commit()
        conn.close()

        mock_client = _mock_anthropic_client()
        app = create_app(narrative_db_path, anthropic_api_key="test-key")
        app.state.anthropic_client = mock_client
        transport = ASGITransport(app=app)

        sl_id = narrative_artist_ids["Stereolab"]
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get(f"/graph/artists/{ae_id}/explain/{sl_id}/narrative")
        assert resp.status_code == 200

        last_call = mock_client.messages.create.call_args
        messages = last_call.kwargs.get("messages") or last_call[1].get("messages", [])
        user_message = messages[0]["content"]
        prompt_data = json.loads(user_message)

        source_audio = prompt_data["source"].get("audio")
        assert source_audio is not None
        assert source_audio["primary_genre"] == "clearly electronic"
        assert source_audio["danceability"] == "minimal pulse"
        assert source_audio["voice_instrumental"] == "instrumental"
        assert "electronic" in source_audio["top_moods"]
        assert "aggressive" in source_audio["top_moods"]
        assert source_audio["recording_count"] == 12

        # No raw decimals anywhere in the prompt — the model should have no
        # number to quote. The stringified prompt must not contain the input
        # decimal values.
        assert "0.15" not in user_message
        assert "0.85" not in user_message
        assert "0.10" not in user_message

    @pytest.mark.asyncio
    async def test_prompt_graceful_without_audio_profile(
        self, client: AsyncClient, narrative_artist_ids: dict[str, int]
    ) -> None:
        """Artists without audio profiles should not have an 'audio' key."""
        ae_id = narrative_artist_ids["Autechre"]
        cp_id = narrative_artist_ids["Cat Power"]
        resp = await client.get(f"/graph/artists/{ae_id}/explain/{cp_id}/narrative")
        assert resp.status_code == 200

        mock_client = client._transport.app.state.anthropic_client  # type: ignore[union-attr]
        last_call = mock_client.messages.create.call_args
        messages = last_call.kwargs.get("messages") or last_call[1].get("messages", [])
        prompt_data = json.loads(messages[0]["content"])

        # Cat Power has no audio profile
        assert "audio" not in prompt_data["target"]


def _build_aa_fixture_db() -> str:
    """Build a synthetic graph where AA and degree-based ranking disagree.

    Two pivots A and B share two neighbors:
      - X: degree 2 (only connected to A and B). 1/log(2) ≈ 1.44.
      - Y: degree 50 (connected to A, B, and 48 other artists). 1/log(50) ≈ 0.26.

    A degree-descending or play-count-descending ranking puts Y first; AA puts X
    first, which is the whole point of the rerank.
    """
    path = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE artist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            canonical_name TEXT NOT NULL UNIQUE,
            genre TEXT,
            total_plays INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE dj_transition (
            source_id INTEGER NOT NULL,
            target_id INTEGER NOT NULL,
            raw_count INTEGER NOT NULL,
            pmi REAL NOT NULL,
            PRIMARY KEY (source_id, target_id)
        );
        """
    )

    # Pivots + low-degree shared (X) + high-degree shared (Y) + 48 fillers
    # for Y's degree.
    fillers = [f"Filler {i:02d}" for i in range(48)]
    names = ["Father John Misty", "Caetano Veloso", "Joe McPhee", "Yo La Tengo", *fillers]
    conn.executemany(
        "INSERT INTO artist (canonical_name) VALUES (?)",
        [(n,) for n in names],
    )

    name_to_id = {
        r[1]: r[0] for r in conn.execute("SELECT id, canonical_name FROM artist").fetchall()
    }
    a, b = name_to_id["Father John Misty"], name_to_id["Caetano Veloso"]
    x, y = name_to_id["Joe McPhee"], name_to_id["Yo La Tengo"]

    # A and B each link to X (so X has degree 2) and to Y.
    edges = [(a, x), (b, x), (a, y), (b, y)]
    # Y also connects to all fillers, taking its degree to 50.
    for filler_name in fillers:
        edges.append((y, name_to_id[filler_name]))
    conn.executemany(
        "INSERT INTO dj_transition (source_id, target_id, raw_count, pmi) VALUES (?, ?, 1, 1.0)",
        edges,
    )
    conn.commit()
    conn.close()
    return path


class TestAdamicAdarReranking:
    def test_aa_outranks_high_degree_hub(self) -> None:
        """The low-degree shared neighbor scores higher than the high-degree hub."""
        path = _build_aa_fixture_db()
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        ids = {
            r["canonical_name"]: r["id"]
            for r in conn.execute("SELECT id, canonical_name FROM artist")
        }

        result = _rank_shared_neighbors_by_aa(conn, ids["Father John Misty"], ids["Caetano Veloso"])
        conn.close()

        names = [r["name"] for r in result]
        assert names[0] == "Joe McPhee", f"AA should rank low-degree neighbor first; got {names}"
        assert names[1] == "Yo La Tengo"

    def test_aa_score_matches_formula(self) -> None:
        """1/log(degree) is the score, rounded to 3 decimals."""
        path = _build_aa_fixture_db()
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        ids = {
            r["canonical_name"]: r["id"]
            for r in conn.execute("SELECT id, canonical_name FROM artist")
        }

        result = _rank_shared_neighbors_by_aa(conn, ids["Father John Misty"], ids["Caetano Veloso"])
        conn.close()

        joe = next(r for r in result if r["name"] == "Joe McPhee")
        ylt = next(r for r in result if r["name"] == "Yo La Tengo")
        assert joe["degree"] == 2
        assert ylt["degree"] == 50
        assert joe["aa_score"] == round(1.0 / math.log(2), 3)
        assert ylt["aa_score"] == round(1.0 / math.log(50), 3)

    def test_returns_full_list_uncapped(self) -> None:
        """The function returns ALL shared neighbors — top-K capping is the caller's job.

        Locks the contract: the threshold check needs the full sum to match true
        pair AA, not a top-K underestimate. If a future refactor pushes capping
        back inside this function, this test fails first.
        """
        path = _build_aa_fixture_db()
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        ids = {
            r["canonical_name"]: r["id"]
            for r in conn.execute("SELECT id, canonical_name FROM artist")
        }

        result = _rank_shared_neighbors_by_aa(conn, ids["Father John Misty"], ids["Caetano Veloso"])
        conn.close()

        # Fixture has exactly 2 shared neighbors (Joe McPhee, Yo La Tengo).
        assert [r["name"] for r in result] == ["Joe McPhee", "Yo La Tengo"]

    def test_no_shared_neighbors_returns_empty(self) -> None:
        """Artists with no shared neighbors return an empty list."""
        path = _build_aa_fixture_db()
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        ids = {
            r["canonical_name"]: r["id"]
            for r in conn.execute("SELECT id, canonical_name FROM artist")
        }

        # Filler 00 only connects to Yo La Tengo; Joe McPhee only connects to
        # the two pivots. They have no common neighbor.
        result = _rank_shared_neighbors_by_aa(conn, ids["Joe McPhee"], ids["Filler 00"])
        conn.close()

        assert result == []

    @pytest.mark.asyncio
    async def test_prompt_includes_shared_neighbors(
        self, client: AsyncClient, narrative_artist_ids: dict[str, int]
    ) -> None:
        """Generated prompt JSON carries an AA-ranked shared_neighbors list."""
        ae_id = narrative_artist_ids["Autechre"]
        sl_id = narrative_artist_ids["Stereolab"]
        resp = await client.get(f"/graph/artists/{ae_id}/explain/{sl_id}/narrative")
        assert resp.status_code == 200

        mock_client = client._transport.app.state.anthropic_client  # type: ignore[union-attr]
        last_call = mock_client.messages.create.call_args
        messages = last_call.kwargs.get("messages") or last_call[1].get("messages", [])
        prompt_data = json.loads(messages[0]["content"])

        # The fixture has only 2 pairs (Autechre-Stereolab, Autechre-Cat Power),
        # so Autechre and Stereolab have no shared neighbors and the key is
        # omitted. Asserting only that the field is absent or a list.
        if "shared_neighbors" in prompt_data:
            assert isinstance(prompt_data["shared_neighbors"], list)
            for entry in prompt_data["shared_neighbors"]:
                assert {"name", "degree", "aa_score"} <= set(entry)


class TestPromptVersionEviction:
    @pytest.mark.asyncio
    async def test_bump_invalidates_prior_version_cache(
        self,
        narrative_db_path: str,
        narrative_artist_ids: dict[str, int],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A ``_PROMPT_VERSION`` bump misses prior-version cache entries.

        Guards the contract that subsequent prompt edits in #220–#223 can rely
        on bumping the constant alone to evict stale narratives.
        """
        _clear_narrative_cache(narrative_db_path)

        mock_client = _mock_anthropic_client()
        app = create_app(narrative_db_path, anthropic_api_key="test-key")
        app.state.anthropic_client = mock_client
        transport = ASGITransport(app=app)

        ae_id = narrative_artist_ids["Autechre"]
        sl_id = narrative_artist_ids["Stereolab"]

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            # Populate cache at the current prompt version, then confirm the
            # repeat hits the cache.
            resp_first = await ac.get(f"/graph/artists/{ae_id}/explain/{sl_id}/narrative")
            assert resp_first.json()["cached"] is False
            resp_repeat = await ac.get(f"/graph/artists/{ae_id}/explain/{sl_id}/narrative")
            assert resp_repeat.json()["cached"] is True

            # Bump the prompt version: the prior row is still on disk but its
            # PK no longer matches reads, so the request must regenerate.
            monkeypatch.setattr("semantic_index.api.narrative._PROMPT_VERSION", 99)
            resp_after_bump = await ac.get(f"/graph/artists/{ae_id}/explain/{sl_id}/narrative")
            assert resp_after_bump.json()["cached"] is False, (
                "cache should miss after _PROMPT_VERSION bump"
            )
            # And the new version becomes the warm cache.
            resp_warm = await ac.get(f"/graph/artists/{ae_id}/explain/{sl_id}/narrative")
            assert resp_warm.json()["cached"] is True


def _build_aa_threshold_fixture_db() -> str:
    """Build a graph with one above-threshold pair and one below-threshold pair.

    - Above: ``A_above`` and ``B_above`` share two degree-2 neighbors
      (``Niche 1``, ``Niche 2``). Each contributes ~1.44 → total ~2.88, above
      the 0.8 default.
    - Below: ``A_below`` and ``B_below`` share one degree-50 neighbor
      (``Mega Hub``) plus 49 disjoint fillers. Total ~0.26, below the floor.
    """
    path = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE artist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            canonical_name TEXT NOT NULL UNIQUE,
            genre TEXT,
            total_plays INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE dj_transition (
            source_id INTEGER NOT NULL,
            target_id INTEGER NOT NULL,
            raw_count INTEGER NOT NULL,
            pmi REAL NOT NULL,
            PRIMARY KEY (source_id, target_id)
        );
        """
    )

    fillers = [f"Mega Filler {i:02d}" for i in range(49)]
    names = [
        "Aldous Harding",  # A_above
        "Cate Le Bon",  # B_above
        "Beverly Glenn-Copeland",  # niche 1 (deg 2)
        "Hermanos Gutiérrez",  # niche 2 (deg 2)
        "Frank Sinatra",  # A_below
        "Sun Ra",  # B_below
        "Mega Hub",  # deg 50 shared neighbor
        *fillers,
    ]
    conn.executemany(
        "INSERT INTO artist (canonical_name) VALUES (?)",
        [(n,) for n in names],
    )

    name_to_id = {
        r[1]: r[0] for r in conn.execute("SELECT id, canonical_name FROM artist").fetchall()
    }

    edges = [
        # Above-threshold pair shares two niche neighbors.
        (name_to_id["Aldous Harding"], name_to_id["Beverly Glenn-Copeland"]),
        (name_to_id["Cate Le Bon"], name_to_id["Beverly Glenn-Copeland"]),
        (name_to_id["Aldous Harding"], name_to_id["Hermanos Gutiérrez"]),
        (name_to_id["Cate Le Bon"], name_to_id["Hermanos Gutiérrez"]),
        # Below-threshold pair shares only a high-degree hub.
        (name_to_id["Frank Sinatra"], name_to_id["Mega Hub"]),
        (name_to_id["Sun Ra"], name_to_id["Mega Hub"]),
    ]
    # Pump Mega Hub up to degree 50 with 49 disjoint fillers.
    for filler in fillers:
        edges.append((name_to_id["Mega Hub"], name_to_id[filler]))
    conn.executemany(
        "INSERT INTO dj_transition (source_id, target_id, raw_count, pmi) VALUES (?, ?, 1, 1.0)",
        edges,
    )
    conn.commit()
    conn.close()
    return path


@pytest.fixture(scope="module")
def threshold_db_path() -> str:
    return _build_aa_threshold_fixture_db()


@pytest.fixture(scope="module")
def threshold_artist_ids(threshold_db_path: str) -> dict[str, int]:
    conn = sqlite3.connect(threshold_db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT id, canonical_name FROM artist").fetchall()
    conn.close()
    return {r["canonical_name"]: r["id"] for r in rows}


class TestAaThreshold:
    @pytest.mark.asyncio
    async def test_below_threshold_short_circuits_llm(
        self,
        threshold_db_path: str,
        threshold_artist_ids: dict[str, int],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A pair whose total AA falls below the floor returns the canned narrative.

        The LLM client is set on the app but should never be called.
        """
        monkeypatch.setenv("NARRATIVE_MIN_AA_SCORE", "0.8")
        _clear_narrative_cache(threshold_db_path)
        mock_client = _mock_anthropic_client()
        app = create_app(threshold_db_path, anthropic_api_key="test-key")
        app.state.anthropic_client = mock_client
        transport = ASGITransport(app=app)

        sinatra = threshold_artist_ids["Frank Sinatra"]
        sun_ra = threshold_artist_ids["Sun Ra"]

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get(f"/graph/artists/{sinatra}/explain/{sun_ra}/narrative")

        assert resp.status_code == 200
        body = resp.json()
        assert body["insufficient_signal"] is True
        assert body["cached"] is False
        assert body["narrative"] == _INSUFFICIENT_SIGNAL_NARRATIVE
        assert mock_client.messages.create.call_count == 0, (
            "LLM should not be called below threshold"
        )

    @pytest.mark.asyncio
    async def test_above_threshold_calls_llm(
        self,
        threshold_db_path: str,
        threshold_artist_ids: dict[str, int],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A pair whose total AA clears the floor reaches the LLM normally."""
        monkeypatch.setenv("NARRATIVE_MIN_AA_SCORE", "0.8")
        _clear_narrative_cache(threshold_db_path)
        mock_client = _mock_anthropic_client()
        app = create_app(threshold_db_path, anthropic_api_key="test-key")
        app.state.anthropic_client = mock_client
        transport = ASGITransport(app=app)

        aldous = threshold_artist_ids["Aldous Harding"]
        cate = threshold_artist_ids["Cate Le Bon"]

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get(f"/graph/artists/{aldous}/explain/{cate}/narrative")

        assert resp.status_code == 200
        body = resp.json()
        assert body["insufficient_signal"] is False
        assert body["narrative"] == MOCK_NARRATIVE
        assert mock_client.messages.create.call_count == 1

    @pytest.mark.asyncio
    async def test_threshold_env_var_overrides_default(
        self,
        threshold_db_path: str,
        threshold_artist_ids: dict[str, int],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Lowering ``NARRATIVE_MIN_AA_SCORE`` lifts the below-default pair above the floor."""
        # Drop the threshold so the previously-below pair clears it.
        monkeypatch.setenv("NARRATIVE_MIN_AA_SCORE", "0.1")
        _clear_narrative_cache(threshold_db_path)
        mock_client = _mock_anthropic_client()
        app = create_app(threshold_db_path, anthropic_api_key="test-key")
        app.state.anthropic_client = mock_client
        transport = ASGITransport(app=app)

        sinatra = threshold_artist_ids["Frank Sinatra"]
        sun_ra = threshold_artist_ids["Sun Ra"]

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get(f"/graph/artists/{sinatra}/explain/{sun_ra}/narrative")

        body = resp.json()
        assert body["insufficient_signal"] is False
        assert mock_client.messages.create.call_count == 1

    @pytest.mark.asyncio
    async def test_below_threshold_response_is_cached(
        self,
        threshold_db_path: str,
        threshold_artist_ids: dict[str, int],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A below-threshold response is cached so repeat requests stay cheap."""
        monkeypatch.setenv("NARRATIVE_MIN_AA_SCORE", "0.8")
        _clear_narrative_cache(threshold_db_path)
        mock_client = _mock_anthropic_client()
        app = create_app(threshold_db_path, anthropic_api_key="test-key")
        app.state.anthropic_client = mock_client
        transport = ASGITransport(app=app)

        sinatra = threshold_artist_ids["Frank Sinatra"]
        sun_ra = threshold_artist_ids["Sun Ra"]

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            first = await ac.get(f"/graph/artists/{sinatra}/explain/{sun_ra}/narrative")
            second = await ac.get(f"/graph/artists/{sinatra}/explain/{sun_ra}/narrative")

        assert first.json()["cached"] is False
        assert first.json()["insufficient_signal"] is True
        assert second.json()["cached"] is True
        assert second.json()["insufficient_signal"] is True
        assert mock_client.messages.create.call_count == 0

    @pytest.mark.asyncio
    async def test_threshold_uses_full_pair_aa_not_top_k_sum(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The threshold sums the full ranked list, not just the top-K surfaced.

        Constructs a graph with 6 shared neighbors (one more than top-K=5),
        each at degree 50 (AA contribution ~0.256). The top-K sum is ~1.28;
        the full sum is ~1.535. With NARRATIVE_MIN_AA_SCORE=1.4, a buggy
        implementation that summed only the surfaced top-K would short-circuit;
        the correct implementation reaches the LLM.
        """
        # Sanity: top-K is 5, so we need >5 shared neighbors to exercise the gap.
        assert _SHARED_NEIGHBORS_TOP_K == 5, "test designed around top-K = 5"

        path = tempfile.mktemp(suffix=".db")
        conn = sqlite3.connect(path)
        conn.executescript(
            """
            CREATE TABLE artist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                canonical_name TEXT NOT NULL UNIQUE,
                genre TEXT,
                total_plays INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE dj_transition (
                source_id INTEGER NOT NULL,
                target_id INTEGER NOT NULL,
                raw_count INTEGER NOT NULL,
                pmi REAL NOT NULL,
                PRIMARY KEY (source_id, target_id)
            );
            """
        )
        shared = [f"Shared {i}" for i in range(6)]
        fillers = [f"Filler {i:02d}" for i in range(48)]
        names = ["A", "B", *shared, *fillers]
        conn.executemany("INSERT INTO artist (canonical_name) VALUES (?)", [(n,) for n in names])
        ids = {r[0]: r[1] for r in conn.execute("SELECT canonical_name, id FROM artist").fetchall()}
        edges: list[tuple[int, int]] = []
        # Each shared neighbor connects to A, B, and all 48 fillers → degree 50.
        for s in shared:
            edges.append((ids["A"], ids[s]))
            edges.append((ids["B"], ids[s]))
            for f in fillers:
                edges.append((ids[s], ids[f]))
        conn.executemany(
            "INSERT INTO dj_transition (source_id, target_id, raw_count, pmi) "
            "VALUES (?, ?, 1, 1.0)",
            edges,
        )
        conn.commit()
        conn.close()

        # Threshold is between top-5 (~1.28) and full-6 (~1.54).
        monkeypatch.setenv("NARRATIVE_MIN_AA_SCORE", "1.4")
        _clear_narrative_cache(path)
        mock_client = _mock_anthropic_client()
        app = create_app(path, anthropic_api_key="test-key")
        app.state.anthropic_client = mock_client
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get(f"/graph/artists/{ids['A']}/explain/{ids['B']}/narrative")

        body = resp.json()
        assert body["insufficient_signal"] is False, (
            "threshold should sum all 6 shared neighbors, not just top-5"
        )
        assert mock_client.messages.create.call_count == 1
        # Sanity: prompt was capped to top-K so the LLM input stays bounded.
        last_call = mock_client.messages.create.call_args
        messages = last_call.kwargs.get("messages") or last_call[1].get("messages", [])
        prompt_data = json.loads(messages[0]["content"])
        assert len(prompt_data["shared_neighbors"]) == _SHARED_NEIGHBORS_TOP_K

    @pytest.mark.asyncio
    async def test_invalid_threshold_falls_back_to_default(
        self,
        threshold_db_path: str,
        threshold_artist_ids: dict[str, int],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A garbage ``NARRATIVE_MIN_AA_SCORE`` reverts to the 0.8 default."""
        monkeypatch.setenv("NARRATIVE_MIN_AA_SCORE", "not-a-float")
        _clear_narrative_cache(threshold_db_path)
        mock_client = _mock_anthropic_client()
        app = create_app(threshold_db_path, anthropic_api_key="test-key")
        app.state.anthropic_client = mock_client
        transport = ASGITransport(app=app)

        sinatra = threshold_artist_ids["Frank Sinatra"]
        sun_ra = threshold_artist_ids["Sun Ra"]

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get(f"/graph/artists/{sinatra}/explain/{sun_ra}/narrative")

        # Default 0.8 should have applied; below-threshold pair short-circuits.
        assert resp.json()["insufficient_signal"] is True
        assert mock_client.messages.create.call_count == 0


class TestStylesCap:
    def test_long_style_list_capped_in_metadata(self) -> None:
        """An artist with 10 styles surfaces only ``_STYLES_TOP_N`` in the prompt metadata.

        Prevents the Outkast/Destroyer hallucination mode where the model latches
        onto a minor-release outlier ("makina", "breakbeat") and describes a hip
        hop or indie-rock artist as channeling it.

        Uses an isolated fixture and calls ``_lookup_artist_metadata`` directly
        so the test doesn't mutate the shared module-scoped narrative fixture.
        """
        from semantic_index.api.narrative import _lookup_artist_metadata

        path = tempfile.mktemp(suffix=".db")
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        conn.executescript(
            """
            CREATE TABLE artist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                canonical_name TEXT NOT NULL UNIQUE,
                genre TEXT,
                total_plays INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE artist_style (
                artist_id INTEGER NOT NULL,
                style_tag TEXT NOT NULL,
                PRIMARY KEY (artist_id, style_tag)
            );
            """
        )
        conn.execute(
            "INSERT INTO artist (canonical_name, genre, total_plays) VALUES (?, ?, ?)",
            ("Outkast", "Hip Hop", 200),
        )
        artist_id = conn.execute("SELECT id FROM artist").fetchone()["id"]

        # 10 styles, deliberately not in alphabetical order so the SQL ORDER BY
        # is exercised. Alphabetical top-5 = ['Crunk', 'Dirty South', 'Funk',
        # 'Hip Hop', 'P.Funk'].
        ten_styles = [
            "Hip Hop",
            "Soul",
            "P.Funk",
            "Crunk",
            "Funk",
            "Trip Hop",
            "Rap",
            "RnB",
            "Reggae",
            "Dirty South",
        ]
        conn.executemany(
            "INSERT INTO artist_style (artist_id, style_tag) VALUES (?, ?)",
            [(artist_id, s) for s in ten_styles],
        )
        conn.commit()

        meta = _lookup_artist_metadata(conn, artist_id, "Outkast", "Hip Hop", 200)
        conn.close()

        assert len(meta["styles"]) == _STYLES_TOP_N, (
            f"10 styles should be capped at {_STYLES_TOP_N}; got {len(meta['styles'])}"
        )
        assert meta["styles"] == sorted(ten_styles)[:_STYLES_TOP_N]

    def test_short_style_list_unaffected(self) -> None:
        """An artist with fewer than ``_STYLES_TOP_N`` styles passes them all."""
        from semantic_index.api.narrative import _lookup_artist_metadata

        path = tempfile.mktemp(suffix=".db")
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        conn.executescript(
            """
            CREATE TABLE artist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                canonical_name TEXT NOT NULL UNIQUE,
                genre TEXT,
                total_plays INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE artist_style (
                artist_id INTEGER NOT NULL,
                style_tag TEXT NOT NULL,
                PRIMARY KEY (artist_id, style_tag)
            );
            """
        )
        conn.execute("INSERT INTO artist (canonical_name) VALUES ('Stereolab')")
        artist_id = conn.execute("SELECT id FROM artist").fetchone()["id"]
        conn.executemany(
            "INSERT INTO artist_style (artist_id, style_tag) VALUES (?, ?)",
            [(artist_id, s) for s in ("Krautrock", "Post-Rock", "Indie")],
        )
        conn.commit()

        meta = _lookup_artist_metadata(conn, artist_id, "Stereolab", None, 0)
        conn.close()

        assert meta["styles"] == ["Indie", "Krautrock", "Post-Rock"]


def _make_audio_row(**fields) -> sqlite3.Row:
    """Build a sqlite3.Row stand-in for ``_qualitative_audio_descriptors`` tests.

    sqlite3.Row holds the column data after the connection closes, so it's
    safe to close the connection before returning the row.
    """
    defaults = {
        "avg_danceability": None,
        "primary_genre": None,
        "primary_genre_probability": None,
        "voice_instrumental_ratio": None,
        "feature_centroid": None,
        "recording_count": 1,
    }
    defaults.update(fields)
    conn = sqlite3.connect(":memory:")
    try:
        conn.row_factory = sqlite3.Row
        cols = ", ".join(defaults.keys())
        placeholders = ", ".join("?" * len(defaults))
        conn.execute(f"CREATE TABLE t ({cols})")  # noqa: S608
        conn.execute(  # noqa: S608
            f"INSERT INTO t VALUES ({placeholders})", tuple(defaults.values())
        )
        return conn.execute("SELECT * FROM t").fetchone()
    finally:
        conn.close()


def _make_bpm_row(avg_bpm: float | None = None, primary_key: str | None = None) -> sqlite3.Row:
    """Build a sqlite3.Row stand-in for the BPM/key fetch in the helper."""
    conn = sqlite3.connect(":memory:")
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE t (avg_bpm REAL, primary_key TEXT)")
        conn.execute("INSERT INTO t VALUES (?, ?)", (avg_bpm, primary_key))
        return conn.execute("SELECT * FROM t").fetchone()
    finally:
        conn.close()


class TestQualitativeAudioDescriptors:
    """Boundary tests for ``_qualitative_audio_descriptors``.

    The principle is: describe extremes, omit middles. Values in the middle
    band must produce *no* corresponding key in the output so the model has
    no number to quote.
    """

    def test_high_danceability_labeled(self) -> None:
        from semantic_index.api.narrative import _qualitative_audio_descriptors

        desc = _qualitative_audio_descriptors(_make_audio_row(avg_danceability=0.7), None)
        assert desc["danceability"] == "highly danceable"

    def test_low_danceability_labeled(self) -> None:
        from semantic_index.api.narrative import _qualitative_audio_descriptors

        desc = _qualitative_audio_descriptors(_make_audio_row(avg_danceability=0.2), None)
        assert desc["danceability"] == "minimal pulse"

    def test_middle_danceability_omitted(self) -> None:
        from semantic_index.api.narrative import _qualitative_audio_descriptors

        desc = _qualitative_audio_descriptors(_make_audio_row(avg_danceability=0.45), None)
        assert "danceability" not in desc

    def test_vocal_forward_labeled(self) -> None:
        from semantic_index.api.narrative import _qualitative_audio_descriptors

        desc = _qualitative_audio_descriptors(_make_audio_row(voice_instrumental_ratio=0.85), None)
        assert desc["voice_instrumental"] == "vocal-forward"

    def test_instrumental_labeled(self) -> None:
        from semantic_index.api.narrative import _qualitative_audio_descriptors

        desc = _qualitative_audio_descriptors(_make_audio_row(voice_instrumental_ratio=0.05), None)
        assert desc["voice_instrumental"] == "instrumental"

    def test_ambiguous_voice_omitted(self) -> None:
        from semantic_index.api.narrative import _qualitative_audio_descriptors

        desc = _qualitative_audio_descriptors(_make_audio_row(voice_instrumental_ratio=0.5), None)
        assert "voice_instrumental" not in desc

    def test_confident_genre_labeled(self) -> None:
        from semantic_index.api.narrative import _qualitative_audio_descriptors

        desc = _qualitative_audio_descriptors(
            _make_audio_row(primary_genre="rock", primary_genre_probability=0.9), None
        )
        assert desc["primary_genre"] == "clearly rock"

    def test_low_confidence_genre_omitted(self) -> None:
        """Below 0.7 confidence the model just learns wrong — drop the genre entirely."""
        from semantic_index.api.narrative import _qualitative_audio_descriptors

        desc = _qualitative_audio_descriptors(
            _make_audio_row(primary_genre="rock", primary_genre_probability=0.5), None
        )
        assert "primary_genre" not in desc

    def test_only_recording_count_returns_empty(self) -> None:
        """A bare recording_count with no remarkable fields suppresses the whole block.

        A descriptor of ``{"recording_count": 3}`` alone tells the model there's
        audio data without anchoring it to anything, so the caller drops the
        ``audio`` field entirely.
        """
        from semantic_index.api.narrative import _qualitative_audio_descriptors

        desc = _qualitative_audio_descriptors(_make_audio_row(recording_count=3), None)
        assert desc == {}

    def test_recording_count_kept_when_other_field_remarkable(self) -> None:
        """When one remarkable field surfaces, recording_count rides along."""
        from semantic_index.api.narrative import _qualitative_audio_descriptors

        desc = _qualitative_audio_descriptors(
            _make_audio_row(avg_danceability=0.8, recording_count=3), None
        )
        assert desc["danceability"] == "highly danceable"
        assert desc["recording_count"] == 3

    def test_danceability_low_boundary_omitted(self) -> None:
        """Exactly ``_DANCEABILITY_LOW`` (0.3) sits in the omit band — the cutoff is strict."""
        from semantic_index.api.narrative import _qualitative_audio_descriptors

        desc = _qualitative_audio_descriptors(_make_audio_row(avg_danceability=0.3), None)
        assert "danceability" not in desc

    def test_danceability_high_boundary_omitted(self) -> None:
        """Exactly ``_DANCEABILITY_HIGH`` (0.6) sits in the omit band — the cutoff is strict."""
        from semantic_index.api.narrative import _qualitative_audio_descriptors

        desc = _qualitative_audio_descriptors(
            _make_audio_row(
                avg_danceability=0.6, primary_genre="rock", primary_genre_probability=0.9
            ),
            None,
        )
        # Genre forces a non-empty descriptor so we can read it back.
        assert "danceability" not in desc

    def test_slow_tempo_labeled(self) -> None:
        from semantic_index.api.narrative import _qualitative_audio_descriptors

        desc = _qualitative_audio_descriptors(
            _make_audio_row(avg_danceability=0.7),
            _make_bpm_row(avg_bpm=70.0),
        )
        assert desc["tempo"] == "slow"

    def test_fast_tempo_labeled(self) -> None:
        from semantic_index.api.narrative import _qualitative_audio_descriptors

        desc = _qualitative_audio_descriptors(
            _make_audio_row(avg_danceability=0.7),
            _make_bpm_row(avg_bpm=145.0),
        )
        assert desc["tempo"] == "fast"

    def test_middle_tempo_omitted(self) -> None:
        from semantic_index.api.narrative import _qualitative_audio_descriptors

        desc = _qualitative_audio_descriptors(
            _make_audio_row(avg_danceability=0.7),
            _make_bpm_row(avg_bpm=110.0),
        )
        assert "tempo" not in desc

    def test_key_passes_through(self) -> None:
        """Key is a string ('C major'), not a number — passes through unchanged."""
        from semantic_index.api.narrative import _qualitative_audio_descriptors

        desc = _qualitative_audio_descriptors(
            _make_audio_row(avg_danceability=0.7),
            _make_bpm_row(primary_key="C major"),
        )
        assert desc["key"] == "C major"

    def test_missing_bpm_row_skips_tempo_and_key(self) -> None:
        """A None bpm_row (older schema) cleanly omits tempo + key without erroring."""
        from semantic_index.api.narrative import _qualitative_audio_descriptors

        desc = _qualitative_audio_descriptors(_make_audio_row(avg_danceability=0.7), None)
        assert "tempo" not in desc
        assert "key" not in desc

    def test_no_raw_numbers_in_output(self) -> None:
        """No value in the descriptor dict (except recording_count) should be a float."""
        from semantic_index.api.narrative import _qualitative_audio_descriptors

        desc = _qualitative_audio_descriptors(
            _make_audio_row(
                avg_danceability=0.05,
                primary_genre="ambient",
                primary_genre_probability=0.95,
                voice_instrumental_ratio=0.99,
                recording_count=5,
            ),
            None,
        )
        for key, value in desc.items():
            if key == "recording_count":
                continue
            assert not isinstance(value, float), f"{key} leaks a raw float: {value!r}"


class TestOmitEmptyAndPlaceholderFields:
    """The ``Konono No 1`` failure mode: empty fields tempt the model to invent.

    Verifies that empty lists, ``None``, and placeholder strings ("Unknown",
    "Various", "Various Artists", "V/A", etc.) are stripped before the prompt
    is built, and that the system prompt is told via a ``caveat`` field when
    an artist has only ``name`` + ``total_plays`` after filtering.
    """

    def test_konono_empty_styles_drops_key(self) -> None:
        """An artist with zero Discogs styles gets ``styles`` omitted from the prompt."""
        from semantic_index.api.narrative import _lookup_artist_metadata

        path = tempfile.mktemp(suffix=".db")
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        conn.executescript(
            """
            CREATE TABLE artist (id INTEGER PRIMARY KEY, canonical_name TEXT, genre TEXT);
            CREATE TABLE artist_style (artist_id INTEGER, style_tag TEXT,
                                       PRIMARY KEY (artist_id, style_tag));
            INSERT INTO artist (id, canonical_name, genre)
                VALUES (1, 'Konono No 1', 'World');
            """
        )
        meta = _lookup_artist_metadata(conn, 1, "Konono No 1", "World", 25)
        conn.close()

        assert "styles" not in meta, "empty styles should be omitted, not emitted as []"
        assert meta["genre"] == "World"
        assert meta["name"] == "Konono No 1"

    def test_unknown_genre_dropped(self) -> None:
        """Genre = 'Unknown' is a placeholder; treat as missing."""
        from semantic_index.api.narrative import _lookup_artist_metadata

        path = tempfile.mktemp(suffix=".db")
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        conn.executescript(
            "CREATE TABLE artist (id INTEGER PRIMARY KEY, canonical_name TEXT);"
            "CREATE TABLE artist_style (artist_id INTEGER, style_tag TEXT,"
            " PRIMARY KEY (artist_id, style_tag));"
            "INSERT INTO artist (id, canonical_name) VALUES (1, 'Test');"
        )
        meta = _lookup_artist_metadata(conn, 1, "Test", "Unknown", 5)
        conn.close()

        assert "genre" not in meta

    def test_various_artists_genre_dropped(self) -> None:
        """Genre = 'Various Artists' is a compilation indicator, not a real genre."""
        from semantic_index.api.narrative import _lookup_artist_metadata

        path = tempfile.mktemp(suffix=".db")
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        conn.executescript(
            "CREATE TABLE artist (id INTEGER PRIMARY KEY, canonical_name TEXT);"
            "CREATE TABLE artist_style (artist_id INTEGER, style_tag TEXT,"
            " PRIMARY KEY (artist_id, style_tag));"
            "INSERT INTO artist (id, canonical_name) VALUES (1, 'Test');"
        )
        for placeholder in ("Various Artists", "Various", "V/A", "v.a.", "  unknown "):
            meta = _lookup_artist_metadata(conn, 1, "Test", placeholder, 5)
            assert "genre" not in meta, f"placeholder {placeholder!r} should be dropped"
        conn.close()

    def test_placeholder_styles_filtered_out(self) -> None:
        """Individual placeholder style tags drop out of the styles list."""
        from semantic_index.api.narrative import _lookup_artist_metadata

        path = tempfile.mktemp(suffix=".db")
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        conn.executescript(
            "CREATE TABLE artist (id INTEGER PRIMARY KEY, canonical_name TEXT);"
            "CREATE TABLE artist_style (artist_id INTEGER, style_tag TEXT,"
            " PRIMARY KEY (artist_id, style_tag));"
            "INSERT INTO artist (id, canonical_name) VALUES (1, 'Test');"
            "INSERT INTO artist_style VALUES (1, 'Folk'), (1, 'Unknown'),"
            " (1, 'Rock'), (1, 'N/A');"
        )
        meta = _lookup_artist_metadata(conn, 1, "Test", None, 5)
        conn.close()

        assert meta["styles"] == ["Folk", "Rock"]

    def test_empty_relationships_omitted_from_prompt(self) -> None:
        """An empty ``relationships`` list is dropped — no key in the prompt JSON."""
        from semantic_index.api.narrative import _build_prompt

        prompt = _build_prompt(
            source_meta={"name": "A", "total_plays": 10, "genre": "Rock"},
            target_meta={"name": "B", "total_plays": 8, "genre": "Jazz"},
            relationships=[],
        )
        data = json.loads(prompt)
        assert "relationships" not in data

    def test_limited_metadata_sets_caveat(self) -> None:
        """Artist with only name + total_plays after filtering triggers the caveat."""
        from semantic_index.api.narrative import _build_prompt

        prompt = _build_prompt(
            source_meta={"name": "Konono No 1", "total_plays": 25},
            target_meta={"name": "Stereolab", "total_plays": 30, "genre": "Rock"},
            relationships=[{"type": "djTransition", "raw_count": 3, "pmi": 2.1}],
        )
        data = json.loads(prompt)
        assert "caveat" in data
        assert "Konono No 1" in data["caveat"]
        assert "Stereolab" not in data["caveat"]

    def test_no_caveat_when_both_artists_rich(self) -> None:
        """Both artists with content fields → no caveat in the prompt."""
        from semantic_index.api.narrative import _build_prompt

        prompt = _build_prompt(
            source_meta={"name": "A", "total_plays": 10, "genre": "Rock"},
            target_meta={"name": "B", "total_plays": 8, "styles": ["Jazz"]},
            relationships=[{"type": "djTransition", "raw_count": 3, "pmi": 2.1}],
        )
        data = json.loads(prompt)
        assert "caveat" not in data

    def test_caveat_lists_both_when_both_limited(self) -> None:
        """Both artists thin → caveat names both."""
        from semantic_index.api.narrative import _build_prompt

        prompt = _build_prompt(
            source_meta={"name": "Konono No 1", "total_plays": 25},
            target_meta={"name": "Pastor T.L. Barrett", "total_plays": 12},
            relationships=[],
        )
        data = json.loads(prompt)
        assert "caveat" in data
        assert "Konono No 1" in data["caveat"]
        assert "Pastor T.L. Barrett" in data["caveat"]


class TestNamingOnlyInstruction:
    """The matrix experiment's biggest single lever: 45% → 20% hallucination.

    Adding "When naming shared neighbors, state ONLY their names" to the
    system prompt cut neighbor-characterization hallucination dramatically
    (the model inventing adjectives like "folk innovators like Astor Piazzolla"
    or "introspective indie voices like Jamila Woods" for shared neighbors).
    """

    @pytest.mark.asyncio
    async def test_system_prompt_includes_naming_only_instruction(
        self, client: AsyncClient, narrative_artist_ids: dict[str, int]
    ) -> None:
        """The Anthropic call's ``system=`` argument carries the naming-only sentence."""
        ae_id = narrative_artist_ids["Autechre"]
        sl_id = narrative_artist_ids["Stereolab"]
        resp = await client.get(f"/graph/artists/{ae_id}/explain/{sl_id}/narrative")
        assert resp.status_code == 200

        mock_client = client._transport.app.state.anthropic_client  # type: ignore[union-attr]
        last_call = mock_client.messages.create.call_args
        system_prompt = last_call.kwargs.get("system") or last_call[1].get("system", "")

        # Lock the exact instruction so a future edit that softens or drops it
        # trips the test.
        assert (
            "When naming shared neighbors, state ONLY their names. "
            "Do not describe, characterize, or categorize the neighbors in any way."
        ) in system_prompt


class TestAnonymization:
    """Anonymize >threshold-play artist names before generation, restore after (#227)."""

    def test_no_anonymization_below_threshold(self) -> None:
        """Both artists under the threshold → empty substitution map."""
        from semantic_index.api.narrative import _build_anonymization_map

        source = {"name": "Niche A", "total_plays": 50}
        target = {"name": "Niche B", "total_plays": 100}
        sub_map = _build_anonymization_map(source, target, threshold=800)
        assert sub_map == {}

    def test_high_fame_source_mapped_to_artist_a(self) -> None:
        from semantic_index.api.narrative import _build_anonymization_map

        source = {"name": "Bob Dylan", "total_plays": 1500}
        target = {"name": "Niche Folk", "total_plays": 80}
        sub_map = _build_anonymization_map(source, target, threshold=800)
        assert sub_map == {"Bob Dylan": "Artist A"}

    def test_high_fame_target_mapped_to_artist_b(self) -> None:
        from semantic_index.api.narrative import _build_anonymization_map

        source = {"name": "Niche Folk", "total_plays": 80}
        target = {"name": "Joni Mitchell", "total_plays": 1200}
        sub_map = _build_anonymization_map(source, target, threshold=800)
        assert sub_map == {"Joni Mitchell": "Artist B"}

    def test_both_high_fame(self) -> None:
        from semantic_index.api.narrative import _build_anonymization_map

        source = {"name": "Bob Dylan", "total_plays": 1500}
        target = {"name": "Joan Baez", "total_plays": 950}
        sub_map = _build_anonymization_map(source, target, threshold=800)
        assert sub_map == {"Bob Dylan": "Artist A", "Joan Baez": "Artist B"}

    def test_threshold_is_strict_greater(self) -> None:
        """Exactly at threshold → not anonymized. The cutoff is strict ``>``."""
        from semantic_index.api.narrative import _build_anonymization_map

        source = {"name": "Edge Case", "total_plays": 800}
        target = {"name": "Niche", "total_plays": 50}
        sub_map = _build_anonymization_map(source, target, threshold=800)
        assert sub_map == {}

    def test_apply_substitution_replaces_name_in_meta_dict(self) -> None:
        """Apply the sub map to an artist meta dict — name field gets the alias."""
        from semantic_index.api.narrative import _apply_anonymization

        meta = {"name": "Bob Dylan", "total_plays": 1500, "genre": "Folk"}
        sub_map = {"Bob Dylan": "Artist A"}
        out = _apply_anonymization(meta, sub_map)
        assert out["name"] == "Artist A"
        # Other fields untouched.
        assert out["genre"] == "Folk"
        assert out["total_plays"] == 1500

    def test_apply_substitution_no_op_when_name_not_in_map(self) -> None:
        from semantic_index.api.narrative import _apply_anonymization

        meta = {"name": "Niche", "total_plays": 50}
        out = _apply_anonymization(meta, {"Bob Dylan": "Artist A"})
        assert out["name"] == "Niche"

    def test_reverse_anonymization_restores_real_names(self) -> None:
        """``Artist A`` in the model output gets swapped back to the real name."""
        from semantic_index.api.narrative import _reverse_anonymization

        sub_map = {"Bob Dylan": "Artist A", "Joan Baez": "Artist B"}
        narrative = "WXYC DJs pair Artist A with Artist B in 12 transitions."
        out = _reverse_anonymization(narrative, sub_map)
        assert out == "WXYC DJs pair Bob Dylan with Joan Baez in 12 transitions."

    def test_reverse_anonymization_uses_word_boundaries(self) -> None:
        """Don't substitute ``Tina`` inside ``Argentina`` — issue's stated robustness criterion."""
        from semantic_index.api.narrative import _reverse_anonymization

        # Place an alias that's a substring of an unrelated word in the narrative.
        sub_map = {"Tina Turner": "Tina"}
        narrative = "Argentina's tradition pairs Tina with the local scene."
        out = _reverse_anonymization(narrative, sub_map)
        # The standalone "Tina" gets restored; the "tina" inside "Argentina" doesn't.
        assert out == "Argentina's tradition pairs Tina Turner with the local scene."

    @pytest.mark.asyncio
    async def test_high_fame_pair_anonymized_in_prompt_and_restored_in_response(
        self,
        narrative_db_path: str,
        narrative_artist_ids: dict[str, int],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """End-to-end: high-fame source has its name anonymized in the LLM prompt,
        and the LLM's response (which references ``Artist A``) gets the real name
        substituted back before being returned and cached."""
        # Force Autechre into the high-fame band for this test.
        monkeypatch.setenv("NARRATIVE_ANON_PLAY_THRESHOLD", "10")
        _clear_narrative_cache(narrative_db_path)

        # Mock returns the canned alias-shaped response.
        anon_response = "WXYC DJs pair Artist A with Stereolab across many transitions."
        mock_client = _mock_anthropic_client(anon_response)
        app = create_app(narrative_db_path, anthropic_api_key="test-key")
        app.state.anthropic_client = mock_client
        transport = ASGITransport(app=app)

        ae_id = narrative_artist_ids["Autechre"]  # 50 plays in fixture
        sl_id = narrative_artist_ids["Stereolab"]  # 30 plays in fixture

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get(f"/graph/artists/{ae_id}/explain/{sl_id}/narrative")
        assert resp.status_code == 200

        # The prompt sent to the mock contains the alias, not the real name.
        last_call = mock_client.messages.create.call_args
        messages = last_call.kwargs.get("messages") or last_call[1].get("messages", [])
        sent_prompt = messages[0]["content"]
        assert "Artist A" in sent_prompt
        assert "Autechre" not in sent_prompt, "real high-fame name leaked into the prompt"

        # The response we return has the real name back.
        body = resp.json()
        assert "Autechre" in body["narrative"]
        assert "Artist A" not in body["narrative"]


class TestFewShotExamples:
    """Two gold-standard narratives in the system prompt — the matrix experiment's
    second-largest lever (45% → 21% baseline-wide; 7% on HIGH+RICH+SAME).

    Few-shot demonstrates what a grounded output looks like more reliably than
    negative instructions alone.
    """

    @pytest.mark.asyncio
    async def test_system_prompt_includes_both_examples(
        self, client: AsyncClient, narrative_artist_ids: dict[str, int]
    ) -> None:
        """Both rich-data and thin-data examples appear in the system prompt."""
        ae_id = narrative_artist_ids["Autechre"]
        sl_id = narrative_artist_ids["Stereolab"]
        resp = await client.get(f"/graph/artists/{ae_id}/explain/{sl_id}/narrative")
        assert resp.status_code == 200

        mock_client = client._transport.app.state.anthropic_client  # type: ignore[union-attr]
        last_call = mock_client.messages.create.call_args
        system_prompt = last_call.kwargs.get("system") or last_call[1].get("system", "")

        assert "<example_rich>" in system_prompt
        assert "</example_rich>" in system_prompt
        assert "<example_thin>" in system_prompt
        assert "</example_thin>" in system_prompt

        # Both example narratives are present verbatim — guards against accidental
        # truncation or whitespace mangling in future edits.
        assert "Stereolab" in system_prompt
        assert "Yo La Tengo" in system_prompt
        assert "Tortoise" in system_prompt
        assert "Pastor T.L. Barrett" in system_prompt
        assert "Konono No 1" in system_prompt

    @pytest.mark.asyncio
    async def test_examples_avoid_banned_phrasings(
        self, client: AsyncClient, narrative_artist_ids: dict[str, int]
    ) -> None:
        """Few-shot must not teach the model the failure-mode phrasings.

        ``occupy`` / ``reach for`` / ``represent`` are flagged as banned in
        plan §4 — symptomatic of the inventing-context failure mode. Examples
        that contain them would teach the model to produce them too.
        """
        import re

        ae_id = narrative_artist_ids["Autechre"]
        sl_id = narrative_artist_ids["Stereolab"]
        resp = await client.get(f"/graph/artists/{ae_id}/explain/{sl_id}/narrative")
        assert resp.status_code == 200

        mock_client = client._transport.app.state.anthropic_client  # type: ignore[union-attr]
        last_call = mock_client.messages.create.call_args
        system_prompt = last_call.kwargs.get("system") or last_call[1].get("system", "")

        # Extract just the example narratives so we don't false-trigger on
        # instruction text that legitimately uses words like "characterize".
        examples_text = "\n".join(
            re.findall(r"<example_\w+>(.*?)</example_\w+>", system_prompt, re.DOTALL)
        )
        assert examples_text, "examples should be extractable from system prompt"

        for banned in ("occupy", "reach for", "represent"):
            assert banned.lower() not in examples_text.lower(), (
                f"banned phrasing {banned!r} found in few-shot examples — "
                f"would teach the model the failure-mode phrasing"
            )
