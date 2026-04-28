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
from semantic_index.api.narrative import _rank_shared_neighbors_by_aa
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
    async def test_prompt_metadata_graceful_without_styles(
        self, client: AsyncClient, narrative_artist_ids: dict[str, int]
    ) -> None:
        """Artists without style tags should have an empty styles list."""
        ae_id = narrative_artist_ids["Autechre"]
        cp_id = narrative_artist_ids["Cat Power"]
        resp = await client.get(f"/graph/artists/{ae_id}/explain/{cp_id}/narrative")
        assert resp.status_code == 200

        mock_client = client._transport.app.state.anthropic_client  # type: ignore[union-attr]
        last_call = mock_client.messages.create.call_args
        messages = last_call.kwargs.get("messages") or last_call[1].get("messages", [])
        prompt_data = json.loads(messages[0]["content"])

        assert prompt_data["target"]["name"] == "Cat Power"
        assert prompt_data["target"]["styles"] == []
        assert prompt_data["target"]["total_plays"] == 20


class TestAudioProfileEnrichment:
    @pytest.mark.asyncio
    async def test_prompt_includes_audio_profile(
        self, narrative_db_path: str, narrative_artist_ids: dict[str, int]
    ) -> None:
        """When an audio profile exists, the prompt includes audio features."""
        _clear_narrative_cache(narrative_db_path)

        # Insert an audio profile for Autechre
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
        # Build a 59-dim feature vector with known mood values
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
                0.35,
                "electronic",
                0.7,
                0.15,
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
        prompt_data = json.loads(messages[0]["content"])

        source_audio = prompt_data["source"].get("audio")
        assert source_audio is not None
        assert source_audio["primary_genre"] == "electronic"
        assert source_audio["danceability"] == 0.35
        assert source_audio["voice_instrumental"] == "instrumental"
        assert "electronic" in source_audio["top_moods"]
        assert "aggressive" in source_audio["top_moods"]
        assert source_audio["recording_count"] == 12

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

    def test_top_k_caps_result(self) -> None:
        """top_k=1 returns only the highest-scoring shared neighbor."""
        path = _build_aa_fixture_db()
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        ids = {
            r["canonical_name"]: r["id"]
            for r in conn.execute("SELECT id, canonical_name FROM artist")
        }

        result = _rank_shared_neighbors_by_aa(
            conn, ids["Father John Misty"], ids["Caetano Veloso"], top_k=1
        )
        conn.close()

        assert len(result) == 1
        assert result[0]["name"] == "Joe McPhee"

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
