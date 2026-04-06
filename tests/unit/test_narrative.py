"""Tests for the narrative endpoint — LLM-generated edge explanations."""

from __future__ import annotations

import json
import sqlite3
import tempfile
from unittest.mock import MagicMock

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from semantic_index.api.app import create_app
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
