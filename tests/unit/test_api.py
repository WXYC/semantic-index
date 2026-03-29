"""Tests for the Graph API scaffolding."""

from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from semantic_index.api.models import ArtistResponse, NeighborResponse, SearchResult
from semantic_index.models import ArtistStats, PmiEdge
from semantic_index.sqlite_export import export_sqlite


@pytest.fixture()
def test_db(tmp_path: Path) -> Path:
    """Create a test SQLite database with sample WXYC artists."""
    db_path = tmp_path / "test_graph.db"
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
        "Father John Misty": ArtistStats(
            canonical_name="Father John Misty",
            total_plays=20,
            genre="Rock",
            active_first_year=2015,
            active_last_year=2025,
            dj_count=8,
            request_ratio=0.15,
            show_count=18,
        ),
    }
    edges = [
        PmiEdge(source="Autechre", target="Stereolab", raw_count=5, pmi=3.2),
        PmiEdge(source="Stereolab", target="Father John Misty", raw_count=3, pmi=1.8),
    ]
    export_sqlite(str(db_path), artist_stats=stats, pmi_edges=edges, xref_edges=[], min_count=1)
    return db_path


@pytest.fixture()
def client(test_db: Path) -> TestClient:
    """Create a test client with the test database."""
    with patch.dict("os.environ", {"DB_PATH": str(test_db)}):
        from semantic_index.api.app import create_app

        app = create_app(str(test_db))
        with TestClient(app) as tc:
            yield tc


@pytest.fixture()
def client_missing_db(tmp_path: Path) -> TestClient:
    """Create a test client pointing to a nonexistent database."""
    missing = tmp_path / "nonexistent.db"
    with patch.dict("os.environ", {"DB_PATH": str(missing)}):
        from semantic_index.api.app import create_app

        app = create_app(str(missing))
        with TestClient(app) as tc:
            yield tc


class TestHealthEndpoint:
    """Health endpoint tests — placeholder until /health is added."""


class TestApiModels:
    def test_artist_response_fields(self):
        artist = ArtistResponse(
            id=1,
            canonical_name="Autechre",
            genre="Electronic",
            total_plays=50,
            active_first_year=2004,
            active_last_year=2025,
            dj_count=15,
            request_ratio=0.1,
            show_count=40,
        )
        assert artist.canonical_name == "Autechre"
        assert artist.genre == "Electronic"
        assert artist.total_plays == 50

    def test_artist_response_optional_fields(self):
        artist = ArtistResponse(
            id=1,
            canonical_name="Unknown",
            total_plays=0,
        )
        assert artist.genre is None
        assert artist.active_first_year is None
        assert artist.active_last_year is None

    def test_neighbor_response_fields(self):
        neighbor = NeighborResponse(
            artist=ArtistResponse(id=1, canonical_name="Stereolab", total_plays=30),
            raw_count=5,
            pmi=3.2,
        )
        assert neighbor.artist.canonical_name == "Stereolab"
        assert neighbor.pmi == 3.2

    def test_search_result_fields(self):
        result = SearchResult(
            id=1,
            canonical_name="Autechre",
            genre="Electronic",
        )
        assert result.canonical_name == "Autechre"


class TestAppCreation:
    def test_app_has_title(self, client: TestClient):
        assert "Graph API" in client.app.title  # type: ignore[union-attr]

    def test_openapi_available(self, client: TestClient):
        response = client.get("/openapi.json")
        assert response.status_code == 200
        assert "paths" in response.json()
