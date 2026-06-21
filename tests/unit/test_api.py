"""Tests for the Graph API scaffolding."""

import os
import time
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from semantic_index.api.schemas import ArtistSummary, NeighborEntry
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
    def test_health_returns_200_with_artist_count(self, client: TestClient):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["artist_count"] == 3

    def test_health_returns_503_when_db_missing(self, client_missing_db: TestClient):
        response = client_missing_db.get("/health")
        assert response.status_code == 503
        data = response.json()
        assert data["status"] == "unhealthy"

    def test_health_reports_graph_db_age_seconds(self, client: TestClient, test_db: Path):
        """The freshness field reflects the serving DB file's mtime.

        The wxyc-canary freshness check (WXYC/wxyc-canary#53) reads this to
        catch SIGKILL-class silent nightly-sync failures (#348/#329).
        """
        # Pin the DB mtime to a known point ~2 hours ago.
        two_hours_ago = time.time() - 7200
        os.utime(test_db, (two_hours_ago, two_hours_ago))

        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        age = data["graph_db_age_seconds"]
        assert isinstance(age, (int, float))
        # Allow generous slack for clock granularity / test execution time.
        assert 7100 <= age <= 7300

    def test_health_age_is_small_for_fresh_db(self, client: TestClient, test_db: Path):
        """A just-written DB reports a near-zero age."""
        os.utime(test_db, None)  # set mtime to now
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["graph_db_age_seconds"] >= 0
        assert data["graph_db_age_seconds"] < 60

    def test_health_age_degrades_gracefully_when_db_missing(self, client_missing_db: TestClient):
        """A missing DB (e.g. mid-swap) must not 500; age is reported as null."""
        response = client_missing_db.get("/health")
        # The unhealthy path already returns 503; it must still carry the
        # freshness field so the canary can distinguish stale from absent.
        assert response.status_code == 503
        data = response.json()
        assert data["status"] == "unhealthy"
        assert data["graph_db_age_seconds"] is None


class TestReadinessEndpoint:
    """`/health/ready` is mounted via `wxyc_fastapi.healthcheck.readiness_router`.

    Snapshot tests for the new route. `/health/ready` is the only route allowed
    to fail with 503; aggregate semantics come from the shared router.
    """

    def test_health_ready_returns_200_when_db_reachable(self, client: TestClient):
        response = client.get("/health/ready")
        assert response.status_code == 200
        data = response.json()
        assert data == {"status": "healthy", "services": {"database": "ok"}}

    def test_health_ready_returns_503_when_db_missing(self, client_missing_db: TestClient):
        response = client_missing_db.get("/health/ready")
        assert response.status_code == 503
        data = response.json()
        assert data == {"status": "unhealthy", "services": {"database": "unavailable"}}

    @pytest.mark.asyncio
    async def test_health_ready_probe_does_not_block_event_loop(
        self, test_db: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Concurrent readiness probes must run in parallel via asyncio.to_thread.

        If `_probe_artist_count_query` were to call `sqlite3.connect` inline
        (sync) on the event loop, two concurrent requests would serialize and
        take ~2x the per-call latency. Offloading to a thread pool lets them
        overlap, so wall-clock time should stay close to the per-call latency.
        """
        import asyncio
        import sqlite3
        import time

        from httpx import ASGITransport, AsyncClient

        from semantic_index.api.app import create_app

        per_call_delay = 0.2  # seconds
        real_connect = sqlite3.connect

        def slow_connect(*args, **kwargs):
            time.sleep(per_call_delay)
            return real_connect(*args, **kwargs)

        monkeypatch.setattr("semantic_index.api.app.sqlite3.connect", slow_connect)

        app = create_app(str(test_db))
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            start = time.monotonic()
            r1, r2 = await asyncio.gather(
                ac.get("/health/ready"),
                ac.get("/health/ready"),
            )
            elapsed = time.monotonic() - start

        assert r1.status_code == 200
        assert r2.status_code == 200
        # Two serialized 0.2s probes would take ~0.4s; parallel should be ~0.2s.
        # Generous ceiling at 1.5x per-call latency leaves room for CI jitter
        # while still failing if the probe blocks the event loop.
        assert elapsed < per_call_delay * 1.5, (
            f"Concurrent readiness probes took {elapsed:.3f}s, "
            f"expected < {per_call_delay * 1.5:.3f}s. The probe is likely "
            "blocking the event loop instead of using asyncio.to_thread."
        )


class TestRootRoute:
    def test_root_returns_explorer_html(self, client: TestClient):
        response = client.get("/")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]
        assert "WXYC" in response.text

    def test_url_state_js_served(self, client: TestClient):
        response = client.get("/url-state.js")
        assert response.status_code == 200
        assert "javascript" in response.headers["content-type"]
        assert "parseURL" in response.text


class TestApiSchemas:
    def test_artist_summary_fields(self):
        artist = ArtistSummary(
            id=1,
            canonical_name="Autechre",
            genre="Electronic",
            total_plays=50,
        )
        assert artist.canonical_name == "Autechre"
        assert artist.genre == "Electronic"
        assert artist.total_plays == 50

    def test_artist_summary_optional_genre(self):
        artist = ArtistSummary(
            id=1,
            canonical_name="Unknown",
            genre=None,
            total_plays=0,
        )
        assert artist.genre is None

    def test_neighbor_entry_fields(self):
        neighbor = NeighborEntry(
            artist=ArtistSummary(id=1, canonical_name="Stereolab", genre="Rock", total_plays=30),
            weight=3.2,
            detail={"raw_count": 5, "pmi": 3.2},
        )
        assert neighbor.artist.canonical_name == "Stereolab"
        assert neighbor.weight == 3.2


class TestAppCreation:
    def test_app_has_title(self, client: TestClient):
        assert "Graph API" in client.app.title  # type: ignore[union-attr]

    def test_openapi_available(self, client: TestClient):
        response = client.get("/openapi.json")
        assert response.status_code == 200
        assert "paths" in response.json()
