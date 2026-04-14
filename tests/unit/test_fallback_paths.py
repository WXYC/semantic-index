"""Fallback path tests for semantic-index.

Tests the graceful degradation when the discogs-cache PostgreSQL database
is unavailable:

1. DiscogsClient: When PG connection fails, search_artist returns None,
   get_releases_for_artist returns [], and get_bulk_enrichment returns {}.
2. ArtistReconciler: When cache connection returns None, reconcile_batch
   marks all artists as errored (not crash). The pipeline can still proceed
   with local-only data (no enrichment, no reconciliation).
3. Fallback parity: The entity IDs assigned to artists are identical
   regardless of whether reconciliation succeeded or was skipped, because
   entity IDs come from the local SQLite entity store, not PG.

Pattern: Use monkeypatch/mock to simulate PG unavailability. Run both paths
on the same fixture data, assert identical results or graceful degradation.
"""

from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock, patch

import pytest
from semantic_index.entity_store import EntityStore
from semantic_index.reconciliation import ArtistReconciler

from semantic_index.discogs_client import DiscogsClient

# ---------------------------------------------------------------------------
# Fixture data: representative WXYC artists
# ---------------------------------------------------------------------------

CANONICAL_ARTISTS = [
    "Stereolab",
    "Autechre",
    "Cat Power",
    "Jessica Pratt",
    "Juana Molina",
    "Father John Misty",
    "Chuquimamani-Condori",
    "Sessa",
]

# Old artist table schema (before EntityStore migration adds columns)
_OLD_ARTIST_SCHEMA = """
CREATE TABLE artist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_name TEXT NOT NULL UNIQUE,
    genre TEXT,
    total_plays INTEGER NOT NULL DEFAULT 0,
    active_first_year INTEGER,
    active_last_year INTEGER,
    dj_count INTEGER NOT NULL DEFAULT 0,
    request_ratio REAL NOT NULL DEFAULT 0.0,
    show_count INTEGER NOT NULL DEFAULT 0,
    discogs_artist_id INTEGER
);
"""


@pytest.fixture()
def store(tmp_path) -> EntityStore:
    """An initialized EntityStore with the old artist table (triggers migration)."""
    db_path = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_path)
    conn.executescript(_OLD_ARTIST_SCHEMA)
    conn.close()
    s = EntityStore(db_path)
    s.initialize()
    return s


@pytest.fixture()
def fresh_store(tmp_path) -> EntityStore:
    """An initialized EntityStore with no pre-existing artist table."""
    db_path = str(tmp_path / "fresh.db")
    s = EntityStore(db_path)
    s.initialize()
    return s


# ---------------------------------------------------------------------------
# DiscogsClient fallback: PG connection failure
# ---------------------------------------------------------------------------


class TestDiscogsClientPgUnavailable:
    """When the PG cache_dsn connection fails, DiscogsClient methods degrade gracefully."""

    def test_search_artist_returns_none_when_pg_down(self) -> None:
        """search_artist returns None when PG is unreachable and no API configured."""
        client = DiscogsClient(
            cache_dsn="postgresql://bad-host:5432/nonexistent", api_base_url=None
        )
        # Patch _get_cache_conn to return None (simulating connection failure)
        with patch.object(client, "_get_cache_conn", return_value=None):
            result = client.search_artist("Stereolab")
        assert result is None

    def test_get_releases_returns_empty_when_pg_down(self) -> None:
        """get_releases_for_artist returns empty list when PG is unreachable."""
        client = DiscogsClient(
            cache_dsn="postgresql://bad-host:5432/nonexistent", api_base_url=None
        )
        with patch.object(client, "_get_cache_conn", return_value=None):
            result = client.get_releases_for_artist("Stereolab")
        assert result == []

    def test_get_bulk_enrichment_returns_empty_when_pg_down(self) -> None:
        """get_bulk_enrichment returns empty dict when PG is unreachable."""
        client = DiscogsClient(
            cache_dsn="postgresql://bad-host:5432/nonexistent", api_base_url=None
        )
        with patch.object(client, "_get_cache_conn", return_value=None):
            result = client.get_bulk_enrichment(CANONICAL_ARTISTS)
        assert result == {}

    def test_no_cache_dsn_returns_none(self) -> None:
        """When cache_dsn is None, _get_cache_conn returns None."""
        client = DiscogsClient(cache_dsn=None, api_base_url=None)
        assert client._get_cache_conn() is None

    def test_search_artist_no_cache_no_api(self) -> None:
        """When both cache and API are None, search_artist returns None."""
        client = DiscogsClient(cache_dsn=None, api_base_url=None)
        result = client.search_artist("Stereolab")
        assert result is None


class TestDiscogsClientConnectionRetry:
    """When PG connection drops mid-session, the client retries on next call."""

    def test_closed_connection_triggers_reconnect_attempt(self) -> None:
        """A closed mock connection triggers a reconnect attempt."""
        client = DiscogsClient(cache_dsn="postgresql://bad-host:5432/db", api_base_url=None)
        mock_conn = MagicMock()
        mock_conn.closed = True  # Connection is closed
        client._cache_conn = mock_conn

        # Since we can't actually connect, _get_cache_conn will try and fail
        # and return None (not crash)
        with patch("semantic_index.discogs_client.psycopg") as mock_psycopg:
            mock_psycopg.connect.side_effect = Exception("Connection refused")
            result = client._get_cache_conn()
        assert result is None


# ---------------------------------------------------------------------------
# ArtistReconciler fallback: PG unavailable during reconciliation
# ---------------------------------------------------------------------------


class TestReconcilerPgUnavailable:
    """When PG is unavailable, reconcile_batch should handle errors gracefully."""

    def test_reconcile_batch_with_no_cache(self, store: EntityStore) -> None:
        """When cache conn is None, all artists are marked errored, not crashed."""
        store.bulk_upsert_artists(CANONICAL_ARTISTS)

        # Client with no PG connection
        client = DiscogsClient(cache_dsn=None, api_base_url=None)
        reconciler = ArtistReconciler(store, client)

        report = reconciler.reconcile_batch()
        # All artists attempted, none succeeded (no_match since cache returned {})
        assert report.attempted == len(CANONICAL_ARTISTS)
        assert report.succeeded == 0
        assert report.no_match == len(CANONICAL_ARTISTS)

    def test_reconcile_members_with_no_cache(self, store: EntityStore) -> None:
        """reconcile_members handles missing PG gracefully."""
        store.bulk_upsert_artists(["Stereolab", "Autechre"])
        # Mark them as no_match first (precondition for member reconciliation)
        for artist_id, _name in store.get_unreconciled_artists():
            store.update_reconciliation_status(artist_id, "no_match")

        client = DiscogsClient(cache_dsn=None, api_base_url=None)
        reconciler = ArtistReconciler(store, client)

        report = reconciler.reconcile_members()
        assert report.attempted == 2
        assert report.succeeded == 0


# ---------------------------------------------------------------------------
# Entity ID stability: IDs assigned by entity store are PG-independent
# ---------------------------------------------------------------------------


class TestEntityIdStabilityWithoutPg:
    """Entity IDs assigned by the local SQLite entity store must be identical
    regardless of whether PG reconciliation ran.

    The entity store assigns auto-increment IDs. Since entity IDs come from
    SQLite, they depend on insertion order, not PG data. This test verifies
    that the artist table has consistent IDs after bulk_upsert_artists.
    """

    def test_artist_ids_stable_across_runs(self, tmp_path) -> None:
        """Two fresh stores with the same artists get the same IDs."""
        db_path_a = str(tmp_path / "a.db")
        db_path_b = str(tmp_path / "b.db")

        store_a = EntityStore(db_path_a)
        store_a.initialize()
        store_a.bulk_upsert_artists(CANONICAL_ARTISTS)

        store_b = EntityStore(db_path_b)
        store_b.initialize()
        store_b.bulk_upsert_artists(CANONICAL_ARTISTS)

        # Both stores should assign the same artist IDs
        for name in CANONICAL_ARTISTS:
            artist_a = store_a.get_artist_by_name(name)
            artist_b = store_b.get_artist_by_name(name)
            assert artist_a is not None and artist_b is not None
            assert artist_a["id"] == artist_b["id"], (
                f"Artist ID mismatch for {name!r}: "
                f"store_a={artist_a['id']}, store_b={artist_b['id']}"
            )

    def test_reconciliation_does_not_change_artist_ids(self, store: EntityStore) -> None:
        """Artist IDs remain the same before and after (failed) reconciliation."""
        store.bulk_upsert_artists(CANONICAL_ARTISTS)

        # Record IDs before reconciliation
        ids_before = {}
        for name in CANONICAL_ARTISTS:
            artist = store.get_artist_by_name(name)
            assert artist is not None
            ids_before[name] = artist["id"]

        # Run reconciliation with no PG (all no_match)
        client = DiscogsClient(cache_dsn=None, api_base_url=None)
        reconciler = ArtistReconciler(store, client)
        reconciler.reconcile_batch()

        # IDs should not have changed
        for name in CANONICAL_ARTISTS:
            artist = store.get_artist_by_name(name)
            assert artist is not None
            assert artist["id"] == ids_before[name], (
                f"Artist ID changed for {name!r}: before={ids_before[name]}, after={artist['id']}"
            )


class TestReconciliationStatusWithoutPg:
    """When PG is unavailable, artists get 'no_match' status (not crash or corrupt state)."""

    def test_all_artists_get_no_match_status(self, store: EntityStore) -> None:
        """After reconcile_batch with no PG, all artists have no_match status."""
        store.bulk_upsert_artists(CANONICAL_ARTISTS)

        client = DiscogsClient(cache_dsn=None, api_base_url=None)
        reconciler = ArtistReconciler(store, client)
        reconciler.reconcile_batch()

        for name in CANONICAL_ARTISTS:
            artist = store.get_artist_by_name(name)
            assert artist is not None
            assert artist["reconciliation_status"] == "no_match", (
                f"Expected no_match for {name!r}, got {artist['reconciliation_status']!r}"
            )

    def test_graceful_error_message_not_crash(self, store: EntityStore) -> None:
        """A mock PG that raises on execute should not crash reconcile_batch."""
        store.bulk_upsert_artists(["Stereolab"])

        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_conn.execute = MagicMock(side_effect=Exception("connection reset"))

        client = DiscogsClient(cache_dsn="postgresql://test", api_base_url=None)
        client._cache_conn = mock_conn
        reconciler = ArtistReconciler(store, client)

        # Should not raise -- errors are caught and logged
        report = reconciler.reconcile_batch()
        assert report.errored == 1
        assert report.succeeded == 0
