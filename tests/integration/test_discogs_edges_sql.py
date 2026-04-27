"""Integration tests for SQL-based Discogs edge computation via DiscogsClient.

These tests query the materialized summary tables in a populated discogs-cache
PostgreSQL (port 5433 by default). They are skipped when the DB is unreachable
or when the expected tables/data are absent. The DSN comes from the
``DATABASE_URL_DISCOGS`` env var, falling back to ``postgresql://jake@localhost/discogs``
for the original developer's local layout.
"""

import os

import psycopg
import pytest

from semantic_index.discogs_client import DiscogsClient
from semantic_index.models import (
    CompilationEdge,
    LabelFamilyEdge,
    SharedPersonnelEdge,
    SharedStyleEdge,
)


@pytest.fixture
def discogs_dsn():
    """DSN for the discogs-cache PostgreSQL.

    Honours ``DATABASE_URL_DISCOGS`` for CI / non-default layouts. Falls back to
    the historical local-developer DSN.
    """
    return os.environ.get("DATABASE_URL_DISCOGS", "postgresql://jake@localhost/discogs")


@pytest.fixture
def client(discogs_dsn):
    """DiscogsClient with cache connection only (no API)."""
    return DiscogsClient(cache_dsn=discogs_dsn, api_base_url=None)


@pytest.fixture
def _verify_discogs_db(discogs_dsn):
    """Skip if discogs-cache database is not available."""
    try:
        conn = psycopg.connect(discogs_dsn)
        conn.execute("SELECT 1 FROM artist_style_summary LIMIT 1")
        conn.close()
    except Exception:
        pytest.skip("discogs-cache PostgreSQL not available")


@pytest.mark.pg
@pytest.mark.usefixtures("_verify_discogs_db")
class TestComputeSharedStylesSql:
    """Tests for SQL-based shared style edge computation."""

    def test_returns_shared_style_edges(self, client):
        """Known Warp artists should share IDM/Abstract styles."""
        artist_names = ["autechre", "aphex twin", "boards of canada"]
        edges = client.compute_shared_styles_sql(artist_names, min_jaccard=0.05)

        assert isinstance(edges, list)
        assert all(isinstance(e, SharedStyleEdge) for e in edges)
        # These artists definitely share styles in the Discogs data
        assert len(edges) > 0
        # Jaccard should be between 0 and 1
        for e in edges:
            assert 0 < e.jaccard <= 1.0
            assert len(e.shared_tags) > 0

    def test_max_artists_reduces_edges(self, client):
        """A low max_artists should produce fewer edges than no cap."""
        artist_names = ["autechre", "aphex twin", "boards of canada", "squarepusher"]
        edges_uncapped = client.compute_shared_styles_sql(artist_names, min_jaccard=0.05)
        edges_capped = client.compute_shared_styles_sql(
            artist_names, min_jaccard=0.05, max_artists=2
        )

        # Capped should have <= uncapped edges (may be equal if no popular styles)
        assert len(edges_capped) <= len(edges_uncapped)

    def test_empty_artist_list(self, client):
        edges = client.compute_shared_styles_sql([], min_jaccard=0.1)

        assert edges == []

    def test_unknown_artists_return_empty(self, client):
        edges = client.compute_shared_styles_sql(["zzz_nonexistent_artist_12345"], min_jaccard=0.1)

        assert edges == []


@pytest.mark.pg
@pytest.mark.usefixtures("_verify_discogs_db")
class TestComputeSharedPersonnelSql:
    """Tests for SQL-based shared personnel edge computation."""

    def test_returns_shared_personnel_edges(self, client):
        """Known Warp artists should share some session musicians."""
        artist_names = ["autechre", "aphex twin", "boards of canada"]
        edges = client.compute_shared_personnel_sql(artist_names)

        assert isinstance(edges, list)
        assert all(isinstance(e, SharedPersonnelEdge) for e in edges)

    def test_max_artists_reduces_edges(self, client):
        artist_names = ["autechre", "aphex twin", "boards of canada", "squarepusher"]
        edges_uncapped = client.compute_shared_personnel_sql(artist_names)
        edges_capped = client.compute_shared_personnel_sql(artist_names, max_artists=2)

        assert len(edges_capped) <= len(edges_uncapped)

    def test_empty_artist_list(self, client):
        edges = client.compute_shared_personnel_sql([])

        assert edges == []


@pytest.mark.pg
@pytest.mark.usefixtures("_verify_discogs_db")
class TestComputeLabelFamilySql:
    """Tests for SQL-based label family edge computation."""

    def test_returns_label_family_edges(self, client):
        """Artists on the same label should produce edges."""
        # Both on Warp Records
        artist_names = ["autechre", "aphex twin"]
        edges = client.compute_label_family_sql(artist_names)

        assert isinstance(edges, list)
        assert all(isinstance(e, LabelFamilyEdge) for e in edges)
        assert len(edges) > 0
        # Should include "Warp Records" or similar
        all_labels = {label for e in edges for label in e.shared_labels}
        assert len(all_labels) > 0

    def test_max_label_artists_excludes_mega_labels(self, client):
        artist_names = ["autechre", "aphex twin", "boards of canada"]
        edges_uncapped = client.compute_label_family_sql(artist_names)
        edges_capped = client.compute_label_family_sql(artist_names, max_label_artists=2)

        assert len(edges_capped) <= len(edges_uncapped)

    def test_empty_artist_list(self, client):
        edges = client.compute_label_family_sql([])

        assert edges == []


@pytest.mark.pg
@pytest.mark.usefixtures("_verify_discogs_db")
class TestComputeCompilationSql:
    """Tests for SQL-based compilation co-appearance edge computation."""

    def test_returns_compilation_edges(self, client):
        """Artists on Warp compilations should share co-appearances."""
        artist_names = ["autechre", "aphex twin", "boards of canada", "squarepusher"]
        edges = client.compute_compilation_sql(artist_names)

        assert isinstance(edges, list)
        assert all(isinstance(e, CompilationEdge) for e in edges)

    def test_empty_artist_list(self, client):
        edges = client.compute_compilation_sql([])

        assert edges == []
