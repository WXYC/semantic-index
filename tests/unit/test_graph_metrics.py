"""Tests for graph_metrics: community detection, centrality, and discovery score persistence."""

from __future__ import annotations

import sqlite3
import tempfile

from semantic_index.graph_metrics import (
    GraphMetricsReport,
    _ensure_schema,
    _generate_community_labels,
    compute_and_persist,
)
from semantic_index.models import PmiEdge
from semantic_index.sqlite_export import export_sqlite
from tests.conftest import make_artist_stats


def _build_fixture_db(*, with_acoustic: bool = True) -> str:
    """Build a fixture with 7 artists forming 2 communities connected by a bridge.

    Community A (electronic): Autechre, Boards of Canada, Aphex Twin
    Community B (indie rock): Pavement, Guided by Voices, Yo La Tengo
    Bridge: Stereolab (connected to both clusters)
    """
    path = tempfile.mktemp(suffix=".db")
    stats = {
        "Autechre": make_artist_stats("Autechre", total_plays=50, genre="Electronic"),
        "Boards of Canada": make_artist_stats(
            "Boards of Canada", total_plays=40, genre="Electronic"
        ),
        "Aphex Twin": make_artist_stats("Aphex Twin", total_plays=45, genre="Electronic"),
        "Pavement": make_artist_stats("Pavement", total_plays=35, genre="Rock"),
        "Guided by Voices": make_artist_stats("Guided by Voices", total_plays=30, genre="Rock"),
        "Yo La Tengo": make_artist_stats("Yo La Tengo", total_plays=55, genre="Rock"),
        "Stereolab": make_artist_stats("Stereolab", total_plays=38, genre="Rock"),
    }
    pmi_edges = [
        # Dense electronic cluster
        PmiEdge(source="Autechre", target="Boards of Canada", raw_count=10, pmi=4.0),
        PmiEdge(source="Autechre", target="Aphex Twin", raw_count=8, pmi=3.5),
        PmiEdge(source="Boards of Canada", target="Aphex Twin", raw_count=7, pmi=3.0),
        PmiEdge(source="Boards of Canada", target="Autechre", raw_count=9, pmi=3.8),
        PmiEdge(source="Aphex Twin", target="Autechre", raw_count=6, pmi=2.8),
        PmiEdge(source="Aphex Twin", target="Boards of Canada", raw_count=5, pmi=2.5),
        # Dense indie cluster
        PmiEdge(source="Pavement", target="Guided by Voices", raw_count=9, pmi=3.8),
        PmiEdge(source="Pavement", target="Yo La Tengo", raw_count=6, pmi=2.5),
        PmiEdge(source="Guided by Voices", target="Yo La Tengo", raw_count=5, pmi=2.0),
        PmiEdge(source="Guided by Voices", target="Pavement", raw_count=8, pmi=3.5),
        PmiEdge(source="Yo La Tengo", target="Pavement", raw_count=5, pmi=2.2),
        PmiEdge(source="Yo La Tengo", target="Guided by Voices", raw_count=4, pmi=1.8),
        # Bridge edges (Stereolab connects both clusters)
        PmiEdge(source="Stereolab", target="Autechre", raw_count=4, pmi=2.0),
        PmiEdge(source="Stereolab", target="Pavement", raw_count=3, pmi=1.5),
        PmiEdge(source="Autechre", target="Stereolab", raw_count=3, pmi=1.8),
        PmiEdge(source="Pavement", target="Stereolab", raw_count=2, pmi=1.2),
    ]
    export_sqlite(path, artist_stats=stats, pmi_edges=pmi_edges, xref_edges=[], min_count=2)

    if with_acoustic:
        conn = sqlite3.connect(path)
        # Create acoustic_similarity table
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS acoustic_similarity (
                artist_a_id INTEGER NOT NULL REFERENCES artist(id),
                artist_b_id INTEGER NOT NULL REFERENCES artist(id),
                similarity REAL NOT NULL,
                PRIMARY KEY (artist_a_id, artist_b_id)
            );
            CREATE INDEX IF NOT EXISTS idx_acoustic_sim_a ON acoustic_similarity(artist_a_id);
            CREATE INDEX IF NOT EXISTS idx_acoustic_sim_b ON acoustic_similarity(artist_b_id);
        """)
        # Look up IDs
        ids = {}
        for row in conn.execute("SELECT id, canonical_name FROM artist"):
            ids[row[1]] = row[0]

        # Within-cluster pairs at 0.97 (above 0.95 threshold → counted)
        within_electronic = [
            (ids["Autechre"], ids["Boards of Canada"], 0.97),
            (ids["Autechre"], ids["Aphex Twin"], 0.97),
            (ids["Boards of Canada"], ids["Aphex Twin"], 0.97),
        ]
        within_indie = [
            (ids["Pavement"], ids["Guided by Voices"], 0.97),
            (ids["Pavement"], ids["Yo La Tengo"], 0.97),
            (ids["Guided by Voices"], ids["Yo La Tengo"], 0.97),
        ]
        # Cross-cluster pairs at 0.90 (below 0.95 threshold → not counted)
        cross_cluster = [
            (ids["Autechre"], ids["Pavement"], 0.90),
            (ids["Stereolab"], ids["Aphex Twin"], 0.90),
        ]
        conn.executemany(
            "INSERT INTO acoustic_similarity VALUES (?, ?, ?)",
            within_electronic + within_indie + cross_cluster,
        )
        conn.commit()
        conn.close()

    return path


class TestEnsureSchema:
    def test_adds_columns_to_existing_db(self):
        path = _build_fixture_db(with_acoustic=False)
        conn = sqlite3.connect(path)
        _ensure_schema(conn)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(artist)")}
        assert "community_id" in cols
        assert "betweenness" in cols
        assert "pagerank" in cols
        assert "discovery_score" in cols
        assert "dj_edge_count" in cols
        assert "acoustic_neighbor_count" in cols
        conn.close()

    def test_creates_community_table(self):
        path = _build_fixture_db(with_acoustic=False)
        conn = sqlite3.connect(path)
        _ensure_schema(conn)
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert "community" in tables
        conn.close()

    def test_idempotent(self):
        path = _build_fixture_db(with_acoustic=False)
        conn = sqlite3.connect(path)
        _ensure_schema(conn)
        _ensure_schema(conn)  # second call should not error
        cols = {r[1] for r in conn.execute("PRAGMA table_info(artist)")}
        assert "community_id" in cols
        conn.close()


class TestComputeAndPersist:
    def test_assigns_communities(self):
        path = _build_fixture_db()
        compute_and_persist(path)

        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT canonical_name, community_id FROM artist WHERE community_id IS NOT NULL"
        ).fetchall()
        assert len(rows) == 7

        communities = {r["canonical_name"]: r["community_id"] for r in rows}
        # Electronic cluster should share a community
        assert communities["Autechre"] == communities["Boards of Canada"]
        assert communities["Autechre"] == communities["Aphex Twin"]
        # Indie cluster should share a different community
        assert communities["Pavement"] == communities["Guided by Voices"]
        assert communities["Pavement"] == communities["Yo La Tengo"]
        # The two clusters are different
        assert communities["Autechre"] != communities["Pavement"]
        conn.close()

    def test_bridge_has_highest_betweenness(self):
        path = _build_fixture_db()
        compute_and_persist(path)

        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        stereolab = conn.execute(
            "SELECT betweenness FROM artist WHERE canonical_name = 'Stereolab'"
        ).fetchone()
        autechre = conn.execute(
            "SELECT betweenness FROM artist WHERE canonical_name = 'Autechre'"
        ).fetchone()
        pavement = conn.execute(
            "SELECT betweenness FROM artist WHERE canonical_name = 'Pavement'"
        ).fetchone()
        assert stereolab["betweenness"] > autechre["betweenness"]
        assert stereolab["betweenness"] > pavement["betweenness"]
        conn.close()

    def test_persists_pagerank(self):
        path = _build_fixture_db()
        compute_and_persist(path)

        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT pagerank FROM artist WHERE pagerank IS NOT NULL").fetchall()
        assert len(rows) == 7
        for r in rows:
            assert r["pagerank"] > 0
        conn.close()

    def test_community_table_populated(self):
        path = _build_fixture_db()
        compute_and_persist(path)

        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM community ORDER BY size DESC").fetchall()
        assert len(rows) >= 2
        # Largest communities should have at least 3 members
        assert rows[0]["size"] >= 3
        assert rows[0]["label"] is not None
        conn.close()

    def test_discovery_scores_from_acoustic(self):
        path = _build_fixture_db(with_acoustic=True)
        compute_and_persist(path)

        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        # Artists with within-cluster acoustic neighbors at >= 0.95 should have positive scores
        rows = conn.execute(
            "SELECT canonical_name, discovery_score, dj_edge_count, acoustic_neighbor_count "
            "FROM artist WHERE discovery_score IS NOT NULL AND discovery_score > 0"
        ).fetchall()
        assert len(rows) > 0
        for r in rows:
            assert r["acoustic_neighbor_count"] > 0
            assert r["dj_edge_count"] >= 0
        conn.close()

    def test_discovery_scores_zero_without_acoustic(self):
        path = _build_fixture_db(with_acoustic=False)
        compute_and_persist(path)

        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT discovery_score FROM artist WHERE discovery_score > 0"
        ).fetchall()
        assert len(rows) == 0
        conn.close()

    def test_returns_report(self):
        path = _build_fixture_db()
        report = compute_and_persist(path)
        assert isinstance(report, GraphMetricsReport)
        assert report.community_count >= 2
        assert report.artists_scored == 7
        assert report.largest_community_size >= 3

    def test_idempotent(self):
        path = _build_fixture_db()
        r1 = compute_and_persist(path)
        r2 = compute_and_persist(path)
        assert r1.community_count == r2.community_count
        assert r1.artists_scored == r2.artists_scored


class TestLLMLabels:
    def test_skipped_without_api_key(self):
        """Labels stay as Discogs-derived when api_key is None."""
        path = _build_fixture_db()
        compute_and_persist(path, api_key=None)
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT label FROM community WHERE label IS NOT NULL").fetchall()
        assert len(rows) >= 2
        # Labels should be Discogs-derived (no LLM call made)
        for r in rows:
            assert r["label"] is not None
        conn.close()

    def test_override_takes_precedence(self):
        """Hand-curated overrides bypass LLM entirely."""
        import json
        from unittest.mock import patch

        path = _build_fixture_db()
        compute_and_persist(path)
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        top = conn.execute(
            "SELECT top_artists, label FROM community ORDER BY size DESC LIMIT 1"
        ).fetchone()
        anchor_artists = json.loads(top["top_artists"])[:3]
        # Read community metadata back for _generate_community_labels
        meta = [
            {
                "id": r["id"],
                "size": r["size"],
                "label": r["label"],
                "top_genres": r["top_genres"],
                "top_artists": r["top_artists"],
            }
            for r in conn.execute("SELECT * FROM community ORDER BY size DESC")
        ]
        conn.close()

        override = {frozenset(anchor_artists): "My Custom Label"}
        with patch("semantic_index.graph_metrics.COMMUNITY_LABEL_OVERRIDES", override):
            _generate_community_labels(meta, min_size=1, api_key="fake-key")

        assert meta[0]["label"] == "My Custom Label"

    def test_graceful_on_api_failure(self):
        """LLM failures preserve Discogs labels for all communities."""
        from unittest.mock import MagicMock, patch

        path = _build_fixture_db()
        compute_and_persist(path)
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        meta = [
            {
                "id": r["id"],
                "size": r["size"],
                "label": r["label"],
                "top_genres": r["top_genres"],
                "top_artists": r["top_artists"],
            }
            for r in conn.execute("SELECT * FROM community ORDER BY size DESC")
        ]
        conn.close()
        baseline_labels = [m["label"] for m in meta]

        mock_client = MagicMock()
        mock_client.messages.create.side_effect = Exception("API down")
        with patch("semantic_index.graph_metrics.anthropic") as mock_mod:
            mock_mod.Anthropic.return_value = mock_client
            _generate_community_labels(meta, min_size=1, api_key="fake-key")

        for m, baseline in zip(meta, baseline_labels, strict=True):
            assert m["label"] == baseline

    def test_generated_label_stored(self):
        """Mock a successful LLM call and verify the generated label is used."""
        from unittest.mock import MagicMock, patch

        path = _build_fixture_db()
        compute_and_persist(path)
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        meta = [
            {
                "id": r["id"],
                "size": r["size"],
                "label": r["label"],
                "top_genres": r["top_genres"],
                "top_artists": r["top_artists"],
            }
            for r in conn.execute("SELECT * FROM community ORDER BY size DESC")
        ]
        conn.close()

        mock_content = MagicMock()
        mock_content.text = "Beat-Driven IDM"
        mock_response = MagicMock()
        mock_response.content = [mock_content]
        mock_client = MagicMock()
        mock_client.messages.create.return_value = mock_response

        with patch("semantic_index.graph_metrics.anthropic") as mock_mod:
            mock_mod.Anthropic.return_value = mock_client
            _generate_community_labels(meta, min_size=1, api_key="fake-key")

        labels = [m["label"] for m in meta]
        assert "Beat-Driven IDM" in labels
