"""Full pipeline E2E test: run the complete semantic-index pipeline on the tubafrenzy fixture dump.

Runs run_pipeline.py as an in-process call against the tubafrenzy fixture dump at
tubafrenzy/scripts/dev/fixtures/wxycmusic-fixture.sql. Verifies:
  - SQLite output has correct schema (all expected tables exist)
  - Non-zero artist count
  - Non-zero dj_transition edges with PMI scores computed
  - Community assignments exist (Louvain)
  - PageRank values are valid (non-null, in (0, 1])
  - Cross-reference edges are extracted
  - Facet tables are populated

The fixture has ~750 music entries and ~500 unique artists, enough for
structural validation but not meaningful PMI interpretation.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from pathlib import Path

import pytest

pytestmark = pytest.mark.e2e

# Walk up from this file to find the WXYC org directory containing sibling repos.
_FIXTURE_RELATIVE = "tubafrenzy/scripts/dev/fixtures/wxycmusic-fixture.sql"


def _find_fixture() -> Path:
    override = os.environ.get("TUBAFRENZY_FIXTURE")
    if override:
        return Path(override)
    d = Path(__file__).resolve().parent
    while d != d.parent:
        candidate = d / _FIXTURE_RELATIVE
        if candidate.exists():
            return candidate
        d = d.parent
    return Path(_FIXTURE_RELATIVE)


FIXTURE_PATH = _find_fixture()


class TestFullPipeline:
    """Run the complete pipeline and verify the SQLite output."""

    @pytest.fixture(autouse=True, scope="class")
    def _run_pipeline(self):
        """Run the full pipeline in-process against the fixture dump."""
        if not FIXTURE_PATH.exists():
            pytest.skip(f"Fixture dump not found at {FIXTURE_PATH}")

        tmpdir = tempfile.mkdtemp(prefix="semantic_index_e2e_")
        self.__class__._tmpdir = tmpdir
        self.__class__._db_path = os.path.join(tmpdir, "wxyc_artist_graph.db")

        from run_pipeline import main

        main(
            [
                str(FIXTURE_PATH),
                "--output-dir",
                tmpdir,
                "--min-count",
                "1",
                "--skip-enrichment",
            ]
        )

        yield

    @pytest.fixture(autouse=True)
    def _connect(self):
        """Provide a SQLite connection to the pipeline output for each test."""
        self.conn = sqlite3.connect(self.__class__._db_path)
        self.conn.row_factory = sqlite3.Row
        yield
        self.conn.close()

    # -- Schema validation --

    def test_expected_tables_exist(self) -> None:
        """All core pipeline tables are present in the output database."""
        rows = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        tables = {r["name"] for r in rows}

        expected = {
            "artist",
            "dj_transition",
            "cross_reference",
            "community",
            "dj",
            "play",
            "artist_month_count",
            "artist_dj_count",
            "month_total",
            "dj_total",
        }
        missing = expected - tables
        assert not missing, f"Missing tables: {missing}"

    def test_artist_table_schema(self) -> None:
        """The artist table has all expected columns."""
        rows = self.conn.execute("PRAGMA table_info(artist)").fetchall()
        columns = {r["name"] for r in rows}

        expected_columns = {
            "id",
            "canonical_name",
            "genre",
            "total_plays",
            "active_first_year",
            "active_last_year",
            "dj_count",
            "community_id",
            "betweenness",
            "pagerank",
            "request_ratio",
            "show_count",
        }
        missing = expected_columns - columns
        assert not missing, f"Missing artist columns: {missing}"

    def test_dj_transition_table_schema(self) -> None:
        """The dj_transition table has source_id, target_id, raw_count, pmi."""
        rows = self.conn.execute("PRAGMA table_info(dj_transition)").fetchall()
        columns = {r["name"] for r in rows}

        expected = {"source_id", "target_id", "raw_count", "pmi"}
        missing = expected - columns
        assert not missing, f"Missing dj_transition columns: {missing}"

    # -- Non-zero counts --

    def test_nonzero_artist_count(self) -> None:
        """The pipeline produces a non-zero number of artists."""
        count = self.conn.execute("SELECT count(*) FROM artist").fetchone()[0]
        assert count > 0, "artist table is empty"
        # The fixture has ~500 unique artist names
        assert count > 100, f"Expected >100 artists from fixture, got {count}"

    def test_nonzero_dj_transition_count(self) -> None:
        """The pipeline produces non-zero DJ transition edges."""
        count = self.conn.execute("SELECT count(*) FROM dj_transition").fetchone()[0]
        assert count > 0, "dj_transition table is empty"

    def test_all_transitions_have_pmi(self) -> None:
        """Every dj_transition edge has a non-null PMI score."""
        null_count = self.conn.execute(
            "SELECT count(*) FROM dj_transition WHERE pmi IS NULL"
        ).fetchone()[0]
        assert null_count == 0, f"{null_count} dj_transition rows have NULL pmi"

    def test_all_transitions_have_positive_raw_count(self) -> None:
        """Every dj_transition edge has raw_count >= 1."""
        bad = self.conn.execute(
            "SELECT count(*) FROM dj_transition WHERE raw_count < 1"
        ).fetchone()[0]
        assert bad == 0, f"{bad} dj_transition rows have raw_count < 1"

    # -- Graph metrics --

    def test_community_assignments_exist(self) -> None:
        """At least one community is assigned by Louvain."""
        count = self.conn.execute("SELECT count(*) FROM community").fetchone()[0]
        assert count > 0, "community table is empty -- Louvain did not run"

    def test_artists_have_community_ids(self) -> None:
        """Some artists have community_id set by graph_metrics."""
        count = self.conn.execute(
            "SELECT count(*) FROM artist WHERE community_id IS NOT NULL"
        ).fetchone()[0]
        assert count > 0, "No artists have community_id"

    def test_pagerank_values_valid(self) -> None:
        """Artists with PageRank have values in (0, 1]."""
        rows = self.conn.execute(
            "SELECT canonical_name, pagerank FROM artist WHERE pagerank IS NOT NULL"
        ).fetchall()
        assert len(rows) > 0, "No artists have PageRank values"
        for row in rows:
            pr = row["pagerank"]
            assert 0 < pr <= 1.0, f"Artist '{row['canonical_name']}' has invalid PageRank {pr}"

    def test_betweenness_values_nonnegative(self) -> None:
        """Artists with betweenness centrality have non-negative values."""
        rows = self.conn.execute(
            "SELECT canonical_name, betweenness FROM artist WHERE betweenness IS NOT NULL"
        ).fetchall()
        assert len(rows) > 0, "No artists have betweenness values"
        for row in rows:
            assert row["betweenness"] >= 0, (
                f"Artist '{row['canonical_name']}' has negative betweenness {row['betweenness']}"
            )

    # -- Cross-references --

    def test_cross_reference_edges_extracted(self) -> None:
        """Cross-reference edges are extracted from the fixture.

        Both extraction paths (LIBRARY_CODE_CROSS_REFERENCE and
        RELEASE_CROSS_REFERENCE) must produce at least one edge. The fixture's
        historical cross-ref rows reference LIBRARY_CODE / LIBRARY_RELEASE IDs
        that fall outside the top-1000-by-ID truncation window and are silently
        skipped at extraction time; the fixture compensates by either (a)
        appending synthetic rows whose FKs land inside the truncation window
        or (b) pulling in the extra referenced rows via the supplemental
        ``--no-create-info`` mysqldump invocations in
        ``scripts/dev/generate-fixture-dump.sh``. Either mechanism keeps both
        ``source`` flavours populated; if you regenerate the fixture and this
        test starts failing, that's the contract that broke. See
        WXYC/semantic-index#185 and WXYC/tubafrenzy#486.

        Source of truth for the cross-ref IDs:
        ``tubafrenzy/scripts/dev/fixtures/wxycmusic-fixture.sql``.
        """
        rows = self.conn.execute(
            "SELECT source, count(*) FROM cross_reference GROUP BY source"
        ).fetchall()
        by_source = {row["source"]: row[1] for row in rows}
        total = sum(by_source.values())
        assert total > 0, "cross_reference table is empty"
        assert by_source.get("library_code", 0) > 0, (
            "no library_code cross-ref edges -- LIBRARY_CODE_CROSS_REFERENCE "
            "extraction path is not exercised by the fixture"
        )
        assert by_source.get("release", 0) > 0, (
            "no release cross-ref edges -- RELEASE_CROSS_REFERENCE extraction "
            "path is not exercised by the fixture"
        )

    # -- Facet tables --

    def test_play_table_populated(self) -> None:
        """The play table has entries from the fixture flowsheet."""
        count = self.conn.execute("SELECT count(*) FROM play").fetchone()[0]
        assert count > 0, "play table is empty"

    def test_dj_table_populated(self) -> None:
        """The dj table has entries from the fixture shows."""
        count = self.conn.execute("SELECT count(*) FROM dj").fetchone()[0]
        assert count > 0, "dj table is empty"

    def test_month_total_populated(self) -> None:
        """The month_total table has aggregated monthly play counts."""
        count = self.conn.execute("SELECT count(*) FROM month_total").fetchone()[0]
        assert count > 0, "month_total table is empty"

    # -- GEXF output --

    def test_gexf_output_exists(self) -> None:
        """The GEXF file is written alongside the SQLite database."""
        gexf_path = Path(self.__class__._tmpdir) / "wxyc_artist_pmi.gexf"
        assert gexf_path.exists(), f"GEXF file not found at {gexf_path}"
        assert gexf_path.stat().st_size > 0, "GEXF file is empty"

    def test_gexf_is_valid_graph(self) -> None:
        """The GEXF file is parseable by NetworkX."""
        import networkx as nx

        gexf_path = Path(self.__class__._tmpdir) / "wxyc_artist_pmi.gexf"
        graph = nx.read_gexf(str(gexf_path))
        assert isinstance(graph, nx.Graph)
        assert graph.number_of_nodes() > 0
        assert graph.number_of_edges() > 0

    # -- Referential integrity --

    def test_dj_transition_fk_integrity(self) -> None:
        """All dj_transition source_id and target_id reference existing artists."""
        orphan_sources = self.conn.execute(
            "SELECT count(*) FROM dj_transition dt "
            "LEFT JOIN artist a ON dt.source_id = a.id "
            "WHERE a.id IS NULL"
        ).fetchone()[0]
        orphan_targets = self.conn.execute(
            "SELECT count(*) FROM dj_transition dt "
            "LEFT JOIN artist a ON dt.target_id = a.id "
            "WHERE a.id IS NULL"
        ).fetchone()[0]
        assert orphan_sources == 0, f"{orphan_sources} transitions have orphan source_id"
        assert orphan_targets == 0, f"{orphan_targets} transitions have orphan target_id"

    def test_play_artist_fk_integrity(self) -> None:
        """All play.artist_id reference existing artists."""
        orphans = self.conn.execute(
            "SELECT count(*) FROM play p "
            "LEFT JOIN artist a ON p.artist_id = a.id "
            "WHERE a.id IS NULL"
        ).fetchone()[0]
        assert orphans == 0, f"{orphans} plays have orphan artist_id"


class TestFullPipelineWithEnrichment:
    """Run the pipeline with Discogs enrichment using a canned bulk-enrichment payload.

    The discogs-cache PostgreSQL is unavailable in CI and slow to populate
    locally, so this class monkeypatches ``DiscogsClient.get_bulk_enrichment``
    to return a fixed payload (tests/fixtures/canned_enrichment.json). The
    enrichment payload is overlaid onto the first two canonical artists in the
    requested batch so the test stays robust as the tubafrenzy fixture evolves.

    The wiring is what is tested here -- enrichment + Discogs-edge persistence
    flows from ``run_pipeline.run`` through ``DiscogsEnricher.enrich_batch``,
    ``extract_shared_personnel``/``extract_shared_styles``, and the SQLite
    export. Aggregation logic itself is covered in the unit tests under
    ``tests/unit/test_discogs_enrichment.py`` and ``tests/unit/test_discogs_edges.py``.

    If the upstream ``get_bulk_enrichment`` payload shape changes, regenerate
    the fixture via ``scripts/generate_canned_enrichment.py`` (requires
    discogs-cache PG on port 5433).
    """

    @pytest.fixture(autouse=True, scope="class")
    def _run_pipeline_with_enrichment(self, request, monkeypatch_class, tmp_path_factory):
        import json

        from semantic_index.discogs_client import DiscogsClient

        if not FIXTURE_PATH.exists():
            pytest.skip(f"Fixture dump not found at {FIXTURE_PATH}")

        canned_path = Path(__file__).parent.parent / "fixtures" / "canned_enrichment.json"
        canned = json.loads(canned_path.read_text())
        canned_payloads = [canned["artist_a"], canned["artist_b"]]

        def fake_get_bulk_enrichment(self_client, artist_names):
            """Return canned data overlaid onto the first two requested artists.

            Mirrors the contract of ``DiscogsClient.get_bulk_enrichment``: keys
            are lowercased artist names, values are dicts with ``styles``,
            ``extra_artists``, ``labels``, and ``track_artists``. Lowercasing
            matches the production summary-table path.
            """
            if not artist_names:
                return {}
            chosen = list(artist_names)[: len(canned_payloads)]
            return {name.lower(): canned_payloads[i] for i, name in enumerate(chosen)}

        # Avoid touching real PG: stub the connection accessor and patch the
        # bulk-enrichment method to serve canned data. Force ``_has_summary_tables``
        # to False so ``run_pipeline`` takes the Python-fallback edge path
        # (extract_shared_personnel/styles/etc.) instead of the SQL path that
        # would query non-existent summary tables.
        monkeypatch_class.setattr(DiscogsClient, "_get_cache_conn", lambda self: None)
        monkeypatch_class.setattr(
            DiscogsClient, "_has_summary_tables", staticmethod(lambda conn: False)
        )
        monkeypatch_class.setattr(DiscogsClient, "get_bulk_enrichment", fake_get_bulk_enrichment)

        tmpdir = str(tmp_path_factory.mktemp("enriched_pipeline"))
        request.cls._tmpdir = tmpdir
        request.cls._db_path = os.path.join(tmpdir, "wxyc_artist_graph.db")

        from run_pipeline import main as pipeline_main

        # No --skip-enrichment, and force the Python-fallback edge path by
        # passing a non-empty cache DSN -- the monkeypatched accessor short-
        # circuits any real PG connection attempts.
        pipeline_main(
            [
                str(FIXTURE_PATH),
                "--output-dir",
                tmpdir,
                "--min-count",
                "1",
                "--discogs-cache-dsn",
                "postgresql://canned-discogs-cache/none",
                "--compute-discogs-edges",
            ]
        )
        yield

    @pytest.fixture(autouse=True)
    def _connect(self):
        self.conn = sqlite3.connect(self.__class__._db_path)
        self.conn.row_factory = sqlite3.Row
        yield
        self.conn.close()

    def test_artist_style_table_populated(self) -> None:
        """Discogs styles flow through to the artist_style table."""
        count = self.conn.execute("SELECT count(*) FROM artist_style").fetchone()[0]
        assert count > 0, (
            "artist_style is empty -- enrichment payload should produce style rows for "
            "the two canned artists"
        )

    def test_shared_personnel_edges_present(self) -> None:
        """Shared personnel between the two canned artists yields a shared_personnel edge."""
        count = self.conn.execute("SELECT count(*) FROM shared_personnel").fetchone()[0]
        assert count > 0, (
            "shared_personnel is empty -- canned payload shares 'Sean Booth' across "
            "both artists, which should produce at least one edge"
        )

    def test_shared_style_edges_present(self) -> None:
        """Overlapping styles between the two canned artists yields a shared_style edge."""
        count = self.conn.execute("SELECT count(*) FROM shared_style").fetchone()[0]
        assert count > 0, (
            "shared_style is empty -- canned payload shares 'IDM' and 'Electronic' "
            "across both artists, which should clear the default Jaccard threshold"
        )

    def test_label_family_edges_present(self) -> None:
        """Shared labels between the two canned artists yields a label_family edge."""
        count = self.conn.execute("SELECT count(*) FROM label_family").fetchone()[0]
        assert count > 0, (
            "label_family is empty -- both canned artists are credited to 'Warp', "
            "which should produce at least one label_family edge"
        )

    def test_artist_table_has_canned_artists(self) -> None:
        """At least the two artists targeted by canned enrichment exist in the artist table."""
        count = self.conn.execute("SELECT count(*) FROM artist").fetchone()[0]
        assert count > 0, "artist table is empty"
        assert count > 100, f"Expected >100 artists from fixture, got {count}"
