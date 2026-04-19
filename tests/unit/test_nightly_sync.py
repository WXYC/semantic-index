"""Tests for nightly sync orchestrator helpers.

Tests the utility functions (_prepare_working_db, _clear_recomputed_tables,
_atomic_swap) and verifies enrichment data survives a pipeline re-run on
a copied database.
"""

import argparse
import sqlite3
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _create_test_db(path: Path, *, with_enrichment: bool = False) -> None:
    """Create a minimal SQLite database with pipeline tables.

    When *with_enrichment* is True, also creates enrichment tables and
    inserts sample data that should survive a nightly sync.
    """
    conn = sqlite3.connect(str(path))
    conn.executescript("""
        CREATE TABLE artist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            canonical_name TEXT NOT NULL UNIQUE,
            genre TEXT,
            total_plays INTEGER NOT NULL DEFAULT 0,
            dj_count INTEGER NOT NULL DEFAULT 0,
            request_ratio REAL NOT NULL DEFAULT 0.0,
            show_count INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE dj_transition (
            source_id INTEGER NOT NULL,
            target_id INTEGER NOT NULL,
            raw_count INTEGER NOT NULL,
            pmi REAL NOT NULL,
            PRIMARY KEY (source_id, target_id)
        );
        CREATE TABLE cross_reference (
            artist_a_id INTEGER NOT NULL,
            artist_b_id INTEGER NOT NULL,
            comment TEXT,
            source TEXT NOT NULL,
            PRIMARY KEY (artist_a_id, artist_b_id, source)
        );
        """)

    # Seed some edge data that should be cleared
    conn.execute("INSERT INTO artist (canonical_name, total_plays) VALUES ('Autechre', 500)")
    conn.execute("INSERT INTO artist (canonical_name, total_plays) VALUES ('Stereolab', 400)")
    conn.execute("INSERT INTO dj_transition VALUES (1, 2, 10, 3.5)")
    conn.execute("INSERT INTO cross_reference VALUES (1, 2, 'see also', 'library_code')")

    if with_enrichment:
        conn.executescript("""
            CREATE TABLE entity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wikidata_qid TEXT,
                name TEXT NOT NULL,
                entity_type TEXT NOT NULL DEFAULT 'artist',
                created_at TEXT NOT NULL DEFAULT '2025-01-01',
                updated_at TEXT NOT NULL DEFAULT '2025-01-01'
            );
            CREATE TABLE wikidata_influence (
                source_id INTEGER NOT NULL,
                target_id INTEGER NOT NULL,
                source_qid TEXT NOT NULL,
                target_qid TEXT NOT NULL,
                PRIMARY KEY (source_id, target_id)
            );
            CREATE TABLE audio_profile (
                artist_id INTEGER PRIMARY KEY,
                avg_danceability REAL,
                recording_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT '2025-01-01'
            );
            """)
        conn.execute("INSERT INTO entity (wikidata_qid, name) VALUES ('Q210513', 'Autechre')")
        conn.execute("INSERT INTO wikidata_influence VALUES (1, 2, 'Q210513', 'Q484464')")
        conn.execute("INSERT INTO audio_profile VALUES (1, 0.35, 42, '2025-01-01')")

    conn.commit()
    conn.close()


# ===========================================================================
# _validate_sqlite
# ===========================================================================


class TestValidateSqlite:
    def test_valid_sqlite_file(self, tmp_path):
        from semantic_index.nightly_sync import _validate_sqlite

        db = tmp_path / "test.db"
        conn = sqlite3.connect(str(db))
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.close()

        assert _validate_sqlite(db) is True

    def test_non_sqlite_file(self, tmp_path):
        from semantic_index.nightly_sync import _validate_sqlite

        f = tmp_path / "not_a_db.db"
        f.write_text("this is not sqlite")

        assert _validate_sqlite(f) is False

    def test_nonexistent_file(self, tmp_path):
        from semantic_index.nightly_sync import _validate_sqlite

        assert _validate_sqlite(tmp_path / "missing.db") is False


# ===========================================================================
# _clean_stale_temp_files
# ===========================================================================


class TestCleanStaleTempFiles:
    def test_removes_orphaned_temp_files(self, tmp_path):
        from semantic_index.nightly_sync import _clean_stale_temp_files

        prod = tmp_path / "wxyc_artist_graph.db"
        _create_test_db(prod)

        stale1 = tmp_path / "wxyc_artist_graph.tmp.1234.db"
        stale2 = tmp_path / "wxyc_artist_graph.tmp.5678.db"
        stale1.write_bytes(b"stale")
        stale2.write_bytes(b"stale")

        _clean_stale_temp_files(prod)

        assert not stale1.exists()
        assert not stale2.exists()
        assert prod.exists()

    def test_no_op_when_no_temp_files(self, tmp_path):
        from semantic_index.nightly_sync import _clean_stale_temp_files

        prod = tmp_path / "wxyc_artist_graph.db"
        _create_test_db(prod)

        _clean_stale_temp_files(prod)

        assert prod.exists()

    def test_does_not_remove_unrelated_files(self, tmp_path):
        from semantic_index.nightly_sync import _clean_stale_temp_files

        prod = tmp_path / "wxyc_artist_graph.db"
        _create_test_db(prod)

        cache = tmp_path / "wxyc_artist_graph.db.bio-cache.db"
        bak = tmp_path / "wxyc_artist_graph.db.bak"
        cache.write_bytes(b"cache")
        bak.write_bytes(b"backup")

        _clean_stale_temp_files(prod)

        assert cache.exists()
        assert bak.exists()


# ===========================================================================
# _prepare_working_db
# ===========================================================================


class TestPrepareWorkingDb:
    def test_copies_existing_db(self, tmp_path):
        from semantic_index.nightly_sync import prepare_working_db

        prod = tmp_path / "prod.db"
        _create_test_db(prod)

        temp = prepare_working_db(prod)

        assert temp.exists()
        assert temp != prod
        # Verify content was copied
        conn = sqlite3.connect(str(temp))
        count = conn.execute("SELECT COUNT(*) FROM artist").fetchone()[0]
        conn.close()
        assert count == 2

    def test_first_run_creates_empty_temp(self, tmp_path):
        from semantic_index.nightly_sync import prepare_working_db

        prod = tmp_path / "nonexistent.db"
        temp = prepare_working_db(prod)

        assert temp.exists()
        assert temp.stat().st_size == 0

    def test_temp_in_same_directory_as_production(self, tmp_path):
        from semantic_index.nightly_sync import prepare_working_db

        prod = tmp_path / "data" / "graph.db"
        prod.parent.mkdir()
        _create_test_db(prod)

        temp = prepare_working_db(prod)

        assert temp.parent == prod.parent

    def test_cleans_stale_temps_before_creating_new(self, tmp_path):
        from semantic_index.nightly_sync import prepare_working_db

        prod = tmp_path / "prod.db"
        _create_test_db(prod)

        stale = tmp_path / "prod.tmp.9999.db"
        stale.write_bytes(b"orphaned from previous crash")

        temp = prepare_working_db(prod)

        assert not stale.exists()
        assert temp.exists()
        assert ".tmp." in temp.name


# ===========================================================================
# _clear_recomputed_tables
# ===========================================================================


class TestClearRecomputedTables:
    def test_clears_edge_tables(self, tmp_path):
        from semantic_index.nightly_sync import _clear_recomputed_tables

        db = tmp_path / "test.db"
        _create_test_db(db, with_enrichment=True)

        _clear_recomputed_tables(str(db))

        conn = sqlite3.connect(str(db))
        assert conn.execute("SELECT COUNT(*) FROM dj_transition").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM cross_reference").fetchone()[0] == 0
        conn.close()

    def test_preserves_enrichment_tables(self, tmp_path):
        from semantic_index.nightly_sync import _clear_recomputed_tables

        db = tmp_path / "test.db"
        _create_test_db(db, with_enrichment=True)

        _clear_recomputed_tables(str(db))

        conn = sqlite3.connect(str(db))
        assert conn.execute("SELECT COUNT(*) FROM entity").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM wikidata_influence").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM audio_profile").fetchone()[0] == 1
        conn.close()

    def test_handles_missing_tables_gracefully(self, tmp_path):
        """Works on a fresh DB that doesn't have edge tables yet."""
        from semantic_index.nightly_sync import _clear_recomputed_tables

        db = tmp_path / "empty.db"
        conn = sqlite3.connect(str(db))
        conn.execute("CREATE TABLE artist (id INTEGER PRIMARY KEY)")
        conn.close()

        # Should not raise
        _clear_recomputed_tables(str(db))


# ===========================================================================
# _atomic_swap
# ===========================================================================


class TestAtomicSwap:
    def test_replaces_production_with_temp(self, tmp_path):
        from semantic_index.nightly_sync import atomic_swap

        prod = tmp_path / "prod.db"
        prod.write_bytes(b"old content")

        temp = tmp_path / "temp.db"
        temp.write_bytes(b"new content")

        atomic_swap(temp, prod, dry_run=False)

        assert prod.read_bytes() == b"new content"
        assert not temp.exists()

    def test_dry_run_removes_temp_keeps_production(self, tmp_path):
        from semantic_index.nightly_sync import atomic_swap

        prod = tmp_path / "prod.db"
        prod.write_bytes(b"old content")

        temp = tmp_path / "temp.db"
        temp.write_bytes(b"new content")

        atomic_swap(temp, prod, dry_run=True)

        assert prod.read_bytes() == b"old content"
        assert not temp.exists()

    def test_first_run_no_existing_production(self, tmp_path):
        from semantic_index.nightly_sync import atomic_swap

        prod = tmp_path / "prod.db"
        temp = tmp_path / "temp.db"
        temp.write_bytes(b"new content")

        atomic_swap(temp, prod, dry_run=False)

        assert prod.read_bytes() == b"new content"

    def test_creates_parent_directory(self, tmp_path):
        from semantic_index.nightly_sync import atomic_swap

        prod = tmp_path / "data" / "graph.db"
        temp = tmp_path / "data" / "temp.db"
        temp.parent.mkdir()
        temp.write_bytes(b"new content")

        atomic_swap(temp, prod, dry_run=False)

        assert prod.exists()


# ===========================================================================
# Entity deduplication in nightly_sync
# ===========================================================================


def _stub_wxyc_etl():
    """Pre-populate sys.modules with a stub wxyc_etl so lazy imports succeed."""
    if "wxyc_etl" in sys.modules:
        return
    stub = MagicMock()
    # text sub-module used by artist_resolver and graph_metrics
    stub.text.normalize_artist_name = lambda name: name.lower().strip()
    stub.text.is_compilation_artist = lambda name: name.lower() in ("various artists", "v/a")
    stub.text.split_artist_name = lambda name: [name]
    for name in (
        "wxyc_etl",
        "wxyc_etl.text",
        "wxyc_etl.parser",
        "wxyc_etl.schema",
    ):
        sys.modules.setdefault(name, stub)


class TestNightlySyncDeduplication:
    """Verify that nightly_sync calls deduplicate_by_qid after export and before facets."""

    def _make_args(self, tmp_path: Path) -> argparse.Namespace:
        return argparse.Namespace(
            db_path=str(tmp_path / "prod.db"),
            dsn="postgresql://fake",
            min_count=2,
            dry_run=True,
        )

    def _set_up_mocks(self, mock_pdb_cls, mock_load, mock_resolver_cls, mock_xref_cls):
        """Configure common mocks for nightly_sync orchestration tests."""
        from semantic_index.models import DeduplicationReport

        mock_load.return_value = (
            {},  # genres
            [],  # codes
            [],  # releases
            [MagicMock(artist_name="Autechre", canonical_name="Autechre")],
            {},  # show_to_dj
            {},  # show_dj_names
            [],  # artist_xrefs
            [],  # release_xrefs
        )

        resolved = MagicMock()
        resolved.resolution_method = "name_match"
        resolved.canonical_name = "Autechre"
        mock_resolver = mock_resolver_cls.return_value
        mock_resolver.resolve.return_value = resolved
        mock_resolver.re_resolve_with_play_counts.return_value = [resolved]

        mock_xref = mock_xref_cls.return_value
        mock_xref.extract_library_code_xrefs.return_value = []
        mock_xref.extract_release_xrefs.return_value = []

        mock_pdb = mock_pdb_cls.return_value
        mock_pdb.bulk_upsert_artists.return_value = {"Autechre": 1}
        mock_pdb.get_name_to_id_mapping.return_value = {"Autechre": 1}
        mock_pdb.deduplicate_by_qid.return_value = DeduplicationReport(
            groups_found=1,
            entities_merged=1,
            artists_reassigned=2,
            edges_rekeyed=3,
        )
        return mock_pdb

    def _run_nightly_sync(self, tmp_path, extra_setup=None):
        """Run nightly_sync with all heavy dependencies mocked out.

        Returns the mock PipelineDB instance for assertions.
        """
        _stub_wxyc_etl()

        prod = tmp_path / "prod.db"
        _create_test_db(prod)

        with (
            patch("semantic_index.nightly_sync._load_from_pg") as mock_load,
            patch("semantic_index.pipeline_db.PipelineDB") as mock_pdb_cls,
            patch("semantic_index.nightly_sync._clear_recomputed_tables"),
            patch("semantic_index.artist_resolver.ArtistResolver") as mock_resolver_cls,
            patch("semantic_index.cross_reference.CrossReferenceExtractor") as mock_xref_cls,
            patch("semantic_index.adjacency.extract_adjacency_pairs", return_value=[]),
            patch("semantic_index.pmi.compute_pmi", return_value=[]),
            patch("semantic_index.node_attributes.compute_artist_stats", return_value={}),
            patch("semantic_index.sqlite_export.export_sqlite") as mock_export,
            patch("semantic_index.facet_export.export_facet_tables") as mock_facets,
            patch("semantic_index.graph_metrics.compute_and_persist") as mock_metrics,
        ):
            mock_pdb = self._set_up_mocks(mock_pdb_cls, mock_load, mock_resolver_cls, mock_xref_cls)
            mock_metrics.return_value = MagicMock(community_count=0, artists_scored=0)

            if extra_setup:
                extra_setup(
                    mock_pdb=mock_pdb,
                    mock_export=mock_export,
                    mock_facets=mock_facets,
                )

            from semantic_index.nightly_sync import nightly_sync

            args = self._make_args(tmp_path)
            nightly_sync(args)

        return mock_pdb

    def test_dedup_called_after_export_before_facets(self, tmp_path):
        mock_pdb = self._run_nightly_sync(tmp_path)
        mock_pdb.deduplicate_by_qid.assert_called_once()

    def test_dedup_runs_before_facets(self, tmp_path):
        """Entity dedup must run before facet export so artist IDs are stable."""
        from semantic_index.models import DeduplicationReport

        call_order = []

        def setup(mock_pdb, mock_export, mock_facets):
            mock_export.side_effect = lambda *a, **kw: call_order.append("export_sqlite")
            mock_pdb.deduplicate_by_qid.side_effect = lambda: (
                call_order.append("deduplicate_by_qid")
                or DeduplicationReport(
                    groups_found=0, entities_merged=0, artists_reassigned=0, edges_rekeyed=0
                )
            )
            mock_facets.side_effect = lambda *a, **kw: call_order.append("export_facet_tables")

        self._run_nightly_sync(tmp_path, extra_setup=setup)

        assert "export_sqlite" in call_order
        assert "deduplicate_by_qid" in call_order
        assert "export_facet_tables" in call_order

        export_idx = call_order.index("export_sqlite")
        dedup_idx = call_order.index("deduplicate_by_qid")
        facet_idx = call_order.index("export_facet_tables")

        assert export_idx < dedup_idx < facet_idx, (
            f"Expected export < dedup < facets, got: {call_order}"
        )
