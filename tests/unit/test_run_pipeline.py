"""Tests for run_pipeline CLI argument parsing and entity store integration."""

from run_pipeline import parse_args


class TestParseArgs:
    def test_entity_store_path_default(self):
        args = parse_args(["dump.sql"])
        assert args.entity_store_path is None

    def test_entity_store_path_flag(self):
        args = parse_args(["dump.sql", "--entity-store-path", "/tmp/store.db"])
        assert args.entity_store_path == "/tmp/store.db"

    def test_skip_reconciliation_default(self):
        args = parse_args(["dump.sql"])
        assert args.skip_reconciliation is False

    def test_skip_reconciliation_flag(self):
        args = parse_args(["dump.sql", "--skip-reconciliation"])
        assert args.skip_reconciliation is True

    def test_compute_discogs_edges_default(self):
        args = parse_args(["dump.sql"])
        assert args.compute_discogs_edges is False

    def test_compute_discogs_edges_flag(self):
        args = parse_args(["dump.sql", "--compute-discogs-edges"])
        assert args.compute_discogs_edges is True

    def test_existing_flags_preserved(self):
        """Existing flags should still work."""
        args = parse_args(
            [
                "dump.sql",
                "--output-dir",
                "out",
                "--min-count",
                "5",
                "--no-sqlite",
                "--skip-enrichment",
                "--verbose",
            ]
        )
        assert args.output_dir == "out"
        assert args.min_count == 5
        assert args.no_sqlite is True
        assert args.skip_enrichment is True
        assert args.verbose is True
