"""Tests for run_pipeline CLI argument parsing and entity store integration."""

import pickle
import sqlite3

import pytest

from run_pipeline import main, parse_args
from semantic_index.models import FlowsheetEntry, ResolvedEntry


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

    def test_facet_only_default(self):
        args = parse_args(["dump.sql"])
        assert args.facet_only is False

    def test_facet_only_flag(self):
        args = parse_args(["dump.sql", "--facet-only", "--cache-dir", "/tmp/cache"])
        assert args.facet_only is True


def _make_resolved_entry(
    entry_id: int,
    artist_name: str,
    show_id: int,
    sequence: int,
    start_time: int | None = 1_700_000_000_000,
) -> ResolvedEntry:
    """Create a minimal ResolvedEntry for testing."""
    return ResolvedEntry(
        entry=FlowsheetEntry(
            id=entry_id,
            artist_name=artist_name,
            song_title="",
            release_title="",
            library_release_id=0,
            label_name="",
            show_id=show_id,
            sequence=sequence,
            entry_type_code=1,
            request_flag=0,
            start_time=start_time,
        ),
        canonical_name=artist_name,
        resolution_method="raw",
    )


def _build_cache(
    cache_path,
    resolved_entries,
    show_to_dj=None,
    show_dj_names=None,
):
    """Write a pipeline cache pickle file."""
    cache = {
        "resolved_entries": resolved_entries,
        "genre_names": {},
        "codes": [],
        "releases": [],
        "show_to_dj": show_to_dj or {},
        "show_dj_names": show_dj_names or {},
        "total_entries": len(resolved_entries),
        "music_entries": len(resolved_entries),
        "catalog_resolved": 0,
    }
    with open(cache_path, "wb") as f:
        pickle.dump(cache, f)


def _create_db_with_artists(db_path, artists: dict[str, int]):
    """Create a SQLite DB with an artist table containing the given name->id mapping."""
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE artist ("
        "  id INTEGER PRIMARY KEY,"
        "  canonical_name TEXT NOT NULL UNIQUE,"
        "  genre TEXT,"
        "  total_plays INTEGER NOT NULL DEFAULT 0,"
        "  active_first_year INTEGER,"
        "  active_last_year INTEGER,"
        "  dj_count INTEGER NOT NULL DEFAULT 0,"
        "  request_ratio REAL NOT NULL DEFAULT 0.0,"
        "  show_count INTEGER NOT NULL DEFAULT 0"
        ")"
    )
    conn.executemany(
        "INSERT INTO artist (id, canonical_name) VALUES (?, ?)",
        [(aid, name) for name, aid in artists.items()],
    )
    conn.commit()
    conn.close()


class TestFacetOnly:
    """Tests for the --facet-only pipeline mode."""

    def test_facet_only_requires_cache_dir(self, tmp_path):
        """--facet-only without --cache-dir should exit with an error."""
        dump = tmp_path / "dump.sql"
        dump.write_text("-- empty dump")
        with pytest.raises(SystemExit):
            main([str(dump), "--facet-only"])

    def test_facet_only_requires_existing_cache(self, tmp_path):
        """--facet-only with --cache-dir but no matching cache file should exit."""
        dump = tmp_path / "dump.sql"
        dump.write_text("-- empty dump")
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        with pytest.raises(SystemExit):
            main([str(dump), "--facet-only", "--cache-dir", str(cache_dir)])

    def test_facet_only_requires_existing_database(self, tmp_path):
        """--facet-only should exit if the target database does not exist."""
        dump = tmp_path / "dump.sql"
        dump.write_text("-- empty dump")
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        # Build the cache with the correct key
        dump_stat = dump.stat()
        cache_key = f"{dump_stat.st_size}_{int(dump_stat.st_mtime)}"
        cache_path = cache_dir / f"resolved_{cache_key}.pkl"
        _build_cache(cache_path, [])

        # No database exists at the default output path
        with pytest.raises(SystemExit):
            main(
                [
                    str(dump),
                    "--facet-only",
                    "--cache-dir",
                    str(cache_dir),
                    "--output-dir",
                    str(tmp_path / "nonexistent"),
                ]
            )

    def test_facet_only_exports_facet_tables(self, tmp_path):
        """--facet-only should load cache, read DB, and export facet tables."""
        dump = tmp_path / "dump.sql"
        dump.write_text("-- empty dump")
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        # Set up test data
        artists = {"Autechre": 1, "Stereolab": 2, "Cat Power": 3}
        entries = [
            _make_resolved_entry(100, "Autechre", show_id=1, sequence=1),
            _make_resolved_entry(101, "Stereolab", show_id=1, sequence=2),
            _make_resolved_entry(102, "Cat Power", show_id=1, sequence=3),
        ]
        show_to_dj = {1: 42}
        show_dj_names = {1: "DJ Test"}

        # Write cache
        dump_stat = dump.stat()
        cache_key = f"{dump_stat.st_size}_{int(dump_stat.st_mtime)}"
        cache_path = cache_dir / f"resolved_{cache_key}.pkl"
        _build_cache(cache_path, entries, show_to_dj, show_dj_names)

        # Create target database
        db_path = output_dir / "wxyc_artist_graph.db"
        _create_db_with_artists(db_path, artists)

        # Run facet-only mode
        main(
            [
                str(dump),
                "--facet-only",
                "--cache-dir",
                str(cache_dir),
                "--output-dir",
                str(output_dir),
            ]
        )

        # Verify facet tables were created
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        # play table should have 3 rows
        play_count = conn.execute("SELECT COUNT(*) FROM play").fetchone()[0]
        assert play_count == 3

        # dj table should have 1 row
        dj_count = conn.execute("SELECT COUNT(*) FROM dj").fetchone()[0]
        assert dj_count == 1

        # artist_month_count should have entries
        amc = conn.execute("SELECT COUNT(*) FROM artist_month_count").fetchone()[0]
        assert amc > 0

        conn.close()

    def test_facet_only_with_entity_store_path(self, tmp_path):
        """--facet-only with --entity-store-path should use that as the target DB."""
        dump = tmp_path / "dump.sql"
        dump.write_text("-- empty dump")
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        artists = {"Autechre": 1, "Stereolab": 2}
        entries = [
            _make_resolved_entry(100, "Autechre", show_id=1, sequence=1),
            _make_resolved_entry(101, "Stereolab", show_id=1, sequence=2),
        ]

        dump_stat = dump.stat()
        cache_key = f"{dump_stat.st_size}_{int(dump_stat.st_mtime)}"
        cache_path = cache_dir / f"resolved_{cache_key}.pkl"
        _build_cache(cache_path, entries, {1: 10}, {1: "DJ Entity"})

        # Create the entity-store DB
        entity_db = tmp_path / "entity_store.db"
        _create_db_with_artists(entity_db, artists)

        main(
            [
                str(dump),
                "--facet-only",
                "--cache-dir",
                str(cache_dir),
                "--entity-store-path",
                str(entity_db),
            ]
        )

        conn = sqlite3.connect(str(entity_db))
        play_count = conn.execute("SELECT COUNT(*) FROM play").fetchone()[0]
        assert play_count == 2
        conn.close()
