"""Tests for the pre-swap graph-DB validation gate.

Used by the out-of-process nightly rebuild conductor (see
``plans/si-out-of-process-rebuild``) as a fail-closed guard: a build that
started from an empty DB (and lost the carried-forward enrichment tables) or
produced an empty/corrupt artist graph must not be swapped into production.
"""

import json
import sqlite3

import pytest

from scripts.validate_graph_db import (
    ENRICHMENT_TABLES,
    ValidationError,
    count_rows,
    main,
    validate,
)

# WXYC-representative artists (per the org test-data convention).
WXYC_ARTISTS = [
    "Stereolab",
    "Autechre",
    "Father John Misty",
    "Juana Molina",
    "Jessica Pratt",
]


def _make_graph_db(path, *, artists=WXYC_ARTISTS, enrichment=None):
    """Create a minimal graph DB: an ``artist`` table plus every enrichment
    table (each a single nullable column — the validator only counts rows and
    checks table existence, so realistic columns add nothing).

    ``enrichment`` maps a table name to the number of filler rows to insert;
    omitted enrichment tables exist but stay empty.
    """
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            "CREATE TABLE artist ("
            "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "  canonical_name TEXT NOT NULL UNIQUE,"
            "  total_plays INTEGER NOT NULL DEFAULT 0)"
        )
        for table in ENRICHMENT_TABLES:
            conn.execute(f"CREATE TABLE {table} (a INTEGER)")  # noqa: S608
        conn.executemany("INSERT INTO artist (canonical_name) VALUES (?)", [(a,) for a in artists])
        for table, n in (enrichment or {}).items():
            conn.executemany(
                f"INSERT INTO {table} (a) VALUES (?)",  # noqa: S608
                [(i,) for i in range(n)],
            )
        conn.commit()
    finally:
        conn.close()
    return path


def test_valid_db_passes(tmp_path):
    db = _make_graph_db(
        tmp_path / "graph.db",
        enrichment={"shared_personnel": 50, "wikidata_influence": 10, "audio_profile": 5},
    )
    # No seed_counts: only header + artist checks.
    validate(db)
    # Equal enrichment counts vs seed pass.
    validate(db, seed_counts={"shared_personnel": 50, "wikidata_influence": 10})


def test_non_sqlite_file_fails(tmp_path):
    p = tmp_path / "junk.db"
    p.write_bytes(b"definitely not a sqlite database")
    with pytest.raises(ValidationError, match="SQLite"):
        validate(p)


def test_missing_db_file_fails(tmp_path):
    with pytest.raises(ValidationError):
        validate(tmp_path / "nope.db")


def test_empty_artist_table_fails(tmp_path):
    db = _make_graph_db(tmp_path / "graph.db", artists=[])
    with pytest.raises(ValidationError, match="artist count"):
        validate(db)


def test_missing_artist_table_fails(tmp_path):
    db = tmp_path / "graph.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE foo (x INTEGER)")
    conn.commit()
    conn.close()
    with pytest.raises(ValidationError, match="artist"):
        validate(db)


def test_min_artists_floor(tmp_path):
    db = _make_graph_db(tmp_path / "graph.db", artists=WXYC_ARTISTS)  # 5 artists
    validate(db, min_artists=5)
    with pytest.raises(ValidationError, match="minimum"):
        validate(db, min_artists=6)


def test_enrichment_collapse_to_zero_fails(tmp_path):
    # Build lost shared_personnel entirely; seed had 100 rows -> regression.
    db = _make_graph_db(tmp_path / "graph.db", enrichment={"wikidata_influence": 10})
    with pytest.raises(ValidationError, match="shared_personnel"):
        validate(db, seed_counts={"shared_personnel": 100})


def test_enrichment_prune_tolerated(tmp_path):
    # Seed had 100; build pruned to 20 (top-K). Floor = max(1, 100*0.1)=10; 20>=10 passes.
    db = _make_graph_db(tmp_path / "graph.db", enrichment={"shared_personnel": 20})
    validate(db, seed_counts={"shared_personnel": 100})


def test_enrichment_just_below_floor_fails(tmp_path):
    # Seed 100, floor 10, build 9 -> fail.
    db = _make_graph_db(tmp_path / "graph.db", enrichment={"shared_personnel": 9})
    with pytest.raises(ValidationError, match="shared_personnel"):
        validate(db, seed_counts={"shared_personnel": 100})


def test_seed_zero_count_tables_ignored(tmp_path):
    # A table that was empty in the seed imposes no requirement on the build.
    db = _make_graph_db(tmp_path / "graph.db", enrichment={"shared_personnel": 5})
    validate(db, seed_counts={"label_hierarchy": 0, "shared_personnel": 5})


def test_count_rows_omits_missing_tables(tmp_path):
    db = tmp_path / "graph.db"
    conn = sqlite3.connect(str(db))
    conn.executescript(
        "CREATE TABLE artist (id INTEGER PRIMARY KEY, canonical_name TEXT);"
        "CREATE TABLE shared_personnel (a INTEGER);"
    )
    conn.execute("INSERT INTO shared_personnel (a) VALUES (1)")
    conn.commit()
    conn.close()
    # Only the enrichment table that exists is reported.
    assert count_rows(db) == {"shared_personnel": 1}


def test_cli_emit_counts(tmp_path, capsys):
    db = _make_graph_db(
        tmp_path / "graph.db",
        enrichment={"shared_personnel": 7, "audio_profile": 3},
    )
    rc = main([str(db), "--emit-counts"])
    assert rc == 0
    printed = json.loads(capsys.readouterr().out)
    assert printed["shared_personnel"] == 7
    assert printed["audio_profile"] == 3


def test_cli_returns_1_on_validation_failure(tmp_path):
    db = _make_graph_db(tmp_path / "graph.db", artists=[])  # empty artist table
    assert main([str(db)]) == 1


def test_cli_passes_with_seed_counts_file(tmp_path):
    db = _make_graph_db(tmp_path / "graph.db", enrichment={"shared_personnel": 40})
    seed_file = tmp_path / "seed.json"
    seed_file.write_text(json.dumps({"shared_personnel": 50}))
    assert main([str(db), "--seed-counts", str(seed_file)]) == 0


def test_enrichment_table_entirely_missing_fails(tmp_path):
    # The real "build started from an empty DB" failure mode: the enrichment
    # table doesn't exist at all (distinct from existing-but-empty). count_rows
    # omits a missing table, so build_counts.get(table, 0) -> 0 -> collapse.
    db = _make_graph_db(tmp_path / "graph.db", enrichment={"wikidata_influence": 10})
    conn = sqlite3.connect(str(db))
    conn.execute("DROP TABLE shared_personnel")
    conn.commit()
    conn.close()
    with pytest.raises(ValidationError, match="shared_personnel"):
        validate(db, seed_counts={"shared_personnel": 100})


def test_enrichment_exactly_at_floor_passes(tmp_path):
    # Boundary: seed=100 -> floor = max(1, int(100*0.1)) = 10; build == floor
    # passes because the guard is `build_n < floor` (strict). Pins the off-by-one.
    db = _make_graph_db(tmp_path / "graph.db", enrichment={"shared_personnel": 10})
    validate(db, seed_counts={"shared_personnel": 100})


def test_corrupt_body_with_valid_header_raises_validation_error(tmp_path):
    # A file with the real SQLite magic header but a corrupt body passes the
    # header precheck, then the query raises sqlite3.DatabaseError — which
    # validate() must convert to ValidationError (not let it escape as a
    # traceback), so a caller catching only ValidationError stays fail-closed.
    db = tmp_path / "graph.db"
    db.write_bytes(b"SQLite format 3\x00" + b"\xde\xad\xbe\xef" * 64)
    with pytest.raises(ValidationError, match="not a valid SQLite database"):
        validate(db)


def test_inactive_guard_warns_when_seed_all_zero(tmp_path, caplog):
    # When the seed had zero enrichment everywhere, the collapse guard can't run.
    # Validation still passes on artist count, but it must WARN (not silently
    # disarm), so an empty-seed -> empty-build chain is visible in the logs.
    import logging

    db = _make_graph_db(tmp_path / "graph.db", enrichment={})
    with caplog.at_level(logging.WARNING):
        validate(db, seed_counts={"shared_personnel": 0, "wikidata_influence": 0})
    assert any("enrichment guard INACTIVE" in r.message for r in caplog.records)


def test_enrichment_table_names_match_real_schema():
    # Guards ENRICHMENT_TABLES against drifting from the actual SQLite schema
    # (the #347 'discogs_edges' module-vs-table confusion): every name must be a
    # real `CREATE TABLE` somewhere in semantic_index, or its preservation guard
    # silently does nothing in production.
    import re
    from pathlib import Path

    si_dir = Path(__file__).resolve().parents[2] / "semantic_index"
    sources = "\n".join(p.read_text() for p in si_dir.rglob("*.py"))
    created = {
        name.lower()
        for name in re.findall(r"CREATE TABLE (?:IF NOT EXISTS )?(\w+)", sources, re.IGNORECASE)
    }
    missing = [t for t in ENRICHMENT_TABLES if t.lower() not in created]
    assert not missing, (
        f"ENRICHMENT_TABLES not found as a CREATE TABLE in semantic_index: {missing}"
    )
