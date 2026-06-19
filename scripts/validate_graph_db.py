#!/usr/bin/env python3
"""Validate a freshly-built ``wxyc_artist_graph.db`` before swapping it into production.

The out-of-process nightly rebuild (see ``plans/si-out-of-process-rebuild``)
builds the graph in a VPC Fargate job and ships it back to the serving host.
Before the conductor atomically swaps the new file into place, it runs this
gate. The gate is **fail-closed**: if validation raises, the conductor keeps
serving the previous (stale-but-enriched) database rather than shipping a
regression.

The most important check is enrichment preservation. ``nightly_sync`` is
*incremental* — it ``shutil.copy2``-es the current production DB forward and
layers fresh PG-derived tables on top, so the Discogs/Wikidata/AcousticBrainz
enrichment tables survive **only** because they are copied from the seed. A
build that started from an empty ``DB_PATH`` would ship a graph with
DJ-transition + cross-reference edges and zero enrichment. We catch that by
comparing the build's enrichment row counts against the seed's.

Note on table names: issue #347 refers to a ``discogs_edges`` table, but that
is a *module* name (``semantic_index/discogs_edges.py``), not a table. Discogs
enrichment materializes into ``shared_personnel`` / ``shared_style`` /
``label_family`` / ``compilation``. The list below is grounded in the actual
SQLite schema (``semantic_index/sqlite_export.py``,
``semantic_index/acousticbrainz.py``).
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

# Enrichment tables produced by separate ``run_pipeline.py`` enrichment runs and
# carried forward across ``nightly_sync`` via ``shutil.copy2`` (NOT recomputed
# from Backend-Service PG each night). If any of these collapses to ~zero in a
# build, the build almost certainly did not start from the seeded production DB.
ENRICHMENT_TABLES: tuple[str, ...] = (
    "shared_personnel",  # Discogs personnel overlap
    "shared_style",  # Discogs style overlap
    "label_family",  # Discogs shared-label
    "compilation",  # Discogs compilation co-appearance
    "wikidata_influence",  # Wikidata P737
    "label_hierarchy",  # Wikidata P749/P355
    "audio_profile",  # AcousticBrainz per-artist profile
    "acoustic_similarity",  # AcousticBrainz pairwise similarity
)

# A build whose enrichment table shrinks below ``fraction * seed_count`` rows is
# treated as a regression. Lenient enough to tolerate the per-artist top-K prune
# applied to ``shared_personnel`` / ``label_family`` on every sync (which can
# legitimately cut an unpruned seed by up to ~an order of magnitude on first
# run), strict enough to catch a build that started empty (count → 0).
DEFAULT_COLLAPSE_FRACTION = 0.1


class ValidationError(Exception):
    """Raised when a built graph DB fails a pre-swap validation check."""


# The full 16-byte SQLite header. We check the whole magic (not just the
# ``b"SQLite"`` prefix) so a truncated/corrupt artifact is rejected here with a
# clean ValidationError rather than slipping through to raise sqlite3.DatabaseError
# from a later query (which `validate` also defends against as a backstop).
_SQLITE_MAGIC = b"SQLite format 3\x00"


def _is_sqlite(path: Path) -> bool:
    """True if *path* exists and begins with the full 16-byte SQLite magic header."""
    try:
        with open(path, "rb") as f:
            return f.read(len(_SQLITE_MAGIC)) == _SQLITE_MAGIC
    except OSError:
        return False


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


def count_rows(
    db_path: str | Path, tables: tuple[str, ...] = ENRICHMENT_TABLES
) -> dict[str, int]:
    """Return ``{table: row_count}`` for each named table that exists.

    Tables absent from the database are omitted from the result (rather than
    reported as ``0``) so a caller can distinguish "table missing" from "table
    present but empty" when it needs to. The rebuild conductor calls this on the
    *seed* to capture the baseline it later validates the build against.
    """
    counts: dict[str, int] = {}
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        for table in tables:
            if _table_exists(conn, table):
                counts[table] = conn.execute(
                    f"SELECT COUNT(*) FROM {table}"  # noqa: S608 — table name guarded by _table_exists above
                ).fetchone()[0]
    finally:
        conn.close()
    return counts


def validate(
    db_path: str | Path,
    *,
    seed_counts: dict[str, int] | None = None,
    min_artists: int = 1,
    collapse_fraction: float = DEFAULT_COLLAPSE_FRACTION,
) -> None:
    """Validate a built graph DB. Raise :class:`ValidationError` on any failure.

    Checks, in order:

    1. The file is a SQLite database (magic header) — catches a truncated or
       non-DB artifact from a failed download/build.
    2. The ``artist`` table exists and has at least ``min_artists`` rows — catches
       an empty or half-written graph. The conductor passes the live artist count
       (scaled down) so a graph that shrank dramatically is rejected.
    3. For each enrichment table that was **non-empty in the seed**, the build
       retains at least ``collapse_fraction * seed_count`` rows (and > 0) — the
       core enrichment-preservation guard (acceptance criterion #2 of #347).

    Args:
        db_path: Path to the candidate (just-built) graph database.
        seed_counts: Enrichment row counts captured from the seed DB (see
            :func:`count_rows`). When ``None``, only checks 1–2 run.
        min_artists: Minimum acceptable ``artist`` row count.
        collapse_fraction: Per-table floor as a fraction of the seed count.
    """
    path = Path(db_path)
    if not _is_sqlite(path):
        raise ValidationError(
            f"{db_path} is not a SQLite database (bad or missing magic header)"
        )

    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            if not _table_exists(conn, "artist"):
                raise ValidationError("missing `artist` table")
            n_artists = conn.execute("SELECT COUNT(*) FROM artist").fetchone()[0]
        finally:
            conn.close()
    except sqlite3.DatabaseError as exc:
        # Header check passed but the body is corrupt/unreadable. Convert the raw
        # sqlite error into ValidationError so callers that catch only
        # ValidationError (e.g. the CLI in main) stay fail-closed instead of
        # crashing with a traceback.
        raise ValidationError(f"{db_path} is not a valid SQLite database: {exc}") from exc

    if n_artists < min_artists:
        raise ValidationError(
            f"artist count {n_artists} is below the required minimum {min_artists}"
        )
    logger.info("artist count: %d (minimum %d)", n_artists, min_artists)

    if seed_counts:
        required = {t: n for t, n in seed_counts.items() if n > 0}
        if not required:
            # The guard compares the build against a seed baseline; with an
            # all-zero seed there is nothing to compare, so it cannot catch an
            # enrichment-collapse regression this run. Make that visible rather
            # than silently passing — an all-empty seed usually means the live
            # graph was already enrichment-empty (e.g. a prior bad swap), which
            # is exactly when the next empty build must NOT sail through unnoticed.
            logger.warning(
                "enrichment guard INACTIVE: seed reported zero enrichment rows for "
                "every table (%s); cannot detect an enrichment-collapse regression "
                "this run — verify the seed was the enriched production graph",
                ", ".join(seed_counts) or "none",
            )
        build_counts = count_rows(db_path, tuple(required))
        problems: list[str] = []
        for table, seed_n in required.items():
            build_n = build_counts.get(table, 0)
            floor = max(1, int(seed_n * collapse_fraction))
            logger.info(
                "enrichment %-20s build=%d seed=%d floor=%d", table, build_n, seed_n, floor
            )
            if build_n < floor:
                problems.append(f"{table}: build={build_n} seed={seed_n} (floor {floor})")
        if problems:
            raise ValidationError(
                "enrichment collapsed vs seed (build likely did not start from "
                "the production DB): " + "; ".join(problems)
            )


def main(argv: list[str] | None = None) -> int:
    """CLI: validate a graph DB, exit 0 on success, 1 on validation failure.

    Invoked by the EC2 conductor against the downloaded build artifact.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("db_path", help="Path to the graph DB to validate or count")
    parser.add_argument(
        "--emit-counts",
        action="store_true",
        help=(
            "Print enrichment row counts of db_path as JSON and exit (no validation). "
            "The conductor uses this on the seed DB to capture the baseline it later "
            "passes back via --seed-counts."
        ),
    )
    parser.add_argument(
        "--seed-counts",
        help="Path to a JSON file of {table: row_count} captured from the seed DB",
    )
    parser.add_argument("--min-artists", type=int, default=1)
    parser.add_argument(
        "--collapse-fraction", type=float, default=DEFAULT_COLLAPSE_FRACTION
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.emit_counts:
        print(json.dumps(count_rows(args.db_path)))
        return 0

    seed_counts = None
    if args.seed_counts:
        seed_counts = json.loads(Path(args.seed_counts).read_text())

    try:
        validate(
            args.db_path,
            seed_counts=seed_counts,
            min_artists=args.min_artists,
            collapse_fraction=args.collapse_fraction,
        )
    except ValidationError as exc:
        logger.error("VALIDATION FAILED: %s", exc)
        return 1
    logger.info("validation passed: %s", args.db_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
