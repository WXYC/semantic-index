"""Nightly sync: refresh the semantic index from Backend-Service PostgreSQL.

Replaces the manual SQL dump pipeline with direct PG queries via
``pg_source``. Recomputes the core graph (artist resolution, PMI,
stats, cross-references, facets, graph metrics) while preserving
enrichment data (Discogs, Wikidata, AcousticBrainz) from the
existing production database.

Atomic swap strategy:
    1. Copy existing production DB to a temp file
    2. Open as PipelineDB (enrichment tables intact)
    3. Clear recomputed edge tables
    4. Run pipeline: PG → resolve → PMI → export → entity dedup → facets → metrics
    5. WAL checkpoint, atomic ``os.replace``
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import sqlite3
import sys
import time
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = "data/wxyc_artist_graph.db"
DEFAULT_MIN_COUNT = 2

# Tables that are fully recomputed each run and must be cleared
# before re-inserting to avoid stale data.  Facet tables and
# community table are self-clearing (handled by their own export
# functions).
_TABLES_TO_CLEAR = ("dj_transition", "cross_reference")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _validate_sqlite(path: Path) -> bool:
    """Check whether *path* is a valid SQLite database (magic header bytes)."""
    if not path.exists():
        return False
    try:
        with open(path, "rb") as f:
            header = f.read(16)
        return header[:6] == b"SQLite"
    except OSError:
        return False


def _clean_stale_temp_files(production_path: Path) -> None:
    """Remove orphaned temp files from previous interrupted runs."""
    for stale in production_path.parent.glob(f"{production_path.stem}.tmp.*.db"):
        size_mb = stale.stat().st_size / (1024 * 1024)
        logger.warning("Removing stale temp file: %s (%.1f MB)", stale.name, size_mb)
        stale.unlink()


def _prepare_working_db(production_path: Path) -> Path:
    """Create a working copy of the production database.

    If the production file exists and is valid SQLite, copies it to a
    temp file in the same directory (ensures same filesystem for atomic
    rename). On first run, creates an empty temp file.

    Returns:
        Path to the temp file.
    """
    _clean_stale_temp_files(production_path)

    temp_path = production_path.with_suffix(f".tmp.{os.getpid()}.db")

    if _validate_sqlite(production_path):
        size_mb = production_path.stat().st_size / (1024 * 1024)
        logger.info("Copying production DB (%.1f MB) to working copy...", size_mb)
        shutil.copy2(production_path, temp_path)
    else:
        logger.info("No existing production DB — first run, creating empty working copy")
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path.touch()

    return temp_path


def _clear_recomputed_tables(db_path: str) -> None:
    """Clear edge tables that will be fully recomputed.

    Handles the case where tables don't exist yet (fresh database).
    Does NOT touch enrichment tables.
    """
    conn = sqlite3.connect(db_path)
    for table in _TABLES_TO_CLEAR:
        try:
            conn.execute(f"DELETE FROM {table}")  # noqa: S608
        except sqlite3.OperationalError:
            pass  # table doesn't exist yet
    conn.commit()
    conn.close()


def _checkpoint_and_close(db_path: str) -> None:
    """Truncate WAL journal so the file is self-contained before swap."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()


def _atomic_swap(temp_path: Path, production_path: Path, *, dry_run: bool = False) -> None:
    """Atomically replace the production DB with the completed working copy.

    Uses ``os.replace`` which is atomic on POSIX when source and
    destination are on the same filesystem.
    """
    if dry_run:
        logger.info("DRY RUN: would swap %s -> %s", temp_path, production_path)
        temp_path.unlink(missing_ok=True)
        return

    production_path.parent.mkdir(parents=True, exist_ok=True)
    os.replace(str(temp_path), str(production_path))
    logger.info("Atomic swap complete: %s", production_path)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _load_from_pg(dsn: str):
    """Connect to Backend-Service PG and load all pipeline inputs.

    Returns:
        Tuple of (genres, codes, releases, entries, show_to_dj,
        show_dj_names, artist_xrefs, release_xrefs).
    """
    import psycopg
    from psycopg.rows import dict_row

    from semantic_index.pg_source import (
        load_catalog,
        load_cross_references,
        load_flowsheet_entries,
        load_genres,
        load_shows,
    )

    logger.info("Connecting to Backend-Service PG...")
    conn = psycopg.connect(dsn, autocommit=True, row_factory=dict_row)
    try:
        genres = load_genres(conn)
        logger.info("  %d genres", len(genres))

        codes, releases = load_catalog(conn)
        logger.info("  %d artists, %d releases", len(codes), len(releases))

        entries = load_flowsheet_entries(conn)
        logger.info("  %d flowsheet track entries", len(entries))

        show_to_dj, show_dj_names = load_shows(conn)
        logger.info("  %d shows with DJ mapping", len(show_to_dj))

        artist_xrefs, release_xrefs = load_cross_references(conn)
        logger.info("  %d artist xrefs, %d release xrefs", len(artist_xrefs), len(release_xrefs))
    finally:
        conn.close()

    return genres, codes, releases, entries, show_to_dj, show_dj_names, artist_xrefs, release_xrefs


def nightly_sync(args: argparse.Namespace) -> None:
    """Main orchestration: PG → resolve → PMI → export → dedup → facets → metrics → swap."""
    from semantic_index.adjacency import extract_adjacency_pairs
    from semantic_index.artist_resolver import ArtistResolver
    from semantic_index.cross_reference import CrossReferenceExtractor
    from semantic_index.facet_export import export_facet_tables
    from semantic_index.graph_metrics import compute_and_persist
    from semantic_index.node_attributes import compute_artist_stats
    from semantic_index.pipeline_db import PipelineDB
    from semantic_index.pmi import compute_pmi
    from semantic_index.sqlite_export import export_sqlite

    t0 = time.time()
    production_path = Path(args.db_path)
    min_count = args.min_count

    # --- Step 1: Prepare working copy ---
    temp_path = _prepare_working_db(production_path)
    swap_ok = False

    try:
        # --- Step 2: Load from PG ---
        (
            genres,
            codes,
            releases,
            entries,
            show_to_dj,
            show_dj_names,
            artist_xrefs,
            release_xrefs,
        ) = _load_from_pg(args.dsn)

        if not entries:
            logger.error("No flowsheet entries loaded from PG — aborting")
            sys.exit(1)

        # --- Step 3: Resolve artists ---
        logger.info("Resolving artists...")
        resolver = ArtistResolver(releases=releases, codes=codes)

        resolved_entries = []
        method_counts: dict[str, int] = {}
        for entry in entries:
            resolved = resolver.resolve(entry)
            method_counts[resolved.resolution_method] = (
                method_counts.get(resolved.resolution_method, 0) + 1
            )
            resolved_entries.append(resolved)

        n = len(entries)
        for method, count in sorted(method_counts.items(), key=lambda x: -x[1]):
            logger.info("  %-25s %8d  (%.1f%%)", method, count, count / n * 100)

        logger.info("Re-resolving raw entries with play-count-weighted fuzzy matching...")
        resolved_entries = resolver.re_resolve_with_play_counts(resolved_entries)

        # Log post-re-resolve breakdown
        post_counts: dict[str, int] = {}
        for r in resolved_entries:
            post_counts[r.resolution_method] = post_counts.get(r.resolution_method, 0) + 1
        raw_pct = post_counts.get("raw", 0) / n * 100 if n else 0
        logger.info(
            "  After re-resolve: %d raw (%.1f%%)",
            post_counts.get("raw", 0),
            raw_pct,
        )

        # --- Step 4: Adjacency + PMI ---
        logger.info("Extracting adjacency pairs...")
        pairs = extract_adjacency_pairs(resolved_entries)
        logger.info("  %d adjacency pairs", len(pairs))

        logger.info("Computing PMI...")
        pmi_edges = compute_pmi(pairs, resolved_entries)
        logger.info("  %d PMI edges", len(pmi_edges))

        # --- Step 5: Artist stats ---
        code_to_genre = {c.id: c.genre_id for c in codes}
        genre_for_release: dict[int, int] = {}
        for r in releases:
            gid = code_to_genre.get(r.library_code_id)
            if gid is not None:
                genre_for_release[r.id] = gid

        logger.info("Computing artist stats...")
        artist_stats = compute_artist_stats(
            resolved_entries,
            show_to_dj,
            genres,
            genre_for_release=genre_for_release,
        )
        logger.info("  %d unique artists", len(artist_stats))

        # --- Step 6: Cross-references ---
        code_names = {c.id: c.presentation_name for c in codes}
        release_to_code = {r.id: r.library_code_id for r in releases}
        xref_extractor = CrossReferenceExtractor(codes=code_names, release_to_code=release_to_code)

        lc_xrefs = xref_extractor.extract_library_code_xrefs(artist_xrefs)
        rel_xrefs = xref_extractor.extract_release_xrefs(release_xrefs)
        xref_edges = lc_xrefs + rel_xrefs
        logger.info("  %d cross-reference edges", len(xref_edges))

        # --- Step 7: Pipeline DB + export ---
        logger.info("Opening pipeline DB: %s", temp_path)
        pipeline_db = PipelineDB(str(temp_path))
        pipeline_db.initialize()

        all_canonical = list(dict.fromkeys(e.canonical_name for e in resolved_entries))
        logger.info("Bulk upserting %d artists...", len(all_canonical))
        pipeline_db.bulk_upsert_artists(all_canonical)

        logger.info("Clearing recomputed edge tables...")
        _clear_recomputed_tables(str(temp_path))

        logger.info("Exporting to SQLite...")
        export_sqlite(
            str(temp_path),
            artist_stats=artist_stats,
            pmi_edges=pmi_edges,
            xref_edges=xref_edges,
            min_count=min_count,
            pipeline_db=pipeline_db,
        )

        # --- Step 7b: Entity deduplication ---
        logger.info("Running entity deduplication...")
        dedup_report = pipeline_db.deduplicate_by_qid()
        if dedup_report.groups_found:
            logger.info(
                "  Dedup: %d groups, %d entities merged, %d artists reassigned, %d edges re-keyed",
                dedup_report.groups_found,
                dedup_report.entities_merged,
                dedup_report.artists_reassigned,
                dedup_report.edges_rekeyed,
            )

        # --- Step 8: Facet tables ---
        logger.info("Exporting facet tables...")
        name_to_id = pipeline_db.get_name_to_id_mapping()

        export_facet_tables(
            db_path=str(temp_path),
            resolved_entries=resolved_entries,
            name_to_id=name_to_id,
            show_to_dj=show_to_dj,
            show_dj_names=show_dj_names,
            adjacency_pairs=pairs,
        )

        # --- Step 9: Graph metrics ---
        pipeline_db.close()

        logger.info("Computing graph metrics...")
        metrics = compute_and_persist(str(temp_path))
        logger.info(
            "  %d communities, %d artists scored",
            metrics.community_count,
            metrics.artists_scored,
        )

        # --- Step 10: Checkpoint + swap ---
        _checkpoint_and_close(str(temp_path))

        size_mb = temp_path.stat().st_size / (1024 * 1024)
        elapsed = time.time() - t0
        logger.info("Pipeline complete in %.1fs (%.1f MB)", elapsed, size_mb)

        _atomic_swap(temp_path, production_path, dry_run=args.dry_run)
        swap_ok = True

    finally:
        # Clean up temp file on failure
        if not swap_ok and temp_path.exists():
            temp_path.unlink(missing_ok=True)
            logger.warning("Cleaned up temp file after failure: %s", temp_path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments and environment variables."""
    parser = argparse.ArgumentParser(
        description="Nightly sync: refresh semantic index from Backend-Service PG.",
    )
    parser.add_argument(
        "--db-path",
        default=os.environ.get("DB_PATH", DEFAULT_DB_PATH),
        help="Production SQLite DB path (default: $DB_PATH or data/wxyc_artist_graph.db)",
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("DATABASE_URL_BACKEND"),
        help="PostgreSQL DSN for Backend-Service (default: $DATABASE_URL_BACKEND)",
    )
    parser.add_argument(
        "--min-count",
        type=int,
        default=int(os.environ.get("MIN_COUNT", str(DEFAULT_MIN_COUNT))),
        help="Minimum co-occurrence count for DJ transition edges (default: 2)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run pipeline but skip atomic swap",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Entry point for the nightly sync script."""
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s [nightly_sync] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    if not args.dsn:
        logger.error("DATABASE_URL_BACKEND not set and --dsn not provided")
        sys.exit(1)

    nightly_sync(args)


if __name__ == "__main__":
    main()
