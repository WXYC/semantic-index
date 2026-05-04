"""One-shot prune of the ``acoustic_similarity`` table to top-K neighbors per artist.

Run this against the production graph DB to immediately shrink the
``acoustic_similarity`` table without waiting for the next pipeline rebuild.
The retained edges are exactly those produced by the prune step that future
``compute_acoustic_similarity`` runs apply by default.

Usage:

    python scripts/prune_acoustic_similarity.py \\
        --db-path data/wxyc_artist_graph.db \\
        --top-k 50 \\
        [--vacuum] [--dry-run]
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import time
from pathlib import Path

from semantic_index.acousticbrainz import prune_acoustic_similarity

logger = logging.getLogger("prune_acoustic_similarity")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", required=True, type=Path, help="Graph SQLite database")
    parser.add_argument("--top-k", type=int, default=50, help="Per-artist neighbor cap")
    parser.add_argument(
        "--vacuum",
        action="store_true",
        help="Run VACUUM after prune to reclaim disk space (rewrites the whole DB)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run prune in a transaction and roll back; report what would change",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if not args.db_path.exists():
        logger.error("DB not found: %s", args.db_path)
        return 1

    conn = sqlite3.connect(str(args.db_path))
    try:
        size_before = args.db_path.stat().st_size
        t0 = time.monotonic()
        if args.dry_run:
            before, after = prune_acoustic_similarity(conn, args.top_k)
            conn.rollback()
            logger.info(
                "[dry-run] %d → %d edges (would prune %d, %.1f%%); no changes written",
                before,
                after,
                before - after,
                (before - after) / before * 100 if before else 0,
            )
            return 0

        before, after = prune_acoustic_similarity(conn, args.top_k)
        conn.commit()
        elapsed = time.monotonic() - t0
        logger.info(
            "Pruned %d → %d edges in %.1fs (kept %.1f%%)",
            before,
            after,
            elapsed,
            (after / before * 100) if before else 0,
        )

        if args.vacuum:
            t0 = time.monotonic()
            logger.info("Running VACUUM to reclaim disk space (this rewrites the DB)…")
            conn.execute("VACUUM")
            logger.info("VACUUM complete in %.1fs", time.monotonic() - t0)

        size_after = args.db_path.stat().st_size
        logger.info(
            "DB file: %.1f MiB → %.1f MiB (%+.1f MiB)",
            size_before / 1024 / 1024,
            size_after / 1024 / 1024,
            (size_after - size_before) / 1024 / 1024,
        )
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
