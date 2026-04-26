#!/usr/bin/env python3
"""Regenerate tests/fixtures/canned_enrichment.json from a real discogs-cache PG.

Run this only when the discogs-cache schema or
``DiscogsClient.get_bulk_enrichment`` output shape changes; the fixture is
checked into git and the E2E test does not require this script to run.

What this script does:
  1. Picks two well-populated canonical artists (defaults: ``Stereolab`` and
     ``Cat Power`` -- both appear in the WXYC canonical-data set and are well
     covered in Discogs).
  2. Calls ``DiscogsClient.get_bulk_enrichment`` against the live discogs-cache
     PostgreSQL.
  3. Writes the two payloads to ``tests/fixtures/canned_enrichment.json`` in the
     same shape the E2E test expects (``artist_a`` and ``artist_b`` keys, each
     with ``styles``, ``extra_artists``, ``labels``, ``track_artists``).

Usage:
    python scripts/generate_canned_enrichment.py \\
        --dsn postgresql://localhost:5433/discogs_cache \\
        [--artist-a "Stereolab"] [--artist-b "Cat Power"]

Requires:
    * A populated discogs-cache PostgreSQL on port 5433 (see discogs-etl repo).
    * ``pip install -e .`` in the semantic-index repo so ``semantic_index`` is importable.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from semantic_index.discogs_client import DiscogsClient

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--dsn",
        required=True,
        help="discogs-cache PostgreSQL DSN (e.g. postgresql://localhost:5433/discogs_cache)",
    )
    parser.add_argument(
        "--artist-a",
        default="Stereolab",
        help="Canonical name for the first canned artist (default: Stereolab)",
    )
    parser.add_argument(
        "--artist-b",
        default="Cat Power",
        help="Canonical name for the second canned artist (default: Cat Power)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Path to write JSON (default: tests/fixtures/canned_enrichment.json)",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    output_path = (
        Path(args.output)
        if args.output
        else Path(__file__).resolve().parent.parent
        / "tests"
        / "fixtures"
        / "canned_enrichment.json"
    )

    logger.info("Connecting to discogs-cache at %s", args.dsn)
    client = DiscogsClient(cache_dsn=args.dsn)

    artist_names = [args.artist_a, args.artist_b]
    logger.info("Fetching enrichment for: %s", artist_names)
    bulk = client.get_bulk_enrichment(artist_names)
    if not bulk:
        logger.error(
            "No enrichment returned. Check that the DSN is reachable and that "
            "the artists exist in the cache: %s",
            artist_names,
        )
        return 1

    # Bulk enrichment lowercases keys; look up by lowercase name.
    payload_a = bulk.get(args.artist_a.lower())
    payload_b = bulk.get(args.artist_b.lower())
    missing = [
        name for name, p in [(args.artist_a, payload_a), (args.artist_b, payload_b)] if not p
    ]
    if missing:
        logger.error(
            "Missing enrichment for: %s. Pick artists with summary-table coverage.", missing
        )
        return 1

    out = {
        "_comment": (
            "Canned bulk-enrichment payload for the full-pipeline E2E enrichment "
            "test. Shape mirrors DiscogsClient.get_bulk_enrichment(). The E2E "
            "test does not hardcode artist names; it overlays this payload onto "
            "the first two canonical artists in the requested batch. Regenerate "
            "via scripts/generate_canned_enrichment.py if the upstream schema changes."
        ),
        "artist_a": payload_a,
        "artist_b": payload_b,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(out, indent=2, sort_keys=False) + "\n")
    logger.info("Wrote %s", output_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
