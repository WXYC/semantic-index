"""CLI: audit cached narratives by claim-ratio (#230).

Samples N cached narratives, runs each through a Haiku verifier, and records
results to a sidecar audit DB. Flags any narrative whose ungrounded-claim
ratio exceeds the threshold for downstream review (manual inspection or
regeneration).

Usage:
    python scripts/audit_narratives.py --db-path data/wxyc_artist_graph.db [--n 100] [--threshold 0.2]

Environment:
    ANTHROPIC_API_KEY        — required; the audit calls Claude Haiku.
    NARRATIVE_AUDIT_CLAIM_THRESHOLD — overrides --threshold default (0.2).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from semantic_index.narrative_audit import run_audit

logger = logging.getLogger(__name__)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db-path",
        default=os.environ.get("DB_PATH", "data/wxyc_artist_graph.db"),
        help="Path to the production SQLite graph database (the narrative-cache "
        "sidecar lives at <db-path>.narrative-cache.db).",
    )
    parser.add_argument("--n", type=int, default=100, help="Sample size (default: 100).")
    parser.add_argument(
        "--threshold",
        type=float,
        default=None,
        help="Claim-ratio threshold above which a narrative is flagged. "
        "Defaults to NARRATIVE_AUDIT_CLAIM_THRESHOLD env var or 0.2.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _parse_args(argv)

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY is required for the audit run")
        return 1

    threshold = args.threshold
    if threshold is None:
        raw = os.environ.get("NARRATIVE_AUDIT_CLAIM_THRESHOLD")
        threshold = float(raw) if raw else 0.2

    try:
        import anthropic
    except ImportError:
        logger.error("anthropic SDK is not installed; install via `pip install anthropic`")
        return 1

    client = anthropic.Anthropic(api_key=api_key)
    summary = run_audit(args.db_path, client=client, n=args.n, threshold=threshold)
    logger.info(
        "Audit complete: audited=%d flagged=%d threshold=%.2f",
        summary["audited"],
        summary["flagged"],
        threshold,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
