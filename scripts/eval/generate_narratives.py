"""Generate production narratives for sampled pairs.

Drives the live ``/graph/artists/{id}/explain/{id}/narrative`` endpoint via
FastAPI's ``TestClient`` so prompt assembly, AA-gating, anonymization, scoring,
and the regen loop are exercised exactly as users see them. The narrative
sidecar cache populates as a side effect.

Output is a JSONL file pairing each input row from ``sample_pairs.py`` with the
endpoint's response, suitable for downstream eval-set construction.

Usage:
    ANTHROPIC_API_KEY=sk-... python -m scripts.eval.generate_narratives \
        --db-path data/wxyc_artist_graph.db \
        --pairs output/eval/eval_pairs.jsonl \
        --out output/eval/eval_narratives.jsonl \
        [--limit 5] [--skip-cached]

Output JSONL row: input row keys (cell_id, source_id, ...) plus:
    {
      "narrative": "...",
      "cached": false,
      "insufficient_signal": false,
      "token_match_score": 0.21,
      "low_grounding": false,
      "http_status": 200
    }

When the API returns a non-2xx, ``narrative`` is null and ``error`` carries the
detail.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

logger = logging.getLogger(__name__)


def _load_pairs(path: Path) -> list[dict]:
    rows: list[dict] = []
    with path.open() as fh:
        for line in fh:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _load_cached_keys(out_path: Path) -> set[tuple[int, int]]:
    """Return set of (source_id, target_id) pairs already written to ``out_path``."""
    if not out_path.exists():
        return set()
    keys: set[tuple[int, int]] = set()
    with out_path.open() as fh:
        for line in fh:
            try:
                row = json.loads(line)
                keys.add((int(row["source_id"]), int(row["target_id"])))
            except (json.JSONDecodeError, KeyError, ValueError):
                continue
    return keys


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db-path", default="data/wxyc_artist_graph.db")
    ap.add_argument("--pairs", required=True, help="Pair list JSONL from sample_pairs.py")
    ap.add_argument("--out", required=True, help="Output JSONL path")
    ap.add_argument("--limit", type=int, default=0, help="Max requests (0 = all)")
    ap.add_argument(
        "--skip-cached",
        action="store_true",
        help="Skip pairs whose (source_id, target_id) already appear in --out",
    )
    ap.add_argument("--sleep", type=float, default=0.0, help="Sleep between requests (seconds)")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY is required")
        return 1

    # Lazy imports so the script's --help works without the optional API extras.
    from fastapi.testclient import TestClient

    from semantic_index.api.app import create_app

    app = create_app(args.db_path, anthropic_api_key=api_key)
    client = TestClient(app)

    pairs = _load_pairs(Path(args.pairs))
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    already = _load_cached_keys(out_path) if args.skip_cached else set()
    if already:
        logger.info("Skipping %d pairs already in %s", len(already), out_path)

    todo = [
        p for p in pairs
        if (int(p["source_id"]), int(p["target_id"])) not in already
        and (int(p["target_id"]), int(p["source_id"])) not in already
    ]
    if args.limit > 0:
        todo = todo[: args.limit]

    logger.info("Generating %d narratives -> %s", len(todo), out_path)
    written = 0
    insufficient = 0
    cached = 0
    errors = 0

    with out_path.open("a") as fh:
        for i, p in enumerate(todo, 1):
            url = f"/graph/artists/{p['source_id']}/explain/{p['target_id']}/narrative"
            t0 = time.time()
            response = client.get(url)
            elapsed = time.time() - t0

            row = dict(p)
            row["http_status"] = response.status_code
            row["latency_ms"] = int(elapsed * 1000)

            if response.status_code == 200:
                body = response.json()
                row["narrative"] = body.get("narrative")
                row["cached"] = body.get("cached", False)
                row["insufficient_signal"] = body.get("insufficient_signal", False)
                row["token_match_score"] = body.get("token_match_score", 0.0)
                row["low_grounding"] = body.get("low_grounding", False)
                if row["insufficient_signal"]:
                    insufficient += 1
                if row["cached"]:
                    cached += 1
            else:
                row["narrative"] = None
                row["error"] = response.text[:500]
                errors += 1

            fh.write(json.dumps(row, separators=(",", ":")) + "\n")
            fh.flush()
            written += 1

            if i % 10 == 0 or i == len(todo):
                logger.info(
                    "%d/%d done (cached=%d insufficient=%d errors=%d, last=%dms)",
                    i, len(todo), cached, insufficient, errors, row["latency_ms"],
                )

            if args.sleep > 0 and i < len(todo):
                time.sleep(args.sleep)

    logger.info(
        "Wrote %d narratives (cached=%d insufficient=%d errors=%d) -> %s",
        written, cached, insufficient, errors, out_path,
    )
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
