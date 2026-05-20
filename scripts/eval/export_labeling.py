"""Export the narrative eval set as a labeling sheet (CSV + JSONL backing).

Joins ``eval_narratives.jsonl`` with the per-artist input data the production
narrative endpoint actually saw (looked up live from the SQLite graph DB), then
emits one row per narrative with empty label columns for human entry.

The CSV is laid out for Google Sheets — narratives, source/target data, and
shared neighbors live in adjacent columns so a labeler can read the narrative
and verify against the data without scrolling. The JSONL preserves the same
data plus a stable ``row_id`` and ``cell_id``, so post-labeling analysis can
stratify by matrix cell.

Usage:
    python -m scripts.eval.export_labeling \
        --db-path data/wxyc_artist_graph.db \
        --narratives output/eval/eval_narratives.jsonl \
        --csv-out output/eval/labeling.csv \
        --jsonl-out output/eval/labeling.jsonl \
        [--seed 7]

Order is randomized with the provided seed so a labeler doesn't see all
HIGH-RICH rows clustered. ``row_id`` lets you re-join labels to the original
narrative regardless of row order.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import random
import sqlite3
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


def _open_ro_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _format_artist_data(meta: dict) -> str:
    """Render a per-artist metadata dict as a readable multi-line string for the sheet."""
    parts: list[str] = []
    parts.append(f"name: {meta.get('name')}")
    parts.append(f"plays: {meta.get('total_plays')}")
    if (g := meta.get("genre")) is not None:
        parts.append(f"genre: {g}")
    if styles := meta.get("styles"):
        parts.append(f"styles: {', '.join(styles)}")
    if audio := meta.get("audio"):
        for k, v in audio.items():
            parts.append(f"audio.{k}: {v}")
    return "\n".join(parts)


def _format_shared_neighbors(neighbors: list[dict]) -> str:
    if not neighbors:
        return ""
    return ", ".join(f"{n['name']} (aa {n['aa_score']:.2f})" for n in neighbors)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--db-path", default="data/wxyc_artist_graph.db")
    ap.add_argument("--narratives", required=True, help="Input eval_narratives.jsonl")
    ap.add_argument(
        "--wrong",
        default=None,
        help="Optional eval_wrong.jsonl. When provided, deliberately-wrong rows "
        "are interleaved with the production rows. Construction-method markers "
        "and expected-label fields stay in the JSONL backing but are NOT shown "
        "in the labeler-facing CSV (so the labeler doesn't see the answer).",
    )
    ap.add_argument(
        "--bait",
        default=None,
        help="Optional eval_bait.jsonl from build_bait_set.py. Interleaved like "
        "--wrong; construction_method=pretraining_bait and the regime tag stay "
        "in the JSONL backing only.",
    )
    ap.add_argument("--csv-out", required=True)
    ap.add_argument("--jsonl-out", required=True)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # Import here so --help works without the API extras.
    from semantic_index.api.narrative import (
        _lookup_artist_metadata,
        _rank_shared_neighbors_by_aa,
    )

    db = _open_ro_db(args.db_path)

    rows_in: list[dict] = []
    with Path(args.narratives).open() as fh:
        for line in fh:
            line = line.strip()
            if line:
                rows_in.append(json.loads(line))

    if args.wrong:
        with Path(args.wrong).open() as fh:
            for line in fh:
                line = line.strip()
                if line:
                    rows_in.append(json.loads(line))
        logger.info("Merged %s wrong rows in addition to production rows", args.wrong)

    if args.bait:
        with Path(args.bait).open() as fh:
            for line in fh:
                line = line.strip()
                if line:
                    rows_in.append(json.loads(line))
        logger.info("Merged %s bait rows in addition to production rows", args.bait)

    rng = random.Random(args.seed)
    rng.shuffle(rows_in)

    csv_path = Path(args.csv_out)
    jsonl_path = Path(args.jsonl_out)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)

    csv_columns = [
        "row_id",
        "cell_id",
        "pair",
        "narrative",
        "source_data",
        "target_data",
        "shared_neighbors",
        "raw_count",
        "aa_sum",
        "insufficient_signal",
        "token_match_score",
        "severity",  # blank — labeler fills (severe / minor / not_wrong)
        "failure_mode",  # blank — labeler fills (subject_hallucination / ...)
        "notes",  # blank — labeler freeform
    ]

    written = 0
    with csv_path.open("w", newline="") as csv_fh, jsonl_path.open("w") as jl_fh:
        writer = csv.DictWriter(csv_fh, fieldnames=csv_columns, quoting=csv.QUOTE_ALL)
        writer.writeheader()

        for i, r in enumerate(rows_in):
            row_id = f"R{i + 1:04d}"
            source_meta = _lookup_artist_metadata(
                db, r["source_id"], r["source_name"], r["source_genre"], r["source_plays"]
            )
            target_meta = _lookup_artist_metadata(
                db, r["target_id"], r["target_name"], r["target_genre"], r["target_plays"]
            )
            neighbors = _rank_shared_neighbors_by_aa(db, r["source_id"], r["target_id"])[:5]

            csv_row = {
                "row_id": row_id,
                "cell_id": r["cell_id"],
                "pair": f"{r['source_name']} <-> {r['target_name']}",
                "narrative": r.get("narrative") or "",
                "source_data": _format_artist_data(source_meta),
                "target_data": _format_artist_data(target_meta),
                "shared_neighbors": _format_shared_neighbors(neighbors),
                "raw_count": r.get("raw_count", "") if r.get("raw_count") is not None else "",
                "aa_sum": r.get("aa_sum", ""),
                "insufficient_signal": "yes" if r.get("insufficient_signal") else "",
                "token_match_score": f"{r.get('token_match_score', 0.0):.3f}",
                "severity": "",
                "failure_mode": "",
                "notes": "",
            }
            writer.writerow(csv_row)

            jl_row = dict(r)
            jl_row["row_id"] = row_id
            jl_row["source_data"] = source_meta
            jl_row["target_data"] = target_meta
            jl_row["shared_neighbors"] = neighbors
            jl_fh.write(json.dumps(jl_row, separators=(",", ":")) + "\n")

            written += 1

    logger.info("Wrote %d rows -> %s + %s", written, csv_path, jsonl_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
