"""Build deliberately-wrong narratives for detector calibration.

The 153 production narratives in ``eval_narratives.jsonl`` are wrongness-by-luck:
some will be wrong, some not, in proportions reflecting the live system. They
test labeler IRR and represent natural distribution. Detector calibration
needs *gold positives* — rows where wrongness is known by construction.

This script generates the data-shuffle variant only:

  1. Pick an existing production pair (A, B) — the names that appear in the
     narrative.
  2. Pick a *different* random pair (C, D) — the metadata source.
  3. Build a prompt that names A and B but supplies C's metadata as ``source``
     and D's metadata as ``target``.
  4. Call Claude Haiku directly (not via the endpoint, so the production
     ``narrative-cache.db`` stays clean). The model produces a narrative that
     names A and B but describes C and D's musical attributes. Wrong by
     construction — failure_mode=subject_hallucination.

Field-corruption and pretraining-bait constructions are not implemented here.

Usage:
    ANTHROPIC_API_KEY=sk-... python -m scripts.eval.build_wrong_set \
        --db-path data/wxyc_artist_graph.db \
        --narratives output/eval/eval_narratives.jsonl \
        --out output/eval/eval_wrong.jsonl \
        [--n 30] [--seed 7]

Output JSONL row mirrors the production-narrative shape so ``export_labeling``
treats both pools identically. The labeler always sees the *real* metadata for
the artists named in the narrative — that's how they detect the wrongness.
Extra fields:
    construction_method: "data_shuffle"
    metadata_source: {source_id: int, target_id: int}  # the C/D pair we shuffled in
    expected_label: {severity: "severe", failure_mode: "subject_hallucination"}
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import sqlite3
import sys
import time
from pathlib import Path

logger = logging.getLogger(__name__)


def _open_ro_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _load_eligible_pairs(narratives_path: Path) -> list[dict]:
    """Return only generative narrative rows (skip canned insufficient_signal placeholders).

    Insufficient-signal rows can't meaningfully be data-shuffled — the canned
    text doesn't make claims about the music, just acknowledges low signal.
    """
    out: list[dict] = []
    with narratives_path.open() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            if r.get("insufficient_signal"):
                continue
            if not r.get("narrative"):
                continue
            out.append(r)
    return out


def _build_user_message(source_name: str, target_name: str, source_meta: dict, target_meta: dict) -> str:
    """Construct the user-message JSON the model sees.

    Identical shape to ``narrative._build_prompt`` but no relationships /
    facets / shared_neighbors fields — those would carry leakage from the real
    pair, defeating the shuffle. The minimal shape is enough to elicit a
    descriptive narrative the model will ground in the (mismatched) metadata.
    """
    source_view = dict(source_meta)
    source_view["name"] = source_name
    target_view = dict(target_meta)
    target_view["name"] = target_name
    payload = {"source": source_view, "target": target_view}
    return json.dumps(payload, separators=(",", ":"))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db-path", default="data/wxyc_artist_graph.db")
    ap.add_argument("--narratives", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--n", type=int, default=30, help="Number of shuffled narratives to generate")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--sleep", type=float, default=0.0)
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY is required")
        return 1

    # Lazy imports.
    import anthropic

    from semantic_index.api.narrative import (
        _SYSTEM_PROMPT,
        _lookup_artist_metadata,
    )

    db = _open_ro_db(args.db_path)
    eligible = _load_eligible_pairs(Path(args.narratives))
    if len(eligible) < 2:
        logger.error("Need at least 2 generative narratives in --narratives; got %d", len(eligible))
        return 1
    logger.info("Eligible production pairs: %d", len(eligible))

    rng = random.Random(args.seed)
    # Sample names_pair (A, B) and metadata_pair (C, D) such that the
    # underlying ARTIST IDs do not overlap. Same-id overlap would partially
    # leak the right metadata into the shuffled prompt and weaken the gold-
    # positive guarantee.
    pairs_for_names = rng.sample(eligible, min(args.n, len(eligible)))

    client = anthropic.Anthropic(api_key=api_key)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    written = 0
    with out_path.open("w") as fh:
        for i, name_row in enumerate(pairs_for_names, 1):
            # Find a metadata-source pair with no id overlap.
            tries = 0
            meta_row = None
            while tries < 50:
                cand = rng.choice(eligible)
                if cand["source_id"] not in (name_row["source_id"], name_row["target_id"]) and cand["target_id"] not in (name_row["source_id"], name_row["target_id"]):
                    meta_row = cand
                    break
                tries += 1
            if meta_row is None:
                logger.warning("Skipping %d: couldn't find non-overlapping metadata pair", i)
                continue

            source_meta = _lookup_artist_metadata(
                db,
                meta_row["source_id"],
                meta_row["source_name"],
                meta_row["source_genre"],
                meta_row["source_plays"],
            )
            target_meta = _lookup_artist_metadata(
                db,
                meta_row["target_id"],
                meta_row["target_name"],
                meta_row["target_genre"],
                meta_row["target_plays"],
            )
            user_message = _build_user_message(
                name_row["source_name"], name_row["target_name"],
                source_meta, target_meta,
            )

            t0 = time.time()
            try:
                response = client.messages.create(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=150,
                    system=_SYSTEM_PROMPT,
                    messages=[{"role": "user", "content": user_message}],
                )
                narrative = response.content[0].text
            except Exception as exc:  # noqa: BLE001 -- exit cleanly on API failure
                logger.exception("Anthropic call failed for %s / %s", name_row["source_name"], name_row["target_name"])
                continue
            elapsed_ms = int((time.time() - t0) * 1000)

            row = {
                # Keep the SAME shape as eval_narratives.jsonl so export_labeling
                # treats both pools identically. source_id/target_id point to the
                # NAMED artists, so the labeler sees the named artists' REAL
                # metadata when reading the row.
                "cell_id": "WRONG-DATA-SHUFFLE",
                "fame": name_row["fame"],
                "richness": name_row["richness"],
                "genre": name_row["genre"],
                "edge": name_row["edge"],
                "source_id": name_row["source_id"],
                "target_id": name_row["target_id"],
                "source_name": name_row["source_name"],
                "target_name": name_row["target_name"],
                "source_genre": name_row["source_genre"],
                "target_genre": name_row["target_genre"],
                "source_plays": name_row["source_plays"],
                "target_plays": name_row["target_plays"],
                "narrative": narrative,
                "cached": False,
                "insufficient_signal": False,
                "token_match_score": 0.0,  # not computed for these
                "low_grounding": False,
                "http_status": 200,
                "latency_ms": elapsed_ms,
                "construction_method": "data_shuffle",
                "metadata_source": {
                    "source_id": meta_row["source_id"],
                    "target_id": meta_row["target_id"],
                    "source_name": meta_row["source_name"],
                    "target_name": meta_row["target_name"],
                },
                "expected_label": {
                    "severity": "severe",
                    "failure_mode": "subject_hallucination",
                },
            }
            fh.write(json.dumps(row, separators=(",", ":")) + "\n")
            fh.flush()
            written += 1
            logger.info(
                "%d/%d: named=%s/%s metadata=%s/%s elapsed=%dms",
                i, len(pairs_for_names),
                name_row["source_name"], name_row["target_name"],
                meta_row["source_name"], meta_row["target_name"],
                elapsed_ms,
            )

            if args.sleep > 0 and i < len(pairs_for_names):
                time.sleep(args.sleep)

    logger.info("Wrote %d data-shuffle rows -> %s", written, out_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
