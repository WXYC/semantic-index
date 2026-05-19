"""Backscore the eval set with the production scoring methods.

For every row in ``labeling.jsonl`` (production + deliberately-wrong rows),
compute:

  - ``token_match_v1`` — the unmatched-content-words ratio against the
    *real* metadata of the named artists (always; even wrong rows get
    re-scored against real data so the result reflects what the labeler is
    judging, not what the model was prompted with).
  - ``claim_ratio_v1`` — Haiku-driven claim decomposition (grounded vs
    ungrounded) against the same real metadata. Mirrors the live
    ``narrative_audit`` pipeline; same prompt, same parser.

Output is ``eval_scored.jsonl`` — one row per labeling row with both scores
attached. Once human labels merge in (via ``merge_labels.py``), a follow-up
function (``compute_metrics``) computes precision / recall / F1 of each
scorer treated as a binary classifier of ``severity == 'severe'``.

Usage:
    ANTHROPIC_API_KEY=sk-... python -m scripts.eval.backscore \
        --db-path data/wxyc_artist_graph.db \
        --labeling-jsonl output/eval/labeling.jsonl \
        --out output/eval/eval_scored.jsonl \
        [--limit 0] [--skip-cached] [--threshold 0.5]

When ``--skip-cached`` is set and ``--out`` already exists, rows already in
the output (matched by ``row_id``) are skipped — useful for resumable runs
on a large corpus.

Once labels exist, generate the metrics report:
    python -m scripts.eval.backscore metrics \
        --scored output/eval/eval_scored.jsonl \
        --labeled output/eval/labeling_labeled.jake.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from pathlib import Path

logger = logging.getLogger(__name__)


def _open_ro_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _load_labeling(path: Path) -> list[dict]:
    rows: list[dict] = []
    with path.open() as fh:
        for line in fh:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _load_existing_row_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    out: set[str] = set()
    with path.open() as fh:
        for line in fh:
            try:
                r = json.loads(line)
                out.add(r["row_id"])
            except (json.JSONDecodeError, KeyError):
                continue
    return out


def _build_score_input(db: sqlite3.Connection, row: dict) -> dict:
    """The scorer's ``provided_data`` — the real metadata of the named artists.

    For production rows this matches what the model was given. For wrong rows
    (data_shuffle), the model was given DIFFERENT metadata; we re-score
    against the REAL metadata of the named artists, which is what the
    labeler sees and judges. This is intentional — the score should mirror
    the labeler's judgment, not the model's input.
    """
    from semantic_index.api.narrative import _lookup_artist_metadata

    source_meta = _lookup_artist_metadata(
        db, row["source_id"], row["source_name"], row["source_genre"], row["source_plays"]
    )
    target_meta = _lookup_artist_metadata(
        db, row["target_id"], row["target_name"], row["target_genre"], row["target_plays"]
    )
    return {"source": source_meta, "target": target_meta}


def _score_one(client, db: sqlite3.Connection, row: dict) -> dict:
    """Compute token-match v1 and claim-ratio v1 for a single eval-set row."""
    from semantic_index.api.narrative import _token_match_score
    from semantic_index.narrative_audit import _CLAIM_DECOMPOSE_PROMPT, parse_claim_counts

    input_data = _build_score_input(db, row)
    narrative = row.get("narrative") or ""
    token_score = _token_match_score(narrative, input_data) if narrative else 0.0

    if not narrative:
        return {
            "token_match_v1": 0.0,
            "claim_ratio_v1": 0.0,
            "claim_grounded": 0,
            "claim_ungrounded": 0,
        }

    verify_payload = json.dumps(
        {"narrative": narrative, "provided_data": input_data},
        separators=(",", ":"),
    )
    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=400,
        system=_CLAIM_DECOMPOSE_PROMPT,
        messages=[{"role": "user", "content": verify_payload}],
    )
    grounded, ungrounded = parse_claim_counts(response.content[0].text)
    total = grounded + ungrounded
    ratio = (ungrounded / total) if total else 0.0

    return {
        "token_match_v1": round(token_score, 4),
        "claim_ratio_v1": round(ratio, 4),
        "claim_grounded": grounded,
        "claim_ungrounded": ungrounded,
    }


def cmd_score(args: argparse.Namespace) -> int:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY is required")
        return 1
    import anthropic

    client = anthropic.Anthropic(api_key=api_key)
    db = _open_ro_db(args.db_path)
    rows = _load_labeling(Path(args.labeling_jsonl))

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    already = _load_existing_row_ids(out_path) if args.skip_cached else set()
    if already:
        logger.info("Skipping %d rows already in %s", len(already), out_path)

    todo = [r for r in rows if r["row_id"] not in already]
    if args.limit > 0:
        todo = todo[: args.limit]

    logger.info("Scoring %d rows -> %s", len(todo), out_path)
    written = errors = 0
    with out_path.open("a") as fh:
        for i, row in enumerate(todo, 1):
            t0 = time.time()
            try:
                scores = _score_one(client, db, row)
            except Exception:  # noqa: BLE001
                logger.exception("Scoring failed for %s", row.get("row_id"))
                errors += 1
                continue
            elapsed_ms = int((time.time() - t0) * 1000)

            out_row = {
                "row_id": row["row_id"],
                "cell_id": row.get("cell_id"),
                "construction_method": row.get("construction_method", "production"),
                "narrative": row.get("narrative"),
                "scores": scores,
                "scored_at_ms": elapsed_ms,
            }
            fh.write(json.dumps(out_row, separators=(",", ":")) + "\n")
            fh.flush()
            written += 1
            if i % 20 == 0 or i == len(todo):
                logger.info(
                    "%d/%d scored (errors=%d, last token=%.3f claim=%.3f, %dms)",
                    i,
                    len(todo),
                    errors,
                    scores["token_match_v1"],
                    scores["claim_ratio_v1"],
                    elapsed_ms,
                )

    logger.info("Done: wrote %d, errors=%d", written, errors)
    return 0 if errors == 0 else 1


def cmd_metrics(args: argparse.Namespace) -> int:
    """Compute precision/recall/F1 for each scorer treated as a binary classifier.

    Positive class = ``severity in {severe, minor}`` (any wrongness). Threshold
    crossings on the scorer's value mark predicted positives. For each method
    we report:
      - precision (predicted_positive AND label_positive) / predicted_positive
      - recall    (predicted_positive AND label_positive) / label_positive
      - F1
      - per-failure_mode breakdown of recall (which categories does each
        scorer catch?)
      - per-construction_method breakdown of recall (load-bearing for #277:
        confirms grounding-fidelity scorers have lower recall on
        field_corruption than on data_shuffle — the gap the constraint
        ontology should fill).
    """
    scored = {r["row_id"]: r for r in _load_labeling(Path(args.scored))}
    labeled = _load_labeling(Path(args.labeled))

    by_method: dict[str, dict[str, list[float]]] = {
        "token_match_v1": {"pos_scores": [], "neg_scores": []},
        "claim_ratio_v1": {"pos_scores": [], "neg_scores": []},
    }
    by_mode_recall: dict[str, dict[str, list[bool]]] = {
        "token_match_v1": {},
        "claim_ratio_v1": {},
    }
    by_construction_recall: dict[str, dict[str, list[bool]]] = {
        "token_match_v1": {},
        "claim_ratio_v1": {},
    }

    n_labeled = n_pos = 0
    for row in labeled:
        rid = row["row_id"]
        if rid not in scored:
            continue
        label = row.get("label") or {}
        severity = label.get("severity")
        if not severity:
            continue
        n_labeled += 1
        is_positive = severity in {"severe", "minor"}
        if is_positive:
            n_pos += 1
        scores = scored[rid]["scores"]
        for method in ("token_match_v1", "claim_ratio_v1"):
            score = scores[method]
            (
                by_method[method]["pos_scores"] if is_positive else by_method[method]["neg_scores"]
            ).append(score)

        if is_positive:
            mode = label.get("failure_mode") or "unspecified"
            construction = scored[rid].get("construction_method") or "production"
            for method in ("token_match_v1", "claim_ratio_v1"):
                hit = scores[method] > args.threshold
                by_mode_recall[method].setdefault(mode, []).append(hit)
                by_construction_recall[method].setdefault(construction, []).append(hit)

    print(f"Labeled rows: {n_labeled} (positive: {n_pos}, negative: {n_labeled - n_pos})")
    print(f"Threshold for binary classification: {args.threshold}")
    print()

    for method in ("token_match_v1", "claim_ratio_v1"):
        pos = by_method[method]["pos_scores"]
        neg = by_method[method]["neg_scores"]
        tp = sum(1 for s in pos if s > args.threshold)
        fn = len(pos) - tp
        fp = sum(1 for s in neg if s > args.threshold)
        tn = len(neg) - fp
        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
        mean_pos = sum(pos) / len(pos) if pos else 0.0
        mean_neg = sum(neg) / len(neg) if neg else 0.0
        print(f"=== {method} ===")
        print(f"  mean(positive)={mean_pos:.3f}  mean(negative)={mean_neg:.3f}")
        print(f"  TP={tp} FP={fp} TN={tn} FN={fn}")
        print(f"  precision={precision:.3f}  recall={recall:.3f}  F1={f1:.3f}")
        if by_mode_recall[method]:
            print("  recall by failure_mode:")
            for mode, hits in sorted(by_mode_recall[method].items()):
                r = sum(hits) / len(hits) if hits else 0.0
                print(f"    {mode:30} n={len(hits):3} recall={r:.3f}")
        if by_construction_recall[method]:
            print("  recall by construction_method:")
            for construction, hits in sorted(by_construction_recall[method].items()):
                r = sum(hits) / len(hits) if hits else 0.0
                print(f"    {construction:30} n={len(hits):3} recall={r:.3f}")
        print()
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    score = sub.add_parser("score", help="Compute scores for the eval-set rows.")
    score.add_argument("--db-path", default="data/wxyc_artist_graph.db")
    score.add_argument("--labeling-jsonl", required=True)
    score.add_argument("--out", required=True)
    score.add_argument("--limit", type=int, default=0)
    score.add_argument("--skip-cached", action="store_true")
    score.set_defaults(func=cmd_score)

    metrics = sub.add_parser("metrics", help="Compute precision/recall/F1 against labels.")
    metrics.add_argument("--scored", required=True)
    metrics.add_argument("--labeled", required=True)
    metrics.add_argument(
        "--threshold",
        type=float,
        default=0.5,
        help="Score above which a row is predicted positive (default 0.5)",
    )
    metrics.set_defaults(func=cmd_metrics)

    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
