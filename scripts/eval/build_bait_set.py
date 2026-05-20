"""Build the pretraining-bait subset of the narrative eval set.

Closes WXYC/semantic-index#278.

Unlike ``build_wrong_set.py`` (which calls Haiku directly with deliberately
mismatched metadata), this script drives the **production** narrative endpoint
via FastAPI's ``TestClient`` — the same pattern as
``scripts/eval/generate_narratives.py``. The point of pretraining-bait isn't to
inject corrupt data; it's to test whether the production prompt suppresses
pretraining-driven hallucinations on pairs where the *real* WXYC data is sparse
and the model's pretraining knowledge of the artists is rich.

Each pair carries a regime label:

  - ``regime=above`` — both artists exceed ``_DEFAULT_ANON_PLAY_THRESHOLD`` (800).
    The endpoint anonymizes them as "Artist A" / "Artist B" before calling the
    LLM. Expected outcome: anonymization suppresses the bait; the narrative
    refuses to invent connections.
  - ``regime=below`` — both artists are at/under the threshold and reach the
    LLM with their real names. Expected outcome: pretraining knowledge leaks
    into the narrative as a subject-hallucination.

The pair list lives in ``scripts/eval/bait_pairs.json`` with curator notes per
pair. Add new pairs to that file rather than editing this script.

Usage::

    ANTHROPIC_API_KEY=sk-... python -m scripts.eval.build_bait_set \\
        --db-path data/wxyc_artist_graph.db \\
        --pairs scripts/eval/bait_pairs.json \\
        --out output/eval/eval_bait.jsonl \\
        [--skip-cached]

Output row shape mirrors ``eval_narratives.jsonl`` + ``eval_wrong.jsonl``: the
endpoint response plus ``construction_method=pretraining_bait``,
``expected_label``, ``regime``, and ``bait_notes``.
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

ANON_PLAY_THRESHOLD = 800  # mirrors semantic_index.api.narrative._DEFAULT_ANON_PLAY_THRESHOLD

CONSTRUCTION_METHOD = "pretraining_bait"
EXPECTED_LABEL = {"severity": "severe", "failure_mode": "subject_hallucination"}


def _open_ro_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _lookup_artist(db: sqlite3.Connection, artist_id: int) -> dict | None:
    """Return id/canonical_name/genre/total_plays or None if unknown."""
    row = db.execute(
        "SELECT id, canonical_name, genre, total_plays FROM artist WHERE id = ?",
        (artist_id,),
    ).fetchone()
    if row is None:
        return None
    return {
        "id": int(row["id"]),
        "name": row["canonical_name"],
        "genre": row["genre"],
        "total_plays": int(row["total_plays"]),
    }


def classify_regime(source_plays: int, target_plays: int, threshold: int) -> str:
    """Return ``above`` (both anonymized), ``below`` (both raw), or ``mixed``.

    Mirrors ``_build_anonymization_map``: anonymization fires strictly when
    ``total_plays > threshold``. A pair at the threshold exactly is *below* for
    that artist (and thus reaches the LLM with the real name).
    """
    a_above = source_plays > threshold
    b_above = target_plays > threshold
    if a_above and b_above:
        return "above"
    if not a_above and not b_above:
        return "below"
    return "mixed"


def load_bait_pairs(path: Path) -> list[dict]:
    """Read and validate the curated bait pair list.

    Rejects empty/missing ``pairs``, missing required keys, unknown regime
    values, and duplicate ``(source_id, target_id)`` entries. The duplicate
    check matters because the driver appends to its output file and uses
    ``(source_id, target_id)`` as the cache key for ``--skip-cached``;
    duplicates in the input would produce duplicate output rows.
    """
    data = json.loads(path.read_text())
    pairs = data.get("pairs", [])
    if not isinstance(pairs, list) or not pairs:
        raise ValueError(f"Bait pair file {path} has no 'pairs' list")
    seen: set[tuple[int, int]] = set()
    for i, p in enumerate(pairs):
        for field in ("source_id", "target_id", "regime", "bait_notes"):
            if field not in p:
                raise ValueError(f"Bait pair #{i} missing required key: {field}")
        if p["regime"] not in ("above", "below", "mixed"):
            raise ValueError(f"Bait pair #{i} regime={p['regime']!r} not in {{above,below,mixed}}")
        pair_key = (int(p["source_id"]), int(p["target_id"]))
        if pair_key in seen:
            raise ValueError(
                f"Bait pair #{i} duplicates (source_id, target_id)={pair_key} from an earlier entry"
            )
        seen.add(pair_key)
    return pairs


def _load_cached_keys(out_path: Path) -> set[tuple[int, int]]:
    """Return the (source_id, target_id) pairs already in the output file."""
    if not out_path.exists():
        return set()
    keys: set[tuple[int, int]] = set()
    with out_path.open() as fh:
        for line in fh:
            try:
                r = json.loads(line)
                keys.add((int(r["source_id"]), int(r["target_id"])))
            except (json.JSONDecodeError, KeyError, ValueError):
                continue
    return keys


def build_row(
    pair: dict,
    source: dict,
    target: dict,
    response_status: int,
    response_body: dict | None,
    response_error: str | None,
    latency_ms: int,
) -> dict:
    """Assemble a JSONL row from one TestClient response.

    Keeps the field order aligned with ``eval_narratives.jsonl`` so
    ``export_labeling.py`` can ingest the file without special-casing.
    """
    regime = classify_regime(source["total_plays"], target["total_plays"], ANON_PLAY_THRESHOLD)
    row: dict = {
        "cell_id": f"BAIT-{regime.upper()}",
        "source_id": source["id"],
        "target_id": target["id"],
        "source_name": source["name"],
        "target_name": target["name"],
        "source_genre": source["genre"],
        "target_genre": target["genre"],
        "source_plays": source["total_plays"],
        "target_plays": target["total_plays"],
        "http_status": response_status,
        "latency_ms": latency_ms,
    }
    if response_body is not None:
        row["narrative"] = response_body.get("narrative")
        row["cached"] = response_body.get("cached", False)
        row["insufficient_signal"] = response_body.get("insufficient_signal", False)
        row["token_match_score"] = response_body.get("token_match_score", 0.0)
        row["low_grounding"] = response_body.get("low_grounding", False)
    else:
        row["narrative"] = None
        row["error"] = (response_error or "")[:500]

    row["construction_method"] = CONSTRUCTION_METHOD
    row["regime"] = regime
    row["bait_notes"] = pair.get("bait_notes", "")
    row["expected_label"] = EXPECTED_LABEL
    return row


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--db-path", default="data/wxyc_artist_graph.db")
    ap.add_argument(
        "--pairs",
        default="scripts/eval/bait_pairs.json",
        help="Path to the curated bait pair JSON",
    )
    ap.add_argument("--out", required=True, help="Output JSONL path")
    ap.add_argument(
        "--skip-cached",
        action="store_true",
        help="Skip pairs whose (source_id, target_id) is already in --out",
    )
    ap.add_argument("--sleep", type=float, default=0.0, help="Sleep between requests (seconds)")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY is required")
        return 1

    # Lazy imports so --help works without the API extras installed.
    from fastapi.testclient import TestClient

    from semantic_index.api.app import create_app

    db = _open_ro_db(args.db_path)
    pairs = load_bait_pairs(Path(args.pairs))
    logger.info("Loaded %d bait pairs from %s", len(pairs), args.pairs)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    already = _load_cached_keys(out_path) if args.skip_cached else set()
    if already:
        logger.info("Skipping %d pairs already in %s", len(already), out_path)

    app = create_app(args.db_path, anthropic_api_key=api_key)
    client = TestClient(app)

    written = errors = insufficient = 0
    with out_path.open("a") as fh:
        for i, pair in enumerate(pairs, 1):
            source = _lookup_artist(db, int(pair["source_id"]))
            target = _lookup_artist(db, int(pair["target_id"]))
            if source is None or target is None:
                logger.error(
                    "Pair %d/%d: missing artist (source=%s target=%s) — skipping",
                    i,
                    len(pairs),
                    pair["source_id"],
                    pair["target_id"],
                )
                errors += 1
                continue

            actual_regime = classify_regime(
                source["total_plays"], target["total_plays"], ANON_PLAY_THRESHOLD
            )
            if actual_regime != pair["regime"]:
                logger.warning(
                    "Pair %d/%d %s↔%s: curator said regime=%s but DB plays (%d, %d) "
                    "classify as %s — proceeding with actual regime",
                    i,
                    len(pairs),
                    source["name"],
                    target["name"],
                    pair["regime"],
                    source["total_plays"],
                    target["total_plays"],
                    actual_regime,
                )

            key = (source["id"], target["id"])
            if key in already:
                continue

            url = f"/graph/artists/{source['id']}/explain/{target['id']}/narrative"
            t0 = time.time()
            response = client.get(url)
            elapsed_ms = int((time.time() - t0) * 1000)

            if response.status_code == 200:
                row = build_row(pair, source, target, 200, response.json(), None, elapsed_ms)
                if row.get("insufficient_signal"):
                    insufficient += 1
                    logger.warning(
                        "Pair %s↔%s came back insufficient_signal — bait test not exercised. "
                        "Re-curate or accept the row as-is.",
                        source["name"],
                        target["name"],
                    )
            else:
                row = build_row(
                    pair, source, target, response.status_code, None, response.text, elapsed_ms
                )
                errors += 1

            fh.write(json.dumps(row, separators=(",", ":")) + "\n")
            fh.flush()
            written += 1
            logger.info(
                "%d/%d %s↔%s regime=%s status=%d %dms",
                i,
                len(pairs),
                source["name"],
                target["name"],
                row.get("regime"),
                response.status_code,
                elapsed_ms,
            )

            if args.sleep > 0 and i < len(pairs):
                time.sleep(args.sleep)

    logger.info(
        "Wrote %d bait rows (errors=%d, insufficient_signal=%d) -> %s",
        written,
        errors,
        insufficient,
        out_path,
    )
    # Exit non-zero on transport errors. ``insufficient_signal`` is surfaced in
    # the count above and visible in each row, but it is a *content* outcome
    # (the production endpoint chose not to narrate) rather than a script
    # failure — leave the exit code to the operator's downstream gating.
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
