"""Build deliberately-wrong narratives for detector calibration.

The 153 production narratives in ``eval_narratives.jsonl`` are wrongness-by-luck:
some will be wrong, some not, in proportions reflecting the live system. They
test labeler IRR and represent natural distribution. Detector calibration
needs *gold positives* — rows where wrongness is known by construction.

Two construction modes are implemented:

  ``data_shuffle`` (default) — Pick an existing production pair (A, B), then
  pick a *different* random pair (C, D). Build a prompt that names A and B
  but supplies C's metadata as ``source`` and D's metadata as ``target``. The
  model produces a narrative that names A and B but describes C and D's
  musical attributes. Wrong by construction; failure_mode=subject_hallucination.

  ``field_corruption`` — Pick a real pair and corrupt one field on one side
  (swap ``audio.voice_instrumental``, inject outlier styles from a different
  genre, or swap ``genre`` to a wildly different one). The model produces a
  narrative faithful to the corrupted input — but the input is misleading, so
  the narrative claims contradict the real metadata the labeler sees. Wrong
  by construction; failure_mode=data_noise. Mirrors the Alex G case from
  whitepaper §6.3.

Both modes call Claude Haiku directly (not via the endpoint, so the production
``narrative-cache.db`` stays clean).

Pretraining-bait construction is not implemented here (see #278).

Usage:
    ANTHROPIC_API_KEY=sk-... python -m scripts.eval.build_wrong_set \\
        --db-path data/wxyc_artist_graph.db \\
        --narratives output/eval/eval_narratives.jsonl \\
        --out output/eval/eval_wrong.jsonl \\
        [--mode data_shuffle|field_corruption] \\
        [--n 30] [--seed 7] [--append]

Output JSONL row mirrors the production-narrative shape so ``export_labeling``
treats both pools identically. The labeler always sees the *real* metadata for
the artists named in the narrative — that's how they detect the wrongness.

Per-mode extra fields:
  data_shuffle:
    construction_method: "data_shuffle"
    metadata_source: {source_id, target_id, source_name, target_name}
    expected_label: {severity: "severe", failure_mode: "subject_hallucination"}
  field_corruption:
    construction_method: "field_corruption"
    corruption: {side: "source"|"target", strategy: "...", original: ..., corrupted: ...}
    expected_label: {severity: "severe", failure_mode: "data_noise"}
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


# ---------------------------------------------------------------------------
# Refusal filter
# ---------------------------------------------------------------------------

_REFUSAL_PHRASES = (
    "unable to",
    "cannot write",
    "i cannot",
    "input data lacks",
    "insufficient data",
    "not enough information",
    "no clear connection",
    "i'm sorry",
)
_REFUSAL_MIN_WORDS = 30


def _is_refusal(narrative: str | None) -> bool:
    """Heuristic: too short, empty, or contains a known refusal phrase.

    Filters narratives where the model declined to write substantive content
    (e.g. when prompted with conflicting fields it can't reconcile). Mirrors
    the manual filter applied during the data_shuffle corpus review.
    """
    if not narrative:
        return True
    text = narrative.strip().lower()
    if len(text.split()) < _REFUSAL_MIN_WORDS:
        return True
    return any(phrase in text for phrase in _REFUSAL_PHRASES)


# ---------------------------------------------------------------------------
# Field-corruption helpers (pure; covered by tests/unit/test_eval_field_corruption.py)
# ---------------------------------------------------------------------------

_VI_FLIP = {"instrumental": "vocal-forward", "vocal-forward": "instrumental"}


def _swap_voice_instrumental(meta: dict) -> dict | None:
    """Return a copy of ``meta`` with ``audio.voice_instrumental`` flipped.

    Returns ``None`` if the field is missing or carries an unexpected value
    (the production qualitative descriptor only emits the two named extremes).
    """
    audio = meta.get("audio")
    if not isinstance(audio, dict):
        return None
    flipped = _VI_FLIP.get(audio.get("voice_instrumental"))
    if flipped is None:
        return None
    new = dict(meta)
    new["audio"] = dict(audio)
    new["audio"]["voice_instrumental"] = flipped
    return new


def _inject_outlier_styles(meta: dict, outlier_styles: list[str]) -> dict | None:
    """Replace ``meta['styles']`` with ``outlier_styles`` (a copy).

    Returns ``None`` if the input has no styles, or if outliers are empty.
    Replacement (not append) mirrors the Alex G case from whitepaper §6.3
    where the entire top-styles list was unrepresentative.
    """
    if not meta.get("styles") or not outlier_styles:
        return None
    new = dict(meta)
    new["styles"] = list(outlier_styles)
    return new


def _swap_genre(meta: dict, new_genre: str) -> dict | None:
    """Return a copy of ``meta`` with ``genre`` replaced.

    Returns ``None`` if the meta has no genre or the new value matches the
    existing one.
    """
    if "genre" not in meta:
        return None
    if meta["genre"] == new_genre:
        return None
    new = dict(meta)
    new["genre"] = new_genre
    return new


def _pick_outlier_styles_for(
    pool: dict[str, list[list[str]]],
    target_genre: str | None,
    rng: random.Random,
) -> list[str]:
    """Pick a random style-list from artists whose genre is NOT ``target_genre``.

    ``pool`` maps genre -> list of style-lists (one per artist). Returns the
    empty list when no candidates exist.
    """
    candidates: list[list[str]] = []
    for genre, style_lists in pool.items():
        if genre == target_genre:
            continue
        candidates.extend(style_lists)
    if not candidates:
        return []
    return list(rng.choice(candidates))


def _pick_corruption(
    source_meta: dict,
    target_meta: dict,
    outlier_pool: dict[str, list[list[str]]],
    rng: random.Random,
    *,
    genre_pool: list[str],
) -> tuple[dict, dict, str, str] | None:
    """Choose one (side, strategy) and apply it.

    Returns ``(new_source, new_target, side, strategy)`` or ``None`` if no
    feasible (side, strategy) exists. Strategies considered:

      - ``voice_instrumental``: requires ``audio.voice_instrumental`` on the
        chosen side.
      - ``outlier_styles``: requires the chosen side to have non-empty
        ``styles`` AND for ``outlier_pool`` to have a non-empty entry under a
        genre different from the side's.
      - ``genre_swap``: requires the chosen side to have ``genre`` AND for
        ``genre_pool`` to contain a different genre.

    Picks uniformly across feasible (side, strategy) combos so each row's
    corruption is interpretable in isolation.
    """
    sides = (("source", source_meta), ("target", target_meta))
    feasible: list[tuple[str, str]] = []
    for side, meta in sides:
        audio = meta.get("audio")
        if isinstance(audio, dict) and audio.get("voice_instrumental") in _VI_FLIP:
            feasible.append((side, "voice_instrumental"))
        if meta.get("styles"):
            if any(outlier_pool[g] for g in outlier_pool if g != meta.get("genre")):
                feasible.append((side, "outlier_styles"))
        if meta.get("genre"):
            if any(g != meta["genre"] for g in genre_pool):
                feasible.append((side, "genre_swap"))

    if not feasible:
        return None

    side, strategy = rng.choice(feasible)
    target_for_side = source_meta if side == "source" else target_meta

    if strategy == "voice_instrumental":
        corrupted = _swap_voice_instrumental(target_for_side)
    elif strategy == "outlier_styles":
        outliers = _pick_outlier_styles_for(outlier_pool, target_for_side.get("genre"), rng)
        corrupted = _inject_outlier_styles(target_for_side, outliers)
    elif strategy == "genre_swap":
        other_genres = [g for g in genre_pool if g != target_for_side["genre"]]
        corrupted = _swap_genre(target_for_side, rng.choice(other_genres))
    else:
        raise AssertionError(f"unknown strategy: {strategy}")

    if corrupted is None:
        return None

    if side == "source":
        return corrupted, target_meta, side, strategy
    return source_meta, corrupted, side, strategy


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _open_ro_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _load_eligible_pairs(narratives_path: Path) -> list[dict]:
    """Return only generative narrative rows (skip canned insufficient_signal placeholders).

    Insufficient-signal rows can't meaningfully be corrupted — the canned text
    doesn't make claims about the music, just acknowledges low signal.
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


def _build_user_message(
    source_name: str, target_name: str, source_meta: dict, target_meta: dict
) -> str:
    """Construct the user-message JSON the model sees.

    Identical minimal shape to the data_shuffle path (no relationships /
    facets / shared_neighbors) so the model has nothing to ground in except
    per-artist metadata — exactly what we want to test against.
    """
    source_view = dict(source_meta)
    source_view["name"] = source_name
    target_view = dict(target_meta)
    target_view["name"] = target_name
    payload = {"source": source_view, "target": target_view}
    return json.dumps(payload, separators=(",", ":"))


def _build_outlier_pool(db: sqlite3.Connection) -> dict[str, list[list[str]]]:
    """Map genre -> list of per-artist style-lists, capped at 5 styles per artist.

    Used by the outlier-style corruption strategy: pick an artist with a
    different genre than the corruption target's, drop their styles in.
    """
    rows = db.execute(
        """
        SELECT a.id, a.genre,
               (SELECT GROUP_CONCAT(style_tag, '||')
                FROM (SELECT style_tag FROM artist_style
                      WHERE artist_id = a.id
                      ORDER BY style_tag LIMIT 5)) AS styles
        FROM artist a
        WHERE a.genre IS NOT NULL
          AND EXISTS (SELECT 1 FROM artist_style s WHERE s.artist_id = a.id)
        """
    ).fetchall()
    out: dict[str, list[list[str]]] = {}
    for r in rows:
        if not r["styles"]:
            continue
        styles = [s for s in r["styles"].split("||") if s]
        if styles:
            out.setdefault(r["genre"], []).append(styles)
    return out


def _build_genre_pool(db: sqlite3.Connection) -> list[str]:
    """Distinct WXYC genre values present in the artist table."""
    rows = db.execute(
        "SELECT DISTINCT genre FROM artist WHERE genre IS NOT NULL AND genre != ''"
    ).fetchall()
    return [r["genre"] for r in rows]


# ---------------------------------------------------------------------------
# Mode: data_shuffle (original)
# ---------------------------------------------------------------------------


def run_data_shuffle(args: argparse.Namespace) -> int:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY is required")
        return 1

    import anthropic

    from semantic_index.api.narrative import _SYSTEM_PROMPT, _lookup_artist_metadata

    db = _open_ro_db(args.db_path)
    eligible = _load_eligible_pairs(Path(args.narratives))
    if len(eligible) < 2:
        logger.error("Need at least 2 generative narratives in --narratives; got %d", len(eligible))
        return 1
    logger.info("Eligible production pairs: %d", len(eligible))

    rng = random.Random(args.seed)
    pairs_for_names = rng.sample(eligible, min(args.n, len(eligible)))

    client = anthropic.Anthropic(api_key=api_key)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    open_mode = "a" if args.append else "w"

    written = 0
    with out_path.open(open_mode) as fh:
        for i, name_row in enumerate(pairs_for_names, 1):
            tries = 0
            meta_row = None
            while tries < 50:
                cand = rng.choice(eligible)
                if cand["source_id"] not in (name_row["source_id"], name_row["target_id"]) and cand[
                    "target_id"
                ] not in (name_row["source_id"], name_row["target_id"]):
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
                name_row["source_name"],
                name_row["target_name"],
                source_meta,
                target_meta,
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
            except Exception:  # noqa: BLE001
                logger.exception(
                    "Anthropic call failed for %s / %s",
                    name_row["source_name"],
                    name_row["target_name"],
                )
                continue
            elapsed_ms = int((time.time() - t0) * 1000)

            row = {
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
                "token_match_score": 0.0,
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
                "expected_label": {"severity": "severe", "failure_mode": "subject_hallucination"},
            }
            fh.write(json.dumps(row, separators=(",", ":")) + "\n")
            fh.flush()
            written += 1
            logger.info(
                "%d/%d: named=%s/%s metadata=%s/%s elapsed=%dms",
                i,
                len(pairs_for_names),
                name_row["source_name"],
                name_row["target_name"],
                meta_row["source_name"],
                meta_row["target_name"],
                elapsed_ms,
            )

            if args.sleep > 0 and i < len(pairs_for_names):
                time.sleep(args.sleep)

    logger.info("Wrote %d data-shuffle rows -> %s", written, out_path)
    return 0


# ---------------------------------------------------------------------------
# Mode: field_corruption (new)
# ---------------------------------------------------------------------------


def run_field_corruption(args: argparse.Namespace) -> int:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY is required")
        return 1

    import anthropic

    from semantic_index.api.narrative import _SYSTEM_PROMPT, _lookup_artist_metadata

    db = _open_ro_db(args.db_path)
    eligible = _load_eligible_pairs(Path(args.narratives))
    if not eligible:
        logger.error("No eligible narratives in %s", args.narratives)
        return 1
    logger.info("Eligible production pairs: %d", len(eligible))

    outlier_pool = _build_outlier_pool(db)
    genre_pool = _build_genre_pool(db)
    logger.info(
        "Outlier-style pool: %d genres, %d total style-lists",
        len(outlier_pool),
        sum(len(v) for v in outlier_pool.values()),
    )
    logger.info("Genre-swap pool: %d genres", len(genre_pool))

    rng = random.Random(args.seed)
    # Oversample to absorb refusals + infeasible pairs.
    candidate_pairs = rng.sample(eligible, min(args.n * 2, len(eligible)))

    client = anthropic.Anthropic(api_key=api_key)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    open_mode = "a" if args.append else "w"

    written = 0
    refused = 0
    infeasible = 0
    with out_path.open(open_mode) as fh:
        for i, row_in in enumerate(candidate_pairs, 1):
            if written >= args.n:
                break

            source_meta = _lookup_artist_metadata(
                db,
                row_in["source_id"],
                row_in["source_name"],
                row_in["source_genre"],
                row_in["source_plays"],
            )
            target_meta = _lookup_artist_metadata(
                db,
                row_in["target_id"],
                row_in["target_name"],
                row_in["target_genre"],
                row_in["target_plays"],
            )

            picked = _pick_corruption(
                source_meta, target_meta, outlier_pool, rng, genre_pool=genre_pool
            )
            if picked is None:
                infeasible += 1
                continue
            new_source, new_target, side, strategy = picked

            # Capture before/after for the corrupted side so a labeler /
            # auditor can read the row without re-running the corruption.
            side_meta_before = source_meta if side == "source" else target_meta
            side_meta_after = new_source if side == "source" else new_target
            corruption_field = {
                "voice_instrumental": "audio.voice_instrumental",
                "outlier_styles": "styles",
                "genre_swap": "genre",
            }[strategy]
            if strategy == "voice_instrumental":
                original_value = side_meta_before["audio"]["voice_instrumental"]
                corrupted_value = side_meta_after["audio"]["voice_instrumental"]
            elif strategy == "outlier_styles":
                original_value = side_meta_before["styles"]
                corrupted_value = side_meta_after["styles"]
            else:
                original_value = side_meta_before["genre"]
                corrupted_value = side_meta_after["genre"]

            user_message = _build_user_message(
                row_in["source_name"],
                row_in["target_name"],
                new_source,
                new_target,
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
            except Exception:  # noqa: BLE001
                logger.exception(
                    "Anthropic call failed for %s / %s",
                    row_in["source_name"],
                    row_in["target_name"],
                )
                continue
            elapsed_ms = int((time.time() - t0) * 1000)

            if _is_refusal(narrative):
                refused += 1
                logger.info(
                    "%d: REFUSAL (strategy=%s side=%s) %s/%s — dropping",
                    i,
                    strategy,
                    side,
                    row_in["source_name"],
                    row_in["target_name"],
                )
                continue

            row_out = {
                "cell_id": "WRONG-FIELD-CORRUPTION",
                "fame": row_in["fame"],
                "richness": row_in["richness"],
                "genre": row_in["genre"],
                "edge": row_in["edge"],
                "source_id": row_in["source_id"],
                "target_id": row_in["target_id"],
                "source_name": row_in["source_name"],
                "target_name": row_in["target_name"],
                "source_genre": row_in["source_genre"],
                "target_genre": row_in["target_genre"],
                "source_plays": row_in["source_plays"],
                "target_plays": row_in["target_plays"],
                "narrative": narrative,
                "cached": False,
                "insufficient_signal": False,
                "token_match_score": 0.0,
                "low_grounding": False,
                "http_status": 200,
                "latency_ms": elapsed_ms,
                "construction_method": "field_corruption",
                "corruption": {
                    "side": side,
                    "strategy": strategy,
                    "field": corruption_field,
                    "original": original_value,
                    "corrupted": corrupted_value,
                },
                "expected_label": {"severity": "severe", "failure_mode": "data_noise"},
            }
            fh.write(json.dumps(row_out, separators=(",", ":")) + "\n")
            fh.flush()
            written += 1
            logger.info(
                "%d/%d: %s/%s side=%s strategy=%s elapsed=%dms",
                written,
                args.n,
                row_in["source_name"],
                row_in["target_name"],
                side,
                strategy,
                elapsed_ms,
            )

            if args.sleep > 0 and written < args.n:
                time.sleep(args.sleep)

    logger.info(
        "Wrote %d field-corruption rows -> %s (refused=%d, infeasible=%d)",
        written,
        out_path,
        refused,
        infeasible,
    )
    if written < args.n:
        logger.warning("Target was %d but only %d rows passed the refusal filter", args.n, written)
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--db-path", default="data/wxyc_artist_graph.db")
    ap.add_argument("--narratives", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--mode", choices=("data_shuffle", "field_corruption"), default="data_shuffle")
    ap.add_argument("--n", type=int, default=30, help="Number of wrong narratives to generate")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--sleep", type=float, default=0.0)
    ap.add_argument(
        "--append",
        action="store_true",
        help="Append to --out rather than truncate. Use when adding field_corruption "
        "rows to an existing eval_wrong.jsonl that already holds data_shuffle rows.",
    )
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.mode == "data_shuffle":
        return run_data_shuffle(args)
    return run_field_corruption(args)


if __name__ == "__main__":
    sys.exit(main())
