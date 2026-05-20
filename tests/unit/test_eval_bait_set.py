"""Tests for the pretraining-bait constructors in ``scripts/eval/build_bait_set.py``.

These cover the pure helpers — regime classification at the anonymization
boundary, pair-file validation, and the JSONL row shape — so that #278's
load-bearing contracts (correct above/below split, ``construction_method``
preserved, ``expected_label`` attached) don't silently regress.

End-to-end TestClient invocation against the production endpoint is exercised
in the integration run, not here.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.eval.build_bait_set import (
    ANON_PLAY_THRESHOLD,
    CONSTRUCTION_METHOD,
    EXPECTED_LABEL,
    build_row,
    classify_regime,
    load_bait_pairs,
)

# ---------------------------------------------------------------------------
# classify_regime — strict-> boundary
# ---------------------------------------------------------------------------


def test_classify_regime_both_above_threshold_is_above():
    assert classify_regime(1000, 900, ANON_PLAY_THRESHOLD) == "above"


def test_classify_regime_both_below_threshold_is_below():
    assert classify_regime(500, 700, ANON_PLAY_THRESHOLD) == "below"


def test_classify_regime_one_above_is_mixed():
    assert classify_regime(1000, 500, ANON_PLAY_THRESHOLD) == "mixed"
    assert classify_regime(500, 1000, ANON_PLAY_THRESHOLD) == "mixed"


def test_classify_regime_exactly_at_threshold_is_below():
    # _build_anonymization_map uses strict `>`, so a pair at exactly 800 is
    # NOT anonymized — the bait test treats this as the raw branch.
    assert classify_regime(800, 800, ANON_PLAY_THRESHOLD) == "below"
    assert classify_regime(800, 900, ANON_PLAY_THRESHOLD) == "mixed"


def test_classify_regime_respects_custom_threshold():
    assert classify_regime(100, 200, threshold=150) == "mixed"
    assert classify_regime(100, 200, threshold=50) == "above"
    assert classify_regime(100, 200, threshold=300) == "below"


# ---------------------------------------------------------------------------
# load_bait_pairs — validation
# ---------------------------------------------------------------------------


def _write(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload))
    return path


def test_load_bait_pairs_accepts_well_formed_file(tmp_path):
    p = _write(
        tmp_path / "pairs.json",
        {
            "pairs": [
                {
                    "source_id": 1,
                    "target_id": 2,
                    "source_name": "A",
                    "target_name": "B",
                    "regime": "above",
                    "bait_notes": "test",
                }
            ]
        },
    )
    out = load_bait_pairs(p)
    assert len(out) == 1
    assert out[0]["source_id"] == 1


def test_load_bait_pairs_rejects_empty_list(tmp_path):
    p = _write(tmp_path / "pairs.json", {"pairs": []})
    with pytest.raises(ValueError, match="no 'pairs' list"):
        load_bait_pairs(p)


def test_load_bait_pairs_rejects_missing_pairs_key(tmp_path):
    p = _write(tmp_path / "pairs.json", {"other": []})
    with pytest.raises(ValueError, match="no 'pairs' list"):
        load_bait_pairs(p)


def test_load_bait_pairs_rejects_missing_required_field(tmp_path):
    p = _write(
        tmp_path / "pairs.json",
        {
            "pairs": [
                {"source_id": 1, "target_id": 2, "regime": "above"}
                # missing bait_notes
            ]
        },
    )
    with pytest.raises(ValueError, match="missing required key: bait_notes"):
        load_bait_pairs(p)


def test_load_bait_pairs_rejects_bad_regime(tmp_path):
    p = _write(
        tmp_path / "pairs.json",
        {"pairs": [{"source_id": 1, "target_id": 2, "regime": "sideways", "bait_notes": ""}]},
    )
    with pytest.raises(ValueError, match="regime='sideways' not in"):
        load_bait_pairs(p)


def test_load_bait_pairs_rejects_duplicate_pair(tmp_path):
    """Duplicate ``(source_id, target_id)`` entries would break the
    ``--skip-cached`` invariant in the driver (cache key is the pair tuple
    and the output file is opened in append mode)."""
    p = _write(
        tmp_path / "pairs.json",
        {
            "pairs": [
                {"source_id": 1, "target_id": 2, "regime": "above", "bait_notes": "a"},
                {"source_id": 1, "target_id": 2, "regime": "below", "bait_notes": "b"},
            ]
        },
    )
    with pytest.raises(ValueError, match="duplicates"):
        load_bait_pairs(p)


def test_load_bait_pairs_shipped_file_is_well_formed():
    """The curated file that ships with the script must parse and satisfy the
    #278 acceptance criterion of at least 4 pairs in each clean regime."""
    here = Path(__file__).resolve().parents[2]
    pairs = load_bait_pairs(here / "scripts" / "eval" / "bait_pairs.json")
    assert len(pairs) >= 10, f"shipped bait file has {len(pairs)} pairs, expected >= 10"
    counts = {r: sum(1 for p in pairs if p["regime"] == r) for r in ("above", "below", "mixed")}
    assert counts["above"] >= 4, f"need >=4 above-threshold pairs; got {counts}"
    assert counts["below"] >= 4, f"need >=4 below-threshold pairs; got {counts}"


# ---------------------------------------------------------------------------
# build_row — output shape contract
# ---------------------------------------------------------------------------


def _meta(id_: int, name: str, plays: int, genre: str = "Rock") -> dict:
    return {"id": id_, "name": name, "genre": genre, "total_plays": plays}


def test_build_row_attaches_construction_method_and_expected_label():
    pair = {"regime": "above", "bait_notes": "test bait"}
    body = {
        "narrative": "A and B share roots.",
        "cached": False,
        "insufficient_signal": False,
        "token_match_score": 0.42,
        "low_grounding": False,
    }
    row = build_row(
        pair,
        _meta(1, "Artist One", 1000),
        _meta(2, "Artist Two", 900),
        200,
        body,
        None,
        1234,
    )
    assert row["construction_method"] == CONSTRUCTION_METHOD
    assert row["expected_label"] == EXPECTED_LABEL
    assert row["bait_notes"] == "test bait"
    assert row["regime"] == "above"
    assert row["cell_id"] == "BAIT-ABOVE"
    assert row["narrative"] == "A and B share roots."
    assert row["http_status"] == 200
    assert row["latency_ms"] == 1234


def test_build_row_below_regime_cell_id():
    pair = {"regime": "below", "bait_notes": ""}
    body = {
        "narrative": "x",
        "cached": False,
        "insufficient_signal": False,
        "token_match_score": 0.0,
        "low_grounding": False,
    }
    row = build_row(pair, _meta(1, "A", 500), _meta(2, "B", 700), 200, body, None, 100)
    assert row["regime"] == "below"
    assert row["cell_id"] == "BAIT-BELOW"


def test_build_row_recomputes_regime_from_actual_plays():
    """If the curator file disagrees with the live DB, build_row trusts the
    DB. (The driver also logs the disagreement; the row itself reflects what
    actually fired.)"""
    pair = {"regime": "above", "bait_notes": "curator was wrong"}
    body = {
        "narrative": "x",
        "cached": False,
        "insufficient_signal": False,
        "token_match_score": 0.0,
        "low_grounding": False,
    }
    # Both artists below threshold despite curator labeling pair as "above".
    row = build_row(pair, _meta(1, "A", 500), _meta(2, "B", 600), 200, body, None, 0)
    assert row["regime"] == "below"
    assert row["cell_id"] == "BAIT-BELOW"


def test_build_row_carries_error_when_endpoint_fails():
    pair = {"regime": "above", "bait_notes": ""}
    row = build_row(pair, _meta(1, "A", 1000), _meta(2, "B", 1000), 500, None, "boom", 50)
    assert row["narrative"] is None
    assert row["error"] == "boom"
    assert row["http_status"] == 500


def test_build_row_truncates_long_error_payloads():
    pair = {"regime": "above", "bait_notes": ""}
    row = build_row(pair, _meta(1, "A", 1000), _meta(2, "B", 1000), 500, None, "x" * 1000, 50)
    assert len(row["error"]) == 500
