"""Test that ``backscore metrics`` reports per-construction_method recall.

This is the load-bearing visibility for #277 — the headline finding is that
grounding-fidelity scorers have *lower* recall on field_corruption rows than
on data_shuffle rows, and that comparison can only happen if the breakdown
exists in the output.
"""

from __future__ import annotations

import json
from pathlib import Path

from scripts.eval.backscore import main as backscore_main


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(r) + "\n" for r in rows))


def test_metrics_reports_per_construction_method_recall(tmp_path, capsys):
    scored_path = tmp_path / "scored.jsonl"
    labeled_path = tmp_path / "labeled.jsonl"

    # Mixed pool: 2 data_shuffle (one above threshold, one below),
    # 2 field_corruption (both below threshold — the canonical data_noise
    # gap), 1 production (above threshold).
    _write_jsonl(
        scored_path,
        [
            {
                "row_id": "R0001",
                "construction_method": "data_shuffle",
                "scores": {"token_match_v1": 0.8, "claim_ratio_v1": 0.6},
            },
            {
                "row_id": "R0002",
                "construction_method": "data_shuffle",
                "scores": {"token_match_v1": 0.1, "claim_ratio_v1": 0.1},
            },
            {
                "row_id": "R0003",
                "construction_method": "field_corruption",
                "scores": {"token_match_v1": 0.1, "claim_ratio_v1": 0.1},
            },
            {
                "row_id": "R0004",
                "construction_method": "field_corruption",
                "scores": {"token_match_v1": 0.2, "claim_ratio_v1": 0.2},
            },
            {
                "row_id": "R0005",
                "construction_method": "production",
                "scores": {"token_match_v1": 0.9, "claim_ratio_v1": 0.7},
            },
        ],
    )
    _write_jsonl(
        labeled_path,
        [
            {
                "row_id": "R0001",
                "label": {"severity": "severe", "failure_mode": "subject_hallucination"},
            },
            {
                "row_id": "R0002",
                "label": {"severity": "severe", "failure_mode": "subject_hallucination"},
            },
            {"row_id": "R0003", "label": {"severity": "severe", "failure_mode": "data_noise"}},
            {"row_id": "R0004", "label": {"severity": "severe", "failure_mode": "data_noise"}},
            {
                "row_id": "R0005",
                "label": {"severity": "severe", "failure_mode": "subject_hallucination"},
            },
        ],
    )

    rc = backscore_main(
        [
            "metrics",
            "--scored",
            str(scored_path),
            "--labeled",
            str(labeled_path),
            "--threshold",
            "0.5",
        ]
    )
    assert rc == 0
    captured = capsys.readouterr().out

    # The new breakdown is visible.
    assert "recall by construction_method:" in captured
    assert "data_shuffle" in captured
    assert "field_corruption" in captured
    assert "production" in captured

    # Spot-check the numbers: for token_match_v1 at threshold 0.5:
    #   data_shuffle:    1/2 = 0.500
    #   field_corruption: 0/2 = 0.000  (the load-bearing finding)
    #   production:      1/1 = 1.000
    assert "data_shuffle" in captured
    # Look for the specific recall pattern.
    assert "field_corruption" in captured
    # The data_noise gap manifests as a 0.000 recall on field_corruption.
    lines = captured.splitlines()
    fc_lines = [line for line in lines if "field_corruption" in line]
    assert any("0.000" in line for line in fc_lines), (
        f"field_corruption recall should be 0.000; saw: {fc_lines}"
    )


def test_metrics_works_without_field_corruption_rows(tmp_path, capsys):
    """Existing behavior — production-only labeled set — should still work."""
    scored_path = tmp_path / "scored.jsonl"
    labeled_path = tmp_path / "labeled.jsonl"
    _write_jsonl(
        scored_path,
        [
            {
                "row_id": "R0001",
                "construction_method": "production",
                "scores": {"token_match_v1": 0.8, "claim_ratio_v1": 0.4},
            },
        ],
    )
    _write_jsonl(
        labeled_path,
        [
            {
                "row_id": "R0001",
                "label": {"severity": "severe", "failure_mode": "subject_hallucination"},
            },
        ],
    )

    rc = backscore_main(
        [
            "metrics",
            "--scored",
            str(scored_path),
            "--labeled",
            str(labeled_path),
            "--threshold",
            "0.5",
        ]
    )
    assert rc == 0
    captured = capsys.readouterr().out
    # Still emits the construction_method section (with just 'production').
    assert "recall by construction_method:" in captured
    assert "production" in captured
