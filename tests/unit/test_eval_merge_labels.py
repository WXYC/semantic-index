"""Unit tests for the label merge utility."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from scripts.eval.merge_labels import (
    VALID_FAILURE_MODE,
    VALID_SEVERITY,
    _normalize_label,
    main,
)


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")


def _write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        path.write_text("")
        return
    cols = list(rows[0].keys())
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=cols)
        writer.writeheader()
        writer.writerows(rows)


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


class TestNormalizeLabel:
    def test_lowercases_and_trims(self):
        assert _normalize_label("  Severe ") == "severe"
        assert _normalize_label("Subject Hallucination") == "subject_hallucination"
        assert _normalize_label("dj-intent") == "dj_intent"

    def test_blank_and_none(self):
        assert _normalize_label(None) == ""
        assert _normalize_label("") == ""
        assert _normalize_label("   ") == ""


class TestMergeHappyPath:
    def test_merges_well_formed_csv(self, tmp_path):
        backing = tmp_path / "labeling.jsonl"
        _write_jsonl(
            backing,
            [
                {"row_id": "R0001", "narrative": "n1"},
                {"row_id": "R0002", "narrative": "n2"},
            ],
        )
        labels = tmp_path / "labels.csv"
        _write_csv(
            labels,
            [
                {
                    "row_id": "R0001",
                    "severity": "severe",
                    "failure_mode": "subject_hallucination",
                    "notes": "wrong artist",
                },
                {"row_id": "R0002", "severity": "not_wrong", "failure_mode": "", "notes": ""},
            ],
        )
        out = tmp_path / "merged.jsonl"
        rc = main(
            [
                "--labeling-jsonl",
                str(backing),
                "--labels-csv",
                str(labels),
                "--labeler",
                "jake",
                "--out",
                str(out),
            ]
        )
        assert rc == 0
        rows = _read_jsonl(out)
        assert len(rows) == 2
        assert rows[0]["label"] == {
            "labeler": "jake",
            "severity": "severe",
            "failure_mode": "subject_hallucination",
            "notes": "wrong artist",
        }
        assert rows[1]["label"]["severity"] == "not_wrong"
        assert rows[1]["label"]["failure_mode"] == ""


class TestValidation:
    def test_invalid_severity_errors_but_skips_row(self, tmp_path):
        backing = tmp_path / "labeling.jsonl"
        _write_jsonl(backing, [{"row_id": "R0001", "narrative": "n1"}])
        labels = tmp_path / "labels.csv"
        _write_csv(
            labels,
            [
                {"row_id": "R0001", "severity": "BOGUS", "failure_mode": "", "notes": ""},
            ],
        )
        out = tmp_path / "merged.jsonl"
        rc = main(
            [
                "--labeling-jsonl",
                str(backing),
                "--labels-csv",
                str(labels),
                "--labeler",
                "jake",
                "--out",
                str(out),
            ]
        )
        assert rc == 1
        # Output exists but is empty (the bad row was rejected)
        assert _read_jsonl(out) == []

    def test_severe_without_failure_mode_errors(self, tmp_path):
        backing = tmp_path / "labeling.jsonl"
        _write_jsonl(backing, [{"row_id": "R0001", "narrative": "n1"}])
        labels = tmp_path / "labels.csv"
        _write_csv(
            labels,
            [
                {"row_id": "R0001", "severity": "severe", "failure_mode": "", "notes": ""},
            ],
        )
        out = tmp_path / "merged.jsonl"
        rc = main(
            [
                "--labeling-jsonl",
                str(backing),
                "--labels-csv",
                str(labels),
                "--labeler",
                "jake",
                "--out",
                str(out),
            ]
        )
        assert rc == 1

    def test_unknown_row_id_errors(self, tmp_path):
        backing = tmp_path / "labeling.jsonl"
        _write_jsonl(backing, [{"row_id": "R0001", "narrative": "n1"}])
        labels = tmp_path / "labels.csv"
        _write_csv(
            labels,
            [
                {
                    "row_id": "R9999",
                    "severity": "severe",
                    "failure_mode": "subject_hallucination",
                    "notes": "",
                },
            ],
        )
        out = tmp_path / "merged.jsonl"
        rc = main(
            [
                "--labeling-jsonl",
                str(backing),
                "--labels-csv",
                str(labels),
                "--labeler",
                "jake",
                "--out",
                str(out),
            ]
        )
        assert rc == 1

    def test_blank_severity_marks_unlabeled_not_error(self, tmp_path):
        backing = tmp_path / "labeling.jsonl"
        _write_jsonl(
            backing,
            [
                {"row_id": "R0001", "narrative": "n1"},
                {"row_id": "R0002", "narrative": "n2"},
            ],
        )
        labels = tmp_path / "labels.csv"
        _write_csv(
            labels,
            [
                {
                    "row_id": "R0001",
                    "severity": "severe",
                    "failure_mode": "subject_hallucination",
                    "notes": "",
                },
                {"row_id": "R0002", "severity": "", "failure_mode": "", "notes": ""},
            ],
        )
        out = tmp_path / "merged.jsonl"
        rc = main(
            [
                "--labeling-jsonl",
                str(backing),
                "--labels-csv",
                str(labels),
                "--labeler",
                "jake",
                "--out",
                str(out),
            ]
        )
        assert rc == 0
        # Only labeled row written
        rows = _read_jsonl(out)
        assert len(rows) == 1
        assert rows[0]["row_id"] == "R0001"


class TestRubricCoverage:
    """Sanity: the label vocab in the script matches the rubric in docs/."""

    def test_severity_vocab(self):
        assert VALID_SEVERITY == {"severe", "minor", "not_wrong"}

    def test_failure_mode_vocab(self):
        assert VALID_FAILURE_MODE == {
            "subject_hallucination",
            "neighbor_characterization",
            "dj_intent",
            "data_noise",
            "other",
        }
