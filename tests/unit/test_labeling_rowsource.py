"""Tests for the labeling app's JSONL row source + redaction."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from semantic_index.labeling_app.row_source import (
    REDACTED_FIELDS,
    RowNotFoundError,
    RowSource,
)


@pytest.fixture()
def jsonl_path(tmp_path: Path) -> Path:
    rows = [
        {
            "row_id": "R0001",
            "cell_id": "HIGH-RICH-SAME-DIRECT",
            "source_name": "Pavement",
            "target_name": "Stereolab",
            "narrative": "They share styles.",
            "source_data": {"name": "Pavement", "total_plays": 907},
            "target_data": {"name": "Stereolab", "total_plays": 1629},
            "shared_neighbors": [{"name": "Roxy Music", "aa_score": 0.26}],
            "raw_count": 2,
            "insufficient_signal": False,
            "token_match_score": 0.33,
            "construction_method": "data_shuffle",
            "expected_label": {
                "severity": "severe",
                "failure_mode": "subject_hallucination",
            },
            "metadata_source": {"source_id": 99, "target_id": 100},
        },
        {
            "row_id": "R0002",
            "cell_id": "LOW-THIN-CROSS-INDIRECT",
            "source_name": "Tom Tom Club",
            "target_name": "The War On Drugs",
            "narrative": "Insufficient signal.",
            "source_data": {"name": "Tom Tom Club"},
            "target_data": {"name": "The War On Drugs"},
            "shared_neighbors": [],
            "insufficient_signal": True,
            "token_match_score": 0.0,
        },
    ]
    p = tmp_path / "labeling.jsonl"
    with p.open("w") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")
    return p


class TestRowOrder:
    def test_row_ids_returned_in_file_order(self, jsonl_path: Path) -> None:
        src = RowSource(jsonl_path)
        assert src.row_ids() == ["R0001", "R0002"]

    def test_summaries_carry_pair_and_cell(self, jsonl_path: Path) -> None:
        src = RowSource(jsonl_path)
        summaries = src.summaries()
        assert summaries[0]["row_id"] == "R0001"
        assert summaries[0]["pair"] == "Pavement ↔ Stereolab"
        assert summaries[0]["cell_id"] == "HIGH-RICH-SAME-DIRECT"


class TestRedaction:
    def test_get_strips_construction_method(self, jsonl_path: Path) -> None:
        src = RowSource(jsonl_path)
        row = src.get("R0001")
        for field in REDACTED_FIELDS:
            assert field not in row, f"{field} leaked to labeler view"

    def test_get_keeps_labeler_visible_fields(self, jsonl_path: Path) -> None:
        src = RowSource(jsonl_path)
        row = src.get("R0001")
        assert row["narrative"] == "They share styles."
        assert row["source_data"]["name"] == "Pavement"
        assert row["target_data"]["name"] == "Stereolab"
        assert row["shared_neighbors"][0]["name"] == "Roxy Music"
        assert row["pair"] == "Pavement ↔ Stereolab"

    def test_get_unknown_row_id_raises(self, jsonl_path: Path) -> None:
        src = RowSource(jsonl_path)
        with pytest.raises(RowNotFoundError):
            src.get("R9999")
