"""End-to-end TestClient coverage for the labeling FastAPI app.

Includes a round-trip test: label rows through the API, export the CSV, then
run that CSV through ``scripts/eval/merge_labels.py`` and confirm it produces
a clean labeled JSONL with no errors. This is the load-bearing contract — if
it ever breaks, the UI has stopped producing merge-able output.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from semantic_index.labeling_app.app import create_app


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")


@pytest.fixture()
def jsonl_path(tmp_path: Path) -> Path:
    rows = [
        {
            "row_id": "R0001",
            "cell_id": "HIGH-RICH-SAME-DIRECT",
            "source_name": "Pavement",
            "target_name": "Stereolab",
            "narrative": "They share styles.",
            "source_data": {"name": "Pavement"},
            "target_data": {"name": "Stereolab"},
            "shared_neighbors": [],
            "insufficient_signal": False,
            "token_match_score": 0.33,
            "construction_method": "data_shuffle",
            "expected_label": {
                "severity": "severe",
                "failure_mode": "subject_hallucination",
            },
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
    _write_jsonl(p, rows)
    return p


@pytest.fixture()
def client(jsonl_path: Path, tmp_path: Path) -> TestClient:
    app = create_app(str(jsonl_path), str(tmp_path / "labels.db"))
    return TestClient(app)


class TestRowsList:
    def test_rows_endpoint_returns_summaries(self, client: TestClient) -> None:
        resp = client.get("/api/rows", params={"labeler": "jake"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert data["labeled"] == 0
        assert {r["row_id"] for r in data["rows"]} == {"R0001", "R0002"}

    def test_progress_reflects_saved_labels(self, client: TestClient) -> None:
        client.post(
            "/api/rows/R0001/label",
            json={
                "labeler": "jake",
                "severity": "severe",
                "failure_mode": "subject_hallucination",
                "notes": "",
            },
        )
        resp = client.get("/api/rows", params={"labeler": "jake"})
        data = resp.json()
        assert data["labeled"] == 1
        statuses = {r["row_id"]: r["my_label"] for r in data["rows"]}
        assert statuses["R0001"] is not None
        assert statuses["R0001"]["severity"] == "severe"
        assert statuses["R0002"] is None

    def test_per_labeler_isolation(self, client: TestClient) -> None:
        client.post(
            "/api/rows/R0001/label",
            json={
                "labeler": "jake",
                "severity": "severe",
                "failure_mode": "subject_hallucination",
                "notes": "",
            },
        )
        resp = client.get("/api/rows", params={"labeler": "alex"})
        data = resp.json()
        assert data["labeled"] == 0


class TestRowDetail:
    def test_row_detail_omits_answer_key(self, client: TestClient) -> None:
        resp = client.get("/api/rows/R0001", params={"labeler": "jake"})
        assert resp.status_code == 200
        body = resp.json()
        assert "construction_method" not in body["row"]
        assert "expected_label" not in body["row"]
        assert body["row"]["narrative"] == "They share styles."

    def test_row_detail_returns_my_label_when_set(self, client: TestClient) -> None:
        client.post(
            "/api/rows/R0001/label",
            json={
                "labeler": "jake",
                "severity": "minor",
                "failure_mode": "data_noise",
                "notes": "hmm",
            },
        )
        resp = client.get("/api/rows/R0001", params={"labeler": "jake"})
        body = resp.json()
        assert body["my_label"]["severity"] == "minor"
        assert body["my_label"]["failure_mode"] == "data_noise"
        assert body["my_label"]["notes"] == "hmm"

    def test_unknown_row_returns_404(self, client: TestClient) -> None:
        resp = client.get("/api/rows/R9999", params={"labeler": "jake"})
        assert resp.status_code == 404


class TestSaveLabel:
    def test_save_then_get(self, client: TestClient) -> None:
        resp = client.post(
            "/api/rows/R0001/label",
            json={
                "labeler": "jake",
                "severity": "severe",
                "failure_mode": "subject_hallucination",
                "notes": "shuffled",
            },
        )
        assert resp.status_code == 200

    def test_invalid_severity_rejected(self, client: TestClient) -> None:
        resp = client.post(
            "/api/rows/R0001/label",
            json={
                "labeler": "jake",
                "severity": "kinda",
                "failure_mode": "data_noise",
                "notes": "",
            },
        )
        assert resp.status_code == 400

    def test_severe_without_failure_mode_rejected(self, client: TestClient) -> None:
        resp = client.post(
            "/api/rows/R0001/label",
            json={"labeler": "jake", "severity": "severe", "failure_mode": "", "notes": ""},
        )
        assert resp.status_code == 400

    def test_not_wrong_clears_failure_mode(self, client: TestClient) -> None:
        client.post(
            "/api/rows/R0001/label",
            json={
                "labeler": "jake",
                "severity": "not_wrong",
                "failure_mode": "data_noise",
                "notes": "",
            },
        )
        resp = client.get("/api/rows/R0001", params={"labeler": "jake"})
        body = resp.json()
        assert body["my_label"]["failure_mode"] == ""

    def test_unknown_row_id_rejected(self, client: TestClient) -> None:
        resp = client.post(
            "/api/rows/R9999/label",
            json={
                "labeler": "jake",
                "severity": "severe",
                "failure_mode": "subject_hallucination",
                "notes": "",
            },
        )
        assert resp.status_code == 404


class TestExportCsv:
    def test_export_only_labeled_rows(self, client: TestClient) -> None:
        client.post(
            "/api/rows/R0001/label",
            json={
                "labeler": "jake",
                "severity": "severe",
                "failure_mode": "subject_hallucination",
                "notes": "names swapped",
            },
        )
        resp = client.get("/api/export.csv", params={"labeler": "jake"})
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/csv")
        body = resp.text
        # Header + one data row. csv.QUOTE_ALL wraps every cell in quotes.
        header = body.splitlines()[0]
        assert "row_id" in header and "severity" in header
        assert "failure_mode" in header and "notes" in header
        assert "R0001" in body
        assert "R0002" not in body

    def test_export_round_trips_through_merge_labels(
        self,
        client: TestClient,
        jsonl_path: Path,
        tmp_path: Path,
    ) -> None:
        # Label both rows through the API.
        client.post(
            "/api/rows/R0001/label",
            json={
                "labeler": "jake",
                "severity": "severe",
                "failure_mode": "subject_hallucination",
                "notes": "",
            },
        )
        client.post(
            "/api/rows/R0002/label",
            json={"labeler": "jake", "severity": "not_wrong", "failure_mode": "", "notes": ""},
        )

        # Save export to disk.
        csv_text = client.get("/api/export.csv", params={"labeler": "jake"}).text
        labels_csv = tmp_path / "labels.csv"
        labels_csv.write_text(csv_text)

        # Run merge_labels.py against it. This is the contract test — if the
        # CSV the UI emits stops being merge_labels-compatible, the whole
        # labeling -> training-data path breaks.
        out_jsonl = tmp_path / "labeled.jsonl"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "scripts.eval.merge_labels",
                "--labeling-jsonl",
                str(jsonl_path),
                "--labels-csv",
                str(labels_csv),
                "--labeler",
                "jake",
                "--out",
                str(out_jsonl),
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
        labeled_lines = out_jsonl.read_text().splitlines()
        assert len(labeled_lines) == 2
        labels = [json.loads(line)["label"] for line in labeled_lines]
        sevs = {row["severity"] for row in labels}
        assert sevs == {"severe", "not_wrong"}


class TestStaticUI:
    def test_index_html_served(self, client: TestClient) -> None:
        resp = client.get("/")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "Narrative labeling" in resp.text
