"""Tests for the labeling app's SQLite-backed label store."""

from __future__ import annotations

from pathlib import Path

import pytest

from semantic_index.labeling_app.storage import (
    InvalidLabelError,
    LabelStore,
)


@pytest.fixture()
def store(tmp_path: Path) -> LabelStore:
    return LabelStore(tmp_path / "labels.db")


class TestUpsert:
    def test_inserts_new_label(self, store: LabelStore) -> None:
        store.upsert_label(
            labeler="jake",
            row_id="R0001",
            severity="severe",
            failure_mode="subject_hallucination",
            notes="names look swapped",
        )
        got = store.get_label("jake", "R0001")
        assert got is not None
        assert got["severity"] == "severe"
        assert got["failure_mode"] == "subject_hallucination"
        assert got["notes"] == "names look swapped"

    def test_updates_existing_label_on_repeat(self, store: LabelStore) -> None:
        store.upsert_label("jake", "R0001", "severe", "subject_hallucination", "first")
        store.upsert_label("jake", "R0001", "minor", "data_noise", "revised")
        got = store.get_label("jake", "R0001")
        assert got is not None
        assert got["severity"] == "minor"
        assert got["failure_mode"] == "data_noise"
        assert got["notes"] == "revised"

    def test_two_labelers_keep_independent_rows(self, store: LabelStore) -> None:
        store.upsert_label("jake", "R0001", "severe", "subject_hallucination", "")
        store.upsert_label("alex", "R0001", "not_wrong", "", "looks fine")
        jake = store.get_label("jake", "R0001")
        alex = store.get_label("alex", "R0001")
        assert jake is not None and jake["severity"] == "severe"
        assert alex is not None and alex["severity"] == "not_wrong"

    def test_not_wrong_clears_failure_mode(self, store: LabelStore) -> None:
        # Even if the client passes a stray failure_mode, "not_wrong" rows
        # should not carry one — matches merge_labels.py contract.
        store.upsert_label("jake", "R0001", "not_wrong", "subject_hallucination", "")
        got = store.get_label("jake", "R0001")
        assert got is not None
        assert got["failure_mode"] == ""


class TestValidation:
    def test_rejects_unknown_severity(self, store: LabelStore) -> None:
        with pytest.raises(InvalidLabelError):
            store.upsert_label("jake", "R0001", "kinda_wrong", "data_noise", "")

    def test_rejects_unknown_failure_mode(self, store: LabelStore) -> None:
        with pytest.raises(InvalidLabelError):
            store.upsert_label("jake", "R0001", "severe", "vibes", "")

    def test_severe_requires_failure_mode(self, store: LabelStore) -> None:
        with pytest.raises(InvalidLabelError):
            store.upsert_label("jake", "R0001", "severe", "", "")

    def test_minor_requires_failure_mode(self, store: LabelStore) -> None:
        with pytest.raises(InvalidLabelError):
            store.upsert_label("jake", "R0001", "minor", "", "")

    def test_blank_severity_rejected(self, store: LabelStore) -> None:
        with pytest.raises(InvalidLabelError):
            store.upsert_label("jake", "R0001", "", "data_noise", "")


class TestProgress:
    def test_progress_reports_labeled_and_total(self, store: LabelStore) -> None:
        all_ids = ["R0001", "R0002", "R0003"]
        store.upsert_label("jake", "R0001", "severe", "subject_hallucination", "")
        store.upsert_label("jake", "R0003", "not_wrong", "", "")
        prog = store.get_progress("jake", all_ids)
        assert prog == {"labeled": 2, "total": 3}

    def test_progress_ignores_other_labelers(self, store: LabelStore) -> None:
        store.upsert_label("alex", "R0001", "severe", "subject_hallucination", "")
        prog = store.get_progress("jake", ["R0001", "R0002"])
        assert prog == {"labeled": 0, "total": 2}

    def test_list_labels_returns_only_my_rows(self, store: LabelStore) -> None:
        store.upsert_label("jake", "R0001", "severe", "subject_hallucination", "")
        store.upsert_label("alex", "R0002", "minor", "data_noise", "")
        mine = store.list_labels("jake")
        assert set(mine.keys()) == {"R0001"}


class TestPersistence:
    def test_reopening_loads_existing_labels(self, tmp_path: Path) -> None:
        path = tmp_path / "labels.db"
        s1 = LabelStore(path)
        s1.upsert_label("jake", "R0001", "severe", "subject_hallucination", "")
        del s1
        s2 = LabelStore(path)
        got = s2.get_label("jake", "R0001")
        assert got is not None
        assert got["severity"] == "severe"
