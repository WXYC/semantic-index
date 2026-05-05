"""SQLite-backed label store for the labeling web UI.

One row per ``(labeler, row_id)`` — upsert on save, so labelers can revisit
and revise. Validation mirrors ``scripts/eval/merge_labels.py`` so anything
the store accepts will round-trip through the merge step unchanged.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path

VALID_SEVERITY = frozenset({"severe", "minor", "not_wrong"})
VALID_FAILURE_MODE = frozenset(
    {
        "subject_hallucination",
        "neighbor_characterization",
        "dj_intent",
        "data_noise",
        "other",
    }
)


class InvalidLabelError(ValueError):
    """Raised when a label fails rubric validation."""


_SCHEMA = """
CREATE TABLE IF NOT EXISTS label (
    labeler TEXT NOT NULL,
    row_id TEXT NOT NULL,
    severity TEXT NOT NULL,
    failure_mode TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL,
    PRIMARY KEY (labeler, row_id)
);
CREATE INDEX IF NOT EXISTS label_by_labeler ON label(labeler);
"""


class LabelStore:
    """Append/upsert label storage keyed on ``(labeler, row_id)``."""

    def __init__(self, db_path: str | Path) -> None:
        self._path = str(db_path)
        Path(self._path).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(_SCHEMA)
            conn.commit()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _validate(severity: str, failure_mode: str) -> tuple[str, str]:
        if severity not in VALID_SEVERITY:
            raise InvalidLabelError(
                f"severity must be one of {sorted(VALID_SEVERITY)}, got {severity!r}"
            )
        if severity == "not_wrong":
            return severity, ""
        if not failure_mode:
            raise InvalidLabelError(f"severity={severity!r} requires a failure_mode")
        if failure_mode not in VALID_FAILURE_MODE:
            raise InvalidLabelError(
                f"failure_mode must be one of {sorted(VALID_FAILURE_MODE)}, got {failure_mode!r}"
            )
        return severity, failure_mode

    def upsert_label(
        self,
        labeler: str,
        row_id: str,
        severity: str,
        failure_mode: str,
        notes: str,
    ) -> None:
        if not labeler:
            raise InvalidLabelError("labeler must be non-empty")
        if not row_id:
            raise InvalidLabelError("row_id must be non-empty")
        severity, failure_mode = self._validate(severity, failure_mode)
        now = datetime.now(UTC).isoformat(timespec="seconds")
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO label (labeler, row_id, severity, failure_mode, notes, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(labeler, row_id) DO UPDATE SET
                    severity = excluded.severity,
                    failure_mode = excluded.failure_mode,
                    notes = excluded.notes,
                    updated_at = excluded.updated_at
                """,
                (labeler, row_id, severity, failure_mode, notes, now),
            )
            conn.commit()

    def get_label(self, labeler: str, row_id: str) -> dict | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT severity, failure_mode, notes, updated_at FROM label "
                "WHERE labeler = ? AND row_id = ?",
                (labeler, row_id),
            ).fetchone()
        return dict(row) if row else None

    def list_labels(self, labeler: str) -> dict[str, dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT row_id, severity, failure_mode, notes, updated_at FROM label "
                "WHERE labeler = ?",
                (labeler,),
            ).fetchall()
        return {r["row_id"]: dict(r) for r in rows}

    def get_progress(self, labeler: str, all_row_ids: Iterable[str]) -> dict[str, int]:
        ids = list(all_row_ids)
        labeled = self.list_labels(labeler)
        return {"labeled": sum(1 for r in ids if r in labeled), "total": len(ids)}

    def list_labelers(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute("SELECT DISTINCT labeler FROM label ORDER BY labeler").fetchall()
        return [r["labeler"] for r in rows]
