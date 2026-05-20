"""JSONL-backed row source for the labeling UI.

Loads ``labeling.jsonl`` once at startup and serves rows by ``row_id``.
Strips construction-method and expected-label fields before returning a row
to the client — the labeler must not see the answer key for deliberately-
wrong rows or they can't act as an honest detector.
"""

from __future__ import annotations

import json
from pathlib import Path

# Fields produced by build_wrong_set.py (and any future wrong-set constructor)
# that reveal the gold label for deliberately-wrong rows. Stripped before any
# row is sent to the labeler-facing client.
REDACTED_FIELDS: frozenset[str] = frozenset(
    {
        "construction_method",
        "expected_label",
        "metadata_source",
        "bait_notes",
        "regime",
    }
)


class RowNotFoundError(KeyError):
    """Raised when a row_id is not in the loaded JSONL."""


class RowSource:
    """Read-only loader/lookup for ``labeling.jsonl``."""

    def __init__(self, jsonl_path: str | Path) -> None:
        self._path = Path(jsonl_path)
        self._rows: list[dict] = []
        self._by_id: dict[str, dict] = {}
        self._load()

    def _load(self) -> None:
        with self._path.open() as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                rid = row.get("row_id")
                if not rid:
                    continue
                self._rows.append(row)
                self._by_id[rid] = row

    def row_ids(self) -> list[str]:
        return [r["row_id"] for r in self._rows]

    def summaries(self) -> list[dict]:
        """Compact rows for the row-list panel."""
        out: list[dict] = []
        for r in self._rows:
            out.append(
                {
                    "row_id": r["row_id"],
                    "cell_id": r.get("cell_id", ""),
                    "pair": f"{r.get('source_name', '?')} ↔ {r.get('target_name', '?')}",
                    "insufficient_signal": bool(r.get("insufficient_signal")),
                }
            )
        return out

    def get(self, row_id: str) -> dict:
        if row_id not in self._by_id:
            raise RowNotFoundError(row_id)
        return _redact(self._by_id[row_id])


def _redact(row: dict) -> dict:
    return {k: v for k, v in row.items() if k not in REDACTED_FIELDS} | {
        "pair": f"{row.get('source_name', '?')} ↔ {row.get('target_name', '?')}",
    }
