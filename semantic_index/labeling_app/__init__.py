"""Web UI for labeling narrative eval-set rows.

Single-page FastAPI app that reads ``output/eval/labeling.jsonl`` (produced by
``scripts/eval/export_labeling.py``), presents one row at a time to a labeler,
and persists their labels in a SQLite sidecar. Labels can be exported as a CSV
that ``scripts/eval/merge_labels.py`` accepts unchanged.

Multiple labelers are isolated by a name they enter on first visit (no auth);
each labeler's progress and labels are tracked separately so multiple people
can label the same eval set in parallel for IRR.
"""

from semantic_index.labeling_app.storage import (
    VALID_FAILURE_MODE,
    VALID_SEVERITY,
    InvalidLabelError,
    LabelStore,
)

__all__ = [
    "InvalidLabelError",
    "LabelStore",
    "VALID_FAILURE_MODE",
    "VALID_SEVERITY",
]
