"""Merge a filled-in labeler CSV back onto the labeling JSONL backing.

A labeler downloads ``labeling.csv`` from Sheets (or any other tool that round-
trips CSV with the same columns), fills the ``severity`` / ``failure_mode`` /
``notes`` columns, and uploads the result. This script joins those filled
labels onto the JSONL backing file by ``row_id``, producing a single
``labeling_labeled.jsonl`` ready for downstream analysis (eval-set training,
backscore precision/recall, etc.).

Validates that:
- Every ``row_id`` in the CSV matches exactly one row in the JSONL.
- ``severity`` is one of the rubric's three values when filled.
- ``failure_mode`` is one of the rubric's five values when filled.

Multiple labelers can run this script with different CSV inputs; each output
JSONL is keyed by ``labeler_id`` (passed via ``--labeler``) so a follow-up
script can compute IRR.

Usage:
    python -m scripts.eval.merge_labels \
        --labeling-jsonl output/eval/labeling.jsonl \
        --labels-csv path/to/labeler-jake.csv \
        --labeler jake \
        --out output/eval/labeling_labeled.jake.jsonl
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

VALID_SEVERITY = {"severe", "minor", "not_wrong"}
VALID_FAILURE_MODE = {
    "subject_hallucination",
    "neighbor_characterization",
    "dj_intent",
    "data_noise",
    "other",
}


def _load_jsonl(path: Path) -> dict[str, dict]:
    """Return ``{row_id: row}`` dict from a JSONL file. Raises on duplicate row_id."""
    rows: dict[str, dict] = {}
    with path.open() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            row_id = r["row_id"]
            if row_id in rows:
                raise ValueError(f"Duplicate row_id in {path}: {row_id}")
            rows[row_id] = r
    return rows


def _normalize_label(value: str | None) -> str:
    """Lowercase + strip + map common variants to canonical rubric values."""
    if value is None:
        return ""
    v = value.strip().lower()
    # tolerate dashes / spaces in failure mode codes
    return v.replace(" ", "_").replace("-", "_")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--labeling-jsonl", required=True)
    ap.add_argument("--labels-csv", required=True)
    ap.add_argument("--labeler", required=True, help="Identifier for this labeler (e.g. 'jake')")
    ap.add_argument("--out", required=True)
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    backing = _load_jsonl(Path(args.labeling_jsonl))
    logger.info("Backing JSONL: %d rows", len(backing))

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    written = unlabeled = errors = 0
    seen_ids: set[str] = set()
    severity_counts: dict[str, int] = {}
    mode_counts: dict[str, int] = {}

    with Path(args.labels_csv).open() as csv_fh, out_path.open("w") as jl_fh:
        rdr = csv.DictReader(csv_fh)
        for csv_row in rdr:
            row_id = csv_row.get("row_id", "").strip()
            if not row_id:
                continue
            if row_id not in backing:
                logger.warning("CSV row_id %s not in backing JSONL", row_id)
                errors += 1
                continue
            if row_id in seen_ids:
                logger.warning("CSV row_id %s appears twice in labels CSV", row_id)
                errors += 1
                continue
            seen_ids.add(row_id)

            severity = _normalize_label(csv_row.get("severity"))
            failure_mode = _normalize_label(csv_row.get("failure_mode"))

            if not severity:
                unlabeled += 1
                continue

            if severity not in VALID_SEVERITY:
                logger.warning("row %s: invalid severity %r", row_id, severity)
                errors += 1
                continue
            if severity != "not_wrong":
                if not failure_mode:
                    logger.warning("row %s: severity=%s but failure_mode blank", row_id, severity)
                    errors += 1
                    continue
                if failure_mode not in VALID_FAILURE_MODE:
                    logger.warning("row %s: invalid failure_mode %r", row_id, failure_mode)
                    errors += 1
                    continue
            else:
                # "not_wrong" rows shouldn't carry a failure_mode; clear if set.
                failure_mode = ""

            severity_counts[severity] = severity_counts.get(severity, 0) + 1
            if failure_mode:
                mode_counts[failure_mode] = mode_counts.get(failure_mode, 0) + 1

            out_row = dict(backing[row_id])
            out_row["label"] = {
                "labeler": args.labeler,
                "severity": severity,
                "failure_mode": failure_mode,
                "notes": csv_row.get("notes", "").strip(),
            }
            jl_fh.write(json.dumps(out_row, separators=(",", ":")) + "\n")
            written += 1

    missing_from_csv = set(backing) - seen_ids
    logger.info(
        "Wrote %d labeled rows -> %s (%d unlabeled in CSV, %d not in CSV at all, %d errors)",
        written,
        out_path,
        unlabeled,
        len(missing_from_csv),
        errors,
    )
    logger.info("Severity counts: %s", severity_counts)
    logger.info("Failure mode counts: %s", mode_counts)
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
