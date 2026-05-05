"""Run the labeling web UI.

Usage:
    python -m semantic_index.labeling_app \\
        --jsonl output/eval/labeling.jsonl \\
        --labels-db output/eval/labels.db \\
        --port 8090

Default ``--labels-db`` is ``<jsonl-path>.labels.db`` (a sidecar next to the
input JSONL), so the typical invocation is just ``--jsonl ...``. The DB is
upsert-only and safe to keep around between sessions; deleting it discards
all labels.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import uvicorn

from semantic_index.labeling_app.app import create_app


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--jsonl", required=True, help="labeling.jsonl backing file")
    ap.add_argument(
        "--labels-db",
        default=None,
        help="SQLite path for label persistence (default: <jsonl>.labels.db)",
    )
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8090)
    args = ap.parse_args(argv)

    jsonl_path = Path(args.jsonl).resolve()
    if not jsonl_path.exists():
        raise SystemExit(f"jsonl file not found: {jsonl_path}")
    labels_db = args.labels_db or f"{jsonl_path}.labels.db"

    app = create_app(str(jsonl_path), labels_db)
    print(f"labeling UI on http://{args.host}:{args.port}")
    print(f"  jsonl:     {jsonl_path}")
    print(f"  labels db: {labels_db}")
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
