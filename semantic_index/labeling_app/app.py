"""FastAPI app for the labeling web UI.

Wires a ``RowSource`` (read-only JSONL) and a ``LabelStore`` (read/write
SQLite) behind a small JSON API plus a static single-page UI.

Endpoints:
    GET  /                          single-page UI (HTML)
    GET  /static/<asset>            UI assets (JS, CSS)
    GET  /api/rows?labeler=...      row summaries + per-row label state
    GET  /api/rows/{row_id}         full row + my_label
    POST /api/rows/{row_id}/label   upsert label
    GET  /api/export.csv            merge_labels-compatible CSV

CSV columns are the four ``merge_labels.py`` reads:
``row_id, severity, failure_mode, notes``. Anything beyond that is ignored
by the merger, so we keep the UI contract narrow.
"""

from __future__ import annotations

import csv
import io
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from semantic_index.labeling_app.row_source import RowNotFoundError, RowSource
from semantic_index.labeling_app.storage import InvalidLabelError, LabelStore

STATIC_DIR = Path(__file__).resolve().parent / "static"


class LabelPayload(BaseModel):
    labeler: str = Field(min_length=1)
    severity: str
    failure_mode: str = ""
    notes: str = ""


def create_app(jsonl_path: str, labels_db_path: str) -> FastAPI:
    """Create a FastAPI app reading rows from JSONL and writing labels to SQLite."""
    app = FastAPI(title="WXYC Narrative Labeling", version="0.1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )

    rows = RowSource(jsonl_path)
    store = LabelStore(labels_db_path)
    app.state.row_source = rows
    app.state.label_store = store

    @app.get("/api/rows")
    def list_rows(labeler: str = Query(min_length=1)) -> dict:
        summaries = rows.summaries()
        labels = store.list_labels(labeler)
        out: list[dict] = []
        for s in summaries:
            label = labels.get(s["row_id"])
            out.append(
                {
                    **s,
                    "my_label": (
                        {
                            "severity": label["severity"],
                            "failure_mode": label["failure_mode"],
                            "notes": label["notes"],
                        }
                        if label
                        else None
                    ),
                }
            )
        return {
            "rows": out,
            "labeled": sum(1 for r in out if r["my_label"] is not None),
            "total": len(out),
        }

    @app.get("/api/rows/{row_id}")
    def get_row(row_id: str, labeler: str = Query(min_length=1)) -> dict:
        try:
            row = rows.get(row_id)
        except RowNotFoundError:
            raise HTTPException(status_code=404, detail=f"row_id {row_id!r} not found") from None
        label = store.get_label(labeler, row_id)
        return {
            "row": row,
            "my_label": (
                {
                    "severity": label["severity"],
                    "failure_mode": label["failure_mode"],
                    "notes": label["notes"],
                }
                if label
                else None
            ),
        }

    @app.post("/api/rows/{row_id}/label")
    def save_label(row_id: str, payload: LabelPayload) -> dict:
        if row_id not in rows.row_ids():
            raise HTTPException(status_code=404, detail=f"row_id {row_id!r} not found")
        try:
            store.upsert_label(
                labeler=payload.labeler,
                row_id=row_id,
                severity=payload.severity,
                failure_mode=payload.failure_mode,
                notes=payload.notes,
            )
        except InvalidLabelError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from None
        return {"status": "ok"}

    @app.get("/api/export.csv")
    def export_csv(labeler: str = Query(min_length=1)) -> PlainTextResponse:
        labels = store.list_labels(labeler)
        buf = io.StringIO()
        w = csv.writer(buf, quoting=csv.QUOTE_ALL)
        w.writerow(["row_id", "severity", "failure_mode", "notes"])
        # Preserve original JSONL order so the CSV reads in the same sequence
        # the labeler saw the rows.
        for rid in rows.row_ids():
            label = labels.get(rid)
            if not label:
                continue
            w.writerow([rid, label["severity"], label["failure_mode"], label["notes"]])
        return PlainTextResponse(
            content=buf.getvalue(),
            media_type="text/csv",
            headers={"content-disposition": f'attachment; filename="labels-{labeler}.csv"'},
        )

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html", media_type="text/html")

    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    return app
