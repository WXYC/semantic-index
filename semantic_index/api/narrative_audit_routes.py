"""Narrative-audit read endpoint."""

from __future__ import annotations

from fastapi import APIRouter, Query, Request

from semantic_index.narrative_audit import read_recent_audits

narrative_audit_router = APIRouter(prefix="/graph", tags=["graph"])


@narrative_audit_router.get("/narrative-audit/recent")
def get_recent_audits(
    request: Request,
    limit: int = Query(default=50, ge=1, le=500),
    flagged_only: bool = Query(default=False),
) -> dict:
    """Return the most-recent narrative-audit rows.

    Returns an empty list when no audits have run yet (fresh deploy before
    the first ``scripts/audit_narratives.py`` invocation).
    """
    rows = read_recent_audits(request.app.state.db_path, limit=limit, flagged_only=flagged_only)
    return {"audits": rows}
