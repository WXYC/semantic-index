"""Narrative endpoint — LLM-generated edge explanations with sidecar caching."""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from semantic_index.api.database import get_db
from semantic_index.api.routes import EdgeType, _get_artist_or_404, _query_explain
from semantic_index.api.schemas import NarrativeResponse

logger = logging.getLogger(__name__)

narrative_router = APIRouter(prefix="/graph", tags=["graph"])

MONTH_NAMES = [
    "",
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]

_SYSTEM_PROMPT = (
    "You are a music knowledge assistant for WXYC 89.3 FM, a freeform college radio station. "
    "Given structured data about the relationship between two artists in the station's play "
    "history, write 2-3 sentences (under 80 words) explaining their connection in plain "
    "English. Be specific — mention shared genres, personnel names, labels, or play patterns "
    "from the data. Do not add information not present in the data."
)

_CACHE_SCHEMA = """
CREATE TABLE IF NOT EXISTS narrative_cache (
    source_id INTEGER NOT NULL,
    target_id INTEGER NOT NULL,
    month INTEGER NOT NULL DEFAULT 0,
    dj_id INTEGER NOT NULL DEFAULT 0,
    edge_type TEXT NOT NULL DEFAULT '',
    narrative TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (source_id, target_id, month, dj_id, edge_type)
);
"""


def _get_cache_db(db_path: str) -> sqlite3.Connection:
    """Open a writable connection to the sidecar narrative cache database.

    Includes a migration that drops the old schema (PK without ``edge_type``)
    before creating the current one. Safe because this is a regenerable cache.
    """
    cache_path = db_path + ".narrative-cache.db"
    conn = sqlite3.connect(cache_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    # Migrate: old schema has PK without edge_type — ALTER TABLE can't change
    # a PK, so drop and recreate.  This is just a cache; entries regenerate.
    cols = {r[1] for r in conn.execute("PRAGMA table_info(narrative_cache)")}
    if cols and "edge_type" not in cols:
        conn.execute("DROP TABLE narrative_cache")
    conn.executescript(_CACHE_SCHEMA)
    return conn


def _get_anthropic_client(request: Request):
    """Get or lazily create the Anthropic client from app state."""
    if request.app.state.anthropic_client is not None:
        return request.app.state.anthropic_client

    api_key = request.app.state.anthropic_api_key
    if not api_key:
        return None

    try:
        import anthropic

        client = anthropic.Anthropic(api_key=api_key)
        request.app.state.anthropic_client = client
        return client
    except ImportError:
        logger.warning("anthropic SDK not installed — narrative generation unavailable")
        return None


def _build_prompt(
    source_meta: dict,
    target_meta: dict,
    relationships: list[dict],
    month: int | None = None,
    dj_name: str | None = None,
    faceted_pair_count: int | None = None,
) -> str:
    """Build the user message for the LLM prompt."""
    data: dict = {
        "source": source_meta,
        "target": target_meta,
        "relationships": relationships,
    }
    if month is not None or dj_name is not None:
        facet: dict = {}
        if month is not None:
            facet["month"] = MONTH_NAMES[month] if 1 <= month <= 12 else str(month)
        if dj_name is not None:
            facet["dj"] = dj_name
        if faceted_pair_count is not None:
            facet["pair_count"] = faceted_pair_count
        data["facet"] = facet

    return json.dumps(data, separators=(",", ":"))


def _lookup_artist_metadata(
    db: sqlite3.Connection, artist_id: int, artist_name: str, genre: str | None, total_plays: int
) -> dict:
    """Build an artist metadata dict with genre, total_plays, and Discogs style tags."""
    styles: list[str] = []
    try:
        rows = db.execute(
            "SELECT style_tag FROM artist_style WHERE artist_id = ? ORDER BY style_tag",
            (artist_id,),
        ).fetchall()
        styles = [r["style_tag"] for r in rows]
    except sqlite3.OperationalError:
        pass  # artist_style table may not exist
    return {
        "name": artist_name,
        "genre": genre,
        "total_plays": total_plays,
        "styles": styles,
    }


def _lookup_dj_name(db: sqlite3.Connection, dj_id: int) -> str | None:
    """Look up a DJ's display name from the dj table."""
    try:
        row = db.execute("SELECT display_name FROM dj WHERE id = ?", (dj_id,)).fetchone()
        return row["display_name"] if row else None
    except sqlite3.OperationalError:
        return None


def _compute_faceted_pair_count(
    db: sqlite3.Connection,
    source_id: int,
    target_id: int,
    month: int | None,
    dj_id: int | None,
) -> int | None:
    """Compute the filtered pair count between two specific artists."""
    try:
        params: dict = {"a": source_id, "b": target_id}
        clauses = []
        if month is not None:
            clauses.append("p1.month = :month")
            params["month"] = month
        if dj_id is not None:
            clauses.append("p1.dj_id = :dj_id")
            params["dj_id"] = dj_id
        if not clauses:
            return None

        where = " AND ".join(clauses)
        # Forward: a at position N, b at N+1
        sql = (
            "SELECT COUNT(*) FROM play p1 "
            "JOIN play p2 ON p2.show_id = p1.show_id AND p2.sequence = p1.sequence + 1 "
            f"WHERE p1.artist_id = :a AND p2.artist_id = :b AND {where} "  # noqa: S608
        )
        forward = db.execute(sql, params).fetchone()[0]

        # Reverse: b at position N, a at N+1
        params2 = {**params, "a": target_id, "b": source_id}
        sql2 = (
            "SELECT COUNT(*) FROM play p1 "
            "JOIN play p2 ON p2.show_id = p1.show_id AND p2.sequence = p1.sequence + 1 "
            f"WHERE p1.artist_id = :a AND p2.artist_id = :b AND {where} "  # noqa: S608
        )
        reverse = db.execute(sql2, params2).fetchone()[0]
        return int(forward + reverse)
    except sqlite3.OperationalError:
        return None


@narrative_router.get(
    "/artists/{source_id}/explain/{target_id}/narrative",
    response_model=NarrativeResponse,
)
def get_narrative(
    source_id: int,
    target_id: int,
    month: int | None = Query(default=None, ge=1, le=12),
    dj_id: int | None = Query(default=None, ge=1),
    edge_type: str | None = Query(default=None),
    request: Request = None,  # type: ignore[assignment]
    db: sqlite3.Connection = Depends(get_db),
) -> NarrativeResponse:
    """Generate a natural-language explanation of the relationship between two artists.

    Uses Claude Haiku to produce a concise sentence from the structured explain data.
    Results are cached in a sidecar SQLite database. When ``ANTHROPIC_API_KEY`` is not
    set, returns HTTP 501.

    Args:
        edge_type: Optional edge type filter (e.g. ``djTransition``,
            ``sharedPersonnel``).  When provided, the narrative focuses on that
            relationship dimension only.  Omit for a cross-dimensional summary.
    """
    # Validate both artists exist
    source = _get_artist_or_404(db, source_id)
    target = _get_artist_or_404(db, target_id)

    # Normalize pair for cache key (lower ID first)
    lo, hi = min(source_id, target_id), max(source_id, target_id)
    cache_month = month or 0
    cache_dj = dj_id or 0
    cache_edge_type = edge_type or ""

    # Check cache
    cache_db = _get_cache_db(request.app.state.db_path)
    try:
        cached_row = cache_db.execute(
            "SELECT narrative FROM narrative_cache "
            "WHERE source_id = ? AND target_id = ? AND month = ? AND dj_id = ? AND edge_type = ?",
            (lo, hi, cache_month, cache_dj, cache_edge_type),
        ).fetchone()
        if cached_row:
            return NarrativeResponse(
                source=source, target=target, narrative=cached_row["narrative"], cached=True
            )
    finally:
        pass  # keep cache_db open for potential write below

    # Check for Anthropic client
    client = _get_anthropic_client(request)
    if client is None:
        cache_db.close()
        raise HTTPException(
            status_code=501,
            detail="Narrative generation not available (ANTHROPIC_API_KEY not set)",
        )

    # Determine which edge types to query
    if edge_type and edge_type in EdgeType.__members__.values():
        query_types = [EdgeType(edge_type)]
    else:
        query_types = list(EdgeType)

    # Build the structured data for the prompt
    relationships = []
    for et in query_types:
        try:
            rels = _query_explain(db, source_id, target_id, et)
        except sqlite3.OperationalError:
            continue  # table doesn't exist in this database
        for rel in rels:
            relationships.append({"type": rel.type, **rel.detail})

    # Artist metadata for enriched prompt
    source_meta = _lookup_artist_metadata(
        db, source_id, source.canonical_name, source.genre, source.total_plays
    )
    target_meta = _lookup_artist_metadata(
        db, target_id, target.canonical_name, target.genre, target.total_plays
    )

    # Facet context
    dj_name = _lookup_dj_name(db, dj_id) if dj_id else None
    faceted_count = _compute_faceted_pair_count(db, source_id, target_id, month, dj_id)

    user_message = _build_prompt(
        source_meta=source_meta,
        target_meta=target_meta,
        relationships=relationships,
        month=month,
        dj_name=dj_name,
        faceted_pair_count=faceted_count,
    )

    # Call LLM
    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=150,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )
        narrative = response.content[0].text
    except Exception:
        logger.exception("Anthropic API call failed")
        cache_db.close()
        raise HTTPException(status_code=502, detail="Narrative generation failed") from None

    # Write to cache
    try:
        cache_db.execute(
            "INSERT OR REPLACE INTO narrative_cache "
            "(source_id, target_id, month, dj_id, edge_type, narrative, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                lo,
                hi,
                cache_month,
                cache_dj,
                cache_edge_type,
                narrative,
                datetime.now(UTC).isoformat(),
            ),
        )
        cache_db.commit()
    finally:
        cache_db.close()

    return NarrativeResponse(source=source, target=target, narrative=narrative, cached=False)
