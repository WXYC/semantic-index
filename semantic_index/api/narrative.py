"""Narrative endpoint — LLM-generated edge explanations with sidecar caching."""

from __future__ import annotations

import json
import logging
import math
import os
import sqlite3
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from semantic_index.api.database import get_db
from semantic_index.api.routes import EdgeType, _get_artist_or_404, _query_explain
from semantic_index.api.schemas import NarrativeResponse

logger = logging.getLogger(__name__)

narrative_router = APIRouter(prefix="/graph", tags=["graph"])

# Bump whenever the prompt's structure or content changes so the sidecar cache
# evicts stale entries instead of serving them indefinitely.
_PROMPT_VERSION = 4

_SHARED_NEIGHBORS_TOP_K = 5

# Long Discogs style lists invite hallucination — a model fed Outkast's 53
# styles latches onto an outlier ("makina", "breakbeat") and describes a hip
# hop group as channeling it. Cap to the most prominent N. Ordering is
# alphabetical for now because the upstream ``artist_style`` table doesn't
# persist a release_count column; proper "top N by release count" ranking is a
# pipeline-side follow-up. Even alphabetical-top-N drops the bulk of garbage —
# 53 entries → 5.
_STYLES_TOP_N = 5

# Minimum total Adamic-Adar contribution across surfaced shared neighbors for a
# pair to be worth narrating. Pairs below this floor share only generic hubs
# (Frank Sinatra, The Beatles), and even AA reranking can't manufacture a real
# story. Empirical sweet spot from compare_neighbor_weighting.py was 0.8 —
# overrideable via NARRATIVE_MIN_AA_SCORE for tuning without a redeploy.
_DEFAULT_MIN_AA_SCORE = 0.8

_INSUFFICIENT_SIGNAL_NARRATIVE = (
    "WXYC DJs occasionally play these artists together, but they don't share "
    "enough specific musical context — same labels, similar styles, common "
    "collaborators — to characterize a meaningful connection."
)


def _min_aa_score() -> float:
    """Read the AA-score threshold from the environment, falling back to default."""
    raw = os.environ.get("NARRATIVE_MIN_AA_SCORE")
    if raw is None:
        return _DEFAULT_MIN_AA_SCORE
    try:
        return float(raw)
    except ValueError:
        logger.warning(
            "Invalid NARRATIVE_MIN_AA_SCORE=%r; using default %.2f", raw, _DEFAULT_MIN_AA_SCORE
        )
        return _DEFAULT_MIN_AA_SCORE


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

_REQUIRED_CACHE_COLUMNS = {"edge_type", "prompt_version", "insufficient_signal"}

_CACHE_SCHEMA = """
CREATE TABLE IF NOT EXISTS narrative_cache (
    source_id INTEGER NOT NULL,
    target_id INTEGER NOT NULL,
    month INTEGER NOT NULL DEFAULT 0,
    dj_id INTEGER NOT NULL DEFAULT 0,
    edge_type TEXT NOT NULL DEFAULT '',
    prompt_version INTEGER NOT NULL DEFAULT 1,
    insufficient_signal INTEGER NOT NULL DEFAULT 0,
    narrative TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (source_id, target_id, month, dj_id, edge_type, prompt_version)
);
"""


def _get_cache_db(db_path: str) -> sqlite3.Connection:
    """Open a writable connection to the sidecar narrative cache database.

    Cache eviction is by exclusion: ``prompt_version`` is part of the row's
    primary key and reads filter on the current ``_PROMPT_VERSION``, so rows
    written under prior versions stay on disk but are never returned. The
    schema-mismatch drop fires for caches missing any required column — once
    a release adds a column (e.g. ``insufficient_signal``) the next connect
    drops and rebuilds. Subsequent version bumps that don't change the schema
    rely on read-side filtering instead.
    """
    cache_path = db_path + ".narrative-cache.db"
    conn = sqlite3.connect(cache_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    cols = {r[1] for r in conn.execute("PRAGMA table_info(narrative_cache)")}
    if cols and not _REQUIRED_CACHE_COLUMNS.issubset(cols):
        conn.execute("DROP TABLE narrative_cache")
    conn.executescript(_CACHE_SCHEMA)
    return conn


def _write_cache_entry(
    cache_db: sqlite3.Connection,
    lo: int,
    hi: int,
    cache_month: int,
    cache_dj: int,
    cache_edge_type: str,
    narrative: str,
    insufficient_signal: bool,
) -> None:
    """Insert (or replace) a cache row at the current ``_PROMPT_VERSION``."""
    cache_db.execute(
        "INSERT OR REPLACE INTO narrative_cache "
        "(source_id, target_id, month, dj_id, edge_type, prompt_version, "
        "insufficient_signal, narrative, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            lo,
            hi,
            cache_month,
            cache_dj,
            cache_edge_type,
            _PROMPT_VERSION,
            int(insufficient_signal),
            narrative,
            datetime.now(UTC).isoformat(),
        ),
    )
    cache_db.commit()


def _rank_shared_neighbors_by_aa(
    db: sqlite3.Connection,
    source_id: int,
    target_id: int,
) -> list[dict]:
    """Rank ALL shared ``dj_transition`` neighbors of two artists by AA contribution.

    Each returned neighbor's score is its Adamic-Adar *contribution* —
    ``1 / log(degree)``, where ``degree`` is its number of distinct
    ``dj_transition`` partners. Note this is the per-neighbor term, not the
    full Adamic-Adar similarity of the source/target *pair* (which would be
    the sum of these terms over all shared neighbors).

    Returns the full ranked list — *no* top-K cap applied here. Callers slice
    for the prompt; the AA-score threshold (#220) sums the full list so it
    matches the true pair AA, not a top-K underestimate.
    """
    rows = db.execute(
        """
        WITH all_edges AS (
            SELECT source_id AS a, target_id AS b FROM dj_transition
            UNION ALL
            SELECT target_id AS a, source_id AS b FROM dj_transition
        ),
        degrees AS (
            SELECT a AS id, COUNT(DISTINCT b) AS degree FROM all_edges GROUP BY a
        ),
        neighbors_a AS (SELECT b AS nid FROM all_edges WHERE a = :a),
        neighbors_b AS (SELECT b AS nid FROM all_edges WHERE a = :b),
        shared AS (SELECT nid FROM neighbors_a INTERSECT SELECT nid FROM neighbors_b)
        SELECT artist.canonical_name, degrees.degree
        FROM shared
        JOIN artist ON artist.id = shared.nid
        JOIN degrees ON degrees.id = shared.nid
        WHERE artist.id NOT IN (:a, :b)
        """,
        {"a": source_id, "b": target_id},
    ).fetchall()

    scored: list[dict] = []
    for r in rows:
        deg = r["degree"]
        # Unreachable: a shared neighbor connects to both source and target,
        # so degree >= 2. Kept as a guard against log(1) = 0 in case the SQL
        # is ever broadened to include non-shared neighbors.
        if deg < 2:
            continue
        scored.append(
            {
                "name": r["canonical_name"],
                "degree": deg,
                "aa_score": round(1.0 / math.log(deg), 3),
            }
        )
    scored.sort(key=lambda x: x["aa_score"], reverse=True)
    return scored


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
    shared_neighbors: list[dict] | None = None,
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
    if shared_neighbors:
        data["shared_neighbors"] = shared_neighbors
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
    """Build an artist metadata dict with genre, total_plays, Discogs styles, and audio profile."""
    styles: list[str] = []
    try:
        rows = db.execute(
            "SELECT style_tag FROM artist_style WHERE artist_id = ? ORDER BY style_tag LIMIT ?",
            (artist_id, _STYLES_TOP_N),
        ).fetchall()
        styles = [r["style_tag"] for r in rows]
    except sqlite3.OperationalError:
        pass  # artist_style table may not exist

    meta: dict = {
        "name": artist_name,
        "genre": genre,
        "total_plays": total_plays,
        "styles": styles,
    }

    # Audio profile: add descriptive features when available
    try:
        profile = db.execute(
            "SELECT avg_danceability, primary_genre, primary_genre_probability, "
            "voice_instrumental_ratio, feature_centroid, recording_count "
            "FROM audio_profile WHERE artist_id = ?",
            (artist_id,),
        ).fetchone()
        if profile and profile["feature_centroid"]:
            centroid = json.loads(profile["feature_centroid"])
            # Extract narratively useful features from the 59-dim centroid
            mood_labels = [
                "acoustic",
                "aggressive",
                "electronic",
                "happy",
                "party",
                "relaxed",
                "sad",
            ]
            mood_vector = centroid[9:16]
            top_moods = sorted(
                zip(mood_labels, mood_vector, strict=True), key=lambda x: x[1], reverse=True
            )
            audio_meta: dict = {
                "primary_genre": profile["primary_genre"],
                "danceability": round(profile["avg_danceability"], 2),
                "voice_instrumental": (
                    "vocal" if profile["voice_instrumental_ratio"] > 0.5 else "instrumental"
                ),
                "top_moods": [m for m, v in top_moods[:3] if v > 0.3],
                "recording_count": profile["recording_count"],
            }
            # Add BPM and key if available (columns may not exist on older DBs)
            try:
                bpm_row = db.execute(
                    "SELECT avg_bpm, primary_key FROM audio_profile WHERE artist_id = ?",
                    (artist_id,),
                ).fetchone()
                if bpm_row:
                    if bpm_row["avg_bpm"]:
                        audio_meta["bpm"] = round(bpm_row["avg_bpm"])
                    if bpm_row["primary_key"]:
                        audio_meta["key"] = bpm_row["primary_key"]
            except sqlite3.OperationalError:
                pass  # avg_bpm/primary_key columns may not exist
            meta["audio"] = audio_meta
    except sqlite3.OperationalError:
        pass  # audio_profile table may not exist

    return meta


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
            "SELECT narrative, insufficient_signal FROM narrative_cache "
            "WHERE source_id = ? AND target_id = ? AND month = ? AND dj_id = ? "
            "AND edge_type = ? AND prompt_version = ?",
            (lo, hi, cache_month, cache_dj, cache_edge_type, _PROMPT_VERSION),
        ).fetchone()
        if cached_row:
            return NarrativeResponse(
                source=source,
                target=target,
                narrative=cached_row["narrative"],
                cached=True,
                insufficient_signal=bool(cached_row["insufficient_signal"]),
            )
    finally:
        pass  # keep cache_db open for potential write below

    # AA-ranked shared neighbors are computed up-front because the threshold
    # check below can short-circuit the LLM call entirely. The full ranked
    # list is summed for the threshold (so it matches true pair AA), then
    # sliced to top-K for the prompt to keep the LLM input bounded.
    all_shared_neighbors = _rank_shared_neighbors_by_aa(db, source_id, target_id)
    total_aa = sum(n["aa_score"] for n in all_shared_neighbors)
    if total_aa < _min_aa_score():
        # No real connection — skip the LLM, cache a deterministic placeholder
        # so subsequent identical requests stay cheap.
        narrative = _INSUFFICIENT_SIGNAL_NARRATIVE
        _write_cache_entry(
            cache_db, lo, hi, cache_month, cache_dj, cache_edge_type, narrative, True
        )
        cache_db.close()
        return NarrativeResponse(
            source=source,
            target=target,
            narrative=narrative,
            cached=False,
            insufficient_signal=True,
        )

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

    # Cap to top-K only for the prompt; threshold check above used the full sum.
    shared_neighbors = all_shared_neighbors[:_SHARED_NEIGHBORS_TOP_K]

    user_message = _build_prompt(
        source_meta=source_meta,
        target_meta=target_meta,
        relationships=relationships,
        shared_neighbors=shared_neighbors,
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
        _write_cache_entry(
            cache_db, lo, hi, cache_month, cache_dj, cache_edge_type, narrative, False
        )
    finally:
        cache_db.close()

    return NarrativeResponse(source=source, target=target, narrative=narrative, cached=False)
