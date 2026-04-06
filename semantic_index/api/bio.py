"""Artist bio endpoint with sidecar caching and multi-source fallback.

Fetches bios from Wikipedia, Discogs, Wikidata, or generates a summary
from flowsheet stats. Results are cached in a sidecar SQLite database.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from datetime import UTC, datetime

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request

from semantic_index.api.database import get_db
from semantic_index.api.schemas import BioResponse

logger = logging.getLogger(__name__)

bio_router = APIRouter(prefix="/graph", tags=["graph"])

_CACHE_SCHEMA = """
CREATE TABLE IF NOT EXISTS bio_cache (
    artist_id INTEGER PRIMARY KEY,
    bio TEXT NOT NULL,
    source TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""

_WIKI_USER_AGENT = "WXYCSemanticIndex/0.1 (https://wxyc.org; engineering@wxyc.org)"


def _get_cache_db(db_path: str) -> sqlite3.Connection:
    """Open a writable connection to the sidecar bio cache database."""
    cache_path = db_path + ".bio-cache.db"
    conn = sqlite3.connect(cache_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(_CACHE_SCHEMA)
    return conn


def _generated_summary(detail: dict) -> str:
    """Generate a bio from flowsheet stats."""
    genre = detail.get("genre") or "Unknown genre"
    plays = detail.get("total_plays", 0)
    djs = detail.get("dj_count", 0)
    first = detail.get("active_first_year")
    last = detail.get("active_last_year")

    parts = [f"{genre} artist played {plays} times by {djs} DJs on WXYC"]
    if first and last:
        parts.append(f"between {first} and {last}")
    elif first:
        parts.append(f"since {first}")

    return " ".join(parts) + "."


def _fetch_wikipedia(wikidata_qid: str) -> str | None:
    """Fetch Wikipedia extract via Wikidata QID -> enwiki sitelink -> REST API."""
    try:
        with httpx.Client(timeout=10, headers={"User-Agent": _WIKI_USER_AGENT}) as client:
            # Step 1: Get enwiki article title from Wikidata
            resp = client.get(
                "https://www.wikidata.org/w/api.php",
                params={
                    "action": "wbgetentities",
                    "ids": wikidata_qid,
                    "props": "sitelinks",
                    "sitefilter": "enwiki",
                    "format": "json",
                },
            )
            resp.raise_for_status()
            data = resp.json()
            entities = data.get("entities", {})
            entity = entities.get(wikidata_qid, {})
            sitelinks = entity.get("sitelinks", {})
            enwiki = sitelinks.get("enwiki", {})
            title = enwiki.get("title")
            if not title:
                return None

            # Step 2: Get extract from Wikipedia REST API
            resp = client.get(
                f"https://en.wikipedia.org/api/rest_v1/page/summary/{title}",
            )
            resp.raise_for_status()
            summary = resp.json()
            extract: str | None = summary.get("extract")
            return extract
    except Exception:
        logger.warning("Wikipedia fetch failed for %s", wikidata_qid, exc_info=True)
        return None


def _fetch_discogs_profile(discogs_artist_id: int) -> str | None:
    """Fetch artist profile from discogs-cache PostgreSQL."""
    dsn = os.environ.get("DATABASE_URL_DISCOGS")
    if not dsn:
        return None

    try:
        import psycopg

        with psycopg.connect(dsn, autocommit=True) as conn:
            row = conn.execute(
                "SELECT profile FROM artist WHERE id = %s", (discogs_artist_id,)
            ).fetchone()
            if row and row[0]:
                result: str = row[0].strip()
                return result
    except Exception:
        logger.warning(
            "Discogs profile fetch failed for artist %d", discogs_artist_id, exc_info=True
        )
    return None


def _fetch_wikidata_description(wikidata_qid: str) -> str | None:
    """Fetch entity description from wikidata-cache PostgreSQL."""
    dsn = os.environ.get("DATABASE_URL_WIKIDATA")
    if not dsn:
        return None

    try:
        import psycopg

        with psycopg.connect(dsn, autocommit=True) as conn:
            row = conn.execute(
                "SELECT description FROM entity WHERE qid = %s", (wikidata_qid,)
            ).fetchone()
            if row and row[0]:
                desc: str = row[0].strip()
                return desc
    except Exception:
        logger.warning("Wikidata description fetch failed for %s", wikidata_qid, exc_info=True)
    return None


def _fetch_bio(detail: dict) -> tuple[str, str]:
    """Run the fallback chain and return (bio, source).

    Order: Wikipedia -> Discogs (pick longer) -> Wikidata description -> generated.
    """
    qid = detail.get("wikidata_qid")
    discogs_id = detail.get("discogs_artist_id")

    wiki_bio = None
    discogs_bio = None

    if qid:
        wiki_bio = _fetch_wikipedia(qid)

    if discogs_id:
        discogs_bio = _fetch_discogs_profile(discogs_id)

    # Pick the longer of Wikipedia vs Discogs
    if wiki_bio and discogs_bio:
        if len(wiki_bio) >= len(discogs_bio):
            return wiki_bio, "wikipedia"
        return discogs_bio, "discogs"
    if wiki_bio:
        return wiki_bio, "wikipedia"
    if discogs_bio:
        return discogs_bio, "discogs"

    # Wikidata description fallback
    if qid:
        wd_desc = _fetch_wikidata_description(qid)
        if wd_desc:
            return wd_desc, "wikidata"

    # Generated summary
    return _generated_summary(detail), "generated"


@bio_router.get(
    "/artists/{artist_id}/bio",
    response_model=BioResponse,
)
def get_bio(
    artist_id: int,
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> BioResponse:
    """Fetch or generate a bio for an artist.

    Uses a sidecar SQLite cache. On cache miss, tries Wikipedia, Discogs,
    Wikidata description, then generates from flowsheet stats.
    """
    # Check artist exists
    row = db.execute("SELECT id, canonical_name FROM artist WHERE id = ?", (artist_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Artist not found")

    # Check cache
    cache_db = _get_cache_db(request.app.state.db_path)
    try:
        cached = cache_db.execute(
            "SELECT bio, source FROM bio_cache WHERE artist_id = ?", (artist_id,)
        ).fetchone()
        if cached:
            return BioResponse(
                artist_id=artist_id,
                bio=cached["bio"],
                source=cached["source"],
                cached=True,
            )

        # Fetch artist detail for the fallback chain
        try:
            detail_row = db.execute(
                "SELECT a.canonical_name, a.genre, a.total_plays, a.dj_count, "
                "  a.active_first_year, a.active_last_year, "
                "  a.discogs_artist_id, e.wikidata_qid "
                "FROM artist a "
                "LEFT JOIN entity e ON a.entity_id = e.id "
                "WHERE a.id = ?",
                (artist_id,),
            ).fetchone()
        except sqlite3.OperationalError:
            # Old schema without entity table
            detail_row = db.execute(
                "SELECT canonical_name, genre, total_plays, dj_count, "
                "  active_first_year, active_last_year "
                "FROM artist WHERE id = ?",
                (artist_id,),
            ).fetchone()

        detail = dict(detail_row) if detail_row else {}

        bio, source = _fetch_bio(detail)

        # Cache the result
        cache_db.execute(
            "INSERT OR REPLACE INTO bio_cache (artist_id, bio, source, created_at) VALUES (?, ?, ?, ?)",
            (artist_id, bio, source, datetime.now(UTC).isoformat()),
        )
        cache_db.commit()

        return BioResponse(
            artist_id=artist_id,
            bio=bio,
            source=source,
            cached=False,
        )
    finally:
        cache_db.close()
