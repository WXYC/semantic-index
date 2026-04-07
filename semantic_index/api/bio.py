"""Artist bio endpoint with sidecar caching and multi-source fallback.

Fetches bios from Wikipedia, Discogs, Wikidata, or generates a summary
from flowsheet stats. Results are cached in a sidecar SQLite database.
"""

from __future__ import annotations

import logging
import os
import re
import sqlite3
from datetime import UTC, datetime

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request

from semantic_index.api.database import get_db
from semantic_index.api.schemas import BandcampAlbumResponse, BioResponse

logger = logging.getLogger(__name__)

# Regex patterns for Discogs markup tag classification
_PAT_ARTIST_NAME = re.compile(r"^a=(.+)$")
_PAT_ARTIST_ID = re.compile(r"^a(\d+)$")
_PAT_RELEASE_ID = re.compile(r"^r=?(\d+)$")
_PAT_MASTER_ID = re.compile(r"^m=?(\d+)$")
_PAT_LABEL_NAME = re.compile(r"^l=(.+)$")
_PAT_URL_OPEN = re.compile(r"^url=(.+)$")
_PAT_CLOSING_TAG = re.compile(r"^/(.+)$")
_PAT_DISAMBIGUATION = re.compile(r" \(\d+\)$")


def parse_discogs_markup(text: str) -> str:
    """Parse Discogs markup to HTML.

    Converts ``[a=Artist Name]`` to Discogs search links,
    ``[l=Label]`` to plain text, ``[b]...[/b]`` to ``<b>``,
    ``[i]...[/i]`` to ``<i>``, ``[u]...[/u]`` to ``<u>``,
    and ``[url=...]...[/url]`` to ``<a>`` tags.

    Port of ``DiscogsMarkupParser.swift`` from wxyc-ios-64/Shared/Metadata.
    """
    tokens = _tokenize(text)
    return _render(tokens)


def _tokenize(text: str) -> list[tuple]:
    """Tokenize Discogs markup into (type, ...) tuples."""
    tokens: list[tuple] = []
    remaining = text
    while remaining:
        idx = remaining.find("[")
        if idx == -1:
            tokens.append(("text", remaining))
            break
        if idx > 0:
            tokens.append(("text", remaining[:idx]))
        close = remaining.find("]", idx)
        if close == -1:
            tokens.append(("text", remaining[idx:]))
            break
        tag = remaining[idx + 1 : close]
        remaining = remaining[close + 1 :]
        if not tag:
            continue
        token = _classify_tag(tag, remaining)
        if token is not None:
            tok, remaining = token
            tokens.append(tok)
    return tokens


def _find_closing(text: str, tag: str) -> tuple[str, str] | None:
    """Find [/tag] in text, return (content, rest) or None."""
    target = f"[/{tag}]"
    idx = text.find(target)
    if idx == -1:
        return None
    return text[:idx], text[idx + len(target) :]


def _classify_tag(tag: str, remaining: str) -> tuple[tuple, str] | None:
    m = _PAT_ARTIST_NAME.match(tag)
    if m:
        name = m.group(1)
        display = _PAT_DISAMBIGUATION.sub("", name)
        return ("artist", name, display), remaining

    m = _PAT_ARTIST_ID.match(tag)
    if m:
        return ("artist_id", int(m.group(1))), remaining

    m = _PAT_RELEASE_ID.match(tag)
    if m:
        return ("release_id", int(m.group(1))), remaining

    m = _PAT_MASTER_ID.match(tag)
    if m:
        return ("master_id", int(m.group(1))), remaining

    m = _PAT_LABEL_NAME.match(tag)
    if m:
        return ("label", m.group(1)), remaining

    m = _PAT_URL_OPEN.match(tag)
    if m:
        result = _find_closing(remaining, "url")
        if result:
            content, rest = result
            return ("url", m.group(1), content), rest
        return ("text", remaining), ""

    if tag in ("b", "i", "u"):
        result = _find_closing(remaining, tag)
        if result:
            content, rest = result
            return (tag, content), rest
        return None

    if _PAT_CLOSING_TAG.match(tag):
        return None

    return None


def _html_escape(text: str) -> str:
    """Escape HTML special characters."""
    return (
        text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
    )


def _render(tokens: list[tuple]) -> str:
    """Render tokens to HTML string."""
    parts: list[str] = []
    for tok in tokens:
        kind = tok[0]
        if kind == "text":
            parts.append(_html_escape(tok[1]))
        elif kind == "artist":
            _name, display = tok[1], tok[2]
            encoded = _html_escape(_name)
            parts.append(
                f'<a href="https://www.discogs.com/search/?q={encoded}&type=artist" '
                f'target="_blank" rel="noopener">{_html_escape(display)}</a>'
            )
        elif kind == "artist_id":
            # Without a resolver, skip ID-only references
            pass
        elif kind == "release_id":
            aid = tok[1]
            parts.append(
                f'<a href="https://www.discogs.com/release/{aid}" target="_blank" rel="noopener">[release]</a>'
            )
        elif kind == "master_id":
            mid = tok[1]
            parts.append(
                f'<a href="https://www.discogs.com/master/{mid}" target="_blank" rel="noopener">[master]</a>'
            )
        elif kind == "label":
            parts.append(_html_escape(tok[1]))
        elif kind == "url":
            url, content = tok[1], tok[2]
            parts.append(
                f'<a href="{_html_escape(url)}" target="_blank" rel="noopener">{_html_escape(content)}</a>'
            )
        elif kind == "b":
            parts.append(f"<b>{_html_escape(tok[1])}</b>")
        elif kind == "i":
            parts.append(f"<i>{_html_escape(tok[1])}</i>")
        elif kind == "u":
            parts.append(f"<u>{_html_escape(tok[1])}</u>")
    return "".join(parts)


bio_router = APIRouter(prefix="/graph", tags=["graph"])

_CACHE_SCHEMA = """
CREATE TABLE IF NOT EXISTS bio_cache (
    artist_id INTEGER PRIMARY KEY,
    bio TEXT NOT NULL,
    source TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bandcamp_album_cache (
    bandcamp_id TEXT PRIMARY KEY,
    album_id TEXT,
    album_title TEXT,
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
                result: str = parse_discogs_markup(row[0].strip())
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


def _fetch_bandcamp_album(bandcamp_id: str) -> tuple[str, str] | None:
    """Scrape the Bandcamp artist page for the first album ID.

    Parses ``data-item-id="album-{numeric_id}"`` attributes from the
    discography grid.

    Returns:
        (album_id, album_title) tuple, or None if not found.
    """
    import re

    try:
        with httpx.Client(
            timeout=10,
            headers={"User-Agent": _WIKI_USER_AGENT},
            follow_redirects=True,
        ) as client:
            resp = client.get(f"https://{bandcamp_id}.bandcamp.com")
            resp.raise_for_status()
            html = resp.text

            # Extract album IDs from data-item-id attributes
            match = re.search(r'data-item-id="album-(\d+)"', html)
            if not match:
                return None

            album_id = match.group(1)

            # Try to extract album title nearby
            title_match = re.search(
                r'data-item-id="album-' + album_id + r'".*?<p class="title">\s*([^<]+)',
                html,
                re.DOTALL,
            )
            album_title = title_match.group(1).strip() if title_match else ""

            return album_id, album_title
    except Exception:
        logger.warning("Bandcamp scrape failed for %s", bandcamp_id, exc_info=True)
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


@bio_router.get(
    "/bandcamp/{bandcamp_id}/album",
    response_model=BandcampAlbumResponse,
)
def get_bandcamp_album(
    bandcamp_id: str,
    request: Request,
) -> BandcampAlbumResponse:
    """Look up the first album ID for a Bandcamp artist, with caching."""
    cache_db = _get_cache_db(request.app.state.db_path)
    try:
        cached = cache_db.execute(
            "SELECT album_id, album_title FROM bandcamp_album_cache WHERE bandcamp_id = ?",
            (bandcamp_id,),
        ).fetchone()
        if cached:
            return BandcampAlbumResponse(
                bandcamp_id=bandcamp_id,
                album_id=cached["album_id"],
                album_title=cached["album_title"],
                cached=True,
            )

        result = _fetch_bandcamp_album(bandcamp_id)
        album_id = result[0] if result else None
        album_title = result[1] if result else None

        cache_db.execute(
            "INSERT OR REPLACE INTO bandcamp_album_cache "
            "(bandcamp_id, album_id, album_title, created_at) VALUES (?, ?, ?, ?)",
            (bandcamp_id, album_id, album_title, datetime.now(UTC).isoformat()),
        )
        cache_db.commit()

        return BandcampAlbumResponse(
            bandcamp_id=bandcamp_id,
            album_id=album_id,
            album_title=album_title,
            cached=False,
        )
    finally:
        cache_db.close()
