"""Preview endpoint — multi-source audio preview lookup with sidecar caching.

Fetches 30-second preview audio URLs for artists from multiple streaming
services, with fallback chain: iTunes lookup -> Spotify -> Bandcamp -> Deezer -> iTunes search.
Results are cached in a sidecar SQLite database.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from datetime import UTC, datetime

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request

from semantic_index.api.database import get_db, open_cache_db
from semantic_index.api.schemas import PreviewResponse

logger = logging.getLogger(__name__)

preview_router = APIRouter(prefix="/graph", tags=["graph"])

_USER_AGENT = "WXYCSemanticIndex/0.1 (https://wxyc.org; engineering@wxyc.org)"

_CACHE_SCHEMA = """
CREATE TABLE IF NOT EXISTS preview_cache (
    artist_id INTEGER PRIMARY KEY,
    preview_url TEXT,
    track_name TEXT,
    artist_name TEXT,
    artwork_url TEXT,
    source TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""


def _get_cache_db(db_path: str) -> sqlite3.Connection:
    """Open a writable connection to the sidecar preview cache database."""
    return open_cache_db(db_path, "preview", _CACHE_SCHEMA)


def _http_get(url: str, **kwargs) -> httpx.Response:
    """Make an HTTP GET request. Extracted for testability."""
    with httpx.Client(
        timeout=10,
        headers={"User-Agent": _USER_AGENT},
        follow_redirects=True,
    ) as client:
        return client.get(url, **kwargs)


def _get_artist_info(db: sqlite3.Connection, artist_id: int) -> dict | None:
    """Fetch artist name and streaming IDs from the database."""
    try:
        row = db.execute(
            "SELECT a.id, a.canonical_name, "
            "  e.apple_music_artist_id, e.spotify_artist_id, e.bandcamp_id "
            "FROM artist a "
            "LEFT JOIN entity e ON a.entity_id = e.id "
            "WHERE a.id = ?",
            (artist_id,),
        ).fetchone()
    except sqlite3.OperationalError:
        # Old schema without entity table
        row = db.execute(
            "SELECT id, canonical_name FROM artist WHERE id = ?",
            (artist_id,),
        ).fetchone()
        if row is None:
            return None
        return {
            "id": row["id"],
            "canonical_name": row["canonical_name"],
            "apple_music_artist_id": None,
            "spotify_artist_id": None,
            "bandcamp_id": None,
        }

    if row is None:
        return None

    return {
        "id": row["id"],
        "canonical_name": row["canonical_name"],
        "apple_music_artist_id": row["apple_music_artist_id"],
        "spotify_artist_id": row["spotify_artist_id"],
        "bandcamp_id": row["bandcamp_id"],
    }


# -- Source-specific lookup functions --


def _try_itunes_lookup(apple_music_artist_id: str) -> dict | None:
    """Fetch preview via iTunes lookup API using Apple Music artist ID."""
    try:
        resp = _http_get(
            f"https://itunes.apple.com/lookup?id={apple_music_artist_id}&entity=song&limit=5"
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        for result in data.get("results", []):
            if result.get("wrapperType") == "track" and result.get("previewUrl"):
                return {
                    "preview_url": result["previewUrl"],
                    "track_name": result.get("trackName"),
                    "artist_name": result.get("artistName"),
                    "artwork_url": result.get("artworkUrl100"),
                    "source": "itunes_lookup",
                }
    except (httpx.TimeoutException, httpx.HTTPError, Exception):
        logger.debug("iTunes lookup failed for %s", apple_music_artist_id, exc_info=True)
    return None


def _try_spotify(spotify_artist_id: str, client_id: str, client_secret: str) -> dict | None:
    """Fetch preview via Spotify top-tracks API using client credentials."""
    try:
        # Get access token via client credentials flow
        with httpx.Client(timeout=10) as client:
            token_resp = client.post(
                "https://accounts.spotify.com/api/token",
                data={"grant_type": "client_credentials"},
                auth=(client_id, client_secret),
            )
            if token_resp.status_code != 200:
                return None
            access_token = token_resp.json().get("access_token")
            if not access_token:
                return None

            # Fetch top tracks
            tracks_resp = client.get(
                f"https://api.spotify.com/v1/artists/{spotify_artist_id}/top-tracks",
                headers={"Authorization": f"Bearer {access_token}"},
            )
            if tracks_resp.status_code != 200:
                return None

            for track in tracks_resp.json().get("tracks", []):
                if track.get("preview_url"):
                    album = track.get("album", {})
                    images = album.get("images", [])
                    artwork = images[0]["url"] if images else None
                    return {
                        "preview_url": track["preview_url"],
                        "track_name": track.get("name"),
                        "artist_name": track.get("artists", [{}])[0].get("name"),
                        "artwork_url": artwork,
                        "source": "spotify",
                    }
    except (httpx.TimeoutException, httpx.HTTPError, Exception):
        logger.debug("Spotify lookup failed for %s", spotify_artist_id, exc_info=True)
    return None


def _try_bandcamp(bandcamp_id: str) -> dict | None:
    """Scrape Bandcamp artist page for a track stream URL."""
    try:
        resp = _http_get(f"https://{bandcamp_id}.bandcamp.com")
        if resp.status_code != 200:
            return None

        # Find album links on the artist page
        album_match = re.search(r'<a href="(/album/[^"]+)"', resp.text)
        album_url = None
        if album_match:
            album_url = f"https://{bandcamp_id}.bandcamp.com{album_match.group(1)}"
        else:
            # Try the artist page itself (some have tracks directly)
            album_url = f"https://{bandcamp_id}.bandcamp.com"

        # Fetch the album page for track data
        if album_url != f"https://{bandcamp_id}.bandcamp.com":
            resp = _http_get(album_url)
            if resp.status_code != 200:
                return None

        # Extract data-tralbum JSON
        tralbum_match = re.search(r"data-tralbum='([^']*)'", resp.text)
        if not tralbum_match:
            # Try double-quote variant
            tralbum_match = re.search(r'data-tralbum="([^"]*)"', resp.text)
        if not tralbum_match:
            return None

        tralbum_raw = tralbum_match.group(1)
        # Unescape HTML entities
        tralbum_raw = tralbum_raw.replace("&quot;", '"').replace("&amp;", "&")
        tralbum = json.loads(tralbum_raw)

        trackinfo = tralbum.get("trackinfo", [])
        for track in trackinfo:
            file_info = track.get("file", {})
            mp3_url = file_info.get("mp3-128")
            if mp3_url:
                current = tralbum.get("current", {})
                return {
                    "preview_url": mp3_url,
                    "track_name": track.get("title"),
                    "artist_name": current.get("artist") or bandcamp_id,
                    "artwork_url": current.get("art_id"),
                    "source": "bandcamp",
                }
    except (httpx.TimeoutException, httpx.HTTPError, json.JSONDecodeError, Exception):
        logger.debug("Bandcamp scrape failed for %s", bandcamp_id, exc_info=True)
    return None


def _try_deezer(artist_name: str) -> dict | None:
    """Search Deezer for a track preview by artist name."""
    try:
        resp = _http_get(f'https://api.deezer.com/search?q=artist:"{artist_name}"&limit=5')
        if resp.status_code != 200:
            return None
        data = resp.json()
        for result in data.get("data", []):
            if result.get("preview"):
                album = result.get("album", {})
                return {
                    "preview_url": result["preview"],
                    "track_name": result.get("title"),
                    "artist_name": result.get("artist", {}).get("name"),
                    "artwork_url": album.get("cover_medium"),
                    "source": "deezer",
                }
    except (httpx.TimeoutException, httpx.HTTPError, Exception):
        logger.debug("Deezer search failed for %s", artist_name, exc_info=True)
    return None


def _try_itunes_search(artist_name: str) -> dict | None:
    """Search iTunes for a track preview by artist name."""
    try:
        resp = _http_get(f"https://itunes.apple.com/search?term={artist_name}&entity=song&limit=5")
        if resp.status_code != 200:
            return None
        data = resp.json()
        for result in data.get("results", []):
            if result.get("wrapperType") == "track" and result.get("previewUrl"):
                return {
                    "preview_url": result["previewUrl"],
                    "track_name": result.get("trackName"),
                    "artist_name": result.get("artistName"),
                    "artwork_url": result.get("artworkUrl100"),
                    "source": "itunes_search",
                }
    except (httpx.TimeoutException, httpx.HTTPError, Exception):
        logger.debug("iTunes search failed for %s", artist_name, exc_info=True)
    return None


def _lookup_preview(
    artist_info: dict,
    spotify_client_id: str | None = None,
    spotify_client_secret: str | None = None,
) -> dict:
    """Run the multi-source fallback chain and return preview data.

    Returns a dict with keys: preview_url, track_name, artist_name, artwork_url, source.
    """
    apple_music_id = artist_info.get("apple_music_artist_id")
    spotify_id = artist_info.get("spotify_artist_id")
    bandcamp_id = artist_info.get("bandcamp_id")
    name = artist_info["canonical_name"]

    # 1. iTunes lookup (by Apple Music ID)
    if apple_music_id:
        result = _try_itunes_lookup(apple_music_id)
        if result:
            return result

    # 2. Spotify top tracks (by Spotify ID, if credentials configured)
    if spotify_id and spotify_client_id and spotify_client_secret:
        result = _try_spotify(spotify_id, spotify_client_id, spotify_client_secret)
        if result:
            return result

    # 3. Bandcamp (by bandcamp_id)
    if bandcamp_id:
        result = _try_bandcamp(bandcamp_id)
        if result:
            return result

    # 4. Deezer search (by name)
    result = _try_deezer(name)
    if result:
        return result

    # 5. iTunes search (by name)
    result = _try_itunes_search(name)
    if result:
        return result

    # Nothing found
    return {
        "preview_url": None,
        "track_name": None,
        "artist_name": None,
        "artwork_url": None,
        "source": "none",
    }


@preview_router.get(
    "/artists/{artist_id}/preview",
    response_model=PreviewResponse,
)
def get_preview(
    artist_id: int,
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> PreviewResponse:
    """Return a preview audio URL for an artist.

    Checks sidecar cache first, then runs a multi-source fallback chain:
    iTunes lookup -> Spotify -> Bandcamp -> Deezer -> iTunes search.
    """
    # Verify artist exists and get streaming IDs
    artist_info = _get_artist_info(db, artist_id)
    if artist_info is None:
        raise HTTPException(status_code=404, detail="Artist not found")

    # Check cache
    cache_db = _get_cache_db(request.app.state.db_path)
    try:
        cached = cache_db.execute(
            "SELECT preview_url, track_name, artist_name, artwork_url, source "
            "FROM preview_cache WHERE artist_id = ?",
            (artist_id,),
        ).fetchone()

        if cached is not None:
            return PreviewResponse(
                artist_id=artist_id,
                preview_url=cached["preview_url"],
                track_name=cached["track_name"],
                artist_name=cached["artist_name"],
                artwork_url=cached["artwork_url"],
                source=cached["source"],
                cached=True,
            )

        # Run fallback chain
        spotify_client_id = getattr(request.app.state, "spotify_client_id", None)
        spotify_client_secret = getattr(request.app.state, "spotify_client_secret", None)

        result = _lookup_preview(artist_info, spotify_client_id, spotify_client_secret)

        # Cache the result (including null)
        cache_db.execute(
            "INSERT OR REPLACE INTO preview_cache "
            "(artist_id, preview_url, track_name, artist_name, artwork_url, source, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                artist_id,
                result["preview_url"],
                result["track_name"],
                result["artist_name"],
                result["artwork_url"],
                result["source"],
                datetime.now(UTC).isoformat(),
            ),
        )
        cache_db.commit()

        return PreviewResponse(
            artist_id=artist_id,
            preview_url=result["preview_url"],
            track_name=result["track_name"],
            artist_name=result["artist_name"],
            artwork_url=result["artwork_url"],
            source=result["source"],
            cached=False,
        )
    finally:
        cache_db.close()
