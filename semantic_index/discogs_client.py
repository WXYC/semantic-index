"""Two-tier Discogs client: discogs-cache PostgreSQL with library-metadata-lookup API fallback.

The client queries the discogs-cache PostgreSQL database first (fast, no rate limit),
then falls back to the library-metadata-lookup HTTP API for cache misses (rate-limited).
Either or both backends can be omitted for graceful degradation.

Note: Styles are only available from the API (discogs-cache has no release_style table).
"""

import logging
import time

import httpx
import psycopg

from semantic_index.models import (
    DiscogsCredit,
    DiscogsLabel,
    DiscogsRelease,
    DiscogsSearchResult,
    DiscogsTrack,
)

logger = logging.getLogger(__name__)

# Rate limit: 50 req/min → 1.2s between requests
_API_INTERVAL = 1.2
_MAX_RETRIES = 3


class DiscogsClient:
    """Two-tier client for Discogs data.

    Args:
        cache_dsn: PostgreSQL connection string for discogs-cache. None to skip cache.
        api_base_url: Base URL for library-metadata-lookup API. None to skip API.
    """

    def __init__(self, cache_dsn: str | None, api_base_url: str | None) -> None:
        self._cache_dsn = cache_dsn
        self._api_base_url = api_base_url.rstrip("/") if api_base_url else None
        self._last_api_call: float = 0
        self._cache_conn: psycopg.Connection | None = None

    def _get_cache_conn(self) -> psycopg.Connection | None:
        """Get or create the cache PostgreSQL connection."""
        if self._cache_dsn is None:
            return None
        if self._cache_conn is None or self._cache_conn.closed:
            try:
                self._cache_conn = psycopg.connect(self._cache_dsn)
            except Exception:
                logger.warning("Failed to connect to discogs-cache", exc_info=True)
                return None
        return self._cache_conn

    def _get_http_client(self) -> httpx.Client | None:
        """Create an HTTP client for the API."""
        if self._api_base_url is None:
            return None
        return httpx.Client(base_url=self._api_base_url, timeout=30)

    def _rate_limit(self) -> None:
        """Sleep to respect the API rate limit."""
        elapsed = time.time() - self._last_api_call
        if elapsed < _API_INTERVAL:
            time.sleep(_API_INTERVAL - elapsed)
        self._last_api_call = time.time()

    def search_artist(
        self, name: str, release_title: str | None = None
    ) -> DiscogsSearchResult | None:
        """Search for an artist by name, returning Discogs identity if found.

        Tries cache first, then API.
        """
        # Try cache
        result = self._search_artist_cache(name)
        if result is not None:
            return result

        # Try API
        return self._search_artist_api(name, release_title)

    def get_release(self, release_id: int) -> DiscogsRelease | None:
        """Get full release metadata by Discogs release ID.

        Tries cache first, then API. Note: cache results have empty styles.
        """
        # Try cache
        release = self._get_release_cache(release_id)
        if release is not None:
            return release

        # Try API
        return self._get_release_api(release_id)

    def get_releases_for_artist(self, artist_name: str) -> list[int]:
        """Get release IDs for an artist from the cache.

        Tries release_artist first (primary credits), then falls back to
        release_track_artist (per-track credits) if no results found.

        Args:
            artist_name: The artist name to search for (case-insensitive).

        Returns:
            List of distinct release IDs where this artist appears.
        """
        conn = self._get_cache_conn()
        if conn is None:
            return []
        try:
            # Try primary artist credits first
            rows = conn.execute(
                """
                SELECT DISTINCT release_id
                FROM release_artist
                WHERE lower(artist_name) = lower(%s) AND extra = 0
                """,
                (artist_name,),
            ).fetchall()
            if rows:
                return [row[0] for row in rows]

            # Fall back to per-track credits
            rows = conn.execute(
                """
                SELECT DISTINCT release_id
                FROM release_track_artist
                WHERE lower(artist_name) = lower(%s)
                """,
                (artist_name,),
            ).fetchall()
            return [row[0] for row in rows]
        except Exception:
            logger.warning(
                "Cache get_releases_for_artist failed for %r", artist_name, exc_info=True
            )
            return []

    def get_bulk_enrichment(self, artist_names: list[str]) -> dict:
        """Fetch enrichment data for ALL artists in a few large queries.

        Instead of per-artist queries, this runs massive JOINs that return
        data for all artists at once, grouped by artist name.

        Returns a dict keyed by artist_name, each containing:
            styles, extra_artists, labels, track_artists
        """
        conn = self._get_cache_conn()
        if conn is None or not artist_names:
            return {}

        try:
            result: dict[str, dict] = {}
            lower_names = list({n.lower() for n in artist_names})
            batch_size = 5000
            use_summary = self._has_summary_tables(conn)

            for batch_start in range(0, len(lower_names), batch_size):
                batch = lower_names[batch_start : batch_start + batch_size]
                batch_num = batch_start // batch_size + 1
                total_batches = (len(lower_names) + batch_size - 1) // batch_size

                if use_summary:
                    # Fast path: query pre-joined summary tables directly by artist name
                    batch_result = self._enrich_from_summaries(conn, batch)
                else:
                    # Slow path: join release tables by release_id
                    batch_result = self._enrich_from_releases(conn, batch)

                result.update(batch_result)

                logger.info(
                    "  Batch %d/%d: %d names → %d enriched",
                    batch_num,
                    total_batches,
                    len(batch),
                    len(batch_result),
                )

            logger.info("Bulk enrichment complete: %d artists enriched", len(result))
            return result
        except Exception:
            logger.warning("Bulk enrichment failed", exc_info=True)
            return {}

    @staticmethod
    def _has_summary_tables(conn: object) -> bool:
        """Check if materialized summary tables exist."""
        execute = conn.execute  # type: ignore[attr-defined]
        try:
            execute("SELECT 1 FROM artist_style_summary LIMIT 1")
            execute("SELECT 1 FROM artist_personnel_summary LIMIT 1")
            execute("SELECT 1 FROM artist_label_summary LIMIT 1")
            return True
        except Exception:
            return False

    @staticmethod
    def _enrich_from_summaries(conn: object, batch: list[str]) -> dict[str, dict]:
        """Fast enrichment using pre-joined summary tables."""
        execute = conn.execute  # type: ignore[attr-defined]
        result: dict[str, dict] = {}

        # Styles: artist_name → style
        style_rows = execute(
            "SELECT artist_name, style FROM artist_style_summary WHERE artist_name = ANY(%s)",
            (batch,),
        ).fetchall()
        artist_styles: dict[str, list[str]] = {}
        for name, style in style_rows:
            artist_styles.setdefault(name, []).append(style)

        # Personnel: artist_name → personnel_name, role
        extra_rows = execute(
            "SELECT artist_name, personnel_name, role FROM artist_personnel_summary WHERE artist_name = ANY(%s)",
            (batch,),
        ).fetchall()
        artist_extras: dict[str, list[tuple[str, str | None]]] = {}
        for name, personnel, role in extra_rows:
            artist_extras.setdefault(name, []).append((personnel, role))

        # Labels: artist_name → label_id, label_name
        label_rows = execute(
            "SELECT artist_name, label_id, label_name FROM artist_label_summary WHERE artist_name = ANY(%s)",
            (batch,),
        ).fetchall()
        artist_labels: dict[str, list[tuple[int | None, str]]] = {}
        for name, label_id, label_name in label_rows:
            artist_labels.setdefault(name, []).append((label_id, label_name))

        # Build result for all artists that had any data
        all_names = set(artist_styles) | set(artist_extras) | set(artist_labels)
        for name in all_names:
            result[name] = {
                "styles": artist_styles.get(name, []),
                "extra_artists": artist_extras.get(name, []),
                "labels": artist_labels.get(name, []),
                "track_artists": [],  # Not available from summary tables
            }

        return result

    @staticmethod
    def _enrich_from_releases(conn: object, batch: list[str]) -> dict[str, dict]:
        """Slow enrichment path: join release tables by release_id."""
        execute = conn.execute  # type: ignore[attr-defined]
        result: dict[str, dict] = {}

        rows = execute(
            "SELECT ra.artist_name, ra.release_id FROM release_artist ra WHERE ra.extra = 0 AND lower(ra.artist_name) = ANY(%s)",
            (batch,),
        ).fetchall()

        artist_releases: dict[str, list[int]] = {}
        for artist_name, release_id in rows:
            artist_releases.setdefault(artist_name, []).append(release_id)

        all_release_ids = list({rid for rids in artist_releases.values() for rid in rids})
        if not all_release_ids:
            return {}

        style_rows = execute(
            "SELECT release_id, style FROM release_style WHERE release_id = ANY(%s)",
            (all_release_ids,),
        ).fetchall()
        release_styles: dict[int, list[str]] = {}
        for rid, style in style_rows:
            release_styles.setdefault(rid, []).append(style)

        extra_rows = execute(
            "SELECT release_id, artist_name, role FROM release_artist WHERE extra = 1 AND release_id = ANY(%s)",
            (all_release_ids,),
        ).fetchall()
        release_extras: dict[int, list[tuple[str, str | None]]] = {}
        for rid, name, role in extra_rows:
            release_extras.setdefault(rid, []).append((name, role))

        label_rows = execute(
            "SELECT release_id, label_id, label_name FROM release_label WHERE release_id = ANY(%s)",
            (all_release_ids,),
        ).fetchall()
        release_labels: dict[int, list[tuple[int | None, str]]] = {}
        for rid, label_id, label_name in label_rows:
            release_labels.setdefault(rid, []).append((label_id, label_name))

        track_artist_rows = execute(
            "SELECT release_id, artist_name FROM release_track_artist WHERE release_id = ANY(%s)",
            (all_release_ids,),
        ).fetchall()
        release_track_artists: dict[int, list[str]] = {}
        for rid, name in track_artist_rows:
            release_track_artists.setdefault(rid, []).append(name)

        for artist_name, rids in artist_releases.items():
            styles: set[str] = set()
            extras: list[tuple[str, str | None]] = []
            labels: list[tuple[int | None, str]] = []
            track_artists: list[tuple[int, str]] = []
            for rid in rids:
                styles.update(release_styles.get(rid, []))
                extras.extend(release_extras.get(rid, []))
                labels.extend(release_labels.get(rid, []))
                for ta_name in release_track_artists.get(rid, []):
                    track_artists.append((rid, ta_name))
            result[artist_name] = {
                "styles": list(styles),
                "extra_artists": extras,
                "labels": labels,
                "track_artists": track_artists,
            }

        return result

    def get_enrichment_for_artist(self, artist_name: str, release_ids: list[int]) -> dict:
        """Fetch all enrichment data for an artist's releases in bulk.

        Instead of N per-release queries, this runs 4 bulk queries across all
        release IDs at once. Returns a dict with keys: styles, extra_artists,
        labels, track_artists.
        """
        conn = self._get_cache_conn()
        if conn is None or not release_ids:
            return {"styles": [], "extra_artists": [], "labels": [], "track_artists": []}

        try:
            ids_tuple = tuple(release_ids)
            placeholder = ",".join(["%s"] * len(ids_tuple))

            # Styles (from release_style table)
            style_rows = conn.execute(
                f"SELECT DISTINCT style FROM release_style WHERE release_id IN ({placeholder})",  # noqa: S608
                ids_tuple,
            ).fetchall()

            # Extra artists (personnel credits)
            extra_rows = conn.execute(
                f"SELECT DISTINCT artist_name, role FROM release_artist WHERE release_id IN ({placeholder}) AND extra = 1",  # noqa: S608
                ids_tuple,
            ).fetchall()

            # Labels
            label_rows = conn.execute(
                f"SELECT DISTINCT label_id, label_name FROM release_label WHERE release_id IN ({placeholder})",  # noqa: S608
                ids_tuple,
            ).fetchall()

            # Track artists (for compilation detection)
            track_artist_rows = conn.execute(
                f"SELECT release_id, artist_name FROM release_track_artist WHERE release_id IN ({placeholder})",  # noqa: S608
                ids_tuple,
            ).fetchall()

            return {
                "styles": [r[0] for r in style_rows],
                "extra_artists": [(r[0], r[1]) for r in extra_rows],
                "labels": [(r[0], r[1]) for r in label_rows],
                "track_artists": [(r[0], r[1]) for r in track_artist_rows],
            }
        except Exception:
            logger.warning(
                "Cache get_enrichment_for_artist failed for %r", artist_name, exc_info=True
            )
            return {"styles": [], "extra_artists": [], "labels": [], "track_artists": []}

    def _search_artist_cache(self, name: str) -> DiscogsSearchResult | None:
        """Search for an artist in the discogs-cache PostgreSQL.

        Tries release_artist first, then falls back to release_track_artist.
        """
        conn = self._get_cache_conn()
        if conn is None:
            return None
        try:
            # Try primary artist credits
            rows = conn.execute(
                """
                SELECT DISTINCT ra.artist_id, ra.artist_name
                FROM release_artist ra
                WHERE ra.extra = 0 AND lower(ra.artist_name) = lower(%s)
                LIMIT 1
                """,
                (name,),
            ).fetchall()
            if rows:
                return DiscogsSearchResult(
                    artist_name=rows[0][1],
                    artist_id=rows[0][0],
                )

            # Fall back to per-track credits
            rows = conn.execute(
                """
                SELECT DISTINCT rta.artist_name
                FROM release_track_artist rta
                WHERE lower(rta.artist_name) = lower(%s)
                LIMIT 1
                """,
                (name,),
            ).fetchall()
            if rows:
                return DiscogsSearchResult(
                    artist_name=rows[0][0],
                    artist_id=None,
                )
            return None
        except Exception:
            logger.warning("Cache search failed for %r", name, exc_info=True)
            return None

    def _search_artist_api(
        self, name: str, release_title: str | None = None
    ) -> DiscogsSearchResult | None:
        """Search for an artist via library-metadata-lookup API."""
        client = self._get_http_client()
        if client is None:
            return None
        try:
            self._rate_limit()
            body: dict = {"artist": name}
            if release_title:
                body["album"] = release_title
            response = client.post("/api/v1/discogs/search", json=body)
            if response.status_code != 200:
                return None
            data = response.json()
            results = data.get("results", [])
            if not results:
                return None
            best = results[0]
            return DiscogsSearchResult(
                artist_name=best.get("artist", name),
                artist_id=None,
                release_id=best.get("release_id"),
                confidence=best.get("confidence", 0),
            )
        except Exception:
            logger.warning("API search failed for %r", name, exc_info=True)
            return None
        finally:
            client.close()

    def _get_release_cache(self, release_id: int) -> DiscogsRelease | None:
        """Get release metadata from discogs-cache PostgreSQL."""
        conn = self._get_cache_conn()
        if conn is None:
            return None
        try:
            # Release header
            row = conn.execute(
                "SELECT id, title, release_year FROM release WHERE id = %s",
                (release_id,),
            ).fetchone()
            if row is None:
                return None

            title = row[1]
            year = row[2]

            # Child tables
            artist_rows = conn.execute(
                "SELECT artist_id, artist_name, extra, role FROM release_artist WHERE release_id = %s",
                (release_id,),
            ).fetchall()

            label_rows = conn.execute(
                "SELECT label_id, label_name, catno FROM release_label WHERE release_id = %s",
                (release_id,),
            ).fetchall()

            track_rows = conn.execute(
                "SELECT position, title, sequence FROM release_track WHERE release_id = %s ORDER BY sequence",
                (release_id,),
            ).fetchall()

            track_artist_rows = conn.execute(
                "SELECT release_id, artist_name FROM release_track_artist WHERE release_id = %s",
                (release_id,),
            ).fetchall()

            # Build model
            main_artists = [
                DiscogsCredit(name=r[1], artist_id=r[0]) for r in artist_rows if r[2] == 0
            ]
            extra_artists = [
                DiscogsCredit(name=r[1], artist_id=r[0], role=r[3])
                for r in artist_rows
                if r[2] == 1
            ]
            labels = [DiscogsLabel(name=r[1], label_id=r[0], catno=r[2]) for r in label_rows]
            tracks = [DiscogsTrack(position=r[0] or "", title=r[1] or "") for r in track_rows]

            # Attach per-track artists for compilation detection
            track_artists_set = {r[1] for r in track_artist_rows}
            if track_artists_set:
                for track in tracks:
                    track.artists = list(track_artists_set)

            artist_name = main_artists[0].name if main_artists else ""
            artist_id = main_artists[0].artist_id if main_artists else None

            return DiscogsRelease(
                release_id=release_id,
                title=title or "",
                artist_name=artist_name,
                artist_id=artist_id,
                year=year,
                styles=[],  # Cache has no styles
                artists=main_artists,
                extra_artists=extra_artists,
                labels=labels,
                tracklist=tracks,
            )
        except Exception:
            logger.warning("Cache get_release failed for %d", release_id, exc_info=True)
            return None

    def _get_release_api(self, release_id: int) -> DiscogsRelease | None:
        """Get release metadata from library-metadata-lookup API."""
        client = self._get_http_client()
        if client is None:
            return None

        for attempt in range(_MAX_RETRIES):
            try:
                self._rate_limit()
                response = client.get(f"/api/v1/discogs/release/{release_id}")

                if response.status_code == 429:
                    backoff = 2 ** (attempt + 1)
                    logger.warning(
                        "Rate limited on release %d, backing off %ds", release_id, backoff
                    )
                    time.sleep(backoff)
                    continue

                if response.status_code != 200:
                    return None

                data = response.json()
                return DiscogsRelease(
                    release_id=data["release_id"],
                    title=data.get("title", ""),
                    artist_name=data.get("artist", ""),
                    artist_id=data.get("artist_id"),
                    year=data.get("year"),
                    styles=data.get("styles", []),
                    artists=[
                        DiscogsCredit(
                            name=a["name"],
                            artist_id=a.get("artist_id"),
                            role=a.get("role"),
                        )
                        for a in data.get("artists", [])
                    ],
                    extra_artists=[
                        DiscogsCredit(
                            name=a["name"],
                            artist_id=a.get("artist_id"),
                            role=a.get("role"),
                        )
                        for a in data.get("extra_artists", [])
                    ],
                    labels=[
                        DiscogsLabel(
                            name=lbl["name"],
                            label_id=lbl.get("label_id"),
                            catno=lbl.get("catno"),
                        )
                        for lbl in data.get("labels", [])
                    ],
                    tracklist=[
                        DiscogsTrack(
                            position=t.get("position", ""),
                            title=t.get("title", ""),
                            artists=t.get("artists", []),
                        )
                        for t in data.get("tracklist", [])
                    ],
                )
            except Exception:
                logger.warning(
                    "API get_release failed for %d (attempt %d)",
                    release_id,
                    attempt + 1,
                    exc_info=True,
                )
                if attempt < _MAX_RETRIES - 1:
                    time.sleep(2 ** (attempt + 1))
                    continue
                return None
        return None
