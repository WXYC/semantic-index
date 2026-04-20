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
from wxyc_etl.schema import (  # type: ignore[import-untyped]
    RELEASE_ARTIST_TABLE,
    RELEASE_LABEL_TABLE,
    RELEASE_STYLE_TABLE,
    RELEASE_TABLE,
    RELEASE_TRACK_ARTIST_TABLE,
    RELEASE_TRACK_TABLE,
)

from semantic_index.models import (
    CompilationEdge,
    DiscogsCredit,
    DiscogsLabel,
    DiscogsRelease,
    DiscogsSearchResult,
    DiscogsTrack,
    LabelFamilyEdge,
    SharedPersonnelEdge,
    SharedStyleEdge,
)
from semantic_index.utils import LazyPgConnection

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
        self._api_base_url = api_base_url.rstrip("/") if api_base_url else None
        self._last_api_call: float = 0
        self._pg = LazyPgConnection(cache_dsn, "discogs-cache")

    def _get_cache_conn(self) -> psycopg.Connection | None:
        """Get or create the cache PostgreSQL connection."""
        return self._pg.get()

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
                f"""
                SELECT DISTINCT release_id
                FROM {RELEASE_ARTIST_TABLE}
                WHERE lower(artist_name) = lower(%s) AND extra = 0
                """,  # noqa: S608
                (artist_name,),
            ).fetchall()
            if rows:
                return [row[0] for row in rows]

            # Fall back to per-track credits
            rows = conn.execute(
                f"""
                SELECT DISTINCT release_id
                FROM {RELEASE_TRACK_ARTIST_TABLE}
                WHERE lower(artist_name) = lower(%s)
                """,  # noqa: S608
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
    def _fetch_grouped(execute, query: str, params: tuple, value_fn) -> dict[str, list]:
        """Run *query* and group results by the first column.

        Args:
            execute: Callable that accepts ``(query, params)`` and returns a
                cursor with ``.fetchall()``.
            query: SQL query whose first column is the grouping key.
            params: Bind parameters for *query*.
            value_fn: Extracts the value(s) from each row (receives the full row tuple).

        Returns:
            Dict mapping the first column's value to a list of extracted values.
        """
        rows = execute(query, params).fetchall()
        grouped: dict[str, list] = {}
        for row in rows:
            grouped.setdefault(row[0], []).append(value_fn(row))
        return grouped

    @staticmethod
    def _enrich_from_summaries(conn: object, batch: list[str]) -> dict[str, dict]:
        """Fast enrichment using pre-joined summary tables."""
        execute = conn.execute  # type: ignore[attr-defined]
        fg = DiscogsClient._fetch_grouped

        artist_styles = fg(
            execute,
            "SELECT artist_name, style FROM artist_style_summary WHERE artist_name = ANY(%s)",
            (batch,),
            lambda row: row[1],
        )
        artist_extras = fg(
            execute,
            "SELECT artist_name, personnel_name, role FROM artist_personnel_summary WHERE artist_name = ANY(%s)",
            (batch,),
            lambda row: (row[1], row[2]),
        )
        artist_labels = fg(
            execute,
            "SELECT artist_name, label_id, label_name FROM artist_label_summary WHERE artist_name = ANY(%s)",
            (batch,),
            lambda row: (row[1], row[2]),
        )

        # Build result for all artists that had any data
        result: dict[str, dict] = {}
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
        fg = DiscogsClient._fetch_grouped

        artist_releases = fg(
            execute,
            f"SELECT ra.artist_name, ra.release_id FROM {RELEASE_ARTIST_TABLE} ra WHERE ra.extra = 0 AND lower(ra.artist_name) = ANY(%s)",  # noqa: S608
            (batch,),
            lambda row: row[1],
        )

        all_release_ids = list({rid for rids in artist_releases.values() for rid in rids})
        if not all_release_ids:
            return {}

        release_styles = fg(
            execute,
            f"SELECT release_id, style FROM {RELEASE_STYLE_TABLE} WHERE release_id = ANY(%s)",  # noqa: S608
            (all_release_ids,),
            lambda row: row[1],
        )
        release_extras = fg(
            execute,
            f"SELECT release_id, artist_name, role FROM {RELEASE_ARTIST_TABLE} WHERE extra = 1 AND release_id = ANY(%s)",  # noqa: S608
            (all_release_ids,),
            lambda row: (row[1], row[2]),
        )
        release_labels = fg(
            execute,
            f"SELECT release_id, label_id, label_name FROM {RELEASE_LABEL_TABLE} WHERE release_id = ANY(%s)",  # noqa: S608
            (all_release_ids,),
            lambda row: (row[1], row[2]),
        )
        release_track_artists = fg(
            execute,
            f"SELECT release_id, artist_name FROM {RELEASE_TRACK_ARTIST_TABLE} WHERE release_id = ANY(%s)",  # noqa: S608
            (all_release_ids,),
            lambda row: row[1],
        )

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
                f"SELECT DISTINCT style FROM {RELEASE_STYLE_TABLE} WHERE release_id IN ({placeholder})",  # noqa: S608
                ids_tuple,
            ).fetchall()

            # Extra artists (personnel credits)
            extra_rows = conn.execute(
                f"SELECT DISTINCT artist_name, role FROM {RELEASE_ARTIST_TABLE} WHERE release_id IN ({placeholder}) AND extra = 1",  # noqa: S608
                ids_tuple,
            ).fetchall()

            # Labels
            label_rows = conn.execute(
                f"SELECT DISTINCT label_id, label_name FROM {RELEASE_LABEL_TABLE} WHERE release_id IN ({placeholder})",  # noqa: S608
                ids_tuple,
            ).fetchall()

            # Track artists (for compilation detection)
            track_artist_rows = conn.execute(
                f"SELECT release_id, artist_name FROM {RELEASE_TRACK_ARTIST_TABLE} WHERE release_id IN ({placeholder})",  # noqa: S608
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
                f"""
                SELECT DISTINCT ra.artist_id, ra.artist_name
                FROM {RELEASE_ARTIST_TABLE} ra
                WHERE ra.extra = 0 AND lower(ra.artist_name) = lower(%s)
                LIMIT 1
                """,  # noqa: S608
                (name,),
            ).fetchall()
            if rows:
                return DiscogsSearchResult(
                    artist_name=rows[0][1],
                    artist_id=rows[0][0],
                )

            # Fall back to per-track credits
            rows = conn.execute(
                f"""
                SELECT DISTINCT rta.artist_name
                FROM {RELEASE_TRACK_ARTIST_TABLE} rta
                WHERE lower(rta.artist_name) = lower(%s)
                LIMIT 1
                """,  # noqa: S608
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
                f"SELECT id, title, release_year FROM {RELEASE_TABLE} WHERE id = %s",  # noqa: S608
                (release_id,),
            ).fetchone()
            if row is None:
                return None

            title = row[1]
            year = row[2]

            # Child tables
            artist_rows = conn.execute(
                f"SELECT artist_id, artist_name, extra, role FROM {RELEASE_ARTIST_TABLE} WHERE release_id = %s",  # noqa: S608
                (release_id,),
            ).fetchall()

            label_rows = conn.execute(
                f"SELECT label_id, label_name, catno FROM {RELEASE_LABEL_TABLE} WHERE release_id = %s",  # noqa: S608
                (release_id,),
            ).fetchall()

            track_rows = conn.execute(
                f"SELECT position, title, sequence FROM {RELEASE_TRACK_TABLE} WHERE release_id = %s ORDER BY sequence",  # noqa: S608
                (release_id,),
            ).fetchall()

            track_artist_rows = conn.execute(
                f"SELECT release_id, artist_name FROM {RELEASE_TRACK_ARTIST_TABLE} WHERE release_id = %s",  # noqa: S608
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

    # --- SQL-based edge computation ---
    #
    # These methods push combinatorial pair generation to PostgreSQL via self-joins
    # on the pre-materialized summary tables. This avoids O(n^2) Python loops and
    # lets PostgreSQL handle the heavy lifting with hash joins.

    def _create_graph_artists_temp_table(
        self, conn: psycopg.Connection, artist_names: list[str]
    ) -> None:
        """Create and populate a temp table of graph artist names."""
        conn.execute("CREATE TEMP TABLE IF NOT EXISTS _graph_artists (name TEXT PRIMARY KEY)")
        conn.execute("DELETE FROM _graph_artists")
        if artist_names:
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO _graph_artists (name) VALUES (%s) ON CONFLICT DO NOTHING",
                    [(n,) for n in artist_names],
                )

    def compute_shared_styles_sql(
        self,
        artist_names: list[str],
        min_jaccard: float = 0.1,
        max_artists: int | None = None,
    ) -> list[SharedStyleEdge]:
        """Compute shared-style edges via SQL self-join on artist_style_summary.

        Args:
            artist_names: Lowercased canonical artist names to include.
            min_jaccard: Minimum Jaccard similarity threshold.
            max_artists: Exclude styles shared by more than this many graph artists.

        Returns:
            List of SharedStyleEdge.
        """
        conn = self._get_cache_conn()
        if conn is None or not artist_names:
            return []

        try:
            self._create_graph_artists_temp_table(conn, artist_names)

            max_clause = ""
            if max_artists is not None:
                max_clause = f"HAVING COUNT(DISTINCT s2.artist_name) <= {int(max_artists)}"

            rows = conn.execute(
                f"""
                WITH filtered_styles AS (
                    SELECT DISTINCT s.artist_name, s.style
                    FROM artist_style_summary s
                    JOIN _graph_artists g ON s.artist_name = g.name
                    WHERE s.style IN (
                        SELECT s2.style
                        FROM artist_style_summary s2
                        JOIN _graph_artists g2 ON s2.artist_name = g2.name
                        GROUP BY s2.style
                        {max_clause}
                    )
                ),
                style_counts AS (
                    SELECT artist_name, COUNT(DISTINCT style) AS cnt
                    FROM filtered_styles
                    GROUP BY artist_name
                ),
                shared AS (
                    SELECT a.artist_name AS artist_a, b.artist_name AS artist_b,
                           COUNT(*) AS isect,
                           array_agg(DISTINCT a.style ORDER BY a.style) AS shared_tags
                    FROM filtered_styles a
                    JOIN filtered_styles b
                        ON a.style = b.style AND a.artist_name < b.artist_name
                    GROUP BY a.artist_name, b.artist_name
                )
                SELECT s.artist_a, s.artist_b,
                       s.isect::float / (ca.cnt + cb.cnt - s.isect) AS jaccard,
                       s.shared_tags
                FROM shared s
                JOIN style_counts ca ON s.artist_a = ca.artist_name
                JOIN style_counts cb ON s.artist_b = cb.artist_name
                WHERE s.isect::float / (ca.cnt + cb.cnt - s.isect) >= %s
                ORDER BY s.artist_a, s.artist_b
                """,  # noqa: S608
                (min_jaccard,),
            ).fetchall()

            edges = [
                SharedStyleEdge(
                    artist_a=row[0],
                    artist_b=row[1],
                    jaccard=row[2],
                    shared_tags=row[3],
                )
                for row in rows
            ]
            logger.info("SQL: %d shared-style edges", len(edges))
            return edges
        except Exception:
            logger.warning("SQL shared styles computation failed", exc_info=True)
            conn.rollback()
            return []

    def compute_shared_personnel_sql(
        self,
        artist_names: list[str],
        min_shared: int = 1,
        max_artists: int | None = None,
    ) -> list[SharedPersonnelEdge]:
        """Compute shared-personnel edges via SQL self-join on artist_personnel_summary.

        Args:
            artist_names: Lowercased canonical artist names to include.
            min_shared: Minimum number of shared personnel to emit an edge.
            max_artists: Exclude personnel credited on more than this many graph artists.

        Returns:
            List of SharedPersonnelEdge.
        """
        conn = self._get_cache_conn()
        if conn is None or not artist_names:
            return []

        try:
            self._create_graph_artists_temp_table(conn, artist_names)

            max_having = "HAVING COUNT(DISTINCT p2.artist_name) >= 2"
            if max_artists is not None:
                max_having += f" AND COUNT(DISTINCT p2.artist_name) <= {int(max_artists)}"

            rows = conn.execute(
                f"""
                WITH filtered_personnel AS (
                    SELECT DISTINCT p.artist_name, p.personnel_name
                    FROM artist_personnel_summary p
                    JOIN _graph_artists g ON p.artist_name = g.name
                    WHERE p.personnel_name IN (
                        SELECT p2.personnel_name
                        FROM artist_personnel_summary p2
                        JOIN _graph_artists g2 ON p2.artist_name = g2.name
                        GROUP BY p2.personnel_name
                        {max_having}
                    )
                ),
                shared AS (
                    SELECT a.artist_name AS artist_a, b.artist_name AS artist_b,
                           COUNT(*) AS shared_count,
                           array_agg(DISTINCT a.personnel_name ORDER BY a.personnel_name) AS shared_names
                    FROM filtered_personnel a
                    JOIN filtered_personnel b
                        ON a.personnel_name = b.personnel_name AND a.artist_name < b.artist_name
                    GROUP BY a.artist_name, b.artist_name
                )
                SELECT artist_a, artist_b, shared_count, shared_names
                FROM shared
                WHERE shared_count >= %s
                ORDER BY artist_a, artist_b
                """,  # noqa: S608
                (min_shared,),
            ).fetchall()

            edges = [
                SharedPersonnelEdge(
                    artist_a=row[0],
                    artist_b=row[1],
                    shared_count=row[2],
                    shared_names=row[3],
                )
                for row in rows
            ]
            logger.info("SQL: %d shared-personnel edges", len(edges))
            return edges
        except Exception:
            logger.warning("SQL shared personnel computation failed", exc_info=True)
            conn.rollback()
            return []

    def compute_label_family_sql(
        self,
        artist_names: list[str],
        max_label_artists: int = 500,
    ) -> list[LabelFamilyEdge]:
        """Compute label-family edges via SQL self-join on artist_label_summary.

        Args:
            artist_names: Lowercased canonical artist names to include.
            max_label_artists: Exclude labels with more than this many graph artists.

        Returns:
            List of LabelFamilyEdge.
        """
        conn = self._get_cache_conn()
        if conn is None or not artist_names:
            return []

        try:
            self._create_graph_artists_temp_table(conn, artist_names)

            rows = conn.execute(
                """
                WITH filtered_labels AS (
                    SELECT DISTINCT l.artist_name, l.label_name
                    FROM artist_label_summary l
                    JOIN _graph_artists g ON l.artist_name = g.name
                    WHERE l.label_name IN (
                        SELECT l2.label_name
                        FROM artist_label_summary l2
                        JOIN _graph_artists g2 ON l2.artist_name = g2.name
                        GROUP BY l2.label_name
                        HAVING COUNT(DISTINCT l2.artist_name) BETWEEN 2 AND %s
                    )
                ),
                shared AS (
                    SELECT a.artist_name AS artist_a, b.artist_name AS artist_b,
                           array_agg(DISTINCT a.label_name ORDER BY a.label_name) AS shared_labels
                    FROM filtered_labels a
                    JOIN filtered_labels b
                        ON a.label_name = b.label_name AND a.artist_name < b.artist_name
                    GROUP BY a.artist_name, b.artist_name
                )
                SELECT artist_a, artist_b, shared_labels
                FROM shared
                ORDER BY artist_a, artist_b
                """,
                (max_label_artists,),
            ).fetchall()

            edges = [
                LabelFamilyEdge(
                    artist_a=row[0],
                    artist_b=row[1],
                    shared_labels=row[2],
                )
                for row in rows
            ]
            logger.info("SQL: %d label-family edges", len(edges))
            return edges
        except Exception:
            logger.warning("SQL label family computation failed", exc_info=True)
            conn.rollback()
            return []

    def compute_compilation_sql(
        self,
        artist_names: list[str],
    ) -> list[CompilationEdge]:
        """Compute compilation co-appearance edges via SQL self-join on artist_compilation_summary.

        Args:
            artist_names: Lowercased canonical artist names to include.

        Returns:
            List of CompilationEdge.
        """
        conn = self._get_cache_conn()
        if conn is None or not artist_names:
            return []

        try:
            self._create_graph_artists_temp_table(conn, artist_names)

            rows = conn.execute(
                """
                WITH comp_appearances AS (
                    SELECT DISTINCT c.artist_name, c.release_id
                    FROM artist_compilation_summary c
                    JOIN _graph_artists g ON c.artist_name = g.name
                    UNION
                    SELECT DISTINCT c.track_artist, c.release_id
                    FROM artist_compilation_summary c
                    JOIN _graph_artists g ON c.track_artist = g.name
                    WHERE c.track_artist IS NOT NULL
                ),
                shared AS (
                    SELECT a.artist_name AS artist_a, b.artist_name AS artist_b,
                           COUNT(DISTINCT a.release_id) AS compilation_count
                    FROM comp_appearances a
                    JOIN comp_appearances b
                        ON a.release_id = b.release_id AND a.artist_name < b.artist_name
                    GROUP BY a.artist_name, b.artist_name
                )
                SELECT artist_a, artist_b, compilation_count
                FROM shared
                ORDER BY artist_a, artist_b
                """
            ).fetchall()

            edges = [
                CompilationEdge(
                    artist_a=row[0],
                    artist_b=row[1],
                    compilation_count=row[2],
                    compilation_titles=[],  # Titles not available from summary table
                )
                for row in rows
            ]
            logger.info("SQL: %d compilation edges", len(edges))
            return edges
        except Exception:
            logger.warning("SQL compilation computation failed", exc_info=True)
            conn.rollback()
            return []
