"""MusicBrainz cache client for artist lookups.

Queries the musicbrainz-cache PostgreSQL database for artist matching
by name. Supports exact match on artist name and aliases, with batch
lookup for pipeline reconciliation.
"""

import logging

import psycopg

logger = logging.getLogger(__name__)


class MusicBrainzClient:
    """Client for the musicbrainz-cache PostgreSQL database.

    Args:
        cache_dsn: PostgreSQL connection string for musicbrainz-cache.
    """

    def __init__(self, cache_dsn: str) -> None:
        self._cache_dsn = cache_dsn
        self._cache_conn: psycopg.Connection | None = None

    def _get_conn(self) -> psycopg.Connection | None:
        """Get or create the cache connection."""
        if self._cache_conn is None or self._cache_conn.closed:
            try:
                self._cache_conn = psycopg.connect(self._cache_dsn, autocommit=True)
            except Exception:
                logger.warning("Failed to connect to musicbrainz-cache", exc_info=True)
                return None
        return self._cache_conn

    def lookup_by_name(self, name: str) -> tuple[int, str] | None:
        """Look up a MusicBrainz artist by exact name match.

        Checks both mb_artist.name and mb_artist_alias.name
        (case-insensitive).

        Args:
            name: Artist name to search for.

        Returns:
            Tuple of (mb_artist_id, mb_artist_name) or None if not found.
        """
        if not name.strip():
            return None

        conn = self._get_conn()
        if conn is None:
            return None

        try:
            row = conn.execute(
                "SELECT a.id, a.name FROM mb_artist a "
                "WHERE lower(a.name) = lower(%s) "
                "UNION "
                "SELECT a.id, a.name FROM mb_artist a "
                "JOIN mb_artist_alias aa ON a.id = aa.artist "
                "WHERE lower(aa.name) = lower(%s) "
                "LIMIT 1",
                (name, name),
            ).fetchone()
            if row:
                return (row[0], row[1])
            return None
        except Exception:
            logger.warning("MusicBrainz lookup failed for %r", name, exc_info=True)
            return None

    def batch_lookup(self, names: list[str]) -> dict[str, tuple[int, str]]:
        """Look up multiple artists by name in a single query.

        Returns matches keyed by lowercased input name.

        Args:
            names: List of artist names to search.

        Returns:
            Dict mapping lowercased name to (mb_artist_id, mb_artist_name).
        """
        if not names:
            return {}

        conn = self._get_conn()
        if conn is None:
            return {}

        try:
            lower_names = [n.lower() for n in names]
            result: dict[str, tuple[int, str]] = {}
            batch_size = 5000
            for i in range(0, len(lower_names), batch_size):
                batch = lower_names[i : i + batch_size]
                rows = conn.execute(
                    "SELECT lower(q.name) AS query_name, a.id, a.name "
                    "FROM unnest(%s::text[]) AS q(name) "
                    "JOIN mb_artist a ON lower(a.name) = lower(q.name) "
                    "UNION "
                    "SELECT lower(q.name) AS query_name, a.id, a.name "
                    "FROM unnest(%s::text[]) AS q(name) "
                    "JOIN mb_artist_alias aa ON lower(aa.name) = lower(q.name) "
                    "JOIN mb_artist a ON a.id = aa.artist",
                    (batch, batch),
                ).fetchall()
                for query_name, mb_id, mb_name in rows:
                    if query_name not in result:
                        result[query_name] = (mb_id, mb_name)
            return result
        except Exception:
            logger.warning("MusicBrainz batch lookup failed", exc_info=True)
            return {}
