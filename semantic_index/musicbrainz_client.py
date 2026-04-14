"""MusicBrainz cache client for recording resolution.

Queries the musicbrainz-cache PostgreSQL database to resolve artists
to their recording MBIDs via the ``mb_artist_recording`` materialized view.
Identity resolution methods (lookup_by_name, batch_lookup) have been
moved to LML.
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

    def get_recording_mbids(self, mb_artist_ids: list[int]) -> dict[int, list[str]]:
        """Get recording MBIDs for a set of MusicBrainz artist IDs.

        Uses the ``mb_artist_recording`` materialized view which maps
        artist IDs to recording UUIDs via artist credits.

        Args:
            mb_artist_ids: List of MusicBrainz internal artist IDs.

        Returns:
            Dict mapping artist ID to list of recording MBID strings.
        """
        if not mb_artist_ids:
            return {}

        conn = self._get_conn()
        if conn is None:
            return {}

        try:
            result: dict[int, list[str]] = {}
            batch_size = 1000
            for i in range(0, len(mb_artist_ids), batch_size):
                batch = mb_artist_ids[i : i + batch_size]
                rows = conn.execute(
                    "SELECT artist_id, recording_mbid::text "
                    "FROM mb_artist_recording "
                    "WHERE artist_id = ANY(%s)",
                    (batch,),
                ).fetchall()
                for artist_id, mbid in rows:
                    result.setdefault(artist_id, []).append(mbid)

                if (i + batch_size) % 5000 == 0:
                    logger.info(
                        "  Recording lookup: %d/%d artist batches",
                        i // batch_size + 1,
                        (len(mb_artist_ids) + batch_size - 1) // batch_size,
                    )

            return result
        except Exception:
            logger.warning("Recording MBID lookup failed", exc_info=True)
            return {}
