"""MusicBrainz cache client for recording resolution.

Queries the musicbrainz-cache PostgreSQL database to resolve artists
to their recording MBIDs via the ``mb_artist_recording`` materialized view.
Identity resolution methods (lookup_by_name, batch_lookup) have been
moved to LML.
"""

import logging

from semantic_index.utils import LazyPgConnection, batched_with_log

logger = logging.getLogger(__name__)


class MusicBrainzClient:
    """Client for the musicbrainz-cache PostgreSQL database.

    Args:
        cache_dsn: PostgreSQL connection string for musicbrainz-cache.
    """

    def __init__(self, cache_dsn: str) -> None:
        self._pg = LazyPgConnection(cache_dsn, "musicbrainz-cache")

    def _get_conn(self):
        """Get or create the cache connection."""
        return self._pg.get()

    def resolve_gids_to_ids(self, gids: list[str]) -> dict[str, int]:
        """Resolve MusicBrainz artist GIDs (UUIDs) to integer IDs.

        Queries ``mb_artist`` in the musicbrainz-cache to map artist GID
        strings to the internal integer IDs needed by ``mb_artist_recording``.

        Args:
            gids: List of MusicBrainz artist GID strings (UUIDs).

        Returns:
            Dict mapping GID string to integer artist ID. GIDs not found
            in ``mb_artist`` are omitted.
        """
        if not gids:
            return {}

        conn = self._get_conn()
        if conn is None:
            return {}

        try:
            result: dict[str, int] = {}
            for batch in batched_with_log(gids, label="GID resolution"):
                rows = conn.execute(
                    "SELECT id, gid::text FROM mb_artist WHERE gid = ANY(%s)",
                    (batch,),
                ).fetchall()
                for artist_id, gid in rows:
                    result[gid] = artist_id

            return result
        except Exception:
            logger.warning("GID-to-ID resolution failed", exc_info=True)
            return {}

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
            for batch in batched_with_log(mb_artist_ids, label="Recording lookup"):
                rows = conn.execute(
                    "SELECT artist_id, recording_mbid::text "
                    "FROM mb_artist_recording "
                    "WHERE artist_id = ANY(%s)",
                    (batch,),
                ).fetchall()
                for artist_id, mbid in rows:
                    result.setdefault(artist_id, []).append(mbid)

            return result
        except Exception:
            logger.warning("Recording MBID lookup failed", exc_info=True)
            return {}
