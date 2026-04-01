"""Reconciliation module: bulk Discogs matching for unreconciled artists.

Queries the entity store for artists with ``reconciliation_status='unreconciled'``,
matches them against the discogs-cache PostgreSQL ``release_artist`` table in
batches, persists per-artist styles from ``release_style``, and updates each
artist's ``discogs_artist_id`` and reconciliation status.

Usage::

    from semantic_index.discogs_client import DiscogsClient
    from semantic_index.entity_store import EntityStore
    from semantic_index.reconciliation import ArtistReconciler

    store = EntityStore("output/wxyc_artist_graph.db")
    store.initialize()
    client = DiscogsClient(cache_dsn="postgresql://...", api_base_url=None)
    reconciler = ArtistReconciler(store, client)
    report = reconciler.reconcile_batch(batch_size=1000)
"""

from __future__ import annotations

import logging

from semantic_index.discogs_client import DiscogsClient
from semantic_index.entity_store import EntityStore
from semantic_index.models import ReconciliationReport

logger = logging.getLogger(__name__)


class ArtistReconciler:
    """Bulk Discogs matching for unreconciled artists.

    Args:
        store: Entity store for reading/writing artist data.
        client: Discogs client whose cache connection is used for bulk lookups.
    """

    def __init__(self, store: EntityStore, client: DiscogsClient) -> None:
        self._store = store
        self._client = client

    def reconcile_batch(self, batch_size: int = 1000) -> ReconciliationReport:
        """Query unreconciled artists and process in batches.

        Only artists with ``reconciliation_status='unreconciled'`` are
        attempted. Artists with any other status are counted as skipped.

        Args:
            batch_size: Number of artist names per bulk Discogs query.

        Returns:
            ReconciliationReport with counts of attempted, succeeded,
            no_match, errored, and skipped artists.
        """
        total = self._store._conn.execute("SELECT COUNT(*) FROM artist").fetchone()[0]
        unreconciled = self._store.get_unreconciled_artists()
        skipped = total - len(unreconciled)

        succeeded = 0
        no_match = 0
        errored = 0

        # Build id lookup for the unreconciled set
        name_to_id = {name: aid for aid, name in unreconciled}

        for batch_start in range(0, len(unreconciled), batch_size):
            batch = unreconciled[batch_start : batch_start + batch_size]
            batch_names = [name for _, name in batch]

            try:
                matches = self._reconcile_discogs_bulk(batch_names)
            except Exception:
                logger.warning("Bulk reconciliation failed for batch", exc_info=True)
                errored += len(batch_names)
                continue

            for name in batch_names:
                artist_id = name_to_id[name]
                if name in matches:
                    discogs_id, styles = matches[name]
                    self._store.upsert_artist(name, discogs_artist_id=discogs_id)
                    if styles:
                        self._store.persist_artist_styles(artist_id, styles)
                    self._store.log_reconciliation(
                        artist_id=artist_id,
                        source="discogs",
                        external_id=str(discogs_id),
                        confidence=None,
                        method="cache_lookup",
                    )
                    self._store.update_reconciliation_status(artist_id, "reconciled")
                    succeeded += 1
                else:
                    self._store.update_reconciliation_status(artist_id, "no_match")
                    no_match += 1

        attempted = succeeded + no_match + errored
        logger.info(
            "Reconciliation complete: %d attempted, %d succeeded, %d no_match, %d errored, %d skipped",
            attempted,
            succeeded,
            no_match,
            errored,
            skipped,
        )
        return ReconciliationReport(
            total=total,
            attempted=attempted,
            succeeded=succeeded,
            no_match=no_match,
            errored=errored,
            skipped=skipped,
        )

    @staticmethod
    def _query_with_fallback(
        conn: object, primary_sql: str, fallback_sql: str, params: tuple
    ) -> list:
        """Try a query against a materialized summary table, fall back to join."""
        execute = getattr(conn, "execute")
        try:
            result = execute(primary_sql, params).fetchall()
            if result:
                return result  # type: ignore[no-any-return]
            return execute(fallback_sql, params).fetchall()  # type: ignore[no-any-return]
        except Exception:
            return execute(fallback_sql, params).fetchall()  # type: ignore[no-any-return]

    def _reconcile_discogs_bulk(self, names: list[str]) -> dict[str, tuple[int, list[str]]]:
        """Batch Discogs lookup via the ``release_artist`` table.

        Queries the discogs-cache PostgreSQL using ``ANY()`` for efficient
        bulk matching, then fetches per-artist styles from ``release_style``.

        Args:
            names: List of canonical artist names to look up.

        Returns:
            Dict mapping canonical_name to ``(discogs_artist_id, [styles])``.
            Only names with a match are included.
        """
        if not names:
            return {}

        conn = self._client._get_cache_conn()
        if conn is None:
            return {}

        lower_to_canonical: dict[str, str] = {n.lower(): n for n in names}
        lower_names = list(lower_to_canonical.keys())

        # 1. Match artist names against Discogs
        # Try materialized summary table first, fall back to release_artist join
        rows = self._query_with_fallback(
            conn,
            "SELECT artist_name, discogs_artist_id FROM artist_discogs_id WHERE artist_name = ANY(%s)",
            "SELECT DISTINCT lower(ra.artist_name), ra.artist_id "
            "FROM release_artist ra "
            "WHERE ra.extra = 0 AND lower(ra.artist_name) = ANY(%s)",
            (lower_names,),
        )

        matches: dict[str, int] = {}
        for lower_name, artist_id in rows:
            canonical = lower_to_canonical.get(lower_name)
            if canonical is not None and artist_id is not None:
                matches[canonical] = artist_id

        if not matches:
            return {}

        # 2. Fetch styles for matched artists
        matched_lower = [n.lower() for n in matches]
        style_rows = self._query_with_fallback(
            conn,
            "SELECT artist_name, style FROM artist_style_summary WHERE artist_name = ANY(%s)",
            "SELECT DISTINCT lower(ra.artist_name), rs.style "
            "FROM release_style rs "
            "JOIN release_artist ra ON rs.release_id = ra.release_id "
            "WHERE ra.extra = 0 AND lower(ra.artist_name) = ANY(%s)",
            (matched_lower,),
        )

        artist_styles: dict[str, list[str]] = {}
        for lower_name, style in style_rows:
            canonical = lower_to_canonical.get(lower_name)
            if canonical is not None:
                artist_styles.setdefault(canonical, []).append(style)

        # 3. Build result
        return {
            canonical: (discogs_id, artist_styles.get(canonical, []))
            for canonical, discogs_id in matches.items()
        }
