"""Reconciliation module: bulk Discogs and Wikidata matching for artists.

Queries the entity store for artists with ``reconciliation_status='unreconciled'``,
matches them against the discogs-cache PostgreSQL ``release_artist`` table in
batches, persists per-artist styles from ``release_style``, and updates each
artist's ``discogs_artist_id`` and reconciliation status.

Also provides Wikidata reconciliation by Discogs artist ID: for artists with
a ``discogs_artist_id``, queries Wikidata for matching P1953 entities and
populates ``entity.wikidata_qid``.

Usage::

    from semantic_index.discogs_client import DiscogsClient
    from semantic_index.entity_store import EntityStore
    from semantic_index.reconciliation import ArtistReconciler
    from semantic_index.wikidata_client import WikidataClient

    store = EntityStore("output/wxyc_artist_graph.db")
    store.initialize()
    discogs = DiscogsClient(cache_dsn="postgresql://...", api_base_url=None)
    wikidata = WikidataClient()
    reconciler = ArtistReconciler(store, discogs, wikidata_client=wikidata)
    report = reconciler.reconcile_batch(batch_size=1000)
    wikidata_report = reconciler.reconcile_wikidata()
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from wxyc_etl.schema import (  # type: ignore[import-untyped]
    RELEASE_ARTIST_TABLE,
    RELEASE_STYLE_TABLE,
)

from semantic_index.discogs_client import DiscogsClient
from semantic_index.entity_store import EntityStore
from semantic_index.models import ReconciliationReport
from semantic_index.wikidata_client import WikidataClient

if TYPE_CHECKING:
    from semantic_index.wikidata_client import WikidataClient

logger = logging.getLogger(__name__)


class ArtistReconciler:
    """Bulk Discogs and Wikidata matching for artists.

    Args:
        store: Entity store for reading/writing artist data.
        client: Discogs client whose cache connection is used for bulk lookups.
        wikidata_client: Optional Wikidata client for P1953 lookups.
    """

    def __init__(
        self,
        store: EntityStore,
        client: DiscogsClient,
        wikidata_client: WikidataClient | None = None,
    ) -> None:
        self._store = store
        self._client = client
        self._wikidata_client = wikidata_client

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

    def reconcile_members(self, batch_size: int = 1000) -> ReconciliationReport:
        """Re-try no_match artists against the Discogs ``artist_member`` table.

        Queries in both directions: WXYC artist name as a Discogs group name
        (has members), and as a Discogs member name. Only artists with
        ``reconciliation_status='no_match'`` are attempted.

        Args:
            batch_size: Number of artist names per bulk Discogs query.

        Returns:
            ReconciliationReport with counts.
        """
        total = self._store._conn.execute("SELECT COUNT(*) FROM artist").fetchone()[0]
        no_match_artists = self._store.get_no_match_artists()
        skipped = total - len(no_match_artists)

        succeeded = 0
        no_match = 0
        errored = 0

        name_to_id = {name: aid for aid, name in no_match_artists}

        for batch_start in range(0, len(no_match_artists), batch_size):
            batch = no_match_artists[batch_start : batch_start + batch_size]
            batch_names = [name for _, name in batch]

            try:
                matches = self._reconcile_member_bulk(batch_names)
            except Exception:
                logger.warning("Member reconciliation failed for batch", exc_info=True)
                errored += len(batch_names)
                continue

            for name in batch_names:
                artist_id = name_to_id[name]
                if name in matches:
                    discogs_id, styles, method = matches[name]
                    self._store.upsert_artist(name, discogs_artist_id=discogs_id)
                    if styles:
                        self._store.persist_artist_styles(artist_id, styles)
                    self._store.log_reconciliation(
                        artist_id=artist_id,
                        source="discogs",
                        external_id=str(discogs_id),
                        confidence=None,
                        method=method,
                    )
                    self._store.update_reconciliation_status(artist_id, "reconciled")
                    succeeded += 1
                else:
                    no_match += 1

        attempted = succeeded + no_match + errored
        logger.info(
            "Member reconciliation complete: %d attempted, %d succeeded, %d no_match, %d errored, %d skipped",
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

    def reconcile_wikidata(self, wikidata_client: WikidataClient) -> ReconciliationReport:
        """Search Wikidata by name for artists with no Discogs match.

        For each ``no_match`` artist, searches Wikidata for musician entities
        matching the artist name. The top result (if any) is linked via the
        entity store and logged as a ``wikidata`` / ``name_search`` reconciliation.

        Args:
            wikidata_client: WikidataClient instance for name search.

        Returns:
            ReconciliationReport with counts.
        """
        total = self._store._conn.execute("SELECT COUNT(*) FROM artist").fetchone()[0]
        no_match_artists = self._store.get_no_match_artists()
        skipped = total - len(no_match_artists)

        succeeded = 0
        no_match = 0
        errored = 0

        for artist_id, name in no_match_artists:
            try:
                results = wikidata_client.search_musician_by_name(name, limit=5)
            except Exception:
                logger.warning(
                    "Wikidata name search failed for artist %d (%s)", artist_id, name, exc_info=True
                )
                errored += 1
                continue

            if not results:
                no_match += 1
                continue

            best = results[0]

            # Reuse existing entity if one already has this QID
            entity = self._store.get_entity_by_qid(best.qid)
            if entity is None:
                entity = self._store.get_or_create_entity(
                    name=best.name,
                    entity_type="artist",
                    wikidata_qid=best.qid,
                )

            self._store.upsert_artist(name, entity_id=entity.id)
            self._store.log_reconciliation(
                artist_id=artist_id,
                source="wikidata",
                external_id=best.qid,
                confidence=None,
                method="name_search",
            )
            self._store.update_reconciliation_status(artist_id, "reconciled")
            succeeded += 1

        attempted = succeeded + no_match + errored
        logger.info(
            "Wikidata reconciliation complete: %d attempted, %d succeeded, %d no_match, %d errored, %d skipped",
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

    def reconcile_streaming_ids(self, wikidata_client: WikidataClient) -> int:
        """Fetch streaming service IDs from Wikidata for entities that need them.

        Queries Wikidata P1902 (Spotify), P2850 (Apple Music), and P3283
        (Bandcamp) for entities that have a Wikidata QID but no streaming
        IDs yet.

        Args:
            wikidata_client: WikidataClient instance for SPARQL queries.

        Returns:
            Number of entities updated with at least one streaming ID.
        """
        entities = self._store.get_entities_needing_streaming_ids()
        if not entities:
            logger.info("No entities need streaming IDs")
            return 0

        qid_to_entity_id = {qid: eid for eid, qid in entities}
        qids = list(qid_to_entity_id.keys())
        logger.info("Looking up streaming IDs for %d entities", len(qids))

        streaming_ids = wikidata_client.lookup_streaming_ids(qids)
        updated = 0
        for qid, ids in streaming_ids.items():
            entity_id = qid_to_entity_id.get(qid)
            if entity_id is None:
                continue
            self._store.update_entity_streaming_ids(
                entity_id,
                spotify=ids.spotify_artist_id,
                apple_music=ids.apple_music_artist_id,
                bandcamp=ids.bandcamp_id,
            )
            updated += 1

        logger.info("Updated streaming IDs for %d/%d entities", updated, len(entities))
        return updated

    @staticmethod
    def _query_with_fallback(
        conn: object, primary_sql: str, fallback_sql: str, params: tuple
    ) -> list:
        """Try a query against a materialized summary table, fall back to join."""
        execute = conn.execute  # type: ignore[attr-defined]
        try:
            result = execute(primary_sql, params).fetchall()
            if result:
                return result  # type: ignore[no-any-return]
            return execute(fallback_sql, params).fetchall()  # type: ignore[no-any-return]
        except Exception:
            return execute(fallback_sql, params).fetchall()  # type: ignore[no-any-return]

    def _reconcile_member_bulk(self, names: list[str]) -> dict[str, tuple[int, list[str], str]]:
        """Batch lookup against the Discogs ``artist_member`` table in both directions.

        Direction 1 (group): WXYC artist name matches a Discogs group name
        that has members in ``artist_member``. Returns the group's ``artist.id``.

        Direction 2 (member): WXYC artist name matches ``artist_member.member_name``.
        Returns the member's ``member_id`` (Discogs artist ID).

        Group matches take priority when a name matches in both directions.

        Args:
            names: List of canonical artist names to look up.

        Returns:
            Dict mapping canonical_name to ``(discogs_artist_id, [styles], method)``.
            Method is ``"member_group"`` or ``"member_name"``.
        """
        if not names:
            return {}

        conn = self._client._get_cache_conn()
        if conn is None:
            return {}

        lower_to_canonical: dict[str, str] = {n.lower(): n for n in names}
        lower_names = list(lower_to_canonical.keys())

        matches: dict[str, tuple[int, str]] = {}  # canonical -> (discogs_id, method)

        # Direction 1: name matches a group with members
        group_rows = conn.execute(
            "SELECT lower(a.name), a.id FROM artist a "
            "WHERE lower(a.name) = ANY(%s) "
            "AND EXISTS (SELECT 1 FROM artist_member am WHERE am.artist_id = a.id)",
            (lower_names,),
        ).fetchall()

        for lower_name, artist_id in group_rows:
            canonical = lower_to_canonical.get(lower_name)
            if canonical is not None and artist_id is not None:
                matches[canonical] = (artist_id, "member_group")

        # Direction 2: name matches a member name (skip already-matched)
        remaining_lower = [n for n in lower_names if lower_to_canonical.get(n) not in matches]
        if remaining_lower:
            member_rows = conn.execute(
                "SELECT DISTINCT lower(am.member_name), am.member_id "
                "FROM artist_member am "
                "WHERE lower(am.member_name) = ANY(%s)",
                (remaining_lower,),
            ).fetchall()

            for lower_name, member_id in member_rows:
                canonical = lower_to_canonical.get(lower_name)
                if canonical is not None and member_id is not None and canonical not in matches:
                    matches[canonical] = (member_id, "member_name")

        if not matches:
            return {}

        # Fetch styles by discogs_artist_id
        matched_ids = list({did for did, _ in matches.values()})
        style_rows = conn.execute(
            f"SELECT DISTINCT ra.artist_id, rs.style "  # noqa: S608
            f"FROM {RELEASE_STYLE_TABLE} rs "
            f"JOIN {RELEASE_ARTIST_TABLE} ra ON rs.release_id = ra.release_id "
            f"WHERE ra.extra = 0 AND ra.artist_id = ANY(%s)",
            (matched_ids,),
        ).fetchall()

        id_styles: dict[int, list[str]] = {}
        for artist_id, style in style_rows:
            id_styles.setdefault(artist_id, []).append(style)

        return {
            canonical: (discogs_id, id_styles.get(discogs_id, []), method)
            for canonical, (discogs_id, method) in matches.items()
        }

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
            f"SELECT DISTINCT lower(ra.artist_name), ra.artist_id "  # noqa: S608
            f"FROM {RELEASE_ARTIST_TABLE} ra "
            f"WHERE ra.extra = 0 AND lower(ra.artist_name) = ANY(%s)",
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
            f"SELECT DISTINCT lower(ra.artist_name), rs.style "  # noqa: S608
            f"FROM {RELEASE_STYLE_TABLE} rs "
            f"JOIN {RELEASE_ARTIST_TABLE} ra ON rs.release_id = ra.release_id "
            f"WHERE ra.extra = 0 AND lower(ra.artist_name) = ANY(%s)",
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

    def _reconcile_discogs_aliases(self, names: list[str]) -> dict[str, tuple[int, list[str]]]:
        """Batch Discogs alias lookup via ``artist_alias`` and ``artist_name_variation``.

        Queries the discogs-cache PostgreSQL using ``ANY()`` for efficient
        bulk matching against alias names and name variations, then fetches
        per-artist styles from ``release_style``.

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

        # 1. Try artist_alias table
        alias_rows = conn.execute(
            "SELECT lower(alias_name), artist_id FROM artist_alias WHERE lower(alias_name) = ANY(%s)",
            (lower_names,),
        ).fetchall()

        matches: dict[str, int] = {}
        for lower_name, artist_id in alias_rows:
            canonical = lower_to_canonical.get(lower_name)
            if canonical is not None and artist_id is not None:
                matches[canonical] = artist_id

        # 2. Try artist_name_variation for remaining unmatched names
        remaining = [n for n in lower_names if lower_to_canonical.get(n) not in matches]
        if remaining:
            variation_rows = conn.execute(
                "SELECT lower(name), artist_id "
                "FROM artist_name_variation WHERE lower(name) = ANY(%s)",
                (remaining,),
            ).fetchall()

            for lower_name, artist_id in variation_rows:
                canonical = lower_to_canonical.get(lower_name)
                if canonical is not None and artist_id is not None:
                    matches[canonical] = artist_id

        if not matches:
            return {}

        # 3. Fetch styles by artist_id for matched artists
        matched_ids = list(set(matches.values()))
        style_rows = conn.execute(
            f"SELECT DISTINCT ra.artist_id, rs.style "  # noqa: S608
            f"FROM {RELEASE_STYLE_TABLE} rs "
            f"JOIN {RELEASE_ARTIST_TABLE} ra ON rs.release_id = ra.release_id "
            f"WHERE ra.extra = 0 AND ra.artist_id = ANY(%s)",
            (matched_ids,),
        ).fetchall()

        id_styles: dict[int, list[str]] = {}
        for artist_id, style in style_rows:
            id_styles.setdefault(artist_id, []).append(style)

        # 4. Build result
        return {
            canonical: (discogs_id, id_styles.get(discogs_id, []))
            for canonical, discogs_id in matches.items()
        }
