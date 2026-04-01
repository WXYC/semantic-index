"""Discogs enrichment module for artist metadata aggregation.

Enriches canonical artists with Discogs metadata: styles, personnel credits,
labels, and compilation appearances. Uses bulk queries against the Discogs cache
to minimize database round-trips.
"""

import logging
from collections import defaultdict

from semantic_index.discogs_client import DiscogsClient
from semantic_index.models import (
    ArtistEnrichment,
    CompilationAppearance,
    LabelInfo,
    PersonnelCredit,
)

logger = logging.getLogger(__name__)

_PROGRESS_INTERVAL = 500

# Maximum release IDs per bulk query to avoid excessive IN-clause sizes
_BATCH_SIZE = 5000


class DiscogsEnricher:
    """Enriches canonical artists with Discogs metadata."""

    def __init__(self, client: DiscogsClient) -> None:
        self._client = client

    def enrich_artist(
        self, canonical_name: str, discogs_artist_id: int | None = None
    ) -> ArtistEnrichment | None:
        """Fetch and aggregate Discogs metadata for a canonical artist.

        Uses bulk queries: gets all release IDs first, then fetches styles,
        personnel, labels, and compilation data in 4 queries total.

        Args:
            canonical_name: The canonical artist name to enrich.
            discogs_artist_id: Known Discogs artist ID, if available.

        Returns:
            ArtistEnrichment with aggregated metadata, or None if artist not found.
        """
        # Step 1: Resolve Discogs identity
        if discogs_artist_id is None:
            search_result = self._client.search_artist(canonical_name)
            if search_result is None:
                return None
            discogs_artist_id = search_result.artist_id

        # Step 2: Get all release IDs for this artist
        release_ids = self._client.get_releases_for_artist(canonical_name)
        if not release_ids:
            return ArtistEnrichment(
                canonical_name=canonical_name,
                discogs_artist_id=discogs_artist_id,
            )

        # Step 3: Fetch all enrichment data in bulk (4 queries, not N*5)
        all_styles: set[str] = set()
        personnel_map: dict[str, set[str]] = {}
        labels_seen: dict[str, LabelInfo] = {}
        compilations: list[CompilationAppearance] = []

        # Process in batches to avoid huge IN clauses
        for batch_start in range(0, len(release_ids), _BATCH_SIZE):
            batch_ids = release_ids[batch_start : batch_start + _BATCH_SIZE]
            data = self._client.get_enrichment_for_artist(canonical_name, batch_ids)

            # Styles
            all_styles.update(data["styles"])

            # Personnel (extra artists with roles)
            for name, role in data["extra_artists"]:
                if name not in personnel_map:
                    personnel_map[name] = set()
                if role:
                    personnel_map[name].add(role)

            # Labels
            for label_id, label_name in data["labels"]:
                if label_name not in labels_seen:
                    labels_seen[label_name] = LabelInfo(name=label_name, label_id=label_id)

            # Compilation detection: group track artists by release
            tracks_by_release: dict[int, list[str]] = defaultdict(list)
            for rid, artist in data["track_artists"]:
                if artist != canonical_name:
                    tracks_by_release[rid].append(artist)
            for rid, artists in tracks_by_release.items():
                if artists:
                    compilations.append(
                        CompilationAppearance(
                            release_id=rid,
                            release_title="",  # Title not fetched in bulk
                            other_artists=sorted(set(artists)),
                        )
                    )

        return ArtistEnrichment(
            canonical_name=canonical_name,
            discogs_artist_id=discogs_artist_id,
            styles=sorted(all_styles),
            personnel=[
                PersonnelCredit(name=name, roles=sorted(roles))
                for name, roles in sorted(personnel_map.items())
            ],
            labels=[labels_seen[name] for name in sorted(labels_seen)],
            compilation_appearances=compilations,
        )

    def enrich_batch(self, artists: dict[str, int | None]) -> dict[str, ArtistEnrichment]:
        """Enrich a batch of artists.

        Args:
            artists: Mapping of canonical_name to discogs_artist_id (or None).

        Returns:
            Dict of canonical_name to ArtistEnrichment (only for successful enrichments).
        """
        results: dict[str, ArtistEnrichment] = {}
        total = len(artists)

        for i, (name, artist_id) in enumerate(artists.items(), start=1):
            enrichment = self.enrich_artist(name, discogs_artist_id=artist_id)
            if enrichment is not None:
                results[name] = enrichment

            if i % _PROGRESS_INTERVAL == 0:
                logger.info("Enriched %d / %d artists (%d successful)", i, total, len(results))

        logger.info(
            "Enrichment complete: %d / %d artists enriched successfully", len(results), total
        )
        return results
