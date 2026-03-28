"""Discogs enrichment module for artist metadata aggregation.

Enriches canonical artists with Discogs metadata: styles, personnel credits,
labels, and compilation appearances. Uses the DiscogsClient to fetch release
data and aggregates across all releases per artist.
"""

import logging

from semantic_index.discogs_client import DiscogsClient
from semantic_index.models import (
    ArtistEnrichment,
    CompilationAppearance,
    DiscogsRelease,
    LabelInfo,
    PersonnelCredit,
)

logger = logging.getLogger(__name__)

_PROGRESS_INTERVAL = 500


class DiscogsEnricher:
    """Enriches canonical artists with Discogs metadata."""

    def __init__(self, client: DiscogsClient) -> None:
        self._client = client

    def enrich_artist(
        self, canonical_name: str, discogs_artist_id: int | None = None
    ) -> ArtistEnrichment | None:
        """Fetch and aggregate Discogs metadata for a canonical artist.

        1. Search for artist in Discogs (by name or ID)
        2. Get all releases for that artist
        3. Aggregate: styles (union), personnel (extra_artists with roles), labels, compilations

        Args:
            canonical_name: The canonical artist name to enrich.
            discogs_artist_id: Known Discogs artist ID, if available. Skips search when provided.

        Returns:
            ArtistEnrichment with aggregated metadata, or None if artist not found in Discogs.
        """
        # Step 1: Resolve Discogs identity
        if discogs_artist_id is None:
            search_result = self._client.search_artist(canonical_name)
            if search_result is None:
                return None
            discogs_artist_id = search_result.artist_id

        # Step 2: Get all release IDs for this artist
        release_ids = self._client.get_releases_for_artist(canonical_name)

        # Step 3: Fetch each release and aggregate
        all_styles: set[str] = set()
        personnel_map: dict[str, set[str]] = {}  # name -> set of roles
        labels_seen: dict[str, LabelInfo] = {}  # name -> LabelInfo
        compilations: list[CompilationAppearance] = []

        for release_id in release_ids:
            release = self._client.get_release(release_id)
            if release is None:
                continue
            self._aggregate_release(
                release, canonical_name, all_styles, personnel_map, labels_seen, compilations
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
            artists: Mapping of canonical_name to discogs_artist_id (or None if unknown).

        Returns:
            Dict of canonical_name to ArtistEnrichment (only for successfully enriched artists).
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

    @staticmethod
    def _aggregate_release(
        release: DiscogsRelease,
        canonical_name: str,
        all_styles: set[str],
        personnel_map: dict[str, set[str]],
        labels_seen: dict[str, LabelInfo],
        compilations: list[CompilationAppearance],
    ) -> None:
        """Aggregate data from a single release into the running accumulators."""
        # Styles
        all_styles.update(release.styles)

        # Personnel from extra_artists
        for credit in release.extra_artists:
            if credit.name not in personnel_map:
                personnel_map[credit.name] = set()
            if credit.role is not None:
                personnel_map[credit.name].add(credit.role)

        # Labels
        for label in release.labels:
            if label.name not in labels_seen:
                labels_seen[label.name] = LabelInfo(name=label.name, label_id=label.label_id)

        # Compilation detection: tracks with per-track artist credits
        has_track_artists = any(len(track.artists) > 0 for track in release.tracklist)
        if has_track_artists:
            other_artists: list[str] = []
            for track in release.tracklist:
                for artist in track.artists:
                    if artist != canonical_name and artist not in other_artists:
                        other_artists.append(artist)
            compilations.append(
                CompilationAppearance(
                    release_id=release.release_id,
                    release_title=release.title,
                    other_artists=other_artists,
                )
            )
