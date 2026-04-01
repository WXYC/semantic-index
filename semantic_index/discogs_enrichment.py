"""Discogs enrichment module for artist metadata aggregation.

Enriches canonical artists with Discogs metadata: styles, personnel credits,
labels, and compilation appearances. Uses bulk PostgreSQL queries to fetch
all enrichment data in a handful of large queries instead of per-artist round-trips.
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


class DiscogsEnricher:
    """Enriches canonical artists with Discogs metadata."""

    def __init__(self, client: DiscogsClient) -> None:
        self._client = client

    def enrich_artist(
        self, canonical_name: str, discogs_artist_id: int | None = None
    ) -> ArtistEnrichment | None:
        """Fetch and aggregate Discogs metadata for a single artist.

        For batch enrichment, prefer enrich_batch() which is much faster.
        """
        results = self.enrich_batch({canonical_name: discogs_artist_id})
        return results.get(canonical_name)

    def enrich_batch(self, artists: dict[str, int | None]) -> dict[str, ArtistEnrichment]:
        """Enrich all artists in a single bulk operation.

        Fetches all enrichment data across all artists in ~5 large PostgreSQL
        queries (instead of N*6 per-artist queries). Then groups and aggregates
        in Python.

        Args:
            artists: Mapping of canonical_name to discogs_artist_id (or None).

        Returns:
            Dict of canonical_name to ArtistEnrichment.
        """
        artist_names = list(artists.keys())
        logger.info("Starting bulk enrichment for %d artists...", len(artist_names))

        # One bulk call that fetches everything
        bulk_data = self._client.get_bulk_enrichment(artist_names)

        # Build ArtistEnrichment for each artist that had data
        results: dict[str, ArtistEnrichment] = {}
        for name, data in bulk_data.items():
            # Styles
            all_styles = sorted(set(data["styles"]))

            # Personnel (deduplicate by name, merge roles)
            personnel_map: dict[str, set[str]] = {}
            for credit_name, role in data["extra_artists"]:
                if credit_name not in personnel_map:
                    personnel_map[credit_name] = set()
                if role:
                    personnel_map[credit_name].add(role)

            # Labels (deduplicate by name)
            labels_seen: dict[str, LabelInfo] = {}
            for label_id, label_name in data["labels"]:
                if label_name not in labels_seen:
                    labels_seen[label_name] = LabelInfo(name=label_name, label_id=label_id)

            # Compilations (group track artists by release)
            tracks_by_release: dict[int, list[str]] = defaultdict(list)
            for rid, artist in data["track_artists"]:
                if artist != name:
                    tracks_by_release[rid].append(artist)
            compilations = [
                CompilationAppearance(
                    release_id=rid,
                    release_title="",
                    other_artists=sorted(set(comp_artists)),
                )
                for rid, comp_artists in tracks_by_release.items()
                if comp_artists
            ]

            results[name] = ArtistEnrichment(
                canonical_name=name,
                discogs_artist_id=artists.get(name),
                styles=all_styles,
                personnel=[
                    PersonnelCredit(name=pname, roles=sorted(roles))
                    for pname, roles in sorted(personnel_map.items())
                ],
                labels=[labels_seen[lname] for lname in sorted(labels_seen)],
                compilation_appearances=compilations,
            )

        logger.info("Enrichment complete: %d / %d artists enriched", len(results), len(artists))
        return results
