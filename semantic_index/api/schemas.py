"""Response models for the Graph API."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from generated.api_models import ReconciledIdentity


class ArtistSummary(BaseModel):
    """Minimal artist representation for search results and edge endpoints."""

    id: int
    canonical_name: str
    genre: str | None
    total_plays: int
    community_id: int | None = None
    pagerank: float | None = None


class SearchResponse(BaseModel):
    """Response for GET /graph/artists/search."""

    results: list[ArtistSummary]


class NeighborEntry(BaseModel):
    """A single neighbor with edge weight and type-specific detail."""

    artist: ArtistSummary
    weight: float
    detail: dict[str, Any]


class NeighborsResponse(BaseModel):
    """Response for GET /graph/artists/{id}/neighbors."""

    artist: ArtistSummary
    edge_type: str
    neighbors: list[NeighborEntry]


class BatchNeighborsResponse(BaseModel):
    """Response for POST /graph/artists/neighbors/batch."""

    results: dict[str, NeighborsResponse]


class Relationship(BaseModel):
    """A single relationship type between two artists."""

    type: str
    weight: float
    detail: dict[str, Any]


class ExplainResponse(BaseModel):
    """Response for GET /graph/artists/{id}/explain/{target_id}."""

    source: ArtistSummary
    target: ArtistSummary
    relationships: list[Relationship]


class ArtistDetail(BaseModel):
    """Full artist detail including external IDs from joined entity table.

    The flat external-ID fields (discogs_artist_id, musicbrainz_artist_id, …)
    are kept for backward compatibility with the existing graph explorer
    frontend. New consumers should read the nested `reconciled_identity` field,
    which conforms to the shared `@wxyc/shared` `ReconciledIdentity` schema.
    """

    id: int
    canonical_name: str
    genre: str | None = None
    total_plays: int = 0
    active_first_year: int | None = None
    active_last_year: int | None = None
    dj_count: int = 0
    request_ratio: float = 0.0
    show_count: int = 0
    entity_id: int | None = None
    discogs_artist_id: int | None = None
    musicbrainz_artist_id: str | None = None
    wikidata_qid: str | None = None
    reconciliation_status: str = "unreconciled"
    spotify_artist_id: str | None = None
    apple_music_artist_id: str | None = None
    bandcamp_id: str | None = None
    reconciled_identity: ReconciledIdentity | None = None


class EntityArtists(BaseModel):
    """Response for GET /graph/entities/{id}/artists — all artists sharing an entity."""

    entity_id: int
    entity_name: str
    wikidata_qid: str | None = None
    artists: list[ArtistSummary]


class NarrativeResponse(BaseModel):
    """Response for GET /graph/artists/{id}/explain/{target_id}/narrative.

    ``insufficient_signal`` is ``True`` when the pair's total Adamic-Adar
    contribution from shared neighbors falls below ``NARRATIVE_MIN_AA_SCORE``
    (default 0.8). The endpoint short-circuits the LLM call and returns a
    deterministic placeholder ``narrative`` for these pairs — DJs do play
    them together, but with no consistent neighborhood context to narrate.
    Clients can render these differently (muted card, hint, etc.).
    """

    source: ArtistSummary
    target: ArtistSummary
    narrative: str
    cached: bool
    insufficient_signal: bool = False


class BioResponse(BaseModel):
    """Response for GET /graph/artists/{id}/bio."""

    artist_id: int
    bio: str
    source: str  # 'wikipedia', 'discogs', 'wikidata', 'generated'
    cached: bool


class BandcampAlbumResponse(BaseModel):
    """Response for GET /graph/bandcamp/{slug}/album."""

    bandcamp_id: str
    album_id: str | None = None
    album_title: str | None = None
    cached: bool


class AudioProfileResponse(BaseModel):
    """Response for GET /graph/artists/{id}/audio."""

    artist_id: int
    avg_danceability: float | None = None
    primary_genre: str | None = None
    primary_genre_probability: float | None = None
    voice_instrumental_ratio: float | None = None
    recording_count: int = 0
    feature_centroid: list[float] | None = None


class DjSummary(BaseModel):
    """Minimal DJ representation for facet dropdowns."""

    id: int
    display_name: str


class FacetsResponse(BaseModel):
    """Response for GET /graph/facets — available facet values for filtering."""

    months: list[int]
    djs: list[DjSummary]


class CommunityDetail(BaseModel):
    """A single Louvain community with metadata."""

    id: int
    size: int
    label: str | None = None
    top_genres: list[list] | None = None
    top_artists: list[str] | None = None


class CommunitiesResponse(BaseModel):
    """Response for GET /graph/communities."""

    communities: list[CommunityDetail]
    total_artists: int = 0


class PreviewResponse(BaseModel):
    """Response for GET /graph/artists/{id}/preview.

    Returns a preview audio URL for an artist, sourced from iTunes, Spotify,
    Bandcamp, or Deezer with multi-source fallback. Cached in a sidecar database.
    """

    artist_id: int
    preview_url: str | None = None
    track_name: str | None = None
    artist_name: str | None = None
    artwork_url: str | None = None
    source: str  # 'itunes_lookup' | 'spotify' | 'bandcamp' | 'deezer' | 'itunes_search' | 'none'
    cached: bool
