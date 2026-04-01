"""Response models for the Graph API."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class ArtistSummary(BaseModel):
    """Minimal artist representation for search results and edge endpoints."""

    id: int
    canonical_name: str
    genre: str | None
    total_plays: int


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
    """Full artist detail including external IDs from joined entity table."""

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


class EntityArtists(BaseModel):
    """Response for GET /graph/entities/{id}/artists — all artists sharing an entity."""

    entity_id: int
    entity_name: str
    wikidata_qid: str | None = None
    artists: list[ArtistSummary]
