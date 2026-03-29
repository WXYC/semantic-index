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
