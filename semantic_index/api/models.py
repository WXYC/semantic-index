"""API response models for the Graph API."""

from pydantic import BaseModel


class ArtistResponse(BaseModel):
    """An artist node from the graph database.

    Attributes:
        id: SQLite row ID.
        canonical_name: Resolved artist name.
        genre: Genre classification from the library catalog, if available.
        total_plays: Total flowsheet appearances.
        active_first_year: First year the artist appeared on flowsheets.
        active_last_year: Most recent year the artist appeared on flowsheets.
        dj_count: Number of distinct DJs who played this artist.
        request_ratio: Fraction of plays that were listener requests.
        show_count: Number of distinct shows featuring this artist.
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


class NeighborResponse(BaseModel):
    """A neighbor in the DJ transition graph.

    Attributes:
        artist: The neighboring artist.
        raw_count: Number of DJ transitions between the two artists.
        pmi: Pointwise Mutual Information score for the pair.
    """

    artist: ArtistResponse
    raw_count: int
    pmi: float


class SearchResult(BaseModel):
    """A search result from the artist table.

    Attributes:
        id: SQLite row ID.
        canonical_name: Resolved artist name.
        genre: Genre classification, if available.
    """

    id: int
    canonical_name: str
    genre: str | None = None


class ExplainResponse(BaseModel):
    """Explanation of the relationship between two artists.

    Attributes:
        source: Source artist name.
        target: Target artist name.
        edges: List of edge descriptions connecting the two artists.
    """

    source: str
    target: str
    edges: list[dict[str, object]]
