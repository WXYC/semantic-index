"""Data models for the semantic index pipeline."""

from pydantic import BaseModel


class FlowsheetEntry(BaseModel):
    """A music entry from FLOWSHEET_ENTRY_PROD (type code < 7)."""

    id: int
    artist_name: str
    song_title: str
    release_title: str
    library_release_id: int
    label_name: str
    show_id: int
    sequence: int
    entry_type_code: int
    request_flag: int = 0
    start_time: int | None = None


class LibraryRelease(BaseModel):
    """A row from LIBRARY_RELEASE — only the fields needed for Tier 1 resolution."""

    id: int
    library_code_id: int


class LibraryCode(BaseModel):
    """A row from LIBRARY_CODE — canonical artist name and genre."""

    id: int
    genre_id: int
    presentation_name: str


class ResolvedEntry(BaseModel):
    """A FlowsheetEntry after artist name resolution."""

    entry: FlowsheetEntry
    canonical_name: str
    resolution_method: str  # "catalog" or "raw"


class AdjacencyPair(BaseModel):
    """Two consecutive artists within a radio show."""

    source: str
    target: str
    show_id: int


class RadioShow(BaseModel):
    """A row from FLOWSHEET_RADIO_SHOW_PROD."""

    id: int
    dj_id: int | None = None
    dj_name: str = ""


class ArtistStats(BaseModel):
    """Aggregated statistics for a single artist."""

    canonical_name: str
    total_plays: int
    genre: str | None = None
    active_first_year: int | None = None
    active_last_year: int | None = None
    dj_count: int = 0
    request_ratio: float = 0.0
    show_count: int = 0


class PmiEdge(BaseModel):
    """A weighted edge between two artists."""

    source: str
    target: str
    raw_count: int
    pmi: float


class CrossReferenceEdge(BaseModel):
    """An explicit cross-reference edge from the library catalog."""

    artist_a: str  # canonical name
    artist_b: str  # canonical name
    comment: str
    source: str  # "library_code" or "release"


# --- Discogs models ---


class DiscogsCredit(BaseModel):
    """A personnel credit on a Discogs release."""

    name: str
    artist_id: int | None = None
    role: str | None = None


class DiscogsLabel(BaseModel):
    """A label credit on a Discogs release."""

    name: str
    label_id: int | None = None
    catno: str | None = None


class DiscogsTrack(BaseModel):
    """A track on a Discogs release (for compilations)."""

    position: str
    title: str
    artists: list[str] = []


class DiscogsRelease(BaseModel):
    """Discogs release metadata for enrichment."""

    release_id: int
    title: str
    artist_name: str
    artist_id: int | None = None
    year: int | None = None
    styles: list[str] = []
    artists: list[DiscogsCredit] = []
    extra_artists: list[DiscogsCredit] = []
    labels: list[DiscogsLabel] = []
    tracklist: list[DiscogsTrack] = []


class DiscogsSearchResult(BaseModel):
    """A search result from Discogs."""

    artist_name: str
    artist_id: int | None = None
    release_id: int | None = None
    confidence: float = 0.0


# --- Enrichment models ---


class PersonnelCredit(BaseModel):
    """A credited musician across an artist's releases."""

    name: str
    roles: list[str] = []


class LabelInfo(BaseModel):
    """A label an artist has released on."""

    name: str
    label_id: int | None = None


class CompilationAppearance(BaseModel):
    """An artist's appearance on a compilation release."""

    release_id: int
    release_title: str
    other_artists: list[str] = []


class ArtistEnrichment(BaseModel):
    """Discogs enrichment data for a single canonical artist."""

    canonical_name: str
    discogs_artist_id: int | None = None
    styles: list[str] = []
    personnel: list[PersonnelCredit] = []
    labels: list[LabelInfo] = []
    compilation_appearances: list[CompilationAppearance] = []
