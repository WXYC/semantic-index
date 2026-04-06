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


# --- Discogs-derived edge models ---


class SharedPersonnelEdge(BaseModel):
    """Edge between two artists who share credited musicians."""

    artist_a: str
    artist_b: str
    shared_count: int
    shared_names: list[str]


class SharedStyleEdge(BaseModel):
    """Edge between two artists with overlapping Discogs style tags."""

    artist_a: str
    artist_b: str
    jaccard: float
    shared_tags: list[str]


class LabelFamilyEdge(BaseModel):
    """Edge between two artists who share a record label."""

    artist_a: str
    artist_b: str
    shared_labels: list[str]


class CompilationEdge(BaseModel):
    """Edge between two artists who appear on the same compilation."""

    artist_a: str
    artist_b: str
    compilation_count: int
    compilation_titles: list[str]


# --- Entity store models ---


class Entity(BaseModel):
    """A real-world person, group, or organization in the entity store."""

    id: int
    wikidata_qid: str | None = None
    name: str
    entity_type: str = "artist"


class ReconciliationEvent(BaseModel):
    """A single reconciliation lookup result from an external knowledge base."""

    source: str  # 'discogs', 'musicbrainz', 'wikidata'
    external_id: str
    confidence: float | None = None
    method: str  # 'exact', 'fuzzy', 'api_search', 'cache_lookup'


class ReconciliationReport(BaseModel):
    """Summary of a reconciliation batch run."""

    total: int  # Total artists in the input set
    attempted: int  # Artists where reconciliation was attempted (status was 'unreconciled')
    succeeded: int  # Attempts that found at least one external match
    no_match: int  # Attempts where the lookup returned no result
    errored: int  # Attempts that failed due to an exception
    skipped: int  # Artists already in 'partial' or 'reconciled' status


# --- Wikidata models ---


class WikidataEntity(BaseModel):
    """A Wikidata entity with optional Discogs artist ID.

    Returned by SPARQL lookups (P1953 Discogs ID) and name search
    (wbsearchentities API).
    """

    qid: str  # e.g. "Q2774"
    name: str  # rdfs:label
    description: str | None = None
    discogs_artist_id: int | None = None  # P1953


class WikidataStreamingIds(BaseModel):
    """Streaming service IDs from Wikidata for a single entity.

    Fetched via SPARQL OPTIONAL queries for P1902 (Spotify artist ID),
    P2850 (Apple Music artist ID), and P3283 (Bandcamp profile ID).
    """

    qid: str
    spotify_artist_id: str | None = None  # P1902
    apple_music_artist_id: str | None = None  # P2850
    bandcamp_id: str | None = None  # P3283


class WikidataInfluence(BaseModel):
    """An influence relationship (P737) between two Wikidata entities.

    Represents "source is influenced by target".
    """

    source_qid: str
    target_qid: str
    target_name: str


class WikidataInfluenceEdge(BaseModel):
    """A directed influence edge between two artists from Wikidata P737.

    Represents "source_artist is influenced by target_artist".
    Both artists must exist in the graph (resolved via entity store QIDs).
    """

    source_artist: str  # influenced artist (canonical name)
    target_artist: str  # influence source (canonical name)
    source_qid: str  # Wikidata QID of the influenced artist
    target_qid: str  # Wikidata QID of the influence


class WikidataLabelHierarchy(BaseModel):
    """A parent-child label relationship (P749 parent org / P355 subsidiary)."""

    parent_qid: str
    parent_name: str
    child_qid: str
    child_name: str


class LabelHierarchyReport(BaseModel):
    """Summary of a label hierarchy population run."""

    labels_created: int  # Unique labels inserted into the label table
    labels_matched: int  # Labels matched to Wikidata QIDs
    hierarchy_edges: int  # Parent-child relationships inserted


class DeduplicationReport(BaseModel):
    """Summary of an entity deduplication run."""

    groups_found: int  # QIDs with 2+ entities
    entities_merged: int  # Total entities merged (deleted)
    artists_reassigned: int  # Total artist rows re-parented
