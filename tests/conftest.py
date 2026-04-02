"""Shared test fixtures and factory functions.

Uses WXYC example artists from the canonical data in wxyc-shared.
"""

from semantic_index.models import (
    AdjacencyPair,
    ArtistEnrichment,
    CompilationAppearance,
    CrossReferenceEdge,
    DiscogsCredit,
    DiscogsLabel,
    DiscogsRelease,
    DiscogsTrack,
    Entity,
    FlowsheetEntry,
    LabelInfo,
    LibraryCode,
    LibraryRelease,
    PersonnelCredit,
    ResolvedEntry,
    WikidataEntity,
    WikidataInfluence,
    WikidataLabelHierarchy,
)


def make_flowsheet_entry(
    id: int = 1,
    artist_name: str = "Autechre",
    song_title: str = "VI Scose Poise",
    release_title: str = "Confield",
    library_release_id: int = 100,
    label_name: str = "Warp",
    show_id: int = 1,
    sequence: int = 1,
    entry_type_code: int = 1,
    request_flag: int = 0,
    start_time: int | None = None,
) -> FlowsheetEntry:
    return FlowsheetEntry(
        id=id,
        artist_name=artist_name,
        song_title=song_title,
        release_title=release_title,
        library_release_id=library_release_id,
        label_name=label_name,
        show_id=show_id,
        sequence=sequence,
        entry_type_code=entry_type_code,
        request_flag=request_flag,
        start_time=start_time,
    )


def make_library_release(
    id: int = 100,
    library_code_id: int = 200,
) -> LibraryRelease:
    return LibraryRelease(id=id, library_code_id=library_code_id)


def make_library_code(
    id: int = 200,
    genre_id: int = 15,  # Electronic
    presentation_name: str = "Autechre",
) -> LibraryCode:
    return LibraryCode(id=id, genre_id=genre_id, presentation_name=presentation_name)


def make_resolved_entry(
    canonical_name: str = "Autechre",
    resolution_method: str = "catalog",
    **entry_kwargs,
) -> ResolvedEntry:
    return ResolvedEntry(
        entry=make_flowsheet_entry(**entry_kwargs),
        canonical_name=canonical_name,
        resolution_method=resolution_method,
    )


def make_adjacency_pair(
    source: str = "Autechre",
    target: str = "Stereolab",
    show_id: int = 1,
) -> AdjacencyPair:
    return AdjacencyPair(source=source, target=target, show_id=show_id)


def make_cross_reference_edge(
    artist_a: str = "Autechre",
    artist_b: str = "Stereolab",
    comment: str = "See also",
    source: str = "library_code",
) -> CrossReferenceEdge:
    return CrossReferenceEdge(
        artist_a=artist_a,
        artist_b=artist_b,
        comment=comment,
        source=source,
    )


def make_discogs_release(
    release_id: int = 12345,
    title: str = "Confield",
    artist_name: str = "Autechre",
    artist_id: int | None = 42,
    year: int | None = 2001,
    styles: list[str] | None = None,
    artists: list[DiscogsCredit] | None = None,
    extra_artists: list[DiscogsCredit] | None = None,
    labels: list[DiscogsLabel] | None = None,
    tracklist: list[DiscogsTrack] | None = None,
) -> DiscogsRelease:
    return DiscogsRelease(
        release_id=release_id,
        title=title,
        artist_name=artist_name,
        artist_id=artist_id,
        year=year,
        styles=styles if styles is not None else ["IDM", "Abstract"],
        artists=artists
        if artists is not None
        else [DiscogsCredit(name=artist_name, artist_id=artist_id)],
        extra_artists=extra_artists if extra_artists is not None else [],
        labels=labels
        if labels is not None
        else [DiscogsLabel(name="Warp Records", label_id=100, catno="WARPCD85")],
        tracklist=tracklist
        if tracklist is not None
        else [DiscogsTrack(position="1", title="VI Scose Poise")],
    )


def make_artist_enrichment(
    canonical_name: str = "Autechre",
    discogs_artist_id: int | None = 42,
    styles: list[str] | None = None,
    personnel: list[PersonnelCredit] | None = None,
    labels: list[LabelInfo] | None = None,
    compilation_appearances: list[CompilationAppearance] | None = None,
) -> ArtistEnrichment:
    return ArtistEnrichment(
        canonical_name=canonical_name,
        discogs_artist_id=discogs_artist_id,
        styles=styles if styles is not None else ["IDM", "Abstract"],
        personnel=personnel if personnel is not None else [],
        labels=labels if labels is not None else [LabelInfo(name="Warp Records", label_id=100)],
        compilation_appearances=compilation_appearances
        if compilation_appearances is not None
        else [],
    )


def make_personnel_credit(
    name: str = "Rob Brown",
    roles: list[str] | None = None,
) -> PersonnelCredit:
    return PersonnelCredit(
        name=name,
        roles=roles if roles is not None else ["Written-By"],
    )


def make_entity(
    name: str = "Autechre",
    entity_type: str = "artist",
    wikidata_qid: str | None = None,
) -> Entity:
    """Create an Entity instance for testing.

    Uses a fixed id=1 since the real id is assigned by SQLite AUTOINCREMENT.
    """
    return Entity(id=1, name=name, entity_type=entity_type, wikidata_qid=wikidata_qid)


def make_wikidata_entity(
    qid: str = "Q2774",
    name: str = "Autechre",
    description: str | None = "British electronic music duo",
    discogs_artist_id: int | None = 2774,
) -> WikidataEntity:
    return WikidataEntity(
        qid=qid, name=name, description=description, discogs_artist_id=discogs_artist_id
    )


def make_wikidata_influence(
    source_qid: str = "Q2774",
    target_qid: str = "Q484641",
    target_name: str = "Kraftwerk",
) -> WikidataInfluence:
    return WikidataInfluence(source_qid=source_qid, target_qid=target_qid, target_name=target_name)


def make_wikidata_label_hierarchy(
    parent_qid: str = "Q21077",
    parent_name: str = "Universal Music Group",
    child_qid: str = "Q1312934",
    child_name: str = "Warp Records",
) -> WikidataLabelHierarchy:
    return WikidataLabelHierarchy(
        parent_qid=parent_qid,
        parent_name=parent_name,
        child_qid=child_qid,
        child_name=child_name,
    )
