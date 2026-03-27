"""Shared test fixtures and factory functions.

Uses WXYC example artists from the canonical data in wxyc-shared.
"""

from semantic_index.models import (
    AdjacencyPair,
    CrossReferenceEdge,
    FlowsheetEntry,
    LibraryCode,
    LibraryRelease,
    ResolvedEntry,
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
