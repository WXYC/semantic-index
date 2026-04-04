"""Discogs-derived edge extraction from ArtistEnrichment data.

Four pure functions that compute edges between artists based on shared personnel,
overlapping style tags, shared record labels, and co-appearance on compilations.
All functions use inverted indexes for efficient pair generation.
"""

import logging
from collections import defaultdict
from itertools import combinations

from semantic_index.models import (
    ArtistEnrichment,
    CompilationEdge,
    LabelFamilyEdge,
    SharedPersonnelEdge,
    SharedStyleEdge,
)

logger = logging.getLogger(__name__)


def extract_shared_personnel(
    enrichments: dict[str, ArtistEnrichment],
    min_shared: int = 1,
    max_artists: int | None = None,
) -> list[SharedPersonnelEdge]:
    """Build edges between artists who share credited musicians.

    Builds an inverted index from personnel name to set of artists, then emits
    edges for all pairs of artists sharing at least ``min_shared`` personnel.

    Args:
        enrichments: Mapping of canonical artist name to enrichment data.
        min_shared: Minimum number of shared personnel to emit an edge.
        max_artists: Skip personnel credited on more than this many artists
            (ubiquitous engineers/producers are noise). None disables the cap.

    Returns:
        Deterministically sorted list of SharedPersonnelEdge.
    """
    # Inverted index: personnel_name -> set of artist names
    personnel_to_artists: dict[str, set[str]] = defaultdict(set)
    for artist_name, enrichment in enrichments.items():
        for credit in enrichment.personnel:
            personnel_to_artists[credit.name].add(artist_name)

    # For each pair of artists, count shared personnel
    skipped = 0
    pair_shared: dict[tuple[str, str], set[str]] = defaultdict(set)
    for personnel_name, artists in personnel_to_artists.items():
        if len(artists) < 2:
            continue
        if max_artists is not None and len(artists) > max_artists:
            skipped += 1
            continue
        for a, b in combinations(sorted(artists), 2):
            pair_shared[(a, b)].add(personnel_name)

    if skipped:
        logger.info("  Skipped %d personnel names exceeding max_artists=%d", skipped, max_artists)

    # Build edges, filtering by min_shared
    edges: list[SharedPersonnelEdge] = []
    for (a, b), shared_names in sorted(pair_shared.items()):
        if len(shared_names) >= min_shared:
            edges.append(
                SharedPersonnelEdge(
                    artist_a=a,
                    artist_b=b,
                    shared_count=len(shared_names),
                    shared_names=sorted(shared_names),
                )
            )

    logger.info("Extracted %d shared-personnel edges", len(edges))
    return edges


def extract_shared_styles(
    enrichments: dict[str, ArtistEnrichment],
    min_jaccard: float = 0.1,
    max_artists: int | None = None,
) -> list[SharedStyleEdge]:
    """Build edges between artists with overlapping Discogs style tags.

    Uses an inverted index (style tag -> artists) to avoid O(n^2) pairwise
    comparison. Only pairs sharing at least one tag are considered, then Jaccard
    similarity is computed: ``|intersection| / |union|``.

    Args:
        enrichments: Mapping of canonical artist name to enrichment data.
        min_jaccard: Minimum Jaccard similarity to emit an edge.
        max_artists: Skip style tags shared by more than this many artists
            (broad tags like "Experimental" are noise). None disables the cap.

    Returns:
        Deterministically sorted list of SharedStyleEdge.
    """
    # Build style sets per artist
    artist_styles: dict[str, set[str]] = {}
    for artist_name, enrichment in enrichments.items():
        if enrichment.styles:
            artist_styles[artist_name] = set(enrichment.styles)

    # Inverted index: style -> set of artists (only artists with non-empty styles)
    style_to_artists: dict[str, set[str]] = defaultdict(set)
    for artist_name, styles in artist_styles.items():
        for style in styles:
            style_to_artists[style].add(artist_name)

    # Collect candidate pairs (those sharing at least one non-excluded tag)
    skipped = 0
    candidate_pairs: set[tuple[str, str]] = set()
    excluded_styles: set[str] = set()
    for style, artists in style_to_artists.items():
        if len(artists) < 2:
            continue
        if max_artists is not None and len(artists) > max_artists:
            skipped += 1
            excluded_styles.add(style)
            continue
        for a, b in combinations(sorted(artists), 2):
            candidate_pairs.add((a, b))

    if skipped:
        logger.info("  Skipped %d styles exceeding max_artists=%d", skipped, max_artists)

    # Compute Jaccard for each candidate pair (using only non-excluded styles)
    edges: list[SharedStyleEdge] = []
    for a, b in sorted(candidate_pairs):
        styles_a = artist_styles[a] - excluded_styles
        styles_b = artist_styles[b] - excluded_styles
        if not styles_a or not styles_b:
            continue
        intersection = styles_a & styles_b
        union = styles_a | styles_b
        jaccard = len(intersection) / len(union)

        if jaccard >= min_jaccard:
            edges.append(
                SharedStyleEdge(
                    artist_a=a,
                    artist_b=b,
                    jaccard=jaccard,
                    shared_tags=sorted(intersection),
                )
            )

    logger.info("Extracted %d shared-style edges", len(edges))
    return edges


def extract_label_family(
    enrichments: dict[str, ArtistEnrichment],
    max_label_artists: int = 500,
) -> list[LabelFamilyEdge]:
    """Build edges between artists who share a record label.

    Builds an inverted index from label name to set of artists, then emits
    edges for all pairs. Labels with more than ``max_label_artists`` are excluded
    to avoid noise from mega-labels.

    Args:
        enrichments: Mapping of canonical artist name to enrichment data.
        max_label_artists: Maximum number of artists on a label before it is excluded.

    Returns:
        Deterministically sorted list of LabelFamilyEdge.
    """
    # Inverted index: label_name -> set of artist names
    label_to_artists: dict[str, set[str]] = defaultdict(set)
    for artist_name, enrichment in enrichments.items():
        for label in enrichment.labels:
            label_to_artists[label.name].add(artist_name)

    # For each pair of artists, collect shared labels
    pair_labels: dict[tuple[str, str], list[str]] = defaultdict(list)
    for label_name, artists in label_to_artists.items():
        if len(artists) < 2 or len(artists) > max_label_artists:
            continue
        for a, b in combinations(sorted(artists), 2):
            pair_labels[(a, b)].append(label_name)

    # Build edges
    edges: list[LabelFamilyEdge] = []
    for (a, b), labels in sorted(pair_labels.items()):
        edges.append(
            LabelFamilyEdge(
                artist_a=a,
                artist_b=b,
                shared_labels=sorted(labels),
            )
        )

    logger.info("Extracted %d label-family edges", len(edges))
    return edges


def extract_compilation_coappearance(
    enrichments: dict[str, ArtistEnrichment],
) -> list[CompilationEdge]:
    """Build edges between artists who appear on the same compilation.

    Builds an inverted index from compilation release_id to the set of graph
    artists appearing on it (only artists present in ``enrichments``). Emits
    edges for all pairs on compilations with 2+ graph artists.

    Args:
        enrichments: Mapping of canonical artist name to enrichment data.

    Returns:
        Deterministically sorted list of CompilationEdge.
    """
    graph_artists = set(enrichments.keys())

    # Inverted index: release_id -> (release_title, set of graph artists)
    comp_index: dict[int, tuple[str, set[str]]] = {}
    for artist_name, enrichment in enrichments.items():
        for comp in enrichment.compilation_appearances:
            if comp.release_id not in comp_index:
                comp_index[comp.release_id] = (comp.release_title, set())
            comp_index[comp.release_id][1].add(artist_name)
            # Also add other_artists that are in the graph
            for other in comp.other_artists:
                if other in graph_artists:
                    comp_index[comp.release_id][1].add(other)

    # For each pair of artists, collect shared compilations
    pair_comps: dict[tuple[str, str], list[str]] = defaultdict(list)
    for _release_id, (title, artists) in comp_index.items():
        if len(artists) < 2:
            continue
        for a, b in combinations(sorted(artists), 2):
            pair_comps[(a, b)].append(title)

    # Build edges
    edges: list[CompilationEdge] = []
    for (a, b), titles in sorted(pair_comps.items()):
        edges.append(
            CompilationEdge(
                artist_a=a,
                artist_b=b,
                compilation_count=len(titles),
                compilation_titles=sorted(titles),
            )
        )

    logger.info("Extracted %d compilation-coappearance edges", len(edges))
    return edges
