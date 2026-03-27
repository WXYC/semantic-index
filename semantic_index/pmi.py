"""Compute Pointwise Mutual Information for artist co-occurrences.

PMI(a, b) = log2(P(a,b) / (P(a) * P(b)))

High PMI means two artists appear together in DJ transitions more than
random chance would predict — they share curatorial affinity.
"""

import math
from collections import Counter
from collections.abc import Iterable

from semantic_index.models import AdjacencyPair, PmiEdge, ResolvedEntry


def compute_pmi(
    pairs: Iterable[AdjacencyPair],
    entries: Iterable[ResolvedEntry],
) -> list[PmiEdge]:
    """Compute PMI for all observed artist adjacency pairs.

    Args:
        pairs: Adjacency pairs extracted from flowsheet shows.
        entries: All resolved entries (used for marginal artist frequencies).

    Returns:
        A PmiEdge for each unique (source, target) pair, sorted by PMI descending.
    """
    artist_counts: Counter[str] = Counter()
    for entry in entries:
        artist_counts[entry.canonical_name] += 1

    pair_counts: Counter[tuple[str, str]] = Counter()
    pairs_list = list(pairs)
    for pair in pairs_list:
        pair_counts[(pair.source, pair.target)] += 1

    total_entries = sum(artist_counts.values())
    total_pairs = len(pairs_list)

    if total_pairs == 0 or total_entries == 0:
        return []

    edges: list[PmiEdge] = []
    for (source, target), count in pair_counts.items():
        p_pair = count / total_pairs
        p_source = artist_counts[source] / total_entries
        p_target = artist_counts[target] / total_entries

        pmi = math.log2(p_pair / (p_source * p_target))

        edges.append(
            PmiEdge(
                source=source,
                target=target,
                raw_count=count,
                pmi=pmi,
            )
        )

    edges.sort(key=lambda e: e.pmi, reverse=True)
    return edges


def top_neighbors(edges: Iterable[PmiEdge], artist: str, n: int = 20) -> list[PmiEdge]:
    """Return the top-N neighbors for a given artist, sorted by PMI descending.

    Considers both directions: edges where the artist is source or target.
    """
    relevant = [e for e in edges if e.source == artist or e.target == artist]
    relevant.sort(key=lambda e: e.pmi, reverse=True)
    return relevant[:n]
