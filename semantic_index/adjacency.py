"""Extract adjacency pairs from resolved flowsheet entries.

Groups entries by radio show and emits pairs for consecutive entries
within each show, sorted by sequence number.
"""

from collections import defaultdict
from collections.abc import Iterable

from semantic_index.models import AdjacencyPair, ResolvedEntry


def extract_adjacency_pairs(entries: Iterable[ResolvedEntry]) -> list[AdjacencyPair]:
    """Extract consecutive artist pairs within each radio show.

    Entries are grouped by show_id and sorted by sequence. Each consecutive
    pair of entries within a show produces one AdjacencyPair.
    """
    by_show: dict[int, list[ResolvedEntry]] = defaultdict(list)
    for entry in entries:
        by_show[entry.entry.show_id].append(entry)

    pairs: list[AdjacencyPair] = []
    for show_id in sorted(by_show):
        show_entries = sorted(by_show[show_id], key=lambda e: e.entry.sequence)
        for i in range(len(show_entries) - 1):
            pairs.append(
                AdjacencyPair(
                    source=show_entries[i].canonical_name,
                    target=show_entries[i + 1].canonical_name,
                    show_id=show_id,
                )
            )

    return pairs
