"""Build a NetworkX graph from PMI edges and export to GEXF.

GEXF (Graph Exchange XML Format) is the native format for Gephi,
a graph visualization tool.
"""

import networkx as nx

from semantic_index.models import ArtistStats, PmiEdge
from semantic_index.pmi import top_neighbors


def build_graph(
    edges: list[PmiEdge],
    artist_stats: dict[str, ArtistStats],
    min_count: int = 2,
) -> nx.Graph:
    """Build an undirected graph from PMI edges.

    Args:
        edges: All computed PMI edges.
        artist_stats: Per-artist statistics for node attributes.
        min_count: Minimum raw co-occurrence count to include an edge.

    Returns:
        A NetworkX Graph with node attributes (label, genre, total_plays)
        and edge attributes (weight=PMI, raw_count).
    """
    graph: nx.Graph = nx.Graph()

    for edge in edges:
        if edge.raw_count < min_count:
            continue

        for name in (edge.source, edge.target):
            if name not in graph:
                stats = artist_stats.get(name)
                graph.add_node(
                    name,
                    label=name,
                    genre=stats.genre or "" if stats else "",
                    total_plays=stats.total_plays if stats else 0,
                )

        graph.add_edge(
            edge.source,
            edge.target,
            weight=edge.pmi,
            raw_count=edge.raw_count,
        )

    return graph


def export_gexf(graph: nx.Graph, path: str) -> None:
    """Write the graph to a GEXF file."""
    nx.write_gexf(graph, path)


def print_top_neighbors(edges: list[PmiEdge], artists: list[str], n: int = 20) -> None:
    """Print the top-N neighbors for each artist.

    Args:
        edges: All computed PMI edges.
        artists: List of artist names to display neighbors for.
        n: Number of neighbors to show per artist.
    """
    for artist in artists:
        neighbors = top_neighbors(edges, artist, n=n)
        if not neighbors:
            print(f"\n{artist}: no neighbors found")
            continue

        print(f"\n{'─' * 60}")
        print(f"  {artist} — top {min(n, len(neighbors))} neighbors")
        print(f"{'─' * 60}")
        for i, edge in enumerate(neighbors, 1):
            other = edge.target if edge.source == artist else edge.source
            direction = "→" if edge.source == artist else "←"
            print(f"  {i:3d}. {direction} {other:<38s} PMI={edge.pmi:+.3f}  n={edge.raw_count}")
