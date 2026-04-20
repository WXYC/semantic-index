"""Compute and persist graph metrics: Louvain communities, centrality, and discovery scores.

Idempotent post-processing step that reads the existing SQLite database,
computes graph analytics, and writes them back. Can be run standalone or
as an optional final pipeline step.

Usage:
    python -m semantic_index.graph_metrics [path/to/wxyc_artist_graph.db]
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from collections import Counter
from dataclasses import dataclass

import networkx as nx
from networkx.algorithms.community import louvain_communities
from wxyc_etl.text import is_compilation_artist  # type: ignore[import-untyped]

from semantic_index.utils import ensure_columns

logger = logging.getLogger(__name__)

# Columns added by this module
_GRAPH_METRIC_COLUMNS = [
    ("community_id", "INTEGER"),
    ("betweenness", "REAL"),
    ("pagerank", "REAL"),
]

_COMMUNITY_TABLE_SCHEMA = """\
CREATE TABLE IF NOT EXISTS community (
    id INTEGER PRIMARY KEY,
    size INTEGER NOT NULL,
    label TEXT,
    top_genres TEXT,
    top_artists TEXT
);
CREATE INDEX IF NOT EXISTS idx_artist_community ON artist(community_id);
"""


@dataclass
class GraphMetricsReport:
    """Summary of what compute_and_persist computed."""

    community_count: int
    artists_scored: int
    largest_community_size: int


def _ensure_schema(conn: sqlite3.Connection) -> None:
    """Add graph metric columns to artist table and create community table if needed."""
    ensure_columns(conn, "artist", _GRAPH_METRIC_COLUMNS)
    conn.executescript(_COMMUNITY_TABLE_SCHEMA)
    conn.commit()


def _build_transition_graph(
    conn: sqlite3.Connection,
) -> tuple[nx.DiGraph, dict[int, dict]]:
    """Build directed graph from dj_transition (raw_count >= 2), excluding Various Artists.

    Returns (directed_graph, artist_lookup) where artist_lookup maps
    artist_id -> {name, genre, total_plays}.
    """
    # Load all artists, filter out Various Artists
    rows = conn.execute("SELECT id, canonical_name, genre, total_plays FROM artist").fetchall()
    artists = {}
    for r in rows:
        if not is_compilation_artist(r[1]):
            artists[r[0]] = {"name": r[1], "genre": r[2], "total_plays": r[3]}

    valid_ids = set(artists.keys())

    graph: nx.DiGraph = nx.DiGraph()
    edges = conn.execute(
        "SELECT source_id, target_id, raw_count, pmi FROM dj_transition WHERE raw_count >= 2"
    ).fetchall()
    for source, target, count, pmi in edges:
        if source in valid_ids and target in valid_ids:
            graph.add_edge(source, target, raw_count=count, pmi=pmi)

    return graph, artists


def _compute_communities(g_undirected: nx.Graph, seed: int = 42) -> list[set[int]]:
    """Louvain community detection, sorted by size descending."""
    communities = louvain_communities(g_undirected, seed=seed, weight="raw_count")
    return sorted(communities, key=len, reverse=True)


def _compute_centrality(
    directed: nx.DiGraph, undirected: nx.Graph, bc_k: int = 2000
) -> tuple[dict[int, float], dict[int, float]]:
    """Compute betweenness centrality and PageRank.

    Returns (betweenness_dict, pagerank_dict).
    """
    logger.info("Computing betweenness centrality (k=%d sample)...", bc_k)
    t0 = time.time()
    betweenness = nx.betweenness_centrality(undirected, k=min(bc_k, len(undirected)))
    logger.info("  done in %.1fs", time.time() - t0)

    logger.info("Computing PageRank...")
    t0 = time.time()
    pagerank = nx.pagerank(directed, alpha=0.85)
    logger.info("  done in %.1fs", time.time() - t0)

    return betweenness, pagerank


def _build_community_metadata(
    communities: list[set[int]],
    artists: dict[int, dict],
    conn: sqlite3.Connection,
) -> list[dict]:
    """Build community table rows with label, top_genres, top_artists."""
    # Load styles if available
    artist_styles: dict[int, list[str]] = {}
    try:
        style_col = "style_tag"
        # Check which column name the table uses
        cols = {r[1] for r in conn.execute("PRAGMA table_info(artist_style)")}
        if "style" in cols:
            style_col = "style"
        rows = conn.execute(f"SELECT artist_id, {style_col} FROM artist_style").fetchall()
        for artist_id, style in rows:
            artist_styles.setdefault(artist_id, []).append(style)
    except sqlite3.OperationalError:
        pass

    metadata = []
    for idx, comm in enumerate(communities):
        members = sorted(
            comm,
            key=lambda x: artists.get(x, {}).get("total_plays", 0),
            reverse=True,
        )

        # Discogs style distribution for label (richer than WXYC genre taxonomy)
        style_counts: Counter[str] = Counter()
        for m in members:
            for s in artist_styles.get(m, []):
                style_counts[s] += 1

        # Skip overly generic styles that don't differentiate communities
        generic = {"Experimental", "Abstract"}
        distinctive = [(s, c) for s, c in style_counts.most_common(10) if s not in generic]

        # Label from top 2 distinctive Discogs styles, falling back to WXYC genre
        label: str | None = None
        if len(distinctive) >= 2:
            label = f"{distinctive[0][0]} / {distinctive[1][0]}"
        elif distinctive:
            label = distinctive[0][0]
        else:
            genre_counts = Counter(artists.get(m, {}).get("genre") for m in members)
            genre_counts.pop(None, None)
            top_genre = genre_counts.most_common(1)
            label = str(top_genre[0][0]) if top_genre else None

        top_genres = style_counts.most_common(5)

        # Top artist names
        top_artists = [artists.get(m, {}).get("name", str(m)) for m in members[:5]]

        metadata.append(
            {
                "id": idx,
                "size": len(comm),
                "label": label,
                "top_genres": json.dumps(top_genres),
                "top_artists": json.dumps(top_artists),
            }
        )

    return metadata


def _persist(
    conn: sqlite3.Connection,
    node_community: dict[int, int],
    communities_meta: list[dict],
    betweenness: dict[int, float],
    pagerank: dict[int, float],
) -> None:
    """Clear old metrics and write new values."""
    conn.execute("UPDATE artist SET community_id = NULL, betweenness = NULL, pagerank = NULL")
    conn.execute("DELETE FROM community")

    for artist_id, comm_id in node_community.items():
        bc = betweenness.get(artist_id, 0.0)
        pr = pagerank.get(artist_id, 0.0)
        conn.execute(
            "UPDATE artist SET community_id = ?, betweenness = ?, pagerank = ? WHERE id = ?",
            (comm_id, bc, pr, artist_id),
        )

    # Insert community metadata
    for meta in communities_meta:
        conn.execute(
            "INSERT INTO community (id, size, label, top_genres, top_artists) "
            "VALUES (?, ?, ?, ?, ?)",
            (meta["id"], meta["size"], meta["label"], meta["top_genres"], meta["top_artists"]),
        )

    conn.commit()


def compute_and_persist(db_path: str, *, seed: int = 42, bc_k: int = 2000) -> GraphMetricsReport:
    """Compute graph metrics and persist to the SQLite database. Idempotent."""
    conn = sqlite3.connect(db_path)

    logger.info("Ensuring graph metrics schema...")
    _ensure_schema(conn)

    logger.info("Building transition graph...")
    directed, artists = _build_transition_graph(conn)
    undirected = directed.to_undirected()
    graph_nodes = set(directed.nodes())
    logger.info("  %d nodes, %d edges", directed.number_of_nodes(), directed.number_of_edges())

    if len(directed) == 0:
        logger.warning("Empty transition graph, nothing to compute")
        conn.close()
        return GraphMetricsReport(community_count=0, artists_scored=0, largest_community_size=0)

    logger.info("Computing communities...")
    communities = _compute_communities(undirected, seed=seed)
    logger.info("  found %d communities", len(communities))

    # Build node -> community mapping
    node_community: dict[int, int] = {}
    for idx, comm in enumerate(communities):
        for node in comm:
            node_community[node] = idx

    logger.info("Computing centrality...")
    betweenness, pagerank = _compute_centrality(directed, undirected, bc_k=bc_k)

    logger.info("Building community metadata...")
    communities_meta = _build_community_metadata(communities, artists, conn)

    logger.info("Persisting results...")
    _persist(conn, node_community, communities_meta, betweenness, pagerank)

    conn.close()

    return GraphMetricsReport(
        community_count=len(communities),
        artists_scored=len(graph_nodes),
        largest_community_size=len(communities[0]) if communities else 0,
    )


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Compute and persist graph metrics to the SQLite database."
    )
    parser.add_argument(
        "db_path",
        nargs="?",
        default="data/wxyc_artist_graph.db",
        help="Path to SQLite graph database (default: data/wxyc_artist_graph.db)",
    )
    args = parser.parse_args()

    report = compute_and_persist(args.db_path)
    print(
        f"Communities: {report.community_count}, "
        f"Artists scored: {report.artists_scored}, "
        f"Largest community: {report.largest_community_size}"
    )
