"""Graph API query endpoints: search, neighbors, explain."""

from __future__ import annotations

import json
import logging
import math
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from semantic_index.api.database import get_db
from semantic_index.api.schemas import (
    ArtistDetail,
    ArtistSummary,
    AudioProfileResponse,
    CommunitiesResponse,
    CommunityDetail,
    DiscoveryEntry,
    DiscoveryResponse,
    DjSummary,
    EntityArtists,
    ExplainResponse,
    FacetsResponse,
    NeighborEntry,
    NeighborsResponse,
    Relationship,
    SearchResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/graph", tags=["graph"])

# Cached flag: whether the artist table has graph metrics columns
_HAS_METRICS: bool | None = None


def _init_metrics_flag(db: sqlite3.Connection) -> None:
    """Check once per process whether graph metrics columns exist."""
    global _HAS_METRICS
    if _HAS_METRICS is None:
        cols = {r[1] for r in db.execute("PRAGMA table_info(artist)")}
        _HAS_METRICS = "community_id" in cols


def _scols(prefix: str = "") -> str:
    """Return SQL column list for artist summary SELECTs."""
    p = f"{prefix}." if prefix else ""
    s = f"{p}id, {p}canonical_name, {p}genre, {p}total_plays"
    if _HAS_METRICS:
        s += f", {p}community_id, {p}pagerank"
    return s


class EdgeType(StrEnum):
    """Supported edge types for neighbor queries."""

    AFFINITY = "affinity"
    DJ_TRANSITION = "djTransition"
    SHARED_PERSONNEL = "sharedPersonnel"
    SHARED_STYLE = "sharedStyle"
    LABEL_FAMILY = "labelFamily"
    COMPILATION = "compilation"
    CROSS_REFERENCE = "crossReference"
    WIKIDATA_INFLUENCE = "wikidataInfluence"
    ACOUSTIC_SIMILARITY = "acousticSimilarity"


@dataclass(frozen=True, slots=True)
class EdgeSchema:
    """Configuration for a symmetric edge type's table and columns."""

    table: str
    weight_col: str | None
    detail_cols: list[str]
    col_a: str = "artist_a_id"
    col_b: str = "artist_b_id"
    affinity_score_expr: str | None = None
    affinity_weight: float = 1.0
    affinity_type_name: str | None = None


EDGE_REGISTRY: dict[EdgeType, EdgeSchema] = {
    EdgeType.SHARED_STYLE: EdgeSchema(
        "shared_style",
        "jaccard",
        ["jaccard", "shared_tags"],
        affinity_score_expr="jaccard",
    ),
    EdgeType.SHARED_PERSONNEL: EdgeSchema(
        "shared_personnel",
        "shared_count",
        ["shared_count", "shared_names"],
        affinity_score_expr="shared_count",
        affinity_weight=1.5,
    ),
    EdgeType.LABEL_FAMILY: EdgeSchema(
        "label_family",
        None,
        ["shared_labels"],
        affinity_score_expr="1",
    ),
    EdgeType.COMPILATION: EdgeSchema(
        "compilation",
        "compilation_count",
        ["compilation_count", "compilation_titles"],
        affinity_score_expr="compilation_count",
    ),
    EdgeType.CROSS_REFERENCE: EdgeSchema(
        "cross_reference",
        None,
        ["comment", "source"],
    ),
    EdgeType.WIKIDATA_INFLUENCE: EdgeSchema(
        "wikidata_influence",
        None,
        ["source_qid", "target_qid"],
        col_a="source_id",
        col_b="target_id",
        affinity_score_expr="1",
        affinity_weight=1.5,
        affinity_type_name="influence",
    ),
    EdgeType.ACOUSTIC_SIMILARITY: EdgeSchema(
        "acoustic_similarity",
        "similarity",
        ["similarity"],
        affinity_score_expr="similarity",
        affinity_weight=2.0,
    ),
}

# member_of is not a user-facing edge type but contributes to affinity scoring
_MEMBER_OF_AFFINITY = EdgeSchema(
    "member_of",
    None,
    [],
    col_a="group_id",
    col_b="member_id",
    affinity_score_expr="1",
    affinity_weight=2.0,
    affinity_type_name="member",
)


def _artist_summary(row: sqlite3.Row) -> ArtistSummary:
    keys = row.keys()
    return ArtistSummary(
        id=row["id"],
        canonical_name=row["canonical_name"],
        genre=row["genre"],
        total_plays=row["total_plays"],
        community_id=row["community_id"] if "community_id" in keys else None,
        pagerank=row["pagerank"] if "pagerank" in keys else None,
    )


def _get_artist_or_404(db: sqlite3.Connection, artist_id: int) -> ArtistSummary:
    _init_metrics_flag(db)
    row = db.execute(
        f"SELECT {_scols()} FROM artist WHERE id = ?",  # noqa: S608
        (artist_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Artist not found")
    return _artist_summary(row)


@router.get("/artists/random", response_model=ArtistSummary)
def random_artist(
    db: sqlite3.Connection = Depends(get_db),
) -> ArtistSummary:
    """Return a random artist that has at least one DJ transition edge."""
    _init_metrics_flag(db)
    row = db.execute(
        f"SELECT {_scols('a')} FROM artist a "  # noqa: S608
        "WHERE a.id IN (SELECT source_id FROM dj_transition "
        "              UNION SELECT target_id FROM dj_transition) "
        "ORDER BY RANDOM() LIMIT 1",
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="No connected artists in database")
    return _artist_summary(row)


@router.get("/artists/search", response_model=SearchResponse)
def search_artists(
    q: str = Query(min_length=1),
    limit: int = Query(default=10, ge=1, le=100),
    db: sqlite3.Connection = Depends(get_db),
) -> SearchResponse:
    """Case-insensitive artist name search.

    Prefix matches rank above substring-only matches. Within each group,
    results are ordered by total_plays descending.
    """
    _init_metrics_flag(db)
    rows = db.execute(
        f"SELECT {_scols()} FROM artist "  # noqa: S608
        "WHERE canonical_name LIKE ? "
        "ORDER BY (canonical_name LIKE ?) DESC, total_plays DESC LIMIT ?",
        (f"%{q}%", f"{q}%", limit),
    ).fetchall()
    return SearchResponse(results=[_artist_summary(r) for r in rows])


@router.get("/facets", response_model=FacetsResponse)
def get_facets(
    db: sqlite3.Connection = Depends(get_db),
) -> FacetsResponse:
    """Return available facet values (months, DJs) for filtering.

    Gracefully returns empty lists on databases without facet tables.
    """
    try:
        months = [
            r[0] for r in db.execute("SELECT month FROM month_total ORDER BY month").fetchall()
        ]
        djs = [
            DjSummary(id=r["id"], display_name=r["display_name"])
            for r in db.execute("SELECT id, display_name FROM dj ORDER BY display_name").fetchall()
        ]
    except sqlite3.OperationalError:
        logger.debug("Facet tables not found — returning empty facets")
        months = []
        djs = []
    return FacetsResponse(months=months, djs=djs)


@router.get("/communities", response_model=CommunitiesResponse)
def get_communities(
    min_size: int = Query(default=5, ge=1),
    limit: int = Query(default=50, ge=1, le=500),
    db: sqlite3.Connection = Depends(get_db),
) -> CommunitiesResponse:
    """Return community metadata, filtered by minimum size."""
    try:
        rows = db.execute(
            "SELECT id, size, label, top_genres, top_artists "
            "FROM community WHERE size >= ? ORDER BY size DESC LIMIT ?",
            (min_size, limit),
        ).fetchall()
    except sqlite3.OperationalError:
        return CommunitiesResponse(communities=[])

    communities = []
    for r in rows:
        communities.append(
            CommunityDetail(
                id=r["id"],
                size=r["size"],
                label=r["label"],
                top_genres=json.loads(r["top_genres"]) if r["top_genres"] else None,
                top_artists=json.loads(r["top_artists"]) if r["top_artists"] else None,
            )
        )
    return CommunitiesResponse(communities=communities)


@router.get("/discovery", response_model=DiscoveryResponse)
def get_discovery(
    limit: int = Query(default=25, ge=1, le=100),
    community_id: int | None = Query(default=None),
    genre: str | None = Query(default=None),
    db: sqlite3.Connection = Depends(get_db),
) -> DiscoveryResponse:
    """Return underplayed sonic fits — artists acoustically similar to many but rarely placed by DJs."""
    _init_metrics_flag(db)
    try:
        sql = (
            f"SELECT {_scols()}, discovery_score, dj_edge_count, acoustic_neighbor_count "  # noqa: S608
            "FROM artist "
            "WHERE discovery_score IS NOT NULL AND discovery_score > 0 "
        )
        params: list = []
        if community_id is not None:
            sql += "AND community_id = ? "
            params.append(community_id)
        if genre is not None:
            sql += "AND genre = ? "
            params.append(genre)
        sql += "ORDER BY discovery_score DESC LIMIT ?"
        params.append(limit)

        rows = db.execute(sql, params).fetchall()
    except sqlite3.OperationalError:
        return DiscoveryResponse(results=[])

    return DiscoveryResponse(
        results=[
            DiscoveryEntry(
                artist=_artist_summary(r),
                discovery_score=r["discovery_score"],
                dj_edge_count=r["dj_edge_count"],
                acoustic_neighbor_count=r["acoustic_neighbor_count"],
            )
            for r in rows
        ]
    )


@router.get("/artists/{artist_id}", response_model=ArtistDetail)
def get_artist_detail(
    artist_id: int,
    db: sqlite3.Connection = Depends(get_db),
) -> ArtistDetail:
    """Return full artist detail including external IDs from the entity table.

    Gracefully handles databases without entity store columns by falling back
    to NULL values for entity fields.
    """
    return _get_artist_detail(db, artist_id)


@router.get("/artists/{artist_id}/audio", response_model=AudioProfileResponse)
def get_audio_profile(
    artist_id: int,
    db: sqlite3.Connection = Depends(get_db),
) -> AudioProfileResponse:
    """Return AcousticBrainz-derived audio profile for an artist.

    Returns 404 if the artist has no audio profile (no AcousticBrainz data
    found for their recordings).
    """
    _get_artist_or_404(db, artist_id)
    try:
        row = db.execute(
            "SELECT artist_id, avg_danceability, primary_genre, "
            "primary_genre_probability, voice_instrumental_ratio, "
            "feature_centroid, recording_count "
            "FROM audio_profile WHERE artist_id = ?",
            (artist_id,),
        ).fetchone()
    except sqlite3.OperationalError:
        raise HTTPException(status_code=404, detail="Audio profiles not available") from None

    if row is None:
        raise HTTPException(status_code=404, detail="No audio profile for this artist")

    centroid = json.loads(row["feature_centroid"]) if row["feature_centroid"] else None
    return AudioProfileResponse(
        artist_id=row["artist_id"],
        avg_danceability=row["avg_danceability"],
        primary_genre=row["primary_genre"],
        primary_genre_probability=row["primary_genre_probability"],
        voice_instrumental_ratio=row["voice_instrumental_ratio"],
        recording_count=row["recording_count"],
        feature_centroid=centroid,
    )


@router.get("/artists/{artist_id}/neighbors", response_model=NeighborsResponse)
def get_neighbors(
    artist_id: int,
    type: EdgeType = Query(default=EdgeType.DJ_TRANSITION),
    limit: int = Query(default=20, ge=1, le=100),
    month: int | None = Query(default=None, ge=1, le=12),
    dj_id: int | None = Query(default=None, ge=1),
    heat: float = Query(default=0.5, ge=0.0, le=1.0),
    db: sqlite3.Connection = Depends(get_db),
) -> NeighborsResponse:
    """Return neighbors of an artist by edge type, ordered by weight descending.

    When ``month`` or ``dj_id`` are provided and the edge type is ``djTransition``,
    computes PMI dynamically from the play table filtered by the given facets.
    Facet parameters are ignored for other edge types.

    ``heat`` controls ranking for DJ transition and affinity edges (0.0–1.0).
    At 0.0 (cool), neighbors are ranked by raw co-occurrence count — the
    predictable, well-worn transitions many DJs have validated. At 1.0 (hot),
    neighbors are ranked by PMI — the rare, surprising pairings. Default 0.5
    blends both signals equally.
    """
    artist = _get_artist_or_404(db, artist_id)
    has_facets = month is not None or dj_id is not None
    if has_facets and type == EdgeType.DJ_TRANSITION:
        neighbors = _neighbors_dj_transition_faceted(db, artist_id, limit, month, dj_id, heat=heat)
    elif type == EdgeType.AFFINITY:
        neighbors = _neighbors_affinity(db, artist_id, limit, heat=heat)
    else:
        neighbors = _query_neighbors(db, artist_id, type, limit, heat=heat)
    return NeighborsResponse(artist=artist, edge_type=type.value, neighbors=neighbors)


@router.get("/artists/{artist_id}/explain/{target_id}", response_model=ExplainResponse)
def explain_relationship(
    artist_id: int,
    target_id: int,
    db: sqlite3.Connection = Depends(get_db),
) -> ExplainResponse:
    """Return all relationship types between two artists."""
    source = _get_artist_or_404(db, artist_id)
    target = _get_artist_or_404(db, target_id)
    relationships: list[Relationship] = []

    for edge_type in EdgeType:
        rels = _query_explain(db, artist_id, target_id, edge_type)
        relationships.extend(rels)

    return ExplainResponse(source=source, target=target, relationships=relationships)


@router.get("/entities/{entity_id}/artists", response_model=EntityArtists)
def get_entity_artists(
    entity_id: int,
    db: sqlite3.Connection = Depends(get_db),
) -> EntityArtists:
    """Return all artists sharing an entity (alias group)."""
    entity_row = db.execute(
        "SELECT id, name, wikidata_qid FROM entity WHERE id = ?",
        (entity_id,),
    ).fetchone()
    if entity_row is None:
        raise HTTPException(status_code=404, detail="Entity not found")

    _init_metrics_flag(db)
    artist_rows = db.execute(
        f"SELECT {_scols()} FROM artist WHERE entity_id = ?",  # noqa: S608
        (entity_id,),
    ).fetchall()

    return EntityArtists(
        entity_id=entity_row["id"],
        entity_name=entity_row["name"],
        wikidata_qid=entity_row["wikidata_qid"],
        artists=[_artist_summary(r) for r in artist_rows],
    )


def _get_artist_detail(db: sqlite3.Connection, artist_id: int) -> ArtistDetail:
    """Fetch full artist detail, joining entity table when available.

    Falls back gracefully when entity store columns don't exist in the database
    (old schema without entity_id, discogs_artist_id, etc.).
    """
    try:
        row = db.execute(
            "SELECT a.id, a.canonical_name, a.genre, a.total_plays, "
            "  a.active_first_year, a.active_last_year, a.dj_count, "
            "  a.request_ratio, a.show_count, "
            "  a.entity_id, a.discogs_artist_id, a.musicbrainz_artist_id, "
            "  a.reconciliation_status, "
            "  e.wikidata_qid, "
            "  e.spotify_artist_id, e.apple_music_artist_id, e.bandcamp_id "
            "FROM artist a "
            "LEFT JOIN entity e ON a.entity_id = e.id "
            "WHERE a.id = ?",
            (artist_id,),
        ).fetchone()
    except sqlite3.OperationalError:
        # Old schema: entity columns or entity table don't exist
        row = db.execute(
            "SELECT id, canonical_name, genre, total_plays, "
            "  active_first_year, active_last_year, dj_count, "
            "  request_ratio, show_count "
            "FROM artist WHERE id = ?",
            (artist_id,),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Artist not found") from None
        return ArtistDetail(
            id=row["id"],
            canonical_name=row["canonical_name"],
            genre=row["genre"],
            total_plays=row["total_plays"],
            active_first_year=row["active_first_year"],
            active_last_year=row["active_last_year"],
            dj_count=row["dj_count"],
            request_ratio=row["request_ratio"],
            show_count=row["show_count"],
        )

    if row is None:
        raise HTTPException(status_code=404, detail="Artist not found")

    return ArtistDetail(
        id=row["id"],
        canonical_name=row["canonical_name"],
        genre=row["genre"],
        total_plays=row["total_plays"],
        active_first_year=row["active_first_year"],
        active_last_year=row["active_last_year"],
        dj_count=row["dj_count"],
        request_ratio=row["request_ratio"],
        show_count=row["show_count"],
        entity_id=row["entity_id"],
        discogs_artist_id=row["discogs_artist_id"],
        musicbrainz_artist_id=row["musicbrainz_artist_id"],
        wikidata_qid=row["wikidata_qid"],
        reconciliation_status=row["reconciliation_status"],
        spotify_artist_id=row["spotify_artist_id"],
        apple_music_artist_id=row["apple_music_artist_id"],
        bandcamp_id=row["bandcamp_id"],
    )


def _query_neighbors(
    db: sqlite3.Connection,
    artist_id: int,
    edge_type: EdgeType,
    limit: int,
    *,
    heat: float = 0.5,
) -> list[NeighborEntry]:
    """Query neighbors for a given edge type."""
    if edge_type == EdgeType.AFFINITY:
        return _neighbors_affinity(db, artist_id, limit, heat=heat)
    if edge_type == EdgeType.DJ_TRANSITION:
        return _neighbors_dj_transition(db, artist_id, limit, heat=heat)
    if edge_type == EdgeType.SHARED_PERSONNEL:
        return _neighbors_shared_personnel(db, artist_id, limit)
    schema = EDGE_REGISTRY[edge_type]
    return _neighbors_symmetric(
        db,
        artist_id,
        limit,
        schema.table,
        schema.weight_col,
        schema.detail_cols,
        col_a=schema.col_a,
        col_b=schema.col_b,
    )


def _rank_by_heat(rows: list[sqlite3.Row], heat: float, limit: int) -> list[NeighborEntry]:
    """Rank DJ transition rows by blending raw_count and PMI.

    heat=0 sorts by raw_count (cool/predictable), heat=1 sorts by PMI (hot/rare).
    Normalizes both signals to 0-1 before blending.
    """
    max_count = max(r["raw_count"] for r in rows) or 1
    max_pmi = max(r["pmi"] for r in rows) or 1

    scored = []
    for r in rows:
        norm_count = r["raw_count"] / max_count
        norm_pmi = r["pmi"] / max_pmi
        score = (1 - heat) * norm_count + heat * norm_pmi
        scored.append((r, score))

    scored.sort(key=lambda x: x[1], reverse=True)

    return [
        NeighborEntry(
            artist=_artist_summary(r),
            weight=score,
            detail={"raw_count": r["raw_count"], "pmi": r["pmi"]},
        )
        for r, score in scored[:limit]
    ]


def _neighbors_dj_transition(
    db: sqlite3.Connection, artist_id: int, limit: int, *, heat: float = 0.5
) -> list[NeighborEntry]:
    """Return DJ transition neighbors ranked by a cool/hot blend.

    heat=0.0 ranks by raw_count (predictable, well-worn transitions).
    heat=1.0 ranks by PMI (rare, surprising pairings).
    """
    acols = _scols("a")
    rows = db.execute(
        f"SELECT {acols}, dt.raw_count, dt.pmi "  # noqa: S608
        "FROM dj_transition dt "
        "JOIN artist a ON a.id = dt.target_id "
        "WHERE dt.source_id = ? "
        "UNION ALL "
        f"SELECT {acols}, dt.raw_count, dt.pmi "
        "FROM dj_transition dt "
        "JOIN artist a ON a.id = dt.source_id "
        "WHERE dt.target_id = ?",
        (artist_id, artist_id),
    ).fetchall()
    if not rows:
        return []
    return _rank_by_heat(rows, heat, limit)


def _neighbors_dj_transition_faceted(
    db: sqlite3.Connection,
    artist_id: int,
    limit: int,
    month: int | None = None,
    dj_id: int | None = None,
    *,
    heat: float = 0.5,
) -> list[NeighborEntry]:
    """Compute faceted PMI neighbors dynamically from the play table.

    Three query paths depending on active facets:
    - Month only: marginals from artist_month_count, totals from month_total
    - DJ only: marginals from artist_dj_count, totals from dj_total
    - Both: marginals and totals computed dynamically from play table
    """
    # Step 1: Pair counts (self-join on play table, scoped to center artist)
    pair_counts: dict[int, int] = defaultdict(int)

    # Forward: center plays at position N, neighbor at N+1
    forward_sql = (
        "SELECT p2.artist_id AS neighbor_id, COUNT(*) AS pair_count "
        "FROM play p1 "
        "JOIN play p2 ON p2.show_id = p1.show_id AND p2.sequence = p1.sequence + 1 "
        "WHERE p1.artist_id = :center"
    )
    # Reverse: neighbor plays at position N, center at N+1
    reverse_sql = (
        "SELECT p1.artist_id AS neighbor_id, COUNT(*) AS pair_count "
        "FROM play p1 "
        "JOIN play p2 ON p2.show_id = p1.show_id AND p2.sequence = p1.sequence + 1 "
        "WHERE p2.artist_id = :center"
    )

    params: dict = {"center": artist_id}
    facet_clauses = []
    if month is not None:
        facet_clauses.append("p1.month = :month")
        params["month"] = month
    if dj_id is not None:
        facet_clauses.append("p1.dj_id = :dj_id")
        params["dj_id"] = dj_id

    if facet_clauses:
        facet_where = " AND " + " AND ".join(facet_clauses)
        forward_sql += facet_where
        # For reverse, facets apply to p2 (the center's row)
        reverse_facet = facet_where.replace("p1.", "p2.")
        reverse_sql += reverse_facet

    forward_sql += " GROUP BY p2.artist_id"
    reverse_sql += " GROUP BY p1.artist_id"

    for row in db.execute(forward_sql, params).fetchall():
        pair_counts[row["neighbor_id"]] += row["pair_count"]
    for row in db.execute(reverse_sql, params).fetchall():
        pair_counts[row["neighbor_id"]] += row["pair_count"]

    if not pair_counts:
        return []

    # Step 2: Marginals (play counts per artist in the filtered slice)
    all_artist_ids = [artist_id, *pair_counts.keys()]
    marginals = _get_faceted_marginals(db, all_artist_ids, month, dj_id)

    # Step 3: Totals (total plays and pairs in the filtered slice)
    total_plays, total_pairs = _get_faceted_totals(db, month, dj_id)

    if total_plays == 0 or total_pairs == 0:
        return []

    center_plays = marginals.get(artist_id, 0)
    if center_plays == 0:
        return []

    # Step 4: Compute PMI
    candidates: list[tuple[int, int, float]] = []  # (neighbor_id, raw_count, pmi)
    for neighbor_id, raw_count in pair_counts.items():
        neighbor_plays = marginals.get(neighbor_id, 0)
        if neighbor_plays == 0:
            continue

        p_pair = raw_count / total_pairs
        p_center = center_plays / total_plays
        p_neighbor = neighbor_plays / total_plays

        denominator = p_center * p_neighbor
        if denominator == 0:
            continue

        pmi = math.log2(p_pair / denominator)
        if pmi > 0:
            candidates.append((neighbor_id, raw_count, pmi))

    if not candidates:
        return []

    # Step 4b: Rank by heat blend (cool=raw_count, hot=PMI)
    max_count = max(c[1] for c in candidates) or 1
    max_pmi = max(c[2] for c in candidates) or 1
    scored = [
        (nid, rc, p, (1 - heat) * (rc / max_count) + heat * (p / max_pmi))
        for nid, rc, p in candidates
    ]
    scored.sort(key=lambda x: x[3], reverse=True)
    scored = scored[:limit]

    # Step 5: Build response (look up artist summaries)
    neighbor_ids = [s[0] for s in scored]
    placeholders = ",".join("?" * len(neighbor_ids))
    artist_rows = db.execute(
        f"SELECT {_scols()} FROM artist WHERE id IN ({placeholders})",  # noqa: S608
        neighbor_ids,
    ).fetchall()
    artist_map = {r["id"]: _artist_summary(r) for r in artist_rows}

    return [
        NeighborEntry(
            artist=artist_map[nid],
            weight=blend_score,
            detail={"raw_count": raw_count, "pmi": round(pmi, 4)},
        )
        for nid, raw_count, pmi, blend_score in scored
        if nid in artist_map
    ]


def _get_faceted_marginals(
    db: sqlite3.Connection,
    artist_ids: list[int],
    month: int | None,
    dj_id: int | None,
) -> dict[int, int]:
    """Get per-artist play counts for the given facet filter."""
    placeholders = ",".join("?" * len(artist_ids))

    if month is not None and dj_id is not None:
        # Both facets: query play table directly
        rows = db.execute(
            f"SELECT artist_id, COUNT(*) AS play_count FROM play "  # noqa: S608
            f"WHERE artist_id IN ({placeholders}) AND month = ? AND dj_id = ? "
            f"GROUP BY artist_id",
            [*artist_ids, month, dj_id],
        ).fetchall()
    elif month is not None:
        rows = db.execute(
            f"SELECT artist_id, play_count FROM artist_month_count "  # noqa: S608
            f"WHERE artist_id IN ({placeholders}) AND month = ?",
            [*artist_ids, month],
        ).fetchall()
    elif dj_id is not None:
        rows = db.execute(
            f"SELECT artist_id, play_count FROM artist_dj_count "  # noqa: S608
            f"WHERE artist_id IN ({placeholders}) AND dj_id = ?",
            [*artist_ids, dj_id],
        ).fetchall()
    else:
        return {}

    return {r["artist_id"]: r["play_count"] for r in rows}


def _get_faceted_totals(
    db: sqlite3.Connection,
    month: int | None,
    dj_id: int | None,
) -> tuple[int, int]:
    """Get total plays and total pairs for the given facet filter."""
    if month is not None and dj_id is not None:
        # Both facets: compute dynamically
        plays_row = db.execute(
            "SELECT COUNT(*) FROM play WHERE month = ? AND dj_id = ?",
            (month, dj_id),
        ).fetchone()
        total_plays = plays_row[0] if plays_row else 0

        # Count pairs: consecutive plays in the same show within the facet
        pairs_row = db.execute(
            "SELECT COUNT(*) FROM play p1 "
            "JOIN play p2 ON p2.show_id = p1.show_id AND p2.sequence = p1.sequence + 1 "
            "WHERE p1.month = ? AND p1.dj_id = ?",
            (month, dj_id),
        ).fetchone()
        total_pairs = pairs_row[0] if pairs_row else 0
    elif month is not None:
        row = db.execute(
            "SELECT total_plays, total_pairs FROM month_total WHERE month = ?", (month,)
        ).fetchone()
        total_plays = row["total_plays"] if row else 0
        total_pairs = row["total_pairs"] if row else 0
    elif dj_id is not None:
        row = db.execute(
            "SELECT total_plays, total_pairs FROM dj_total WHERE dj_id = ?", (dj_id,)
        ).fetchone()
        total_plays = row["total_plays"] if row else 0
        total_pairs = row["total_pairs"] if row else 0
    else:
        return 0, 0

    return total_plays, total_pairs


def _parse_detail(row: sqlite3.Row, cols: list[str]) -> dict[str, Any]:
    """Parse detail columns from a row, deserializing JSON array strings."""
    detail: dict[str, Any] = {}
    for col in cols:
        val = row[col]
        if isinstance(val, str) and val.startswith("["):
            val = json.loads(val)
        detail[col] = val
    return detail


def _neighbors_symmetric(
    db: sqlite3.Connection,
    artist_id: int,
    limit: int,
    table: str,
    weight_col: str | None,
    detail_cols: list[str],
    *,
    col_a: str = "artist_a_id",
    col_b: str = "artist_b_id",
) -> list[NeighborEntry]:
    """Query symmetric edge neighbors, joining artist for both directions."""
    order = f"{weight_col} DESC" if weight_col else "1"
    acols = _scols("a")
    detail_select = ", ".join(f"e.{c}" for c in detail_cols)
    rows = db.execute(
        f"SELECT {acols}, {detail_select} "  # noqa: S608
        f"FROM {table} e "
        f"JOIN artist a ON a.id = e.{col_b} "
        f"WHERE e.{col_a} = ? "
        f"UNION ALL "
        f"SELECT {acols}, {detail_select} "
        f"FROM {table} e "
        f"JOIN artist a ON a.id = e.{col_a} "
        f"WHERE e.{col_b} = ? "
        f"ORDER BY {order} LIMIT ?",
        (artist_id, artist_id, limit),
    ).fetchall()
    return [
        NeighborEntry(
            artist=_artist_summary(r),
            weight=float(r[weight_col]) if weight_col else 1.0,
            detail=_parse_detail(r, detail_cols),
        )
        for r in rows
    ]


def _neighbors_affinity(
    db: sqlite3.Connection, artist_id: int, limit: int, *, heat: float = 0.5
) -> list[NeighborEntry]:
    """Composite affinity score across all edge types.

    Queries every edge table, normalizes each score to 0-1, and sums
    across dimensions. Neighbors connected on multiple dimensions rank
    higher. The detail dict lists which edge types contributed.

    ``heat`` controls how DJ transition scores are blended within the
    affinity computation (0=raw_count, 1=PMI).
    """
    # Build affinity source list from EDGE_REGISTRY + member_of
    affinity_sources: list[tuple[str, EdgeSchema]] = [
        (edge_type.value, schema)
        for edge_type, schema in EDGE_REGISTRY.items()
        if schema.affinity_score_expr is not None
    ]
    affinity_sources.append((_MEMBER_OF_AFFINITY.affinity_type_name or "", _MEMBER_OF_AFFINITY))

    edge_queries: list[tuple[str, tuple[int, ...]]] = []

    # DJ transitions — special case: needs both pmi and raw_count for heat blending
    edge_queries.append(
        (
            "SELECT target_id AS nid, 'djTransition' AS etype, pmi AS score, raw_count "
            "FROM dj_transition WHERE source_id = ? "
            "UNION ALL "
            "SELECT source_id, 'djTransition', pmi, raw_count "
            "FROM dj_transition WHERE target_id = ?",
            (artist_id, artist_id),
        )
    )

    # Generate queries from registry
    for etype_default, source in affinity_sources:
        etype = source.affinity_type_name or etype_default
        edge_queries.append(
            (
                f"SELECT {source.col_b}, '{etype}', {source.affinity_score_expr} "
                f"FROM {source.table} WHERE {source.col_a} = ? "
                f"UNION ALL "
                f"SELECT {source.col_a}, '{etype}', {source.affinity_score_expr} "
                f"FROM {source.table} WHERE {source.col_b} = ?",
                (artist_id, artist_id),
            )
        )

    # Collect all edges, keyed by neighbor ID.
    # DJ transition rows have an extra raw_count column for heat blending.
    neighbor_edges: dict[int, list[tuple[str, float]]] = {}
    dj_raw: dict[int, tuple[float, float]] = {}  # nid -> (pmi, raw_count)
    for sql, params in edge_queries:
        try:
            for row in db.execute(sql, params):
                nid = row[0]
                etype = row[1]
                score = float(row[2])
                if etype == "djTransition":
                    raw_count = float(row[3])
                    dj_raw[nid] = (score, raw_count)
                neighbor_edges.setdefault(nid, []).append((etype, score))
        except sqlite3.OperationalError:
            continue  # table doesn't exist

    # Apply heat blend to DJ transition scores
    if dj_raw:
        max_pmi = max(p for p, _ in dj_raw.values()) or 1
        max_rc = max(rc for _, rc in dj_raw.values()) or 1
        for nid, (pmi, rc) in dj_raw.items():
            blended = (1 - heat) * (rc / max_rc) + heat * (pmi / max_pmi)
            edges = neighbor_edges[nid]
            neighbor_edges[nid] = [
                (etype, blended if etype == "djTransition" else score) for etype, score in edges
            ]

    if not neighbor_edges:
        return []

    # Compute per-type max scores for normalization
    type_maxes: dict[str, float] = {}
    for edges in neighbor_edges.values():
        for etype, score in edges:
            type_maxes[etype] = max(type_maxes.get(etype, 0), score)

    # Dimension weights derived from registry
    dim_weights: dict[str, float] = {"djTransition": 3.0}
    for etype_default, source in affinity_sources:
        dim_weights[source.affinity_type_name or etype_default] = source.affinity_weight

    # Compute composite scores
    scored: list[tuple[int, float, list[str]]] = []
    for nid, edges in neighbor_edges.items():
        composite = 0.0
        types = []
        seen_types: set[str] = set()
        for etype, score in edges:
            if etype in seen_types:
                continue
            seen_types.add(etype)
            max_score = type_maxes.get(etype, 1) or 1
            w = dim_weights.get(etype, 1.0)
            composite += w * (score / max_score)  # weighted normalized score
            types.append(etype)
        scored.append((nid, composite, types))

    # Sort by composite score descending, take top N
    scored.sort(key=lambda x: x[1], reverse=True)
    top = scored[:limit]

    # Fetch artist details for the top neighbors
    ids = [nid for nid, _, _ in top]
    placeholders = ",".join("?" * len(ids))
    artist_rows = db.execute(
        f"SELECT {_scols()} FROM artist WHERE id IN ({placeholders})",  # noqa: S608
        ids,
    ).fetchall()
    artist_map = {r["id"]: r for r in artist_rows}

    results: list[NeighborEntry] = []
    for nid, composite, types in top:
        r = artist_map.get(nid)
        if not r:
            continue
        results.append(
            NeighborEntry(
                artist=_artist_summary(r),
                weight=round(composite, 3),
                detail={"dimensions": len(types), "types": types},
            )
        )
    return results


def _neighbors_shared_personnel(
    db: sqlite3.Connection, artist_id: int, limit: int
) -> list[NeighborEntry]:
    """Query shared personnel + band membership as a single edge type.

    .. seealso:: :func:`_neighbors_affinity` for the composite edge type.
    """
    # Try shared_personnel table first, then union with member_of
    acols = _scols("a")
    try:
        rows = db.execute(
            f"SELECT {acols}, "  # noqa: S608
            "  e.shared_count AS weight, e.shared_names AS detail, 'personnel' AS source "
            "FROM shared_personnel e "
            "JOIN artist a ON a.id = e.artist_b_id "
            "WHERE e.artist_a_id = ? "
            "UNION ALL "
            f"SELECT {acols}, "
            "  e.shared_count, e.shared_names, 'personnel' "
            "FROM shared_personnel e "
            "JOIN artist a ON a.id = e.artist_a_id "
            "WHERE e.artist_b_id = ? "
            "UNION ALL "
            f"SELECT {acols}, "
            "  1, '\"member\"', 'member' "
            "FROM member_of m "
            "JOIN artist a ON a.id = m.member_id "
            "WHERE m.group_id = ? "
            "UNION ALL "
            f"SELECT {acols}, "
            "  1, '\"member\"', 'member' "
            "FROM member_of m "
            "JOIN artist a ON a.id = m.group_id "
            "WHERE m.member_id = ? "
            "ORDER BY weight DESC LIMIT ?",
            (artist_id, artist_id, artist_id, artist_id, limit),
        ).fetchall()
    except sqlite3.OperationalError:
        # Fallback if member_of table doesn't exist
        return _neighbors_symmetric(
            db,
            artist_id,
            limit,
            "shared_personnel",
            "shared_count",
            ["shared_count", "shared_names"],
        )

    seen: set[int] = set()
    results: list[NeighborEntry] = []
    for r in rows:
        if r["id"] in seen:
            continue
        seen.add(r["id"])
        source = r["source"]
        if source == "member":
            detail = {"relationship": "member/group"}
        else:
            shared_names = r["detail"]
            if isinstance(shared_names, str) and shared_names.startswith("["):
                shared_names = json.loads(shared_names)
            detail = {"shared_count": r["weight"], "shared_names": shared_names}
        results.append(
            NeighborEntry(artist=_artist_summary(r), weight=float(r["weight"]), detail=detail)
        )
    return results


def _query_explain(
    db: sqlite3.Connection,
    source_id: int,
    target_id: int,
    edge_type: EdgeType,
) -> list[Relationship]:
    """Query a single edge type between two specific artists."""
    if edge_type == EdgeType.DJ_TRANSITION:
        return _explain_dj_transition(db, source_id, target_id)
    if edge_type == EdgeType.AFFINITY:
        return []  # affinity is a composite — explain uses individual types
    schema = EDGE_REGISTRY[edge_type]
    return _explain_symmetric(
        db,
        source_id,
        target_id,
        edge_type.value,
        schema.table,
        schema.weight_col,
        schema.detail_cols,
        col_a=schema.col_a,
        col_b=schema.col_b,
    )


def _explain_dj_transition(
    db: sqlite3.Connection, source_id: int, target_id: int
) -> list[Relationship]:
    row = db.execute(
        "SELECT raw_count, pmi FROM dj_transition "
        "WHERE (source_id = ? AND target_id = ?) OR (source_id = ? AND target_id = ?)",
        (source_id, target_id, target_id, source_id),
    ).fetchone()
    if row is None:
        return []
    return [
        Relationship(
            type="djTransition",
            weight=row["pmi"],
            detail={"raw_count": row["raw_count"], "pmi": row["pmi"]},
        )
    ]


def _explain_symmetric(
    db: sqlite3.Connection,
    source_id: int,
    target_id: int,
    type_name: str,
    table: str,
    weight_col: str | None,
    detail_cols: list[str],
    *,
    col_a: str = "artist_a_id",
    col_b: str = "artist_b_id",
) -> list[Relationship]:
    """Query a single symmetric edge between two specific artists."""
    col_list = ", ".join(detail_cols)
    try:
        row = db.execute(
            f"SELECT {col_list} FROM {table} "  # noqa: S608
            f"WHERE ({col_a} = ? AND {col_b} = ?) "
            f"   OR ({col_a} = ? AND {col_b} = ?)",
            (source_id, target_id, target_id, source_id),
        ).fetchone()
    except sqlite3.OperationalError:
        return []  # table doesn't exist
    if row is None:
        return []
    weight = float(row[weight_col]) if weight_col else 1.0
    return [Relationship(type=type_name, weight=weight, detail=_parse_detail(row, detail_cols))]
