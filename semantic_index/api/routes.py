"""Graph API query endpoints: search, neighbors, explain."""

from __future__ import annotations

import json
import logging
import math
import sqlite3
from collections import defaultdict
from enum import StrEnum

from fastapi import APIRouter, Depends, HTTPException, Query

from semantic_index.api.database import get_db
from semantic_index.api.schemas import (
    ArtistDetail,
    ArtistSummary,
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


class EdgeType(StrEnum):
    """Supported edge types for neighbor queries."""

    DJ_TRANSITION = "djTransition"
    SHARED_PERSONNEL = "sharedPersonnel"
    SHARED_STYLE = "sharedStyle"
    LABEL_FAMILY = "labelFamily"
    COMPILATION = "compilation"
    CROSS_REFERENCE = "crossReference"
    WIKIDATA_INFLUENCE = "wikidataInfluence"


def _artist_summary(row: sqlite3.Row) -> ArtistSummary:
    return ArtistSummary(
        id=row["id"],
        canonical_name=row["canonical_name"],
        genre=row["genre"],
        total_plays=row["total_plays"],
    )


def _get_artist_or_404(db: sqlite3.Connection, artist_id: int) -> ArtistSummary:
    row = db.execute(
        "SELECT id, canonical_name, genre, total_plays FROM artist WHERE id = ?",
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
    row = db.execute(
        "SELECT a.id, a.canonical_name, a.genre, a.total_plays FROM artist a "
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
    """Case-insensitive artist name search, ordered by total_plays descending."""
    rows = db.execute(
        "SELECT id, canonical_name, genre, total_plays FROM artist "
        "WHERE canonical_name LIKE ? ORDER BY total_plays DESC LIMIT ?",
        (f"%{q}%", limit),
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


@router.get("/artists/{artist_id}/neighbors", response_model=NeighborsResponse)
def get_neighbors(
    artist_id: int,
    type: EdgeType = Query(default=EdgeType.DJ_TRANSITION),
    limit: int = Query(default=20, ge=1, le=100),
    month: int | None = Query(default=None, ge=1, le=12),
    dj_id: int | None = Query(default=None, ge=1),
    db: sqlite3.Connection = Depends(get_db),
) -> NeighborsResponse:
    """Return neighbors of an artist by edge type, ordered by weight descending.

    When ``month`` or ``dj_id`` are provided and the edge type is ``djTransition``,
    computes PMI dynamically from the play table filtered by the given facets.
    Facet parameters are ignored for other edge types.
    """
    artist = _get_artist_or_404(db, artist_id)
    has_facets = month is not None or dj_id is not None
    if has_facets and type == EdgeType.DJ_TRANSITION:
        neighbors = _neighbors_dj_transition_faceted(db, artist_id, limit, month, dj_id)
    else:
        neighbors = _query_neighbors(db, artist_id, type, limit)
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

    artist_rows = db.execute(
        "SELECT id, canonical_name, genre, total_plays FROM artist WHERE entity_id = ?",
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
) -> list[NeighborEntry]:
    """Query neighbors for a given edge type."""
    match edge_type:
        case EdgeType.DJ_TRANSITION:
            return _neighbors_dj_transition(db, artist_id, limit)
        case EdgeType.SHARED_PERSONNEL:
            return _neighbors_shared_personnel(db, artist_id, limit)
        case EdgeType.SHARED_STYLE:
            return _neighbors_symmetric(
                db, artist_id, limit, "shared_style", "jaccard", ["jaccard", "shared_tags"]
            )
        case EdgeType.LABEL_FAMILY:
            return _neighbors_symmetric(
                db, artist_id, limit, "label_family", None, ["shared_labels"]
            )
        case EdgeType.COMPILATION:
            return _neighbors_symmetric(
                db,
                artist_id,
                limit,
                "compilation",
                "compilation_count",
                ["compilation_count", "compilation_titles"],
            )
        case EdgeType.CROSS_REFERENCE:
            return _neighbors_cross_reference(db, artist_id, limit)
        case EdgeType.WIKIDATA_INFLUENCE:
            return _neighbors_wikidata_influence(db, artist_id, limit)


def _neighbors_dj_transition(
    db: sqlite3.Connection, artist_id: int, limit: int
) -> list[NeighborEntry]:
    rows = db.execute(
        "SELECT a.id, a.canonical_name, a.genre, a.total_plays, "
        "  dt.raw_count, dt.pmi "
        "FROM dj_transition dt "
        "JOIN artist a ON a.id = dt.target_id "
        "WHERE dt.source_id = ? "
        "UNION ALL "
        "SELECT a.id, a.canonical_name, a.genre, a.total_plays, "
        "  dt.raw_count, dt.pmi "
        "FROM dj_transition dt "
        "JOIN artist a ON a.id = dt.source_id "
        "WHERE dt.target_id = ? "
        "ORDER BY pmi DESC LIMIT ?",
        (artist_id, artist_id, limit),
    ).fetchall()
    return [
        NeighborEntry(
            artist=_artist_summary(r),
            weight=r["pmi"],
            detail={"raw_count": r["raw_count"], "pmi": r["pmi"]},
        )
        for r in rows
    ]


def _neighbors_dj_transition_faceted(
    db: sqlite3.Connection,
    artist_id: int,
    limit: int,
    month: int | None = None,
    dj_id: int | None = None,
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

    # Step 4: Compute PMI and filter
    scored: list[tuple[int, int, float]] = []  # (neighbor_id, raw_count, pmi)
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
            scored.append((neighbor_id, raw_count, pmi))

    scored.sort(key=lambda x: x[2], reverse=True)
    scored = scored[:limit]

    if not scored:
        return []

    # Step 5: Build response (look up artist summaries)
    neighbor_ids = [s[0] for s in scored]
    placeholders = ",".join("?" * len(neighbor_ids))
    artist_rows = db.execute(
        f"SELECT id, canonical_name, genre, total_plays FROM artist WHERE id IN ({placeholders})",  # noqa: S608
        neighbor_ids,
    ).fetchall()
    artist_map = {r["id"]: _artist_summary(r) for r in artist_rows}

    return [
        NeighborEntry(
            artist=artist_map[nid],
            weight=pmi,
            detail={"raw_count": raw_count, "pmi": round(pmi, 4)},
        )
        for nid, raw_count, pmi in scored
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


def _neighbors_symmetric(
    db: sqlite3.Connection,
    artist_id: int,
    limit: int,
    table: str,
    weight_col: str | None,
    detail_cols: list[str],
) -> list[NeighborEntry]:
    order = f"{weight_col} DESC" if weight_col else "1"
    rows = db.execute(
        f"SELECT a.id, a.canonical_name, a.genre, a.total_plays, "  # noqa: S608
        f"  {', '.join(f'e.{c}' for c in detail_cols)} "
        f"FROM {table} e "
        f"JOIN artist a ON a.id = e.artist_b_id "
        f"WHERE e.artist_a_id = ? "
        f"UNION ALL "
        f"SELECT a.id, a.canonical_name, a.genre, a.total_plays, "
        f"  {', '.join(f'e.{c}' for c in detail_cols)} "
        f"FROM {table} e "
        f"JOIN artist a ON a.id = e.artist_a_id "
        f"WHERE e.artist_b_id = ? "
        f"ORDER BY {order} LIMIT ?",
        (artist_id, artist_id, limit),
    ).fetchall()
    results = []
    for r in rows:
        detail = {}
        for col in detail_cols:
            val = r[col]
            if isinstance(val, str) and val.startswith("["):
                val = json.loads(val)
            detail[col] = val
        weight = float(r[weight_col]) if weight_col else 1.0
        results.append(NeighborEntry(artist=_artist_summary(r), weight=weight, detail=detail))
    return results


def _neighbors_shared_personnel(
    db: sqlite3.Connection, artist_id: int, limit: int
) -> list[NeighborEntry]:
    """Query shared personnel + band membership as a single edge type."""
    # Try shared_personnel table first, then union with member_of
    try:
        rows = db.execute(
            "SELECT a.id, a.canonical_name, a.genre, a.total_plays, "
            "  e.shared_count AS weight, e.shared_names AS detail, 'personnel' AS source "
            "FROM shared_personnel e "
            "JOIN artist a ON a.id = e.artist_b_id "
            "WHERE e.artist_a_id = ? "
            "UNION ALL "
            "SELECT a.id, a.canonical_name, a.genre, a.total_plays, "
            "  e.shared_count, e.shared_names, 'personnel' "
            "FROM shared_personnel e "
            "JOIN artist a ON a.id = e.artist_a_id "
            "WHERE e.artist_b_id = ? "
            "UNION ALL "
            "SELECT a.id, a.canonical_name, a.genre, a.total_plays, "
            "  1, '\"member\"', 'member' "
            "FROM member_of m "
            "JOIN artist a ON a.id = m.member_id "
            "WHERE m.group_id = ? "
            "UNION ALL "
            "SELECT a.id, a.canonical_name, a.genre, a.total_plays, "
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


def _neighbors_cross_reference(
    db: sqlite3.Connection, artist_id: int, limit: int
) -> list[NeighborEntry]:
    rows = db.execute(
        "SELECT a.id, a.canonical_name, a.genre, a.total_plays, "
        "  cr.comment, cr.source "
        "FROM cross_reference cr "
        "JOIN artist a ON a.id = cr.artist_b_id "
        "WHERE cr.artist_a_id = ? "
        "UNION ALL "
        "SELECT a.id, a.canonical_name, a.genre, a.total_plays, "
        "  cr.comment, cr.source "
        "FROM cross_reference cr "
        "JOIN artist a ON a.id = cr.artist_a_id "
        "WHERE cr.artist_b_id = ? "
        "LIMIT ?",
        (artist_id, artist_id, limit),
    ).fetchall()
    return [
        NeighborEntry(
            artist=_artist_summary(r),
            weight=1.0,
            detail={"comment": r["comment"], "source": r["source"]},
        )
        for r in rows
    ]


def _query_explain(
    db: sqlite3.Connection,
    source_id: int,
    target_id: int,
    edge_type: EdgeType,
) -> list[Relationship]:
    """Query a single edge type between two specific artists."""
    match edge_type:
        case EdgeType.DJ_TRANSITION:
            return _explain_dj_transition(db, source_id, target_id)
        case EdgeType.SHARED_PERSONNEL:
            return _explain_symmetric(
                db,
                source_id,
                target_id,
                "sharedPersonnel",
                "shared_personnel",
                "shared_count",
                ["shared_count", "shared_names"],
            )
        case EdgeType.SHARED_STYLE:
            return _explain_symmetric(
                db,
                source_id,
                target_id,
                "sharedStyle",
                "shared_style",
                "jaccard",
                ["jaccard", "shared_tags"],
            )
        case EdgeType.LABEL_FAMILY:
            return _explain_symmetric(
                db, source_id, target_id, "labelFamily", "label_family", None, ["shared_labels"]
            )
        case EdgeType.COMPILATION:
            return _explain_symmetric(
                db,
                source_id,
                target_id,
                "compilation",
                "compilation",
                "compilation_count",
                ["compilation_count", "compilation_titles"],
            )
        case EdgeType.CROSS_REFERENCE:
            return _explain_cross_reference(db, source_id, target_id)
        case EdgeType.WIKIDATA_INFLUENCE:
            return _explain_wikidata_influence(db, source_id, target_id)


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
) -> list[Relationship]:
    col_list = ", ".join(detail_cols)
    row = db.execute(
        f"SELECT {col_list} FROM {table} "  # noqa: S608
        f"WHERE (artist_a_id = ? AND artist_b_id = ?) "
        f"   OR (artist_a_id = ? AND artist_b_id = ?)",
        (source_id, target_id, target_id, source_id),
    ).fetchone()
    if row is None:
        return []
    detail = {}
    for col in detail_cols:
        val = row[col]
        if isinstance(val, str) and val.startswith("["):
            val = json.loads(val)
        detail[col] = val
    weight = float(row[weight_col]) if weight_col else 1.0
    return [Relationship(type=type_name, weight=weight, detail=detail)]


def _explain_cross_reference(
    db: sqlite3.Connection, source_id: int, target_id: int
) -> list[Relationship]:
    rows = db.execute(
        "SELECT comment, source FROM cross_reference "
        "WHERE (artist_a_id = ? AND artist_b_id = ?) "
        "   OR (artist_a_id = ? AND artist_b_id = ?)",
        (source_id, target_id, target_id, source_id),
    ).fetchall()
    return [
        Relationship(
            type="crossReference",
            weight=1.0,
            detail={"comment": r["comment"], "source": r["source"]},
        )
        for r in rows
    ]


def _neighbors_wikidata_influence(
    db: sqlite3.Connection, artist_id: int, limit: int
) -> list[NeighborEntry]:
    """Query Wikidata influence neighbors in both directions."""
    rows = db.execute(
        "SELECT a.id, a.canonical_name, a.genre, a.total_plays, "
        "  wi.source_qid, wi.target_qid "
        "FROM wikidata_influence wi "
        "JOIN artist a ON a.id = wi.target_id "
        "WHERE wi.source_id = ? "
        "UNION ALL "
        "SELECT a.id, a.canonical_name, a.genre, a.total_plays, "
        "  wi.source_qid, wi.target_qid "
        "FROM wikidata_influence wi "
        "JOIN artist a ON a.id = wi.source_id "
        "WHERE wi.target_id = ? "
        "LIMIT ?",
        (artist_id, artist_id, limit),
    ).fetchall()
    return [
        NeighborEntry(
            artist=_artist_summary(r),
            weight=1.0,
            detail={"source_qid": r["source_qid"], "target_qid": r["target_qid"]},
        )
        for r in rows
    ]


def _explain_wikidata_influence(
    db: sqlite3.Connection, source_id: int, target_id: int
) -> list[Relationship]:
    """Query Wikidata influence edge between two specific artists."""
    row = db.execute(
        "SELECT source_qid, target_qid FROM wikidata_influence "
        "WHERE (source_id = ? AND target_id = ?) OR (source_id = ? AND target_id = ?)",
        (source_id, target_id, target_id, source_id),
    ).fetchone()
    if row is None:
        return []
    return [
        Relationship(
            type="wikidataInfluence",
            weight=1.0,
            detail={"source_qid": row["source_qid"], "target_qid": row["target_qid"]},
        )
    ]
