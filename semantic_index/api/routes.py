"""Graph API query endpoints: search, neighbors, explain."""

from __future__ import annotations

import json
import sqlite3
from enum import StrEnum

from fastapi import APIRouter, Depends, HTTPException, Query

from semantic_index.api.database import get_db
from semantic_index.api.schemas import (
    ArtistDetail,
    ArtistSummary,
    EntityArtists,
    ExplainResponse,
    NeighborEntry,
    NeighborsResponse,
    Relationship,
    SearchResponse,
)

router = APIRouter(prefix="/graph", tags=["graph"])


class EdgeType(StrEnum):
    """Supported edge types for neighbor queries."""

    DJ_TRANSITION = "djTransition"
    SHARED_PERSONNEL = "sharedPersonnel"
    SHARED_STYLE = "sharedStyle"
    LABEL_FAMILY = "labelFamily"
    COMPILATION = "compilation"
    CROSS_REFERENCE = "crossReference"


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
    db: sqlite3.Connection = Depends(get_db),
) -> NeighborsResponse:
    """Return neighbors of an artist by edge type, ordered by weight descending."""
    artist = _get_artist_or_404(db, artist_id)
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
            "  e.wikidata_qid "
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
            return _neighbors_symmetric(
                db,
                artist_id,
                limit,
                "shared_personnel",
                "shared_count",
                ["shared_count", "shared_names"],
            )
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
