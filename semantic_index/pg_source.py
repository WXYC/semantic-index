"""Query Backend-Service PostgreSQL for pipeline input data.

Replaces SQL dump parsing (``sql_parser.py``) for the nightly sync pipeline.
Each function queries ``wxyc_schema.*`` tables and returns the same pipeline
types that ``run_pipeline.py`` constructs from raw SQL tuples.

Schema mappings (PG → pipeline types):
    - ``wxyc_schema.artists`` → ``LibraryCode``
      (id, genre_id from genre_artist_crossreference, artist_name → presentation_name)
    - ``wxyc_schema.library`` → ``LibraryRelease``
      (id, artist_id → library_code_id)
    - ``wxyc_schema.flowsheet`` → ``FlowsheetEntry``
      (entry_type = 'track' filter replaces type_code < 7)
    - ``wxyc_schema.shows`` → show-to-DJ mapping
      (show.id as key, primary_dj_id as value)

Requires a psycopg connection with ``dict_row`` row factory.
"""

from __future__ import annotations

import logging
from typing import Any

from semantic_index.models import FlowsheetEntry, LibraryCode, LibraryRelease

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQL queries
# ---------------------------------------------------------------------------

_GENRES_SQL = """\
SELECT id, genre_name
FROM wxyc_schema.genres
ORDER BY id
"""

_ARTISTS_SQL = """\
SELECT id, artist_name
FROM wxyc_schema.artists
ORDER BY id
"""

_GENRE_ARTIST_XREF_SQL = """\
SELECT artist_id, genre_id
FROM wxyc_schema.genre_artist_crossreference
ORDER BY artist_id, artist_genre_code
"""

_LIBRARY_SQL = """\
SELECT id, artist_id
FROM wxyc_schema.library
ORDER BY id
"""

_FLOWSHEET_SQL = """\
SELECT id, artist_name, track_title, album_title, record_label,
       show_id, play_order, album_id, request_flag,
       EXTRACT(EPOCH FROM add_time)::bigint AS add_time_epoch,
       legacy_entry_id
FROM wxyc_schema.flowsheet
WHERE entry_type = 'track'
ORDER BY show_id, play_order
"""

_SHOWS_SQL = """\
SELECT id, primary_dj_id, legacy_show_id
FROM wxyc_schema.shows
ORDER BY id
"""

_ARTIST_XREF_SQL = """\
SELECT source_artist_id, target_artist_id, comment
FROM wxyc_schema.artist_crossreference
"""

_RELEASE_XREF_SQL = """\
SELECT artist_id, library_id, comment
FROM wxyc_schema.artist_library_crossreference
"""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_genres(conn: Any) -> dict[int, str]:
    """Load genre ID → name mapping from ``wxyc_schema.genres``.

    Args:
        conn: psycopg connection (dict_row factory).

    Returns:
        Dict mapping genre ID to genre name.
    """
    rows = conn.execute(_GENRES_SQL).fetchall()
    return {row["id"]: row["genre_name"] for row in rows}


def load_catalog(conn: Any) -> tuple[list[LibraryCode], list[LibraryRelease]]:
    """Load library catalog (artists + releases) from PG.

    Queries ``wxyc_schema.artists``, ``genre_artist_crossreference``, and
    ``library`` to build the ``LibraryCode`` and ``LibraryRelease`` lists
    that the resolver needs.

    For artists with multiple genre entries, the first one (lowest
    ``artist_genre_code``) is used. Artists without a genre entry get
    ``genre_id=0``.

    Args:
        conn: psycopg connection (dict_row factory).

    Returns:
        Tuple of (codes, releases).
    """
    # Build artist_id → genre_id mapping (first genre wins)
    genre_xref_rows = conn.execute(_GENRE_ARTIST_XREF_SQL).fetchall()
    artist_genre: dict[int, int] = {}
    for row in genre_xref_rows:
        artist_id = row["artist_id"]
        if artist_id not in artist_genre:
            artist_genre[artist_id] = row["genre_id"]

    # Build LibraryCode list from artists
    artist_rows = conn.execute(_ARTISTS_SQL).fetchall()
    codes = [
        LibraryCode(
            id=row["id"],
            genre_id=artist_genre.get(row["id"], 0),
            presentation_name=row["artist_name"],
        )
        for row in artist_rows
    ]

    # Build LibraryRelease list from library
    library_rows = conn.execute(_LIBRARY_SQL).fetchall()
    releases = [
        LibraryRelease(id=row["id"], library_code_id=row["artist_id"]) for row in library_rows
    ]

    logger.info("Loaded catalog: %d artists, %d releases", len(codes), len(releases))
    return codes, releases


def load_flowsheet_entries(conn: Any) -> list[FlowsheetEntry]:
    """Load music flowsheet entries from PG.

    Queries ``wxyc_schema.flowsheet`` filtered to ``entry_type = 'track'``
    and maps PG columns to ``FlowsheetEntry`` fields:

    - ``album_id`` → ``library_release_id`` (NULL → 0)
    - ``request_flag`` boolean → int (0 or 1)
    - ``add_time`` timestamptz → epoch seconds int (via EXTRACT(EPOCH ...))
    - ``entry_type`` enum 'track' → ``entry_type_code = 1``

    Args:
        conn: psycopg connection (dict_row factory).

    Returns:
        List of FlowsheetEntry, ordered by (show_id, play_order).
    """
    rows = conn.execute(_FLOWSHEET_SQL).fetchall()
    entries: list[FlowsheetEntry] = []

    for row in rows:
        album_id = row["album_id"]
        add_time_epoch = row["add_time_epoch"]

        entries.append(
            FlowsheetEntry(
                id=row["id"],
                artist_name=row["artist_name"] or "",
                song_title=row["track_title"] or "",
                release_title=row["album_title"] or "",
                label_name=row["record_label"] or "",
                show_id=row["show_id"] if row["show_id"] is not None else 0,
                sequence=row["play_order"],
                library_release_id=album_id if isinstance(album_id, int) else 0,
                entry_type_code=1,  # all rows are 'track' (filtered in SQL)
                request_flag=1 if row["request_flag"] else 0,
                start_time=int(add_time_epoch) if add_time_epoch is not None else None,
            )
        )

    logger.info("Loaded %d flowsheet track entries", len(entries))
    return entries


def load_shows(conn: Any) -> tuple[dict[int, int | str], dict[int, str]]:
    """Load show → DJ mapping from PG.

    Queries ``wxyc_schema.shows`` for ``primary_dj_id``. Shows where
    ``primary_dj_id`` is NULL are skipped.

    Args:
        conn: psycopg connection (dict_row factory).

    Returns:
        Tuple of (show_to_dj, show_dj_names).
        ``show_to_dj`` maps show ID to DJ identifier (string or int).
        ``show_dj_names`` maps show ID to display name (currently same as DJ ID).
    """
    rows = conn.execute(_SHOWS_SQL).fetchall()
    show_to_dj: dict[int, int | str] = {}
    show_dj_names: dict[int, str] = {}

    for row in rows:
        dj_id = row["primary_dj_id"]
        if dj_id is not None:
            show_id = row["id"]
            show_to_dj[show_id] = dj_id
            show_dj_names[show_id] = str(dj_id)

    logger.info("Loaded %d shows, %d with DJ mapping", len(rows), len(show_to_dj))
    return show_to_dj, show_dj_names


def load_cross_references(
    conn: Any,
) -> tuple[list[tuple], list[tuple]]:
    """Load cross-reference data from PG.

    Returns tuples in the shape expected by ``CrossReferenceExtractor``:
    ``(id, source_artist_id, target_artist_id, comment)`` for artist xrefs,
    ``(id, artist_id, library_id, comment)`` for release xrefs.

    The PG tables don't have a row ID, so a synthetic ``0`` is used.

    Args:
        conn: psycopg connection (dict_row factory).

    Returns:
        Tuple of (artist_xrefs, release_xrefs).
    """
    artist_rows = conn.execute(_ARTIST_XREF_SQL).fetchall()
    artist_xrefs = [
        (0, row["source_artist_id"], row["target_artist_id"], row.get("comment"))
        for row in artist_rows
    ]

    release_rows = conn.execute(_RELEASE_XREF_SQL).fetchall()
    release_xrefs = [
        (0, row["artist_id"], row["library_id"], row.get("comment")) for row in release_rows
    ]

    logger.info(
        "Loaded cross-references: %d artist, %d release",
        len(artist_xrefs),
        len(release_xrefs),
    )
    return artist_xrefs, release_xrefs
