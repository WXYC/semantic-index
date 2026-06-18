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
from collections.abc import Callable
from typing import Any

from semantic_index.models import FlowsheetEntry, LibraryCode, LibraryRelease

logger = logging.getLogger(__name__)

# Rows fetched per libpq round-trip for the high-cardinality server-side
# cursors (catalog, flowsheet, cross-references). ~10K bounds the libpq buffer
# to a few MiB while amortising network round-trips over the ~462K-row catalog.
_ITERSIZE = 10_000

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
SELECT id, primary_dj_id, legacy_dj_name, legacy_show_id
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
# Streaming helper
# ---------------------------------------------------------------------------


def _stream_into_list[T](conn: Any, name: str, sql: str, build: Callable[[Any], T]) -> list[T]:
    """Run ``sql`` through a named (server-side) cursor and build a list of rows.

    libpq fetches rows in ``_ITERSIZE`` chunks rather than buffering the full
    result set in C memory, and *build* is applied per row so only the built
    objects are retained -- the dict rows are freed incrementally. This is the
    memory-bounding pattern the high-cardinality loaders need to fit the sync
    under the 1 GiB cgroup cap (WXYC/semantic-index#345).

    A named cursor is used inside an explicit transaction because the
    connection is autocommit=True (named cursors error outside a transaction on
    autocommit connections), and ``WITHOUT HOLD`` is the psycopg3 default, so
    iteration MUST complete inside the ``with`` block -- hence materialising
    into a list here rather than yielding rows to the caller.

    Centralising this means every high-cardinality loader gets the
    ``itersize`` + transaction guarantee structurally; a caller can't forget
    to set ``itersize`` (whose psycopg3 default of 100 would reintroduce
    thousands of round-trips).
    """
    with conn.transaction(), conn.cursor(name=name) as cursor:
        cursor.itersize = _ITERSIZE
        cursor.execute(sql)
        return [build(row) for row in cursor]


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

    All three queries (genre xref, artists, releases) stream through named
    (server-side) cursors via :func:`_stream_into_list` so libpq fetches in
    ``_ITERSIZE`` chunks rather than buffering each full result set. This is
    the loader the nightly sync OOM-died in (WXYC/semantic-index#345).

    Args:
        conn: psycopg connection (dict_row factory, autocommit=True).

    Returns:
        Tuple of (codes, releases).
    """
    # Build artist_id → genre_id mapping (first genre wins). Inlined rather
    # than via _stream_into_list because it accumulates a dedup dict, not a
    # list, but uses the same named-cursor / itersize streaming discipline.
    artist_genre: dict[int, int] = {}
    with conn.transaction(), conn.cursor(name="catalog_genre_xref") as cursor:
        cursor.itersize = _ITERSIZE
        cursor.execute(_GENRE_ARTIST_XREF_SQL)
        for row in cursor:
            artist_id = row["artist_id"]
            if artist_id not in artist_genre:
                artist_genre[artist_id] = row["genre_id"]

    codes = _stream_into_list(
        conn,
        "catalog_artists",
        _ARTISTS_SQL,
        lambda row: LibraryCode(
            id=row["id"],
            genre_id=artist_genre.get(row["id"], 0),
            presentation_name=row["artist_name"],
        ),
    )

    releases = _stream_into_list(
        conn,
        "catalog_library",
        _LIBRARY_SQL,
        lambda row: LibraryRelease(id=row["id"], library_code_id=row["artist_id"]),
    )

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

    Streams through a named (server-side) cursor via :func:`_stream_into_list`
    so libpq fetches in ``_ITERSIZE`` chunks rather than buffering the full
    ~1M-row result set in C memory.

    Args:
        conn: psycopg connection (dict_row factory, autocommit=True).

    Returns:
        List of FlowsheetEntry, ordered by (show_id, play_order).
    """
    entries = _stream_into_list(conn, "flowsheet_load", _FLOWSHEET_SQL, _build_flowsheet_entry)
    logger.info("Loaded %d flowsheet track entries", len(entries))
    return entries


def _build_flowsheet_entry(row: Any) -> FlowsheetEntry:
    """Map one ``wxyc_schema.flowsheet`` dict row to a FlowsheetEntry.

    - ``album_id`` → ``library_release_id`` (NULL → 0)
    - ``request_flag`` boolean → int (0 or 1)
    - ``add_time`` timestamptz → epoch seconds int (via EXTRACT(EPOCH ...))
    - ``entry_type`` enum 'track' → ``entry_type_code = 1``
    """
    album_id = row["album_id"]
    add_time_epoch = row["add_time_epoch"]
    return FlowsheetEntry(
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


def load_shows(conn: Any) -> tuple[dict[int, int | str], dict[int, str]]:
    """Load show → DJ mapping from PG.

    Queries ``wxyc_schema.shows`` for ``primary_dj_id`` (auth user FK) with
    ``legacy_dj_name`` as a fallback for shows imported from tubafrenzy.

    Args:
        conn: psycopg connection (dict_row factory).

    Returns:
        Tuple of (show_to_dj, show_dj_names).
        ``show_to_dj`` maps show ID to DJ identifier (string or int).
        ``show_dj_names`` maps show ID to display name.
    """
    rows = conn.execute(_SHOWS_SQL).fetchall()
    show_to_dj: dict[int, int | str] = {}
    show_dj_names: dict[int, str] = {}

    for row in rows:
        dj_id = row["primary_dj_id"]
        legacy_name = row.get("legacy_dj_name")
        if dj_id is not None:
            show_id = row["id"]
            show_to_dj[show_id] = dj_id
            show_dj_names[show_id] = str(dj_id)
        elif legacy_name:
            show_id = row["id"]
            show_to_dj[show_id] = legacy_name
            show_dj_names[show_id] = legacy_name

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

    Both queries stream through named (server-side) cursors via
    :func:`_stream_into_list` rather than ``.fetchall()``-ing each table under
    ``dict_row`` (WXYC/semantic-index#345).

    Args:
        conn: psycopg connection (dict_row factory, autocommit=True).

    Returns:
        Tuple of (artist_xrefs, release_xrefs).
    """
    artist_xrefs = _stream_into_list(
        conn,
        "artist_xref",
        _ARTIST_XREF_SQL,
        lambda row: (0, row["source_artist_id"], row["target_artist_id"], row.get("comment")),
    )

    release_xrefs = _stream_into_list(
        conn,
        "release_xref",
        _RELEASE_XREF_SQL,
        lambda row: (0, row["artist_id"], row["library_id"], row.get("comment")),
    )

    logger.info(
        "Loaded cross-references: %d artist, %d release",
        len(artist_xrefs),
        len(release_xrefs),
    )
    return artist_xrefs, release_xrefs
