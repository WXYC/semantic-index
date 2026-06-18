"""Tests for pg_source — PostgreSQL query functions for the nightly sync pipeline.

Mocks psycopg cursors to verify column mapping from the Backend-Service
PG schema (wxyc_schema.*) to pipeline types (FlowsheetEntry, LibraryCode,
LibraryRelease, etc.).
"""

from unittest.mock import MagicMock

from semantic_index.models import LibraryRelease

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_conn_with_rows(rows: list[dict]) -> MagicMock:
    """Create a mock psycopg connection supporting both cursor patterns.

    1. **Client-side** (``conn.execute(SQL).fetchall()`` or iteration):
       used by the small-table loaders ``load_genres`` and ``load_shows``.
    2. **Server-side** (``with conn.transaction(): with conn.cursor(name=...)
       as cur: cur.execute(SQL); for row in cur``): used by
       ``load_flowsheet_entries`` to bound libpq's row buffer.

    The high-cardinality loaders (``load_catalog``, ``load_cross_references``)
    are also server-side but run multiple distinct queries, so their tests use
    ``_mock_conn_with_queries`` (which routes rows by query substring) rather
    than this single-row-set helper.

    Each row is a dict (simulating psycopg's dict_row factory). ``__iter__``
    uses ``side_effect`` (not ``return_value``) so each call returns a fresh
    iterator -- a test that iterates twice won't see a silently-exhausted
    sequence on the second pass.
    """
    mock_conn = MagicMock()

    # Client-side cursor pattern
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = rows
    mock_cursor.__iter__.side_effect = lambda: iter(rows)
    mock_conn.execute.return_value = mock_cursor

    # Server-side cursor pattern: conn.transaction() and conn.cursor(name=...)
    # both need to behave as context managers. MagicMock handles __enter__/
    # __exit__ implicitly, but we need __enter__ to return a usable mock and
    # __iter__ to yield rows.
    named_cursor = MagicMock()
    named_cursor.__enter__.return_value = named_cursor
    named_cursor.__exit__.return_value = None
    named_cursor.__iter__.side_effect = lambda: iter(rows)
    mock_conn.cursor.return_value = named_cursor

    tx = MagicMock()
    tx.__enter__.return_value = tx
    tx.__exit__.return_value = None
    mock_conn.transaction.return_value = tx

    return mock_conn


def _mock_conn_with_queries(query_results: dict[str, list[dict]]) -> MagicMock:
    """Create a mock connection that routes results by query substring.

    *query_results* maps a substring of the SQL query to the rows it should
    return. Supports BOTH cursor patterns (like ``_mock_conn_with_rows``):

    1. **Client-side** -- ``conn.execute(SQL).fetchall()`` (small-table
       loaders: genres, shows).
    2. **Server-side** -- ``with conn.transaction(): with conn.cursor(name=...)
       as cur: cur.execute(SQL); for row in cur`` (the high-cardinality
       loaders: catalog, cross-references).

    Each server-side cursor created during the call is recorded on
    ``conn.created_cursors`` so discipline tests can assert no ``.fetchall()``
    was called and ``itersize`` was set.
    """
    mock_conn = MagicMock()
    created_cursors: list[MagicMock] = []

    def _rows_for(query: str) -> list[dict]:
        for key, rows in query_results.items():
            if key in query:
                return rows
        return []

    # Client-side cursor pattern
    def _conn_execute(query, params=None):
        cursor = MagicMock()
        rows = _rows_for(query)
        cursor.fetchall.return_value = rows
        cursor.__iter__.side_effect = lambda: iter(rows)
        return cursor

    mock_conn.execute.side_effect = _conn_execute

    # Server-side cursor pattern: a fresh named cursor per conn.cursor() call,
    # whose iterated rows are determined by the query passed to cursor.execute().
    def _make_named_cursor(*args, **kwargs):
        cur = MagicMock()
        cur.__enter__.return_value = cur
        cur.__exit__.return_value = None
        holder: dict[str, list[dict]] = {"rows": []}

        def _cursor_execute(query, params=None):
            holder["rows"] = _rows_for(query)

        cur.execute.side_effect = _cursor_execute
        cur.__iter__.side_effect = lambda: iter(holder["rows"])
        created_cursors.append(cur)
        return cur

    mock_conn.cursor.side_effect = _make_named_cursor

    tx = MagicMock()
    tx.__enter__.return_value = tx
    tx.__exit__.return_value = None
    mock_conn.transaction.return_value = tx

    mock_conn.created_cursors = created_cursors
    return mock_conn


def _instrument_cursor_transaction_depth(conn: MagicMock) -> list[int]:
    """Record, per ``conn.cursor(name=...)`` call, how many ``conn.transaction()``
    contexts are active at that moment.

    Returns a list (one entry per cursor opened) the caller inspects AFTER
    running the loader. Every entry must be ``>= 1``: a named (server-side)
    cursor opened with no active transaction raises on a real autocommit
    connection.

    This expresses the actual invariant -- "each cursor is opened inside a
    transaction" -- without baking in HOW MANY transactions there are, so it
    passes whether the loader opens one transaction per cursor or wraps several
    cursors in a single transaction, while still catching a cursor opened
    outside any transaction (e.g. the inlined genre-xref cursor losing its
    ``conn.transaction()``).
    """
    depth = {"n": 0}
    depths: list[int] = []
    tx = conn.transaction.return_value

    def _enter(*args, **kwargs):
        depth["n"] += 1
        return tx

    def _exit(*args, **kwargs):
        depth["n"] -= 1
        return None

    tx.__enter__.side_effect = _enter
    tx.__exit__.side_effect = _exit

    inner_cursor_factory = conn.cursor.side_effect

    def _record(*args, **kwargs):
        depths.append(depth["n"])
        return inner_cursor_factory(*args, **kwargs)

    conn.cursor.side_effect = _record
    return depths


# ===========================================================================
# load_genres
# ===========================================================================


class TestLoadGenres:
    """load_genres() queries wxyc_schema.genres and returns {id: name}."""

    def test_returns_genre_id_to_name_mapping(self):
        from semantic_index.pg_source import load_genres

        rows = [
            {"id": 6, "genre_name": "Hiphop"},
            {"id": 15, "genre_name": "Electronic"},
            {"id": 1, "genre_name": "Rock"},
        ]
        conn = _mock_conn_with_rows(rows)

        result = load_genres(conn)

        assert result == {6: "Hiphop", 15: "Electronic", 1: "Rock"}

    def test_empty_table_returns_empty_dict(self):
        from semantic_index.pg_source import load_genres

        conn = _mock_conn_with_rows([])
        assert load_genres(conn) == {}


# ===========================================================================
# load_catalog
# ===========================================================================


class TestLoadCatalog:
    """load_catalog() queries artists, genre_artist_crossreference, and library."""

    _DEFAULT_ARTISTS = [
        {"id": 1, "artist_name": "A Guy Called Gerald"},
        {"id": 2, "artist_name": "A Tribe Called Quest"},
    ]
    _DEFAULT_LIBRARY = [
        {"id": 10, "artist_id": 1},
        {"id": 11, "artist_id": 2},
    ]

    def _make_catalog_conn(
        self,
        artists: list[dict] | None = None,
        genre_xrefs: list[dict] | None = None,
        library: list[dict] | None = None,
    ) -> MagicMock:
        """Build a mock connection with canned results for catalog queries."""
        return _mock_conn_with_queries(
            {
                "genre_artist_crossreference": genre_xrefs if genre_xrefs is not None else [],
                "artists": artists if artists is not None else self._DEFAULT_ARTISTS,
                "library": library if library is not None else self._DEFAULT_LIBRARY,
            }
        )

    def test_returns_library_codes_from_artists(self):
        from semantic_index.pg_source import load_catalog

        conn = self._make_catalog_conn(
            artists=[
                {"id": 19516, "artist_name": "Autechre"},
                {"id": 100, "artist_name": "Stereolab"},
            ],
            genre_xrefs=[
                {"artist_id": 19516, "genre_id": 15},
                {"artist_id": 100, "genre_id": 1},
            ],
            library=[],
        )

        codes, releases = load_catalog(conn)

        assert len(codes) == 2
        autechre = next(c for c in codes if c.id == 19516)
        assert autechre.presentation_name == "Autechre"
        assert autechre.genre_id == 15

        stereolab = next(c for c in codes if c.id == 100)
        assert stereolab.presentation_name == "Stereolab"
        assert stereolab.genre_id == 1

    def test_artist_without_genre_gets_zero(self):
        """Artists not in genre_artist_crossreference get genre_id=0."""
        from semantic_index.pg_source import load_catalog

        conn = self._make_catalog_conn(
            artists=[{"id": 42, "artist_name": "Unknown Artist"}],
            genre_xrefs=[],
            library=[],
        )

        codes, _ = load_catalog(conn)

        assert len(codes) == 1
        assert codes[0].genre_id == 0

    def test_artist_with_multiple_genres_uses_first(self):
        """When an artist has multiple genre entries, use the first one returned."""
        from semantic_index.pg_source import load_catalog

        conn = self._make_catalog_conn(
            artists=[{"id": 5, "artist_name": "Genre Hopper"}],
            genre_xrefs=[
                {"artist_id": 5, "genre_id": 3},
                {"artist_id": 5, "genre_id": 7},
            ],
            library=[],
        )

        codes, _ = load_catalog(conn)

        assert len(codes) == 1
        assert codes[0].genre_id == 3

    def test_returns_library_releases(self):
        from semantic_index.pg_source import load_catalog

        conn = self._make_catalog_conn(
            artists=[{"id": 1, "artist_name": "A Guy Called Gerald"}],
            genre_xrefs=[{"artist_id": 1, "genre_id": 6}],
            library=[
                {"id": 10, "artist_id": 1},
                {"id": 11, "artist_id": 1},
            ],
        )

        _, releases = load_catalog(conn)

        assert len(releases) == 2
        assert releases[0] == LibraryRelease(id=10, library_code_id=1)
        assert releases[1] == LibraryRelease(id=11, library_code_id=1)

    def test_empty_tables_return_empty_lists(self):
        from semantic_index.pg_source import load_catalog

        conn = self._make_catalog_conn(artists=[], genre_xrefs=[], library=[])

        codes, releases = load_catalog(conn)

        assert codes == []
        assert releases == []


# ===========================================================================
# load_catalog -- server-side cursor lifecycle
# ===========================================================================


class TestLoadCatalogServerSideCursor:
    """``load_catalog`` loads ~462K artists + ~196K releases and is the phase
    the nightly sync OOM-dies in (WXYC/semantic-index#345). It MUST stream via
    server-side cursors -- one per query -- rather than buffering each full
    result set with ``.fetchall()`` under ``dict_row``.
    """

    _QUERIES = {
        "genre_artist_crossreference": [{"artist_id": 1, "genre_id": 15}],
        "artists": [{"id": 1, "artist_name": "Autechre"}],
        "library": [{"id": 10, "artist_id": 1}],
    }

    def test_does_not_buffer_with_fetchall_or_conn_execute(self):
        from semantic_index.pg_source import load_catalog

        conn = _mock_conn_with_queries(self._QUERIES)

        load_catalog(conn)

        conn.execute.assert_not_called()
        for cur in conn.created_cursors:
            cur.fetchall.assert_not_called()

    def test_opens_named_cursors_inside_transaction(self):
        from semantic_index.pg_source import load_catalog

        conn = _mock_conn_with_queries(self._QUERIES)
        cursor_open_depths = _instrument_cursor_transaction_depth(conn)

        load_catalog(conn)

        # Three queries (genre xref, artists, library) -> three named cursors.
        assert conn.cursor.call_count == 3
        for call in conn.cursor.call_args_list:
            assert call.kwargs.get("name"), (
                "every catalog cursor must be named -- a name is what makes it "
                "server-side in psycopg3"
            )
        # Each cursor MUST be opened inside an active transaction (named cursors
        # error outside one on an autocommit connection). Asserting the depth at
        # open time -- not the transaction count -- catches the inlined
        # genre-xref cursor being opened outside a transaction while still
        # allowing a future single-transaction-for-all-reads refactor.
        assert all(d >= 1 for d in cursor_open_depths), (
            f"every catalog cursor must be opened inside a transaction; "
            f"open-time transaction depths were {cursor_open_depths}"
        )

    def test_sets_itersize_to_bound_libpq_buffer(self):
        from semantic_index.pg_source import load_catalog

        conn = _mock_conn_with_queries(self._QUERIES)

        load_catalog(conn)

        assert conn.created_cursors, "expected server-side cursors to be created"
        for cur in conn.created_cursors:
            assert cur.itersize >= 1000, (
                f"itersize must be >=1000 to amortize round-trips over the "
                f"~462K-row catalog; got {cur.itersize}"
            )


# ===========================================================================
# load_flowsheet_entries
# ===========================================================================


class TestLoadFlowsheetEntries:
    """load_flowsheet_entries() queries wxyc_schema.flowsheet WHERE entry_type='track'."""

    def test_maps_pg_columns_to_flowsheet_entry(self):
        from semantic_index.pg_source import load_flowsheet_entries

        rows = [
            {
                "id": 155,
                "artist_name": "Jett Rink",
                "track_title": "Born Hungry",
                "album_title": "Bandwidth",
                "record_label": "WXYC",
                "show_id": 3210,
                "play_order": 2,
                "album_id": None,
                "request_flag": False,
                "add_time_epoch": 1099537681,
                "legacy_entry_id": 2,
            },
        ]
        conn = _mock_conn_with_rows(rows)

        entries = load_flowsheet_entries(conn)

        assert len(entries) == 1
        e = entries[0]
        assert e.id == 155
        assert e.artist_name == "Jett Rink"
        assert e.song_title == "Born Hungry"
        assert e.release_title == "Bandwidth"
        assert e.label_name == "WXYC"
        assert e.show_id == 3210
        assert e.sequence == 2
        assert e.library_release_id == 0  # album_id is NULL
        assert e.request_flag == 0  # boolean False → int 0
        assert e.entry_type_code == 1  # track entries get code 1
        assert e.start_time == 1099537681  # epoch seconds

    def test_request_flag_true_becomes_one(self):
        from semantic_index.pg_source import load_flowsheet_entries

        rows = [
            {
                "id": 200,
                "artist_name": "Cat Power",
                "track_title": "Cross Bones Style",
                "album_title": "Moon Pix",
                "record_label": "Matador Records",
                "show_id": 5000,
                "play_order": 10,
                "album_id": 42,
                "request_flag": True,
                "add_time_epoch": 1276632000,
                "legacy_entry_id": None,
            },
        ]
        conn = _mock_conn_with_rows(rows)

        entries = load_flowsheet_entries(conn)

        assert entries[0].request_flag == 1
        assert entries[0].library_release_id == 42

    def test_null_fields_become_empty_strings(self):
        """NULL artist_name, track_title, album_title, record_label → empty strings."""
        from semantic_index.pg_source import load_flowsheet_entries

        rows = [
            {
                "id": 300,
                "artist_name": None,
                "track_title": None,
                "album_title": None,
                "record_label": None,
                "show_id": 1000,
                "play_order": 1,
                "album_id": None,
                "request_flag": False,
                "add_time_epoch": None,
                "legacy_entry_id": None,
            },
        ]
        conn = _mock_conn_with_rows(rows)

        entries = load_flowsheet_entries(conn)

        e = entries[0]
        assert e.artist_name == ""
        assert e.song_title == ""
        assert e.release_title == ""
        assert e.label_name == ""
        assert e.start_time is None

    def test_null_show_id_becomes_zero(self):
        from semantic_index.pg_source import load_flowsheet_entries

        rows = [
            {
                "id": 400,
                "artist_name": "Sessa",
                "track_title": "Pequena Vertigem",
                "album_title": "Pequena Vertigem de Amor",
                "record_label": "Mexican Summer",
                "show_id": None,
                "play_order": 5,
                "album_id": None,
                "request_flag": False,
                "add_time_epoch": 1672531200,
                "legacy_entry_id": None,
            },
        ]
        conn = _mock_conn_with_rows(rows)

        entries = load_flowsheet_entries(conn)

        assert entries[0].show_id == 0

    def test_multiple_entries_preserve_order(self):
        from semantic_index.pg_source import load_flowsheet_entries

        rows = [
            {
                "id": 1,
                "artist_name": "Autechre",
                "track_title": "VI Scose Poise",
                "album_title": "Confield",
                "record_label": "Warp",
                "show_id": 100,
                "play_order": 1,
                "album_id": None,
                "request_flag": False,
                "add_time_epoch": 1577836800,
                "legacy_entry_id": None,
            },
            {
                "id": 2,
                "artist_name": "Stereolab",
                "track_title": "Metronomic Underground",
                "album_title": "Emperor Tomato Ketchup",
                "record_label": "Duophonic",
                "show_id": 100,
                "play_order": 2,
                "album_id": None,
                "request_flag": False,
                "add_time_epoch": 1577837100,
                "legacy_entry_id": None,
            },
        ]
        conn = _mock_conn_with_rows(rows)

        entries = load_flowsheet_entries(conn)

        assert len(entries) == 2
        assert entries[0].artist_name == "Autechre"
        assert entries[1].artist_name == "Stereolab"

    def test_empty_table_returns_empty_list(self):
        from semantic_index.pg_source import load_flowsheet_entries

        conn = _mock_conn_with_rows([])

        assert load_flowsheet_entries(conn) == []


# ===========================================================================
# load_flowsheet_entries -- server-side cursor lifecycle
# ===========================================================================


class TestLoadFlowsheetEntriesServerSideCursor:
    """The flowsheet loader MUST use a psycopg3 server-side cursor inside an
    explicit transaction so libpq fetches rows in ``itersize`` chunks rather
    than buffering the entire ~1M-row result set in C memory.
    """

    _SAMPLE_ROW = {
        "id": 1,
        "artist_name": "Juana Molina",
        "track_title": "la paradoja",
        "album_title": "DOGA",
        "record_label": "Sonamos",
        "show_id": 100,
        "play_order": 1,
        "album_id": None,
        "request_flag": False,
        "add_time_epoch": 1577836800,
        "legacy_entry_id": None,
    }

    def test_does_not_call_fetchall_or_plain_conn_execute(self):
        """Server-side cursors require ``cursor.execute()``, not
        ``conn.execute()``; and they MUST be iterated, never ``.fetchall()``-ed
        (which would defeat the libpq-buffer-bounding purpose)."""
        from semantic_index.pg_source import load_flowsheet_entries

        conn = _mock_conn_with_rows([self._SAMPLE_ROW])

        load_flowsheet_entries(conn)

        conn.execute.assert_not_called()
        conn.cursor.return_value.fetchall.assert_not_called()

    def test_opens_named_cursor_inside_transaction(self):
        """The cursor must be named (psycopg3 server-side-cursor requirement)
        AND opened inside a transaction (named cursors error outside one when
        the connection is autocommit=True, which production uses)."""
        from semantic_index.pg_source import load_flowsheet_entries

        conn = _mock_conn_with_rows([self._SAMPLE_ROW])

        load_flowsheet_entries(conn)

        conn.transaction.assert_called_once()
        conn.cursor.assert_called_once()
        cursor_call = conn.cursor.call_args
        assert "name" in cursor_call.kwargs, (
            "load_flowsheet_entries must call conn.cursor(name=...) -- a "
            "name is what makes it server-side in psycopg3"
        )
        assert cursor_call.kwargs["name"], "cursor name must not be empty"

    def test_sets_itersize_to_bound_libpq_buffer(self):
        """``itersize`` controls how many rows libpq holds at a time. Without
        it the default is 100 (too small -- thousands of round trips for 1M
        rows). With it set to ~10K, libpq buffer is bounded to ~5 MiB."""
        from semantic_index.pg_source import load_flowsheet_entries

        conn = _mock_conn_with_rows([self._SAMPLE_ROW])

        load_flowsheet_entries(conn)

        named_cursor = conn.cursor.return_value
        assert named_cursor.itersize >= 1000, (
            f"itersize must be >=1000 to amortize network round-trips over the "
            f"~1M-row production flowsheet; got {named_cursor.itersize}"
        )

    def test_transaction_entered_before_cursor_opened(self):
        """psycopg3 server-side cursors require an open transaction at the
        time ``conn.cursor(name=...)`` is called. Reversing this order would
        raise at runtime in production."""
        from semantic_index.pg_source import load_flowsheet_entries

        conn = _mock_conn_with_rows([self._SAMPLE_ROW])
        call_order: list[str] = []

        original_tx_enter = conn.transaction.return_value.__enter__
        original_cursor_call = conn.cursor

        def tx_enter_recorder(*args, **kwargs):
            call_order.append("transaction_entered")
            return original_tx_enter(*args, **kwargs)

        def cursor_open_recorder(*args, **kwargs):
            call_order.append("cursor_opened")
            return original_cursor_call(*args, **kwargs)

        conn.transaction.return_value.__enter__ = tx_enter_recorder
        conn.cursor = cursor_open_recorder

        load_flowsheet_entries(conn)

        assert call_order.index("transaction_entered") < call_order.index("cursor_opened"), (
            "conn.transaction() must be entered before conn.cursor(name=...) is called"
        )


# ===========================================================================
# load_shows
# ===========================================================================


class TestLoadShows:
    """load_shows() queries shows + show_djs and returns (show_to_dj, show_dj_names)."""

    def test_maps_show_id_to_primary_dj_id(self):
        from semantic_index.pg_source import load_shows

        conn = _mock_conn_with_queries(
            {
                "shows": [
                    {
                        "id": 3210,
                        "primary_dj_id": "dj_42",
                        "legacy_dj_name": None,
                        "legacy_show_id": 121,
                    },
                    {
                        "id": 3211,
                        "primary_dj_id": None,
                        "legacy_dj_name": None,
                        "legacy_show_id": 124,
                    },
                ],
                "djs": [],
            }
        )

        show_to_dj, show_dj_names = load_shows(conn)

        assert show_to_dj[3210] == "dj_42"
        assert 3211 not in show_to_dj

    def test_legacy_dj_name_fallback(self):
        """When primary_dj_id is NULL, fall back to legacy_dj_name."""
        from semantic_index.pg_source import load_shows

        conn = _mock_conn_with_queries(
            {
                "shows": [
                    {
                        "id": 100,
                        "primary_dj_id": None,
                        "legacy_dj_name": "Ellie Blake",
                        "legacy_show_id": 1,
                    },
                    {
                        "id": 200,
                        "primary_dj_id": "auth_uuid",
                        "legacy_dj_name": "Old Name",
                        "legacy_show_id": 2,
                    },
                ],
                "djs": [],
            }
        )

        show_to_dj, show_dj_names = load_shows(conn)

        assert show_to_dj[100] == "Ellie Blake"
        assert show_dj_names[100] == "Ellie Blake"
        assert show_to_dj[200] == "auth_uuid"

    def test_empty_shows_returns_empty_dicts(self):
        from semantic_index.pg_source import load_shows

        conn = _mock_conn_with_queries({"shows": [], "djs": []})

        show_to_dj, show_dj_names = load_shows(conn)

        assert show_to_dj == {}
        assert show_dj_names == {}

    def test_all_null_dj_ids_and_names_returns_empty(self):
        from semantic_index.pg_source import load_shows

        conn = _mock_conn_with_queries(
            {
                "shows": [
                    {"id": 100, "primary_dj_id": None, "legacy_dj_name": None, "legacy_show_id": 1},
                    {"id": 200, "primary_dj_id": None, "legacy_dj_name": None, "legacy_show_id": 2},
                ],
                "djs": [],
            }
        )

        show_to_dj, show_dj_names = load_shows(conn)

        assert show_to_dj == {}


# ===========================================================================
# load_cross_references
# ===========================================================================


class TestLoadCrossReferences:
    """load_cross_references() queries artist_crossreference and artist_library_crossreference."""

    def test_returns_artist_crossref_tuples(self):
        from semantic_index.pg_source import load_cross_references

        conn = _mock_conn_with_queries(
            {
                "artist_crossreference": [
                    {"source_artist_id": 1, "target_artist_id": 2, "comment": "see also"},
                ],
                "artist_library_crossreference": [],
            }
        )

        artist_xrefs, release_xrefs = load_cross_references(conn)

        assert len(artist_xrefs) == 1
        # Returns tuples matching the shape expected by CrossReferenceExtractor
        # (id, source_artist_id, target_artist_id, comment)
        assert artist_xrefs[0] == (0, 1, 2, "see also")

    def test_returns_release_crossref_tuples(self):
        from semantic_index.pg_source import load_cross_references

        conn = _mock_conn_with_queries(
            {
                "artist_crossreference": [],
                "artist_library_crossreference": [
                    {"artist_id": 10, "library_id": 20, "comment": "compilation"},
                ],
            }
        )

        artist_xrefs, release_xrefs = load_cross_references(conn)

        assert len(release_xrefs) == 1
        assert release_xrefs[0] == (0, 10, 20, "compilation")

    def test_empty_tables_return_empty_lists(self):
        from semantic_index.pg_source import load_cross_references

        conn = _mock_conn_with_queries(
            {
                "artist_crossreference": [],
                "artist_library_crossreference": [],
            }
        )

        artist_xrefs, release_xrefs = load_cross_references(conn)

        assert artist_xrefs == []
        assert release_xrefs == []


# ===========================================================================
# load_cross_references -- server-side cursor lifecycle
# ===========================================================================


class TestLoadCrossReferencesServerSideCursor:
    """``load_cross_references`` fetchall'd both xref tables under ``dict_row``;
    it must stream them via named server-side cursors like ``load_catalog``
    (WXYC/semantic-index#345).
    """

    _QUERIES = {
        "artist_crossreference": [
            {"source_artist_id": 1, "target_artist_id": 2, "comment": "see also"}
        ],
        "artist_library_crossreference": [
            {"artist_id": 10, "library_id": 20, "comment": "compilation"}
        ],
    }

    def test_does_not_buffer_with_fetchall_or_conn_execute(self):
        from semantic_index.pg_source import load_cross_references

        conn = _mock_conn_with_queries(self._QUERIES)

        load_cross_references(conn)

        conn.execute.assert_not_called()
        for cur in conn.created_cursors:
            cur.fetchall.assert_not_called()

    def test_opens_named_cursors_inside_transaction(self):
        from semantic_index.pg_source import load_cross_references

        conn = _mock_conn_with_queries(self._QUERIES)
        cursor_open_depths = _instrument_cursor_transaction_depth(conn)

        load_cross_references(conn)

        # Two queries (artist xref, release xref) -> two named cursors, each
        # opened inside an active transaction (see the catalog test for why this
        # asserts open-time depth rather than transaction count).
        assert conn.cursor.call_count == 2
        for call in conn.cursor.call_args_list:
            assert call.kwargs.get("name"), "every xref cursor must be named (server-side)"
        assert all(d >= 1 for d in cursor_open_depths), (
            f"every xref cursor must be opened inside a transaction; "
            f"open-time transaction depths were {cursor_open_depths}"
        )

    def test_sets_itersize_to_bound_libpq_buffer(self):
        from semantic_index.pg_source import load_cross_references

        conn = _mock_conn_with_queries(self._QUERIES)

        load_cross_references(conn)

        assert conn.created_cursors, "expected server-side cursors to be created"
        for cur in conn.created_cursors:
            assert cur.itersize >= 1000, f"itersize must be >=1000; got {cur.itersize}"

    def test_still_maps_columns_correctly_when_streamed(self):
        """Behavioural guard: streaming must not change the returned tuple shape."""
        from semantic_index.pg_source import load_cross_references

        conn = _mock_conn_with_queries(self._QUERIES)

        artist_xrefs, release_xrefs = load_cross_references(conn)

        assert artist_xrefs == [(0, 1, 2, "see also")]
        assert release_xrefs == [(0, 10, 20, "compilation")]
