"""Tests for the reconciliation module: ArtistReconciler batch Discogs matching."""

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from semantic_index.discogs_client import DiscogsClient
from semantic_index.entity_store import EntityStore
from semantic_index.models import ReconciliationReport, WikidataEntity
from semantic_index.reconciliation import ArtistReconciler
from semantic_index.wikidata_client import WikidataClient

# The old artist schema — matches sqlite_export._SCHEMA artist table
_OLD_ARTIST_SCHEMA = """
CREATE TABLE artist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_name TEXT NOT NULL UNIQUE,
    genre TEXT,
    total_plays INTEGER NOT NULL DEFAULT 0,
    active_first_year INTEGER,
    active_last_year INTEGER,
    dj_count INTEGER NOT NULL DEFAULT 0,
    request_ratio REAL NOT NULL DEFAULT 0.0,
    show_count INTEGER NOT NULL DEFAULT 0,
    discogs_artist_id INTEGER
);
"""


@pytest.fixture()
def store(tmp_path) -> EntityStore:
    """An initialized EntityStore with a pre-migrated artist table."""
    db_path = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_path)
    conn.executescript(_OLD_ARTIST_SCHEMA)
    conn.close()
    s = EntityStore(db_path)
    s.initialize()
    return s


def _make_mock_cache_conn(
    artist_matches: list[tuple[str, int]],
    style_rows: list[tuple[str, str]],
) -> MagicMock:
    """Build a mock psycopg connection for bulk reconciliation queries.

    Args:
        artist_matches: List of (artist_name, artist_id) rows from release_artist.
        style_rows: List of (artist_name, style) rows from release_style JOIN release_artist.
    """
    mock_conn = MagicMock()

    def execute_side_effect(sql, params=None):
        result = MagicMock()
        sql_lower = sql.strip().lower()
        if "release_artist" in sql_lower and "release_style" not in sql_lower:
            result.fetchall.return_value = artist_matches
        elif "release_style" in sql_lower:
            result.fetchall.return_value = style_rows
        else:
            result.fetchall.return_value = []
        return result

    mock_conn.execute.side_effect = execute_side_effect
    return mock_conn


def _make_discogs_client_with_mock(mock_conn: MagicMock) -> DiscogsClient:
    """Create a DiscogsClient whose cache connection is the given mock."""
    # Mark mock as not closed so _get_cache_conn() reuses it
    mock_conn.closed = False
    client = DiscogsClient(cache_dsn="postgresql://test", api_base_url=None)
    client._cache_conn = mock_conn
    return client


# ---------------------------------------------------------------------------
# _reconcile_discogs_bulk
# ---------------------------------------------------------------------------


class TestReconcileDiscogsBulk:
    def test_returns_matches_with_artist_ids(self, store: EntityStore):
        mock_conn = _make_mock_cache_conn(
            artist_matches=[("autechre", 42), ("stereolab", 99)],
            style_rows=[("autechre", "IDM"), ("autechre", "Abstract"), ("stereolab", "Krautrock")],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        result = reconciler._reconcile_discogs_bulk(["Autechre", "Stereolab"])
        assert "Autechre" in result
        assert result["Autechre"][0] == 42
        assert "Stereolab" in result
        assert result["Stereolab"][0] == 99

    def test_returns_styles_for_matched_artists(self, store: EntityStore):
        mock_conn = _make_mock_cache_conn(
            artist_matches=[("autechre", 42)],
            style_rows=[("autechre", "IDM"), ("autechre", "Abstract")],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        result = reconciler._reconcile_discogs_bulk(["Autechre"])
        assert set(result["Autechre"][1]) == {"IDM", "Abstract"}

    def test_unmatched_names_excluded(self, store: EntityStore):
        mock_conn = _make_mock_cache_conn(
            artist_matches=[("autechre", 42)],
            style_rows=[("autechre", "IDM")],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        result = reconciler._reconcile_discogs_bulk(["Autechre", "Unknown Band"])
        assert "Autechre" in result
        assert "Unknown Band" not in result

    def test_no_matches_returns_empty(self, store: EntityStore):
        mock_conn = _make_mock_cache_conn(artist_matches=[], style_rows=[])
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        result = reconciler._reconcile_discogs_bulk(["Unknown Band"])
        assert result == {}

    def test_no_cache_returns_empty(self, store: EntityStore):
        client = DiscogsClient(cache_dsn=None, api_base_url=None)
        reconciler = ArtistReconciler(store, client)

        result = reconciler._reconcile_discogs_bulk(["Autechre"])
        assert result == {}

    def test_empty_names_returns_empty(self, store: EntityStore):
        mock_conn = _make_mock_cache_conn(artist_matches=[], style_rows=[])
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        result = reconciler._reconcile_discogs_bulk([])
        assert result == {}

    def test_case_insensitive_matching(self, store: EntityStore):
        """Canonical names may differ in case from Discogs names."""
        mock_conn = _make_mock_cache_conn(
            artist_matches=[("father john misty", 555)],
            style_rows=[("father john misty", "Indie Rock")],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        result = reconciler._reconcile_discogs_bulk(["Father John Misty"])
        assert "Father John Misty" in result
        assert result["Father John Misty"][0] == 555

    def test_matched_with_no_styles(self, store: EntityStore):
        mock_conn = _make_mock_cache_conn(
            artist_matches=[("sessa", 777)],
            style_rows=[],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        result = reconciler._reconcile_discogs_bulk(["Sessa"])
        assert result["Sessa"] == (777, [])


# ---------------------------------------------------------------------------
# reconcile_batch
# ---------------------------------------------------------------------------


class TestReconcileBatch:
    def test_returns_reconciliation_report(self, store: EntityStore):
        store.bulk_upsert_artists(["Autechre", "Cat Power"])
        mock_conn = _make_mock_cache_conn(
            artist_matches=[("autechre", 42)],
            style_rows=[("autechre", "IDM")],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        report = reconciler.reconcile_batch()
        assert isinstance(report, ReconciliationReport)
        assert report.total == 2
        assert report.attempted == 2
        assert report.succeeded == 1
        assert report.no_match == 1
        assert report.errored == 0
        assert report.skipped == 0

    def test_updates_discogs_artist_id(self, store: EntityStore):
        store.upsert_artist("Autechre")
        mock_conn = _make_mock_cache_conn(
            artist_matches=[("autechre", 42)],
            style_rows=[],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        reconciler.reconcile_batch()
        row = store.get_artist_by_name("Autechre")
        assert row is not None
        assert row["discogs_artist_id"] == 42

    def test_persists_styles(self, store: EntityStore):
        aid = store.upsert_artist("Autechre")
        mock_conn = _make_mock_cache_conn(
            artist_matches=[("autechre", 42)],
            style_rows=[("autechre", "IDM"), ("autechre", "Abstract")],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        reconciler.reconcile_batch()
        styles = store.get_artist_styles(aid)
        assert set(styles) == {"IDM", "Abstract"}

    def test_logs_reconciliation_event(self, store: EntityStore):
        aid = store.upsert_artist("Autechre")
        mock_conn = _make_mock_cache_conn(
            artist_matches=[("autechre", 42)],
            style_rows=[],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        reconciler.reconcile_batch()
        history = store.get_reconciliation_history(aid)
        assert len(history) == 1
        assert history[0].source == "discogs"
        assert history[0].external_id == "42"
        assert history[0].method == "cache_lookup"

    def test_updates_status_reconciled(self, store: EntityStore):
        aid = store.upsert_artist("Autechre")
        mock_conn = _make_mock_cache_conn(
            artist_matches=[("autechre", 42)],
            style_rows=[],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        reconciler.reconcile_batch()
        row = store._conn.execute(
            "SELECT reconciliation_status FROM artist WHERE id = ?", (aid,)
        ).fetchone()
        assert row[0] == "reconciled"

    def test_updates_status_no_match(self, store: EntityStore):
        aid = store.upsert_artist("Unknown Band")
        mock_conn = _make_mock_cache_conn(artist_matches=[], style_rows=[])
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        reconciler.reconcile_batch()
        row = store._conn.execute(
            "SELECT reconciliation_status FROM artist WHERE id = ?", (aid,)
        ).fetchone()
        assert row[0] == "no_match"

    def test_incremental_skips_already_reconciled(self, store: EntityStore):
        aid_reconciled = store.upsert_artist("Autechre", discogs_artist_id=42)
        store.update_reconciliation_status(aid_reconciled, "reconciled")
        store.upsert_artist("Stereolab")

        mock_conn = _make_mock_cache_conn(
            artist_matches=[("stereolab", 99)],
            style_rows=[],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        report = reconciler.reconcile_batch()
        assert report.total == 2
        assert report.skipped == 1
        assert report.attempted == 1
        assert report.succeeded == 1

    def test_incremental_skips_no_match_status(self, store: EntityStore):
        aid = store.upsert_artist("Unknown Band")
        store.update_reconciliation_status(aid, "no_match")
        store.upsert_artist("Cat Power")

        mock_conn = _make_mock_cache_conn(
            artist_matches=[("cat power", 88)],
            style_rows=[],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        report = reconciler.reconcile_batch()
        assert report.skipped == 1
        assert report.attempted == 1

    def test_empty_artist_table(self, store: EntityStore):
        mock_conn = _make_mock_cache_conn(artist_matches=[], style_rows=[])
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        report = reconciler.reconcile_batch()
        assert report.total == 0
        assert report.attempted == 0
        assert report.succeeded == 0
        assert report.skipped == 0

    def test_batch_size_parameter(self, store: EntityStore):
        """With batch_size=2, three artists should be processed in two batches."""
        store.bulk_upsert_artists(["Autechre", "Stereolab", "Cat Power"])

        call_count = 0
        original_method = ArtistReconciler._reconcile_discogs_bulk

        def tracking_bulk(self_inner, names):
            nonlocal call_count
            call_count += 1
            return original_method(self_inner, names)

        mock_conn = _make_mock_cache_conn(
            artist_matches=[("autechre", 42), ("stereolab", 99), ("cat power", 88)],
            style_rows=[],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        with patch.object(ArtistReconciler, "_reconcile_discogs_bulk", tracking_bulk):
            reconciler.reconcile_batch(batch_size=2)

        assert call_count == 2  # ceil(3/2) = 2 batches

    def test_multiple_artists_matched(self, store: EntityStore):
        store.bulk_upsert_artists(["Autechre", "Stereolab", "Father John Misty"])
        mock_conn = _make_mock_cache_conn(
            artist_matches=[
                ("autechre", 42),
                ("stereolab", 99),
                ("father john misty", 555),
            ],
            style_rows=[
                ("autechre", "IDM"),
                ("stereolab", "Krautrock"),
                ("stereolab", "Post-Rock"),
                ("father john misty", "Indie Rock"),
            ],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        report = reconciler.reconcile_batch()
        assert report.succeeded == 3
        assert report.no_match == 0

        # Verify each artist got the right discogs_artist_id
        assert store.get_artist_by_name("Autechre")["discogs_artist_id"] == 42
        assert store.get_artist_by_name("Stereolab")["discogs_artist_id"] == 99
        assert store.get_artist_by_name("Father John Misty")["discogs_artist_id"] == 555

    def test_cache_error_counts_as_no_match(self, store: EntityStore):
        """If the cache connection fails, all artists should be no_match."""
        store.upsert_artist("Autechre")
        client = DiscogsClient(cache_dsn=None, api_base_url=None)
        reconciler = ArtistReconciler(store, client)

        report = reconciler.reconcile_batch()
        assert report.attempted == 1
        assert report.no_match == 1
        assert report.succeeded == 0


# ---------------------------------------------------------------------------
# _reconcile_member_bulk
# ---------------------------------------------------------------------------


def _make_mock_member_conn(
    group_matches: list[tuple[str, int]],
    member_matches: list[tuple[str, int]],
    style_rows: list[tuple[int, str]],
) -> MagicMock:
    """Build a mock psycopg connection for member reconciliation queries.

    Args:
        group_matches: List of (group_name_lower, group_artist_id) rows from artist JOIN artist_member.
        member_matches: List of (member_name_lower, member_id) rows from artist_member.
        style_rows: List of (artist_id, style) rows from release_style JOIN release_artist by ID.
    """
    mock_conn = MagicMock()

    def execute_side_effect(sql, params=None):
        result = MagicMock()
        sql_lower = sql.strip().lower()
        if "a.name" in sql_lower and "artist_member" in sql_lower:
            result.fetchall.return_value = group_matches
        elif "member_name" in sql_lower and "member_id" in sql_lower:
            result.fetchall.return_value = member_matches
        elif "release_style" in sql_lower:
            result.fetchall.return_value = style_rows
        else:
            result.fetchall.return_value = []
        return result

    mock_conn.execute.side_effect = execute_side_effect
    return mock_conn


class TestReconcileMemberBulk:
    def test_group_name_match(self, store: EntityStore):
        """Artist name matching a Discogs group returns the group's artist_id."""
        mock_conn = _make_mock_member_conn(
            group_matches=[("stereolab", 99)],
            member_matches=[],
            style_rows=[],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        result = reconciler._reconcile_member_bulk(["Stereolab"])
        assert "Stereolab" in result
        discogs_id, styles, method = result["Stereolab"]
        assert discogs_id == 99
        assert method == "member_group"

    def test_member_name_match(self, store: EntityStore):
        """Artist name matching a Discogs member returns the member's ID."""
        mock_conn = _make_mock_member_conn(
            group_matches=[],
            member_matches=[("jessica pratt", 444)],
            style_rows=[],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        result = reconciler._reconcile_member_bulk(["Jessica Pratt"])
        assert "Jessica Pratt" in result
        discogs_id, styles, method = result["Jessica Pratt"]
        assert discogs_id == 444
        assert method == "member_name"

    def test_both_directions(self, store: EntityStore):
        """Different artists matched via group and member lookups."""
        mock_conn = _make_mock_member_conn(
            group_matches=[("stereolab", 99)],
            member_matches=[("jessica pratt", 444)],
            style_rows=[],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        result = reconciler._reconcile_member_bulk(["Stereolab", "Jessica Pratt"])
        assert result["Stereolab"][2] == "member_group"
        assert result["Jessica Pratt"][2] == "member_name"

    def test_group_match_takes_priority(self, store: EntityStore):
        """When name matches as both group and member, group match wins."""
        mock_conn = _make_mock_member_conn(
            group_matches=[("cat power", 88)],
            member_matches=[("cat power", 999)],
            style_rows=[],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        result = reconciler._reconcile_member_bulk(["Cat Power"])
        assert result["Cat Power"][0] == 88
        assert result["Cat Power"][2] == "member_group"

    def test_no_matches_returns_empty(self, store: EntityStore):
        mock_conn = _make_mock_member_conn(group_matches=[], member_matches=[], style_rows=[])
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        result = reconciler._reconcile_member_bulk(["Unknown Band"])
        assert result == {}

    def test_no_cache_returns_empty(self, store: EntityStore):
        client = DiscogsClient(cache_dsn=None, api_base_url=None)
        reconciler = ArtistReconciler(store, client)

        result = reconciler._reconcile_member_bulk(["Autechre"])
        assert result == {}

    def test_empty_names_returns_empty(self, store: EntityStore):
        mock_conn = _make_mock_member_conn(group_matches=[], member_matches=[], style_rows=[])
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        result = reconciler._reconcile_member_bulk([])
        assert result == {}

    def test_case_insensitive_matching(self, store: EntityStore):
        mock_conn = _make_mock_member_conn(
            group_matches=[("father john misty", 555)],
            member_matches=[],
            style_rows=[],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        result = reconciler._reconcile_member_bulk(["Father John Misty"])
        assert "Father John Misty" in result
        assert result["Father John Misty"][0] == 555

    def test_styles_fetched_by_artist_id(self, store: EntityStore):
        """Styles are looked up by discogs_artist_id, not by name."""
        mock_conn = _make_mock_member_conn(
            group_matches=[("autechre", 42)],
            member_matches=[],
            style_rows=[(42, "IDM"), (42, "Abstract")],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        result = reconciler._reconcile_member_bulk(["Autechre"])
        assert set(result["Autechre"][1]) == {"IDM", "Abstract"}

    def test_matched_with_no_styles(self, store: EntityStore):
        mock_conn = _make_mock_member_conn(
            group_matches=[("sessa", 777)],
            member_matches=[],
            style_rows=[],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        result = reconciler._reconcile_member_bulk(["Sessa"])
        assert result["Sessa"] == (777, [], "member_group")


# ---------------------------------------------------------------------------
# reconcile_members
# ---------------------------------------------------------------------------


class TestReconcileMembers:
    def test_returns_reconciliation_report(self, store: EntityStore):
        aid1 = store.upsert_artist("Autechre")
        aid2 = store.upsert_artist("Cat Power")
        store.update_reconciliation_status(aid1, "no_match")
        store.update_reconciliation_status(aid2, "no_match")

        mock_conn = _make_mock_member_conn(
            group_matches=[("autechre", 42)],
            member_matches=[],
            style_rows=[],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        report = reconciler.reconcile_members()
        assert isinstance(report, ReconciliationReport)
        assert report.total == 2
        assert report.attempted == 2
        assert report.succeeded == 1
        assert report.no_match == 1
        assert report.errored == 0
        assert report.skipped == 0

    def test_updates_discogs_artist_id(self, store: EntityStore):
        aid = store.upsert_artist("Autechre")
        store.update_reconciliation_status(aid, "no_match")

        mock_conn = _make_mock_member_conn(
            group_matches=[("autechre", 42)],
            member_matches=[],
            style_rows=[],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        reconciler.reconcile_members()
        row = store.get_artist_by_name("Autechre")
        assert row is not None
        assert row["discogs_artist_id"] == 42

    def test_persists_styles(self, store: EntityStore):
        aid = store.upsert_artist("Autechre")
        store.update_reconciliation_status(aid, "no_match")

        mock_conn = _make_mock_member_conn(
            group_matches=[("autechre", 42)],
            member_matches=[],
            style_rows=[(42, "IDM"), (42, "Abstract")],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        reconciler.reconcile_members()
        styles = store.get_artist_styles(aid)
        assert set(styles) == {"IDM", "Abstract"}

    def test_logs_reconciliation_with_member_method(self, store: EntityStore):
        aid = store.upsert_artist("Jessica Pratt")
        store.update_reconciliation_status(aid, "no_match")

        mock_conn = _make_mock_member_conn(
            group_matches=[],
            member_matches=[("jessica pratt", 444)],
            style_rows=[],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        reconciler.reconcile_members()
        history = store.get_reconciliation_history(aid)
        assert len(history) == 1
        assert history[0].source == "discogs"
        assert history[0].external_id == "444"
        assert history[0].method == "member_name"

    def test_logs_group_method(self, store: EntityStore):
        aid = store.upsert_artist("Stereolab")
        store.update_reconciliation_status(aid, "no_match")

        mock_conn = _make_mock_member_conn(
            group_matches=[("stereolab", 99)],
            member_matches=[],
            style_rows=[],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        reconciler.reconcile_members()
        history = store.get_reconciliation_history(aid)
        assert history[0].method == "member_group"

    def test_updates_status_to_reconciled(self, store: EntityStore):
        aid = store.upsert_artist("Autechre")
        store.update_reconciliation_status(aid, "no_match")

        mock_conn = _make_mock_member_conn(
            group_matches=[("autechre", 42)],
            member_matches=[],
            style_rows=[],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        reconciler.reconcile_members()
        row = store._conn.execute(
            "SELECT reconciliation_status FROM artist WHERE id = ?", (aid,)
        ).fetchone()
        assert row[0] == "reconciled"

    def test_unmatched_stays_no_match(self, store: EntityStore):
        aid = store.upsert_artist("Unknown Band")
        store.update_reconciliation_status(aid, "no_match")

        mock_conn = _make_mock_member_conn(group_matches=[], member_matches=[], style_rows=[])
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        reconciler.reconcile_members()
        row = store._conn.execute(
            "SELECT reconciliation_status FROM artist WHERE id = ?", (aid,)
        ).fetchone()
        assert row[0] == "no_match"

    def test_only_processes_no_match_artists(self, store: EntityStore):
        """Reconciled and unreconciled artists are skipped."""
        aid_reconciled = store.upsert_artist("Autechre", discogs_artist_id=42)
        store.update_reconciliation_status(aid_reconciled, "reconciled")
        store.upsert_artist("Stereolab")  # stays unreconciled
        aid_no_match = store.upsert_artist("Cat Power")
        store.update_reconciliation_status(aid_no_match, "no_match")

        mock_conn = _make_mock_member_conn(
            group_matches=[("cat power", 88)],
            member_matches=[],
            style_rows=[],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        report = reconciler.reconcile_members()
        assert report.total == 3
        assert report.skipped == 2
        assert report.attempted == 1
        assert report.succeeded == 1

    def test_empty_no_match_set(self, store: EntityStore):
        store.upsert_artist("Autechre")  # unreconciled, not no_match
        mock_conn = _make_mock_member_conn(group_matches=[], member_matches=[], style_rows=[])
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        report = reconciler.reconcile_members()
        assert report.total == 1
        assert report.attempted == 0
        assert report.succeeded == 0
        assert report.skipped == 1

    def test_no_cache_counts_as_errored(self, store: EntityStore):
        aid = store.upsert_artist("Autechre")
        store.update_reconciliation_status(aid, "no_match")
        client = DiscogsClient(cache_dsn=None, api_base_url=None)
        reconciler = ArtistReconciler(store, client)

        report = reconciler.reconcile_members()
        assert report.attempted == 1
        assert report.no_match == 1
        assert report.succeeded == 0


# ---------------------------------------------------------------------------
# reconcile_wikidata
# ---------------------------------------------------------------------------


class TestReconcileWikidata:
    def test_creates_entity_and_populates_qid(self, store: EntityStore):
        """Artists with discogs_artist_id get entities with wikidata_qid."""
        store.upsert_artist("Autechre", discogs_artist_id=2774)

        wikidata_client = MagicMock(spec=WikidataClient)
        wikidata_client.lookup_by_discogs_ids.return_value = {
            2774: WikidataEntity(qid="Q2774", name="Autechre", discogs_artist_id=2774),
        }

        client = DiscogsClient(cache_dsn=None, api_base_url=None)
        reconciler = ArtistReconciler(store, client, wikidata_client=wikidata_client)

        report = reconciler.reconcile_wikidata()
        assert report.succeeded == 1
        assert report.no_match == 0

        row = store.get_artist_by_name("Autechre")
        assert row is not None
        assert row["entity_id"] is not None

        entity = store._conn.execute(
            "SELECT wikidata_qid, name FROM entity WHERE id = ?", (row["entity_id"],)
        ).fetchone()
        assert entity[0] == "Q2774"
        assert entity[1] == "Autechre"

    def test_updates_existing_entity_qid(self, store: EntityStore):
        """If artist already has an entity without QID, updates it."""
        entity = store.get_or_create_entity("Autechre", "artist")
        store.upsert_artist("Autechre", discogs_artist_id=2774, entity_id=entity.id)

        wikidata_client = MagicMock(spec=WikidataClient)
        wikidata_client.lookup_by_discogs_ids.return_value = {
            2774: WikidataEntity(qid="Q2774", name="Autechre", discogs_artist_id=2774),
        }

        client = DiscogsClient(cache_dsn=None, api_base_url=None)
        reconciler = ArtistReconciler(store, client, wikidata_client=wikidata_client)

        report = reconciler.reconcile_wikidata()
        assert report.succeeded == 1

        updated_entity = store.get_entity_by_qid("Q2774")
        assert updated_entity is not None
        assert updated_entity.id == entity.id

    def test_no_match_counted(self, store: EntityStore):
        """Artists whose discogs ID has no Wikidata match are counted as no_match."""
        store.upsert_artist("Autechre", discogs_artist_id=2774)

        wikidata_client = MagicMock(spec=WikidataClient)
        wikidata_client.lookup_by_discogs_ids.return_value = {}

        client = DiscogsClient(cache_dsn=None, api_base_url=None)
        reconciler = ArtistReconciler(store, client, wikidata_client=wikidata_client)

        report = reconciler.reconcile_wikidata()
        assert report.attempted == 1
        assert report.succeeded == 0
        assert report.no_match == 1

    def test_skips_artists_already_with_qid(self, store: EntityStore):
        """Artists whose entity already has a QID are skipped."""
        entity = store.get_or_create_entity("Autechre", "artist", wikidata_qid="Q2774")
        store.upsert_artist("Autechre", discogs_artist_id=2774, entity_id=entity.id)
        store.upsert_artist("Stereolab", discogs_artist_id=10272)

        wikidata_client = MagicMock(spec=WikidataClient)
        wikidata_client.lookup_by_discogs_ids.return_value = {
            10272: WikidataEntity(qid="Q650826", name="Stereolab", discogs_artist_id=10272),
        }

        client = DiscogsClient(cache_dsn=None, api_base_url=None)
        reconciler = ArtistReconciler(store, client, wikidata_client=wikidata_client)

        report = reconciler.reconcile_wikidata()
        assert report.skipped == 1
        assert report.attempted == 1
        assert report.succeeded == 1

    def test_skips_artists_without_discogs_id(self, store: EntityStore):
        """Artists without discogs_artist_id are not attempted."""
        store.upsert_artist("Unknown Band")
        store.upsert_artist("Autechre", discogs_artist_id=2774)

        wikidata_client = MagicMock(spec=WikidataClient)
        wikidata_client.lookup_by_discogs_ids.return_value = {
            2774: WikidataEntity(qid="Q2774", name="Autechre", discogs_artist_id=2774),
        }

        client = DiscogsClient(cache_dsn=None, api_base_url=None)
        reconciler = ArtistReconciler(store, client, wikidata_client=wikidata_client)

        report = reconciler.reconcile_wikidata()
        assert report.total == 2
        assert report.attempted == 1
        assert report.succeeded == 1
        assert report.skipped == 1

    def test_logs_reconciliation_event(self, store: EntityStore):
        aid = store.upsert_artist("Autechre", discogs_artist_id=2774)

        wikidata_client = MagicMock(spec=WikidataClient)
        wikidata_client.lookup_by_discogs_ids.return_value = {
            2774: WikidataEntity(qid="Q2774", name="Autechre", discogs_artist_id=2774),
        }

        client = DiscogsClient(cache_dsn=None, api_base_url=None)
        reconciler = ArtistReconciler(store, client, wikidata_client=wikidata_client)

        reconciler.reconcile_wikidata()
        history = store.get_reconciliation_history(aid)
        assert len(history) == 1
        assert history[0].source == "wikidata"
        assert history[0].external_id == "Q2774"
        assert history[0].method == "discogs_id_lookup"

    def test_multiple_artists(self, store: EntityStore):
        store.upsert_artist("Autechre", discogs_artist_id=2774)
        store.upsert_artist("Stereolab", discogs_artist_id=10272)
        store.upsert_artist("Father John Misty", discogs_artist_id=555)

        wikidata_client = MagicMock(spec=WikidataClient)
        wikidata_client.lookup_by_discogs_ids.return_value = {
            2774: WikidataEntity(qid="Q2774", name="Autechre", discogs_artist_id=2774),
            10272: WikidataEntity(qid="Q650826", name="Stereolab", discogs_artist_id=10272),
        }

        client = DiscogsClient(cache_dsn=None, api_base_url=None)
        reconciler = ArtistReconciler(store, client, wikidata_client=wikidata_client)

        report = reconciler.reconcile_wikidata()
        assert report.total == 3
        assert report.attempted == 3
        assert report.succeeded == 2
        assert report.no_match == 1

    def test_reuses_existing_entity_by_qid(self, store: EntityStore):
        """If an entity already exists with the matching QID, reuses it."""
        existing = store.get_or_create_entity(
            "Autechre (electronic duo)", "artist", wikidata_qid="Q2774"
        )
        store.upsert_artist("Autechre", discogs_artist_id=2774)

        wikidata_client = MagicMock(spec=WikidataClient)
        wikidata_client.lookup_by_discogs_ids.return_value = {
            2774: WikidataEntity(qid="Q2774", name="Autechre", discogs_artist_id=2774),
        }

        client = DiscogsClient(cache_dsn=None, api_base_url=None)
        reconciler = ArtistReconciler(store, client, wikidata_client=wikidata_client)

        reconciler.reconcile_wikidata()
        row = store.get_artist_by_name("Autechre")
        assert row["entity_id"] == existing.id

    def test_no_wikidata_client_raises(self, store: EntityStore):
        """reconcile_wikidata raises ValueError when no WikidataClient is configured."""
        store.upsert_artist("Autechre", discogs_artist_id=2774)
        client = DiscogsClient(cache_dsn=None, api_base_url=None)
        reconciler = ArtistReconciler(store, client)

        with pytest.raises(ValueError, match="WikidataClient"):
            reconciler.reconcile_wikidata()

    def test_empty_artist_table(self, store: EntityStore):
        wikidata_client = MagicMock(spec=WikidataClient)
        client = DiscogsClient(cache_dsn=None, api_base_url=None)
        reconciler = ArtistReconciler(store, client, wikidata_client=wikidata_client)

        report = reconciler.reconcile_wikidata()
        assert report.total == 0
        assert report.attempted == 0
        assert report.succeeded == 0
        # lookup_by_discogs_ids should not be called with no IDs
        wikidata_client.lookup_by_discogs_ids.assert_not_called()

    def test_wikidata_error_counts_as_errored(self, store: EntityStore):
        store.upsert_artist("Autechre", discogs_artist_id=2774)

        wikidata_client = MagicMock(spec=WikidataClient)
        wikidata_client.lookup_by_discogs_ids.side_effect = Exception("SPARQL timeout")

        client = DiscogsClient(cache_dsn=None, api_base_url=None)
        reconciler = ArtistReconciler(store, client, wikidata_client=wikidata_client)

        report = reconciler.reconcile_wikidata()
        assert report.attempted == 1
        assert report.errored == 1
        assert report.succeeded == 0
