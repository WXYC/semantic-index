"""Tests for the reconciliation module: ArtistReconciler batch Discogs matching."""

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from semantic_index.discogs_client import DiscogsClient
from semantic_index.entity_store import EntityStore
from semantic_index.models import ReconciliationReport
from semantic_index.reconciliation import ArtistReconciler

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
# _reconcile_discogs_aliases
# ---------------------------------------------------------------------------


def _make_mock_alias_cache_conn(
    alias_matches: list[tuple[str, int]],
    variation_matches: list[tuple[str, int]],
    style_rows: list[tuple[int, str]],
) -> MagicMock:
    """Build a mock psycopg connection for alias reconciliation queries.

    Args:
        alias_matches: List of (alias_name, artist_id) rows from artist_alias.
        variation_matches: List of (name, artist_id) rows from artist_name_variation.
        style_rows: List of (artist_id, style) rows from release_style JOIN release_artist.
    """
    mock_conn = MagicMock()

    def execute_side_effect(sql, params=None):
        result = MagicMock()
        sql_lower = sql.strip().lower()
        if "artist_alias" in sql_lower:
            result.fetchall.return_value = alias_matches
        elif "artist_name_variation" in sql_lower:
            result.fetchall.return_value = variation_matches
        elif "release_style" in sql_lower:
            result.fetchall.return_value = style_rows
        else:
            result.fetchall.return_value = []
        return result

    mock_conn.execute.side_effect = execute_side_effect
    return mock_conn


class TestReconcileDiscogsAliases:
    def test_matches_via_alias(self, store: EntityStore):
        mock_conn = _make_mock_alias_cache_conn(
            alias_matches=[("j dilla", 259)],
            variation_matches=[],
            style_rows=[(259, "Hip Hop"), (259, "Instrumental")],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        result = reconciler._reconcile_discogs_aliases(["J Dilla"])
        assert "J Dilla" in result
        assert result["J Dilla"][0] == 259
        assert set(result["J Dilla"][1]) == {"Hip Hop", "Instrumental"}

    def test_matches_via_name_variation(self, store: EntityStore):
        mock_conn = _make_mock_alias_cache_conn(
            alias_matches=[],
            variation_matches=[("aphex twin", 45)],
            style_rows=[(45, "IDM"), (45, "Ambient")],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        result = reconciler._reconcile_discogs_aliases(["Aphex Twin"])
        assert "Aphex Twin" in result
        assert result["Aphex Twin"][0] == 45

    def test_alias_takes_precedence_over_variation(self, store: EntityStore):
        """If both tables match, alias result wins."""
        mock_conn = _make_mock_alias_cache_conn(
            alias_matches=[("autechre", 42)],
            variation_matches=[("autechre", 999)],
            style_rows=[],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        result = reconciler._reconcile_discogs_aliases(["Autechre"])
        assert result["Autechre"][0] == 42

    def test_no_matches_returns_empty(self, store: EntityStore):
        mock_conn = _make_mock_alias_cache_conn(
            alias_matches=[], variation_matches=[], style_rows=[]
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        result = reconciler._reconcile_discogs_aliases(["Unknown Band"])
        assert result == {}

    def test_no_cache_returns_empty(self, store: EntityStore):
        client = DiscogsClient(cache_dsn=None, api_base_url=None)
        reconciler = ArtistReconciler(store, client)

        result = reconciler._reconcile_discogs_aliases(["Autechre"])
        assert result == {}

    def test_empty_names_returns_empty(self, store: EntityStore):
        mock_conn = _make_mock_alias_cache_conn(
            alias_matches=[], variation_matches=[], style_rows=[]
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        result = reconciler._reconcile_discogs_aliases([])
        assert result == {}

    def test_case_insensitive_matching(self, store: EntityStore):
        mock_conn = _make_mock_alias_cache_conn(
            alias_matches=[("cat power", 88)],
            variation_matches=[],
            style_rows=[(88, "Indie Rock")],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        result = reconciler._reconcile_discogs_aliases(["Cat Power"])
        assert "Cat Power" in result
        assert result["Cat Power"][0] == 88

    def test_matched_with_no_styles(self, store: EntityStore):
        mock_conn = _make_mock_alias_cache_conn(
            alias_matches=[("sessa", 777)],
            variation_matches=[],
            style_rows=[],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        result = reconciler._reconcile_discogs_aliases(["Sessa"])
        assert result["Sessa"] == (777, [])

    def test_multiple_names_mixed_sources(self, store: EntityStore):
        """Some names match via alias, others via name variation."""
        mock_conn = _make_mock_alias_cache_conn(
            alias_matches=[("stereolab", 99)],
            variation_matches=[("jessica pratt", 333)],
            style_rows=[(99, "Krautrock"), (333, "Folk")],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        result = reconciler._reconcile_discogs_aliases(["Stereolab", "Jessica Pratt", "Unknown"])
        assert "Stereolab" in result
        assert "Jessica Pratt" in result
        assert "Unknown" not in result


# ---------------------------------------------------------------------------
# reconcile_aliases
# ---------------------------------------------------------------------------


class TestReconcileAliases:
    def _set_up_no_match_artists(self, store: EntityStore, names: list[str]) -> dict[str, int]:
        """Insert artists and set them to no_match status. Returns name->id mapping."""
        mapping: dict[str, int] = {}
        for name in names:
            aid = store.upsert_artist(name)
            store.update_reconciliation_status(aid, "no_match")
            mapping[name] = aid
        return mapping

    def test_returns_reconciliation_report(self, store: EntityStore):
        self._set_up_no_match_artists(store, ["Autechre", "Cat Power"])
        mock_conn = _make_mock_alias_cache_conn(
            alias_matches=[("autechre", 42)],
            variation_matches=[],
            style_rows=[(42, "IDM")],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        report = reconciler.reconcile_aliases()
        assert isinstance(report, ReconciliationReport)
        assert report.total == 2
        assert report.attempted == 2
        assert report.succeeded == 1
        assert report.no_match == 1
        assert report.errored == 0
        assert report.skipped == 0

    def test_updates_discogs_artist_id(self, store: EntityStore):
        self._set_up_no_match_artists(store, ["Autechre"])
        mock_conn = _make_mock_alias_cache_conn(
            alias_matches=[("autechre", 42)],
            variation_matches=[],
            style_rows=[],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        reconciler.reconcile_aliases()
        row = store.get_artist_by_name("Autechre")
        assert row is not None
        assert row["discogs_artist_id"] == 42

    def test_persists_styles(self, store: EntityStore):
        mapping = self._set_up_no_match_artists(store, ["Autechre"])
        mock_conn = _make_mock_alias_cache_conn(
            alias_matches=[("autechre", 42)],
            variation_matches=[],
            style_rows=[(42, "IDM"), (42, "Abstract")],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        reconciler.reconcile_aliases()
        styles = store.get_artist_styles(mapping["Autechre"])
        assert set(styles) == {"IDM", "Abstract"}

    def test_logs_reconciliation_event_with_alias_method(self, store: EntityStore):
        mapping = self._set_up_no_match_artists(store, ["Autechre"])
        mock_conn = _make_mock_alias_cache_conn(
            alias_matches=[("autechre", 42)],
            variation_matches=[],
            style_rows=[],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        reconciler.reconcile_aliases()
        history = store.get_reconciliation_history(mapping["Autechre"])
        assert len(history) == 1
        assert history[0].source == "discogs"
        assert history[0].external_id == "42"
        assert history[0].method == "alias_lookup"

    def test_updates_status_reconciled(self, store: EntityStore):
        mapping = self._set_up_no_match_artists(store, ["Autechre"])
        mock_conn = _make_mock_alias_cache_conn(
            alias_matches=[("autechre", 42)],
            variation_matches=[],
            style_rows=[],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        reconciler.reconcile_aliases()
        row = store._conn.execute(
            "SELECT reconciliation_status FROM artist WHERE id = ?", (mapping["Autechre"],)
        ).fetchone()
        assert row[0] == "reconciled"

    def test_unmatched_stay_no_match(self, store: EntityStore):
        mapping = self._set_up_no_match_artists(store, ["Unknown Band"])
        mock_conn = _make_mock_alias_cache_conn(
            alias_matches=[], variation_matches=[], style_rows=[]
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        reconciler.reconcile_aliases()
        row = store._conn.execute(
            "SELECT reconciliation_status FROM artist WHERE id = ?", (mapping["Unknown Band"],)
        ).fetchone()
        assert row[0] == "no_match"

    def test_skips_unreconciled_and_reconciled(self, store: EntityStore):
        store.upsert_artist("Stereolab")  # default 'unreconciled'
        aid_rec = store.upsert_artist("Father John Misty")
        store.update_reconciliation_status(aid_rec, "reconciled")
        self._set_up_no_match_artists(store, ["Cat Power"])

        mock_conn = _make_mock_alias_cache_conn(
            alias_matches=[("cat power", 88)],
            variation_matches=[],
            style_rows=[],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        report = reconciler.reconcile_aliases()
        assert report.total == 3
        assert report.skipped == 2
        assert report.attempted == 1
        assert report.succeeded == 1

    def test_empty_no_match_set(self, store: EntityStore):
        store.upsert_artist("Autechre")  # default 'unreconciled'
        mock_conn = _make_mock_alias_cache_conn(
            alias_matches=[], variation_matches=[], style_rows=[]
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        report = reconciler.reconcile_aliases()
        assert report.total == 1
        assert report.attempted == 0
        assert report.skipped == 1

    def test_batch_size_parameter(self, store: EntityStore):
        self._set_up_no_match_artists(store, ["Autechre", "Stereolab", "Cat Power"])

        call_count = 0
        original_method = ArtistReconciler._reconcile_discogs_aliases

        def tracking_alias(self_inner, names):
            nonlocal call_count
            call_count += 1
            return original_method(self_inner, names)

        mock_conn = _make_mock_alias_cache_conn(
            alias_matches=[("autechre", 42), ("stereolab", 99), ("cat power", 88)],
            variation_matches=[],
            style_rows=[],
        )
        client = _make_discogs_client_with_mock(mock_conn)
        reconciler = ArtistReconciler(store, client)

        with patch.object(ArtistReconciler, "_reconcile_discogs_aliases", tracking_alias):
            reconciler.reconcile_aliases(batch_size=2)

        assert call_count == 2  # ceil(3/2) = 2 batches

    def test_no_cache_counts_as_no_match(self, store: EntityStore):
        self._set_up_no_match_artists(store, ["Autechre"])
        client = DiscogsClient(cache_dsn=None, api_base_url=None)
        reconciler = ArtistReconciler(store, client)

        report = reconciler.reconcile_aliases()
        assert report.attempted == 1
        assert report.no_match == 1
        assert report.succeeded == 0
