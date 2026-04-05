"""Tests for facet_export — play table, DJ table, and aggregate tables."""

import sqlite3
import tempfile

import pytest

from semantic_index.models import ArtistStats, PmiEdge
from semantic_index.sqlite_export import export_sqlite
from tests.conftest import make_adjacency_pair, make_resolved_entry


def _build_test_db(
    resolved_entries=None,
    adjacency_pairs=None,
    show_to_dj=None,
    show_dj_names=None,
    artist_stats=None,
    pmi_edges=None,
):
    """Build a test DB with artist table populated, then export facet tables."""
    from semantic_index.facet_export import export_facet_tables

    path = tempfile.mktemp(suffix=".db")

    # Create the base DB with artist table
    stats = artist_stats or {
        "Autechre": ArtistStats(
            canonical_name="Autechre", total_plays=10, genre="Electronic"
        ),
        "Stereolab": ArtistStats(
            canonical_name="Stereolab", total_plays=8, genre="Rock"
        ),
        "Cat Power": ArtistStats(
            canonical_name="Cat Power", total_plays=5, genre="Rock"
        ),
    }
    edges = pmi_edges or [
        PmiEdge(source="Autechre", target="Stereolab", raw_count=3, pmi=2.5),
    ]
    export_sqlite(path, artist_stats=stats, pmi_edges=edges, xref_edges=[], min_count=1)

    # Read name_to_id from the DB
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT id, canonical_name FROM artist").fetchall()
    name_to_id = {r["canonical_name"]: r["id"] for r in rows}
    conn.close()

    # Export facet tables
    export_facet_tables(
        db_path=path,
        resolved_entries=resolved_entries or [],
        name_to_id=name_to_id,
        show_to_dj=show_to_dj or {},
        show_dj_names=show_dj_names or {},
        adjacency_pairs=adjacency_pairs or [],
    )

    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn, name_to_id


# -- January 2024 timestamps (Unix ms) --
JAN_15_2024 = 1705276800000  # 2024-01-15 00:00 UTC
JAN_20_2024 = 1705708800000  # 2024-01-20 00:00 UTC

# -- July 2024 timestamps --
JUL_10_2024 = 1720569600000  # 2024-07-10 00:00 UTC


class TestDjTable:
    def test_dj_table_from_int_id(self):
        conn, _ = _build_test_db(
            show_to_dj={1: 42},
            show_dj_names={1: "DJ Cool"},
        )
        row = conn.execute("SELECT * FROM dj").fetchone()
        assert row["original_id"] == "42"
        assert row["display_name"] == "DJ Cool"
        conn.close()

    def test_dj_table_from_str_id(self):
        conn, _ = _build_test_db(
            show_to_dj={1: "DJ Cool"},
            show_dj_names={1: "DJ Cool"},
        )
        row = conn.execute("SELECT * FROM dj").fetchone()
        assert row["original_id"] == "DJ Cool"
        assert row["display_name"] == "DJ Cool"
        conn.close()

    def test_dj_table_mixed_ids(self):
        conn, _ = _build_test_db(
            show_to_dj={1: 42, 2: "DJ Sunshine", 3: 42},
            show_dj_names={1: "DJ Cool", 2: "DJ Sunshine", 3: "DJ Cool"},
        )
        rows = conn.execute("SELECT * FROM dj ORDER BY display_name").fetchall()
        assert len(rows) == 2
        names = {r["display_name"] for r in rows}
        assert names == {"DJ Cool", "DJ Sunshine"}
        conn.close()

    def test_dj_display_name_falls_back_to_original_id(self):
        """When show_dj_names is missing for a DJ, fall back to str(original_id)."""
        conn, _ = _build_test_db(
            show_to_dj={1: 42},
            show_dj_names={},  # no name available
        )
        row = conn.execute("SELECT * FROM dj").fetchone()
        assert row["display_name"] == "42"
        conn.close()


class TestPlayTable:
    def test_play_table_row_count(self):
        entries = [
            make_resolved_entry(id=1, canonical_name="Autechre", show_id=1, sequence=1, start_time=JAN_15_2024),
            make_resolved_entry(id=2, canonical_name="Stereolab", show_id=1, sequence=2, start_time=JAN_15_2024),
            make_resolved_entry(id=3, canonical_name="Cat Power", show_id=1, sequence=3, start_time=JAN_15_2024),
        ]
        conn, _ = _build_test_db(resolved_entries=entries)
        count = conn.execute("SELECT COUNT(*) FROM play").fetchone()[0]
        assert count == 3
        conn.close()

    def test_play_month_extraction(self):
        entries = [
            make_resolved_entry(id=1, canonical_name="Autechre", show_id=1, sequence=1, start_time=JAN_15_2024),
            make_resolved_entry(id=2, canonical_name="Stereolab", show_id=1, sequence=2, start_time=JUL_10_2024),
        ]
        conn, _ = _build_test_db(resolved_entries=entries)
        rows = conn.execute("SELECT id, month FROM play ORDER BY id").fetchall()
        assert rows[0]["month"] == 1
        assert rows[1]["month"] == 7
        conn.close()

    def test_play_month_zero_for_missing_timestamp(self):
        entries = [
            make_resolved_entry(id=1, canonical_name="Autechre", show_id=1, sequence=1, start_time=None),
        ]
        conn, _ = _build_test_db(resolved_entries=entries)
        row = conn.execute("SELECT month FROM play").fetchone()
        assert row["month"] == 0
        conn.close()

    def test_play_artist_id_mapped(self):
        entries = [
            make_resolved_entry(id=1, canonical_name="Autechre", show_id=1, sequence=1, start_time=JAN_15_2024),
        ]
        conn, name_to_id = _build_test_db(resolved_entries=entries)
        row = conn.execute("SELECT artist_id FROM play").fetchone()
        assert row["artist_id"] == name_to_id["Autechre"]
        conn.close()

    def test_play_dj_id_mapped(self):
        entries = [
            make_resolved_entry(id=1, canonical_name="Autechre", show_id=1, sequence=1, start_time=JAN_15_2024),
        ]
        conn, _ = _build_test_db(
            resolved_entries=entries,
            show_to_dj={1: 42},
            show_dj_names={1: "DJ Cool"},
        )
        play_row = conn.execute("SELECT dj_id FROM play").fetchone()
        dj_row = conn.execute("SELECT id FROM dj WHERE original_id = '42'").fetchone()
        assert play_row["dj_id"] == dj_row["id"]
        conn.close()

    def test_play_dj_id_null_for_unmapped_show(self):
        entries = [
            make_resolved_entry(id=1, canonical_name="Autechre", show_id=99, sequence=1, start_time=JAN_15_2024),
        ]
        conn, _ = _build_test_db(
            resolved_entries=entries,
            show_to_dj={1: 42},  # show 99 not mapped
            show_dj_names={1: "DJ Cool"},
        )
        row = conn.execute("SELECT dj_id FROM play").fetchone()
        assert row["dj_id"] is None
        conn.close()

    def test_play_preserves_request_flag(self):
        entries = [
            make_resolved_entry(id=1, canonical_name="Autechre", show_id=1, sequence=1, start_time=JAN_15_2024, request_flag=1),
        ]
        conn, _ = _build_test_db(resolved_entries=entries)
        row = conn.execute("SELECT request_flag FROM play").fetchone()
        assert row["request_flag"] == 1
        conn.close()

    def test_play_skips_entries_with_unknown_artist(self):
        """Entries whose canonical_name is not in name_to_id are skipped."""
        entries = [
            make_resolved_entry(id=1, canonical_name="Autechre", show_id=1, sequence=1, start_time=JAN_15_2024),
            make_resolved_entry(id=2, canonical_name="Unknown Artist", show_id=1, sequence=2, start_time=JAN_15_2024),
        ]
        conn, _ = _build_test_db(resolved_entries=entries)
        count = conn.execute("SELECT COUNT(*) FROM play").fetchone()[0]
        assert count == 1
        conn.close()


class TestArtistMonthCount:
    def test_aggregation(self):
        entries = [
            make_resolved_entry(id=1, canonical_name="Autechre", show_id=1, sequence=1, start_time=JAN_15_2024),
            make_resolved_entry(id=2, canonical_name="Autechre", show_id=2, sequence=1, start_time=JAN_20_2024),
            make_resolved_entry(id=3, canonical_name="Autechre", show_id=3, sequence=1, start_time=JUL_10_2024),
            make_resolved_entry(id=4, canonical_name="Stereolab", show_id=1, sequence=2, start_time=JAN_15_2024),
        ]
        conn, name_to_id = _build_test_db(resolved_entries=entries)
        ae_id = name_to_id["Autechre"]

        jan_row = conn.execute(
            "SELECT play_count FROM artist_month_count WHERE artist_id = ? AND month = 1",
            (ae_id,),
        ).fetchone()
        assert jan_row["play_count"] == 2

        jul_row = conn.execute(
            "SELECT play_count FROM artist_month_count WHERE artist_id = ? AND month = 7",
            (ae_id,),
        ).fetchone()
        assert jul_row["play_count"] == 1
        conn.close()

    def test_excludes_month_zero(self):
        entries = [
            make_resolved_entry(id=1, canonical_name="Autechre", show_id=1, sequence=1, start_time=None),
        ]
        conn, _ = _build_test_db(resolved_entries=entries)
        count = conn.execute("SELECT COUNT(*) FROM artist_month_count").fetchone()[0]
        assert count == 0
        conn.close()


class TestArtistDjCount:
    def test_aggregation(self):
        entries = [
            make_resolved_entry(id=1, canonical_name="Autechre", show_id=1, sequence=1, start_time=JAN_15_2024),
            make_resolved_entry(id=2, canonical_name="Autechre", show_id=2, sequence=1, start_time=JAN_20_2024),
            make_resolved_entry(id=3, canonical_name="Stereolab", show_id=1, sequence=2, start_time=JAN_15_2024),
        ]
        conn, name_to_id = _build_test_db(
            resolved_entries=entries,
            show_to_dj={1: 42, 2: 99},
            show_dj_names={1: "DJ Cool", 2: "DJ Sunshine"},
        )
        ae_id = name_to_id["Autechre"]
        rows = conn.execute(
            "SELECT play_count FROM artist_dj_count WHERE artist_id = ? ORDER BY play_count DESC",
            (ae_id,),
        ).fetchall()
        # Autechre played once in show 1 (DJ Cool) and once in show 2 (DJ Sunshine)
        assert len(rows) == 2
        assert rows[0]["play_count"] == 1
        assert rows[1]["play_count"] == 1
        conn.close()


class TestMonthTotal:
    def test_total_plays_and_pairs(self):
        entries = [
            make_resolved_entry(id=1, canonical_name="Autechre", show_id=1, sequence=1, start_time=JAN_15_2024),
            make_resolved_entry(id=2, canonical_name="Stereolab", show_id=1, sequence=2, start_time=JAN_15_2024),
            make_resolved_entry(id=3, canonical_name="Cat Power", show_id=1, sequence=3, start_time=JAN_15_2024),
            make_resolved_entry(id=4, canonical_name="Autechre", show_id=2, sequence=1, start_time=JUL_10_2024),
        ]
        pairs = [
            make_adjacency_pair(source="Autechre", target="Stereolab", show_id=1),
            make_adjacency_pair(source="Stereolab", target="Cat Power", show_id=1),
        ]
        conn, _ = _build_test_db(resolved_entries=entries, adjacency_pairs=pairs)

        jan = conn.execute("SELECT * FROM month_total WHERE month = 1").fetchone()
        assert jan["total_plays"] == 3
        assert jan["total_pairs"] == 2  # both pairs are in show 1 (January)

        jul = conn.execute("SELECT * FROM month_total WHERE month = 7").fetchone()
        assert jul["total_plays"] == 1
        assert jul["total_pairs"] == 0  # only one entry in July, no pairs
        conn.close()


class TestDjTotal:
    def test_total_plays_and_pairs(self):
        entries = [
            make_resolved_entry(id=1, canonical_name="Autechre", show_id=1, sequence=1, start_time=JAN_15_2024),
            make_resolved_entry(id=2, canonical_name="Stereolab", show_id=1, sequence=2, start_time=JAN_15_2024),
            make_resolved_entry(id=3, canonical_name="Cat Power", show_id=2, sequence=1, start_time=JAN_20_2024),
        ]
        pairs = [
            make_adjacency_pair(source="Autechre", target="Stereolab", show_id=1),
        ]
        conn, _ = _build_test_db(
            resolved_entries=entries,
            adjacency_pairs=pairs,
            show_to_dj={1: 42, 2: 99},
            show_dj_names={1: "DJ Cool", 2: "DJ Sunshine"},
        )

        # Find DJ Cool's id
        dj_cool = conn.execute("SELECT id FROM dj WHERE original_id = '42'").fetchone()
        row = conn.execute(
            "SELECT * FROM dj_total WHERE dj_id = ?", (dj_cool["id"],)
        ).fetchone()
        assert row["total_plays"] == 2  # Autechre + Stereolab in show 1
        assert row["total_pairs"] == 1  # one pair in show 1

        dj_sunshine = conn.execute("SELECT id FROM dj WHERE original_id = '99'").fetchone()
        row2 = conn.execute(
            "SELECT * FROM dj_total WHERE dj_id = ?", (dj_sunshine["id"],)
        ).fetchone()
        assert row2["total_plays"] == 1  # Cat Power in show 2
        assert row2["total_pairs"] == 0  # no pairs in show 2
        conn.close()
