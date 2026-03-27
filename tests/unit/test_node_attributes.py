"""Tests for node attribute computation."""

from datetime import UTC, datetime

from semantic_index.node_attributes import compute_artist_stats
from tests.conftest import make_resolved_entry


def _ts(year: int, month: int = 6, day: int = 15) -> int:
    """Create a Java epoch millisecond timestamp for a given year."""
    return int(datetime(year, month, day, tzinfo=UTC).timestamp() * 1000)


class TestActiveYears:
    def test_single_timestamp(self):
        entries = [make_resolved_entry(canonical_name="A", start_time=_ts(2015))]
        stats = compute_artist_stats(entries, {}, {})
        assert stats["A"].active_first_year == 2015
        assert stats["A"].active_last_year == 2015

    def test_spanning_multiple_years(self):
        entries = [
            make_resolved_entry(canonical_name="A", start_time=_ts(2010), sequence=1),
            make_resolved_entry(canonical_name="A", start_time=_ts(2020), sequence=2, id=2),
        ]
        stats = compute_artist_stats(entries, {}, {})
        assert stats["A"].active_first_year == 2010
        assert stats["A"].active_last_year == 2020

    def test_all_null_timestamps(self):
        entries = [
            make_resolved_entry(canonical_name="A", start_time=None),
            make_resolved_entry(canonical_name="A", start_time=None, id=2, sequence=2),
        ]
        stats = compute_artist_stats(entries, {}, {})
        assert stats["A"].active_first_year is None
        assert stats["A"].active_last_year is None

    def test_zero_timestamp_treated_as_null(self):
        entries = [make_resolved_entry(canonical_name="A", start_time=0)]
        stats = compute_artist_stats(entries, {}, {})
        assert stats["A"].active_first_year is None
        assert stats["A"].active_last_year is None

    def test_one_valid_among_nulls(self):
        entries = [
            make_resolved_entry(canonical_name="A", start_time=None, sequence=1),
            make_resolved_entry(canonical_name="A", start_time=_ts(2018), sequence=2, id=2),
            make_resolved_entry(canonical_name="A", start_time=0, sequence=3, id=3),
        ]
        stats = compute_artist_stats(entries, {}, {})
        assert stats["A"].active_first_year == 2018
        assert stats["A"].active_last_year == 2018


class TestDjCount:
    def test_single_dj(self):
        entries = [
            make_resolved_entry(canonical_name="A", show_id=1),
            make_resolved_entry(canonical_name="A", show_id=2, id=2, sequence=2),
        ]
        show_to_dj = {1: 100, 2: 100}  # same DJ
        stats = compute_artist_stats(entries, show_to_dj, {})
        assert stats["A"].dj_count == 1

    def test_multiple_djs(self):
        entries = [
            make_resolved_entry(canonical_name="A", show_id=1),
            make_resolved_entry(canonical_name="A", show_id=2, id=2, sequence=2),
            make_resolved_entry(canonical_name="A", show_id=3, id=3, sequence=3),
        ]
        show_to_dj = {1: 100, 2: 200, 3: 300}
        stats = compute_artist_stats(entries, show_to_dj, {})
        assert stats["A"].dj_count == 3

    def test_dj_name_fallback(self):
        """DJ_ID might be None; fall back to DJ_NAME (string)."""
        entries = [
            make_resolved_entry(canonical_name="A", show_id=1),
            make_resolved_entry(canonical_name="A", show_id=2, id=2, sequence=2),
        ]
        show_to_dj = {1: "DJ Cool", 2: "DJ Rad"}  # string names as fallback
        stats = compute_artist_stats(entries, show_to_dj, {})
        assert stats["A"].dj_count == 2

    def test_show_not_in_mapping(self):
        """Shows not in the mapping are ignored for DJ counting."""
        entries = [make_resolved_entry(canonical_name="A", show_id=999)]
        stats = compute_artist_stats(entries, {}, {})
        assert stats["A"].dj_count == 0


class TestRequestRatio:
    def test_all_requests(self):
        entries = [
            make_resolved_entry(canonical_name="A", request_flag=1),
            make_resolved_entry(canonical_name="A", request_flag=1, id=2, sequence=2),
        ]
        stats = compute_artist_stats(entries, {}, {})
        assert stats["A"].request_ratio == 1.0

    def test_no_requests(self):
        entries = [
            make_resolved_entry(canonical_name="A", request_flag=0),
            make_resolved_entry(canonical_name="A", request_flag=0, id=2, sequence=2),
        ]
        stats = compute_artist_stats(entries, {}, {})
        assert stats["A"].request_ratio == 0.0

    def test_mixed_requests(self):
        entries = [
            make_resolved_entry(canonical_name="A", request_flag=1, sequence=1),
            make_resolved_entry(canonical_name="A", request_flag=0, id=2, sequence=2),
            make_resolved_entry(canonical_name="A", request_flag=0, id=3, sequence=3),
            make_resolved_entry(canonical_name="A", request_flag=1, id=4, sequence=4),
        ]
        stats = compute_artist_stats(entries, {}, {})
        assert stats["A"].request_ratio == 0.5


class TestShowCount:
    def test_single_show(self):
        entries = [make_resolved_entry(canonical_name="A", show_id=1)]
        stats = compute_artist_stats(entries, {}, {})
        assert stats["A"].show_count == 1

    def test_multiple_shows(self):
        entries = [
            make_resolved_entry(canonical_name="A", show_id=1),
            make_resolved_entry(canonical_name="A", show_id=2, id=2, sequence=2),
            make_resolved_entry(canonical_name="A", show_id=3, id=3, sequence=3),
        ]
        stats = compute_artist_stats(entries, {}, {})
        assert stats["A"].show_count == 3

    def test_same_show_counted_once(self):
        entries = [
            make_resolved_entry(canonical_name="A", show_id=1, sequence=1),
            make_resolved_entry(canonical_name="A", show_id=1, sequence=2, id=2),
        ]
        stats = compute_artist_stats(entries, {}, {})
        assert stats["A"].show_count == 1


class TestTotalPlaysAndGenre:
    def test_total_plays(self):
        entries = [
            make_resolved_entry(canonical_name="A"),
            make_resolved_entry(canonical_name="A", id=2, sequence=2),
            make_resolved_entry(canonical_name="B", id=3),
        ]
        stats = compute_artist_stats(entries, {}, {})
        assert stats["A"].total_plays == 2
        assert stats["B"].total_plays == 1

    def test_genre_from_mapping(self):
        entries = [
            make_resolved_entry(
                canonical_name="Autechre",
                library_release_id=100,
            ),
        ]
        genre_names = {15: "Electronic"}
        stats = compute_artist_stats(entries, {}, genre_names, genre_for_release={100: 15})
        assert stats["Autechre"].genre == "Electronic"

    def test_genre_none_when_unmapped(self):
        entries = [make_resolved_entry(canonical_name="A", library_release_id=0)]
        stats = compute_artist_stats(entries, {}, {})
        assert stats["A"].genre is None


class TestMultipleArtists:
    def test_independent_stats(self):
        entries = [
            make_resolved_entry(
                canonical_name="A", show_id=1, request_flag=1, start_time=_ts(2010)
            ),
            make_resolved_entry(
                canonical_name="B", show_id=2, request_flag=0, start_time=_ts(2020), id=2
            ),
        ]
        show_to_dj = {1: 100, 2: 200}
        stats = compute_artist_stats(entries, show_to_dj, {})
        assert stats["A"].total_plays == 1
        assert stats["A"].request_ratio == 1.0
        assert stats["A"].active_first_year == 2010
        assert stats["B"].total_plays == 1
        assert stats["B"].request_ratio == 0.0
        assert stats["B"].active_first_year == 2020
