"""Tests for archive S3 client and audio segment extraction."""

from datetime import UTC, datetime

from semantic_index.archive_client import (
    SearchWindow,
    compute_search_windows,
    merge_overlapping_windows,
    timestamp_to_s3_key,
)

# ---------------------------------------------------------------------------
# S3 key construction
# ---------------------------------------------------------------------------


class TestTimestampToS3Key:
    def test_basic_key(self):
        ts = datetime(2020, 3, 14, 19, 30, 0, tzinfo=UTC)
        assert timestamp_to_s3_key(ts) == "2020/03/14/202003141900.mp3"

    def test_midnight(self):
        ts = datetime(2021, 1, 1, 0, 15, 0, tzinfo=UTC)
        assert timestamp_to_s3_key(ts) == "2021/01/01/202101010000.mp3"

    def test_end_of_day(self):
        ts = datetime(2019, 12, 31, 23, 59, 59, tzinfo=UTC)
        assert timestamp_to_s3_key(ts) == "2019/12/31/201912312300.mp3"

    def test_truncates_to_hour(self):
        """Minutes and seconds are ignored — S3 files are hourly."""
        ts1 = datetime(2020, 6, 15, 14, 0, 0, tzinfo=UTC)
        ts2 = datetime(2020, 6, 15, 14, 59, 59, tzinfo=UTC)
        assert timestamp_to_s3_key(ts1) == timestamp_to_s3_key(ts2)


# ---------------------------------------------------------------------------
# Search window computation
# ---------------------------------------------------------------------------


class TestComputeSearchWindows:
    def test_single_entry_centered(self):
        """Window is centered on the play offset within the hour."""
        # Play at 14:20:00, hour starts at 14:00:00 → offset 1,200,000ms
        # Window: offset ± 300,000ms (5 minutes)
        windows = compute_search_windows(
            play_offsets_ms=[1_200_000],
            window_half_width_ms=300_000,
            hour_duration_ms=3_600_000,
        )
        assert len(windows) == 1
        assert windows[0].start_ms == 900_000  # 1,200,000 - 300,000
        assert windows[0].end_ms == 1_500_000  # 1,200,000 + 300,000

    def test_clamps_to_hour_boundaries(self):
        """Windows at the start/end of the hour are clamped to [0, 3600000]."""
        windows = compute_search_windows(
            play_offsets_ms=[60_000],  # 1 minute into the hour
            window_half_width_ms=300_000,  # ± 5 minutes
            hour_duration_ms=3_600_000,
        )
        assert len(windows) == 1
        assert windows[0].start_ms == 0
        assert windows[0].end_ms == 360_000

    def test_clamps_end_of_hour(self):
        windows = compute_search_windows(
            play_offsets_ms=[3_500_000],  # 58:20 into the hour
            window_half_width_ms=300_000,
            hour_duration_ms=3_600_000,
        )
        assert len(windows) == 1
        assert windows[0].start_ms == 3_200_000
        assert windows[0].end_ms == 3_600_000

    def test_multiple_entries_non_overlapping(self):
        windows = compute_search_windows(
            play_offsets_ms=[600_000, 2_400_000],  # 10min and 40min
            window_half_width_ms=300_000,
            hour_duration_ms=3_600_000,
        )
        assert len(windows) == 2

    def test_preserves_play_ids(self):
        """Each window retains the play IDs that contributed to it."""
        windows = compute_search_windows(
            play_offsets_ms=[600_000],
            window_half_width_ms=300_000,
            hour_duration_ms=3_600_000,
            play_ids=[42],
        )
        assert windows[0].play_ids == [42]


class TestMergeOverlappingWindows:
    def test_no_overlap(self):
        windows = [
            SearchWindow(start_ms=0, end_ms=300_000, play_ids=[1]),
            SearchWindow(start_ms=600_000, end_ms=900_000, play_ids=[2]),
        ]
        merged = merge_overlapping_windows(windows)
        assert len(merged) == 2

    def test_overlapping_windows_merged(self):
        windows = [
            SearchWindow(start_ms=0, end_ms=600_000, play_ids=[1]),
            SearchWindow(start_ms=400_000, end_ms=1_000_000, play_ids=[2]),
        ]
        merged = merge_overlapping_windows(windows)
        assert len(merged) == 1
        assert merged[0].start_ms == 0
        assert merged[0].end_ms == 1_000_000
        assert sorted(merged[0].play_ids) == [1, 2]

    def test_adjacent_windows_merged(self):
        """Windows that touch (end == start) are merged."""
        windows = [
            SearchWindow(start_ms=0, end_ms=300_000, play_ids=[1]),
            SearchWindow(start_ms=300_000, end_ms=600_000, play_ids=[2]),
        ]
        merged = merge_overlapping_windows(windows)
        assert len(merged) == 1

    def test_three_overlapping_windows(self):
        windows = [
            SearchWindow(start_ms=0, end_ms=400_000, play_ids=[1]),
            SearchWindow(start_ms=200_000, end_ms=600_000, play_ids=[2]),
            SearchWindow(start_ms=500_000, end_ms=800_000, play_ids=[3]),
        ]
        merged = merge_overlapping_windows(windows)
        assert len(merged) == 1
        assert merged[0].start_ms == 0
        assert merged[0].end_ms == 800_000
        assert sorted(merged[0].play_ids) == [1, 2, 3]

    def test_empty_input(self):
        assert merge_overlapping_windows([]) == []

    def test_single_window(self):
        windows = [SearchWindow(start_ms=0, end_ms=300_000, play_ids=[1])]
        merged = merge_overlapping_windows(windows)
        assert len(merged) == 1
