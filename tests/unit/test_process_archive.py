"""Tests for the process_archive CLI helpers."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from process_archive import _entries_to_offsets, _group_entries_by_hour


class TestGroupEntriesByHour:
    def test_single_hour(self):
        entries = [
            {"add_time_epoch": 1584212400, "id": 1},  # 2020-03-14 19:00:00 UTC
            {"add_time_epoch": 1584213000, "id": 2},  # 2020-03-14 19:10:00 UTC
        ]
        groups = _group_entries_by_hour(entries)
        assert len(groups) == 1
        assert "2020/03/14/202003141900.mp3" in groups
        assert len(groups["2020/03/14/202003141900.mp3"]) == 2

    def test_two_hours(self):
        entries = [
            {"add_time_epoch": 1584212400, "id": 1},  # 2020-03-14 19:00 UTC
            {"add_time_epoch": 1584216000, "id": 2},  # 2020-03-14 20:00 UTC
        ]
        groups = _group_entries_by_hour(entries)
        assert len(groups) == 2
        assert "2020/03/14/202003141900.mp3" in groups
        assert "2020/03/14/202003142000.mp3" in groups

    def test_empty(self):
        assert _group_entries_by_hour([]) == {}


class TestEntriesToOffsets:
    def test_offsets_within_hour(self):
        hour_key = "2020/03/14/202003141900.mp3"
        # Hour starts at 2020-03-14 19:00:00 UTC = epoch 1584212400
        entries = [
            {"add_time_epoch": 1584212400, "id": 1},  # +0s
            {"add_time_epoch": 1584213000, "id": 2},  # +600s = 10 min
            {"add_time_epoch": 1584214200, "id": 3},  # +1800s = 30 min
        ]
        offsets, play_ids = _entries_to_offsets(entries, hour_key)
        assert offsets == [0, 600_000, 1_800_000]
        assert play_ids == [1, 2, 3]

    def test_clamps_negative_offset(self):
        """An entry before the hour start is clamped to 0."""
        hour_key = "2020/03/14/202003141900.mp3"
        entries = [
            {"add_time_epoch": 1584212399, "id": 1},  # 1s before hour start
        ]
        offsets, _ = _entries_to_offsets(entries, hour_key)
        assert offsets == [0]

    def test_clamps_beyond_hour(self):
        """An entry beyond 3600s is clamped to 3,600,000ms."""
        hour_key = "2020/03/14/202003141900.mp3"
        entries = [
            {"add_time_epoch": 1584216001, "id": 1},  # 3601s after hour start
        ]
        offsets, _ = _entries_to_offsets(entries, hour_key)
        assert offsets == [3_600_000]
