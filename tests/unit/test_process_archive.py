"""Tests for the process_archive CLI helpers."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from process_archive import (
    ArchiveCheckpointDB,
    _entry_offset_in_hour,
    _group_entries_by_hour,
)

from semantic_index.acousticbrainz import FEATURE_VECTOR_DIM
from semantic_index.archive_essentia import SegmentFeatures


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


class TestEntryOffsetInHour:
    def test_at_hour_start(self):
        """Entry at the hour boundary has offset 0."""
        hour_key = "2020/03/14/202003141900.mp3"
        entry = {"add_time_epoch": 1584212400}  # 2020-03-14 19:00:00 UTC
        assert _entry_offset_in_hour(entry, hour_key) == 0.0

    def test_mid_hour(self):
        """Entry 10 minutes into the hour."""
        hour_key = "2020/03/14/202003141900.mp3"
        entry = {"add_time_epoch": 1584213000}  # +600s
        assert abs(_entry_offset_in_hour(entry, hour_key) - 600.0) < 0.1

    def test_clamps_negative(self):
        """Entry before the hour start is clamped to 0."""
        hour_key = "2020/03/14/202003141900.mp3"
        entry = {"add_time_epoch": 1584212399}  # 1s before hour start
        assert _entry_offset_in_hour(entry, hour_key) == 0.0

    def test_clamps_beyond_hour(self):
        """Entry beyond 3600s is clamped to 3600."""
        hour_key = "2020/03/14/202003141900.mp3"
        entry = {"add_time_epoch": 1584216001}  # 3601s after hour start
        assert _entry_offset_in_hour(entry, hour_key) == 3600.0


class TestArchiveCheckpointDB:
    @pytest.fixture()
    def checkpoint(self, tmp_path):
        db = ArchiveCheckpointDB(str(tmp_path / "test_checkpoint.db"))
        db.initialize()
        yield db
        db.close()

    def test_hour_lifecycle(self, checkpoint):
        """Hour goes through started → complete lifecycle."""
        key = "2021/06/01/202106011200.mp3"
        assert not checkpoint.is_hour_complete(key)
        checkpoint.mark_hour_started(key, play_count=5)
        assert not checkpoint.is_hour_complete(key)
        checkpoint.mark_hour_complete(key, segments_classified=4)
        assert checkpoint.is_hour_complete(key)

    def test_failed_hours(self, checkpoint):
        """Failed hours are returned by get_failed_hours."""
        checkpoint.mark_hour_started("h1", 3)
        checkpoint.mark_hour_failed("h1", "download error")
        checkpoint.mark_hour_started("h2", 5)
        checkpoint.mark_hour_complete("h2", 5)
        assert checkpoint.get_failed_hours() == ["h1"]

    def test_save_and_load_segment(self, checkpoint):
        """Segments can be saved and loaded back with feature vectors."""
        fv = [0.1] * FEATURE_VECTOR_DIM
        seg = SegmentFeatures(
            artist_name="Autechre",
            danceability=0.3,
            genre="electronic",
            genre_probability=0.7,
            genre_vector=[0.0, 0.0, 0.7, 0.0, 0.0, 0.0, 0.0, 0.0, 0.3],
            mood_vector=[0.2, 0.6, 0.8, 0.1, 0.3, 0.1, 0.2],
            voice_instrumental="instrumental",
            voice_instrumental_probability=0.9,
            feature_vector=fv,
        )
        key = "2021/06/01/202106011200.mp3"
        checkpoint.mark_hour_started(key, 1)
        checkpoint.save_segment(seg, key, play_id=42, offset_s=120.0, duration_s=30.0)
        checkpoint.mark_hour_complete(key, 1)

        loaded = checkpoint.load_all_segments()
        assert len(loaded) == 1
        assert loaded[0].artist_name == "Autechre"
        assert loaded[0].genre == "electronic"
        assert len(loaded[0].feature_vector) == FEATURE_VECTOR_DIM

    def test_segment_idempotent(self, checkpoint):
        """Duplicate segment saves are ignored (UNIQUE constraint)."""
        fv = [0.0] * FEATURE_VECTOR_DIM
        seg = SegmentFeatures(
            artist_name="Stereolab",
            danceability=0.5,
            genre="rock",
            genre_probability=0.4,
            genre_vector=[0.0] * 9,
            mood_vector=[0.0] * 7,
            voice_instrumental="voice",
            voice_instrumental_probability=0.6,
            feature_vector=fv,
        )
        key = "2021/06/01/202106011200.mp3"
        checkpoint.mark_hour_started(key, 1)
        checkpoint.save_segment(seg, key, play_id=1, offset_s=0.0, duration_s=30.0)
        checkpoint.save_segment(seg, key, play_id=1, offset_s=0.0, duration_s=30.0)
        checkpoint.mark_hour_complete(key, 1)

        loaded = checkpoint.load_all_segments()
        assert len(loaded) == 1
