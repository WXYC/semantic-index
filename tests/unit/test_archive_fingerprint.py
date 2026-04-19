"""Tests for archive fingerprinting and AcoustID lookup."""

import sqlite3

from semantic_index.archive_fingerprint import (
    AcoustIDMatch,
    CheckpointDB,
    _best_match_per_play,
    _generate_fingerprint_offsets,
)

# ---------------------------------------------------------------------------
# Fingerprint offset generation
# ---------------------------------------------------------------------------


class TestGenerateFingerprintOffsets:
    def test_basic_offsets(self):
        """Offsets are generated at step_ms intervals within the window."""
        offsets = _generate_fingerprint_offsets(
            window_start_ms=0,
            window_end_ms=60_000,
            segment_duration_ms=15_000,
            step_ms=10_000,
        )
        # Offsets: 0, 10000, 20000, 30000, 40000 (45000 would exceed 60000-15000=45000)
        assert offsets == [0, 10_000, 20_000, 30_000, 40_000, 45_000]

    def test_window_smaller_than_segment(self):
        """If the window is smaller than a segment, return one offset at the start."""
        offsets = _generate_fingerprint_offsets(
            window_start_ms=100_000,
            window_end_ms=110_000,
            segment_duration_ms=15_000,
            step_ms=10_000,
        )
        assert offsets == [100_000]

    def test_exact_fit(self):
        """Window exactly fits one segment."""
        offsets = _generate_fingerprint_offsets(
            window_start_ms=0,
            window_end_ms=15_000,
            segment_duration_ms=15_000,
            step_ms=10_000,
        )
        assert offsets == [0]

    def test_last_offset_flush_with_end(self):
        """Last offset is pushed back so segment doesn't exceed window end."""
        offsets = _generate_fingerprint_offsets(
            window_start_ms=0,
            window_end_ms=30_000,
            segment_duration_ms=15_000,
            step_ms=10_000,
        )
        # Regular: 0, 10000. Last possible: 30000 - 15000 = 15000.
        # 10000 < 15000, so 15000 is appended.
        assert offsets == [0, 10_000, 15_000]


# ---------------------------------------------------------------------------
# Best match per play
# ---------------------------------------------------------------------------


class TestBestMatchPerPlay:
    def test_selects_highest_score(self):
        matches = [
            AcoustIDMatch(
                offset_ms=0,
                recording_mbid="aaa",
                score=0.6,
                play_id=1,
            ),
            AcoustIDMatch(
                offset_ms=10_000,
                recording_mbid="aaa",
                score=0.9,
                play_id=1,
            ),
            AcoustIDMatch(
                offset_ms=20_000,
                recording_mbid="bbb",
                score=0.7,
                play_id=1,
            ),
        ]
        best = _best_match_per_play(matches)
        assert len(best) == 1
        assert best[0].score == 0.9
        assert best[0].recording_mbid == "aaa"

    def test_multiple_plays(self):
        matches = [
            AcoustIDMatch(offset_ms=0, recording_mbid="aaa", score=0.8, play_id=1),
            AcoustIDMatch(offset_ms=0, recording_mbid="bbb", score=0.9, play_id=2),
        ]
        best = _best_match_per_play(matches)
        assert len(best) == 2

    def test_empty(self):
        assert _best_match_per_play([]) == []


# ---------------------------------------------------------------------------
# Checkpoint DB
# ---------------------------------------------------------------------------


class TestCheckpointDB:
    def test_create_tables(self, tmp_path):
        db_path = tmp_path / "checkpoint.db"
        cp = CheckpointDB(str(db_path))
        cp.initialize()

        conn = sqlite3.connect(str(db_path))
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        assert "hour_progress" in tables
        assert "segment_match" in tables
        conn.close()

    def test_mark_hour_complete(self, tmp_path):
        db_path = tmp_path / "checkpoint.db"
        cp = CheckpointDB(str(db_path))
        cp.initialize()

        cp.mark_hour_started("2020/03/14/202003141900.mp3", play_count=5)
        cp.mark_hour_complete(
            "2020/03/14/202003141900.mp3",
            segments_fingerprinted=30,
            segments_matched=3,
            segments_extracted=0,
        )
        assert cp.is_hour_complete("2020/03/14/202003141900.mp3")

    def test_incomplete_hour_not_skipped(self, tmp_path):
        db_path = tmp_path / "checkpoint.db"
        cp = CheckpointDB(str(db_path))
        cp.initialize()
        assert not cp.is_hour_complete("2020/03/14/202003141900.mp3")

    def test_mark_hour_failed(self, tmp_path):
        db_path = tmp_path / "checkpoint.db"
        cp = CheckpointDB(str(db_path))
        cp.initialize()

        cp.mark_hour_started("2020/03/14/202003141900.mp3", play_count=5)
        cp.mark_hour_failed("2020/03/14/202003141900.mp3", "S3 not found")
        assert not cp.is_hour_complete("2020/03/14/202003141900.mp3")

    def test_save_segment_match(self, tmp_path):
        db_path = tmp_path / "checkpoint.db"
        cp = CheckpointDB(str(db_path))
        cp.initialize()

        match = AcoustIDMatch(
            offset_ms=120_000,
            recording_mbid="aaa-bbb",
            score=0.85,
            play_id=42,
        )
        cp.save_segment_match(
            archive_key="2020/03/14/202003141900.mp3",
            match=match,
            duration_ms=15_000,
            artist_name="Autechre",
        )

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute("SELECT * FROM segment_match").fetchall()
        assert len(rows) == 1
        conn.close()

    def test_idempotent_segment_insert(self, tmp_path):
        """Duplicate (archive_key, offset_ms) is ignored."""
        db_path = tmp_path / "checkpoint.db"
        cp = CheckpointDB(str(db_path))
        cp.initialize()

        match = AcoustIDMatch(
            offset_ms=120_000,
            recording_mbid="aaa-bbb",
            score=0.85,
            play_id=42,
        )
        cp.save_segment_match("key.mp3", match, 15_000, "Autechre")
        cp.save_segment_match("key.mp3", match, 15_000, "Autechre")  # duplicate

        conn = sqlite3.connect(str(db_path))
        count = conn.execute("SELECT COUNT(*) FROM segment_match").fetchone()[0]
        assert count == 1
        conn.close()

    def test_get_failed_hours(self, tmp_path):
        db_path = tmp_path / "checkpoint.db"
        cp = CheckpointDB(str(db_path))
        cp.initialize()

        cp.mark_hour_started("hour1.mp3", play_count=5)
        cp.mark_hour_failed("hour1.mp3", "timeout")
        cp.mark_hour_started("hour2.mp3", play_count=3)
        cp.mark_hour_complete("hour2.mp3", 10, 2, 0)

        failed = cp.get_failed_hours()
        assert failed == ["hour1.mp3"]
