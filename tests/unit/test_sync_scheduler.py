"""Tests for the nightly sync scheduler."""

import fcntl
import threading
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pytest

import semantic_index.api.sync_scheduler as sched_mod
from semantic_index.api.sync_scheduler import _seconds_until_next_run, start_scheduler


@pytest.fixture(autouse=True)
def _reset_lock():
    """Reset the module-level lock between tests."""
    old = sched_mod._lock_file
    sched_mod._lock_file = None
    yield
    # Close if a test left a lock open
    if sched_mod._lock_file is not None:
        try:
            sched_mod._lock_file.close()
        except Exception:
            pass
    sched_mod._lock_file = old


class TestStartSchedulerLock:
    """Test file-lock guard for single-worker scheduling."""

    def test_first_call_returns_thread(self, tmp_path: Path) -> None:
        """First worker to call start_scheduler gets a Thread."""
        db_path = str(tmp_path / "graph.db")
        with patch("semantic_index.api.sync_scheduler._scheduler_loop"):
            result = start_scheduler(db_path, dsn="postgresql://localhost/test")

        assert isinstance(result, threading.Thread)

    def test_second_call_returns_none(self, tmp_path: Path) -> None:
        """Second call returns None when lock is already held."""
        db_path = str(tmp_path / "graph.db")
        with patch("semantic_index.api.sync_scheduler._scheduler_loop"):
            first = start_scheduler(db_path, dsn="postgresql://localhost/test")
            second = start_scheduler(db_path, dsn="postgresql://localhost/test")

        assert isinstance(first, threading.Thread)
        assert second is None

    def test_lock_released_after_file_close(self, tmp_path: Path) -> None:
        """After the lock file is closed, a new scheduler can acquire it."""
        db_path = str(tmp_path / "graph.db")
        lock_path = Path(db_path).with_suffix(".sync.lock")

        # Manually acquire and release a lock
        lock_file = open(lock_path, "w")
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_file.close()  # releases the lock

        # Now start_scheduler should succeed
        with patch("semantic_index.api.sync_scheduler._scheduler_loop"):
            result = start_scheduler(db_path, dsn="postgresql://localhost/test")

        assert isinstance(result, threading.Thread)

    def test_lock_file_created_adjacent_to_db(self, tmp_path: Path) -> None:
        """Lock file is created in the same directory as the database."""
        db_path = str(tmp_path / "data" / "graph.db")
        Path(db_path).parent.mkdir(parents=True)

        with patch("semantic_index.api.sync_scheduler._scheduler_loop"):
            start_scheduler(db_path, dsn="postgresql://localhost/test")

        lock_path = Path(db_path).with_suffix(".sync.lock")
        assert lock_path.exists()
        assert lock_path.parent == Path(db_path).parent


class TestSecondsUntilNextRun:
    """Test the scheduling time computation."""

    def test_future_hour_today(self) -> None:
        """If the target hour hasn't passed yet today, schedules for today."""
        # Fix time to 06:00 UTC, target 09:00 UTC → 3 hours
        fake_now = datetime(2026, 4, 19, 6, 0, 0, tzinfo=UTC)
        with patch("semantic_index.api.sync_scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = fake_now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            result = _seconds_until_next_run(9)

        assert result == 3 * 3600

    def test_past_hour_schedules_tomorrow(self) -> None:
        """If the target hour already passed, schedules for tomorrow."""
        fake_now = datetime(2026, 4, 19, 10, 0, 0, tzinfo=UTC)
        with patch("semantic_index.api.sync_scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = fake_now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            result = _seconds_until_next_run(9)

        # Should be ~23 hours, not negative
        assert result > 0
        assert abs(result - 23 * 3600) < 1

    def test_end_of_month_wraps_correctly(self) -> None:
        """Scheduling past the last day of a month rolls to the next month."""
        # 11:00 UTC on Jan 31 with target hour 9 → should schedule Feb 1 at 09:00
        fake_now = datetime(2026, 1, 31, 11, 0, 0, tzinfo=UTC)
        with patch("semantic_index.api.sync_scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = fake_now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            result = _seconds_until_next_run(9)

        # Should be 22 hours (to Feb 1 09:00)
        assert result > 0
        assert abs(result - 22 * 3600) < 1
