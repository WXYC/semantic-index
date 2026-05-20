"""Tests for the nightly sync scheduler."""

import fcntl
import logging
import threading
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pytest

import semantic_index.api.sync_scheduler as sched_mod
from semantic_index.api.sync_scheduler import (
    _scheduler_loop,
    _seconds_until_next_run,
    _sleep_with_heartbeat,
    start_scheduler,
)


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


class TestSleepWithHeartbeat:
    """Sleeping the daily window must emit periodic heartbeat logs so
    ``docker logs`` reflects whether the scheduler thread is alive without
    inspecting ``/proc/1/task``. See WXYC/semantic-index#322.
    """

    def test_heartbeat_logs_between_chunks(self, caplog) -> None:
        """A multi-chunk sleep produces one heartbeat log per completed chunk
        (except the final chunk, which is followed immediately by the sync run).
        """
        with (
            patch("semantic_index.api.sync_scheduler.time.sleep"),
            patch.object(sched_mod, "_HEARTBEAT_INTERVAL_SECONDS", 100),
            caplog.at_level(logging.INFO, logger=sched_mod.__name__),
        ):
            _sleep_with_heartbeat(total_seconds=250, hour_utc=9)

        heartbeats = [r for r in caplog.records if "heartbeat" in r.getMessage().lower()]
        # 250 / 100 = 3 chunks of [100, 100, 50]; heartbeats fire after the
        # first two chunks (the third lands at the sync boundary).
        assert len(heartbeats) == 2

    def test_no_heartbeat_for_short_sleep(self, caplog) -> None:
        """A sleep shorter than the heartbeat interval emits no heartbeat."""
        with (
            patch("semantic_index.api.sync_scheduler.time.sleep"),
            patch.object(sched_mod, "_HEARTBEAT_INTERVAL_SECONDS", 1000),
            caplog.at_level(logging.INFO, logger=sched_mod.__name__),
        ):
            _sleep_with_heartbeat(total_seconds=200, hour_utc=9)

        heartbeats = [r for r in caplog.records if "heartbeat" in r.getMessage().lower()]
        assert heartbeats == []

    def test_zero_sleep_returns_immediately(self) -> None:
        """``_sleep_with_heartbeat(0, ...)`` must not call ``time.sleep``."""
        with patch("semantic_index.api.sync_scheduler.time.sleep") as mock_sleep:
            _sleep_with_heartbeat(total_seconds=0, hour_utc=9)
        mock_sleep.assert_not_called()


class TestSchedulerLoopCrashHandling:
    """The scheduler runs as a daemon thread. If the outer loop dies silently,
    the daily sync never runs and we have no signal. WXYC/semantic-index#322
    is the canonical incident: 16 days of silent sync failure.
    """

    def test_outer_exception_logs_and_reraises(self, caplog) -> None:
        """Any exception escaping the loop must be logged with traceback before
        the thread exits, so ``docker logs`` shows the cause and Sentry receives
        the event (via the logging integration).
        """

        def raise_on_compute(_hour: int) -> float:
            raise RuntimeError("clock broke")

        with (
            patch(
                "semantic_index.api.sync_scheduler._seconds_until_next_run",
                side_effect=raise_on_compute,
            ),
            caplog.at_level(logging.ERROR, logger=sched_mod.__name__),
            pytest.raises(RuntimeError, match="clock broke"),
        ):
            _scheduler_loop(
                db_path="/tmp/never-used.db",
                dsn="postgresql://localhost/test",
                min_count=2,
                hour_utc=9,
            )

        crash_records = [
            r for r in caplog.records if r.levelno == logging.ERROR and r.exc_info is not None
        ]
        assert crash_records, "scheduler thread crash must log with exception info"
        assert "scheduler" in crash_records[-1].getMessage().lower()
