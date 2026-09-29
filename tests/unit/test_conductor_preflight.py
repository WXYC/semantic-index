"""Tests for the conductor's disk-space and stale-artifact preflight checks.

``scripts/ec2-build-conductor.sh`` shells out to ``scripts/conductor_preflight.py``
exactly like it already does to ``validate_graph_db.py`` (WXYC/semantic-index#385):
the checks are plain Python functions here so they're unit-testable, with a thin
CLI wrapper the bash script calls.
"""

from pathlib import Path

import pytest

import scripts.conductor_preflight as preflight


def _disk_usage(total: int, used: int, free: int):
    def fake(_path):
        return (total, used, free)

    return fake


# --- required_bytes ----------------------------------------------------------


def test_required_bytes_applies_default_margin():
    assert preflight.required_bytes(1000) == 1100


def test_required_bytes_applies_custom_margin():
    assert preflight.required_bytes(1000, margin=0.5) == 1500


def test_required_bytes_rounds_up():
    # 1000 * 1.1 == 1100.0 exactly; use a margin that forces a fraction.
    assert preflight.required_bytes(999, margin=0.1) == 1099  # ceil(1098.9)


# --- check_free_space ---------------------------------------------------------


def test_check_free_space_passes_when_enough_free(monkeypatch):
    monkeypatch.setattr(preflight.shutil, "disk_usage", _disk_usage(2000, 500, 1500))
    preflight.check_free_space("/data", payload_bytes=1000, step="snapshot")  # 1100 needed


def test_check_free_space_passes_exactly_at_threshold(monkeypatch):
    monkeypatch.setattr(preflight.shutil, "disk_usage", _disk_usage(2000, 900, 1100))
    preflight.check_free_space("/data", payload_bytes=1000, step="snapshot")


def test_check_free_space_raises_actionable_message_when_short(monkeypatch):
    monkeypatch.setattr(preflight.shutil, "disk_usage", _disk_usage(2000, 950, 1050))
    with pytest.raises(preflight.InsufficientDiskSpaceError) as excinfo:
        preflight.check_free_space("/data", payload_bytes=1000, step="download")
    message = str(excinfo.value)
    assert "download" in message
    assert "/data" in message
    # Names both the requirement and what's actually available, not just "full".
    assert "1.1" in message or "1,100" in message or "1100" in message
    assert "1050" in message or "1.0" in message


def test_check_free_space_scales_with_payload_not_a_fixed_constant(monkeypatch):
    # A payload far smaller than some hardcoded "needs N GB free" constant must
    # still pass against modest free space -- the floor tracks the actual size.
    monkeypatch.setattr(preflight.shutil, "disk_usage", _disk_usage(10_000, 8_000, 2_000))
    preflight.check_free_space("/data", payload_bytes=100, step="snapshot")


# --- find_stale_artifacts / clear_stale_artifacts -----------------------------

DB_NAME = "wxyc_artist_graph.db"


def _touch(dir_: Path, name: str, size: int = 0) -> Path:
    p = dir_ / name
    p.write_bytes(b"x" * size)
    return p


def test_find_stale_artifacts_detects_seed_and_incoming_families(tmp_path):
    expected = [
        _touch(tmp_path, f"{DB_NAME}.seed", 10),
        _touch(tmp_path, f"{DB_NAME}.seed-wal", 1),
        _touch(tmp_path, f"{DB_NAME}.seed-shm", 1),
        _touch(tmp_path, f"{DB_NAME}.seed-counts.json", 2),
        _touch(tmp_path, f"{DB_NAME}.incoming", 20),
        _touch(tmp_path, f"{DB_NAME}.incoming-wal", 1),
        _touch(tmp_path, f"{DB_NAME}.incoming-shm", 1),
    ]
    found = preflight.find_stale_artifacts(tmp_path, DB_NAME)
    assert sorted(found) == sorted(expected)


def test_find_stale_artifacts_detects_generalized_preprune_pattern(tmp_path):
    # Generalized form of the one-off manual-backup shape from #385 -- any
    # ".preprune-<suffix>" file, not just the specific timestamp it shipped with.
    orphan = _touch(tmp_path, f"{DB_NAME}.preprune-20260525-212821", 5)
    found = preflight.find_stale_artifacts(tmp_path, DB_NAME)
    assert found == [orphan]


def test_find_stale_artifacts_ignores_live_db_and_its_own_wal_shm(tmp_path):
    _touch(tmp_path, DB_NAME, 100)
    _touch(tmp_path, f"{DB_NAME}-wal", 1)
    _touch(tmp_path, f"{DB_NAME}-shm", 1)
    assert preflight.find_stale_artifacts(tmp_path, DB_NAME) == []


def test_find_stale_artifacts_ignores_permanent_cache_sidecars(tmp_path):
    # Regression guard: the data directory also holds permanent API cache
    # sidecars in the identical "<db-name>.<label>.db" shape
    # (semantic_index/api/database.py's f".{suffix}-cache.db") -- observed on
    # the production host as wxyc_artist_graph.db.{narrative,bio,preview}-cache.db.
    # A blanket "{db_name}.*" sweep would delete these; it must not.
    _touch(tmp_path, f"{DB_NAME}.narrative-cache.db", 50)
    _touch(tmp_path, f"{DB_NAME}.narrative-cache.db-wal", 1)
    _touch(tmp_path, f"{DB_NAME}.narrative-cache.db-shm", 1)
    _touch(tmp_path, f"{DB_NAME}.bio-cache.db", 30)
    _touch(tmp_path, f"{DB_NAME}.preview-cache.db", 10)
    assert preflight.find_stale_artifacts(tmp_path, DB_NAME) == []


def test_clear_stale_artifacts_removes_files_and_reports_sizes(tmp_path):
    seed = _touch(tmp_path, f"{DB_NAME}.seed", 12)
    removed = preflight.clear_stale_artifacts(tmp_path, DB_NAME)
    assert removed == [(seed, 12)]
    assert not seed.exists()


def test_clear_stale_artifacts_leaves_cache_sidecars_and_live_db_untouched(tmp_path):
    live = _touch(tmp_path, DB_NAME, 100)
    cache = _touch(tmp_path, f"{DB_NAME}.bio-cache.db", 30)
    _touch(tmp_path, f"{DB_NAME}.seed", 12)
    preflight.clear_stale_artifacts(tmp_path, DB_NAME)
    assert live.exists()
    assert cache.exists()


def test_clear_stale_artifacts_no_op_when_nothing_stale(tmp_path):
    assert preflight.clear_stale_artifacts(tmp_path, DB_NAME) == []


# --- CLI -----------------------------------------------------------------------


def test_cli_check_space_ok_prints_and_returns_0(monkeypatch, capsys):
    monkeypatch.setattr(preflight.shutil, "disk_usage", _disk_usage(2000, 0, 2000))
    rc = preflight.main(
        ["check-space", "--path", "/data", "--needed-bytes", "1000", "--step", "snapshot"]
    )
    assert rc == 0
    assert "snapshot" in capsys.readouterr().out


def test_cli_check_space_fails_returns_1_and_prints_to_stderr(monkeypatch, capsys):
    monkeypatch.setattr(preflight.shutil, "disk_usage", _disk_usage(2000, 1990, 10))
    rc = preflight.main(
        ["check-space", "--path", "/data", "--needed-bytes", "1000", "--step", "download"]
    )
    assert rc == 1
    err = capsys.readouterr().err
    assert "download" in err
    assert "ERROR" in err


def test_cli_clear_stale_reports_no_artifacts(tmp_path, capsys):
    rc = preflight.main(["clear-stale", "--data-dir", str(tmp_path), "--db-name", DB_NAME])
    assert rc == 0
    assert "no stale artifacts" in capsys.readouterr().out


def test_cli_clear_stale_reports_removed_with_reclaimed_total(tmp_path, capsys):
    _touch(tmp_path, f"{DB_NAME}.seed", 1024)
    rc = preflight.main(["clear-stale", "--data-dir", str(tmp_path), "--db-name", DB_NAME])
    assert rc == 0
    out = capsys.readouterr().out
    assert f"{DB_NAME}.seed" in out
    assert "reclaimed" in out
