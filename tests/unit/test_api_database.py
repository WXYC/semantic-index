"""Tests for SQLite connection PRAGMAs in the Graph API."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from semantic_index.api.database import _CACHE_PAGES, _MMAP_BYTES, _open_db


@pytest.fixture()
def empty_db(tmp_path: Path) -> str:
    """Create an empty SQLite database with valid header."""
    path = tmp_path / "empty.db"
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE _stub (id INTEGER)")
    conn.commit()
    conn.close()
    return str(path)


def test_open_db_applies_mmap_and_cache_pragmas(empty_db: str):
    """_open_db must apply the tuned PRAGMAs and the values must fit inside the
    1 GiB cgroup cap from deploy.yml.

    The cgroup memory.max set by `docker run --memory 1g` counts file-backed
    mmap pages, so mmap_size + per-connection cache + Python heap must all
    coexist in 1 GiB. Hard ceiling sanity-checked here: mmap + cache must
    leave at least ~512 MiB for the Python heap and overhead.
    """
    container_cap_bytes = 1 << 30  # matches --memory 1g in deploy.yml
    heap_reservation_bytes = 1 << 29  # 512 MiB for Python heap + OS

    cache_bytes = -_CACHE_PAGES * 1024  # _CACHE_PAGES is negative KiB
    assert _MMAP_BYTES + cache_bytes + heap_reservation_bytes <= container_cap_bytes, (
        "mmap + cache leaves no room for the Python heap inside the 1 GiB container cap"
    )

    with _open_db(empty_db) as conn:
        mmap = conn.execute("PRAGMA mmap_size").fetchone()[0]
        cache = conn.execute("PRAGMA cache_size").fetchone()[0]
        query_only = conn.execute("PRAGMA query_only").fetchone()[0]

    assert mmap == _MMAP_BYTES
    assert cache == _CACHE_PAGES
    assert query_only == 1
