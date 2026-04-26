"""Tests for the M0.2 mojibake duplicate-artist audit."""

from __future__ import annotations

import importlib.util
import sqlite3
import sys
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "audit" / "si_mojibake_scan.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("si_mojibake_scan", SCRIPT_PATH)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules["si_mojibake_scan"] = mod  # required for dataclasses on 3.14
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def mod():
    return _load_module()


# Mojibake fixtures (kept as escape sequences so the file's UTF-8 encoding
# can't ambiguate the byte sequence we're testing).
BJORK_MOJIBAKE = "bj\u00c3\u00b6rk"  # "björk" as latin1-misread of utf-8 "björk"
BJORK_FIXED = "bj\u00f6rk"  # "björk"
MU_MOJIBAKE = "\u00ce\u00bc-ziq"  # "Î¼-ziq" — mojibake of "μ-ziq"
MU_FIXED = "\u03bc-ziq"  # "μ-ziq"


class TestTryFix:
    def test_recovers_double_encoded_lowercase_diacritic(self, mod):
        assert mod.try_fix(BJORK_MOJIBAKE) == BJORK_FIXED

    def test_recovers_greek(self, mod):
        assert mod.try_fix(MU_MOJIBAKE) == MU_FIXED

    def test_returns_none_for_clean_string(self, mod):
        assert mod.try_fix(BJORK_FIXED) is None

    def test_returns_none_for_ascii(self, mod):
        assert mod.try_fix("autechre") is None

    def test_returns_none_for_unrecoverable_lossy(self, mod):
        # '?' bytes cannot round-trip back into a valid utf-8 sequence
        assert mod.try_fix("b\u00e3?rns") is None

    def test_returns_none_for_empty(self, mod):
        assert mod.try_fix("") is None
        assert mod.try_fix(None) is None

    def test_rejects_replacement_char(self, mod):
        # If decoding produces U+FFFD, the recovery is unreliable.
        assert mod.try_fix("\ufffd") is None


def _make_db() -> sqlite3.Connection:
    """In-memory schema that mirrors the audit's reads."""
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE artist (
            id INTEGER PRIMARY KEY,
            canonical_name TEXT NOT NULL UNIQUE,
            total_plays INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE dj_transition (
            source_id INTEGER NOT NULL,
            target_id INTEGER NOT NULL,
            raw_count INTEGER NOT NULL,
            pmi REAL NOT NULL,
            PRIMARY KEY (source_id, target_id)
        );
        CREATE TABLE cross_reference (
            artist_a_id INTEGER NOT NULL,
            artist_b_id INTEGER NOT NULL,
            comment TEXT,
            source TEXT NOT NULL,
            PRIMARY KEY (artist_a_id, artist_b_id, source)
        );
        """
    )
    return conn


class TestFindDuplicates:
    def test_finds_pair_when_both_forms_exist(self, mod):
        conn = _make_db()
        conn.executemany(
            "INSERT INTO artist (id, canonical_name, total_plays) VALUES (?, ?, ?)",
            [
                (1, BJORK_FIXED, 100),
                (2, BJORK_MOJIBAKE, 5),
                (3, "autechre", 200),
            ],
        )
        conn.execute(
            "INSERT INTO dj_transition VALUES (?, ?, ?, ?)", (2, 3, 4, 0.1)
        )  # corrupted -> autechre
        conn.execute(
            "INSERT INTO dj_transition VALUES (?, ?, ?, ?)", (3, 1, 7, 0.2)
        )  # autechre -> fixed
        conn.execute("INSERT INTO cross_reference VALUES (?, ?, ?, ?)", (2, 3, "see", "lc"))

        pairs = mod.find_mojibake_duplicates(conn)

        assert len(pairs) == 1
        p = pairs[0]
        assert p.corrupted_id == 2
        assert p.fixed_id == 1
        assert p.corrupted_name == BJORK_MOJIBAKE
        assert p.fixed_name == BJORK_FIXED
        assert p.corrupted_play_count == 5
        assert p.fixed_play_count == 100
        assert p.corrupted_edge_count == 2  # one dj_transition + one xref
        assert p.fixed_edge_count == 1  # one dj_transition

    def test_no_pair_when_only_corrupted_exists(self, mod):
        conn = _make_db()
        conn.execute(
            "INSERT INTO artist (id, canonical_name, total_plays) VALUES (?, ?, ?)",
            (1, BJORK_MOJIBAKE, 5),
        )
        assert mod.find_mojibake_duplicates(conn) == []

    def test_no_pair_for_clean_unicode(self, mod):
        conn = _make_db()
        conn.executemany(
            "INSERT INTO artist (id, canonical_name, total_plays) VALUES (?, ?, ?)",
            [(1, BJORK_FIXED, 100), (2, "jo\u00e3o gilberto", 50)],
        )
        assert mod.find_mojibake_duplicates(conn) == []

    def test_handles_missing_optional_edge_tables(self, mod):
        conn = sqlite3.connect(":memory:")
        conn.executescript(
            """
            CREATE TABLE artist (
                id INTEGER PRIMARY KEY,
                canonical_name TEXT NOT NULL UNIQUE,
                total_plays INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE dj_transition (
                source_id INTEGER NOT NULL,
                target_id INTEGER NOT NULL,
                raw_count INTEGER NOT NULL,
                pmi REAL NOT NULL,
                PRIMARY KEY (source_id, target_id)
            );
            """
        )
        conn.executemany(
            "INSERT INTO artist (id, canonical_name, total_plays) VALUES (?, ?, ?)",
            [(1, BJORK_FIXED, 100), (2, BJORK_MOJIBAKE, 5)],
        )
        # No cross_reference table — must not crash.
        pairs = mod.find_mojibake_duplicates(conn)
        assert len(pairs) == 1
        assert pairs[0].corrupted_id == 2


class TestRender:
    def test_csv_round_trip(self, mod, tmp_path):
        pair = mod.DuplicatePair(
            corrupted_id=2,
            corrupted_name=BJORK_MOJIBAKE,
            fixed_id=1,
            fixed_name=BJORK_FIXED,
            corrupted_edge_count=3,
            fixed_edge_count=10,
            corrupted_play_count=5,
            fixed_play_count=100,
        )
        csv_path = tmp_path / "out.csv"
        mod.write_csv([pair], csv_path)
        text = csv_path.read_text(encoding="utf-8")
        assert BJORK_MOJIBAKE in text
        assert BJORK_FIXED in text
        assert text.splitlines()[0].startswith("corrupted_id")

    def test_summary_includes_totals_and_top(self, mod, tmp_path):
        pairs = [
            mod.DuplicatePair(2, BJORK_MOJIBAKE, 1, BJORK_FIXED, 3, 10, 5, 100),
            mod.DuplicatePair(4, MU_MOJIBAKE, 3, MU_FIXED, 1, 2, 1, 5),
        ]
        path = tmp_path / "summary.md"
        mod.write_summary(pairs, path)
        text = path.read_text(encoding="utf-8")
        assert "Total pairs: 2" in text
        assert "Total edges affected: 16" in text  # 3+10+1+2
        assert BJORK_FIXED in text
        assert MU_FIXED in text

    def test_summary_reports_zero_pairs_with_diagnostics(self, mod, tmp_path):
        path = tmp_path / "summary.md"
        counts = mod.ScanCounts(total_artists=100, fixable_names=0, lossy_mojibake_names=42)
        mod.write_summary([], path, counts=counts)
        text = path.read_text(encoding="utf-8")
        assert "Total pairs: 0" in text
        assert "Total artists scanned: 100" in text
        assert "Lossy-mojibake names" in text
        assert "42" in text
        assert "No round-trippable duplicate pairs" in text


class TestScanCounts:
    def test_counts_partition_artists(self, mod):
        conn = _make_db()
        conn.executemany(
            "INSERT INTO artist (id, canonical_name, total_plays) VALUES (?, ?, ?)",
            [
                (1, BJORK_FIXED, 0),  # clean unicode
                (2, BJORK_MOJIBAKE, 0),  # round-trippable
                (3, "autechre", 0),  # ascii
                (4, "b\u00e3?rns", 0),  # lossy
                (5, "ã??ã?¯", 0),  # lossy
            ],
        )
        c = mod.scan_counts(conn)
        assert c.total_artists == 5
        assert c.fixable_names == 1
        assert c.lossy_mojibake_names == 2
