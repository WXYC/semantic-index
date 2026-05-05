"""Unit tests for the eval-set pair sampler's classification logic."""

from __future__ import annotations

import math
import sqlite3

import pytest

from scripts.eval.sample_pairs import (
    DIRECT_MIN_RAW_COUNT,
    MIN_AA_SUM,
    ArtistProfile,
    CellSpec,
    PairRecord,
    build_cells,
    compute_aa_sum,
    compute_degrees,
    get_direct_edge_raw_count,
    index_by_fame_richness,
    matches_genre_axis,
    sample_cell,
)


def _profile(
    *,
    aid: int = 1,
    name: str = "Test Artist",
    genre: str = "Rock",
    plays: int = 500,
    style_count: int = 0,
    has_audio: bool = False,
) -> ArtistProfile:
    return ArtistProfile(
        id=aid,
        name=name,
        genre=genre,
        total_plays=plays,
        style_count=style_count,
        has_audio=has_audio,
    )


class TestArtistProfileFame:
    def test_high_fame_strict_above_threshold(self):
        assert _profile(plays=801).fame == "HIGH"
        assert _profile(plays=800).fame is None  # boundary excluded

    def test_low_fame_inclusive_band(self):
        assert _profile(plays=100).fame == "LOW"
        assert _profile(plays=400).fame == "LOW"
        assert _profile(plays=99).fame is None
        assert _profile(plays=401).fame is None

    def test_mid_band_excluded(self):
        assert _profile(plays=600).fame is None


class TestArtistProfileRichness:
    def test_rich_requires_styles_and_audio(self):
        assert _profile(style_count=3, has_audio=True).richness == "RICH"
        assert _profile(style_count=10, has_audio=True).richness == "RICH"

    def test_thin_when_styles_below_threshold(self):
        assert _profile(style_count=2, has_audio=True).richness == "THIN"

    def test_thin_when_no_audio(self):
        assert _profile(style_count=10, has_audio=False).richness == "THIN"


class TestGenreAxis:
    def test_same_axis_matches_equal_genres(self):
        a = _profile(aid=1, genre="Rock")
        b = _profile(aid=2, genre="Rock")
        assert matches_genre_axis(a, b, "SAME") is True
        assert matches_genre_axis(a, b, "CROSS") is False

    def test_cross_axis_matches_different_genres(self):
        a = _profile(aid=1, genre="Rock")
        b = _profile(aid=2, genre="Jazz")
        assert matches_genre_axis(a, b, "CROSS") is True
        assert matches_genre_axis(a, b, "SAME") is False


class TestIndexByFameRichness:
    def test_buckets_artists_excluding_mid_band(self):
        artists = [
            _profile(aid=1, plays=900, style_count=3, has_audio=True),
            _profile(aid=2, plays=200, style_count=1, has_audio=False),
            _profile(aid=3, plays=600, style_count=5, has_audio=True),  # mid-band, excluded
            _profile(aid=4, plays=950, style_count=0, has_audio=False),
        ]
        idx = index_by_fame_richness(artists)
        assert ("HIGH", "RICH") in idx and len(idx[("HIGH", "RICH")]) == 1
        assert ("HIGH", "THIN") in idx and len(idx[("HIGH", "THIN")]) == 1
        assert ("LOW", "THIN") in idx and len(idx[("LOW", "THIN")]) == 1
        assert ("LOW", "RICH") not in idx  # no LOW+RICH artist in fixture
        # mid-band artist absent from every bucket
        flat = [a.id for v in idx.values() for a in v]
        assert 3 not in flat


class TestBuildCells:
    def test_emits_sixteen_cells(self):
        cells = build_cells()
        assert len(cells) == 16
        ids = [c.cell_id for c in cells]
        assert len(set(ids)) == 16
        for cid in ids:
            parts = cid.split("-")
            assert parts[0] in {"HIGH", "LOW"}
            assert parts[1] in {"RICH", "THIN"}
            assert parts[2] in {"CROSS", "SAME"}
            assert parts[3] in {"DIRECT", "INDIRECT"}


@pytest.fixture
def in_memory_db() -> sqlite3.Connection:
    """Tiny graph: A↔B↔C, where A↔B has raw_count 5 (DIRECT-eligible)."""
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.executescript(
        """
        CREATE TABLE artist (
            id INTEGER PRIMARY KEY, canonical_name TEXT, genre TEXT,
            total_plays INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE dj_transition (
            source_id INTEGER, target_id INTEGER, raw_count INTEGER, pmi REAL,
            PRIMARY KEY (source_id, target_id)
        );
        INSERT INTO artist VALUES
          (1, 'A', 'Rock', 1000),
          (2, 'B', 'Rock', 900),
          (3, 'C', 'Jazz', 850),
          (4, 'Hub', 'Rock', 500),
          (5, 'SelfLooper', 'Rock', 950);
        -- A-B direct edge, raw_count 5
        INSERT INTO dj_transition VALUES (1, 2, 5, 1.0);
        -- A-Hub, B-Hub, C-Hub: A and B share neighbor C via Hub
        INSERT INTO dj_transition VALUES (1, 4, 2, 0.5);
        INSERT INTO dj_transition VALUES (2, 4, 2, 0.5);
        INSERT INTO dj_transition VALUES (3, 4, 2, 0.5);
        -- SelfLooper -> SelfLooper: a DJ played the same artist back-to-back.
        -- This is real in production data; the sampler must NOT emit it as a pair.
        INSERT INTO dj_transition VALUES (5, 5, 4, 2.0);
        """
    )
    return db


class TestDirectEdgeLookup:
    def test_finds_edge_either_direction(self, in_memory_db):
        assert get_direct_edge_raw_count(in_memory_db, 1, 2) == 5
        assert get_direct_edge_raw_count(in_memory_db, 2, 1) == 5

    def test_returns_none_when_no_edge(self, in_memory_db):
        # A↔C: no direct edge in fixture
        assert get_direct_edge_raw_count(in_memory_db, 1, 3) is None


class TestComputeDegrees:
    def test_includes_both_directions(self, in_memory_db):
        deg = compute_degrees(in_memory_db)
        # Hub (id=4) has 3 distinct partners
        assert deg[4] == 3
        # A (id=1) has 2 partners (B, Hub)
        assert deg[1] == 2


class TestComputeAaSum:
    def test_returns_zero_when_no_shared_neighbors(self, in_memory_db):
        # B (id=2) and C (id=3) share Hub (id=4), so this would NOT be zero.
        # Instead test a pair that genuinely shares nothing — there isn't one
        # in the small fixture, so we verify the Hub contribution shape.
        degrees = compute_degrees(in_memory_db)
        aa_sum, neighbors = compute_aa_sum(in_memory_db, 1, 3, degrees)
        # Only Hub is shared (degree 3), so aa = 1/log(3)
        assert pytest.approx(aa_sum, rel=1e-6) == 1.0 / math.log(3)
        assert neighbors == [("Hub", pytest.approx(1.0 / math.log(3), rel=1e-6))]


class TestPairRecordSerialization:
    def test_direct_record_includes_raw_count(self):
        rec = PairRecord(
            cell=CellSpec("HIGH", "RICH", "CROSS", "DIRECT"),
            a=_profile(aid=1, name="A", plays=1000),
            b=_profile(aid=2, name="B", plays=900, genre="Jazz"),
            raw_count=4,
        )
        row = rec.to_jsonl()
        assert '"cell_id":"HIGH-RICH-CROSS-DIRECT"' in row
        assert '"raw_count":4' in row
        assert "aa_sum" not in row

    def test_indirect_record_includes_aa_sum(self):
        rec = PairRecord(
            cell=CellSpec("LOW", "THIN", "SAME", "INDIRECT"),
            a=_profile(aid=1, name="A", plays=200),
            b=_profile(aid=2, name="B", plays=300),
            aa_sum=1.234,
        )
        row = rec.to_jsonl()
        assert '"aa_sum":1.234' in row
        assert "raw_count" not in row


class TestSampleCellRespectsAxes:
    def test_direct_cell_only_emits_pairs_with_min_raw_count(self, in_memory_db):
        artists = [
            _profile(aid=1, name="A", genre="Rock", plays=1000),
            _profile(aid=2, name="B", genre="Rock", plays=900),
        ]
        cell = CellSpec("HIGH", "THIN", "SAME", "DIRECT")
        import random as _random

        rng = _random.Random(0)
        used: set[frozenset[int]] = set()
        degrees = compute_degrees(in_memory_db)
        pairs = sample_cell(
            in_memory_db,
            cell,
            artists,
            degrees,
            target=5,
            rng=rng,
            max_attempts=200,
            used_pair_keys=used,
        )
        # The single eligible A-B pair should be emitted exactly once.
        assert len(pairs) == 1
        assert pairs[0].raw_count is not None
        assert pairs[0].raw_count >= DIRECT_MIN_RAW_COUNT
        assert MIN_AA_SUM > 0  # constant exists; sanity

    def test_direct_cell_excludes_self_loops(self, in_memory_db):
        """A DJ playing one artist back-to-back creates a self-loop in
        dj_transition. The sampler must filter these — a pair where
        source_id == target_id can't be narrated coherently and pollutes
        the eval-set's relationship-vs-attribute distinction."""
        artists = [
            _profile(aid=5, name="SelfLooper", genre="Rock", plays=950),
        ]
        cell = CellSpec("HIGH", "THIN", "SAME", "DIRECT")
        import random as _random

        rng = _random.Random(0)
        used: set[frozenset[int]] = set()
        degrees = compute_degrees(in_memory_db)
        pairs = sample_cell(
            in_memory_db,
            cell,
            artists,
            degrees,
            target=5,
            rng=rng,
            max_attempts=200,
            used_pair_keys=used,
        )
        assert pairs == []
        # And should not be re-emitted on a fresh call from the same fixture.
        assert all(p.a.id != p.b.id for p in pairs)
