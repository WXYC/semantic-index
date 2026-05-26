"""Tests for Discogs-derived edge extraction functions."""

import json
import sqlite3
from pathlib import Path

import pytest

from semantic_index.discogs_edges import (
    extract_compilation_coappearance,
    extract_label_family,
    extract_shared_personnel,
    extract_shared_styles,
    prune_label_family,
    prune_shared_personnel,
)
from semantic_index.models import CompilationAppearance, LabelInfo
from tests.conftest import make_artist_enrichment, make_personnel_credit


class TestExtractSharedPersonnel:
    """Tests for extract_shared_personnel."""

    def test_two_artists_sharing_one_person(self):
        enrichments = {
            "Autechre": make_artist_enrichment(
                canonical_name="Autechre",
                personnel=[make_personnel_credit(name="Rob Brown")],
            ),
            "Stereolab": make_artist_enrichment(
                canonical_name="Stereolab",
                personnel=[make_personnel_credit(name="Rob Brown")],
            ),
        }

        edges = extract_shared_personnel(enrichments)

        assert len(edges) == 1
        assert edges[0].artist_a == "Autechre"
        assert edges[0].artist_b == "Stereolab"
        assert edges[0].shared_count == 1
        assert edges[0].shared_names == ["Rob Brown"]

    def test_two_artists_sharing_two_people(self):
        enrichments = {
            "Autechre": make_artist_enrichment(
                canonical_name="Autechre",
                personnel=[
                    make_personnel_credit(name="Rob Brown"),
                    make_personnel_credit(name="Sean Booth"),
                ],
            ),
            "Stereolab": make_artist_enrichment(
                canonical_name="Stereolab",
                personnel=[
                    make_personnel_credit(name="Rob Brown"),
                    make_personnel_credit(name="Sean Booth"),
                ],
            ),
        }

        edges = extract_shared_personnel(enrichments)

        assert len(edges) == 1
        assert edges[0].shared_count == 2
        assert edges[0].shared_names == ["Rob Brown", "Sean Booth"]

    def test_three_artists_sharing_one_person(self):
        enrichments = {
            "Autechre": make_artist_enrichment(
                canonical_name="Autechre",
                personnel=[make_personnel_credit(name="Rob Brown")],
            ),
            "Cat Power": make_artist_enrichment(
                canonical_name="Cat Power",
                personnel=[make_personnel_credit(name="Rob Brown")],
            ),
            "Stereolab": make_artist_enrichment(
                canonical_name="Stereolab",
                personnel=[make_personnel_credit(name="Rob Brown")],
            ),
        }

        edges = extract_shared_personnel(enrichments)

        assert len(edges) == 3
        pairs = [(e.artist_a, e.artist_b) for e in edges]
        assert ("Autechre", "Cat Power") in pairs
        assert ("Autechre", "Stereolab") in pairs
        assert ("Cat Power", "Stereolab") in pairs

    def test_no_shared_personnel(self):
        enrichments = {
            "Autechre": make_artist_enrichment(
                canonical_name="Autechre",
                personnel=[make_personnel_credit(name="Rob Brown")],
            ),
            "Stereolab": make_artist_enrichment(
                canonical_name="Stereolab",
                personnel=[make_personnel_credit(name="Tim Gane")],
            ),
        }

        edges = extract_shared_personnel(enrichments)

        assert len(edges) == 0

    def test_min_shared_filter(self):
        enrichments = {
            "Autechre": make_artist_enrichment(
                canonical_name="Autechre",
                personnel=[
                    make_personnel_credit(name="Rob Brown"),
                    make_personnel_credit(name="Sean Booth"),
                ],
            ),
            "Stereolab": make_artist_enrichment(
                canonical_name="Stereolab",
                personnel=[
                    make_personnel_credit(name="Rob Brown"),
                ],
            ),
            "Cat Power": make_artist_enrichment(
                canonical_name="Cat Power",
                personnel=[
                    make_personnel_credit(name="Rob Brown"),
                    make_personnel_credit(name="Sean Booth"),
                ],
            ),
        }

        edges = extract_shared_personnel(enrichments, min_shared=2)

        assert len(edges) == 1
        assert edges[0].artist_a == "Autechre"
        assert edges[0].artist_b == "Cat Power"
        assert edges[0].shared_count == 2

    def test_empty_personnel(self):
        enrichments = {
            "Autechre": make_artist_enrichment(
                canonical_name="Autechre",
                personnel=[],
            ),
            "Stereolab": make_artist_enrichment(
                canonical_name="Stereolab",
                personnel=[],
            ),
        }

        edges = extract_shared_personnel(enrichments)

        assert len(edges) == 0

    def test_max_artists_excludes_ubiquitous_personnel(self):
        """Personnel credited on more than max_artists should be skipped."""
        # "Bob Ludwig" appears on all 4 artists — should be excluded with max_artists=3
        # "Rob Brown" appears on only 2 — should still generate an edge
        enrichments = {
            "Autechre": make_artist_enrichment(
                canonical_name="Autechre",
                personnel=[
                    make_personnel_credit(name="Bob Ludwig"),
                    make_personnel_credit(name="Rob Brown"),
                ],
            ),
            "Cat Power": make_artist_enrichment(
                canonical_name="Cat Power",
                personnel=[make_personnel_credit(name="Bob Ludwig")],
            ),
            "Father John Misty": make_artist_enrichment(
                canonical_name="Father John Misty",
                personnel=[make_personnel_credit(name="Bob Ludwig")],
            ),
            "Stereolab": make_artist_enrichment(
                canonical_name="Stereolab",
                personnel=[
                    make_personnel_credit(name="Bob Ludwig"),
                    make_personnel_credit(name="Rob Brown"),
                ],
            ),
        }

        edges = extract_shared_personnel(enrichments, max_artists=3)

        # Only the Rob Brown edge (Autechre-Stereolab) should survive
        assert len(edges) == 1
        assert edges[0].artist_a == "Autechre"
        assert edges[0].artist_b == "Stereolab"
        assert edges[0].shared_names == ["Rob Brown"]

    def test_max_artists_none_disables_cap(self):
        """max_artists=None (default) should not filter anything."""
        enrichments = {
            "Autechre": make_artist_enrichment(
                canonical_name="Autechre",
                personnel=[make_personnel_credit(name="Rob Brown")],
            ),
            "Stereolab": make_artist_enrichment(
                canonical_name="Stereolab",
                personnel=[make_personnel_credit(name="Rob Brown")],
            ),
        }

        edges = extract_shared_personnel(enrichments, max_artists=None)

        assert len(edges) == 1

    def test_max_artists_at_boundary_included(self):
        """Personnel on exactly max_artists should be included."""
        enrichments = {
            "Autechre": make_artist_enrichment(
                canonical_name="Autechre",
                personnel=[make_personnel_credit(name="Rob Brown")],
            ),
            "Stereolab": make_artist_enrichment(
                canonical_name="Stereolab",
                personnel=[make_personnel_credit(name="Rob Brown")],
            ),
        }

        edges = extract_shared_personnel(enrichments, max_artists=2)

        assert len(edges) == 1


class TestExtractSharedStyles:
    """Tests for extract_shared_styles."""

    def test_identical_styles_full_jaccard(self):
        enrichments = {
            "Autechre": make_artist_enrichment(
                canonical_name="Autechre",
                styles=["IDM", "Abstract"],
            ),
            "Stereolab": make_artist_enrichment(
                canonical_name="Stereolab",
                styles=["IDM", "Abstract"],
            ),
        }

        edges = extract_shared_styles(enrichments)

        assert len(edges) == 1
        assert edges[0].jaccard == 1.0
        assert edges[0].shared_tags == ["Abstract", "IDM"]

    def test_fifty_percent_overlap(self):
        enrichments = {
            "Autechre": make_artist_enrichment(
                canonical_name="Autechre",
                styles=["IDM", "Abstract"],
            ),
            "Stereolab": make_artist_enrichment(
                canonical_name="Stereolab",
                styles=["IDM", "Krautrock"],
            ),
        }

        edges = extract_shared_styles(enrichments, min_jaccard=0.0)

        assert len(edges) == 1
        # intersection = {"IDM"}, union = {"IDM", "Abstract", "Krautrock"} -> 1/3
        expected_jaccard = 1.0 / 3.0
        assert abs(edges[0].jaccard - expected_jaccard) < 1e-9
        assert edges[0].shared_tags == ["IDM"]

    def test_zero_overlap_no_edge(self):
        enrichments = {
            "Autechre": make_artist_enrichment(
                canonical_name="Autechre",
                styles=["IDM", "Abstract"],
            ),
            "Father John Misty": make_artist_enrichment(
                canonical_name="Father John Misty",
                styles=["Folk", "Indie Rock"],
            ),
        }

        edges = extract_shared_styles(enrichments, min_jaccard=0.0)

        assert len(edges) == 0

    def test_empty_styles_no_edge(self):
        enrichments = {
            "Autechre": make_artist_enrichment(
                canonical_name="Autechre",
                styles=[],
            ),
            "Stereolab": make_artist_enrichment(
                canonical_name="Stereolab",
                styles=["IDM"],
            ),
        }

        edges = extract_shared_styles(enrichments, min_jaccard=0.0)

        assert len(edges) == 0

    def test_min_jaccard_filter(self):
        enrichments = {
            "Autechre": make_artist_enrichment(
                canonical_name="Autechre",
                styles=["IDM", "Abstract", "Ambient", "Experimental"],
            ),
            "Stereolab": make_artist_enrichment(
                canonical_name="Stereolab",
                styles=["IDM", "Krautrock", "Post-Rock", "Space Rock"],
            ),
        }

        # intersection = {"IDM"}, union has 7 items -> jaccard = 1/7 ≈ 0.143
        edges_low = extract_shared_styles(enrichments, min_jaccard=0.1)
        assert len(edges_low) == 1

        edges_high = extract_shared_styles(enrichments, min_jaccard=0.5)
        assert len(edges_high) == 0

    def test_inverted_index_optimization_only_considers_pairs_with_shared_tags(self):
        """Artists with no overlapping tags should never be compared."""
        enrichments = {
            "Autechre": make_artist_enrichment(
                canonical_name="Autechre",
                styles=["IDM"],
            ),
            "Stereolab": make_artist_enrichment(
                canonical_name="Stereolab",
                styles=["IDM"],
            ),
            "Father John Misty": make_artist_enrichment(
                canonical_name="Father John Misty",
                styles=["Folk"],
            ),
        }

        edges = extract_shared_styles(enrichments, min_jaccard=0.0)

        # Only Autechre-Stereolab should appear, not any pair with Father John Misty
        assert len(edges) == 1
        assert edges[0].artist_a == "Autechre"
        assert edges[0].artist_b == "Stereolab"

    def test_max_artists_excludes_ubiquitous_style(self):
        """Styles shared by more than max_artists should be excluded from pairing."""
        # "Experimental" on all 4 artists — excluded with max_artists=3
        # "IDM" on only Autechre and Stereolab — should still produce an edge
        enrichments = {
            "Autechre": make_artist_enrichment(
                canonical_name="Autechre",
                styles=["Experimental", "IDM"],
            ),
            "Cat Power": make_artist_enrichment(
                canonical_name="Cat Power",
                styles=["Experimental", "Folk"],
            ),
            "Father John Misty": make_artist_enrichment(
                canonical_name="Father John Misty",
                styles=["Experimental", "Indie Rock"],
            ),
            "Stereolab": make_artist_enrichment(
                canonical_name="Stereolab",
                styles=["Experimental", "IDM"],
            ),
        }

        edges = extract_shared_styles(enrichments, min_jaccard=0.0, max_artists=3)

        # Only IDM-based edge should survive (Autechre-Stereolab)
        pairs = {(e.artist_a, e.artist_b) for e in edges}
        assert ("Autechre", "Stereolab") in pairs
        # No edges from the "Experimental" tag
        for e in edges:
            assert "Experimental" not in e.shared_tags

    def test_max_artists_at_boundary_included(self):
        """Styles on exactly max_artists should be included."""
        enrichments = {
            "Autechre": make_artist_enrichment(
                canonical_name="Autechre",
                styles=["IDM"],
            ),
            "Stereolab": make_artist_enrichment(
                canonical_name="Stereolab",
                styles=["IDM"],
            ),
        }

        edges = extract_shared_styles(enrichments, min_jaccard=0.0, max_artists=2)

        assert len(edges) == 1


class TestExtractLabelFamily:
    """Tests for extract_label_family."""

    def test_two_artists_on_same_label(self):
        enrichments = {
            "Autechre": make_artist_enrichment(
                canonical_name="Autechre",
                labels=[LabelInfo(name="Warp Records", label_id=100)],
            ),
            "Stereolab": make_artist_enrichment(
                canonical_name="Stereolab",
                labels=[LabelInfo(name="Warp Records", label_id=100)],
            ),
        }

        edges = extract_label_family(enrichments)

        assert len(edges) == 1
        assert edges[0].artist_a == "Autechre"
        assert edges[0].artist_b == "Stereolab"
        assert edges[0].shared_labels == ["Warp Records"]

    def test_mega_label_excluded(self):
        # Build a mega-label with more than max_label_artists artists
        enrichments = {}
        for i in range(501):
            name = f"Artist {i:03d}"
            enrichments[name] = make_artist_enrichment(
                canonical_name=name,
                labels=[LabelInfo(name="Not On Label", label_id=None)],
            )

        edges = extract_label_family(enrichments, max_label_artists=500)

        assert len(edges) == 0

    def test_multiple_shared_labels(self):
        enrichments = {
            "Autechre": make_artist_enrichment(
                canonical_name="Autechre",
                labels=[
                    LabelInfo(name="Warp Records", label_id=100),
                    LabelInfo(name="Skam", label_id=200),
                ],
            ),
            "Stereolab": make_artist_enrichment(
                canonical_name="Stereolab",
                labels=[
                    LabelInfo(name="Warp Records", label_id=100),
                    LabelInfo(name="Skam", label_id=200),
                ],
            ),
        }

        edges = extract_label_family(enrichments)

        assert len(edges) == 1
        assert sorted(edges[0].shared_labels) == ["Skam", "Warp Records"]

    def test_no_shared_labels(self):
        enrichments = {
            "Autechre": make_artist_enrichment(
                canonical_name="Autechre",
                labels=[LabelInfo(name="Warp Records", label_id=100)],
            ),
            "Father John Misty": make_artist_enrichment(
                canonical_name="Father John Misty",
                labels=[LabelInfo(name="Sub Pop", label_id=300)],
            ),
        }

        edges = extract_label_family(enrichments)

        assert len(edges) == 0

    def test_label_just_at_max_included(self):
        """A label with exactly max_label_artists should be included."""
        enrichments = {}
        for i in range(3):
            name = f"Artist {i}"
            enrichments[name] = make_artist_enrichment(
                canonical_name=name,
                labels=[LabelInfo(name="Small Label")],
            )

        edges = extract_label_family(enrichments, max_label_artists=3)

        # 3 artists -> 3 edges (A0-A1, A0-A2, A1-A2)
        assert len(edges) == 3


class TestExtractCompilationCoappearance:
    """Tests for extract_compilation_coappearance."""

    def test_two_artists_on_same_compilation(self):
        enrichments = {
            "Autechre": make_artist_enrichment(
                canonical_name="Autechre",
                compilation_appearances=[
                    CompilationAppearance(
                        release_id=5000,
                        release_title="Warp 20 (Recreated)",
                        other_artists=["Stereolab"],
                    )
                ],
            ),
            "Stereolab": make_artist_enrichment(
                canonical_name="Stereolab",
                compilation_appearances=[
                    CompilationAppearance(
                        release_id=5000,
                        release_title="Warp 20 (Recreated)",
                        other_artists=["Autechre"],
                    )
                ],
            ),
        }

        edges = extract_compilation_coappearance(enrichments)

        assert len(edges) == 1
        assert edges[0].artist_a == "Autechre"
        assert edges[0].artist_b == "Stereolab"
        assert edges[0].compilation_count == 1
        assert edges[0].compilation_titles == ["Warp 20 (Recreated)"]

    def test_three_artists_on_same_compilation(self):
        enrichments = {
            "Autechre": make_artist_enrichment(
                canonical_name="Autechre",
                compilation_appearances=[
                    CompilationAppearance(
                        release_id=5000,
                        release_title="Warp 20 (Recreated)",
                        other_artists=["Stereolab", "Cat Power"],
                    )
                ],
            ),
            "Cat Power": make_artist_enrichment(
                canonical_name="Cat Power",
                compilation_appearances=[
                    CompilationAppearance(
                        release_id=5000,
                        release_title="Warp 20 (Recreated)",
                        other_artists=["Autechre", "Stereolab"],
                    )
                ],
            ),
            "Stereolab": make_artist_enrichment(
                canonical_name="Stereolab",
                compilation_appearances=[
                    CompilationAppearance(
                        release_id=5000,
                        release_title="Warp 20 (Recreated)",
                        other_artists=["Autechre", "Cat Power"],
                    )
                ],
            ),
        }

        edges = extract_compilation_coappearance(enrichments)

        assert len(edges) == 3
        pairs = [(e.artist_a, e.artist_b) for e in edges]
        assert ("Autechre", "Cat Power") in pairs
        assert ("Autechre", "Stereolab") in pairs
        assert ("Cat Power", "Stereolab") in pairs

    def test_artist_alone_on_compilation_no_edges(self):
        enrichments = {
            "Autechre": make_artist_enrichment(
                canonical_name="Autechre",
                compilation_appearances=[
                    CompilationAppearance(
                        release_id=5000,
                        release_title="Warp 20 (Recreated)",
                        other_artists=["Unknown Artist"],
                    )
                ],
            ),
        }

        edges = extract_compilation_coappearance(enrichments)

        assert len(edges) == 0

    def test_no_compilation_appearances(self):
        enrichments = {
            "Autechre": make_artist_enrichment(
                canonical_name="Autechre",
                compilation_appearances=[],
            ),
            "Stereolab": make_artist_enrichment(
                canonical_name="Stereolab",
                compilation_appearances=[],
            ),
        }

        edges = extract_compilation_coappearance(enrichments)

        assert len(edges) == 0

    def test_two_shared_compilations(self):
        enrichments = {
            "Autechre": make_artist_enrichment(
                canonical_name="Autechre",
                compilation_appearances=[
                    CompilationAppearance(
                        release_id=5000,
                        release_title="Warp 20 (Recreated)",
                        other_artists=["Stereolab"],
                    ),
                    CompilationAppearance(
                        release_id=5001,
                        release_title="Artificial Intelligence",
                        other_artists=["Stereolab"],
                    ),
                ],
            ),
            "Stereolab": make_artist_enrichment(
                canonical_name="Stereolab",
                compilation_appearances=[
                    CompilationAppearance(
                        release_id=5000,
                        release_title="Warp 20 (Recreated)",
                        other_artists=["Autechre"],
                    ),
                    CompilationAppearance(
                        release_id=5001,
                        release_title="Artificial Intelligence",
                        other_artists=["Autechre"],
                    ),
                ],
            ),
        }

        edges = extract_compilation_coappearance(enrichments)

        assert len(edges) == 1
        assert edges[0].compilation_count == 2
        assert sorted(edges[0].compilation_titles) == [
            "Artificial Intelligence",
            "Warp 20 (Recreated)",
        ]


# ---------------------------------------------------------------------------
# Top-K-per-artist prune for shared_personnel and label_family
# ---------------------------------------------------------------------------
#
# These tables grow with the cross-product of artists who share any personnel
# or any label. On the production graph, popular artists accumulate 8K–10K
# neighbors in each table — heavy enough that cold-cache reads dominate the
# /graph/artists/{id}/neighbors?type=affinity latency. Mirrors the existing
# prune_acoustic_similarity contract: caller owns the transaction; rows are
# kept if either endpoint considers the other a top-K neighbor.


@pytest.fixture
def personnel_db(tmp_path: Path) -> sqlite3.Connection:
    """Fresh DB with the shared_personnel schema and indexes."""
    conn = sqlite3.connect(str(tmp_path / "personnel.db"))
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE shared_personnel (
            artist_a_id INTEGER NOT NULL,
            artist_b_id INTEGER NOT NULL,
            shared_count INTEGER NOT NULL,
            shared_names TEXT NOT NULL,
            PRIMARY KEY (artist_a_id, artist_b_id)
        );
        CREATE INDEX idx_shared_personnel_a ON shared_personnel(artist_a_id);
        CREATE INDEX idx_shared_personnel_b ON shared_personnel(artist_b_id);
        """
    )
    return conn


@pytest.fixture
def label_db(tmp_path: Path) -> sqlite3.Connection:
    """Fresh DB with the label_family schema and indexes."""
    conn = sqlite3.connect(str(tmp_path / "label.db"))
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE label_family (
            artist_a_id INTEGER NOT NULL,
            artist_b_id INTEGER NOT NULL,
            shared_labels TEXT NOT NULL,
            PRIMARY KEY (artist_a_id, artist_b_id)
        );
        CREATE INDEX idx_label_family_a ON label_family(artist_a_id);
        CREATE INDEX idx_label_family_b ON label_family(artist_b_id);
        """
    )
    return conn


def _personnel_edges(conn: sqlite3.Connection) -> set[tuple[int, int]]:
    return {
        (r["artist_a_id"], r["artist_b_id"])
        for r in conn.execute("SELECT artist_a_id, artist_b_id FROM shared_personnel")
    }


def _label_edges(conn: sqlite3.Connection) -> set[tuple[int, int]]:
    return {
        (r["artist_a_id"], r["artist_b_id"])
        for r in conn.execute("SELECT artist_a_id, artist_b_id FROM label_family")
    }


def _seed_personnel_complete(conn: sqlite3.Connection, n: int) -> None:
    """Insert the complete graph on artists 1..n with strictly decreasing
    ``shared_count`` so the order of (a,b) by weight is reverse-lex on a, then b.

    For n=5:
      (1,2)=10 (1,3)=9 (1,4)=8 (1,5)=7
      (2,3)=6 (2,4)=5 (2,5)=4
      (3,4)=3 (3,5)=2
      (4,5)=1
    """
    count = 10
    for a in range(1, n + 1):
        for b in range(a + 1, n + 1):
            conn.execute(
                "INSERT INTO shared_personnel VALUES (?, ?, ?, ?)",
                (a, b, count, json.dumps([f"person-{count}"])),
            )
            count -= 1
    conn.commit()


def _seed_label_complete(conn: sqlite3.Connection, n: int) -> None:
    """Insert the complete graph on artists 1..n with strictly decreasing
    label count (= JSON array length) so the order of (a,b) by weight is
    reverse-lex on a, then b.
    """
    count = 10
    for a in range(1, n + 1):
        for b in range(a + 1, n + 1):
            conn.execute(
                "INSERT INTO label_family VALUES (?, ?, ?)",
                (a, b, json.dumps([f"label-{i}" for i in range(count)])),
            )
            count -= 1
    conn.commit()


class TestPruneSharedPersonnel:
    """Top-K-per-artist (either-side) prune of shared_personnel, ranked by shared_count DESC."""

    def test_top_k_keeps_top_neighbors_per_artist(self, personnel_db: sqlite3.Connection) -> None:
        _seed_personnel_complete(personnel_db, 5)
        # Same topology as the acoustic case: each artist's top-2 → union of 7 edges,
        # the three (3,4)/(3,5)/(4,5) edges drop.
        before, after = prune_shared_personnel(personnel_db, top_k=2)
        assert before == 10
        assert after == 7
        assert _personnel_edges(personnel_db) == {
            (1, 2),
            (1, 3),
            (2, 3),
            (1, 4),
            (2, 4),
            (1, 5),
            (2, 5),
        }

    def test_either_side_semantics(self, personnel_db: sqlite3.Connection) -> None:
        """Edges in B's top-K but not A's top-K must still be kept."""
        edges = [(1, 2, 99), (1, 3, 98), (1, 4, 97), (1, 5, 96)]
        for a, b, c in edges:
            personnel_db.execute(
                "INSERT INTO shared_personnel VALUES (?, ?, ?, ?)",
                (a, b, c, json.dumps(["x"])),
            )
        personnel_db.commit()
        before, after = prune_shared_personnel(personnel_db, top_k=1)
        assert before == 4
        assert after == 4
        assert _personnel_edges(personnel_db) == {(1, 2), (1, 3), (1, 4), (1, 5)}

    def test_large_k_keeps_everything(self, personnel_db: sqlite3.Connection) -> None:
        _seed_personnel_complete(personnel_db, 4)
        before, after = prune_shared_personnel(personnel_db, top_k=100)
        assert before == 6
        assert after == 6

    def test_empty_table_no_error(self, personnel_db: sqlite3.Connection) -> None:
        before, after = prune_shared_personnel(personnel_db, top_k=10)
        assert before == 0
        assert after == 0

    def test_invalid_top_k_raises(self, personnel_db: sqlite3.Connection) -> None:
        _seed_personnel_complete(personnel_db, 3)
        with pytest.raises(ValueError):
            prune_shared_personnel(personnel_db, top_k=0)
        with pytest.raises(ValueError):
            prune_shared_personnel(personnel_db, top_k=-1)

    def test_preserves_canonical_order(self, personnel_db: sqlite3.Connection) -> None:
        _seed_personnel_complete(personnel_db, 6)
        prune_shared_personnel(personnel_db, top_k=3)
        bad = personnel_db.execute(
            "SELECT COUNT(*) FROM shared_personnel WHERE artist_a_id >= artist_b_id"
        ).fetchone()[0]
        assert bad == 0

    def test_does_not_commit_so_rollback_works(self, personnel_db: sqlite3.Connection) -> None:
        _seed_personnel_complete(personnel_db, 5)
        personnel_db.commit()
        before, after = prune_shared_personnel(personnel_db, top_k=2)
        assert before == 10
        assert after == 7
        personnel_db.rollback()
        restored = personnel_db.execute("SELECT COUNT(*) FROM shared_personnel").fetchone()[0]
        assert restored == 10

    def test_preserves_non_key_columns(self, personnel_db: sqlite3.Connection) -> None:
        """Surviving rows keep their shared_count and shared_names intact."""
        _seed_personnel_complete(personnel_db, 5)
        prune_shared_personnel(personnel_db, top_k=2)
        row = personnel_db.execute(
            "SELECT shared_count, shared_names FROM shared_personnel WHERE artist_a_id=1 AND artist_b_id=2"
        ).fetchone()
        # The (1,2) edge was seeded with the highest count (10).
        assert row["shared_count"] == 10
        assert json.loads(row["shared_names"]) == ["person-10"]


class TestPruneLabelFamily:
    """Top-K-per-artist (either-side) prune of label_family, ranked by label count DESC.

    label_family has no scalar weight column — rank by ``json_array_length(shared_labels)``.
    """

    def test_top_k_keeps_top_neighbors_per_artist(self, label_db: sqlite3.Connection) -> None:
        _seed_label_complete(label_db, 5)
        before, after = prune_label_family(label_db, top_k=2)
        assert before == 10
        assert after == 7
        assert _label_edges(label_db) == {
            (1, 2),
            (1, 3),
            (2, 3),
            (1, 4),
            (2, 4),
            (1, 5),
            (2, 5),
        }

    def test_either_side_semantics(self, label_db: sqlite3.Connection) -> None:
        for a, b, n_labels in [(1, 2, 9), (1, 3, 8), (1, 4, 7), (1, 5, 6)]:
            label_db.execute(
                "INSERT INTO label_family VALUES (?, ?, ?)",
                (a, b, json.dumps([f"l{i}" for i in range(n_labels)])),
            )
        label_db.commit()
        before, after = prune_label_family(label_db, top_k=1)
        assert before == 4
        assert after == 4
        assert _label_edges(label_db) == {(1, 2), (1, 3), (1, 4), (1, 5)}

    def test_large_k_keeps_everything(self, label_db: sqlite3.Connection) -> None:
        _seed_label_complete(label_db, 4)
        before, after = prune_label_family(label_db, top_k=100)
        assert before == 6
        assert after == 6

    def test_empty_table_no_error(self, label_db: sqlite3.Connection) -> None:
        before, after = prune_label_family(label_db, top_k=10)
        assert before == 0
        assert after == 0

    def test_invalid_top_k_raises(self, label_db: sqlite3.Connection) -> None:
        _seed_label_complete(label_db, 3)
        with pytest.raises(ValueError):
            prune_label_family(label_db, top_k=0)
        with pytest.raises(ValueError):
            prune_label_family(label_db, top_k=-1)

    def test_preserves_canonical_order(self, label_db: sqlite3.Connection) -> None:
        _seed_label_complete(label_db, 6)
        prune_label_family(label_db, top_k=3)
        bad = label_db.execute(
            "SELECT COUNT(*) FROM label_family WHERE artist_a_id >= artist_b_id"
        ).fetchone()[0]
        assert bad == 0

    def test_does_not_commit_so_rollback_works(self, label_db: sqlite3.Connection) -> None:
        _seed_label_complete(label_db, 5)
        label_db.commit()
        before, after = prune_label_family(label_db, top_k=2)
        assert before == 10
        assert after == 7
        label_db.rollback()
        restored = label_db.execute("SELECT COUNT(*) FROM label_family").fetchone()[0]
        assert restored == 10

    def test_preserves_shared_labels_payload(self, label_db: sqlite3.Connection) -> None:
        _seed_label_complete(label_db, 5)
        prune_label_family(label_db, top_k=2)
        row = label_db.execute(
            "SELECT shared_labels FROM label_family WHERE artist_a_id=1 AND artist_b_id=2"
        ).fetchone()
        # (1,2) was seeded with the most labels (10).
        assert json.loads(row["shared_labels"]) == [f"label-{i}" for i in range(10)]
