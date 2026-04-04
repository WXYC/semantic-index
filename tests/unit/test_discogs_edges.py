"""Tests for Discogs-derived edge extraction functions."""

from semantic_index.discogs_edges import (
    extract_compilation_coappearance,
    extract_label_family,
    extract_shared_personnel,
    extract_shared_styles,
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
