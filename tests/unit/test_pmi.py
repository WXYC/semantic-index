"""Tests for PMI computation."""

import math

import pytest

from semantic_index.pmi import compute_pmi, top_neighbors
from tests.conftest import make_adjacency_pair, make_resolved_entry


class TestComputePmi:
    def test_basic_pmi_computation(self):
        """Hand-computed PMI for a simple case.

        4 entries: A, B, A, B (in 2 shows of 2 each)
        2 pairs: (A, B), (A, B)
        P(A,B) = 2/2 = 1.0
        P(A) = 2/4 = 0.5
        P(B) = 2/4 = 0.5
        PMI = log2(1.0 / (0.5 * 0.5)) = log2(4) = 2.0
        """
        entries = [
            make_resolved_entry(canonical_name="A", show_id=1, sequence=1),
            make_resolved_entry(canonical_name="B", show_id=1, sequence=2),
            make_resolved_entry(canonical_name="A", show_id=2, sequence=1),
            make_resolved_entry(canonical_name="B", show_id=2, sequence=2),
        ]
        pairs = [
            make_adjacency_pair(source="A", target="B", show_id=1),
            make_adjacency_pair(source="A", target="B", show_id=2),
        ]
        edges = compute_pmi(pairs, entries)
        assert len(edges) == 1
        assert edges[0].source == "A"
        assert edges[0].target == "B"
        assert edges[0].raw_count == 2
        assert math.isclose(edges[0].pmi, 2.0)

    def test_positive_pmi_means_above_chance(self):
        """Artists appearing together more than random chance → positive PMI."""
        entries = [
            make_resolved_entry(canonical_name="A", show_id=1, sequence=1),
            make_resolved_entry(canonical_name="B", show_id=1, sequence=2),
            make_resolved_entry(canonical_name="A", show_id=2, sequence=1),
            make_resolved_entry(canonical_name="B", show_id=2, sequence=2),
            make_resolved_entry(canonical_name="C", show_id=3, sequence=1),
            make_resolved_entry(canonical_name="C", show_id=3, sequence=2, id=99),
        ]
        pairs = [
            make_adjacency_pair(source="A", target="B", show_id=1),
            make_adjacency_pair(source="A", target="B", show_id=2),
            make_adjacency_pair(source="C", target="C", show_id=3),
        ]
        edges = compute_pmi(pairs, entries)
        ab_edge = next(e for e in edges if e.source == "A" and e.target == "B")
        assert ab_edge.pmi > 0

    @pytest.mark.parametrize(
        "pairs_data,entries_data,expected_count",
        [
            # Single pair → 1 edge
            (
                [("A", "B", 1)],
                [("A", 1, 1), ("B", 1, 2)],
                1,
            ),
            # Two different pairs → 2 edges
            (
                [("A", "B", 1), ("C", "D", 2)],
                [("A", 1, 1), ("B", 1, 2), ("C", 2, 1), ("D", 2, 2)],
                2,
            ),
        ],
    )
    def test_edge_count(self, pairs_data, entries_data, expected_count):
        pairs = [make_adjacency_pair(source=s, target=t, show_id=sid) for s, t, sid in pairs_data]
        entries = [
            make_resolved_entry(canonical_name=name, show_id=sid, sequence=seq)
            for name, sid, seq in entries_data
        ]
        edges = compute_pmi(pairs, entries)
        assert len(edges) == expected_count

    def test_raw_count_aggregation(self):
        """Multiple occurrences of the same pair are counted."""
        entries = [
            make_resolved_entry(canonical_name="A", show_id=1, sequence=1),
            make_resolved_entry(canonical_name="B", show_id=1, sequence=2),
            make_resolved_entry(canonical_name="A", show_id=2, sequence=1),
            make_resolved_entry(canonical_name="B", show_id=2, sequence=2),
            make_resolved_entry(canonical_name="A", show_id=3, sequence=1),
            make_resolved_entry(canonical_name="B", show_id=3, sequence=2),
        ]
        pairs = [
            make_adjacency_pair(source="A", target="B", show_id=1),
            make_adjacency_pair(source="A", target="B", show_id=2),
            make_adjacency_pair(source="A", target="B", show_id=3),
        ]
        edges = compute_pmi(pairs, entries)
        assert edges[0].raw_count == 3

    def test_directional_pairs(self):
        """(A,B) and (B,A) are counted separately."""
        entries = [
            make_resolved_entry(canonical_name="A", show_id=1, sequence=1),
            make_resolved_entry(canonical_name="B", show_id=1, sequence=2),
            make_resolved_entry(canonical_name="B", show_id=2, sequence=1),
            make_resolved_entry(canonical_name="A", show_id=2, sequence=2),
        ]
        pairs = [
            make_adjacency_pair(source="A", target="B", show_id=1),
            make_adjacency_pair(source="B", target="A", show_id=2),
        ]
        edges = compute_pmi(pairs, entries)
        assert len(edges) == 2

    def test_empty_pairs(self):
        edges = compute_pmi([], [])
        assert edges == []


class TestTopNeighbors:
    def _make_edges(self):
        """Create a small set of edges for testing."""
        from semantic_index.models import PmiEdge

        return [
            PmiEdge(source="A", target="B", raw_count=5, pmi=3.0),
            PmiEdge(source="A", target="C", raw_count=3, pmi=1.5),
            PmiEdge(source="D", target="A", raw_count=2, pmi=2.0),
            PmiEdge(source="B", target="C", raw_count=1, pmi=0.5),
        ]

    def test_returns_neighbors_sorted_by_pmi(self):
        edges = self._make_edges()
        result = top_neighbors(edges, "A")
        assert len(result) == 3
        assert result[0].pmi == 3.0  # A→B
        assert result[1].pmi == 2.0  # D→A
        assert result[2].pmi == 1.5  # A→C

    def test_considers_both_directions(self):
        edges = self._make_edges()
        result = top_neighbors(edges, "A")
        # D→A should appear (A as target)
        targets = {(e.source, e.target) for e in result}
        assert ("D", "A") in targets

    def test_limits_to_n(self):
        edges = self._make_edges()
        result = top_neighbors(edges, "A", n=2)
        assert len(result) == 2

    def test_unknown_artist_returns_empty(self):
        edges = self._make_edges()
        result = top_neighbors(edges, "UNKNOWN")
        assert result == []
