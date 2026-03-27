"""Tests for adjacency pair extraction."""

from semantic_index.adjacency import extract_adjacency_pairs
from tests.conftest import make_resolved_entry


class TestExtractAdjacencyPairs:
    def test_two_entries_in_same_show(self):
        entries = [
            make_resolved_entry(canonical_name="Autechre", show_id=1, sequence=1),
            make_resolved_entry(canonical_name="Stereolab", show_id=1, sequence=2),
        ]
        pairs = extract_adjacency_pairs(entries)
        assert len(pairs) == 1
        assert pairs[0].source == "Autechre"
        assert pairs[0].target == "Stereolab"
        assert pairs[0].show_id == 1

    def test_three_entries_produce_two_pairs(self):
        entries = [
            make_resolved_entry(canonical_name="Autechre", show_id=1, sequence=1),
            make_resolved_entry(canonical_name="Stereolab", show_id=1, sequence=2),
            make_resolved_entry(canonical_name="Cat Power", show_id=1, sequence=3),
        ]
        pairs = extract_adjacency_pairs(entries)
        assert len(pairs) == 2
        assert pairs[0].source == "Autechre"
        assert pairs[0].target == "Stereolab"
        assert pairs[1].source == "Stereolab"
        assert pairs[1].target == "Cat Power"

    def test_different_shows_produce_no_cross_show_pairs(self):
        entries = [
            make_resolved_entry(canonical_name="Autechre", show_id=1, sequence=1),
            make_resolved_entry(canonical_name="Stereolab", show_id=2, sequence=1),
        ]
        pairs = extract_adjacency_pairs(entries)
        assert len(pairs) == 0

    def test_sorts_by_sequence_within_show(self):
        entries = [
            make_resolved_entry(canonical_name="Stereolab", show_id=1, sequence=3),
            make_resolved_entry(canonical_name="Autechre", show_id=1, sequence=1),
            make_resolved_entry(canonical_name="Cat Power", show_id=1, sequence=2),
        ]
        pairs = extract_adjacency_pairs(entries)
        assert pairs[0].source == "Autechre"
        assert pairs[0].target == "Cat Power"
        assert pairs[1].source == "Cat Power"
        assert pairs[1].target == "Stereolab"

    def test_self_loop_preserved(self):
        entries = [
            make_resolved_entry(canonical_name="Autechre", show_id=1, sequence=1),
            make_resolved_entry(canonical_name="Autechre", show_id=1, sequence=2),
        ]
        pairs = extract_adjacency_pairs(entries)
        assert len(pairs) == 1
        assert pairs[0].source == "Autechre"
        assert pairs[0].target == "Autechre"

    def test_single_entry_produces_no_pairs(self):
        entries = [
            make_resolved_entry(canonical_name="Autechre", show_id=1, sequence=1),
        ]
        pairs = extract_adjacency_pairs(entries)
        assert len(pairs) == 0

    def test_empty_input(self):
        pairs = extract_adjacency_pairs([])
        assert pairs == []

    def test_multiple_shows(self):
        entries = [
            make_resolved_entry(canonical_name="Autechre", show_id=1, sequence=1),
            make_resolved_entry(canonical_name="Stereolab", show_id=1, sequence=2),
            make_resolved_entry(canonical_name="Cat Power", show_id=2, sequence=1),
            make_resolved_entry(canonical_name="Jessica Pratt", show_id=2, sequence=2),
        ]
        pairs = extract_adjacency_pairs(entries)
        assert len(pairs) == 2
        assert pairs[0].source == "Autechre"
        assert pairs[0].target == "Stereolab"
        assert pairs[1].source == "Cat Power"
        assert pairs[1].target == "Jessica Pratt"
