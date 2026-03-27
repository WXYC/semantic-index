"""Tests for graph construction and GEXF export."""

import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

from semantic_index.graph_export import build_graph, export_gexf
from semantic_index.models import ArtistStats, PmiEdge


class TestBuildGraph:
    def _make_stats(self):
        return {
            "Autechre": ArtistStats(canonical_name="Autechre", total_plays=50, genre="Electronic"),
            "Stereolab": ArtistStats(canonical_name="Stereolab", total_plays=30, genre="Rock"),
            "Cat Power": ArtistStats(canonical_name="Cat Power", total_plays=20, genre="Rock"),
        }

    def _make_edges(self):
        return [
            PmiEdge(source="Autechre", target="Stereolab", raw_count=5, pmi=3.0),
            PmiEdge(source="Stereolab", target="Cat Power", raw_count=3, pmi=1.5),
            PmiEdge(source="Autechre", target="Cat Power", raw_count=1, pmi=0.5),
        ]

    def test_graph_has_correct_node_count(self):
        graph = build_graph(self._make_edges(), self._make_stats(), min_count=1)
        assert graph.number_of_nodes() == 3

    def test_graph_has_correct_edge_count(self):
        graph = build_graph(self._make_edges(), self._make_stats(), min_count=1)
        assert graph.number_of_edges() == 3

    def test_min_count_filters_edges(self):
        graph = build_graph(self._make_edges(), self._make_stats(), min_count=3)
        assert graph.number_of_edges() == 2  # only edges with raw_count >= 3

    def test_node_attributes(self):
        graph = build_graph(self._make_edges(), self._make_stats(), min_count=1)
        node = graph.nodes["Autechre"]
        assert node["label"] == "Autechre"
        assert node["genre"] == "Electronic"
        assert node["total_plays"] == 50

    def test_edge_attributes(self):
        graph = build_graph(self._make_edges(), self._make_stats(), min_count=1)
        edge = graph.edges["Autechre", "Stereolab"]
        assert edge["weight"] == 3.0
        assert edge["raw_count"] == 5

    def test_nodes_without_stats_get_defaults(self):
        edges = [PmiEdge(source="Unknown", target="Also Unknown", raw_count=2, pmi=1.0)]
        graph = build_graph(edges, {}, min_count=1)
        node = graph.nodes["Unknown"]
        assert node["total_plays"] == 0
        assert node["genre"] == ""

    def test_min_count_removes_isolated_nodes(self):
        """Nodes that lose all edges due to min_count filtering are still removed."""
        edges = [PmiEdge(source="A", target="B", raw_count=1, pmi=0.5)]
        stats = {
            "A": ArtistStats(canonical_name="A", total_plays=1),
            "B": ArtistStats(canonical_name="B", total_plays=1),
        }
        graph = build_graph(edges, stats, min_count=2)
        assert graph.number_of_nodes() == 0
        assert graph.number_of_edges() == 0

    def test_negative_pmi_edges_excluded(self):
        """Negative PMI means artists co-occur less than chance — not a useful edge."""
        edges = [
            PmiEdge(source="A", target="B", raw_count=5, pmi=3.0),
            PmiEdge(source="A", target="C", raw_count=2, pmi=-1.5),
        ]
        stats = {
            "A": ArtistStats(canonical_name="A", total_plays=10),
            "B": ArtistStats(canonical_name="B", total_plays=5),
            "C": ArtistStats(canonical_name="C", total_plays=5),
        }
        graph = build_graph(edges, stats, min_count=1)
        assert graph.number_of_edges() == 1
        assert not graph.has_edge("A", "C")


class TestExportGexf:
    def test_writes_valid_xml(self):
        edges = [PmiEdge(source="Autechre", target="Stereolab", raw_count=5, pmi=3.0)]
        stats = {
            "Autechre": ArtistStats(canonical_name="Autechre", total_plays=50, genre="Electronic"),
            "Stereolab": ArtistStats(canonical_name="Stereolab", total_plays=30, genre="Rock"),
        }
        graph = build_graph(edges, stats, min_count=1)

        with tempfile.NamedTemporaryFile(suffix=".gexf", delete=False) as f:
            path = f.name

        export_gexf(graph, path)

        tree = ET.parse(path)
        root = tree.getroot()
        assert "gexf" in root.tag.lower()

    def test_file_created(self):
        edges = [PmiEdge(source="A", target="B", raw_count=2, pmi=1.0)]
        stats = {}
        graph = build_graph(edges, stats, min_count=1)

        with tempfile.NamedTemporaryFile(suffix=".gexf", delete=False) as f:
            path = f.name

        export_gexf(graph, path)
        assert Path(path).exists()
        assert Path(path).stat().st_size > 0
