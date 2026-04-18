"""Tests for SQLite graph database export."""

import sqlite3
import tempfile
from pathlib import Path

from semantic_index.models import (
    ArtistStats,
    CompilationEdge,
    CrossReferenceEdge,
    LabelFamilyEdge,
    PmiEdge,
    SharedPersonnelEdge,
    SharedStyleEdge,
    WikidataInfluenceEdge,
)
from semantic_index.sqlite_export import export_sqlite


def _export_and_connect(**kwargs) -> tuple[sqlite3.Connection, str]:
    """Export to a temp SQLite file and return an open connection."""
    path = tempfile.mktemp(suffix=".db")
    export_sqlite(path, **kwargs)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn, path


class TestSchemaCreation:
    def test_tables_exist(self):
        conn, _ = _export_and_connect(artist_stats={}, pmi_edges=[], xref_edges=[])
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        assert "artist" in tables
        assert "dj_transition" in tables
        assert "cross_reference" in tables
        assert "artist_style" in tables
        assert "artist_personnel" in tables
        assert "artist_label" in tables
        assert "shared_personnel" in tables
        assert "shared_style" in tables
        assert "label_family" in tables
        assert "compilation" in tables
        assert "wikidata_influence" in tables

    def test_indexes_exist(self):
        conn, _ = _export_and_connect(artist_stats={}, pmi_edges=[], xref_edges=[])
        indexes = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()
        }
        assert "idx_transition_source" in indexes
        assert "idx_transition_target" in indexes
        assert "idx_xref_a" in indexes
        assert "idx_xref_b" in indexes
        assert "idx_wikidata_influence_source" in indexes
        assert "idx_wikidata_influence_target" in indexes


class TestArtistInsertion:
    def test_artists_inserted_with_all_attributes(self):
        stats = {
            "Autechre": ArtistStats(
                canonical_name="Autechre",
                total_plays=50,
                genre="Electronic",
                active_first_year=2004,
                active_last_year=2025,
                dj_count=15,
                request_ratio=0.1,
                show_count=40,
            ),
        }
        conn, _ = _export_and_connect(artist_stats=stats, pmi_edges=[], xref_edges=[])
        row = conn.execute("SELECT * FROM artist WHERE canonical_name = 'Autechre'").fetchone()
        assert row is not None
        assert row["total_plays"] == 50
        assert row["genre"] == "Electronic"
        assert row["active_first_year"] == 2004
        assert row["active_last_year"] == 2025
        assert row["dj_count"] == 15
        assert abs(row["request_ratio"] - 0.1) < 1e-6
        assert row["show_count"] == 40

    def test_multiple_artists(self):
        stats = {
            "Autechre": ArtistStats(canonical_name="Autechre", total_plays=50),
            "Stereolab": ArtistStats(canonical_name="Stereolab", total_plays=30),
        }
        conn, _ = _export_and_connect(artist_stats=stats, pmi_edges=[], xref_edges=[])
        count = conn.execute("SELECT COUNT(*) FROM artist").fetchone()[0]
        assert count == 2

    def test_null_genre(self):
        stats = {"A": ArtistStats(canonical_name="A", total_plays=1, genre=None)}
        conn, _ = _export_and_connect(artist_stats=stats, pmi_edges=[], xref_edges=[])
        row = conn.execute("SELECT genre FROM artist WHERE canonical_name = 'A'").fetchone()
        assert row["genre"] is None


class TestPmiEdgeInsertion:
    def test_edges_inserted(self):
        stats = {
            "A": ArtistStats(canonical_name="A", total_plays=10),
            "B": ArtistStats(canonical_name="B", total_plays=5),
        }
        edges = [PmiEdge(source="A", target="B", raw_count=5, pmi=3.0)]
        conn, _ = _export_and_connect(
            artist_stats=stats, pmi_edges=edges, xref_edges=[], min_count=1
        )
        row = conn.execute("SELECT * FROM dj_transition").fetchone()
        assert row is not None
        assert row["raw_count"] == 5
        assert abs(row["pmi"] - 3.0) < 1e-6

    def test_min_count_filters_edges(self):
        stats = {
            "A": ArtistStats(canonical_name="A", total_plays=10),
            "B": ArtistStats(canonical_name="B", total_plays=5),
        }
        edges = [PmiEdge(source="A", target="B", raw_count=1, pmi=0.5)]
        conn, _ = _export_and_connect(
            artist_stats=stats, pmi_edges=edges, xref_edges=[], min_count=2
        )
        count = conn.execute("SELECT COUNT(*) FROM dj_transition").fetchone()[0]
        assert count == 0

    def test_negative_pmi_filtered(self):
        stats = {
            "A": ArtistStats(canonical_name="A", total_plays=10),
            "B": ArtistStats(canonical_name="B", total_plays=5),
        }
        edges = [PmiEdge(source="A", target="B", raw_count=5, pmi=-1.0)]
        conn, _ = _export_and_connect(
            artist_stats=stats, pmi_edges=edges, xref_edges=[], min_count=1
        )
        count = conn.execute("SELECT COUNT(*) FROM dj_transition").fetchone()[0]
        assert count == 0

    def test_query_neighbors_by_name(self):
        stats = {
            "A": ArtistStats(canonical_name="A", total_plays=10),
            "B": ArtistStats(canonical_name="B", total_plays=5),
            "C": ArtistStats(canonical_name="C", total_plays=3),
        }
        edges = [
            PmiEdge(source="A", target="B", raw_count=5, pmi=3.0),
            PmiEdge(source="A", target="C", raw_count=2, pmi=1.0),
        ]
        conn, _ = _export_and_connect(
            artist_stats=stats, pmi_edges=edges, xref_edges=[], min_count=1
        )
        rows = conn.execute(
            """
            SELECT a2.canonical_name, dt.pmi
            FROM dj_transition dt
            JOIN artist a1 ON dt.source_id = a1.id
            JOIN artist a2 ON dt.target_id = a2.id
            WHERE a1.canonical_name = 'A'
            ORDER BY dt.pmi DESC
            """,
        ).fetchall()
        assert len(rows) == 2
        assert rows[0]["canonical_name"] == "B"
        assert rows[1]["canonical_name"] == "C"


class TestCrossReferenceInsertion:
    def test_xref_edges_inserted(self):
        stats = {
            "A": ArtistStats(canonical_name="A", total_plays=10),
            "B": ArtistStats(canonical_name="B", total_plays=5),
        }
        xrefs = [
            CrossReferenceEdge(
                artist_a="A", artist_b="B", comment="See also", source="library_code"
            )
        ]
        conn, _ = _export_and_connect(artist_stats=stats, pmi_edges=[], xref_edges=xrefs)
        row = conn.execute("SELECT * FROM cross_reference").fetchone()
        assert row is not None
        assert row["comment"] == "See also"
        assert row["source"] == "library_code"

    def test_catalog_only_artists_created(self):
        """Cross-ref edges may reference artists not in flowsheet data."""
        stats = {"A": ArtistStats(canonical_name="A", total_plays=10)}
        xrefs = [
            CrossReferenceEdge(
                artist_a="A", artist_b="CatalogOnly", comment="", source="library_code"
            )
        ]
        conn, _ = _export_and_connect(artist_stats=stats, pmi_edges=[], xref_edges=xrefs)
        row = conn.execute("SELECT * FROM artist WHERE canonical_name = 'CatalogOnly'").fetchone()
        assert row is not None
        assert row["total_plays"] == 0

    def test_xref_count(self):
        stats = {
            "A": ArtistStats(canonical_name="A", total_plays=10),
            "B": ArtistStats(canonical_name="B", total_plays=5),
            "C": ArtistStats(canonical_name="C", total_plays=3),
        }
        xrefs = [
            CrossReferenceEdge(artist_a="A", artist_b="B", comment="", source="library_code"),
            CrossReferenceEdge(artist_a="B", artist_b="C", comment="", source="release"),
        ]
        conn, _ = _export_and_connect(artist_stats=stats, pmi_edges=[], xref_edges=xrefs)
        count = conn.execute("SELECT COUNT(*) FROM cross_reference").fetchone()[0]
        assert count == 2


class TestWikidataInfluenceInsertion:
    def test_influence_edges_inserted(self):
        stats = {
            "Autechre": ArtistStats(canonical_name="Autechre", total_plays=50),
            "Stereolab": ArtistStats(canonical_name="Stereolab", total_plays=30),
        }
        influence_edges = [
            WikidataInfluenceEdge(
                source_artist="Autechre",
                target_artist="Stereolab",
                source_qid="Q2774",
                target_qid="Q650826",
            ),
        ]
        conn, _ = _export_and_connect(
            artist_stats=stats,
            pmi_edges=[],
            xref_edges=[],
            wikidata_influence_edges=influence_edges,
        )
        row = conn.execute("SELECT * FROM wikidata_influence").fetchone()
        assert row is not None
        assert row["source_qid"] == "Q2774"
        assert row["target_qid"] == "Q650826"

    def test_influence_edges_reference_correct_artists(self):
        stats = {
            "Autechre": ArtistStats(canonical_name="Autechre", total_plays=50),
            "Stereolab": ArtistStats(canonical_name="Stereolab", total_plays=30),
            "Cat Power": ArtistStats(canonical_name="Cat Power", total_plays=20),
        }
        influence_edges = [
            WikidataInfluenceEdge(
                source_artist="Autechre",
                target_artist="Stereolab",
                source_qid="Q2774",
                target_qid="Q650826",
            ),
            WikidataInfluenceEdge(
                source_artist="Autechre",
                target_artist="Cat Power",
                source_qid="Q2774",
                target_qid="Q218981",
            ),
        ]
        conn, _ = _export_and_connect(
            artist_stats=stats,
            pmi_edges=[],
            xref_edges=[],
            wikidata_influence_edges=influence_edges,
        )
        rows = conn.execute(
            "SELECT a_src.canonical_name AS source_name, a_tgt.canonical_name AS target_name "
            "FROM wikidata_influence wi "
            "JOIN artist a_src ON wi.source_id = a_src.id "
            "JOIN artist a_tgt ON wi.target_id = a_tgt.id "
            "ORDER BY target_name"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0]["source_name"] == "Autechre"
        assert rows[0]["target_name"] == "Cat Power"
        assert rows[1]["target_name"] == "Stereolab"

    def test_influence_edges_empty_by_default(self):
        stats = {"Autechre": ArtistStats(canonical_name="Autechre", total_plays=50)}
        conn, _ = _export_and_connect(artist_stats=stats, pmi_edges=[], xref_edges=[])
        count = conn.execute("SELECT COUNT(*) FROM wikidata_influence").fetchone()[0]
        assert count == 0

    def test_influence_edge_unknown_artist_skipped(self):
        stats = {
            "Autechre": ArtistStats(canonical_name="Autechre", total_plays=50),
        }
        influence_edges = [
            WikidataInfluenceEdge(
                source_artist="Autechre",
                target_artist="Unknown",
                source_qid="Q2774",
                target_qid="Q999",
            ),
        ]
        conn, _ = _export_and_connect(
            artist_stats=stats,
            pmi_edges=[],
            xref_edges=[],
            wikidata_influence_edges=influence_edges,
        )
        count = conn.execute("SELECT COUNT(*) FROM wikidata_influence").fetchone()[0]
        assert count == 0


class TestDiscogsEdgeCaseInsensitive:
    """Discogs summary tables return lowercase names but artist table may have mixed case."""

    def test_shared_personnel_resolves_lowercase_names(self):
        stats = {
            "Yo La Tengo": ArtistStats(canonical_name="Yo La Tengo", total_plays=100),
            "Stereolab": ArtistStats(canonical_name="Stereolab", total_plays=80),
        }
        # Edge names are lowercase (as returned by Discogs summary tables)
        sp_edges = [
            SharedPersonnelEdge(
                artist_a="stereolab",
                artist_b="yo la tengo",
                shared_count=3,
                shared_names=["John McEntire", "Jim O'Rourke", "Roger Moutenot"],
            )
        ]
        conn, _ = _export_and_connect(
            artist_stats=stats,
            pmi_edges=[],
            xref_edges=[],
            shared_personnel_edges=sp_edges,
        )
        count = conn.execute("SELECT COUNT(*) FROM shared_personnel").fetchone()[0]
        assert count == 1
        row = conn.execute(
            "SELECT a.canonical_name, b.canonical_name "
            "FROM shared_personnel sp "
            "JOIN artist a ON sp.artist_a_id = a.id "
            "JOIN artist b ON sp.artist_b_id = b.id"
        ).fetchone()
        assert set(row) == {"Yo La Tengo", "Stereolab"}

    def test_shared_style_resolves_lowercase_names(self):
        stats = {
            "Beach House": ArtistStats(canonical_name="Beach House", total_plays=50),
            "Cocteau Twins": ArtistStats(canonical_name="Cocteau Twins", total_plays=40),
        }
        ss_edges = [
            SharedStyleEdge(
                artist_a="beach house",
                artist_b="cocteau twins",
                jaccard=0.6,
                shared_tags=["Dream Pop", "Shoegaze"],
            )
        ]
        conn, _ = _export_and_connect(
            artist_stats=stats,
            pmi_edges=[],
            xref_edges=[],
            shared_style_edges=ss_edges,
        )
        count = conn.execute("SELECT COUNT(*) FROM shared_style").fetchone()[0]
        assert count == 1

    def test_label_family_resolves_lowercase_names(self):
        stats = {
            "Cat Power": ArtistStats(canonical_name="Cat Power", total_plays=30),
            "Jessica Pratt": ArtistStats(canonical_name="Jessica Pratt", total_plays=20),
        }
        lf_edges = [
            LabelFamilyEdge(
                artist_a="cat power",
                artist_b="jessica pratt",
                shared_labels=["Matador Records"],
            )
        ]
        conn, _ = _export_and_connect(
            artist_stats=stats,
            pmi_edges=[],
            xref_edges=[],
            label_family_edges=lf_edges,
        )
        count = conn.execute("SELECT COUNT(*) FROM label_family").fetchone()[0]
        assert count == 1

    def test_compilation_resolves_lowercase_names(self):
        stats = {
            "Autechre": ArtistStats(canonical_name="Autechre", total_plays=50),
            "Aphex Twin": ArtistStats(canonical_name="Aphex Twin", total_plays=45),
        }
        comp_edges = [
            CompilationEdge(
                artist_a="aphex twin",
                artist_b="autechre",
                compilation_count=2,
                compilation_titles=["Artificial Intelligence", "We Are Reasonable People"],
            )
        ]
        conn, _ = _export_and_connect(
            artist_stats=stats,
            pmi_edges=[],
            xref_edges=[],
            compilation_edges=comp_edges,
        )
        count = conn.execute("SELECT COUNT(*) FROM compilation").fetchone()[0]
        assert count == 1


class TestRoundtrip:
    def test_file_created_and_nonempty(self):
        stats = {"A": ArtistStats(canonical_name="A", total_plays=1)}
        path = tempfile.mktemp(suffix=".db")
        export_sqlite(path, artist_stats=stats, pmi_edges=[], xref_edges=[])
        assert Path(path).exists()
        assert Path(path).stat().st_size > 0
