"""Tests for sqlite_export integration with EntityStore.

When an EntityStore is provided, export_sqlite should:
- Skip artist table creation (artists already exist in the entity store)
- Use EntityStore.get_name_to_id_mapping() for ID resolution
- Use EntityStore.bulk_update_stats() for stats
- Use EntityStore.persist_artist_styles() for enrichment styles
- Still create edge tables and insert edges normally
"""

import sqlite3

import pytest

from semantic_index.entity_store import EntityStore
from semantic_index.models import (
    ArtistEnrichment,
    ArtistStats,
    CrossReferenceEdge,
    LabelInfo,
    PersonnelCredit,
    PmiEdge,
)
from semantic_index.sqlite_export import export_sqlite


@pytest.fixture()
def entity_store_db(tmp_path):
    """Create an EntityStore with pre-populated artists and return (store, db_path)."""
    db_path = str(tmp_path / "test.db")
    store = EntityStore(db_path)
    store.initialize()

    # Bulk upsert some artists — simulates pipeline step
    store.bulk_upsert_artists(["Autechre", "Stereolab", "Cat Power"])

    return store, db_path


class TestEntityStoreMode:
    def test_uses_existing_artist_ids(self, entity_store_db):
        """Artist IDs come from the entity store, not from fresh INSERTs."""
        store, db_path = entity_store_db

        expected_mapping = store.get_name_to_id_mapping()
        stats = {
            "Autechre": ArtistStats(canonical_name="Autechre", total_plays=50),
            "Stereolab": ArtistStats(canonical_name="Stereolab", total_plays=30),
        }
        edges = [PmiEdge(source="Autechre", target="Stereolab", raw_count=5, pmi=3.0)]

        export_sqlite(
            db_path,
            artist_stats=stats,
            pmi_edges=edges,
            xref_edges=[],
            min_count=1,
            entity_store=store,
        )

        # Verify the transition edge uses the entity store's IDs
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM dj_transition").fetchone()
        assert row is not None
        assert row["source_id"] == expected_mapping["Autechre"]
        assert row["target_id"] == expected_mapping["Stereolab"]
        conn.close()

    def test_updates_stats_on_existing_artists(self, entity_store_db):
        """Stats are applied to existing artist rows via entity_store."""
        store, db_path = entity_store_db

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

        export_sqlite(
            db_path,
            artist_stats=stats,
            pmi_edges=[],
            xref_edges=[],
            entity_store=store,
        )

        row = store.get_artist_by_name("Autechre")
        assert row is not None
        assert row["total_plays"] == 50
        assert row["genre"] == "Electronic"
        assert row["dj_count"] == 15

    def test_does_not_duplicate_artists(self, entity_store_db):
        """Entity store mode should not create duplicate artist rows."""
        store, db_path = entity_store_db

        stats = {
            "Autechre": ArtistStats(canonical_name="Autechre", total_plays=50),
        }

        export_sqlite(
            db_path,
            artist_stats=stats,
            pmi_edges=[],
            xref_edges=[],
            entity_store=store,
        )

        conn = sqlite3.connect(db_path)
        count = conn.execute(
            "SELECT COUNT(*) FROM artist WHERE canonical_name = 'Autechre'"
        ).fetchone()[0]
        assert count == 1
        conn.close()

    def test_creates_edge_tables(self, entity_store_db):
        """Edge tables are created even in entity store mode."""
        store, db_path = entity_store_db

        export_sqlite(
            db_path,
            artist_stats={},
            pmi_edges=[],
            xref_edges=[],
            entity_store=store,
        )

        conn = sqlite3.connect(db_path)
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        assert "dj_transition" in tables
        assert "cross_reference" in tables
        assert "shared_personnel" in tables
        assert "shared_style" in tables
        assert "label_family" in tables
        assert "compilation" in tables
        conn.close()

    def test_inserts_cross_reference_edges(self, entity_store_db):
        """Cross-reference edges use entity store IDs."""
        store, db_path = entity_store_db
        mapping = store.get_name_to_id_mapping()

        xrefs = [
            CrossReferenceEdge(
                artist_a="Autechre",
                artist_b="Stereolab",
                comment="See also",
                source="library_code",
            )
        ]

        export_sqlite(
            db_path,
            artist_stats={},
            pmi_edges=[],
            xref_edges=xrefs,
            entity_store=store,
        )

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM cross_reference").fetchone()
        assert row is not None
        assert row["artist_a_id"] == mapping["Autechre"]
        assert row["artist_b_id"] == mapping["Stereolab"]
        assert row["comment"] == "See also"
        conn.close()

    def test_xref_artists_not_in_store_are_upserted(self, entity_store_db):
        """Cross-ref artists not already in the entity store get upserted."""
        store, db_path = entity_store_db

        xrefs = [
            CrossReferenceEdge(
                artist_a="Autechre",
                artist_b="Father John Misty",
                comment="",
                source="library_code",
            )
        ]

        export_sqlite(
            db_path,
            artist_stats={},
            pmi_edges=[],
            xref_edges=xrefs,
            entity_store=store,
        )

        row = store.get_artist_by_name("Father John Misty")
        assert row is not None

    def test_enrichment_styles_persisted_via_store(self, entity_store_db):
        """When entity_store is provided, enrichment styles go through persist_artist_styles."""
        store, db_path = entity_store_db

        enrichments = {
            "Autechre": ArtistEnrichment(
                canonical_name="Autechre",
                discogs_artist_id=42,
                styles=["IDM", "Abstract"],
                personnel=[],
                labels=[],
                compilation_appearances=[],
            ),
        }

        export_sqlite(
            db_path,
            artist_stats={},
            pmi_edges=[],
            xref_edges=[],
            enrichments=enrichments,
            entity_store=store,
        )

        styles = store.get_artist_styles(store.get_name_to_id_mapping()["Autechre"])
        assert "IDM" in styles
        assert "Abstract" in styles

    def test_enrichment_personnel_inserted(self, entity_store_db):
        """Personnel enrichment data is still inserted into artist_personnel."""
        store, db_path = entity_store_db

        enrichments = {
            "Autechre": ArtistEnrichment(
                canonical_name="Autechre",
                discogs_artist_id=42,
                styles=[],
                personnel=[PersonnelCredit(name="Rob Brown", roles=["Written-By"])],
                labels=[],
                compilation_appearances=[],
            ),
        }

        export_sqlite(
            db_path,
            artist_stats={},
            pmi_edges=[],
            xref_edges=[],
            enrichments=enrichments,
            entity_store=store,
        )

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM artist_personnel").fetchone()
        assert row is not None
        assert row["personnel_name"] == "Rob Brown"
        conn.close()

    def test_enrichment_labels_inserted(self, entity_store_db):
        """Label enrichment data is still inserted into artist_label."""
        store, db_path = entity_store_db

        enrichments = {
            "Autechre": ArtistEnrichment(
                canonical_name="Autechre",
                discogs_artist_id=42,
                styles=[],
                personnel=[],
                labels=[LabelInfo(name="Warp Records", label_id=100)],
                compilation_appearances=[],
            ),
        }

        export_sqlite(
            db_path,
            artist_stats={},
            pmi_edges=[],
            xref_edges=[],
            enrichments=enrichments,
            entity_store=store,
        )

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM artist_label").fetchone()
        assert row is not None
        assert row["label_name"] == "Warp Records"
        conn.close()


class TestBackwardCompatibility:
    def test_without_entity_store_unchanged(self):
        """Without entity_store, behavior is identical to before."""
        import tempfile

        stats = {
            "Autechre": ArtistStats(canonical_name="Autechre", total_plays=50),
            "Stereolab": ArtistStats(canonical_name="Stereolab", total_plays=30),
        }
        edges = [PmiEdge(source="Autechre", target="Stereolab", raw_count=5, pmi=3.0)]
        path = tempfile.mktemp(suffix=".db")

        export_sqlite(
            path,
            artist_stats=stats,
            pmi_edges=edges,
            xref_edges=[],
            min_count=1,
        )

        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        count = conn.execute("SELECT COUNT(*) FROM artist").fetchone()[0]
        assert count == 2
        transition = conn.execute("SELECT COUNT(*) FROM dj_transition").fetchone()[0]
        assert transition == 1
        conn.close()
