"""Audit tests: verify deleted modules are not imported by surviving code.

These tests pass BEFORE deletion (surviving modules don't reference the
dead code) and continue to pass after deletion.
"""

import importlib
import importlib.util

# Modules that must survive the cleanup
SURVIVING_MODULES = [
    "semantic_index.adjacency",
    "semantic_index.artist_resolver",
    "semantic_index.cross_reference",
    "semantic_index.discogs_client",
    "semantic_index.discogs_edges",
    "semantic_index.discogs_enrichment",
    "semantic_index.facet_export",
    "semantic_index.graph_export",
    "semantic_index.graph_metrics",
    "semantic_index.label_hierarchy",
    "semantic_index.lml_identity",
    "semantic_index.models",
    "semantic_index.musicbrainz_client",
    "semantic_index.node_attributes",
    "semantic_index.pmi",
    "semantic_index.sql_parser",
    "semantic_index.utils",
    "semantic_index.wikidata_client",
    "semantic_index.wikidata_influence",
]

# Methods that are being deleted from wikidata_client.py.
# Note: lookup_labels_by_discogs_ids is kept because label_hierarchy.py
# (surviving code) depends on it for P1902 label QID lookups.
DELETED_WIKIDATA_METHODS = [
    "lookup_by_discogs_ids",
    "lookup_streaming_ids",
    "search_by_name",
    "search_musician_by_name",
    "search_musicians_batch",
    "_filter_musicians",
]

DELETED_MUSICBRAINZ_METHODS = [
    "lookup_by_name",
    "batch_lookup",
]


def _read_module_source(mod_name: str) -> str:
    """Read the source of a module without importing it."""
    spec = importlib.util.find_spec(mod_name)
    if spec is None or spec.origin is None:
        return ""
    with open(spec.origin) as f:
        return f.read()


class TestEntityStoreNotImported:
    """Verify no surviving module imports entity_store."""

    def test_no_entity_store_import(self):
        for mod_name in SURVIVING_MODULES:
            if mod_name in ("semantic_index.label_hierarchy", "semantic_index.wikidata_influence"):
                # These use TYPE_CHECKING imports only — they'll be updated
                # to import from label_store instead
                continue
            source = _read_module_source(mod_name)
            # Check for direct imports (not TYPE_CHECKING blocks)
            lines = source.split("\n")
            in_type_checking = False
            for line in lines:
                stripped = line.strip()
                if stripped == "if TYPE_CHECKING:":
                    in_type_checking = True
                    continue
                if (
                    in_type_checking
                    and stripped
                    and not stripped.startswith(("#", "from ", "import "))
                ):
                    in_type_checking = False
                if not in_type_checking:
                    assert "from semantic_index.entity_store import" not in line, (
                        f"{mod_name} has a runtime import of entity_store"
                    )


class TestReconciliationNotImported:
    """Verify no surviving module imports reconciliation."""

    def test_no_reconciliation_import(self):
        for mod_name in SURVIVING_MODULES:
            source = _read_module_source(mod_name)
            assert "from semantic_index.reconciliation import" not in source, (
                f"{mod_name} still imports reconciliation"
            )
            assert "import semantic_index.reconciliation" not in source, (
                f"{mod_name} still imports reconciliation"
            )


class TestDeletedMethodsNotCalled:
    """Verify no surviving module calls deleted wikidata/musicbrainz methods."""

    def test_deleted_wikidata_methods_not_called(self):
        for mod_name in SURVIVING_MODULES:
            if mod_name == "semantic_index.wikidata_client":
                continue  # The methods exist there but will be deleted
            source = _read_module_source(mod_name)
            for method in DELETED_WIKIDATA_METHODS:
                assert f".{method}(" not in source, (
                    f"{mod_name} calls deleted wikidata method {method}"
                )

    def test_deleted_musicbrainz_methods_not_called(self):
        for mod_name in SURVIVING_MODULES:
            if mod_name == "semantic_index.musicbrainz_client":
                continue  # The methods exist there but will be deleted
            source = _read_module_source(mod_name)
            for method in DELETED_MUSICBRAINZ_METHODS:
                assert f".{method}(" not in source, (
                    f"{mod_name} calls deleted musicbrainz method {method}"
                )


class TestDeletedModulesCannotBeImported:
    """After deletion, verify the modules are truly gone."""

    def test_entity_store_module_deleted(self):
        spec = importlib.util.find_spec("semantic_index.entity_store")
        assert spec is None, "entity_store module should be deleted"

    def test_reconciliation_module_deleted(self):
        spec = importlib.util.find_spec("semantic_index.reconciliation")
        assert spec is None, "reconciliation module should be deleted"
