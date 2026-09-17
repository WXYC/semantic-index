"""Integration test: run the full pipeline against the vendored tubafrenzy fixture dump.

The fixture has minimal data (~3 flowsheet entries, 1000 library codes/releases),
so assertions focus on structural correctness rather than meaningful PMI values.
"""

import os
import tempfile
from pathlib import Path

import pytest

# The fixture is vendored into this repo (see tests/fixtures/README.md for
# provenance) so the suite has no cross-repo path dependency. Override with the
# TUBAFRENZY_FIXTURE env var to run against a different tubafrenzy dump.
_DEFAULT_FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "wxycmusic-fixture.sql"


def _find_fixture() -> Path:
    override = os.environ.get("TUBAFRENZY_FIXTURE")
    return Path(override) if override else _DEFAULT_FIXTURE


FIXTURE_PATH = _find_fixture()


@pytest.fixture
def fixture_dump():
    # Deliberately fail loud rather than skip: the fixture is committed here, so
    # a missing file means it was deleted or the override points somewhere wrong.
    assert FIXTURE_PATH.exists(), f"Fixture dump not found at {FIXTURE_PATH}"
    return str(FIXTURE_PATH)


class TestFullPipeline:
    def test_pipeline_runs_without_error(self, fixture_dump):
        """The full pipeline completes without exceptions on the fixture."""
        from run_pipeline import main

        with tempfile.TemporaryDirectory() as tmpdir:
            main([fixture_dump, "--output-dir", tmpdir, "--min-count", "1"])

    def test_gexf_output_is_parseable(self, fixture_dump):
        """The output GEXF file is valid XML loadable by NetworkX."""
        import networkx as nx

        from run_pipeline import main

        with tempfile.TemporaryDirectory() as tmpdir:
            main([fixture_dump, "--output-dir", tmpdir, "--min-count", "1"])
            gexf_path = Path(tmpdir) / "wxyc_artist_pmi.gexf"
            assert gexf_path.exists()
            graph = nx.read_gexf(str(gexf_path))
            assert isinstance(graph, nx.Graph)

    def test_sql_parser_reads_library_tables(self, fixture_dump):
        """The parser extracts rows from LIBRARY_CODE and LIBRARY_RELEASE."""
        from semantic_index.sql_parser import load_table_rows

        codes = load_table_rows(fixture_dump, "LIBRARY_CODE")
        releases = load_table_rows(fixture_dump, "LIBRARY_RELEASE")
        assert len(codes) == 1000
        assert len(releases) == 1000
