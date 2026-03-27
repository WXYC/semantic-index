"""Integration test: run the full pipeline against the tubafrenzy fixture dump.

The fixture has minimal data (~3 flowsheet entries, 1000 library codes/releases),
so assertions focus on structural correctness rather than meaningful PMI values.
"""

import os
import tempfile
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

# Default: walk up from this file to find the WXYC parent dir containing sibling repos.
# Override with TUBAFRENZY_FIXTURE env var if the layout differs.
_RELATIVE = "tubafrenzy/scripts/dev/fixtures/wxycmusic-fixture.sql"


def _find_fixture() -> Path:
    override = os.environ.get("TUBAFRENZY_FIXTURE")
    if override:
        return Path(override)
    d = Path(__file__).resolve().parent
    while d != d.parent:
        candidate = d / _RELATIVE
        if candidate.exists():
            return candidate
        d = d.parent
    return Path(_RELATIVE)  # will fail gracefully via pytest.skip


FIXTURE_PATH = _find_fixture()


@pytest.fixture
def fixture_dump():
    if not FIXTURE_PATH.exists():
        pytest.skip(f"Fixture dump not found at {FIXTURE_PATH}")
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
