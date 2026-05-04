"""WX-1.2.5 detector: catches future PipelineDB SQLite write-path regressions
that would silently corrupt non-ASCII canonical artist names."""

from __future__ import annotations

import pytest

from semantic_index.pipeline_db import PipelineDB
from tests.charset_torture import CharsetTortureEntry, entry_id, iter_entries

CORPUS_ENTRIES = list(iter_entries())


@pytest.fixture
def pipeline_db(tmp_path) -> PipelineDB:
    db = PipelineDB(str(tmp_path / "graph.sqlite"))
    db.initialize()
    return db


@pytest.mark.parametrize("entry", CORPUS_ENTRIES, ids=entry_id)
def test_pipeline_db_artist_name_roundtrip(
    pipeline_db: PipelineDB, entry: CharsetTortureEntry
) -> None:
    """upsert_artist + SELECT must preserve canonical_name byte-for-byte."""
    artist_id = pipeline_db.upsert_artist(canonical_name=entry["input"])
    row = pipeline_db._conn.execute(
        "SELECT canonical_name FROM artist WHERE id = ?", (artist_id,)
    ).fetchone()
    assert row is not None, f"{entry['category']}: row not found after upsert"
    assert row[0] == entry["input"], (
        f"{entry['category']}: round-trip lost bytes ({entry['notes']})"
    )
