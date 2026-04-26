"""Integration test: --entity-source=lml pipeline mode.

Exercises the import_lml_identities() function against a real SQLite
entity store seeded with artists from the tubafrenzy fixture dump.

Uses a mock PG source (no real PostgreSQL required) that returns
WXYC example artist identity data matching what LML's entity.identity
table would contain.
"""

import os
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from semantic_index.models import FlowsheetEntry, LibraryCode, LibraryRelease

pytestmark = pytest.mark.integration

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
    return Path(_RELATIVE)


FIXTURE_PATH = _find_fixture()

# Mock LML entity.identity rows using artists known to be in the tubafrenzy fixture dump.
# The fixture dump's flowsheet entries reference these artists (resolved via catalog or raw).
LML_IDENTITY_ROWS = [
    {
        "library_name": "Aphex Twin",
        "discogs_artist_id": 45,
        "wikidata_qid": "Q1397",
        "musicbrainz_artist_id": "f22942a1-6f70-4f48-866e-238cb2308fbd",
        "spotify_artist_id": "6kBDZFXuLrZgHnvmPu9NsG",
        "apple_music_artist_id": "3024009",
        "bandcamp_id": None,
        "reconciliation_status": "reconciled",
    },
    {
        "library_name": "DJ Shadow",
        "discogs_artist_id": 314,
        "wikidata_qid": "Q213363",
        "musicbrainz_artist_id": "284c3e7a-5976-4484-829c-e5b6e7e3e5ae",
        "spotify_artist_id": "5EvFsr3xl7cR2gBQ5MTMLE",
        "apple_music_artist_id": "13810",
        "bandcamp_id": None,
        "reconciliation_status": "reconciled",
    },
    {
        "library_name": "Daft Punk",
        "discogs_artist_id": 1289,
        "wikidata_qid": "Q187814",
        "musicbrainz_artist_id": "056e4f3e-d505-4dad-8ec1-d04f521cbb56",
        "spotify_artist_id": "4tZwfgrHOc3mvqYlEYSvVi",
        "apple_music_artist_id": "5468295",
        "bandcamp_id": None,
        "reconciliation_status": "reconciled",
    },
    {
        "library_name": "Gang Starr",
        "discogs_artist_id": 17457,
        "wikidata_qid": None,
        "musicbrainz_artist_id": None,
        "spotify_artist_id": None,
        "apple_music_artist_id": None,
        "bandcamp_id": None,
        "reconciliation_status": "no_match",
    },
]


def _make_mock_pg(rows: list[dict]) -> MagicMock:
    """Create a mock PG source returning the given rows."""
    mock = MagicMock()
    mock.fetchall = MagicMock(return_value=rows)
    mock.close = MagicMock()
    return mock


def _parse_fixture_and_resolve(fixture_dump: str) -> list[str]:
    """Parse fixture dump and resolve artists, returning unique canonical names."""
    from semantic_index.artist_resolver import ArtistResolver
    from semantic_index.sql_parser import iter_table_rows, load_table_rows

    # Parse library tables
    release_rows = load_table_rows(fixture_dump, "LIBRARY_RELEASE")
    releases = [LibraryRelease(id=r[0], library_code_id=r[8]) for r in release_rows]

    code_rows = load_table_rows(fixture_dump, "LIBRARY_CODE")
    codes = [LibraryCode(id=r[0], genre_id=r[1], presentation_name=r[7]) for r in code_rows]

    resolver = ArtistResolver(releases=releases, codes=codes)

    # Stream flowsheet entries and resolve
    resolved_entries = []
    for row in iter_table_rows(fixture_dump, "FLOWSHEET_ENTRY_PROD"):
        entry_type_code = row[15]
        if not isinstance(entry_type_code, int) or entry_type_code >= 7:
            continue
        try:
            start_time_raw = row[10]
            request_flag_raw = row[18]
            entry = FlowsheetEntry(
                id=row[0],
                artist_name=row[1] or "",
                song_title=row[3] or "",
                release_title=row[4] or "",
                library_release_id=row[6] if isinstance(row[6], int) else 0,
                label_name=row[8] or "",
                show_id=row[12] if isinstance(row[12], int) else 0,
                sequence=row[13] if isinstance(row[13], int) else 0,
                entry_type_code=entry_type_code,
                request_flag=request_flag_raw if isinstance(request_flag_raw, int) else 0,
                start_time=start_time_raw if isinstance(start_time_raw, int) else None,
            )
        except Exception:
            continue
        resolved = resolver.resolve(entry)
        resolved_entries.append(resolved)

    return list(dict.fromkeys(e.canonical_name for e in resolved_entries))


@pytest.fixture
def fixture_dump():
    if not FIXTURE_PATH.exists():
        pytest.skip(f"Fixture dump not found at {FIXTURE_PATH}")
    return str(FIXTURE_PATH)


class TestLmlEntitySourcePipeline:
    """Test the --entity-source=lml code path against real entity store."""

    @pytest.fixture(autouse=True)
    def _set_up_entity_store(self, fixture_dump):
        """Parse fixture dump, resolve artists, seed entity store, run LML import."""
        from semantic_index.entity_store import EntityStore

        from semantic_index.lml_identity import import_lml_identities

        all_names = _parse_fixture_and_resolve(fixture_dump)

        tmpdir = tempfile.mkdtemp()
        db_path = os.path.join(tmpdir, "test_entity_store.db")

        store = EntityStore(db_path)
        store.initialize()
        store.bulk_upsert_artists(all_names)

        mock_pg = _make_mock_pg(LML_IDENTITY_ROWS)
        self._report = import_lml_identities(store, mock_pg)
        self._store = store
        self._all_names = all_names

    def test_matched_artists_have_discogs_id(self):
        """Artists matched in entity.identity have their discogs_artist_id populated."""
        # Check each LML identity row that was reconciled
        for row in LML_IDENTITY_ROWS:
            if row["reconciliation_status"] != "reconciled":
                continue
            artist = self._store.get_artist_by_name(row["library_name"])
            if artist is not None:
                assert artist["discogs_artist_id"] == row["discogs_artist_id"], (
                    f"{row['library_name']}: expected discogs_artist_id={row['discogs_artist_id']}, "
                    f"got {artist['discogs_artist_id']}"
                )

    def test_matched_artists_have_reconciliation_status(self):
        """Matched artists are marked with the correct reconciliation status."""
        for row in LML_IDENTITY_ROWS:
            artist = self._store.get_artist_by_name(row["library_name"])
            if artist is not None:
                assert artist["reconciliation_status"] == row["reconciliation_status"]

    def test_entity_created_with_qid_and_streaming_ids(self):
        """When a QID is present, an entity row is created with streaming IDs."""
        artist = self._store.get_artist_by_name("Aphex Twin")
        if artist is None:
            pytest.skip("Aphex Twin not in fixture dump")

        assert artist["entity_id"] is not None
        conn = self._store._conn
        conn.row_factory = sqlite3.Row
        entity = conn.execute(
            "SELECT * FROM entity WHERE id = ?", (artist["entity_id"],)
        ).fetchone()
        conn.row_factory = None
        assert entity is not None
        assert entity["wikidata_qid"] == "Q1397"
        assert entity["spotify_artist_id"] == "6kBDZFXuLrZgHnvmPu9NsG"
        assert entity["apple_music_artist_id"] == "3024009"

    def test_report_counts_are_consistent(self):
        """The report matched + unmatched equals the total number of local artists."""
        total_local = len(self._all_names)
        assert self._report.matched + self._report.unmatched == total_local

    def test_report_has_some_matches(self):
        """At least some artists from the fixture dump match the LML identity rows."""
        assert self._report.matched > 0

    def test_no_match_artists_remain_unreconciled(self):
        """Artists not found in entity.identity keep their default status."""
        lml_names = {r["library_name"] for r in LML_IDENTITY_ROWS}
        unmatched = [n for n in self._all_names if n not in lml_names]
        if not unmatched:
            pytest.skip("All fixture artists are in LML_IDENTITY_ROWS")
        artist = self._store.get_artist_by_name(unmatched[0])
        if artist is not None:
            assert artist["reconciliation_status"] == "unreconciled"

    def test_entity_store_artist_count_matches_resolved(self):
        """The entity store has one artist row per unique resolved name."""
        conn = self._store._conn
        count = conn.execute("SELECT COUNT(*) FROM artist").fetchone()[0]
        assert count == len(self._all_names)

    def test_musicbrainz_id_populated(self):
        """MusicBrainz IDs from entity.identity are written to artist rows."""
        artist = self._store.get_artist_by_name("Aphex Twin")
        if artist is None:
            pytest.skip("Aphex Twin not in fixture dump")
        assert artist["musicbrainz_artist_id"] == "f22942a1-6f70-4f48-866e-238cb2308fbd"

    def test_entities_created_for_qid_artists(self):
        """Entity rows are created for artists with Wikidata QIDs."""
        assert self._report.entities_created > 0


class TestLmlEntitySourceCLI:
    """Verify --entity-source CLI flag parsing."""

    def test_entity_source_lml_parses_without_dsn(self):
        """--entity-source=lml parses; DSN absence is enforced at pipeline run time."""
        from run_pipeline import parse_args

        args = parse_args(["dump.sql", "--entity-source", "lml"])
        assert args.entity_source == "lml"
        assert args.discogs_cache_dsn is None

    def test_entity_source_default_is_none(self):
        """Default entity source is None at parse time.

        ``_resolve_entity_source`` later promotes a missing value to ``local``
        in the safe case, or refuses to start when the operator passed the
        historically-ambiguous combo (--db-path + --discogs-cache-dsn).
        """
        from run_pipeline import parse_args

        args = parse_args(["dump.sql"])
        assert args.entity_source is None

    def test_entity_source_invalid_rejected(self):
        """Invalid entity source values are rejected by argparse."""
        from run_pipeline import parse_args

        with pytest.raises(SystemExit):
            parse_args(["dump.sql", "--entity-source", "invalid"])
