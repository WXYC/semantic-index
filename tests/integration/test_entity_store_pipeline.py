"""Integration test: entity store pipeline mode.

Tests the entity store pipeline: parse fixture dump -> resolve artists ->
seed entity store -> verify schema and artist management.

Uses the tubafrenzy fixture dump for realistic data.
"""

import os
import tempfile
from pathlib import Path

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


def _parse_fixture_and_resolve(fixture_dump: str) -> list[str]:
    """Parse fixture dump and resolve artists, returning unique canonical names."""
    from semantic_index.artist_resolver import ArtistResolver
    from semantic_index.sql_parser import iter_table_rows, load_table_rows

    release_rows = load_table_rows(fixture_dump, "LIBRARY_RELEASE")
    releases = [LibraryRelease(id=r[0], library_code_id=r[8]) for r in release_rows]

    code_rows = load_table_rows(fixture_dump, "LIBRARY_CODE")
    codes = [LibraryCode(id=r[0], genre_id=r[1], presentation_name=r[7]) for r in code_rows]

    resolver = ArtistResolver(releases=releases, codes=codes)

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


class TestEntityStorePipeline:
    """Test entity store creation and artist management from a fixture dump."""

    @pytest.fixture(autouse=True)
    def _set_up(self, fixture_dump):
        """Parse the fixture dump, resolve artists, and seed entity store."""
        from semantic_index.entity_store import EntityStore

        all_names = _parse_fixture_and_resolve(fixture_dump)

        tmpdir = tempfile.mkdtemp()
        db_path = os.path.join(tmpdir, "entity_store_test.db")
        store = EntityStore(db_path)
        store.initialize()
        store.bulk_upsert_artists(all_names)
        self._store = store
        self._all_names = all_names
        self._db_path = db_path

    def test_artist_table_has_entity_store_columns(self):
        """The artist table has all entity store columns after initialization."""
        conn = self._store._conn
        columns = {row[1] for row in conn.execute("PRAGMA table_info(artist)")}
        expected = {
            "id",
            "canonical_name",
            "genre",
            "total_plays",
            "active_first_year",
            "active_last_year",
            "dj_count",
            "request_ratio",
            "show_count",
            "discogs_artist_id",
            "entity_id",
            "musicbrainz_artist_id",
            "wxyc_library_code_id",
            "reconciliation_status",
            "created_at",
            "updated_at",
        }
        assert expected.issubset(columns), f"Missing columns: {expected - columns}"

    def test_entity_table_exists(self):
        """The entity table is created by initialize()."""
        conn = self._store._conn
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='entity'"
        ).fetchone()
        assert row is not None

    def test_reconciliation_log_table_exists(self):
        """The reconciliation_log table is created by initialize()."""
        conn = self._store._conn
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='reconciliation_log'"
        ).fetchone()
        assert row is not None

    def test_all_resolved_artists_are_stored(self):
        """Every unique resolved artist name has a row in the artist table."""
        conn = self._store._conn
        count = conn.execute("SELECT COUNT(*) FROM artist").fetchone()[0]
        assert count == len(self._all_names)

    def test_artists_default_to_unreconciled(self):
        """Freshly upserted artists have reconciliation_status='unreconciled'."""
        artist = self._store.get_artist_by_name(self._all_names[0])
        assert artist is not None
        assert artist["reconciliation_status"] == "unreconciled"

    def test_upsert_is_idempotent(self):
        """Calling bulk_upsert_artists twice doesn't create duplicates."""
        self._store.bulk_upsert_artists(self._all_names)
        conn = self._store._conn
        count = conn.execute("SELECT COUNT(*) FROM artist").fetchone()[0]
        assert count == len(self._all_names)

    def test_entity_dedup_on_empty_store(self):
        """Deduplication on a store with no QIDs is a no-op."""
        report = self._store.deduplicate_by_qid()
        assert report.groups_found == 0
        assert report.entities_merged == 0

    def test_upsert_artist_with_external_ids(self):
        """upsert_artist can set discogs_artist_id."""
        name = self._all_names[0]
        self._store.upsert_artist(name, discogs_artist_id=12345)
        artist = self._store.get_artist_by_name(name)
        assert artist is not None
        assert artist["discogs_artist_id"] == 12345

    def test_entity_created_and_linked(self):
        """Creating an entity and linking it to an artist works correctly."""
        name = self._all_names[0]
        entity = self._store.get_or_create_entity(name, "artist", wikidata_qid="Q123456")

        conn = self._store._conn
        conn.execute("UPDATE artist SET entity_id = ? WHERE canonical_name = ?", (entity.id, name))
        conn.commit()

        artist = self._store.get_artist_by_name(name)
        assert artist is not None
        assert artist["entity_id"] == entity.id

    def test_initialize_is_idempotent(self):
        """Calling initialize() twice doesn't error or corrupt data."""
        self._store.initialize()
        conn = self._store._conn
        count = conn.execute("SELECT COUNT(*) FROM artist").fetchone()[0]
        assert count == len(self._all_names)

    def test_fixture_has_realistic_artist_count(self):
        """The fixture dump produces a reasonable number of unique artists."""
        # The fixture has ~3 flowsheet entries but 1000 library codes
        # so we expect a small number of resolved artists
        assert len(self._all_names) >= 1
