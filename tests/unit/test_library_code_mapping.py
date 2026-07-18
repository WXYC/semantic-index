"""Tests for the graph-artist -> Backend library-code-id mapping (On Tour R3b).

Two units cooperate to populate ``artist.wxyc_library_code_id`` during the
nightly sync (WXYC/semantic-index#358):

* :func:`semantic_index.nightly_sync.build_library_code_map` — a pure function
  that turns the in-memory catalog (``list[LibraryCode]``) into an
  ``{unambiguous presentation_name -> code id}`` dict, excluding homonyms.
* :meth:`semantic_index.pipeline_db.PipelineDB.map_library_code_ids` — applies
  that dict to the ``artist`` table with a case-sensitive exact-name join,
  fully recomputing the column each run.

The mapping deliberately leaves homonym names (a name borne by >=2 catalog
codes, usually *distinct artists sharing a name*) unmapped, because the resolver
conflates them into a single graph node so no code id is correct. See the ticket
and WXYC/semantic-index#360 for the splitting track.
"""

from semantic_index.models import LibraryCode
from semantic_index.nightly_sync import build_library_code_map
from semantic_index.pipeline_db import PipelineDB


def _code(id: int, name: str) -> LibraryCode:
    return LibraryCode(id=id, genre_id=0, presentation_name=name)


def _artist_map(db: PipelineDB) -> dict[str, int | None]:
    """Return ``{canonical_name: wxyc_library_code_id}`` for every artist row."""
    rows = db._conn.execute("SELECT canonical_name, wxyc_library_code_id FROM artist").fetchall()
    return dict(rows)


def _make_db(tmp_path, names: list[str]) -> PipelineDB:
    db = PipelineDB(str(tmp_path / "graph.db"))
    db.initialize()
    for name in names:
        db.upsert_artist(name)
    return db


# ---------------------------------------------------------------------------
# build_library_code_map — ambiguity exclusion, verbatim names
# ---------------------------------------------------------------------------


class TestBuildLibraryCodeMap:
    def test_maps_unambiguous_names(self):
        codes = [_code(1, "Autechre"), _code(2, "Stereolab"), _code(3, "Jessica Pratt")]
        assert build_library_code_map(codes) == {
            "Autechre": 1,
            "Stereolab": 2,
            "Jessica Pratt": 3,
        }

    def test_excludes_homonym_names(self):
        # Three distinct bands all filed as "Lake" (the ticket's real example):
        # the shared name is excluded entirely, the unambiguous ones survive.
        codes = [
            _code(4, "Lake"),
            _code(33, "Lake"),
            _code(168, "Lake"),
            _code(1, "Autechre"),
        ]
        assert build_library_code_map(codes) == {"Autechre": 1}

    def test_case_variants_are_distinct_unambiguous_names(self):
        # "Bola" and "BOLA" are two different presentation names — each is
        # unambiguous on its own and maps. (That a *graph* node cased one way
        # can't claim the other's code is enforced at the join step, below.)
        codes = [_code(7, "Bola"), _code(8, "BOLA")]
        assert build_library_code_map(codes) == {"Bola": 7, "BOLA": 8}

    def test_empty_catalog(self):
        assert build_library_code_map([]) == {}

    def test_distinct_names_yield_distinct_code_ids(self):
        # Injectivity precondition: every value in the map is a distinct code id.
        codes = [_code(10, "Autechre"), _code(20, "Stereolab"), _code(30, "Juana Molina")]
        result = build_library_code_map(codes)
        assert len(set(result.values())) == len(result)


# ---------------------------------------------------------------------------
# PipelineDB.map_library_code_ids — the case-sensitive join
# ---------------------------------------------------------------------------


class TestMapLibraryCodeIds:
    def test_maps_matching_canonical_names(self, tmp_path):
        db = _make_db(tmp_path, ["Autechre", "Stereolab"])
        mapped = db.map_library_code_ids({"Autechre": 10, "Stereolab": 20})
        assert mapped == 2
        assert _artist_map(db) == {"Autechre": 10, "Stereolab": 20}

    def test_names_absent_from_graph_are_skipped(self, tmp_path):
        # A catalog artist WXYC never played has no graph node -> nothing created.
        db = _make_db(tmp_path, ["Autechre"])
        mapped = db.map_library_code_ids({"Autechre": 10, "Father John Misty": 99})
        assert mapped == 1
        assert _artist_map(db) == {"Autechre": 10}

    def test_unlisted_artists_stay_null(self, tmp_path):
        db = _make_db(tmp_path, ["Autechre", "Stereolab"])
        db.map_library_code_ids({"Autechre": 10})
        assert _artist_map(db) == {"Autechre": 10, "Stereolab": None}

    def test_match_is_case_sensitive(self, tmp_path):
        # Graph node "BOLA" (e.g. Discogs-cased) must NOT claim catalog "Bola".
        db = _make_db(tmp_path, ["BOLA"])
        mapped = db.map_library_code_ids({"Bola": 7})
        assert mapped == 0
        assert _artist_map(db) == {"BOLA": None}

    def test_empty_map_clears_and_returns_zero(self, tmp_path):
        db = _make_db(tmp_path, ["Autechre"])
        assert db.map_library_code_ids({}) == 0
        assert _artist_map(db) == {"Autechre": None}

    def test_recompute_clears_stale_mapping(self, tmp_path):
        # A name mapped last night that is absent tonight (e.g. it became a
        # homonym) must drop to NULL, not persist from the copied-forward DB.
        db = _make_db(tmp_path, ["Autechre", "Stereolab"])
        db.map_library_code_ids({"Autechre": 10, "Stereolab": 20})
        db.map_library_code_ids({"Autechre": 10})  # Stereolab no longer unambiguous
        assert _artist_map(db) == {"Autechre": 10, "Stereolab": None}

    def test_remap_updates_changed_code_id(self, tmp_path):
        db = _make_db(tmp_path, ["Autechre"])
        db.map_library_code_ids({"Autechre": 10})
        db.map_library_code_ids({"Autechre": 11})
        assert _artist_map(db) == {"Autechre": 11}

    def test_mapping_is_injective(self, tmp_path):
        db = _make_db(tmp_path, ["Autechre", "Stereolab", "Juana Molina"])
        db.map_library_code_ids({"Autechre": 10, "Stereolab": 20, "Juana Molina": 30})
        dupes = db._conn.execute(
            "SELECT wxyc_library_code_id, COUNT(*) c FROM artist "
            "WHERE wxyc_library_code_id IS NOT NULL GROUP BY 1 HAVING c > 1"
        ).fetchall()
        assert dupes == []

    def test_indexed_lookup_used(self, tmp_path):
        # The partial index on wxyc_library_code_id is what makes the neighbors
        # endpoint's reverse lookup (code id -> graph id) cheap; keep it present.
        db = _make_db(tmp_path, ["Autechre"])
        db.map_library_code_ids({"Autechre": 10})
        idx = db._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_artist_library_code'"
        ).fetchone()
        assert idx is not None


# ---------------------------------------------------------------------------
# End-to-end: build + map together (no live PG; this is the real integration)
# ---------------------------------------------------------------------------


class TestBuildAndMapEndToEnd:
    def test_homonym_node_stays_null_while_unambiguous_maps(self, tmp_path):
        codes = [_code(4, "Lake"), _code(33, "Lake"), _code(1, "Autechre")]
        db = _make_db(tmp_path, ["Lake", "Autechre"])
        db.map_library_code_ids(build_library_code_map(codes))
        assert _artist_map(db) == {"Lake": None, "Autechre": 1}

    def test_case_variant_graph_node_not_mapped(self, tmp_path):
        # Catalog has "Bola" (id 7). The graph has both the catalog-cased node
        # and a Discogs-cased "BOLA" node. Only the exact-case node maps.
        codes = [_code(7, "Bola")]
        db = _make_db(tmp_path, ["Bola", "BOLA"])
        db.map_library_code_ids(build_library_code_map(codes))
        assert _artist_map(db) == {"Bola": 7, "BOLA": None}


def test_column_exists_in_fresh_schema(tmp_path):
    # Guard the column the mapping and the neighbors endpoint depend on.
    db = _make_db(tmp_path, [])
    cols = {r[1] for r in db._conn.execute("PRAGMA table_info(artist)")}
    assert "wxyc_library_code_id" in cols
