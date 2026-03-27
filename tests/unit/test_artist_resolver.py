"""Tests for Tier 1 artist name resolution."""

from semantic_index.artist_resolver import ArtistResolver
from tests.conftest import make_flowsheet_entry, make_library_code, make_library_release


class TestArtistResolver:
    def _make_resolver(self, releases=None, codes=None):
        return ArtistResolver(releases=releases or [], codes=codes or [])

    def test_resolves_via_catalog_fk_chain(self):
        release = make_library_release(id=100, library_code_id=200)
        code = make_library_code(id=200, presentation_name="Autechre")
        resolver = self._make_resolver(releases=[release], codes=[code])

        entry = make_flowsheet_entry(library_release_id=100, artist_name="autechre")
        resolved = resolver.resolve(entry)

        assert resolved.canonical_name == "Autechre"
        assert resolved.resolution_method == "catalog"

    def test_falls_back_when_library_release_id_is_zero(self):
        resolver = self._make_resolver()
        entry = make_flowsheet_entry(library_release_id=0, artist_name="  Stereolab  ")
        resolved = resolver.resolve(entry)

        assert resolved.canonical_name == "stereolab"
        assert resolved.resolution_method == "raw"

    def test_falls_back_when_release_not_in_table(self):
        resolver = self._make_resolver()
        entry = make_flowsheet_entry(library_release_id=999, artist_name="Cat Power")
        resolved = resolver.resolve(entry)

        assert resolved.canonical_name == "cat power"
        assert resolved.resolution_method == "raw"

    def test_falls_back_when_code_not_in_table(self):
        release = make_library_release(id=100, library_code_id=999)
        resolver = self._make_resolver(releases=[release])
        entry = make_flowsheet_entry(library_release_id=100, artist_name="Jessica Pratt")
        resolved = resolver.resolve(entry)

        assert resolved.canonical_name == "jessica pratt"
        assert resolved.resolution_method == "raw"

    def test_strips_whitespace_from_raw_name(self):
        resolver = self._make_resolver()
        entry = make_flowsheet_entry(library_release_id=0, artist_name="  Father John Misty  ")
        resolved = resolver.resolve(entry)

        assert resolved.canonical_name == "father john misty"

    def test_preserves_entry_reference(self):
        resolver = self._make_resolver()
        entry = make_flowsheet_entry(library_release_id=0)
        resolved = resolver.resolve(entry)

        assert resolved.entry is entry

    def test_genre_lookup(self):
        release = make_library_release(id=100, library_code_id=200)
        code = make_library_code(id=200, genre_id=15, presentation_name="Autechre")
        resolver = self._make_resolver(releases=[release], codes=[code])

        assert resolver.get_genre_id(100) == 15

    def test_genre_lookup_returns_none_for_unknown(self):
        resolver = self._make_resolver()
        assert resolver.get_genre_id(999) is None
