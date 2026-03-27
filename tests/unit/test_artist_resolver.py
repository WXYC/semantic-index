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

    def test_name_match_when_fk_missing(self):
        """Entries without a LIBRARY_RELEASE_ID should match by artist name."""
        code = make_library_code(id=200, presentation_name="Stereolab")
        resolver = self._make_resolver(codes=[code])

        entry = make_flowsheet_entry(library_release_id=0, artist_name="Stereolab")
        resolved = resolver.resolve(entry)

        assert resolved.canonical_name == "Stereolab"
        assert resolved.resolution_method == "name_match"

    def test_name_match_case_insensitive(self):
        code = make_library_code(id=200, presentation_name="Cat Power")
        resolver = self._make_resolver(codes=[code])

        entry = make_flowsheet_entry(library_release_id=0, artist_name="cat power")
        resolved = resolver.resolve(entry)

        assert resolved.canonical_name == "Cat Power"
        assert resolved.resolution_method == "name_match"

    def test_name_match_strips_whitespace(self):
        code = make_library_code(id=200, presentation_name="Jessica Pratt")
        resolver = self._make_resolver(codes=[code])

        entry = make_flowsheet_entry(library_release_id=0, artist_name="  Jessica Pratt  ")
        resolved = resolver.resolve(entry)

        assert resolved.canonical_name == "Jessica Pratt"
        assert resolved.resolution_method == "name_match"

    def test_fk_takes_precedence_over_name_match(self):
        """When FK resolves, use it even if name also matches a different code."""
        release = make_library_release(id=100, library_code_id=200)
        code_fk = make_library_code(id=200, presentation_name="Autechre")
        code_name = make_library_code(id=201, presentation_name="autechre")
        resolver = self._make_resolver(releases=[release], codes=[code_fk, code_name])

        entry = make_flowsheet_entry(library_release_id=100, artist_name="autechre")
        resolved = resolver.resolve(entry)

        assert resolved.canonical_name == "Autechre"
        assert resolved.resolution_method == "catalog"

    def test_no_name_match_falls_to_raw(self):
        """When no FK and no name match, fall back to raw lowercased."""
        code = make_library_code(id=200, presentation_name="Autechre")
        resolver = self._make_resolver(codes=[code])

        entry = make_flowsheet_entry(library_release_id=0, artist_name="Some Unknown DJ")
        resolved = resolver.resolve(entry)

        assert resolved.canonical_name == "some unknown dj"
        assert resolved.resolution_method == "raw"

    def test_normalized_match_strips_the(self):
        """'The Beach Boys' should match 'Beach Boys' via normalization."""
        code = make_library_code(id=200, presentation_name="Beach Boys")
        resolver = self._make_resolver(codes=[code])

        entry = make_flowsheet_entry(library_release_id=0, artist_name="The Beach Boys")
        resolved = resolver.resolve(entry)

        assert resolved.canonical_name == "Beach Boys"
        assert resolved.resolution_method == "name_match"

    def test_normalized_match_and_vs_ampersand(self):
        """'Belle & Sebastian' should match 'Belle and Sebastian' via normalization."""
        code = make_library_code(id=200, presentation_name="Belle and Sebastian")
        resolver = self._make_resolver(codes=[code])

        entry = make_flowsheet_entry(library_release_id=0, artist_name="Belle & Sebastian")
        resolved = resolver.resolve(entry)

        assert resolved.canonical_name == "Belle and Sebastian"
        assert resolved.resolution_method == "name_match"

    def test_normalized_match_strips_bracket_suffix(self):
        """'Camera Obscura' should match 'Camera Obscura [Scotland]' if only one exists."""
        code = make_library_code(id=200, presentation_name="Camera Obscura [Scotland]")
        resolver = self._make_resolver(codes=[code])

        entry = make_flowsheet_entry(library_release_id=0, artist_name="Camera Obscura")
        resolved = resolver.resolve(entry)

        assert resolved.canonical_name == "Camera Obscura [Scotland]"
        assert resolved.resolution_method == "name_match"

    def test_normalized_match_rejects_ambiguous_brackets(self):
        """When multiple bracket variants exist, don't guess."""
        code_a = make_library_code(id=200, presentation_name="Camera Obscura [California]")
        code_b = make_library_code(id=201, presentation_name="Camera Obscura [Scotland]")
        resolver = self._make_resolver(codes=[code_a, code_b])

        entry = make_flowsheet_entry(library_release_id=0, artist_name="Camera Obscura")
        resolved = resolver.resolve(entry)

        # Ambiguous — falls through (fuzzy or raw)
        assert resolved.resolution_method != "name_match"

    def test_normalized_match_rolling_stones(self):
        """'Rolling Stones' should match 'The Rolling Stones' (reverse 'The' stripping)."""
        code = make_library_code(id=200, presentation_name="The Rolling Stones")
        resolver = self._make_resolver(codes=[code])

        entry = make_flowsheet_entry(library_release_id=0, artist_name="Rolling Stones")
        resolved = resolver.resolve(entry)

        assert resolved.canonical_name == "The Rolling Stones"
        assert resolved.resolution_method == "name_match"

    def test_normalized_match_slash_alias(self):
        """'J Dilla' should match 'J Dilla / Jay Dee' via alias normalization."""
        code = make_library_code(id=200, presentation_name="J Dilla / Jay Dee")
        resolver = self._make_resolver(codes=[code])

        entry = make_flowsheet_entry(library_release_id=0, artist_name="J Dilla")
        resolved = resolver.resolve(entry)

        assert resolved.canonical_name == "J Dilla / Jay Dee"
        assert resolved.resolution_method == "name_match"

    def test_normalized_match_aka_alias(self):
        """'Caribou' should match 'Manitoba aka Caribou' via alias normalization."""
        code = make_library_code(id=200, presentation_name="Manitoba aka Caribou")
        resolver = self._make_resolver(codes=[code])

        entry = make_flowsheet_entry(library_release_id=0, artist_name="Caribou")
        resolved = resolver.resolve(entry)

        assert resolved.canonical_name == "Manitoba aka Caribou"
        assert resolved.resolution_method == "name_match"

    def test_fuzzy_match_close_variant(self):
        """Fuzzy matching resolves typo-level variants (score > 0.90)."""
        code = make_library_code(id=200, presentation_name="Ariel Pink's Haunted Graffiti")
        resolver = self._make_resolver(codes=[code])

        entry = make_flowsheet_entry(
            library_release_id=0, artist_name="Ariel Pinks Haunted Graffiti"
        )
        resolved = resolver.resolve(entry)

        assert resolved.canonical_name == "Ariel Pink's Haunted Graffiti"
        assert resolved.resolution_method == "fuzzy"

    def test_fuzzy_rejects_low_score(self):
        """Names that are too different should not fuzzy match."""
        code = make_library_code(id=200, presentation_name="Autechre")
        resolver = self._make_resolver(codes=[code])

        entry = make_flowsheet_entry(library_release_id=0, artist_name="Radiohead")
        resolved = resolver.resolve(entry)

        assert resolved.resolution_method == "raw"

    def test_fuzzy_rejects_false_positive(self):
        """'Autechre' should not match 'Auteurs' despite Jaro-Winkler similarity."""
        code = make_library_code(id=200, presentation_name="Auteurs")
        resolver = self._make_resolver(codes=[code])

        entry = make_flowsheet_entry(library_release_id=0, artist_name="Autechre")
        resolved = resolver.resolve(entry)

        assert resolved.resolution_method == "raw"

    def test_fuzzy_after_name_match_precedence(self):
        """Exact name match takes precedence over fuzzy."""
        code_exact = make_library_code(id=200, presentation_name="Alex G")
        code_fuzzy = make_library_code(id=201, presentation_name="Alex Gopher")
        resolver = self._make_resolver(codes=[code_exact, code_fuzzy])

        entry = make_flowsheet_entry(library_release_id=0, artist_name="Alex G")
        resolved = resolver.resolve(entry)

        assert resolved.canonical_name == "Alex G"
        assert resolved.resolution_method == "name_match"

    def test_genre_lookup(self):
        release = make_library_release(id=100, library_code_id=200)
        code = make_library_code(id=200, genre_id=15, presentation_name="Autechre")
        resolver = self._make_resolver(releases=[release], codes=[code])

        assert resolver.get_genre_id(100) == 15

    def test_genre_lookup_returns_none_for_unknown(self):
        resolver = self._make_resolver()
        assert resolver.get_genre_id(999) is None
