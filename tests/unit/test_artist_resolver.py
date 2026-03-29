"""Tests for artist name resolution."""

from unittest.mock import MagicMock

import pytest

from semantic_index.artist_resolver import (
    FUZZY_MIN_SCORE,
    FUZZY_MIN_SCORE_RELAXED,
    FUZZY_RELAXED_MIN_PLAYS,
    ArtistResolver,
)
from semantic_index.models import DiscogsSearchResult, ResolvedEntry
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


class TestDiscogsResolution:
    """Tests for Tier 3 Discogs-based resolution."""

    def _make_resolver(self, releases=None, codes=None, discogs_client=None):
        return ArtistResolver(
            releases=releases or [], codes=codes or [], discogs_client=discogs_client
        )

    def test_discogs_resolves_unknown_artist(self):
        mock_client = MagicMock()
        mock_client.search_artist.return_value = DiscogsSearchResult(
            artist_name="Ty Segall", artist_id=12345, confidence=0.95
        )
        resolver = self._make_resolver(discogs_client=mock_client)

        entry = make_flowsheet_entry(library_release_id=0, artist_name="Ty Segall")
        resolved = resolver.resolve(entry)

        assert resolved.canonical_name == "Ty Segall"
        assert resolved.resolution_method == "discogs"

    def test_discogs_no_match_falls_to_raw(self):
        mock_client = MagicMock()
        mock_client.search_artist.return_value = None
        resolver = self._make_resolver(discogs_client=mock_client)

        entry = make_flowsheet_entry(library_release_id=0, artist_name="ZZZZZ Unknown")
        resolved = resolver.resolve(entry)

        assert resolved.resolution_method == "raw"

    def test_discogs_skipped_when_client_is_none(self):
        resolver = self._make_resolver(discogs_client=None)

        entry = make_flowsheet_entry(library_release_id=0, artist_name="Ty Segall")
        resolved = resolver.resolve(entry)

        assert resolved.resolution_method == "raw"

    def test_catalog_takes_precedence_over_discogs(self):
        """FK chain should resolve before Discogs is even tried."""
        mock_client = MagicMock()
        release = make_library_release(id=100, library_code_id=200)
        code = make_library_code(id=200, presentation_name="Autechre")
        resolver = self._make_resolver(releases=[release], codes=[code], discogs_client=mock_client)

        entry = make_flowsheet_entry(library_release_id=100, artist_name="Autechre")
        resolved = resolver.resolve(entry)

        assert resolved.resolution_method == "catalog"
        mock_client.search_artist.assert_not_called()

    def test_name_match_takes_precedence_over_discogs(self):
        mock_client = MagicMock()
        code = make_library_code(id=200, presentation_name="Stereolab")
        resolver = self._make_resolver(codes=[code], discogs_client=mock_client)

        entry = make_flowsheet_entry(library_release_id=0, artist_name="Stereolab")
        resolved = resolver.resolve(entry)

        assert resolved.resolution_method == "name_match"
        mock_client.search_artist.assert_not_called()

    def test_discogs_passes_release_title(self):
        """The resolver should pass release_title to help Discogs disambiguate."""
        mock_client = MagicMock()
        mock_client.search_artist.return_value = DiscogsSearchResult(
            artist_name="Omar S", artist_id=99, confidence=0.9
        )
        resolver = self._make_resolver(discogs_client=mock_client)

        entry = make_flowsheet_entry(
            library_release_id=0, artist_name="Omar S", release_title="Just Ask The Lonely"
        )
        resolver.resolve(entry)

        mock_client.search_artist.assert_called_once_with("Omar S", "Just Ask The Lonely")


class TestPlayCountWeightedFuzzy:
    """Tests for play-count-weighted fuzzy matching (re_resolve_with_play_counts).

    Uses "Fela Kuti" → "Fela Anikulapo Kuti" (JW score ~0.837) as the
    canonical test case: too low for the standard 0.90 threshold, but above
    the relaxed 0.82 threshold used for names with sufficient play count.
    """

    def _make_resolver(self, codes=None):
        return ArtistResolver(releases=[], codes=codes or [])

    def _make_raw_entries(self, artist_name: str, count: int) -> list[ResolvedEntry]:
        """Create a list of raw-resolved entries for a given artist name."""
        return [
            ResolvedEntry(
                entry=make_flowsheet_entry(id=i, library_release_id=0, artist_name=artist_name),
                canonical_name=artist_name.strip().lower(),
                resolution_method="raw",
            )
            for i in range(count)
        ]

    def test_re_resolve_matches_high_play_count_name(self):
        """Names with 10+ plays should match at the relaxed threshold (0.82)."""
        # "fela kuti" vs "Fela Anikulapo Kuti" has JW ~0.837 — above 0.82 but below 0.90
        code = make_library_code(id=200, presentation_name="Fela Anikulapo Kuti")
        resolver = self._make_resolver(codes=[code])

        raw_entries = self._make_raw_entries("Fela Kuti", count=15)
        result = resolver.re_resolve_with_play_counts(raw_entries)

        assert all(r.canonical_name == "Fela Anikulapo Kuti" for r in result)
        assert all(r.resolution_method == "fuzzy_relaxed" for r in result)

    def test_re_resolve_skips_low_play_count_name(self):
        """Names with fewer than min_plays should stay raw."""
        code = make_library_code(id=200, presentation_name="Fela Anikulapo Kuti")
        resolver = self._make_resolver(codes=[code])

        raw_entries = self._make_raw_entries("Fela Kuti", count=3)
        result = resolver.re_resolve_with_play_counts(raw_entries)

        assert all(r.resolution_method == "raw" for r in result)
        assert all(r.canonical_name == "fela kuti" for r in result)

    def test_re_resolve_preserves_non_raw_entries(self):
        """Entries resolved via catalog, name_match, or fuzzy are not touched."""
        code = make_library_code(id=200, presentation_name="Stereolab")
        resolver = self._make_resolver(codes=[code])

        catalog_entry = ResolvedEntry(
            entry=make_flowsheet_entry(id=1, artist_name="Stereolab"),
            canonical_name="Stereolab",
            resolution_method="catalog",
        )
        fuzzy_entry = ResolvedEntry(
            entry=make_flowsheet_entry(id=2, artist_name="Stereolabb"),
            canonical_name="Stereolab",
            resolution_method="fuzzy",
        )
        resolved = [catalog_entry, fuzzy_entry]
        result = resolver.re_resolve_with_play_counts(resolved)

        assert result[0].resolution_method == "catalog"
        assert result[1].resolution_method == "fuzzy"

    def test_re_resolve_with_custom_min_plays(self):
        """The min_plays parameter controls the play count threshold."""
        code = make_library_code(id=200, presentation_name="Fela Anikulapo Kuti")
        resolver = self._make_resolver(codes=[code])

        # 5 entries — below default (10) but above custom threshold (3)
        raw_entries = self._make_raw_entries("Fela Kuti", count=5)

        # Default threshold: should NOT match
        result_default = resolver.re_resolve_with_play_counts(raw_entries)
        assert all(r.resolution_method == "raw" for r in result_default)

        # Custom threshold of 3: should match
        result_custom = resolver.re_resolve_with_play_counts(raw_entries, min_plays=3)
        assert all(r.resolution_method == "fuzzy_relaxed" for r in result_custom)

    def test_re_resolve_still_rejects_scores_below_relaxed_threshold(self):
        """Names with JW score below the relaxed threshold stay raw even with high play count."""
        # "buck meek" vs "Beck" has JW ~0.694 — well below 0.82
        code = make_library_code(id=200, presentation_name="Beck")
        resolver = self._make_resolver(codes=[code])

        raw_entries = self._make_raw_entries("Buck Meek", count=50)
        result = resolver.re_resolve_with_play_counts(raw_entries)

        assert all(r.resolution_method == "raw" for r in result)

    def test_re_resolve_consistent_across_entries_with_same_name(self):
        """All entries with the same raw name get the same resolution."""
        code = make_library_code(id=200, presentation_name="Fela Anikulapo Kuti")
        resolver = self._make_resolver(codes=[code])

        raw_entries = self._make_raw_entries("Fela Kuti", count=12)
        result = resolver.re_resolve_with_play_counts(raw_entries)

        canonical_names = {r.canonical_name for r in result}
        methods = {r.resolution_method for r in result}
        assert len(canonical_names) == 1
        assert len(methods) == 1

    def test_re_resolve_mixed_raw_and_resolved(self):
        """Only raw entries are candidates for re-resolution."""
        code = make_library_code(id=200, presentation_name="Fela Anikulapo Kuti")
        resolver = self._make_resolver(codes=[code])

        catalog_entry = ResolvedEntry(
            entry=make_flowsheet_entry(id=100, artist_name="Autechre"),
            canonical_name="Autechre",
            resolution_method="catalog",
        )
        raw_entries = self._make_raw_entries("Fela Kuti", count=10)
        resolved = [catalog_entry] + raw_entries

        result = resolver.re_resolve_with_play_counts(resolved)

        assert result[0].resolution_method == "catalog"
        assert result[0].canonical_name == "Autechre"
        assert all(r.resolution_method == "fuzzy_relaxed" for r in result[1:])

    def test_re_resolve_returns_new_list(self):
        """re_resolve_with_play_counts should not mutate the input list."""
        code = make_library_code(id=200, presentation_name="Fela Anikulapo Kuti")
        resolver = self._make_resolver(codes=[code])

        raw_entries = self._make_raw_entries("Fela Kuti", count=15)
        original_methods = [r.resolution_method for r in raw_entries]

        result = resolver.re_resolve_with_play_counts(raw_entries)

        # Input list still has raw methods
        assert [r.resolution_method for r in raw_entries] == original_methods
        # Result is a different list
        assert result is not raw_entries

    @pytest.mark.parametrize(
        "relaxed_threshold,expected_method",
        [
            (0.82, "fuzzy_relaxed"),  # 0.837 > 0.82 → match
            (0.85, "raw"),  # 0.837 < 0.85 → no match
        ],
    )
    def test_re_resolve_with_custom_relaxed_threshold(self, relaxed_threshold, expected_method):
        """The relaxed_threshold parameter controls the minimum score."""
        code = make_library_code(id=200, presentation_name="Fela Anikulapo Kuti")
        resolver = self._make_resolver(codes=[code])

        raw_entries = self._make_raw_entries("Fela Kuti", count=15)
        result = resolver.re_resolve_with_play_counts(
            raw_entries, relaxed_threshold=relaxed_threshold
        )

        assert all(r.resolution_method == expected_method for r in result)

    def test_constants_have_expected_values(self):
        """Verify the module constants match the values from the issue analysis."""
        assert FUZZY_MIN_SCORE == 0.90
        assert FUZZY_MIN_SCORE_RELAXED == 0.82
        assert FUZZY_RELAXED_MIN_PLAYS == 10
