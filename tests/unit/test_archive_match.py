"""Tests for archive artist name matching to production canonical names."""

from __future__ import annotations

from semantic_index.archive_match import ArchiveNameMatcher


def make_matcher(catalog: dict[str, int]) -> ArchiveNameMatcher:
    return ArchiveNameMatcher(canonical_to_id=catalog)


class TestExactMatch:
    def test_exact_match_returns_id(self):
        matcher = make_matcher({"Autechre": 1, "Stereolab": 2})
        assert matcher.resolve("Autechre") == {1}

    def test_no_match_returns_empty(self):
        matcher = make_matcher({"Autechre": 1})
        assert matcher.resolve("Nonexistent Artist") == set()


class TestNormalizedMatch:
    def test_case_insensitive(self):
        matcher = make_matcher({"Autechre": 1})
        assert matcher.resolve("AUTECHRE") == {1}
        assert matcher.resolve("autechre") == {1}

    def test_diacritics_stripped(self):
        matcher = make_matcher({"Beyoncé": 7})
        assert matcher.resolve("Beyonce") == {7}
        assert matcher.resolve("BEYONCE") == {7}

    def test_the_prefix(self):
        matcher = make_matcher({"The Beatles": 3})
        assert matcher.resolve("Beatles") == {3}
        assert matcher.resolve("the beatles") == {3}

    def test_ampersand_to_and(self):
        matcher = make_matcher({"Simon & Garfunkel": 4})
        assert matcher.resolve("Simon and Garfunkel") == {4}

    def test_trailing_brackets_stripped(self):
        matcher = make_matcher({"Stereolab": 2})
        assert matcher.resolve("Stereolab [UK]") == {2}


class TestHtmlEntityDecode:
    def test_numeric_entity(self):
        """Greek mu (μ) encoded as &#956; should match the literal μ."""
        matcher = make_matcher({"μ-Ziq": 9})
        assert matcher.resolve("&#956;-Ziq") == {9}

    def test_hex_entity(self):
        matcher = make_matcher({"μ-Ziq": 9})
        assert matcher.resolve("&#x3bc;-Ziq") == {9}

    def test_named_entity(self):
        matcher = make_matcher({"AC&DC": 11})
        # &amp; → &  (then `& ` → `and ` only applies with surrounding spaces)
        assert matcher.resolve("AC&amp;DC") == {11}


class TestQuoteStripping:
    def test_wrapping_double_quotes(self):
        matcher = make_matcher({"Weird Al Yankovic": 5})
        assert matcher.resolve('"Weird Al" Yankovic') == {5}

    def test_escaped_double_quotes(self):
        """Backslash-escaped quotes that leaked from JSON/SQL escaping."""
        matcher = make_matcher({"Weird Al Yankovic": 5})
        assert matcher.resolve('\\"Weird Al\\" Yankovic') == {5}


class TestMultiArtistSplit:
    def test_comma_split_returns_all_matches(self):
        matcher = make_matcher({"Autechre": 1, "Stereolab": 2})
        assert matcher.resolve("Autechre, Stereolab") == {1, 2}

    def test_slash_split(self):
        matcher = make_matcher({"Autechre": 1, "Stereolab": 2})
        assert matcher.resolve("Autechre / Stereolab") == {1, 2}

    def test_partial_match_split_returns_matched_only(self):
        matcher = make_matcher({"Autechre": 1})
        assert matcher.resolve("Autechre, Unknown Artist") == {1}

    def test_no_split_match_returns_empty(self):
        matcher = make_matcher({"Autechre": 1})
        assert matcher.resolve("Foo, Bar") == set()


class TestCompilationSkip:
    def test_various_artists_returns_empty(self):
        matcher = make_matcher({"Autechre": 1})
        assert matcher.resolve("Various Artists") == set()
        assert matcher.resolve("V/A") == set()

    def test_soundtrack_returns_empty(self):
        matcher = make_matcher({"Autechre": 1})
        assert matcher.resolve("Soundtrack") == set()


class TestAmbiguity:
    def test_ambiguous_normalized_form_returns_empty(self):
        """When a normalized form maps to multiple canonical names, we can't pick."""
        matcher = make_matcher(
            {
                "Beyoncé": 1,
                "Beyonce": 2,  # collision after diacritics stripping
            }
        )
        # Both normalize to "beyonce" — exact match wins where the literal exists,
        # but a casing-only variant has to fall through to normalized lookup,
        # which is ambiguous between id 1 and id 2.
        assert matcher.resolve("Beyonce") == {2}  # exact wins
        assert matcher.resolve("BEYONCE") == set()  # ambiguous via normalization

    def test_exact_match_takes_priority_over_normalized(self):
        matcher = make_matcher({"The Beatles": 1, "Beatles": 2})
        # "Beatles" exact-matches id 2; doesn't fall through to normalized "the beatles"
        assert matcher.resolve("Beatles") == {2}


class TestStats:
    def test_resolve_records_method(self):
        """The matcher tracks how each lookup was resolved (for telemetry)."""
        matcher = make_matcher({"Autechre": 1, "Stereolab": 2, "The Beatles": 3})

        matcher.resolve("Autechre")
        matcher.resolve("the beatles")
        matcher.resolve("Autechre, Stereolab")
        matcher.resolve("Various Artists")
        matcher.resolve("Nonexistent")

        stats = matcher.stats
        assert stats["exact"] == 1
        assert stats["normalized"] == 1
        assert stats["split"] == 1
        assert stats["compilation_skip"] == 1
        assert stats["unmatched"] == 1
