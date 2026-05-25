"""Characterization tests ensuring wxyc_etl functions are parity with local implementations.

These tests verify that:
1. wxyc_etl.text.is_compilation_artist covers all cases that utils.is_various_artists handles
2. wxyc_etl.text.to_match_form (WX-2 Normalizer Charter) produces the expected base
   normalization used by artist_resolver._normalize
3. wxyc_etl.schema constants cover all hardcoded table names in discogs_client.py
"""

import pytest
from wxyc_etl import schema, text

# --- is_compilation_artist parity with utils.is_various_artists ---


@pytest.mark.parametrize(
    "name",
    [
        "V/A",
        "Various",
        "Various Artists",
        "various artists",
    ],
    ids=[
        "V/A",
        "Various",
        "Various_Artists",
        "various_artists_lowercase",
    ],
)
def test_is_compilation_artist_covers_old_is_various_artists(name):
    """wxyc_etl.is_compilation_artist returns True for every input
    that the old is_various_artists returned True for."""
    assert text.is_compilation_artist(name) is True


@pytest.mark.parametrize(
    "name",
    [
        "Soundtrack",
        "Compilation",
        "v.a.",
        "VARIOUS",
    ],
    ids=[
        "Soundtrack",
        "Compilation",
        "v.a.",
        "VARIOUS_caps",
    ],
)
def test_is_compilation_artist_additional_coverage(name):
    """wxyc_etl.is_compilation_artist covers additional cases the old
    narrow check missed."""
    assert text.is_compilation_artist(name) is True


@pytest.mark.parametrize(
    "name",
    [
        # Real artists — never flagged.
        "Autechre",
        "Stereolab",
        "Cat Power",
        "Father John Misty",
        "",
        # Tightened semantics in wxyc-etl 0.5.0 (anchored leading-prefix + exact-only):
        # the legacy substring matcher returned True for these; the new matcher
        # returns False. "soundtrack" / "various" / "compilation" are exact-only;
        # padded forms no longer satisfy the leading anchor at position 0.
        "  Various Artists  ",
        "Original Motion Picture Soundtrack",
        "A Compilation Album",
        "Various Production",
    ],
)
def test_is_compilation_artist_false_for_real_artists(name):
    """Real artist names and tightened non-matches should not be flagged."""
    assert text.is_compilation_artist(name) is False


# --- to_match_form parity with artist_resolver._normalize base layer ---


@pytest.mark.parametrize(
    "input_name,expected",
    [
        # Basic lowercasing and trimming
        ("Autechre", "autechre"),
        ("  Autechre  ", "autechre"),
        ("AUTECHRE", "autechre"),
        # Diacritics (NFKD decomposition)
        ("Bjork", "bjork"),
        ("Sigur Ros", "sigur ros"),
        # NFKD + strip diacritics
        ("Cafe Tacvba", "cafe tacvba"),
        # Empty
        ("", ""),
        # Whitespace edge cases
        ("  Mixed Case  ", "mixed case"),
    ],
)
def test_to_match_form_parity(input_name, expected):
    """wxyc_etl.to_match_form produces the expected base normalization for common cases."""
    assert text.to_match_form(input_name) == expected


# --- split_artist_name parity with artist_resolver._normalized_forms ---


def test_split_artist_name_slash():
    """Slash-separated names split correctly."""
    result = text.split_artist_name("J Dilla / Jay Dee")
    assert result == ["J Dilla", "Jay Dee"]


def test_split_artist_name_no_split():
    """Single names return None."""
    assert text.split_artist_name("Autechre") is None


def test_split_artist_name_ampersand_no_context():
    """Ampersand does not split without context."""
    assert text.split_artist_name("Duke Ellington & John Coltrane") is None


# --- schema constants cover all hardcoded table names ---


class TestSchemaConstants:
    """Verify wxyc_etl.schema has constants for all discogs-cache tables used
    by semantic-index."""

    def test_release_table(self):
        assert schema.RELEASE_TABLE == "release"

    def test_release_artist_table(self):
        assert schema.RELEASE_ARTIST_TABLE == "release_artist"

    def test_release_label_table(self):
        assert schema.RELEASE_LABEL_TABLE == "release_label"

    def test_release_style_table(self):
        assert schema.RELEASE_STYLE_TABLE == "release_style"

    def test_release_track_table(self):
        assert schema.RELEASE_TRACK_TABLE == "release_track"

    def test_release_track_artist_table(self):
        assert schema.RELEASE_TRACK_ARTIST_TABLE == "release_track_artist"

    def test_discogs_tables_includes_all(self):
        """All tables used by discogs_client.py should be in the discogs_tables() list."""
        tables = schema.discogs_tables()
        used_tables = [
            "release",
            "release_artist",
            "release_label",
            "release_style",
            "release_track",
            "release_track_artist",
        ]
        for table in used_tables:
            assert table in tables, f"{table} not in discogs_tables()"
