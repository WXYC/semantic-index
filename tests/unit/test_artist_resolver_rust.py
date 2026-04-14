"""Tests for Rust batch fuzzy resolve parity with Python path.

Verifies that the Rust (wxyc_etl.fuzzy.batch_fuzzy_resolve) path produces
identical resolution results to the existing Python (rapidfuzz) path.
"""

import os

import pytest

from semantic_index.artist_resolver import ArtistResolver
from semantic_index.models import ResolvedEntry
from tests.conftest import make_flowsheet_entry, make_library_code, make_library_release

# Skip all tests in this module if the Rust bindings aren't available
pytest.importorskip("wxyc_etl.fuzzy", reason="wxyc-etl Rust bindings not installed")


def _make_catalog_codes():
    """Build library codes for WXYC example artists used in fuzzy matching tests."""
    artists = [
        (200, "Autechre"),
        (201, "Stereolab"),
        (202, "Cat Power"),
        (203, "Jessica Pratt"),
        (204, "Fela Anikulapo Kuti"),
        (205, "J Dilla / Jay Dee"),
        (206, "Father John Misty"),
        (207, "Ariel Pink's Haunted Graffiti"),
    ]
    return [make_library_code(id=id_, presentation_name=name) for id_, name in artists]


def _resolve_with_path(entries, codes, *, use_rust: bool, releases=None):
    """Resolve entries using either the Rust batch or Python per-entry path.

    When use_rust=True, uses resolve_all() which pre-populates the fuzzy cache
    via the Rust batch call. When use_rust=False, forces the Python fallback by
    setting WXYC_ETL_NO_RUST and using per-entry resolve().
    """
    env_key = "WXYC_ETL_NO_RUST"
    old_val = os.environ.get(env_key)
    try:
        if use_rust:
            os.environ.pop(env_key, None)
            resolver = ArtistResolver(releases=releases or [], codes=codes)
            return resolver.resolve_all(entries)
        else:
            os.environ[env_key] = "1"
            resolver = ArtistResolver(releases=releases or [], codes=codes)
            return [resolver.resolve(entry) for entry in entries]
    finally:
        if old_val is None:
            os.environ.pop(env_key, None)
        else:
            os.environ[env_key] = old_val


class TestBatchFuzzyResolveParity:
    """Verify Rust and Python fuzzy paths produce identical resolution results."""

    def test_batch_fuzzy_resolve_parity(self):
        """All test entries resolve identically via Rust and Python paths."""
        codes = _make_catalog_codes()
        entries = [
            # Tier 2 (exact name match, not fuzzy)
            make_flowsheet_entry(id=1, library_release_id=0, artist_name="autechre"),
            # Should NOT match Autechre (JW 0.868 < 0.90)
            make_flowsheet_entry(id=2, library_release_id=0, artist_name="Auteurs"),
            # Fuzzy match: "Stereo Lab" → "Stereolab" (JW 0.980)
            make_flowsheet_entry(id=3, library_release_id=0, artist_name="Stereo Lab"),
            # Fuzzy match: "Cat Powers" → "Cat Power" (JW 0.980)
            make_flowsheet_entry(id=4, library_release_id=0, artist_name="Cat Powers"),
            # Too different — raw fallback
            make_flowsheet_entry(id=5, library_release_id=0, artist_name="Unknown Artist XYZ"),
            # Fuzzy match: apostrophe variant (JW 0.993)
            make_flowsheet_entry(
                id=6, library_release_id=0, artist_name="Ariel Pinks Haunted Graffiti"
            ),
        ]

        rust_results = _resolve_with_path(entries, codes, use_rust=True)
        python_results = _resolve_with_path(entries, codes, use_rust=False)

        for rust, python in zip(rust_results, python_results, strict=True):
            assert rust.canonical_name == python.canonical_name, (
                f"entry {rust.entry.id}: Rust={rust.canonical_name!r} vs Python={python.canonical_name!r}"
            )
            assert rust.resolution_method == python.resolution_method, (
                f"entry {rust.entry.id}: Rust={rust.resolution_method!r} vs Python={python.resolution_method!r}"
            )

    def test_tiers_1_through_3_unchanged(self):
        """FK chain, name match, and normalized match are untouched by the Rust path."""
        release = make_library_release(id=100, library_code_id=200)
        codes = _make_catalog_codes()
        entries = [
            # Tier 1: FK chain
            make_flowsheet_entry(id=1, library_release_id=100, artist_name="whatever"),
            # Tier 2: exact name match
            make_flowsheet_entry(id=2, library_release_id=0, artist_name="Stereolab"),
            # Tier 3: normalized ("The" stripping)
            make_flowsheet_entry(id=3, library_release_id=0, artist_name="The Stereolab"),
        ]
        results = _resolve_with_path(entries, codes, use_rust=True, releases=[release])

        assert results[0].resolution_method == "catalog"
        assert results[0].canonical_name == "Autechre"
        assert results[1].resolution_method == "name_match"
        assert results[1].canonical_name == "Stereolab"


class TestAmbiguityGuardParity:
    """Verify the Rust path rejects ambiguous matches identically to Python."""

    def test_ambiguity_guard_rejects_close_scores(self):
        """When top-2 candidates score within FUZZY_AMBIGUITY_THRESHOLD, reject."""
        # "Alex G" and "Alex Gee" are very similar to query "Alex Ge"
        codes = [
            make_library_code(id=200, presentation_name="Alex G"),
            make_library_code(id=201, presentation_name="Alex Gee"),
        ]
        entries = [
            make_flowsheet_entry(id=1, library_release_id=0, artist_name="Alex Ge"),
        ]

        rust_results = _resolve_with_path(entries, codes, use_rust=True)
        python_results = _resolve_with_path(entries, codes, use_rust=False)

        assert rust_results[0].resolution_method == python_results[0].resolution_method
        assert rust_results[0].canonical_name == python_results[0].canonical_name

    def test_ambiguity_guard_accepts_clear_winner(self):
        """When top candidate clearly beats the second, accept the match."""
        codes = [
            make_library_code(id=200, presentation_name="Stereolab"),
            make_library_code(id=201, presentation_name="Autechre"),
        ]
        entries = [
            # "Stereo Lab" clearly matches "Stereolab" (0.980) over "Autechre" (~0.4)
            make_flowsheet_entry(id=1, library_release_id=0, artist_name="Stereo Lab"),
        ]

        rust_results = _resolve_with_path(entries, codes, use_rust=True)
        python_results = _resolve_with_path(entries, codes, use_rust=False)

        assert rust_results[0].canonical_name == "Stereolab"
        assert rust_results[0].resolution_method == "fuzzy"
        assert python_results[0].canonical_name == "Stereolab"
        assert python_results[0].resolution_method == "fuzzy"


class TestReResolveWithPlayCountsParity:
    """Verify re_resolve_with_play_counts produces identical results with both paths."""

    def _make_raw_entries(self, artist_name: str, count: int) -> list[ResolvedEntry]:
        return [
            ResolvedEntry(
                entry=make_flowsheet_entry(id=i, library_release_id=0, artist_name=artist_name),
                canonical_name=artist_name.strip().lower(),
                resolution_method="raw",
            )
            for i in range(count)
        ]

    def test_re_resolve_parity(self):
        """Relaxed-threshold re-resolution produces identical results from both paths."""
        codes = _make_catalog_codes()

        # "fela kuti" → "Fela Anikulapo Kuti" (JW 0.837, above relaxed 0.82)
        raw_fela = self._make_raw_entries("Fela Kuti", count=15)
        # "buck meek" → no good match at relaxed threshold
        raw_buck = self._make_raw_entries("Buck Meek", count=12)
        # Mix in a non-raw entry
        catalog_entry = ResolvedEntry(
            entry=make_flowsheet_entry(id=100, artist_name="Autechre"),
            canonical_name="Autechre",
            resolution_method="catalog",
        )
        all_entries = [catalog_entry] + raw_fela + raw_buck

        env_key = "WXYC_ETL_NO_RUST"

        # Python path
        old_val = os.environ.get(env_key)
        try:
            os.environ[env_key] = "1"
            python_resolver = ArtistResolver(releases=[], codes=codes)
            python_result = python_resolver.re_resolve_with_play_counts(all_entries)
        finally:
            if old_val is None:
                os.environ.pop(env_key, None)
            else:
                os.environ[env_key] = old_val

        # Rust path
        try:
            os.environ.pop(env_key, None)
            rust_resolver = ArtistResolver(releases=[], codes=codes)
            rust_result = rust_resolver.re_resolve_with_play_counts(all_entries)
        finally:
            if old_val is None:
                os.environ.pop(env_key, None)
            else:
                os.environ[env_key] = old_val

        assert len(rust_result) == len(python_result)
        for rust, python in zip(rust_result, python_result, strict=True):
            assert rust.canonical_name == python.canonical_name, (
                f"entry {rust.entry.id}: Rust={rust.canonical_name!r} vs Python={python.canonical_name!r}"
            )
            assert rust.resolution_method == python.resolution_method, (
                f"entry {rust.entry.id}: Rust={rust.resolution_method!r} vs Python={python.resolution_method!r}"
            )
