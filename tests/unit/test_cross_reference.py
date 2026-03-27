"""Tests for cross-reference edge extraction from catalog tables."""

from semantic_index.cross_reference import CrossReferenceExtractor
from tests.conftest import make_cross_reference_edge


class TestLibraryCodeCrossReference:
    """Tests for LIBRARY_CODE_CROSS_REFERENCE extraction."""

    def _make_extractor(self, codes=None, release_to_code=None):
        return CrossReferenceExtractor(
            codes=codes or {},
            release_to_code=release_to_code or {},
        )

    def test_both_endpoints_resolvable(self):
        codes = {200: "Autechre", 201: "Stereolab"}
        extractor = self._make_extractor(codes=codes)

        rows = [(1, 200, 201, "See also")]
        edges = extractor.extract_library_code_xrefs(rows)

        assert len(edges) == 1
        assert edges[0] == make_cross_reference_edge(
            artist_a="Autechre",
            artist_b="Stereolab",
            comment="See also",
            source="library_code",
        )

    def test_unresolvable_first_endpoint_skipped(self):
        codes = {201: "Stereolab"}
        extractor = self._make_extractor(codes=codes)

        rows = [(1, 999, 201, "See also")]
        edges = extractor.extract_library_code_xrefs(rows)

        assert len(edges) == 0

    def test_unresolvable_second_endpoint_skipped(self):
        codes = {200: "Autechre"}
        extractor = self._make_extractor(codes=codes)

        rows = [(1, 200, 999, "See also")]
        edges = extractor.extract_library_code_xrefs(rows)

        assert len(edges) == 0

    def test_self_referential_skipped(self):
        codes = {200: "Autechre"}
        extractor = self._make_extractor(codes=codes)

        rows = [(1, 200, 200, "See also")]
        edges = extractor.extract_library_code_xrefs(rows)

        assert len(edges) == 0

    def test_comment_preserved(self):
        codes = {200: "Autechre", 201: "Stereolab"}
        extractor = self._make_extractor(codes=codes)

        rows = [(1, 200, 201, "Similar electronic artists")]
        edges = extractor.extract_library_code_xrefs(rows)

        assert edges[0].comment == "Similar electronic artists"

    def test_null_comment_becomes_empty_string(self):
        codes = {200: "Autechre", 201: "Stereolab"}
        extractor = self._make_extractor(codes=codes)

        rows = [(1, 200, 201, None)]
        edges = extractor.extract_library_code_xrefs(rows)

        assert edges[0].comment == ""

    def test_multiple_rows(self):
        codes = {
            200: "Autechre",
            201: "Stereolab",
            202: "Cat Power",
        }
        extractor = self._make_extractor(codes=codes)

        rows = [
            (1, 200, 201, "See also"),
            (2, 201, 202, "Related"),
        ]
        edges = extractor.extract_library_code_xrefs(rows)

        assert len(edges) == 2
        assert edges[0].artist_a == "Autechre"
        assert edges[0].artist_b == "Stereolab"
        assert edges[1].artist_a == "Stereolab"
        assert edges[1].artist_b == "Cat Power"


class TestReleaseCrossReference:
    """Tests for RELEASE_CROSS_REFERENCE extraction."""

    def _make_extractor(self, codes=None, release_to_code=None):
        return CrossReferenceExtractor(
            codes=codes or {},
            release_to_code=release_to_code or {},
        )

    def test_chains_through_release_to_code(self):
        codes = {200: "Autechre", 201: "Jessica Pratt"}
        release_to_code = {300: 201}
        extractor = self._make_extractor(codes=codes, release_to_code=release_to_code)

        rows = [(1, 200, 300, "Collaboration")]
        edges = extractor.extract_release_xrefs(rows)

        assert len(edges) == 1
        assert edges[0] == make_cross_reference_edge(
            artist_a="Autechre",
            artist_b="Jessica Pratt",
            comment="Collaboration",
            source="release",
        )

    def test_unresolvable_release_id_skipped(self):
        codes = {200: "Autechre"}
        release_to_code = {}
        extractor = self._make_extractor(codes=codes, release_to_code=release_to_code)

        rows = [(1, 200, 999, "See also")]
        edges = extractor.extract_release_xrefs(rows)

        assert len(edges) == 0

    def test_unresolvable_release_code_skipped(self):
        codes = {200: "Autechre"}
        release_to_code = {300: 999}  # code 999 not in codes dict
        extractor = self._make_extractor(codes=codes, release_to_code=release_to_code)

        rows = [(1, 200, 300, "See also")]
        edges = extractor.extract_release_xrefs(rows)

        assert len(edges) == 0

    def test_unresolvable_first_endpoint_skipped(self):
        codes = {201: "Stereolab"}
        release_to_code = {300: 201}
        extractor = self._make_extractor(codes=codes, release_to_code=release_to_code)

        rows = [(1, 999, 300, "See also")]
        edges = extractor.extract_release_xrefs(rows)

        assert len(edges) == 0

    def test_self_referential_through_release_skipped(self):
        codes = {200: "Autechre"}
        release_to_code = {300: 200}  # release 300 belongs to Autechre (code 200)
        extractor = self._make_extractor(codes=codes, release_to_code=release_to_code)

        rows = [(1, 200, 300, "See also")]
        edges = extractor.extract_release_xrefs(rows)

        assert len(edges) == 0

    def test_null_comment_becomes_empty_string(self):
        codes = {200: "Autechre", 201: "Father John Misty"}
        release_to_code = {300: 201}
        extractor = self._make_extractor(codes=codes, release_to_code=release_to_code)

        rows = [(1, 200, 300, None)]
        edges = extractor.extract_release_xrefs(rows)

        assert edges[0].comment == ""

    def test_multiple_rows_with_mixed_results(self):
        codes = {200: "Autechre", 201: "Stereolab", 202: "Cat Power"}
        release_to_code = {300: 201, 301: 202}
        extractor = self._make_extractor(codes=codes, release_to_code=release_to_code)

        rows = [
            (1, 200, 300, "See also"),  # valid
            (2, 999, 301, "Related"),  # unresolvable first endpoint
            (3, 200, 301, "Collaboration"),  # valid
        ]
        edges = extractor.extract_release_xrefs(rows)

        assert len(edges) == 2
        assert edges[0].artist_a == "Autechre"
        assert edges[0].artist_b == "Stereolab"
        assert edges[1].artist_a == "Autechre"
        assert edges[1].artist_b == "Cat Power"
