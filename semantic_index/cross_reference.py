"""Extract cross-reference edges from library catalog tables.

Resolution chains:

LIBRARY_CODE_CROSS_REFERENCE:
    col 1 (CROSS_REFERENCING_ARTIST_ID) -> LIBRARY_CODE.ID -> PRESENTATION_NAME
    col 2 (CROSS_REFERENCED_LIBRARY_CODE_ID) -> LIBRARY_CODE.ID -> PRESENTATION_NAME
    col 3 (COMMENT) -> text

RELEASE_CROSS_REFERENCE:
    col 1 (CROSS_REFERENCING_ARTIST_ID) -> LIBRARY_CODE.ID -> PRESENTATION_NAME
    col 2 (CROSS_REFERENCED_RELEASE_ID) -> LIBRARY_RELEASE.ID -> LIBRARY_CODE_ID -> PRESENTATION_NAME
    col 3 (COMMENT) -> text

Note: Despite the column name CROSS_REFERENCING_ARTIST_ID, this column actually
joins to LIBRARY_CODE.ID (not ARTIST.ID). Confirmed by tubafrenzy Java source.
"""

import logging
from collections.abc import Iterable

from semantic_index.models import CrossReferenceEdge

logger = logging.getLogger(__name__)


class CrossReferenceExtractor:
    """Extracts cross-reference edges from catalog cross-reference tables.

    Args:
        codes: Mapping of library_code_id to presentation_name.
        release_to_code: Mapping of library_release_id to library_code_id.
    """

    def __init__(self, codes: dict[int, str], release_to_code: dict[int, int]) -> None:
        self._codes = codes
        self._release_to_code = release_to_code

    def extract_library_code_xrefs(self, rows: Iterable[tuple]) -> list[CrossReferenceEdge]:
        """Parse LIBRARY_CODE_CROSS_REFERENCE rows into edges.

        Each row is a tuple: (id, cross_referencing_artist_id, cross_referenced_library_code_id, comment).
        Both FK columns join to LIBRARY_CODE.ID.
        """
        edges: list[CrossReferenceEdge] = []
        for row in rows:
            row_id, code_a_id, code_b_id, comment = row[0], row[1], row[2], row[3]

            name_a = self._codes.get(code_a_id)
            if name_a is None:
                logger.warning(
                    "Skipping LIBRARY_CODE_CROSS_REFERENCE row %s: "
                    "unresolvable CROSS_REFERENCING_ARTIST_ID=%s",
                    row_id,
                    code_a_id,
                )
                continue

            name_b = self._codes.get(code_b_id)
            if name_b is None:
                logger.warning(
                    "Skipping LIBRARY_CODE_CROSS_REFERENCE row %s: "
                    "unresolvable CROSS_REFERENCED_LIBRARY_CODE_ID=%s",
                    row_id,
                    code_b_id,
                )
                continue

            if name_a == name_b:
                logger.warning(
                    "Skipping LIBRARY_CODE_CROSS_REFERENCE row %s: "
                    "self-referential (both resolve to %r)",
                    row_id,
                    name_a,
                )
                continue

            edges.append(
                CrossReferenceEdge(
                    artist_a=name_a,
                    artist_b=name_b,
                    comment=comment or "",
                    source="library_code",
                )
            )

        return edges

    def extract_release_xrefs(self, rows: Iterable[tuple]) -> list[CrossReferenceEdge]:
        """Parse RELEASE_CROSS_REFERENCE rows into edges.

        Each row is a tuple: (id, cross_referencing_artist_id, cross_referenced_release_id, comment).
        Col 1 joins to LIBRARY_CODE.ID directly.
        Col 2 chains through LIBRARY_RELEASE.ID -> LIBRARY_CODE_ID -> PRESENTATION_NAME.
        """
        edges: list[CrossReferenceEdge] = []
        for row in rows:
            row_id, code_a_id, release_b_id, comment = row[0], row[1], row[2], row[3]

            name_a = self._codes.get(code_a_id)
            if name_a is None:
                logger.warning(
                    "Skipping RELEASE_CROSS_REFERENCE row %s: "
                    "unresolvable CROSS_REFERENCING_ARTIST_ID=%s",
                    row_id,
                    code_a_id,
                )
                continue

            code_b_id = self._release_to_code.get(release_b_id)
            if code_b_id is None:
                logger.warning(
                    "Skipping RELEASE_CROSS_REFERENCE row %s: "
                    "unresolvable CROSS_REFERENCED_RELEASE_ID=%s",
                    row_id,
                    release_b_id,
                )
                continue

            name_b = self._codes.get(code_b_id)
            if name_b is None:
                logger.warning(
                    "Skipping RELEASE_CROSS_REFERENCE row %s: "
                    "CROSS_REFERENCED_RELEASE_ID=%s resolved to code_id=%s but code not found",
                    row_id,
                    release_b_id,
                    code_b_id,
                )
                continue

            if name_a == name_b:
                logger.warning(
                    "Skipping RELEASE_CROSS_REFERENCE row %s: "
                    "self-referential (both resolve to %r)",
                    row_id,
                    name_a,
                )
                continue

            edges.append(
                CrossReferenceEdge(
                    artist_a=name_a,
                    artist_b=name_b,
                    comment=comment or "",
                    source="release",
                )
            )

        return edges
