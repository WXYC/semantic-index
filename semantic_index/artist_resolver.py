"""Tier 1 artist name resolution via library catalog FK chain.

Resolution chain: FLOWSHEET_ENTRY_PROD.LIBRARY_RELEASE_ID
    → LIBRARY_RELEASE.ID → LIBRARY_RELEASE.LIBRARY_CODE_ID
    → LIBRARY_CODE.PRESENTATION_NAME
"""

from semantic_index.models import FlowsheetEntry, LibraryCode, LibraryRelease, ResolvedEntry


class ArtistResolver:
    """Resolves flowsheet artist names to canonical catalog names.

    Args:
        releases: LIBRARY_RELEASE rows (only id and library_code_id needed).
        codes: LIBRARY_CODE rows (only id, genre_id, and presentation_name needed).
    """

    def __init__(
        self,
        releases: list[LibraryRelease],
        codes: list[LibraryCode],
    ) -> None:
        self._release_to_code: dict[int, int] = {r.id: r.library_code_id for r in releases}
        self._code_to_name: dict[int, str] = {c.id: c.presentation_name for c in codes}
        self._code_to_genre: dict[int, int] = {c.id: c.genre_id for c in codes}

    def resolve(self, entry: FlowsheetEntry) -> ResolvedEntry:
        """Resolve an entry's artist name via the catalog FK chain.

        Returns a ResolvedEntry with resolution_method="catalog" if the FK chain
        resolves, otherwise "raw" with a lowercased, stripped artist name.
        """
        if entry.library_release_id > 0:
            code_id = self._release_to_code.get(entry.library_release_id)
            if code_id is not None:
                name = self._code_to_name.get(code_id)
                if name is not None:
                    return ResolvedEntry(
                        entry=entry,
                        canonical_name=name,
                        resolution_method="catalog",
                    )

        return ResolvedEntry(
            entry=entry,
            canonical_name=entry.artist_name.strip().lower(),
            resolution_method="raw",
        )

    def get_genre_id(self, library_release_id: int) -> int | None:
        """Look up the genre ID for a library release, or None if not found."""
        code_id = self._release_to_code.get(library_release_id)
        if code_id is None:
            return None
        return self._code_to_genre.get(code_id)
