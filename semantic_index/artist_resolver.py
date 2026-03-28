"""Artist name resolution via library catalog and Discogs.

Resolution strategies (in order of precedence):
1. FK chain: LIBRARY_RELEASE_ID → LIBRARY_RELEASE → LIBRARY_CODE → PRESENTATION_NAME
2. Name match: exact case-insensitive match against LIBRARY_CODE.PRESENTATION_NAME
3. Normalized match: strip "The ", "&" → "and", bracket removal, slash/aka alias splitting
4. Fuzzy match: Jaro-Winkler similarity against catalog names (with ambiguity guard)
5. Discogs: search via DiscogsClient (optional)
6. Raw: lowercased, stripped artist name as-is
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

from rapidfuzz.distance import JaroWinkler

from semantic_index.models import FlowsheetEntry, LibraryCode, LibraryRelease, ResolvedEntry

if TYPE_CHECKING:
    from semantic_index.discogs_client import DiscogsClient

logger = logging.getLogger(__name__)

# Minimum Jaro-Winkler similarity to accept a fuzzy match (0-1 scale).
# Set high to avoid false positives like "Autechre" → "Auteurs" (0.868).
FUZZY_MIN_SCORE = 0.90

# If the top two candidates differ by less than this, reject as ambiguous
FUZZY_AMBIGUITY_THRESHOLD = 0.02

_BRACKET_RE = re.compile(r"\s*\[.*?\]\s*$")


def _normalize(name: str) -> str:
    """Normalize an artist name for matching.

    Strips leading 'the ', normalizes '&' → 'and', removes trailing
    bracketed disambiguators like '[Scotland]'.
    """
    s = name.strip().lower()
    s = _BRACKET_RE.sub("", s)
    if s.startswith("the "):
        s = s[4:]
    s = s.replace(" & ", " and ")
    return s


def _normalized_forms(name: str) -> list[str]:
    """Generate all normalized forms of a catalog name for index matching.

    Returns the base normalized form plus alias parts from ' / ' and ' aka '
    separators. E.g., "J Dilla / Jay Dee" → ["j dilla / jay dee", "j dilla", "jay dee"].
    """
    base = _normalize(name)
    forms = [base]

    lowered = name.strip().lower()
    for sep in (" / ", " aka "):
        if sep in lowered:
            parts = lowered.split(sep)
            for part in parts:
                normalized_part = _normalize(part)
                if normalized_part and normalized_part != base:
                    forms.append(normalized_part)

    return forms


class ArtistResolver:
    """Resolves flowsheet artist names to canonical catalog names.

    Args:
        releases: LIBRARY_RELEASE rows (only id and library_code_id needed).
        codes: LIBRARY_CODE rows (only id, genre_id, and presentation_name needed).
        discogs_client: Optional DiscogsClient for Tier 3 resolution.
    """

    def __init__(
        self,
        releases: list[LibraryRelease],
        codes: list[LibraryCode],
        discogs_client: DiscogsClient | None = None,
    ) -> None:
        self._discogs_client = discogs_client
        self._release_to_code: dict[int, int] = {r.id: r.library_code_id for r in releases}
        self._code_to_name: dict[int, str] = {c.id: c.presentation_name for c in codes}
        self._code_to_genre: dict[int, int] = {c.id: c.genre_id for c in codes}

        # Exact name-match index: lowered name → canonical PRESENTATION_NAME
        self._name_index: dict[str, str] = {}
        for c in codes:
            key = c.presentation_name.strip().lower()
            if key not in self._name_index:
                self._name_index[key] = c.presentation_name

        # Normalized name-match index: normalized form → canonical name
        # Only stores unambiguous mappings (one canonical per normalized form)
        self._normalized_index: dict[str, str | None] = {}
        for c in codes:
            forms = _normalized_forms(c.presentation_name)
            for norm in forms:
                if norm in self._name_index:
                    continue  # exact match handles it
                if norm in self._normalized_index:
                    existing = self._normalized_index[norm]
                    if existing is not None and existing != c.presentation_name:
                        self._normalized_index[norm] = None  # ambiguous
                else:
                    self._normalized_index[norm] = c.presentation_name

        # Fuzzy match candidates: list of (lowered_name, canonical_name)
        self._fuzzy_candidates: list[tuple[str, str]] = [
            (key, name) for key, name in self._name_index.items()
        ]

    def resolve(self, entry: FlowsheetEntry) -> ResolvedEntry:
        """Resolve an entry's artist name to a canonical catalog name.

        Tries FK chain first, then exact name match, then normalized name match,
        then fuzzy match, then falls back to raw.
        """
        # Strategy 1: FK chain (LIBRARY_RELEASE_ID → LIBRARY_CODE → PRESENTATION_NAME)
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

        # Strategy 2: Exact name match against catalog
        key = entry.artist_name.strip().lower()
        matched_name = self._name_index.get(key)
        if matched_name is not None:
            return ResolvedEntry(
                entry=entry,
                canonical_name=matched_name,
                resolution_method="name_match",
            )

        # Strategy 3: Normalized name match (strip "The ", "&" → "and", remove brackets)
        norm = _normalize(entry.artist_name)
        # Check normalized form against both exact index and normalized index
        norm_match = self._name_index.get(norm) or self._normalized_index.get(norm)
        if norm_match is not None:
            return ResolvedEntry(
                entry=entry,
                canonical_name=norm_match,
                resolution_method="name_match",
            )

        # Strategy 4: Fuzzy match (Jaro-Winkler)
        fuzzy_result = self._fuzzy_match(key)
        if fuzzy_result is not None:
            return ResolvedEntry(
                entry=entry,
                canonical_name=fuzzy_result,
                resolution_method="fuzzy",
            )

        # Strategy 5: Discogs resolution (optional)
        if self._discogs_client is not None:
            discogs_result = self._discogs_client.search_artist(
                entry.artist_name.strip(), entry.release_title.strip() or None
            )
            if discogs_result is not None:
                return ResolvedEntry(
                    entry=entry,
                    canonical_name=discogs_result.artist_name,
                    resolution_method="discogs",
                )

        # Strategy 6: Raw fallback
        return ResolvedEntry(
            entry=entry,
            canonical_name=key,
            resolution_method="raw",
        )

    def _fuzzy_match(self, query: str) -> str | None:
        """Find the best fuzzy match for a query string.

        Returns the canonical name if a match exceeds the minimum score
        and passes the ambiguity guard. Returns None otherwise.
        """
        if not self._fuzzy_candidates or not query:
            return None

        scored: list[tuple[float, str]] = []
        for candidate_key, canonical_name in self._fuzzy_candidates:
            score = JaroWinkler.similarity(query, candidate_key)
            if score >= FUZZY_MIN_SCORE:
                scored.append((score, canonical_name))

        if not scored:
            return None

        scored.sort(key=lambda x: x[0], reverse=True)
        best_score, best_name = scored[0]

        # Ambiguity guard: if top two candidates are too close, reject
        if len(scored) >= 2:
            second_score, second_name = scored[1]
            if best_score - second_score < FUZZY_AMBIGUITY_THRESHOLD and best_name != second_name:
                return None

        return best_name

    def get_genre_id(self, library_release_id: int) -> int | None:
        """Look up the genre ID for a library release, or None if not found."""
        code_id = self._release_to_code.get(library_release_id)
        if code_id is None:
            return None
        return self._code_to_genre.get(code_id)
