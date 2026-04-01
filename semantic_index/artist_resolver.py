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
from collections import Counter
from typing import TYPE_CHECKING

from rapidfuzz import process as rfprocess
from rapidfuzz.distance import JaroWinkler

from semantic_index.models import FlowsheetEntry, LibraryCode, LibraryRelease, ResolvedEntry

if TYPE_CHECKING:
    from semantic_index.discogs_client import DiscogsClient

logger = logging.getLogger(__name__)

# Minimum Jaro-Winkler similarity to accept a fuzzy match (0-1 scale).
# Set high to avoid false positives like "Autechre" → "Auteurs" (0.868).
FUZZY_MIN_SCORE = 0.90

# Relaxed threshold for names with sufficient play count.
# Names played 10+ times are almost certainly real artists, so a lower
# threshold is safe. Catches variants like "Fela Kuti" → "Fela Anikulapo Kuti" (0.837).
FUZZY_MIN_SCORE_RELAXED = 0.82

# Minimum play count to qualify for the relaxed fuzzy threshold.
FUZZY_RELAXED_MIN_PLAYS = 10

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

        # Fuzzy match: mapping from lowered candidate name to canonical name
        self._fuzzy_choices: dict[str, str] = dict(self._name_index)

        # Cache: lowered query → (canonical_name | None) per threshold
        self._fuzzy_cache: dict[tuple[str, float], str | None] = {}

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

    def _fuzzy_match(self, query: str, min_score: float = FUZZY_MIN_SCORE) -> str | None:
        """Find the best fuzzy match for a query string.

        Uses rapidfuzz.process.extract (C-accelerated batch scoring) and caches
        results so repeated queries for the same name are instant.

        Args:
            query: Lowercased artist name to match.
            min_score: Minimum Jaro-Winkler similarity to accept (default: FUZZY_MIN_SCORE).

        Returns the canonical name if a match exceeds the minimum score
        and passes the ambiguity guard. Returns None otherwise.
        """
        if not self._fuzzy_choices or not query:
            return None

        # Check cache
        cache_key = (query, min_score)
        if cache_key in self._fuzzy_cache:
            return self._fuzzy_cache[cache_key]

        # Use rapidfuzz batch API — scores all candidates in C, returns top N
        results = rfprocess.extract(
            query,
            self._fuzzy_choices.keys(),
            scorer=JaroWinkler.similarity,
            score_cutoff=min_score,
            limit=2,
        )

        if not results:
            self._fuzzy_cache[cache_key] = None
            return None

        best_key, best_score, _ = results[0]
        best_name = self._fuzzy_choices[best_key]

        # Ambiguity guard: if top two candidates are too close, reject
        if len(results) >= 2:
            _, second_score, _ = results[1]
            second_name = self._fuzzy_choices[results[1][0]]
            if best_score - second_score < FUZZY_AMBIGUITY_THRESHOLD and best_name != second_name:
                self._fuzzy_cache[cache_key] = None
                return None

        self._fuzzy_cache[cache_key] = best_name
        return best_name

    def re_resolve_with_play_counts(
        self,
        resolved: list[ResolvedEntry],
        min_plays: int = FUZZY_RELAXED_MIN_PLAYS,
        relaxed_threshold: float = FUZZY_MIN_SCORE_RELAXED,
    ) -> list[ResolvedEntry]:
        """Re-resolve raw entries whose name has enough plays using a relaxed fuzzy threshold.

        Names that appear frequently in the flowsheet are almost certainly real artists,
        so a lower Jaro-Winkler threshold is safe for them.

        Args:
            resolved: Entries from the first resolution pass.
            min_plays: Minimum raw-entry count to qualify for relaxed matching.
            relaxed_threshold: Jaro-Winkler threshold for qualifying names.

        Returns:
            A new list with qualifying raw entries re-resolved as ``"fuzzy_relaxed"``.
        """
        raw_counts: Counter[str] = Counter()
        for r in resolved:
            if r.resolution_method == "raw":
                raw_counts[r.canonical_name] += 1

        # Pre-compute fuzzy matches for qualifying names (once per unique name)
        relaxed_matches: dict[str, str | None] = {}
        for name, count in raw_counts.items():
            if count >= min_plays:
                relaxed_matches[name] = self._fuzzy_match(name, min_score=relaxed_threshold)

        matched = sum(1 for m in relaxed_matches.values() if m is not None)
        entries_resolved = sum(
            count for name, count in raw_counts.items() if relaxed_matches.get(name) is not None
        )
        logger.info(
            "Fuzzy relaxed: %d/%d eligible names matched, resolving %d entries",
            matched,
            len(relaxed_matches),
            entries_resolved,
        )

        result: list[ResolvedEntry] = []
        for r in resolved:
            if r.resolution_method == "raw" and r.canonical_name in relaxed_matches:
                match = relaxed_matches[r.canonical_name]
                if match is not None:
                    result.append(
                        ResolvedEntry(
                            entry=r.entry,
                            canonical_name=match,
                            resolution_method="fuzzy_relaxed",
                        )
                    )
                    continue
            result.append(r)
        return result

    def get_genre_id(self, library_release_id: int) -> int | None:
        """Look up the genre ID for a library release, or None if not found."""
        code_id = self._release_to_code.get(library_release_id)
        if code_id is None:
            return None
        return self._code_to_genre.get(code_id)
