"""Match archive artist names to production canonical names.

The audio archive contains raw flowsheet artist strings — the same noisy
names DJs typed in. To attach a per-segment audio profile to the right row
in ``artist`` we have to fold across:

- case and diacritics (``Beyoncé`` ≡ ``beyonce``)
- HTML entities ("μ-Ziq" was logged as ``&#956;-Ziq`` in some DJ tools)
- nickname quoting (``"Weird Al" Yankovic`` ≡ ``Weird Al Yankovic``)
- shelving brackets (``Stereolab [UK]`` ≡ ``Stereolab``)
- "the " prefix and ``&`` ↔ ``and``
- multi-artist credits (``Foo, Bar`` → both ``Foo`` and ``Bar``)
- compilation/VA strings, which we drop on the floor

Any normalized form that resolves to more than one canonical name is treated
as ambiguous and returns no match — better an unattributed segment than a
wrong attribution.
"""

from __future__ import annotations

import html
from collections import defaultdict
from collections.abc import Mapping

from wxyc_etl.text import (  # type: ignore[import-untyped]
    is_compilation_artist,
    split_artist_name,
)

from semantic_index.artist_resolver import _normalize, _normalized_forms


def _decode_and_strip(name: str) -> str:
    """Decode HTML entities and strip wrapping whitespace and quote noise.

    Removes all ASCII double quotes (which usually wrap nicknames like
    ``"Weird Al"`` rather than being part of the name) and any literal
    backslash characters that leaked from JSON/SQL string escaping.
    """
    decoded = html.unescape(name)
    decoded = decoded.replace('\\"', '"').replace("\\", "")
    decoded = decoded.replace('"', "")
    return decoded.strip()


class ArchiveNameMatcher:
    """Resolve archive artist names to production ``artist.id`` values.

    Build once per aggregation run from the ``canonical_name → id`` mapping,
    then call ``resolve(name)`` per archive name.

    Args:
        canonical_to_id: Map of production ``canonical_name`` to ``artist.id``.
    """

    def __init__(self, canonical_to_id: Mapping[str, int]) -> None:
        self._canonical_to_id: dict[str, int] = dict(canonical_to_id)

        # normalized form → set of canonical names. Forms with multiple
        # canonicals are ambiguous and won't be used.
        self._normalized_index: dict[str, set[str]] = defaultdict(set)
        for canonical in self._canonical_to_id:
            for form in _normalized_forms(canonical):
                self._normalized_index[form].add(canonical)

        self.stats: dict[str, int] = {
            "exact": 0,
            "normalized": 0,
            "split": 0,
            "compilation_skip": 0,
            "unmatched": 0,
        }

    def resolve(self, archive_name: str) -> set[int]:
        """Return the set of production artist IDs for an archive name."""
        if archive_name in self._canonical_to_id:
            self.stats["exact"] += 1
            return {self._canonical_to_id[archive_name]}

        decoded = _decode_and_strip(archive_name)

        if is_compilation_artist(decoded):
            self.stats["compilation_skip"] += 1
            return set()

        # Whole-name normalized lookup
        norm = _normalize(decoded)
        ids = self._lookup_normalized(norm)
        if ids:
            self.stats["normalized"] += 1
            return ids

        # Multi-artist split — attribute to every part that matches unambiguously
        parts = split_artist_name(decoded)
        if parts:
            split_ids: set[int] = set()
            for part in parts:
                part_norm = _normalize(_decode_and_strip(part))
                split_ids |= self._lookup_normalized(part_norm)
            if split_ids:
                self.stats["split"] += 1
                return split_ids

        self.stats["unmatched"] += 1
        return set()

    def _lookup_normalized(self, norm: str) -> set[int]:
        """Look up a normalized form, refusing ambiguous matches."""
        if not norm:
            return set()
        canonicals = self._normalized_index.get(norm)
        if canonicals is None or len(canonicals) != 1:
            return set()
        canonical = next(iter(canonicals))
        return {self._canonical_to_id[canonical]}
