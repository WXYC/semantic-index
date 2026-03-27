"""Compute per-artist node attributes from resolved flowsheet entries.

Extracts temporal range, DJ diversity, request ratio, and show count
for each artist from the flowsheet data.
"""

from collections import defaultdict
from datetime import UTC, datetime

from semantic_index.models import ArtistStats, ResolvedEntry


def compute_artist_stats(
    entries: list[ResolvedEntry],
    show_to_dj: dict[int, int | str],
    genre_names: dict[int, str],
    genre_for_release: dict[int, int] | None = None,
) -> dict[str, ArtistStats]:
    """Compute aggregated statistics for each artist.

    Args:
        entries: All resolved flowsheet entries.
        show_to_dj: Mapping from show_id to DJ identifier (int DJ_ID or str DJ_NAME).
        genre_names: Mapping from genre_id to genre name string.
        genre_for_release: Optional mapping from library_release_id to genre_id.
            If not provided, genre is not resolved.

    Returns:
        Dict mapping canonical artist name to ArtistStats.
    """
    if genre_for_release is None:
        genre_for_release = {}

    per_artist: dict[str, list[ResolvedEntry]] = defaultdict(list)
    for entry in entries:
        per_artist[entry.canonical_name].append(entry)

    result: dict[str, ArtistStats] = {}
    for name, artist_entries in per_artist.items():
        # Total plays
        total_plays = len(artist_entries)

        # Active years from timestamps
        years: list[int] = []
        for e in artist_entries:
            ts = e.entry.start_time
            if ts is not None and ts > 0:
                year = datetime.fromtimestamp(ts / 1000, tz=UTC).year
                years.append(year)

        active_first_year = min(years) if years else None
        active_last_year = max(years) if years else None

        # DJ count
        djs: set[int | str] = set()
        for e in artist_entries:
            dj = show_to_dj.get(e.entry.show_id)
            if dj is not None:
                djs.add(dj)
        dj_count = len(djs)

        # Request ratio
        request_count = sum(1 for e in artist_entries if e.entry.request_flag == 1)
        request_ratio = request_count / total_plays if total_plays > 0 else 0.0

        # Show count
        show_ids = {e.entry.show_id for e in artist_entries}
        show_count = len(show_ids)

        # Genre — use first resolvable library_release_id
        genre: str | None = None
        for e in artist_entries:
            lr_id = e.entry.library_release_id
            if lr_id > 0 and lr_id in genre_for_release:
                genre_id = genre_for_release[lr_id]
                genre = genre_names.get(genre_id)
                if genre is not None:
                    break

        result[name] = ArtistStats(
            canonical_name=name,
            total_plays=total_plays,
            genre=genre,
            active_first_year=active_first_year,
            active_last_year=active_last_year,
            dj_count=dj_count,
            request_ratio=request_ratio,
            show_count=show_count,
        )

    return result
