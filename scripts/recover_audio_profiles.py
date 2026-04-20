"""Recover audio profiles lost in the 36-dim → 59-dim migration.

Finds artists with MusicBrainz GIDs but no audio profile, resolves
GIDs to integer IDs via ``mb_artist``, fetches AcousticBrainz features,
builds 59-dim profiles, and recomputes acoustic similarity edges.

Uses the same atomic copy-and-swap pattern as ``nightly_sync.py``:
all mutations happen on a temp copy; the production file is untouched
until the final ``os.replace``.

Requires:
    - ``mb_artist.gid`` column in musicbrainz-cache PostgreSQL (#153)
    - ``ab_recording`` table populated via ``import_acousticbrainz.py``

Usage:
    python scripts/recover_audio_profiles.py \\
        --db-path data/wxyc_artist_graph.db \\
        --musicbrainz-cache-dsn postgresql://localhost/musicbrainz \\
        [--min-recordings 3] \\
        [--acoustic-similarity-threshold 0.85] \\
        [--dry-run]
"""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
import time
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_MIN_RECORDINGS = 3
DEFAULT_SIMILARITY_THRESHOLD = 0.85


def find_recovery_candidates(conn: sqlite3.Connection) -> list[tuple[int, str]]:
    """Find artists with MusicBrainz GIDs but no audio profile.

    Returns:
        List of (artist_id, musicbrainz_artist_id) tuples.
    """
    return conn.execute(
        "SELECT a.id, a.musicbrainz_artist_id "
        "FROM artist a "
        "WHERE a.musicbrainz_artist_id IS NOT NULL "
        "AND NOT EXISTS ("
        "  SELECT 1 FROM audio_profile ap WHERE ap.artist_id = a.id"
        ")"
    ).fetchall()


def recover(
    db_path: str,
    musicbrainz_cache_dsn: str,
    *,
    min_recordings: int = DEFAULT_MIN_RECORDINGS,
    similarity_threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
    dry_run: bool = False,
    rebuild_all: bool = False,
) -> dict[str, int]:
    """Run the audio profile recovery pipeline.

    Args:
        db_path: Path to the production SQLite database.
        musicbrainz_cache_dsn: PostgreSQL DSN for musicbrainz-cache.
        min_recordings: Minimum recordings to build a profile.
        similarity_threshold: Cosine similarity threshold for edges.
        dry_run: If True, write to temp but skip the atomic swap.
        rebuild_all: If True, clear existing profiles before finding candidates.

    Returns:
        Dict with recovery statistics (profiles_before, profiles_after,
        similarity_before, similarity_after, candidates, resolved, new_profiles).
    """
    from semantic_index.acousticbrainz import (
        build_audio_profiles_from_features,
        compute_acoustic_similarity,
        load_audio_profiles,
        store_audio_profiles,
    )
    from semantic_index.acousticbrainz_client import AcousticBrainzClient
    from semantic_index.musicbrainz_client import MusicBrainzClient
    from semantic_index.nightly_sync import (
        atomic_swap,
        checkpoint_and_close,
        prepare_working_db,
    )

    production_path = Path(db_path)
    temp_path = prepare_working_db(production_path)
    stats: dict[str, int] = {}

    try:
        conn = sqlite3.connect(str(temp_path))

        # Before counts
        stats["profiles_before"] = conn.execute("SELECT COUNT(*) FROM audio_profile").fetchone()[0]
        stats["similarity_before"] = conn.execute(
            "SELECT COUNT(*) FROM acoustic_similarity"
        ).fetchone()[0]

        # Clear existing profiles if rebuilding all (e.g. for dimension migration)
        if rebuild_all:
            conn.execute("DELETE FROM acoustic_similarity")
            conn.execute("DELETE FROM audio_profile")
            conn.commit()
            logger.info(
                "Cleared %d profiles and %d similarity edges for rebuild",
                stats["profiles_before"],
                stats["similarity_before"],
            )

        # Step 1: Find candidates
        candidates = find_recovery_candidates(conn)
        stats["candidates"] = len(candidates)
        logger.info("%d artists with MB GIDs but no audio profile", len(candidates))

        if not candidates:
            logger.info("No recovery candidates — nothing to do")
            conn.close()
            temp_path.unlink(missing_ok=True)
            stats.update(
                profiles_after=stats["profiles_before"],
                similarity_after=stats["similarity_before"],
                resolved=0,
                new_profiles=0,
            )
            return stats

        # Step 2: Resolve MB IDs → integer IDs
        # The musicbrainz_artist_id column contains a mix of formats:
        # - UUID/GID strings (from LML identity import): need resolution via mb_artist
        # - Integer strings (legacy, pre-LML): already are MB integer IDs
        graph_id_to_int: dict[int, int] = {}
        uuid_candidates: list[tuple[int, str]] = []

        for graph_id, mb_id in candidates:
            if mb_id.isdigit():
                graph_id_to_int[graph_id] = int(mb_id)
            else:
                uuid_candidates.append((graph_id, mb_id))

        logger.info(
            "%d legacy integer IDs, %d UUID GIDs to resolve",
            len(graph_id_to_int),
            len(uuid_candidates),
        )

        if uuid_candidates:
            mb_client = MusicBrainzClient(cache_dsn=musicbrainz_cache_dsn)
            gids = list({row[1] for row in uuid_candidates})
            gid_to_int = mb_client.resolve_gids_to_ids(gids)
            logger.info("Resolved %d/%d unique GIDs to integer IDs", len(gid_to_int), len(gids))

            for graph_id, gid in uuid_candidates:
                int_id = gid_to_int.get(gid)
                if int_id is not None:
                    graph_id_to_int[graph_id] = int_id

        stats["resolved"] = len(graph_id_to_int)

        if not graph_id_to_int:
            logger.warning("No MB IDs resolved — check mb_artist.gid column (#153)")
            conn.close()
            temp_path.unlink(missing_ok=True)
            stats.update(
                profiles_after=stats["profiles_before"],
                similarity_after=stats["similarity_before"],
                new_profiles=0,
            )
            return stats

        # Step 3: Map graph IDs to MB integer IDs
        int_to_graph_id = {v: k for k, v in graph_id_to_int.items()}
        mb_ids = list(graph_id_to_int.values())
        logger.info("%d candidate artists with resolved integer IDs", len(mb_ids))

        # Step 4: Fetch AcousticBrainz features
        ab_client = AcousticBrainzClient(cache_dsn=musicbrainz_cache_dsn)
        ab_features = ab_client.get_features_for_artists(mb_ids)
        total_recordings = sum(len(v) for v in ab_features.values())
        logger.info(
            "%d recordings with AB features across %d MB artists",
            total_recordings,
            len(ab_features),
        )

        # Step 5: Remap MB integer IDs → graph IDs
        artist_features: dict[int, list] = {}
        for mb_int, recordings in ab_features.items():
            artist_id = int_to_graph_id.get(mb_int)
            if artist_id is not None:
                artist_features[artist_id] = recordings

        # Step 6: Build and store new profiles
        new_profiles = build_audio_profiles_from_features(
            artist_features, min_recordings=min_recordings
        )
        stats["new_profiles"] = len(new_profiles)
        logger.info("%d new audio profiles built", len(new_profiles))

        if new_profiles:
            store_audio_profiles(conn, new_profiles)

        # Step 7: Load ALL profiles (existing + new) for similarity
        all_profiles = load_audio_profiles(conn)
        logger.info("%d total audio profiles for similarity computation", len(all_profiles))

        # Step 8: Recompute acoustic similarity for all profiles
        if all_profiles:
            similarity_count = compute_acoustic_similarity(
                conn, all_profiles, threshold=similarity_threshold
            )
            logger.info("%d acoustic similarity edges", similarity_count)

        # After counts
        stats["profiles_after"] = conn.execute("SELECT COUNT(*) FROM audio_profile").fetchone()[0]
        stats["similarity_after"] = conn.execute(
            "SELECT COUNT(*) FROM acoustic_similarity"
        ).fetchone()[0]

        conn.close()

        # Close PG connections
        if mb_client._cache_conn and not mb_client._cache_conn.closed:
            mb_client._cache_conn.close()
        if ab_client._conn and not ab_client._conn.closed:
            ab_client._conn.close()

        # Step 9: Checkpoint and swap
        checkpoint_and_close(str(temp_path))
        atomic_swap(temp_path, production_path, dry_run=dry_run)

        logger.info(
            "Recovery complete: %d → %d profiles, %d → %d similarity edges",
            stats["profiles_before"],
            stats["profiles_after"],
            stats["similarity_before"],
            stats["similarity_after"],
        )

    except Exception:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)
            logger.warning("Cleaned up temp file after failure")
        raise

    return stats


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Recover audio profiles lost in the 59-dim migration.",
    )
    parser.add_argument(
        "--db-path",
        default=os.environ.get("DB_PATH", "data/wxyc_artist_graph.db"),
        help="Production SQLite DB path (default: $DB_PATH or data/wxyc_artist_graph.db)",
    )
    parser.add_argument(
        "--musicbrainz-cache-dsn",
        default=os.environ.get("MUSICBRAINZ_CACHE_DSN"),
        help="PostgreSQL DSN for musicbrainz-cache (default: $MUSICBRAINZ_CACHE_DSN)",
    )
    parser.add_argument(
        "--min-recordings",
        type=int,
        default=DEFAULT_MIN_RECORDINGS,
        help=f"Minimum recordings per artist to build a profile (default: {DEFAULT_MIN_RECORDINGS})",
    )
    parser.add_argument(
        "--acoustic-similarity-threshold",
        type=float,
        default=DEFAULT_SIMILARITY_THRESHOLD,
        help=f"Cosine similarity threshold for edges (default: {DEFAULT_SIMILARITY_THRESHOLD})",
    )
    parser.add_argument(
        "--rebuild-all",
        action="store_true",
        help="Clear existing profiles and rebuild from scratch (e.g. for dimension migration)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run recovery but skip the atomic swap",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Entry point for the recovery script."""
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s [recover] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    if not args.musicbrainz_cache_dsn:
        logger.error("MUSICBRAINZ_CACHE_DSN not set and --musicbrainz-cache-dsn not provided")
        sys.exit(1)

    t0 = time.time()
    stats = recover(
        db_path=args.db_path,
        musicbrainz_cache_dsn=args.musicbrainz_cache_dsn,
        min_recordings=args.min_recordings,
        similarity_threshold=args.acoustic_similarity_threshold,
        dry_run=args.dry_run,
        rebuild_all=args.rebuild_all,
    )
    elapsed = time.time() - t0
    logger.info("Done in %.1fs: %s", elapsed, stats)


if __name__ == "__main__":
    main()
