#!/usr/bin/env python3
"""Pipeline CLI: extract adjacency pairs, cross-references, PMI, and Discogs enrichment.

Usage:
    python run_pipeline.py /path/to/wxycmusic.sql [--output-dir output/] [--min-count 2]
    python run_pipeline.py dump.sql --discogs-cache-dsn postgresql://... --api-base-url https://...
"""

import argparse
import logging
import os
import pickle
import sys
import time
from pathlib import Path

from semantic_index.adjacency import extract_adjacency_pairs
from semantic_index.artist_resolver import ArtistResolver
from semantic_index.cross_reference import CrossReferenceExtractor
from semantic_index.discogs_client import DiscogsClient
from semantic_index.discogs_edges import (
    extract_compilation_coappearance,
    extract_label_family,
    extract_shared_personnel,
    extract_shared_styles,
)
from semantic_index.discogs_enrichment import DiscogsEnricher
from semantic_index.entity_store import EntityStore
from semantic_index.graph_export import build_graph, export_gexf, print_top_neighbors
from semantic_index.label_hierarchy import populate_label_hierarchy
from semantic_index.models import (
    FlowsheetEntry,
    LibraryCode,
    LibraryRelease,
)
from semantic_index.node_attributes import compute_artist_stats
from semantic_index.pmi import compute_pmi
from semantic_index.reconciliation import ArtistReconciler
from semantic_index.sql_parser import iter_table_rows, load_table_rows
from semantic_index.sqlite_export import export_sqlite
from semantic_index.wikidata_client import WikidataClient

log = logging.getLogger(__name__)

# Genre ID → name mapping from the tubafrenzy GENRE table
GENRE_NAMES: dict[int, str] = {}

# Well-known WXYC artists to display top neighbors for (from wxyc-shared example data)
SPOTLIGHT_ARTISTS = [
    "Autechre",
    "Stereolab",
    "Cat Power",
    "Father John Misty",
    "Jessica Pratt",
    "Duke Ellington & John Coltrane",
    "Juana Molina",
    "Large Professor",
    "Prince Jammy",
    "Sessa",
]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute PMI artist graph from a tubafrenzy SQL dump.",
    )
    parser.add_argument("dump_path", help="Path to the MySQL dump file (.sql)")
    parser.add_argument("--output-dir", default="output", help="Directory for GEXF output")
    parser.add_argument(
        "--min-count",
        type=int,
        default=2,
        help="Minimum co-occurrence count for graph edges (default: 2)",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    parser.add_argument("--no-sqlite", action="store_true", help="Skip SQLite database export")
    parser.add_argument(
        "--discogs-cache-dsn",
        default=os.environ.get("DATABASE_URL_DISCOGS"),
        help="PostgreSQL DSN for discogs-cache (default: DATABASE_URL_DISCOGS env var)",
    )
    parser.add_argument(
        "--api-base-url",
        default="https://library-metadata-lookup-staging.up.railway.app",
        help="Base URL for library-metadata-lookup API",
    )
    parser.add_argument(
        "--wikidata-cache-dsn",
        default=os.environ.get("DATABASE_URL_WIKIDATA"),
        help="PostgreSQL DSN for wikidata-cache (default: DATABASE_URL_WIKIDATA env var)",
    )
    parser.add_argument(
        "--musicbrainz-cache-dsn",
        default=os.environ.get("DATABASE_URL_MUSICBRAINZ"),
        help="PostgreSQL DSN for musicbrainz-cache (default: DATABASE_URL_MUSICBRAINZ env var)",
    )
    parser.add_argument("--skip-enrichment", action="store_true", help="Skip Discogs enrichment")
    parser.add_argument(
        "--min-jaccard", type=float, default=0.1, help="Minimum Jaccard for shared style edges"
    )
    parser.add_argument(
        "--max-label-artists",
        type=int,
        default=500,
        help="Exclude labels with more than N artists from label family edges",
    )
    parser.add_argument(
        "--max-style-artists",
        type=int,
        default=200,
        help="Exclude styles shared by more than N artists (default: 200)",
    )
    parser.add_argument(
        "--max-personnel-artists",
        type=int,
        default=200,
        help="Exclude personnel credited on more than N artists (default: 200)",
    )
    parser.add_argument(
        "--cache-dir",
        default=None,
        help="Cache resolved entries to skip SQL parsing on reruns. "
        "Uses dump file size+mtime as cache key.",
    )
    parser.add_argument(
        "--entity-store-path",
        default=None,
        help="Path to an entity store SQLite database. Enables entity store mode: "
        "artists are managed by the entity store rather than created fresh.",
    )
    parser.add_argument(
        "--skip-reconciliation",
        action="store_true",
        help="Skip Discogs reconciliation step (requires --entity-store-path)",
    )
    parser.add_argument(
        "--wikidata-reconciliation",
        action="store_true",
        help="Search Wikidata by name for artists with no Discogs match. "
        "Filtered to musicians (P31=human/musical group, P106=musician). "
        "Requires --entity-store-path.",
    )
    parser.add_argument(
        "--compute-discogs-edges",
        action="store_true",
        help="Compute Discogs-derived edges (shared personnel, styles, labels, compilations). "
        "Off by default.",
    )
    parser.add_argument(
        "--populate-label-hierarchy",
        action="store_true",
        help="Populate label and label_hierarchy tables from Wikidata P749/P355. "
        "Requires --entity-store-path and enrichment data.",
    )
    parser.add_argument(
        "--compute-wikidata-influences",
        action="store_true",
        help="Query Wikidata P737 (influenced by) and create directed influence edges. "
        "Requires --entity-store-path with reconciled Wikidata QIDs.",
    )
    parser.add_argument(
        "--fetch-streaming-ids",
        action="store_true",
        help="Fetch Spotify/Apple Music/Bandcamp IDs from Wikidata (P1902/P2850/P3283). "
        "Requires --entity-store-path with reconciled Wikidata QIDs.",
    )
    parser.add_argument(
        "--facet-only",
        action="store_true",
        help="Only export facet tables (requires --cache-dir and an existing database). "
        "Loads resolved entries from cache, recomputes adjacency pairs, reads artist IDs "
        "from the existing database, and calls export_facet_tables. Skips all other steps.",
    )
    parser.add_argument(
        "--acousticbrainz-dir",
        default=os.environ.get("ACOUSTICBRAINZ_DIR"),
        help="Path to extracted AcousticBrainz data dump. "
        "Requires --musicbrainz-cache-dsn for recording MBID resolution.",
    )
    parser.add_argument(
        "--min-recordings",
        type=int,
        default=3,
        help="Minimum recordings per artist for audio profile (default: 3)",
    )
    parser.add_argument(
        "--acoustic-similarity-threshold",
        type=float,
        default=0.85,
        help="Minimum cosine similarity for acoustic similarity edges (default: 0.85)",
    )
    return parser.parse_args(argv)


def _run_facet_only(
    args: argparse.Namespace,
    cache_path: Path | None,
    used_cache: bool,
) -> None:
    """Run only the facet export step using cached data and an existing database.

    Validates that the cache was loaded and the target database exists, then
    recomputes adjacency pairs, reads artist IDs from the database, and
    exports facet tables.
    """
    import sqlite3 as _sqlite3

    from semantic_index.facet_export import export_facet_tables

    t0 = time.time()

    # Validate --cache-dir was provided
    if not args.cache_dir:
        log.error("--facet-only requires --cache-dir")
        sys.exit(1)

    # Validate the cache was found and loaded
    if not used_cache:
        log.error(
            "--facet-only: no cache file found at %s. "
            "Run the full pipeline first to generate the cache.",
            cache_path,
        )
        sys.exit(1)

    # Determine target database path
    if args.entity_store_path:
        sqlite_path = Path(args.entity_store_path)
    else:
        sqlite_path = Path(args.output_dir) / "wxyc_artist_graph.db"

    if not sqlite_path.exists():
        log.error(
            "--facet-only: target database not found at %s. "
            "Run the full pipeline first to generate the database.",
            sqlite_path,
        )
        sys.exit(1)

    # The cache was already loaded and validated by the caller, but those
    # values are local to run(). Re-read from the cache file directly.
    log.info("Loading cache for facet-only export...")
    with open(cache_path, "rb") as f:  # type: ignore[arg-type]
        cache = pickle.load(f)  # noqa: S301
    resolved_entries = cache["resolved_entries"]
    show_to_dj = cache["show_to_dj"]
    show_dj_names = cache.get("show_dj_names", {})

    # Recompute adjacency pairs
    log.info("Extracting adjacency pairs from %d resolved entries...", len(resolved_entries))
    pairs = extract_adjacency_pairs(resolved_entries)
    log.info("  %d adjacency pairs extracted", len(pairs))

    # Read name_to_id from the existing database
    log.info("Reading artist IDs from %s...", sqlite_path)
    conn = _sqlite3.connect(str(sqlite_path))
    conn.row_factory = _sqlite3.Row
    name_to_id = {
        r["canonical_name"]: r["id"]
        for r in conn.execute("SELECT id, canonical_name FROM artist").fetchall()
    }
    conn.close()
    log.info("  %d artists in database", len(name_to_id))

    # Export facet tables
    log.info("Exporting facet tables to %s...", sqlite_path)
    export_facet_tables(
        db_path=str(sqlite_path),
        resolved_entries=resolved_entries,
        name_to_id=name_to_id,
        show_to_dj=show_to_dj,
        show_dj_names=show_dj_names,
        adjacency_pairs=pairs,
    )

    elapsed = time.time() - t0
    log.info("Facet tables written to %s in %.1f seconds.", sqlite_path, elapsed)


def run(args: argparse.Namespace) -> None:
    dump_path = args.dump_path
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not Path(dump_path).exists():
        log.error("Dump file not found: %s", dump_path)
        sys.exit(1)

    t0 = time.time()

    # Check for cached resolved entries
    _cache_path = None
    if args.cache_dir:
        cache_dir = Path(args.cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
        dump_stat = Path(dump_path).stat()
        _cache_key = f"{dump_stat.st_size}_{int(dump_stat.st_mtime)}"
        _cache_path = cache_dir / f"resolved_{_cache_key}.pkl"

    _used_cache = False
    if _cache_path and _cache_path.exists():
        log.info("Loading cached resolved entries from %s", _cache_path)
        with open(_cache_path, "rb") as _f:
            _cache = pickle.load(_f)  # noqa: S301
        resolved_entries = _cache["resolved_entries"]
        GENRE_NAMES.update(_cache["genre_names"])
        codes = _cache["codes"]
        releases = _cache["releases"]
        show_to_dj = _cache["show_to_dj"]
        show_dj_names = _cache.get("show_dj_names", {})
        total_entries = _cache["total_entries"]
        music_entries = _cache["music_entries"]
        catalog_resolved = _cache["catalog_resolved"]
        log.info("  Loaded %d resolved entries from cache", len(resolved_entries))
        _used_cache = True

    # --facet-only: export facet tables from cache + existing DB and exit early
    if args.facet_only:
        _run_facet_only(args, _cache_path, _used_cache)
        return

    if not _used_cache:
        # 1. Parse genre table
        log.info("Parsing GENRE table...")
        for row in iter_table_rows(dump_path, "GENRE"):
            GENRE_NAMES[row[0]] = row[1]
        log.info("  %d genres loaded", len(GENRE_NAMES))

        # 2. Parse library tables
        log.info("Parsing LIBRARY_RELEASE table...")
        release_rows = load_table_rows(dump_path, "LIBRARY_RELEASE")
        releases = [LibraryRelease(id=r[0], library_code_id=r[8]) for r in release_rows]
        log.info("  %d releases loaded", len(releases))

        log.info("Parsing LIBRARY_CODE table...")
        code_rows = load_table_rows(dump_path, "LIBRARY_CODE")
        codes = [LibraryCode(id=r[0], genre_id=r[1], presentation_name=r[7]) for r in code_rows]
        log.info("  %d library codes loaded", len(codes))

        # 3. Parse radio shows for DJ mapping
        log.info("Parsing FLOWSHEET_RADIO_SHOW_PROD table...")
        show_to_dj: dict[int, int | str] = {}
        show_dj_names: dict[int, str] = {}
        for row in iter_table_rows(dump_path, "FLOWSHEET_RADIO_SHOW_PROD"):
            show_id = row[0]
            dj_id = row[3]  # DJ_ID (int or None)
            dj_name = row[2] or ""  # DJ_NAME (str)
            if isinstance(dj_id, int) and dj_id > 0:
                show_to_dj[show_id] = dj_id
            elif dj_name:
                show_to_dj[show_id] = dj_name
            if dj_name:
                show_dj_names[show_id] = dj_name
        log.info("  %d shows with DJ mapping", len(show_to_dj))

        # 4. Build resolver
        resolver = ArtistResolver(releases=releases, codes=codes)

        # 5. Stream flowsheet entries and resolve
        log.info("Parsing FLOWSHEET_ENTRY_PROD and resolving artists...")
        resolved_entries = []
        total_entries = 0
        music_entries = 0
        catalog_resolved = 0

        for row in iter_table_rows(dump_path, "FLOWSHEET_ENTRY_PROD"):
            total_entries += 1
            entry_type_code = row[15]

            if total_entries % 100_000 == 0:
                log.info("  ... %d entries processed", total_entries)

            # Filter to music entries only (type code < 7)
            if not isinstance(entry_type_code, int) or entry_type_code >= 7:
                continue

            try:
                start_time_raw = row[10]
                request_flag_raw = row[18]
                entry = FlowsheetEntry(
                    id=row[0],
                    artist_name=row[1] or "",
                    song_title=row[3] or "",
                    release_title=row[4] or "",
                    library_release_id=row[6] if isinstance(row[6], int) else 0,
                    label_name=row[8] or "",
                    show_id=row[12] if isinstance(row[12], int) else 0,
                    sequence=row[13] if isinstance(row[13], int) else 0,
                    entry_type_code=entry_type_code,
                    request_flag=request_flag_raw if isinstance(request_flag_raw, int) else 0,
                    start_time=start_time_raw if isinstance(start_time_raw, int) else None,
                )
            except Exception:
                log.debug("Skipping unparseable row ID=%s", row[0], exc_info=True)
                continue

            music_entries += 1
            resolved = resolver.resolve(entry)
            if resolved.resolution_method == "catalog":
                catalog_resolved += 1
            resolved_entries.append(resolved)

        log.info(
            "  %d total entries, %d music entries, %d catalog-resolved (%.1f%%)",
            total_entries,
            music_entries,
            catalog_resolved,
            (catalog_resolved / music_entries * 100) if music_entries else 0,
        )

        # 5b. Re-resolve raw entries with sufficient play count using relaxed fuzzy threshold
        log.info("Re-resolving raw entries with play-count-weighted fuzzy matching...")
        resolved_entries = resolver.re_resolve_with_play_counts(resolved_entries)

        # Save to cache for future runs
        if _cache_path:
            log.info("Saving resolved entries to cache: %s", _cache_path)
            with open(_cache_path, "wb") as _f:
                pickle.dump(
                    {
                        "resolved_entries": resolved_entries,
                        "genre_names": dict(GENRE_NAMES),
                        "codes": codes,
                        "releases": releases,
                        "show_to_dj": show_to_dj,
                        "show_dj_names": show_dj_names,
                        "total_entries": total_entries,
                        "music_entries": music_entries,
                        "catalog_resolved": catalog_resolved,
                    },
                    _f,
                )

    # 5c. Entity store setup (optional)
    entity_store: EntityStore | None = None
    dedup_report = None
    if args.entity_store_path:
        store_path = args.entity_store_path
        log.info("Opening entity store: %s", store_path)
        entity_store = EntityStore(store_path)
        entity_store.initialize()

        # Bulk upsert all resolved artist names
        all_canonical = list(dict.fromkeys(e.canonical_name for e in resolved_entries))
        log.info("Bulk upserting %d artists into entity store...", len(all_canonical))
        entity_store.bulk_upsert_artists(all_canonical)

        # 5d. Reconcile via Discogs (unless skipped or no cache DSN)
        reconcile_client = DiscogsClient(
            cache_dsn=args.discogs_cache_dsn,
            api_base_url=None,
        )
        reconciler = ArtistReconciler(entity_store, reconcile_client)

        if not args.skip_reconciliation and args.discogs_cache_dsn:
            log.info("Running Discogs reconciliation...")
            report = reconciler.reconcile_batch()
            log.info(
                "Reconciliation: %d attempted, %d succeeded, %d no_match, %d errored",
                report.attempted,
                report.succeeded,
                report.no_match,
                report.errored,
            )
            # Re-try no_match artists against member/group table
            member_report = reconciler.reconcile_members()
            log.info(
                "Member reconciliation: %d attempted, %d succeeded, %d no_match, %d errored",
                member_report.attempted,
                member_report.succeeded,
                member_report.no_match,
                member_report.errored,
            )
        elif not args.skip_reconciliation:
            log.warning("Skipping reconciliation: no discogs-cache DSN available")

        # 5e. Wikidata name search for remaining no_match artists (opt-in)
        if args.wikidata_reconciliation:
            log.info("Running Wikidata name search for no_match artists...")
            wikidata_client = WikidataClient(cache_dsn=args.wikidata_cache_dsn)
            wikidata_report = reconciler.reconcile_wikidata(wikidata_client)
            log.info(
                "Wikidata reconciliation: %d attempted, %d succeeded, %d no_match, %d errored",
                wikidata_report.attempted,
                wikidata_report.succeeded,
                wikidata_report.no_match,
                wikidata_report.errored,
            )

        # 5e2. Assign Wikidata QIDs from wikidata-cache via Discogs ID bridge
        if args.wikidata_cache_dsn:
            log.info("Assigning Wikidata QIDs from wikidata-cache via Discogs IDs...")
            try:
                import psycopg as _pg

                wikidata_conn = _pg.connect(args.wikidata_cache_dsn, autocommit=True)
                # Get all artists with Discogs IDs — use LEFT JOIN since
                # some artists may not have an entity yet
                unlinked = entity_store._conn.execute(
                    "SELECT a.id, a.discogs_artist_id, a.entity_id FROM artist a "
                    "LEFT JOIN entity e ON a.entity_id = e.id "
                    "WHERE a.discogs_artist_id IS NOT NULL "
                    "AND (a.entity_id IS NULL OR e.wikidata_qid IS NULL)"
                ).fetchall()
                if unlinked:
                    discogs_ids = [str(row[1]) for row in unlinked]
                    artist_by_discogs = {str(row[1]): (row[0], row[2]) for row in unlinked}
                    # Batch lookup in wikidata-cache
                    wk_rows = wikidata_conn.execute(
                        "SELECT dm.discogs_id, dm.qid FROM discogs_mapping dm "
                        "WHERE dm.property = 'P1953' AND dm.discogs_id = ANY(%s)",
                        (discogs_ids,),
                    ).fetchall()
                    qid_assigned = 0
                    now = "strftime('%Y-%m-%dT%H:%M:%SZ', 'now')"
                    for discogs_id_str, qid in wk_rows:
                        match = artist_by_discogs.get(discogs_id_str)
                        if match is None:
                            continue
                        artist_id, entity_id = match
                        if entity_id is not None:
                            entity_store._conn.execute(
                                "UPDATE entity SET wikidata_qid = ?, "
                                f"updated_at = {now} "
                                "WHERE id = ? AND wikidata_qid IS NULL",
                                (qid, entity_id),
                            )
                        else:
                            # Create entity with QID and link to artist
                            artist_name = entity_store._conn.execute(
                                "SELECT canonical_name FROM artist WHERE id = ?",
                                (artist_id,),
                            ).fetchone()[0]
                            cur = entity_store._conn.execute(
                                "INSERT INTO entity (name, entity_type, wikidata_qid, "
                                f"created_at, updated_at) VALUES (?, 'artist', ?, {now}, {now})",
                                (artist_name, qid),
                            )
                            new_entity_id = cur.lastrowid
                            entity_store._conn.execute(
                                "UPDATE artist SET entity_id = ? WHERE id = ?",
                                (new_entity_id, artist_id),
                            )
                        qid_assigned += 1
                    entity_store._conn.commit()
                    log.info(
                        "  %d/%d artists assigned Wikidata QIDs via Discogs ID bridge",
                        qid_assigned,
                        len(unlinked),
                    )
                else:
                    log.info("  No artists with Discogs IDs lacking QIDs")
                wikidata_conn.close()
            except Exception:
                log.warning("Wikidata QID assignment failed", exc_info=True)

        # 5e3a. MusicBrainz ID bridge via Wikidata (Discogs ID → QID → P434)
        if args.wikidata_cache_dsn:
            log.info("Assigning MusicBrainz IDs from wikidata-cache via QID bridge...")
            try:
                import psycopg as _pg2

                wk_conn = _pg2.connect(args.wikidata_cache_dsn, autocommit=True)
                # Get artists with Discogs IDs but no MusicBrainz ID
                need_mb = entity_store._conn.execute(
                    "SELECT a.id, a.discogs_artist_id FROM artist a "
                    "WHERE a.discogs_artist_id IS NOT NULL "
                    "AND a.musicbrainz_artist_id IS NULL"
                ).fetchall()
                if need_mb:
                    discogs_ids = [str(row[1]) for row in need_mb]
                    artist_id_by_discogs = {str(row[1]): row[0] for row in need_mb}
                    # Join: Discogs ID → QID via P1953, then QID → MB ID via P434
                    wk_rows = wk_conn.execute(
                        "SELECT d.discogs_id, m.discogs_id AS mb_id "
                        "FROM discogs_mapping d "
                        "JOIN discogs_mapping m ON d.qid = m.qid AND m.property = 'P434' "
                        "WHERE d.property = 'P1953' AND d.discogs_id = ANY(%s)",
                        (discogs_ids,),
                    ).fetchall()
                    mb_bridged = 0
                    for discogs_id_str, mb_uuid in wk_rows:
                        artist_id = artist_id_by_discogs.get(discogs_id_str)
                        if artist_id is not None:
                            entity_store._conn.execute(
                                "UPDATE artist SET musicbrainz_artist_id = ? "
                                "WHERE id = ? AND musicbrainz_artist_id IS NULL",
                                (mb_uuid, artist_id),
                            )
                            mb_bridged += 1
                    entity_store._conn.commit()
                    log.info(
                        "  MusicBrainz via Wikidata bridge: %d/%d artists",
                        mb_bridged,
                        len(need_mb),
                    )
                wk_conn.close()
            except Exception:
                log.warning("MusicBrainz Wikidata bridge failed", exc_info=True)

        # 5e3b. MusicBrainz name matching for remaining unmatched artists
        if args.musicbrainz_cache_dsn:
            from semantic_index.musicbrainz_client import MusicBrainzClient

            log.info("Running MusicBrainz name matching for remaining artists...")
            mb_client = MusicBrainzClient(cache_dsn=args.musicbrainz_cache_dsn)
            unmatched = entity_store._conn.execute(
                "SELECT id, canonical_name FROM artist WHERE musicbrainz_artist_id IS NULL"
            ).fetchall()
            if unmatched:
                names = [row[1] for row in unmatched]
                id_by_name = {row[1].lower(): row[0] for row in unmatched}
                matches = mb_client.batch_lookup(names)
                mb_matched = 0
                for lower_name, (mb_id, _mb_name) in matches.items():
                    artist_id = id_by_name.get(lower_name)
                    if artist_id is not None:
                        entity_store._conn.execute(
                            "UPDATE artist SET musicbrainz_artist_id = ? "
                            "WHERE id = ? AND musicbrainz_artist_id IS NULL",
                            (str(mb_id), artist_id),
                        )
                        mb_matched += 1
                entity_store._conn.commit()
                log.info(
                    "  MusicBrainz name matching: %d/%d remaining artists matched",
                    mb_matched,
                    len(unmatched),
                )

        # 5f. Entity deduplication by shared QID (runs after all reconciliation)
        dedup_report = entity_store.deduplicate_by_qid()
        if dedup_report.groups_found > 0:
            log.info(
                "Entity deduplication: %d groups, %d entities merged, %d artists reassigned",
                dedup_report.groups_found,
                dedup_report.entities_merged,
                dedup_report.artists_reassigned,
            )

        # 5g. Fetch streaming IDs from Wikidata (opt-in)
        if args.fetch_streaming_ids:
            log.info("Fetching streaming IDs from Wikidata (P1902/P2850/P3283)...")
            streaming_wikidata = WikidataClient()
            updated = reconciler.reconcile_streaming_ids(streaming_wikidata)
            log.info("  Updated streaming IDs for %d entities", updated)

    if args.fetch_streaming_ids and entity_store is None:
        log.warning("--fetch-streaming-ids requires --entity-store-path")

    # 6. Extract adjacency pairs
    log.info("Extracting adjacency pairs...")
    pairs = extract_adjacency_pairs(resolved_entries)
    log.info("  %d adjacency pairs extracted", len(pairs))

    # 7. Compute PMI
    log.info("Computing PMI...")
    edges = compute_pmi(pairs, resolved_entries)
    log.info("  %d unique edges computed", len(edges))

    # 8. Build artist stats
    code_to_genre = {c.id: c.genre_id for c in codes}
    genre_for_release: dict[int, int] = {}
    for r in releases:
        genre_id = code_to_genre.get(r.library_code_id)
        if genre_id is not None:
            genre_for_release[r.id] = genre_id

    log.info("Computing artist stats...")
    artist_stats = compute_artist_stats(
        resolved_entries, show_to_dj, GENRE_NAMES, genre_for_release=genre_for_release
    )

    # 9. Extract cross-reference edges
    log.info("Extracting cross-reference edges...")
    code_names = {c.id: c.presentation_name for c in codes}
    release_to_code = {r.id: r.library_code_id for r in releases}
    xref_extractor = CrossReferenceExtractor(codes=code_names, release_to_code=release_to_code)

    lc_xref_rows = load_table_rows(dump_path, "LIBRARY_CODE_CROSS_REFERENCE")
    lc_xrefs = xref_extractor.extract_library_code_xrefs(lc_xref_rows)
    log.info("  %d library code cross-reference edges", len(lc_xrefs))

    rel_xref_rows = load_table_rows(dump_path, "RELEASE_CROSS_REFERENCE")
    rel_xrefs = xref_extractor.extract_release_xrefs(rel_xref_rows)
    log.info("  %d release cross-reference edges", len(rel_xrefs))

    xref_edges = lc_xrefs + rel_xrefs

    # 10. Discogs enrichment (optional)
    enrichments = {}
    sp_edges = []
    ss_edges = []
    lf_edges = []
    comp_edges = []
    influence_edges = []
    lh_report = None

    if not args.skip_enrichment and (args.discogs_cache_dsn or args.api_base_url):
        log.info("Setting up Discogs client...")
        discogs_client = DiscogsClient(
            cache_dsn=args.discogs_cache_dsn,
            api_base_url=args.api_base_url,
        )

        # Enrich all canonical artists
        log.info("Enriching artists with Discogs metadata...")
        enricher = DiscogsEnricher(discogs_client)
        artist_ids = dict.fromkeys(artist_stats)
        enrichments = enricher.enrich_batch(artist_ids)
        log.info("  %d artists enriched", len(enrichments))

        # Extract Discogs-derived edges (only when explicitly requested)
        if args.compute_discogs_edges:
            if args.discogs_cache_dsn and discogs_client._has_summary_tables(
                discogs_client._get_cache_conn()
            ):
                # Fast SQL path: push self-joins to PostgreSQL
                log.info("Extracting Discogs-derived edges via SQL...")
                artist_names_lower = list(enrichments.keys())
                sp_edges = discogs_client.compute_shared_personnel_sql(
                    artist_names_lower,
                    max_artists=args.max_personnel_artists,
                )
                log.info("  %d shared personnel edges", len(sp_edges))
                ss_edges = discogs_client.compute_shared_styles_sql(
                    artist_names_lower,
                    min_jaccard=args.min_jaccard,
                    max_artists=args.max_style_artists,
                )
                log.info("  %d shared style edges", len(ss_edges))
                lf_edges = discogs_client.compute_label_family_sql(
                    artist_names_lower,
                    max_label_artists=args.max_label_artists,
                )
                log.info("  %d label family edges", len(lf_edges))
                comp_edges = discogs_client.compute_compilation_sql(artist_names_lower)
                log.info("  %d compilation edges", len(comp_edges))
            else:
                # Python fallback with frequency caps
                log.info("Extracting Discogs-derived edges (Python)...")
                sp_edges = extract_shared_personnel(
                    enrichments, max_artists=args.max_personnel_artists
                )
                log.info("  %d shared personnel edges", len(sp_edges))
                ss_edges = extract_shared_styles(
                    enrichments,
                    min_jaccard=args.min_jaccard,
                    max_artists=args.max_style_artists,
                )
                log.info("  %d shared style edges", len(ss_edges))
                lf_edges = extract_label_family(
                    enrichments, max_label_artists=args.max_label_artists
                )
                log.info("  %d label family edges", len(lf_edges))
                comp_edges = extract_compilation_coappearance(enrichments)
                log.info("  %d compilation edges", len(comp_edges))
    elif not args.skip_enrichment:
        log.warning("Skipping Discogs enrichment: no cache DSN or API URL available")

    # 10b. Label hierarchy from Wikidata (optional, requires entity store + enrichments)
    if args.populate_label_hierarchy and entity_store is not None and enrichments:
        log.info("Populating label hierarchy from Wikidata P749/P355...")
        wikidata_client = WikidataClient(cache_dsn=args.wikidata_cache_dsn)
        lh_report = populate_label_hierarchy(entity_store, enrichments, wikidata_client)
        log.info(
            "  %d labels created, %d matched to Wikidata, %d hierarchy edges",
            lh_report.labels_created,
            lh_report.labels_matched,
            lh_report.hierarchy_edges,
        )
    elif args.populate_label_hierarchy:
        if entity_store is None:
            log.warning("Skipping label hierarchy: requires --entity-store-path")
        elif not enrichments:
            log.warning("Skipping label hierarchy: no enrichment data available")

    # 10c. Wikidata influence edges (optional, requires entity store with QIDs)
    if args.compute_wikidata_influences and entity_store is not None:
        from semantic_index.wikidata_influence import extract_wikidata_influences

        log.info("Querying Wikidata P737 influence relationships...")
        wikidata_client = WikidataClient(cache_dsn=args.wikidata_cache_dsn)

        # Collect all QIDs from the entity store
        qid_rows = entity_store._conn.execute(
            "SELECT e.wikidata_qid FROM artist a "
            "JOIN entity e ON a.entity_id = e.id "
            "WHERE e.wikidata_qid IS NOT NULL"
        ).fetchall()
        all_qids = [row[0] for row in qid_rows]
        log.info("  %d artists with Wikidata QIDs", len(all_qids))

        if all_qids:
            raw_influences = wikidata_client.get_influences(all_qids)
            log.info("  %d raw influence relationships from Wikidata", len(raw_influences))
            influence_edges = extract_wikidata_influences(entity_store, raw_influences)
            log.info("  %d influence edges between known artists", len(influence_edges))
    elif args.compute_wikidata_influences:
        log.warning("Skipping Wikidata influences: requires --entity-store-path")

    # 11. Print top neighbors for spotlight artists
    print_top_neighbors(edges, SPOTLIGHT_ARTISTS, n=20)

    # 12. Build graph and export GEXF
    log.info("Building graph (min_count=%d)...", args.min_count)
    graph = build_graph(edges, artist_stats, min_count=args.min_count)
    log.info("  %d nodes, %d edges", graph.number_of_nodes(), graph.number_of_edges())

    gexf_path = output_dir / "wxyc_artist_pmi.gexf"
    export_gexf(graph, str(gexf_path))
    log.info("GEXF written to %s", gexf_path)

    # 13. Export SQLite database
    sqlite_path = (
        Path(args.entity_store_path) if entity_store else output_dir / "wxyc_artist_graph.db"
    )
    audio_profile_count = 0
    acoustic_edge_count = 0
    if not args.no_sqlite:
        log.info("Exporting SQLite database...")
        export_sqlite(
            str(sqlite_path),
            artist_stats=artist_stats,
            pmi_edges=edges,
            xref_edges=xref_edges,
            min_count=args.min_count,
            enrichments=enrichments,
            shared_personnel_edges=sp_edges,
            shared_style_edges=ss_edges,
            label_family_edges=lf_edges,
            compilation_edges=comp_edges,
            entity_store=entity_store,
            wikidata_influence_edges=influence_edges,
        )
        log.info("SQLite written to %s", sqlite_path)

        # 13b. AcousticBrainz audio profiles (optional)
        audio_profile_count = 0
        acoustic_edge_count = 0
        if args.acousticbrainz_dir and args.musicbrainz_cache_dsn:
            import sqlite3 as _ab_sqlite3

            from semantic_index.acousticbrainz import (
                AcousticBrainzLoader,
                TarAcousticBrainzLoader,
                build_audio_profiles,
                compute_acoustic_similarity,
                store_audio_profiles,
            )
            from semantic_index.musicbrainz_client import MusicBrainzClient as _MBClient

            log.info("Building audio profiles from AcousticBrainz...")
            mb_client = _MBClient(cache_dsn=args.musicbrainz_cache_dsn)

            # Get MB artist IDs from the graph database
            _ab_conn = _ab_sqlite3.connect(str(sqlite_path))
            mb_rows = _ab_conn.execute(
                "SELECT id, musicbrainz_artist_id FROM artist "
                "WHERE musicbrainz_artist_id IS NOT NULL"
            ).fetchall()
            _ab_conn.close()

            if mb_rows:
                # Map graph artist IDs → MB artist IDs
                graph_id_to_mb = {row[0]: int(row[1]) for row in mb_rows}
                mb_to_graph_id = {v: k for k, v in graph_id_to_mb.items()}
                mb_ids = list(graph_id_to_mb.values())
                log.info("  %d artists with MusicBrainz IDs", len(mb_ids))

                # Resolve MB artists → recording MBIDs
                mb_recordings = mb_client.get_recording_mbids(mb_ids)
                total_recordings = sum(len(v) for v in mb_recordings.values())
                log.info(
                    "  %d recording MBIDs across %d artists", total_recordings, len(mb_recordings)
                )

                # Remap to graph artist IDs
                artist_recordings: dict[int, list[str]] = {}
                for mb_id, mbids in mb_recordings.items():
                    graph_id = mb_to_graph_id.get(mb_id)
                    if graph_id is not None:
                        artist_recordings[graph_id] = mbids

                # Create loader: use tar-indexed mode if tar files present, else extracted dir
                ab_path = Path(args.acousticbrainz_dir)
                tar_files = list(ab_path.glob("*.tar"))
                if tar_files:
                    all_wanted = {m for mbids in artist_recordings.values() for m in mbids}
                    log.info(
                        "  Using tar-indexed loader (%d tar files, %d wanted MBIDs)",
                        len(tar_files),
                        len(all_wanted),
                    )
                    ab_loader = TarAcousticBrainzLoader(
                        args.acousticbrainz_dir, wanted_mbids=all_wanted
                    )
                else:
                    ab_loader = AcousticBrainzLoader(args.acousticbrainz_dir)

                # Build profiles
                profiles = build_audio_profiles(
                    ab_loader, artist_recordings, min_recordings=args.min_recordings
                )
                audio_profile_count = len(profiles)
                log.info("  %d audio profiles built", audio_profile_count)

                if profiles:
                    _ab_conn = _ab_sqlite3.connect(str(sqlite_path))
                    store_audio_profiles(_ab_conn, profiles)
                    acoustic_edge_count = compute_acoustic_similarity(
                        _ab_conn, profiles, threshold=args.acoustic_similarity_threshold
                    )
                    _ab_conn.close()
                    log.info("  %d acoustic similarity edges", acoustic_edge_count)
            else:
                log.warning("  No artists with MusicBrainz IDs — skipping audio profiles")
        elif args.acousticbrainz_dir and not args.musicbrainz_cache_dsn:
            log.warning(
                "--acousticbrainz-dir requires --musicbrainz-cache-dsn for recording lookup"
            )

        # 14. Export facet tables for dynamic PMI
        log.info("Exporting facet tables...")
        import sqlite3 as _sqlite3

        from semantic_index.facet_export import export_facet_tables

        _facet_conn = _sqlite3.connect(str(sqlite_path))
        _facet_conn.row_factory = _sqlite3.Row
        _name_to_id = {
            r["canonical_name"]: r["id"]
            for r in _facet_conn.execute("SELECT id, canonical_name FROM artist").fetchall()
        }
        _facet_conn.close()

        export_facet_tables(
            db_path=str(sqlite_path),
            resolved_entries=resolved_entries,
            name_to_id=_name_to_id,
            show_to_dj=show_to_dj,
            show_dj_names=show_dj_names,
            adjacency_pairs=pairs,
        )
        log.info("Facet tables written to %s", sqlite_path)

    if entity_store is not None:
        entity_store.close()

    elapsed = time.time() - t0
    log.info("Done in %.1f seconds.", elapsed)

    # Summary
    print(f"\n{'=' * 60}")
    print("  Summary")
    print(f"{'=' * 60}")
    print(f"  Total entries parsed:    {total_entries:>12,}")
    print(f"  Music entries:           {music_entries:>12,}")
    print(
        f"  Catalog-resolved:        {catalog_resolved:>12,} ({catalog_resolved / music_entries * 100:.1f}%)"
        if music_entries
        else ""
    )
    print(f"  Unique artists:          {len(artist_stats):>12,}")
    print(f"  Adjacency pairs:         {len(pairs):>12,}")
    print(f"  Unique PMI edges:        {len(edges):>12,}")
    print(f"  Cross-ref edges:         {len(xref_edges):>12,}")
    if enrichments:
        print(f"  Enriched artists:        {len(enrichments):>12,}")
        if args.compute_discogs_edges:
            print(f"  Shared personnel edges:  {len(sp_edges):>12,}")
            print(f"  Shared style edges:      {len(ss_edges):>12,}")
            print(f"  Label family edges:      {len(lf_edges):>12,}")
            print(f"  Compilation edges:       {len(comp_edges):>12,}")
    if influence_edges:
        print(f"  Influence edges:         {len(influence_edges):>12,}")
    if lh_report is not None:
        print(f"  Labels created:          {lh_report.labels_created:>12,}")
        print(f"  Labels matched (WD):     {lh_report.labels_matched:>12,}")
        print(f"  Label hierarchy edges:   {lh_report.hierarchy_edges:>12,}")
    if audio_profile_count > 0:
        print(f"  Audio profiles:          {audio_profile_count:>12,}")
        print(f"  Acoustic sim edges:      {acoustic_edge_count:>12,}")
    if dedup_report is not None and dedup_report.groups_found > 0:
        print(f"  Dedup groups:            {dedup_report.groups_found:>12,}")
        print(f"  Entities merged:         {dedup_report.entities_merged:>12,}")
        print(f"  Artists reassigned:      {dedup_report.artists_reassigned:>12,}")
    print(f"  Graph nodes:             {graph.number_of_nodes():>12,}")
    print(f"  Graph edges:             {graph.number_of_edges():>12,}")
    print(f"  GEXF output:             {gexf_path}")
    if not args.no_sqlite:
        print(f"  SQLite output:           {sqlite_path}")
    print(f"  Elapsed:                 {elapsed:>11.1f}s")


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    run(args)


if __name__ == "__main__":
    main()
