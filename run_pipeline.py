#!/usr/bin/env python3
"""Pipeline CLI: extract adjacency pairs, cross-references, PMI, and Discogs enrichment.

Usage:
    python run_pipeline.py /path/to/wxycmusic.sql [--output-dir output/] [--min-count 2]
    python run_pipeline.py dump.sql --discogs-cache-dsn postgresql://... --api-base-url https://...
"""

import argparse
import logging
import os
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
from semantic_index.graph_export import build_graph, export_gexf, print_top_neighbors
from semantic_index.models import (
    FlowsheetEntry,
    LibraryCode,
    LibraryRelease,
)
from semantic_index.node_attributes import compute_artist_stats
from semantic_index.pmi import compute_pmi
from semantic_index.sql_parser import iter_table_rows, load_table_rows
from semantic_index.sqlite_export import export_sqlite

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
    return parser.parse_args(argv)


def run(args: argparse.Namespace) -> None:
    dump_path = args.dump_path
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not Path(dump_path).exists():
        log.error("Dump file not found: %s", dump_path)
        sys.exit(1)

    t0 = time.time()

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
    for row in iter_table_rows(dump_path, "FLOWSHEET_RADIO_SHOW_PROD"):
        show_id = row[0]
        dj_id = row[3]  # DJ_ID (int or None)
        dj_name = row[2] or ""  # DJ_NAME (str)
        if isinstance(dj_id, int) and dj_id > 0:
            show_to_dj[show_id] = dj_id
        elif dj_name:
            show_to_dj[show_id] = dj_name
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

    # 5. Extract adjacency pairs
    log.info("Extracting adjacency pairs...")
    pairs = extract_adjacency_pairs(resolved_entries)
    log.info("  %d adjacency pairs extracted", len(pairs))

    # 6. Compute PMI
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

        # Extract Discogs-derived edges
        log.info("Extracting Discogs-derived edges...")
        sp_edges = extract_shared_personnel(enrichments)
        log.info("  %d shared personnel edges", len(sp_edges))
        ss_edges = extract_shared_styles(enrichments, min_jaccard=args.min_jaccard)
        log.info("  %d shared style edges", len(ss_edges))
        lf_edges = extract_label_family(enrichments, max_label_artists=args.max_label_artists)
        log.info("  %d label family edges", len(lf_edges))
        comp_edges = extract_compilation_coappearance(enrichments)
        log.info("  %d compilation edges", len(comp_edges))
    elif not args.skip_enrichment:
        log.warning("Skipping Discogs enrichment: no cache DSN or API URL available")

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
    sqlite_path = output_dir / "wxyc_artist_graph.db"
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
        )
        log.info("SQLite written to %s", sqlite_path)

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
        print(f"  Shared personnel edges:  {len(sp_edges):>12,}")
        print(f"  Shared style edges:      {len(ss_edges):>12,}")
        print(f"  Label family edges:      {len(lf_edges):>12,}")
        print(f"  Compilation edges:       {len(comp_edges):>12,}")
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
