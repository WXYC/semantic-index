#!/usr/bin/env python3
"""Import cross-reference edges into an existing SQLite graph database.

Usage:
    python scripts/import_xrefs.py <xref_dump.sql> <main_dump.sql> <sqlite_db>

Parses cross-reference tables from the xref dump, resolves artist names using
LIBRARY_CODE from the main dump, and inserts edges into the SQLite database.
"""

import argparse
import logging
import sqlite3
import sys

from semantic_index.cross_reference import CrossReferenceExtractor
from semantic_index.models import LibraryCode, LibraryRelease
from semantic_index.sql_parser import load_table_rows

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Import cross-reference edges into SQLite graph.")
    parser.add_argument("xref_dump", help="Path to the xrefs SQL dump (from dump-db-for-index-build.sh xrefs)")
    parser.add_argument("main_dump", help="Path to the main SQL dump (for LIBRARY_CODE and LIBRARY_RELEASE)")
    parser.add_argument("sqlite_db", help="Path to the existing SQLite graph database")
    args = parser.parse_args()

    # Load library tables from main dump
    log.info("Loading LIBRARY_CODE from %s...", args.main_dump)
    code_rows = load_table_rows(args.main_dump, "LIBRARY_CODE")
    codes = {r[0]: r[7] for r in code_rows}  # id → presentation_name
    log.info("  %d library codes", len(codes))

    log.info("Loading LIBRARY_RELEASE from %s...", args.main_dump)
    release_rows = load_table_rows(args.main_dump, "LIBRARY_RELEASE")
    release_to_code = {r[0]: r[8] for r in release_rows}  # id → library_code_id
    log.info("  %d releases", len(release_to_code))

    # Extract cross-reference edges
    extractor = CrossReferenceExtractor(codes=codes, release_to_code=release_to_code)

    log.info("Parsing LIBRARY_CODE_CROSS_REFERENCE from %s...", args.xref_dump)
    lc_rows = load_table_rows(args.xref_dump, "LIBRARY_CODE_CROSS_REFERENCE")
    lc_edges = extractor.extract_library_code_xrefs(lc_rows)
    log.info("  %d edges from %d rows", len(lc_edges), len(lc_rows))

    log.info("Parsing RELEASE_CROSS_REFERENCE from %s...", args.xref_dump)
    rel_rows = load_table_rows(args.xref_dump, "RELEASE_CROSS_REFERENCE")
    rel_edges = extractor.extract_release_xrefs(rel_rows)
    log.info("  %d edges from %d rows", len(rel_edges), len(rel_rows))

    all_edges = lc_edges + rel_edges
    if not all_edges:
        log.warning("No resolvable cross-reference edges found.")
        sys.exit(0)

    # Insert into SQLite
    log.info("Inserting %d edges into %s...", len(all_edges), args.sqlite_db)
    conn = sqlite3.connect(args.sqlite_db)
    conn.row_factory = sqlite3.Row

    # Build name → id mapping
    name_to_id = {}
    for row in conn.execute("SELECT id, canonical_name FROM artist"):
        name_to_id[row["canonical_name"]] = row["id"]

    inserted = 0
    skipped_missing = 0
    for edge in all_edges:
        a_id = name_to_id.get(edge.artist_a)
        b_id = name_to_id.get(edge.artist_b)
        if a_id is None or b_id is None:
            skipped_missing += 1
            continue
        try:
            conn.execute(
                "INSERT OR IGNORE INTO cross_reference (artist_a_id, artist_b_id, comment, source) VALUES (?, ?, ?, ?)",
                (a_id, b_id, edge.comment, edge.source),
            )
            inserted += 1
        except Exception:
            log.debug("Failed to insert xref %s ↔ %s", edge.artist_a, edge.artist_b, exc_info=True)

    conn.commit()
    conn.close()

    log.info("Done: %d inserted, %d skipped (artist not in graph)", inserted, skipped_missing)


if __name__ == "__main__":
    main()
