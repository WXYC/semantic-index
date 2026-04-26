#!/usr/bin/env python3
"""M0.2 audit: find duplicate-artist nodes that exist only because of mojibake.

Scans the semantic-index SQLite graph for artist rows whose `canonical_name`
is double-encoded UTF-8 (e.g. ``björk``) AND whose latin1->utf8 round-trip
recovery (``björk``) also exists as a separate node. Each such pair is one
duplicate that the M2.2 reconciliation step will need to merge.

Read-only. Writes a CSV and a Markdown summary. Does not modify the DB.

Usage:
    python scripts/audit/si_mojibake_scan.py \\
        --db data/wxyc_artist_graph.db \\
        --csv audit/si_duplicate_artists.csv \\
        --summary audit/si_audit_summary.md
"""

from __future__ import annotations

import argparse
import csv
import logging
import sqlite3
import sys
from dataclasses import dataclass, fields
from pathlib import Path

log = logging.getLogger("si_mojibake_scan")

# Edge tables in the semantic-index SQLite graph and the column pair that
# references artist.id. Tables not present in older databases are skipped.
EDGE_TABLES: list[tuple[str, str, str]] = [
    ("dj_transition", "source_id", "target_id"),
    ("cross_reference", "artist_a_id", "artist_b_id"),
    ("wikidata_influence", "source_id", "target_id"),
    ("acoustic_similarity", "artist_a_id", "artist_b_id"),
    ("shared_personnel", "artist_a_id", "artist_b_id"),
    ("shared_style", "artist_a_id", "artist_b_id"),
    ("label_family", "artist_a_id", "artist_b_id"),
    ("compilation", "artist_a_id", "artist_b_id"),
]


def try_fix(s: str | None) -> str | None:
    """Recover a string corrupted by latin1->utf8 double-encoding.

    Returns the recovered string, or None if the input is clean, lossy, or
    not a valid double-encoding.
    """
    if not s:
        return None
    try:
        fixed = s.encode("latin1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return None
    if fixed == s:
        return None
    if "\ufffd" in fixed:
        return None
    return fixed


@dataclass(frozen=True)
class DuplicatePair:
    corrupted_id: int
    corrupted_name: str
    fixed_id: int
    fixed_name: str
    corrupted_edge_count: int
    fixed_edge_count: int
    corrupted_play_count: int
    fixed_play_count: int

    @property
    def total_edges(self) -> int:
        return self.corrupted_edge_count + self.fixed_edge_count


def _existing_edge_tables(conn: sqlite3.Connection) -> list[tuple[str, str, str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    present = {r[0] for r in rows}
    return [t for t in EDGE_TABLES if t[0] in present]


def _edge_counts(conn: sqlite3.Connection, ids: set[int]) -> dict[int, int]:
    """Count edges (incoming + outgoing) per artist id across all edge tables."""
    counts: dict[int, int] = dict.fromkeys(ids, 0)
    if not ids:
        return counts
    id_list = ",".join(str(i) for i in ids)
    for table, col_a, col_b in _existing_edge_tables(conn):
        for col in (col_a, col_b):
            cur = conn.execute(
                f"SELECT {col}, COUNT(*) FROM {table} "
                f"WHERE {col} IN ({id_list}) GROUP BY {col}"
            )
            for artist_id, n in cur:
                counts[artist_id] = counts.get(artist_id, 0) + n
    return counts


def has_latin1_supplement(s: str) -> bool:
    return any(0x80 <= ord(c) <= 0xFF for c in s)


def looks_lossy_mojibake(s: str) -> bool:
    """Strong signal of double-encoded utf-8 corrupted by an intermediate '?' replacement."""
    return "?" in s and has_latin1_supplement(s)


@dataclass(frozen=True)
class ScanCounts:
    total_artists: int
    fixable_names: int
    lossy_mojibake_names: int


def scan_counts(conn: sqlite3.Connection) -> ScanCounts:
    total = 0
    fixable = 0
    lossy = 0
    for (name,) in conn.execute("SELECT canonical_name FROM artist"):
        total += 1
        if try_fix(name) is not None:
            fixable += 1
        elif looks_lossy_mojibake(name):
            lossy += 1
    return ScanCounts(total_artists=total, fixable_names=fixable, lossy_mojibake_names=lossy)


def find_mojibake_duplicates(conn: sqlite3.Connection) -> list[DuplicatePair]:
    """Return all (corrupted, fixed) pairs that both exist as separate artist rows."""
    name_to_row: dict[str, tuple[int, int]] = {}
    for row in conn.execute("SELECT id, canonical_name, total_plays FROM artist"):
        name_to_row[row[1]] = (row[0], row[2] or 0)

    candidates: list[tuple[str, str]] = []
    for name in name_to_row:
        fixed = try_fix(name)
        if fixed is not None and fixed in name_to_row:
            candidates.append((name, fixed))

    all_ids = {name_to_row[n][0] for pair in candidates for n in pair}
    edge_counts = _edge_counts(conn, all_ids)

    pairs: list[DuplicatePair] = []
    for corrupted, fixed in candidates:
        c_id, c_plays = name_to_row[corrupted]
        f_id, f_plays = name_to_row[fixed]
        pairs.append(
            DuplicatePair(
                corrupted_id=c_id,
                corrupted_name=corrupted,
                fixed_id=f_id,
                fixed_name=fixed,
                corrupted_edge_count=edge_counts.get(c_id, 0),
                fixed_edge_count=edge_counts.get(f_id, 0),
                corrupted_play_count=c_plays,
                fixed_play_count=f_plays,
            )
        )
    pairs.sort(key=lambda p: (-p.total_edges, p.fixed_name))
    return pairs


def write_csv(pairs: list[DuplicatePair], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = [f.name for f in fields(DuplicatePair)]
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(cols)
        for p in pairs:
            w.writerow([getattr(p, c) for c in cols])


def write_summary(
    pairs: list[DuplicatePair],
    path: Path,
    counts: ScanCounts | None = None,
    top_n: int = 20,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    total_edges = sum(p.total_edges for p in pairs)
    total_plays = sum(p.corrupted_play_count + p.fixed_play_count for p in pairs)
    lines: list[str] = []
    lines.append("# M0.2 — semantic-index mojibake duplicate-artist audit")
    lines.append("")
    lines.append(f"Total pairs: {len(pairs)}")
    lines.append(f"Total edges affected: {total_edges}")
    lines.append(f"Total plays affected: {total_plays}")
    if counts is not None:
        lines.append("")
        lines.append("## Scan diagnostics")
        lines.append("")
        lines.append(f"- Total artists scanned: {counts.total_artists}")
        lines.append(
            f"- Round-trippable mojibake names (latin1->utf8 reversible): "
            f"{counts.fixable_names}"
        )
        lines.append(
            f"- Lossy-mojibake names (contain `?` + latin1-supplement chars, "
            f"unrecoverable here): {counts.lossy_mojibake_names}"
        )
        lines.append("")
        lines.append(
            "A pair is reported only when **both** the corrupted form and its "
            "round-trippable fixed form exist as separate artist rows. Lossy-mojibake "
            "names cannot be auto-recovered and require V013's human-reviewed lossy "
            "mappings to detect any corresponding duplicates. M2.2 will need to "
            "re-scan after V012 propagates and V013 lands."
        )
    lines.append("")
    lines.append(f"## Top {min(top_n, len(pairs))} pairs by combined edge count")
    lines.append("")
    if pairs:
        lines.append(
            "| Fixed name | Corrupted name | Fixed plays | Corrupted plays | "
            "Fixed edges | Corrupted edges |"
        )
        lines.append("|---|---|---:|---:|---:|---:|")
        for p in pairs[:top_n]:
            lines.append(
                f"| `{p.fixed_name}` | `{p.corrupted_name}` | {p.fixed_play_count} | "
                f"{p.corrupted_play_count} | {p.fixed_edge_count} | "
                f"{p.corrupted_edge_count} |"
            )
    else:
        lines.append("_No round-trippable duplicate pairs found._")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def _open_readonly(db_path: Path) -> sqlite3.Connection:
    uri = f"file:{db_path}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--db", type=Path, required=True, help="Path to wxyc_artist_graph.db")
    parser.add_argument("--csv", type=Path, default=Path("audit/si_duplicate_artists.csv"))
    parser.add_argument("--summary", type=Path, default=Path("audit/si_audit_summary.md"))
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    if not args.db.exists():
        log.error("DB not found: %s", args.db)
        return 2

    log.info("Scanning %s (read-only)...", args.db)
    conn = _open_readonly(args.db)
    try:
        counts = scan_counts(conn)
        pairs = find_mojibake_duplicates(conn)
    finally:
        conn.close()

    write_csv(pairs, args.csv)
    write_summary(pairs, args.summary, counts=counts)

    total_edges = sum(p.total_edges for p in pairs)
    log.info(
        "Scanned %d artists: %d round-trippable mojibake, %d lossy-mojibake, "
        "%d duplicate pairs covering %d edges. CSV: %s — summary: %s",
        counts.total_artists,
        counts.fixable_names,
        counts.lossy_mojibake_names,
        len(pairs),
        total_edges,
        args.csv,
        args.summary,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
