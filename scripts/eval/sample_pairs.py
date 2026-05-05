"""Sample pairs stratified across the narrative risk matrix.

Cells are defined by four binary axes:

  - fame:     HIGH (total_plays > 800) | LOW (100 <= total_plays <= 400)
  - richness: RICH (>=3 styles AND audio_profile) | THIN (<=2 styles OR no audio)
  - genre:    CROSS (different genre) | SAME (same genre)
  - edge:     DIRECT (dj_transition raw_count >= 2)
              | INDIRECT (no edge, but AA-sum across shared neighbors >= 0.8)

That is 16 cells. For each cell, draw N pairs that satisfy the predicates and
emit them as JSONL.

The AA-sum threshold for INDIRECT pairs matches ``_DEFAULT_MIN_AA_SCORE`` in
``semantic_index/api/narrative.py`` so the live narrative endpoint will accept
the pair rather than returning the canned "insufficient signal" reply. DIRECT
pairs require ``raw_count >= 2`` for symmetry with the pipeline's ``min_count``
default.

Usage:
    python -m scripts.eval.sample_pairs \
        --db-path data/wxyc_artist_graph.db \
        --out output/eval/eval_pairs.jsonl \
        --per-cell 12 \
        [--seed 7]

Output JSONL row:
    {
      "cell_id": "HIGH-RICH-CROSS-DIRECT",
      "fame": "HIGH", "richness": "RICH", "genre": "CROSS", "edge": "DIRECT",
      "source_id": 12345, "target_id": 67890,
      "source_name": "...", "target_name": "...",
      "source_genre": "Rock", "target_genre": "Jazz",
      "source_plays": 1240, "target_plays": 905,
      "raw_count": 4,                     # DIRECT only
      "aa_sum": 1.41                      # INDIRECT only
    }
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import random
import sqlite3
import sys
from collections.abc import Iterable
from dataclasses import dataclass, field
from itertools import product
from pathlib import Path

logger = logging.getLogger(__name__)

FAME_HIGH_MIN = 800  # strict-greater in the AA prompt; we want to mirror that
FAME_LOW_MIN = 100
FAME_LOW_MAX = 400
MIN_AA_SUM = 0.8
DIRECT_MIN_RAW_COUNT = 2
RICH_MIN_STYLES = 3

CELL_AXES = ("fame", "richness", "genre", "edge")


@dataclass(frozen=True)
class ArtistProfile:
    id: int
    name: str
    genre: str
    total_plays: int
    style_count: int
    has_audio: bool

    @property
    def fame(self) -> str | None:
        if self.total_plays > FAME_HIGH_MIN:
            return "HIGH"
        if FAME_LOW_MIN <= self.total_plays <= FAME_LOW_MAX:
            return "LOW"
        return None  # mid-band — not eligible for either fame cell

    @property
    def richness(self) -> str:
        return "RICH" if (self.style_count >= RICH_MIN_STYLES and self.has_audio) else "THIN"


@dataclass
class CellSpec:
    fame: str
    richness: str
    genre: str
    edge: str

    @property
    def cell_id(self) -> str:
        return f"{self.fame}-{self.richness}-{self.genre}-{self.edge}"


@dataclass
class PairRecord:
    cell: CellSpec
    a: ArtistProfile
    b: ArtistProfile
    raw_count: int | None = None
    aa_sum: float | None = None
    aa_neighbors: list[tuple[str, float]] = field(default_factory=list)

    def to_jsonl(self) -> str:
        row = {
            "cell_id": self.cell.cell_id,
            "fame": self.cell.fame,
            "richness": self.cell.richness,
            "genre": self.cell.genre,
            "edge": self.cell.edge,
            "source_id": self.a.id,
            "target_id": self.b.id,
            "source_name": self.a.name,
            "target_name": self.b.name,
            "source_genre": self.a.genre,
            "target_genre": self.b.genre,
            "source_plays": self.a.total_plays,
            "target_plays": self.b.total_plays,
        }
        if self.raw_count is not None:
            row["raw_count"] = self.raw_count
        if self.aa_sum is not None:
            row["aa_sum"] = round(self.aa_sum, 3)
        return json.dumps(row, separators=(",", ":"))


def open_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def load_eligible_artists(db: sqlite3.Connection) -> list[ArtistProfile]:
    """Return all artists meeting *either* fame band, with richness annotated.

    Compilation/VA names are excluded explicitly; the broader resolver handles
    these upstream but the SQLite ``artist`` table can still surface them.
    """
    rows = db.execute(
        """
        SELECT a.id, a.canonical_name, a.genre, a.total_plays,
            (SELECT COUNT(*) FROM artist_style s WHERE s.artist_id = a.id) AS style_count,
            EXISTS(SELECT 1 FROM audio_profile ap WHERE ap.artist_id = a.id) AS has_audio
        FROM artist a
        WHERE a.genre IS NOT NULL
          AND (a.total_plays > ?
               OR (a.total_plays BETWEEN ? AND ?))
          AND a.canonical_name NOT LIKE 'Various%'
          AND a.canonical_name NOT LIKE 'V/A%'
          AND a.canonical_name NOT LIKE '%Soundtrack%'
          AND lower(a.canonical_name) != 'unknown'
        """,
        (FAME_HIGH_MIN, FAME_LOW_MIN, FAME_LOW_MAX),
    ).fetchall()
    return [
        ArtistProfile(
            id=r["id"],
            name=r["canonical_name"],
            genre=r["genre"],
            total_plays=r["total_plays"],
            style_count=r["style_count"],
            has_audio=bool(r["has_audio"]),
        )
        for r in rows
    ]


def index_by_fame_richness(
    artists: Iterable[ArtistProfile],
) -> dict[tuple[str, str], list[ArtistProfile]]:
    out: dict[tuple[str, str], list[ArtistProfile]] = {}
    for a in artists:
        if a.fame is None:
            continue
        out.setdefault((a.fame, a.richness), []).append(a)
    return out


def compute_degrees(db: sqlite3.Connection) -> dict[int, int]:
    """Return artist_id -> distinct dj_transition partner count."""
    rows = db.execute(
        """
        WITH all_edges AS (
            SELECT source_id AS a, target_id AS b FROM dj_transition
            UNION ALL
            SELECT target_id AS a, source_id AS b FROM dj_transition
        )
        SELECT a, COUNT(DISTINCT b) AS deg FROM all_edges GROUP BY a
        """
    ).fetchall()
    return {r["a"]: r["deg"] for r in rows}


def get_direct_edge_raw_count(db: sqlite3.Connection, a_id: int, b_id: int) -> int | None:
    """Return raw_count if a direct dj_transition exists in either direction, else None."""
    row = db.execute(
        "SELECT raw_count FROM dj_transition "
        "WHERE (source_id = ? AND target_id = ?) OR (source_id = ? AND target_id = ?) "
        "ORDER BY raw_count DESC LIMIT 1",
        (a_id, b_id, b_id, a_id),
    ).fetchone()
    return int(row["raw_count"]) if row else None


def compute_aa_sum(
    db: sqlite3.Connection,
    a_id: int,
    b_id: int,
    degrees: dict[int, int],
) -> tuple[float, list[tuple[str, float]]]:
    """Return (sum, [(neighbor_name, aa_score), ...]) sorted by score desc."""
    rows = db.execute(
        """
        WITH a_n AS (
            SELECT CASE WHEN source_id = :a THEN target_id ELSE source_id END AS nid
            FROM dj_transition WHERE (source_id = :a OR target_id = :a) AND source_id != target_id
        ),
        b_n AS (
            SELECT CASE WHEN source_id = :b THEN target_id ELSE source_id END AS nid
            FROM dj_transition WHERE (source_id = :b OR target_id = :b) AND source_id != target_id
        )
        SELECT DISTINCT ar.id, ar.canonical_name
        FROM a_n
        JOIN b_n ON a_n.nid = b_n.nid
        JOIN artist ar ON ar.id = a_n.nid
        WHERE ar.id NOT IN (:a, :b)
          AND ar.canonical_name NOT LIKE 'Various%'
          AND ar.canonical_name NOT LIKE 'V/A%'
        """,
        {"a": a_id, "b": b_id},
    ).fetchall()
    scored: list[tuple[str, float]] = []
    for r in rows:
        deg = degrees.get(r["id"], 0)
        if deg < 2:
            continue
        scored.append((r["canonical_name"], 1.0 / math.log(deg)))
    scored.sort(key=lambda x: x[1], reverse=True)
    return sum(s for _, s in scored), scored


def matches_genre_axis(a: ArtistProfile, b: ArtistProfile, axis: str) -> bool:
    if axis == "SAME":
        return a.genre == b.genre
    return a.genre != b.genre


def sample_direct_cell(
    db: sqlite3.Connection,
    cell: CellSpec,
    pool: list[ArtistProfile],
    target: int,
    rng: random.Random,
    *,
    used_pair_keys: set[frozenset[int]],
) -> list[PairRecord]:
    """Driven from the ``dj_transition`` table: enumerate edges over the pool.

    Random sampling pair-wise from a 3000-artist pool wastes attempts on the
    >99% of pairs that lack a direct edge. Starting from the edges and
    filtering by (fame, richness, genre) reaches the target much faster.
    """
    pool_index = {a.id: a for a in pool}
    rows = db.execute(
        f"""
        SELECT source_id, target_id, raw_count FROM dj_transition
        WHERE raw_count >= {DIRECT_MIN_RAW_COUNT}
          AND source_id != target_id
          AND source_id IN ({",".join("?" * len(pool_index))})
          AND target_id IN ({",".join("?" * len(pool_index))})
        """,  # noqa: S608  -- pool ids are int from our SELECT, not user input
        list(pool_index.keys()) + list(pool_index.keys()),
    ).fetchall()

    candidates: list[PairRecord] = []
    seen: set[frozenset[int]] = set()
    for r in rows:
        a, b = pool_index[r["source_id"]], pool_index[r["target_id"]]
        key = frozenset({a.id, b.id})
        if key in seen or key in used_pair_keys:
            continue
        if not matches_genre_axis(a, b, cell.genre):
            continue
        seen.add(key)
        candidates.append(PairRecord(cell=cell, a=a, b=b, raw_count=int(r["raw_count"])))

    rng.shuffle(candidates)
    out = candidates[:target]
    for p in out:
        used_pair_keys.add(frozenset({p.a.id, p.b.id}))
    return out


def sample_indirect_cell(
    db: sqlite3.Connection,
    cell: CellSpec,
    pool: list[ArtistProfile],
    degrees: dict[int, int],
    target: int,
    rng: random.Random,
    *,
    max_attempts: int,
    used_pair_keys: set[frozenset[int]],
) -> list[PairRecord]:
    """Indirect pairs: no direct edge, but AA-sum across shared neighbors >= threshold.

    Strategy: pick a seed artist, query for partners that share at least one
    transition neighbor with the seed (a 1-SQL-query lookup), filter to the
    pool and to no-direct-edge, then run the AA-sum test on the candidates.
    Far cheaper than blind pair-wise random sampling.
    """
    pool_ids = {a.id for a in pool}
    pool_by_id = {a.id: a for a in pool}
    out: list[PairRecord] = []
    seen_in_cell: set[frozenset[int]] = set()
    attempts = 0
    seed_pool = list(pool)
    rng.shuffle(seed_pool)

    for seed in seed_pool:
        if len(out) >= target or attempts >= max_attempts:
            break

        partner_ids = db.execute(
            """
            WITH seed_n AS (
                SELECT CASE WHEN source_id = :s THEN target_id ELSE source_id END AS nid
                FROM dj_transition
                WHERE (source_id = :s OR target_id = :s) AND source_id != target_id
            ),
            two_hop AS (
                SELECT DISTINCT
                    CASE WHEN dj_transition.source_id = seed_n.nid
                         THEN dj_transition.target_id ELSE dj_transition.source_id END AS pid
                FROM seed_n
                JOIN dj_transition
                  ON dj_transition.source_id = seed_n.nid
                  OR dj_transition.target_id = seed_n.nid
            )
            SELECT pid FROM two_hop WHERE pid != :s
            """,
            {"s": seed.id},
        ).fetchall()

        partner_candidates = [pool_by_id[r["pid"]] for r in partner_ids if r["pid"] in pool_ids]
        rng.shuffle(partner_candidates)

        for partner in partner_candidates:
            if len(out) >= target or attempts >= max_attempts:
                break
            attempts += 1
            key = frozenset({seed.id, partner.id})
            if key in seen_in_cell or key in used_pair_keys:
                continue
            seen_in_cell.add(key)

            if not matches_genre_axis(seed, partner, cell.genre):
                continue
            if get_direct_edge_raw_count(db, seed.id, partner.id) is not None:
                continue
            aa_sum, neighbors = compute_aa_sum(db, seed.id, partner.id, degrees)
            if aa_sum < MIN_AA_SUM:
                continue
            out.append(
                PairRecord(
                    cell=cell, a=seed, b=partner,
                    aa_sum=aa_sum, aa_neighbors=neighbors[:5],
                )
            )
            used_pair_keys.add(key)
    return out


def sample_cell(
    db: sqlite3.Connection,
    cell: CellSpec,
    pool: list[ArtistProfile],
    degrees: dict[int, int],
    target: int,
    rng: random.Random,
    *,
    max_attempts: int,
    used_pair_keys: set[frozenset[int]],
) -> list[PairRecord]:
    """Dispatch to the appropriate per-edge-type sampler."""
    if cell.edge == "DIRECT":
        return sample_direct_cell(
            db, cell, pool, target, rng, used_pair_keys=used_pair_keys,
        )
    return sample_indirect_cell(
        db, cell, pool, degrees, target, rng,
        max_attempts=max_attempts, used_pair_keys=used_pair_keys,
    )


def build_cells() -> list[CellSpec]:
    return [
        CellSpec(fame=f, richness=r, genre=g, edge=e)
        for f, r, g, e in product(("HIGH", "LOW"), ("RICH", "THIN"), ("CROSS", "SAME"), ("DIRECT", "INDIRECT"))
    ]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db-path", default="data/wxyc_artist_graph.db")
    ap.add_argument("--out", required=True, help="Output JSONL path")
    ap.add_argument("--per-cell", type=int, default=12, help="Pairs per cell (default 12).")
    ap.add_argument("--max-attempts-per-cell", type=int, default=2000)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    db = open_db(args.db_path)
    artists = load_eligible_artists(db)
    degrees = compute_degrees(db)
    pools = index_by_fame_richness(artists)
    logger.info(
        "Pools: %s",
        {f"{k[0]}/{k[1]}": len(v) for k, v in sorted(pools.items())},
    )

    rng = random.Random(args.seed)
    used: set[frozenset[int]] = set()
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    counts: dict[str, int] = {}
    with out_path.open("w") as fh:
        for cell in build_cells():
            pool_key = (cell.fame, cell.richness)
            pool = pools.get(pool_key, [])
            if len(pool) < 2:
                logger.warning("Pool %s empty/small (%d) — skipping cell %s", pool_key, len(pool), cell.cell_id)
                continue
            pairs = sample_cell(
                db,
                cell,
                pool,
                degrees,
                target=args.per_cell,
                rng=rng,
                max_attempts=args.max_attempts_per_cell,
                used_pair_keys=used,
            )
            for p in pairs:
                fh.write(p.to_jsonl() + "\n")
                written += 1
            counts[cell.cell_id] = len(pairs)
            logger.info("Cell %s: %d pairs", cell.cell_id, len(pairs))

    logger.info("Wrote %d pairs across %d cells -> %s", written, len(counts), out_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
