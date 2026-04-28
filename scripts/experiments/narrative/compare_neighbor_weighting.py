"""Compare neighbor weighting methods across the 8 no-edge-shared-context pairs.

For each pair, computes shared neighbors ranked by:
1. Raw (unweighted, sorted by play count — current behavior)
2. Adamic-Adar: weight = sum(1/log(degree(n)) for shared neighbor n)
3. Resource Allocation: weight = sum(1/degree(n))
4. Degree ceiling: exclude neighbors above 95th percentile degree

Outputs a comparison table showing how each method reranks or filters the neighbors.
"""

import math
import sqlite3

DB_PATH = "data/wxyc_artist_graph.db"

PAIRS = [
    (87013, 780, "Hiatus Kaiyote", "Muddy Waters"),
    (57122, 113128, "Bill Orcutt", "Durand Jones"),
    (32948, 65513, "Religious Knives", "Pastor T.L. Barrett"),
    (1576, 2832, "Bob Marley and the Wailers", "Les Rallizes Dénudés"),
    (2800, 67435, "Peter Brötzmann", "Angel Olsen"),
    (329, 7717, "Caetano Veloso", "Don Cherry"),
    (59053, 301, "Tame Impala", "Smog"),
    (22302, 386, "Wooden Shjips", "Guided by Voices"),
]


def main():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row

    # Compute degree for all artists
    degree = {}
    for row in db.execute(
        """
        SELECT a.id,
               COUNT(DISTINCT CASE WHEN dt.source_id = a.id THEN dt.target_id
                                   ELSE dt.source_id END) AS deg
        FROM artist a
        JOIN dj_transition dt ON (dt.source_id = a.id OR dt.target_id = a.id)
            AND dt.source_id != dt.target_id
        GROUP BY a.id
        """
    ):
        degree[row["id"]] = row["deg"]

    # 95th percentile degree for ceiling method
    all_degrees = sorted(degree.values())
    p95 = all_degrees[int(len(all_degrees) * 0.95)] if all_degrees else 100
    print(f"95th percentile degree: {p95}")
    print(f"Total artists with edges: {len(all_degrees)}")
    print()

    for id_a, id_b, name_a, name_b in PAIRS:
        # Get shared neighbors
        rows = db.execute(
            """
            WITH a_neighbors AS (
                SELECT CASE WHEN source_id = :a THEN target_id ELSE source_id END AS nid
                FROM dj_transition
                WHERE (source_id = :a OR target_id = :a) AND source_id != target_id
            ),
            b_neighbors AS (
                SELECT CASE WHEN source_id = :b THEN target_id ELSE source_id END AS nid
                FROM dj_transition
                WHERE (source_id = :b OR target_id = :b) AND source_id != target_id
            )
            SELECT DISTINCT a.id, a.canonical_name, a.total_plays
            FROM a_neighbors an
            JOIN b_neighbors bn ON an.nid = bn.nid
            JOIN artist a ON a.id = an.nid
            WHERE a.canonical_name NOT LIKE 'Various%'
              AND a.canonical_name NOT LIKE 'V/A%'
              AND a.canonical_name != 'various'
              AND a.canonical_name != 'Unknown'
            """,
            {"a": id_a, "b": id_b},
        ).fetchall()

        neighbors = []
        for r in rows:
            nid = r["id"]
            deg = degree.get(nid, 1)
            aa_weight = 1.0 / math.log(deg) if deg > 1 else 1.0
            ra_weight = 1.0 / deg
            neighbors.append({
                "name": r["canonical_name"],
                "plays": r["total_plays"],
                "degree": deg,
                "adamic_adar": aa_weight,
                "resource_alloc": ra_weight,
            })

        print(f"{'=' * 80}")
        print(f"  {name_a} / {name_b}")
        print(f"  ({len(neighbors)} shared neighbors)")
        print(f"{'=' * 80}")

        # Raw (play count)
        by_plays = sorted(neighbors, key=lambda x: x["plays"], reverse=True)[:8]
        print(f"\n  RAW (by play count):")
        for n in by_plays:
            print(f"    {n['name']:35s}  plays={n['plays']:5d}  degree={n['degree']}")

        # Adamic-Adar
        by_aa = sorted(neighbors, key=lambda x: x["adamic_adar"], reverse=True)[:8]
        print(f"\n  ADAMIC-ADAR (1/log(degree)):")
        for n in by_aa:
            print(f"    {n['name']:35s}  aa={n['adamic_adar']:.4f}  degree={n['degree']}")

        # Resource Allocation
        by_ra = sorted(neighbors, key=lambda x: x["resource_alloc"], reverse=True)[:8]
        print(f"\n  RESOURCE ALLOCATION (1/degree):")
        for n in by_ra:
            print(f"    {n['name']:35s}  ra={n['resource_alloc']:.6f}  degree={n['degree']}")

        # Degree ceiling
        by_ceil = [n for n in neighbors if n["degree"] <= p95]
        by_ceil = sorted(by_ceil, key=lambda x: x["plays"], reverse=True)[:8]
        print(f"\n  DEGREE CEILING (exclude degree > {p95}):")
        if by_ceil:
            for n in by_ceil:
                print(f"    {n['name']:35s}  plays={n['plays']:5d}  degree={n['degree']}")
        else:
            print(f"    (all shared neighbors exceed ceiling)")

        print()

    db.close()


if __name__ == "__main__":
    main()
