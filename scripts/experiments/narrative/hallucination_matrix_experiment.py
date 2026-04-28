"""Experiment: hallucination rates across factor combinations.

Tests each cell in a 2×2×2 matrix of risk factors:
  Fame:  HIGH (>800 plays) / LOW (<400 plays)
  Data:  RICH (3+ styles AND audio profile) / THIN (≤2 styles OR no audio)
  Genre: CROSS (different genres) / SAME (same genre)

Draws 3 pairs per cell = 24 pairs total. Each pair gets a narrative
and a verification pass.
"""

import json
import math
import os
import sqlite3
import sys
import time
from itertools import product

import anthropic

DB_PATH = "data/wxyc_artist_graph.db"

NARRATIVE_PROMPT = (
    "You are a music knowledge assistant for WXYC 89.3 FM, a freeform college radio station. "
    "Given structured data about two artists, write 2-3 sentences (under 80 words) explaining "
    "their connection. "
    "CRITICAL: describe each artist ONLY using the styles, audio, and genre fields provided. "
    "Do not draw on outside knowledge about these artists. If a field is missing, do not guess "
    "what it might contain. If you lack data to describe an artist's sound, focus on the "
    "sequential_context instead. "
    "When sequential_context is present, describe how DJs use these artists in similar ways — "
    "which artists they tend to appear near, and what that suggests about their role in a set. "
    "The shared_set_neighbors list shows artists that both subjects tend to appear near — do not "
    "imply the neighbors are similar to each other. Never reference adjacency or proximity in the "
    "playlist — the connection is about role, not position. "
    "Describe what an artist's music IS, not what it isn't. "
    "Do not quote numerical values from the data (e.g. danceability scores). "
    "Africa is a continent, not a genre. Use the specific tradition from the styles when possible."
)

VERIFY_PROMPT = (
    "You are a fact-checking assistant. You will receive a narrative about two artists and the "
    "structured data that was used to generate it. Your job is to identify every factual claim "
    "in the narrative and check whether it is grounded in the provided data.\n\n"
    "For each claim, output one line:\n"
    "  GROUNDED: <claim> — <which data field supports it>\n"
    "  UNGROUNDED: <claim> — <this is not in the provided data>\n"
    "  AMBIGUOUS: <claim> — <partially supported but extended beyond the data>\n\n"
    "Be strict. If the narrative says an artist 'channels psychedelic textures' but the styles "
    "field only says 'Psychedelic Rock', that's GROUNDED. If the narrative describes an artist's "
    "'nasal vocal delivery' and nothing in the data mentions vocals or vocal style, that's "
    "UNGROUNDED. If the narrative says 'both artists explore the intersection of jazz and "
    "electronic music' and only one artist has jazz-related styles, that's AMBIGUOUS.\n\n"
    "At the end, output a summary line:\n"
    "  VERDICT: X grounded, Y ungrounded, Z ambiguous"
)


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def compute_degrees(db: sqlite3.Connection) -> dict[int, int]:
    degree = {}
    for row in db.execute(
        "SELECT a.id, "
        "COUNT(DISTINCT CASE WHEN dt.source_id = a.id THEN dt.target_id ELSE dt.source_id END) AS deg "
        "FROM artist a "
        "JOIN dj_transition dt ON (dt.source_id = a.id OR dt.target_id = a.id) AND dt.source_id != dt.target_id "
        "GROUP BY a.id"
    ):
        degree[row["id"]] = row["deg"]
    return degree


def get_artist_profile(db: sqlite3.Connection, artist_id: int) -> dict | None:
    row = db.execute(
        "SELECT id, canonical_name, genre, total_plays FROM artist WHERE id = ?",
        (artist_id,),
    ).fetchone()
    if not row:
        return None

    styles = []
    try:
        style_rows = db.execute(
            "SELECT style_tag FROM artist_style WHERE artist_id = ? ORDER BY style_tag",
            (artist_id,),
        ).fetchall()
        styles = [r["style_tag"] for r in style_rows]
    except sqlite3.OperationalError:
        pass

    has_audio = False
    audio = {}
    try:
        profile = db.execute(
            "SELECT avg_danceability, voice_instrumental_ratio, recording_count "
            "FROM audio_profile WHERE artist_id = ?",
            (artist_id,),
        ).fetchone()
        if profile and profile["recording_count"] and profile["recording_count"] > 0:
            has_audio = True
            audio = {
                "danceability": round(profile["avg_danceability"], 2),
                "voice_instrumental": "vocal" if profile["voice_instrumental_ratio"] > 0.5 else "instrumental",
            }
    except sqlite3.OperationalError:
        pass

    return {
        "id": row["id"],
        "name": row["canonical_name"],
        "genre": row["genre"],
        "total_plays": row["total_plays"],
        "styles": styles,
        "has_audio": has_audio,
        "audio": audio,
        "style_count": len(styles),
        "is_rich": len(styles) >= 3 and has_audio,
    }


def get_aa_neighbors(db, id_a, id_b, degree, top_k=4):
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
        SELECT DISTINCT a.id, a.canonical_name
        FROM a_n JOIN b_n ON a_n.nid = b_n.nid
        JOIN artist a ON a.id = a_n.nid
        WHERE a.canonical_name NOT LIKE 'Various%'
          AND a.canonical_name NOT LIKE 'V/A%'
          AND a.canonical_name != 'various'
          AND a.canonical_name != 'Unknown'
        """,
        {"a": id_a, "b": id_b},
    ).fetchall()

    scored = []
    for r in rows:
        deg = degree.get(r["id"], 1)
        aa = 1.0 / math.log(deg) if deg > 1 else 1.0
        scored.append((r["canonical_name"], aa))
    scored.sort(key=lambda x: x[1], reverse=True)
    total = sum(s for _, s in scored)
    return [n for n, _ in scored[:top_k]], total


def find_matching_artists(db, fame: str, data: str) -> list:
    """Find candidate artists matching fame and data criteria."""
    if fame == "HIGH":
        play_clause = "a.total_plays > 800"
    else:
        play_clause = "a.total_plays BETWEEN 150 AND 400"

    candidates = db.execute(
        f"""
        SELECT a.id
        FROM artist a
        JOIN dj_transition dt ON (dt.source_id = a.id OR dt.target_id = a.id)
            AND dt.source_id != dt.target_id
        WHERE {play_clause}
          AND a.canonical_name NOT LIKE 'Various%'
          AND a.canonical_name NOT LIKE 'V/A%'
          AND a.canonical_name != 'various'
          AND a.genre IS NOT NULL
        GROUP BY a.id
        HAVING COUNT(DISTINCT CASE WHEN dt.source_id = a.id THEN dt.target_id
                                    ELSE dt.source_id END) >= 8
        ORDER BY RANDOM()
        LIMIT 120
        """,
    ).fetchall()

    profiles = []
    for c in candidates:
        p = get_artist_profile(db, c["id"])
        if not p:
            continue
        if data == "RICH" and p["is_rich"]:
            profiles.append(p)
        elif data == "THIN" and not p["is_rich"]:
            profiles.append(p)

    return profiles


def find_pairs_for_cell(db, degree, fame, data, genre, count=3):
    """Find pairs for one cell of the matrix."""
    artists = find_matching_artists(db, fame, data)
    cross = genre == "CROSS"

    pairs = []
    used: set[int] = set()

    for i, a in enumerate(artists):
        if len(pairs) >= count:
            break
        if a["id"] in used:
            continue

        for b in artists[i + 1:]:
            if len(pairs) >= count:
                break
            if b["id"] in used:
                continue

            if cross and a["genre"] == b["genre"]:
                continue
            if not cross and a["genre"] != b["genre"]:
                continue

            # No direct edge
            edge = db.execute(
                "SELECT 1 FROM dj_transition WHERE "
                "(source_id = ? AND target_id = ?) OR (source_id = ? AND target_id = ?)",
                (a["id"], b["id"], b["id"], a["id"]),
            ).fetchone()
            if edge:
                continue

            neighbors, aa_total = get_aa_neighbors(db, a["id"], b["id"], degree)
            if aa_total < 0.6 or len(neighbors) < 2:
                continue

            pairs.append({
                "a": a,
                "b": b,
                "neighbors": neighbors,
                "aa_total": aa_total,
                "cell": f"fame={fame} data={data} genre={genre}",
            })
            used.add(a["id"])
            used.add(b["id"])
            break

    return pairs


def build_prompt_data(pair: dict) -> dict:
    a, b = pair["a"], pair["b"]
    source: dict = {"name": a["name"], "genre": a["genre"], "total_plays": a["total_plays"]}
    if a["styles"]:
        source["styles"] = a["styles"][:5]
    if a["has_audio"]:
        source["audio"] = a["audio"]

    target: dict = {"name": b["name"], "genre": b["genre"], "total_plays": b["total_plays"]}
    if b["styles"]:
        target["styles"] = b["styles"][:5]
    if b["has_audio"]:
        target["audio"] = b["audio"]

    return {
        "source": source,
        "target": target,
        "relationships": [],
        "sequential_context": {"shared_set_neighbors": pair["neighbors"]},
    }


def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("Set ANTHROPIC_API_KEY to run this script.", file=sys.stderr)
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)
    db = get_db()
    degree = compute_degrees(db)

    # Generate all 8 cells
    fame_levels = ["HIGH", "LOW"]
    data_levels = ["RICH", "THIN"]
    genre_levels = ["CROSS", "SAME"]

    all_pairs = []
    for fame, data, genre in product(fame_levels, data_levels, genre_levels):
        cell = f"fame={fame} data={data} genre={genre}"
        print(f"Finding pairs for [{cell}]...", file=sys.stderr)
        pairs = find_pairs_for_cell(db, degree, fame, data, genre, count=3)
        print(f"  Found {len(pairs)}", file=sys.stderr)
        all_pairs.extend(pairs)

    print(f"\nTotal pairs: {len(all_pairs)}", file=sys.stderr)
    print(f"Generating narratives and verifying...\n", file=sys.stderr)

    # Track summary stats per cell
    cell_stats: dict[str, dict] = {}

    for pair in all_pairs:
        a, b = pair["a"], pair["b"]
        cell = pair["cell"]
        prompt_data = build_prompt_data(pair)
        user_message = json.dumps(prompt_data, separators=(",", ":"))

        print(f"{'=' * 70}")
        print(f"[{cell}]")
        print(f"{a['name']} / {b['name']}")
        print(f"  plays: {a['total_plays']} / {b['total_plays']}")
        print(f"  genre: {a['genre']} / {b['genre']}")
        print(f"  styles A: {', '.join(a['styles'][:5]) if a['styles'] else '(none)'}")
        print(f"  styles B: {', '.join(b['styles'][:5]) if b['styles'] else '(none)'}")
        print(f"  audio: {a['has_audio']} / {b['has_audio']}")
        print(f"  neighbors: {', '.join(pair['neighbors'])}")

        # Generate
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=150,
            system=NARRATIVE_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )
        narrative = response.content[0].text
        print(f"\n  NARRATIVE:")
        print(f"  {narrative}")
        time.sleep(0.3)

        # Verify
        verify_input = json.dumps({
            "narrative": narrative,
            "provided_data": prompt_data,
        }, indent=2)

        verify_response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=500,
            system=VERIFY_PROMPT,
            messages=[{"role": "user", "content": verify_input}],
        )
        verification = verify_response.content[0].text
        print(f"\n  VERIFICATION:")
        for line in verification.strip().split("\n"):
            print(f"  {line}")
        print()

        # Parse verdict
        for line in verification.strip().split("\n"):
            if line.strip().startswith("VERDICT:"):
                if cell not in cell_stats:
                    cell_stats[cell] = {"grounded": 0, "ungrounded": 0, "ambiguous": 0, "pairs": 0}
                cell_stats[cell]["pairs"] += 1
                parts = line.strip().replace("VERDICT:", "").strip()
                for part in parts.split(","):
                    part = part.strip().lower()
                    for key in ["grounded", "ungrounded", "ambiguous"]:
                        if key in part:
                            try:
                                num = int(part.split()[0])
                                cell_stats[cell][key] += num
                            except (ValueError, IndexError):
                                pass

        time.sleep(0.3)

    # Summary table
    print(f"\n{'=' * 70}")
    print("SUMMARY BY CELL")
    print(f"{'=' * 70}")
    print(f"{'Cell':<40s} {'Pairs':>5s} {'Ground':>7s} {'Ungnd':>7s} {'Ambig':>7s} {'Halluc%':>8s}")
    print("-" * 70)

    for cell in sorted(cell_stats):
        s = cell_stats[cell]
        total_claims = s["grounded"] + s["ungrounded"] + s["ambiguous"]
        halluc_pct = (s["ungrounded"] + s["ambiguous"]) / total_claims * 100 if total_claims else 0
        print(f"{cell:<40s} {s['pairs']:>5d} {s['grounded']:>7d} {s['ungrounded']:>7d} "
              f"{s['ambiguous']:>7d} {halluc_pct:>7.0f}%")

    db.close()


if __name__ == "__main__":
    main()
