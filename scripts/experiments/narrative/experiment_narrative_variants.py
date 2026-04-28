"""Experiment: compare narrative variants across prompt strategies.

Tests three prompt variants on the same set of pairs:
1. BASELINE — current prompt (from generate_narrative_samples.py)
2. ANTI-HALLUCINATION — constrain model to only use provided data
3. ANTI-HALLUCINATION + VARIED LANGUAGE — also address verb monotony

Uses Adamic-Adar weighted neighbors and filters out weak pairs.
"""

import json
import math
import os
import sqlite3
import sys
import time
from collections import defaultdict

import anthropic

DB_PATH = "data/wxyc_artist_graph.db"

PROMPT_BASELINE = (
    "You are a music knowledge assistant for WXYC 89.3 FM, a freeform college radio station. "
    "Given structured data about the relationship between two artists in the station's play "
    "history, write 2-3 sentences (under 80 words) explaining their connection in plain "
    "English. Be specific — mention shared genres, personnel names, labels, or play patterns "
    "from the data. Do not add information not present in the data. "
    "When sequential_context is present, use it to describe how DJs use these artists in "
    "similar ways — which artists they tend to appear near, and what that suggests about their "
    "shared role in a set. Use language like 'DJs reach for both at similar moments' or "
    "'both tend to appear near [artists].' The shared_set_neighbors list shows artists that "
    "both subjects tend to appear near — do not imply the neighbors are similar to each other. "
    "Never reference adjacency or proximity in the playlist when describing sequential context — "
    "the connection is about role, not position. "
    "Never use technical terms like 'embedding,' 'vector,' or 'cosine similarity.' "
    "Describe what an artist's music IS, not what it isn't — avoid 'low-danceability' or similar "
    "negations. "
    "Africa is a continent, not a genre. If the data includes country or region, use that. If it "
    "only says 'Africa' or 'African,' describe the specific musical tradition from the styles "
    "(e.g. 'Desert Blues,' 'Congolese likembe music') rather than generalizing across the continent."
)

PROMPT_ANTI_HALLUCINATION = (
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
    "Africa is a continent, not a genre. Use the specific tradition from the styles when possible."
)

PROMPT_ANTI_HALLUCINATION_VARIED = (
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
    "Africa is a continent, not a genre. Use the specific tradition from the styles when possible. "
    "LANGUAGE: write in a natural, varied voice. Do not reuse the same verbs or phrases across "
    "sentences. Avoid these overused words: occupy, reach, represent, suggest, anchor, bridge, "
    "curate, sonic, territory, sensibility, touchstone. Find fresher ways to say things."
)

PROMPTS = {
    "BASELINE": PROMPT_BASELINE,
    "ANTI-HALLUCINATION": PROMPT_ANTI_HALLUCINATION,
    "ANTI-HALLUC + VARIED": PROMPT_ANTI_HALLUCINATION_VARIED,
}


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


def get_aa_weighted_neighbors(
    db: sqlite3.Connection, id_a: int, id_b: int, degree: dict[int, int], top_k: int = 5
) -> tuple[list[str], float]:
    """Get Adamic-Adar weighted shared neighbors and total AA score."""
    rows = db.execute(
        """
        WITH a_neighbors AS (
            SELECT CASE WHEN source_id = :a THEN target_id ELSE source_id END AS nid
            FROM dj_transition WHERE (source_id = :a OR target_id = :a) AND source_id != target_id
        ),
        b_neighbors AS (
            SELECT CASE WHEN source_id = :b THEN target_id ELSE source_id END AS nid
            FROM dj_transition WHERE (source_id = :b OR target_id = :b) AND source_id != target_id
        )
        SELECT DISTINCT a.id, a.canonical_name
        FROM a_neighbors an JOIN b_neighbors bn ON an.nid = bn.nid
        JOIN artist a ON a.id = an.nid
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
    total_aa = sum(s for _, s in scored)
    top_names = [name for name, _ in scored[:top_k]]
    return top_names, total_aa


def get_artist_meta(db: sqlite3.Connection, artist_id: int, max_styles: int = 5) -> dict:
    row = db.execute(
        "SELECT canonical_name, genre, total_plays FROM artist WHERE id = ?", (artist_id,)
    ).fetchone()
    if not row:
        return {}

    meta: dict = {"name": row["canonical_name"], "genre": row["genre"], "total_plays": row["total_plays"]}

    try:
        styles = db.execute(
            "SELECT style_tag FROM artist_style WHERE artist_id = ? ORDER BY style_tag",
            (artist_id,),
        ).fetchall()
        style_list = [r["style_tag"] for r in styles]
        if style_list:
            meta["styles"] = style_list[:max_styles]
    except sqlite3.OperationalError:
        pass

    try:
        profile = db.execute(
            "SELECT avg_danceability, voice_instrumental_ratio, recording_count "
            "FROM audio_profile WHERE artist_id = ?",
            (artist_id,),
        ).fetchone()
        if profile and profile["recording_count"] and profile["recording_count"] > 0:
            meta["audio"] = {
                "danceability": round(profile["avg_danceability"], 2),
                "voice_instrumental": "vocal" if profile["voice_instrumental_ratio"] > 0.5 else "instrumental",
            }
    except sqlite3.OperationalError:
        pass

    return meta


def find_test_pairs(db: sqlite3.Connection, degree: dict[int, int], count: int = 10) -> list[dict]:
    """Find diverse pairs with strong AA scores."""
    candidates = db.execute(
        """
        SELECT a.id, a.canonical_name, a.genre
        FROM artist a
        JOIN dj_transition dt ON (dt.source_id = a.id OR dt.target_id = a.id) AND dt.source_id != dt.target_id
        WHERE a.total_plays >= 400
          AND a.canonical_name NOT LIKE 'Various%'
          AND a.canonical_name NOT LIKE 'V/A%'
          AND a.canonical_name != 'various'
        GROUP BY a.id
        HAVING COUNT(DISTINCT CASE WHEN dt.source_id = a.id THEN dt.target_id ELSE dt.source_id END) >= 15
        ORDER BY RANDOM()
        LIMIT 80
        """,
    ).fetchall()

    pairs = []
    used_ids: set[int] = set()

    for i, a in enumerate(candidates):
        if len(pairs) >= count:
            break
        if a["id"] in used_ids:
            continue
        for b in candidates[i + 1 :]:
            if len(pairs) >= count:
                break
            if b["id"] in used_ids:
                continue

            # No direct edge
            edge = db.execute(
                "SELECT 1 FROM dj_transition WHERE "
                "(source_id = ? AND target_id = ?) OR (source_id = ? AND target_id = ?)",
                (a["id"], b["id"], b["id"], a["id"]),
            ).fetchone()
            if edge:
                continue

            neighbors, aa_total = get_aa_weighted_neighbors(db, a["id"], b["id"], degree)
            # Filter: require minimum AA sum of 0.8 and at least 3 neighbors
            if aa_total < 0.8 or len(neighbors) < 3:
                continue

            pairs.append({
                "id_a": a["id"],
                "id_b": b["id"],
                "neighbors": neighbors,
                "aa_total": aa_total,
            })
            used_ids.add(a["id"])
            used_ids.add(b["id"])
            break

    return pairs


def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("Set ANTHROPIC_API_KEY to run this script.", file=sys.stderr)
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)
    db = get_db()
    degree = compute_degrees(db)

    print("Finding test pairs (AA-filtered)...", file=sys.stderr)
    pairs = find_test_pairs(db, degree, count=10)
    print(f"Found {len(pairs)} pairs", file=sys.stderr)

    for pair in pairs:
        source_meta = get_artist_meta(db, pair["id_a"])
        target_meta = get_artist_meta(db, pair["id_b"])

        prompt_data = {
            "source": source_meta,
            "target": target_meta,
            "relationships": [],
            "sequential_context": {
                "shared_set_neighbors": pair["neighbors"],
            },
        }

        user_message = json.dumps(prompt_data, separators=(",", ":"))

        print(f"{'=' * 70}")
        print(f"{source_meta['name']} / {target_meta['name']}")
        print(f"  genre: {source_meta.get('genre', '?')} / {target_meta.get('genre', '?')}")
        print(f"  plays: {source_meta['total_plays']} / {target_meta['total_plays']}")
        print(f"  AA score: {pair['aa_total']:.3f}")
        print(f"  neighbors (AA-ranked): {', '.join(pair['neighbors'])}")
        if source_meta.get('styles'):
            print(f"  styles A: {', '.join(source_meta['styles'])}")
        if target_meta.get('styles'):
            print(f"  styles B: {', '.join(target_meta['styles'])}")
        print()

        for label, system_prompt in PROMPTS.items():
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=150,
                system=system_prompt,
                messages=[{"role": "user", "content": user_message}],
            )
            narrative = response.content[0].text
            print(f"  [{label}]")
            print(f"  {narrative}")
            print()
            time.sleep(0.3)

    print(f"{'=' * 70}")
    db.close()


if __name__ == "__main__":
    main()
