"""Experiment: test four hallucination mitigations against the same matrix.

Variants:
1. BASELINE — current anti-hallucination prompt
2. NAMING-ONLY — instruct model to name neighbors without characterizing them
3. PATTERN-NOT-INTENT — describe co-occurrence patterns, not DJ motivations
4. ANONYMIZED — strip artist names, generate with Artist A/B, substitute back
5. FEW-SHOT — add gold-standard example narratives to the system prompt

Same 2×2×2 matrix (fame × data × genre) with 3 pairs per cell.
Each pair gets all 5 variants + verification.
"""

import json
import math
import os
import re
import sqlite3
import sys
import time
from itertools import product

import anthropic

DB_PATH = "data/wxyc_artist_graph.db"

# --- Prompt variants ---

BASELINE = (
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

NAMING_ONLY = (
    "You are a music knowledge assistant for WXYC 89.3 FM, a freeform college radio station. "
    "Given structured data about two artists, write 2-3 sentences (under 80 words) explaining "
    "their connection. "
    "CRITICAL: describe each artist ONLY using the styles, audio, and genre fields provided. "
    "Do not draw on outside knowledge about these artists. If a field is missing, do not guess "
    "what it might contain. If you lack data to describe an artist's sound, focus on the "
    "sequential_context instead. "
    "When naming shared set neighbors, state ONLY their names. Do not describe, characterize, or "
    "categorize the neighbors in any way — you have no data about them. Say 'both appear in sets "
    "alongside X, Y, and Z' and stop there. Do not call them 'experimental,' 'introspective,' "
    "'boundary-pushing,' or any other adjective. "
    "Describe what an artist's music IS, not what it isn't. "
    "Do not quote numerical values from the data. "
    "Africa is a continent, not a genre. Use the specific tradition from the styles when possible."
)

PATTERN_NOT_INTENT = (
    "You are a music knowledge assistant for WXYC 89.3 FM, a freeform college radio station. "
    "Given structured data about two artists, write 2-3 sentences (under 80 words) explaining "
    "their connection. "
    "CRITICAL: describe each artist ONLY using the styles, audio, and genre fields provided. "
    "Do not draw on outside knowledge about these artists. If a field is missing, do not guess "
    "what it might contain. If you lack data to describe an artist's sound, focus on the "
    "sequential_context instead. "
    "When describing sequential context, describe ONLY the observed pattern: 'both tend to appear "
    "in sets alongside X and Y.' Do NOT infer DJ intent, motivation, or curation philosophy. "
    "Never say 'DJs value,' 'DJs pair them,' 'DJs reach for,' 'suggesting DJs,' or 'programmers "
    "seeking.' The data shows co-occurrence, not intent. "
    "Describe what an artist's music IS, not what it isn't. "
    "Do not quote numerical values from the data. "
    "Africa is a continent, not a genre. Use the specific tradition from the styles when possible."
)

ANONYMIZED_TEMPLATE = (
    "You are a music knowledge assistant for WXYC 89.3 FM, a freeform college radio station. "
    "Given structured data about two artists (labeled Artist A and Artist B), write 2-3 sentences "
    "(under 80 words) explaining their connection. "
    "CRITICAL: describe each artist ONLY using the styles, audio, and genre fields provided. "
    "Do not try to identify who Artist A or Artist B might be. Do not draw on outside knowledge. "
    "If a field is missing, do not guess what it might contain. "
    "When sequential_context is present, describe how these artists tend to appear in similar "
    "set contexts. Name the shared neighbors exactly as given. "
    "Describe what an artist's music IS, not what it isn't. "
    "Do not quote numerical values from the data. "
    "Africa is a continent, not a genre. Use the specific tradition from the styles when possible."
)

FEW_SHOT = (
    "You are a music knowledge assistant for WXYC 89.3 FM, a freeform college radio station. "
    "Given structured data about two artists, write 2-3 sentences (under 80 words) explaining "
    "their connection. "
    "CRITICAL: describe each artist ONLY using the styles, audio, and genre fields provided. "
    "Do not draw on outside knowledge about these artists. If a field is missing, do not guess. "
    "Describe what an artist's music IS, not what it isn't. "
    "Do not quote numerical values. "
    "Africa is a continent, not a genre. Use the specific tradition from the styles when possible.\n\n"
    "Here are examples of well-grounded narratives:\n\n"
    "Example 1 (rich data):\n"
    "Data: Artist A — genre: Rock, styles: [Alternative Rock, Garage Rock, Indie Rock, Lo-Fi], "
    "audio: vocal. Artist B — genre: Rock, styles: [Acid Rock, Alternative Rock, Ambient, Art Rock, "
    "Avantgarde], audio: instrumental. Shared neighbors: U.S. Maple, Polvo.\n"
    "Narrative: \"Artist A crafts vocal-driven alternative and garage rock with lo-fi textures. "
    "Artist B pursues instrumental acid rock and avant-garde soundscapes with ambient elements. "
    "Both appear in sets alongside U.S. Maple and Polvo.\"\n\n"
    "Example 2 (thin data):\n"
    "Data: Artist A — genre: Electronic, styles: (none), audio: instrumental. "
    "Artist B — genre: Hiphop, styles: (none), audio: vocal. "
    "Shared neighbors: Noname, Earthly.\n"
    "Narrative: \"One brings instrumental electronic music, the other vocal-driven hip-hop. "
    "Both appear in WXYC sets alongside Noname and Earthly, suggesting they fill similar roles "
    "in programming despite different approaches.\"\n\n"
    "Notice: neighbors are named but never characterized. No DJ intent is attributed. "
    "Claims come only from the data fields provided."
)

COMBINED = (
    "You are a music knowledge assistant for WXYC 89.3 FM, a freeform college radio station. "
    "Given structured data about two artists, write 2-3 sentences (under 80 words) explaining "
    "their connection. "
    "CRITICAL: describe each artist ONLY using the styles, audio, and genre fields provided. "
    "Do not draw on outside knowledge about these artists. If a field is missing, do not guess "
    "what it might contain. If you lack data to describe an artist's sound, focus on the "
    "sequential_context instead. "
    "When naming shared set neighbors, state ONLY their names — no adjectives, no characterization. "
    "When describing sequential context, describe ONLY the observed pattern. Do NOT infer DJ "
    "intent or motivation. Never say 'DJs value,' 'DJs pair them,' 'DJs reach for,' or "
    "'suggesting DJs.' The data shows co-occurrence, not intent. "
    "Describe what an artist's music IS, not what it isn't. "
    "Do not quote numerical values from the data. "
    "Africa is a continent, not a genre. Use the specific tradition from the styles when possible."
)

PROMPTS = {
    "BASELINE": BASELINE,
    "NAMING-ONLY": NAMING_ONLY,
    "PATTERN-NOT-INTENT": PATTERN_NOT_INTENT,
    "ANONYMIZED": ANONYMIZED_TEMPLATE,
    "FEW-SHOT": FEW_SHOT,
    "COMBINED": COMBINED,
}

VERIFY_PROMPT = (
    "You are a strict fact-checking assistant. You will receive a narrative about two artists and "
    "the structured data used to generate it. Identify every factual claim and check whether it "
    "is grounded in the provided data.\n\n"
    "For each claim, output one line:\n"
    "  GROUNDED: <claim> — <which data field supports it>\n"
    "  UNGROUNDED: <claim> — <not in the provided data>\n\n"
    "Be strict. Describing a shared neighbor with ANY adjective not in the data is UNGROUNDED. "
    "Inferring DJ intent, motivation, or curation philosophy is UNGROUNDED. "
    "Stating an artist has a quality not listed in their styles/audio/genre fields is UNGROUNDED.\n\n"
    "At the end, output:\n"
    "  VERDICT: X grounded, Y ungrounded"
)


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def compute_degrees(db):
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


def get_artist_profile(db, artist_id):
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
        "id": row["id"], "name": row["canonical_name"], "genre": row["genre"],
        "total_plays": row["total_plays"], "styles": styles, "has_audio": has_audio,
        "audio": audio, "is_rich": len(styles) >= 3 and has_audio,
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


def find_pairs_for_cell(db, degree, fame, data, genre, count=3):
    if fame == "HIGH":
        play_clause = "a.total_plays > 800"
    else:
        play_clause = "a.total_plays BETWEEN 150 AND 400"

    candidates = db.execute(
        f"""
        SELECT a.id FROM artist a
        JOIN dj_transition dt ON (dt.source_id = a.id OR dt.target_id = a.id) AND dt.source_id != dt.target_id
        WHERE {play_clause}
          AND a.canonical_name NOT LIKE 'Various%'
          AND a.canonical_name NOT LIKE 'V/A%'
          AND a.canonical_name != 'various'
          AND a.canonical_name != 'Unknown'
          AND a.genre IS NOT NULL
        GROUP BY a.id
        HAVING COUNT(DISTINCT CASE WHEN dt.source_id = a.id THEN dt.target_id ELSE dt.source_id END) >= 8
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

    cross = genre == "CROSS"
    pairs = []
    used = set()
    for i, a in enumerate(profiles):
        if len(pairs) >= count:
            break
        if a["id"] in used:
            continue
        for b in profiles[i + 1:]:
            if len(pairs) >= count:
                break
            if b["id"] in used:
                continue
            if cross and a["genre"] == b["genre"]:
                continue
            if not cross and a["genre"] != b["genre"]:
                continue
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
                "a": a, "b": b, "neighbors": neighbors, "aa_total": aa_total,
                "cell": f"fame={fame} data={data} genre={genre}",
            })
            used.add(a["id"])
            used.add(b["id"])
            break
    return pairs


def build_prompt_data(pair):
    a, b = pair["a"], pair["b"]
    source = {"name": a["name"], "genre": a["genre"], "total_plays": a["total_plays"]}
    if a["styles"]:
        source["styles"] = a["styles"][:5]
    if a["has_audio"]:
        source["audio"] = a["audio"]
    target = {"name": b["name"], "genre": b["genre"], "total_plays": b["total_plays"]}
    if b["styles"]:
        target["styles"] = b["styles"][:5]
    if b["has_audio"]:
        target["audio"] = b["audio"]
    return {
        "source": source, "target": target, "relationships": [],
        "sequential_context": {"shared_set_neighbors": pair["neighbors"]},
    }


def anonymize_prompt_data(prompt_data):
    """Replace artist names with Artist A/B and neighbor names with Neighbor 1/2/etc."""
    anon = json.loads(json.dumps(prompt_data))
    name_a = anon["source"]["name"]
    name_b = anon["target"]["name"]
    anon["source"]["name"] = "Artist A"
    anon["target"]["name"] = "Artist B"
    neighbor_map = {}
    new_neighbors = []
    for i, n in enumerate(anon["sequential_context"]["shared_set_neighbors"]):
        label = f"Neighbor {i + 1}"
        neighbor_map[label] = n
        new_neighbors.append(label)
    anon["sequential_context"]["shared_set_neighbors"] = new_neighbors
    return anon, name_a, name_b, neighbor_map


def deanonymize_narrative(narrative, name_a, name_b, neighbor_map):
    """Substitute real names back into the narrative."""
    text = narrative.replace("Artist A", name_a).replace("Artist B", name_b)
    for label, real_name in neighbor_map.items():
        text = text.replace(label, real_name)
    return text


def parse_verdict(verification_text):
    """Extract grounded/ungrounded counts from verdict line."""
    for line in verification_text.strip().split("\n"):
        if line.strip().startswith("VERDICT:"):
            grounded = 0
            ungrounded = 0
            parts = line.strip().replace("VERDICT:", "").strip()
            for part in parts.split(","):
                part = part.strip().lower()
                if "grounded" in part and "ungrounded" not in part:
                    try:
                        grounded = int(part.split()[0])
                    except (ValueError, IndexError):
                        pass
                elif "ungrounded" in part:
                    try:
                        ungrounded = int(part.split()[0])
                    except (ValueError, IndexError):
                        pass
            return grounded, ungrounded
    return 0, 0


def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("Set ANTHROPIC_API_KEY to run this script.", file=sys.stderr)
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)
    db = get_db()
    degree = compute_degrees(db)

    all_pairs = []
    for fame, data, genre in product(["HIGH", "LOW"], ["RICH", "THIN"], ["CROSS", "SAME"]):
        cell = f"fame={fame} data={data} genre={genre}"
        print(f"Finding pairs for [{cell}]...", file=sys.stderr)
        pairs = find_pairs_for_cell(db, degree, fame, data, genre, count=3)
        print(f"  Found {len(pairs)}", file=sys.stderr)
        all_pairs.extend(pairs)

    print(f"\nTotal pairs: {len(all_pairs)}", file=sys.stderr)
    print(f"Running {len(all_pairs)} pairs × {len(PROMPTS)} variants = {len(all_pairs) * len(PROMPTS)} narratives\n", file=sys.stderr)

    # Track stats: variant -> cell -> {grounded, ungrounded, pairs}
    stats = {v: {} for v in PROMPTS}

    for pair_idx, pair in enumerate(all_pairs):
        a, b = pair["a"], pair["b"]
        cell = pair["cell"]
        prompt_data = build_prompt_data(pair)

        print(f"{'=' * 70}")
        print(f"[{cell}] {a['name']} / {b['name']}")
        print(f"  plays: {a['total_plays']} / {b['total_plays']}")
        print(f"  styles A: {', '.join(a['styles'][:5]) if a['styles'] else '(none)'}")
        print(f"  styles B: {', '.join(b['styles'][:5]) if b['styles'] else '(none)'}")
        print(f"  audio: {a['has_audio']} / {b['has_audio']}")
        print(f"  neighbors: {', '.join(pair['neighbors'])}")
        print()

        for variant_name, system_prompt in PROMPTS.items():
            # Build the user message
            if variant_name == "ANONYMIZED":
                anon_data, name_a, name_b, neighbor_map = anonymize_prompt_data(prompt_data)
                user_message = json.dumps(anon_data, separators=(",", ":"))
            else:
                user_message = json.dumps(prompt_data, separators=(",", ":"))

            # Generate
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=150,
                system=system_prompt,
                messages=[{"role": "user", "content": user_message}],
            )
            narrative = response.content[0].text

            # Deanonymize if needed
            if variant_name == "ANONYMIZED":
                narrative = deanonymize_narrative(narrative, name_a, name_b, neighbor_map)

            # Verify (always against the real prompt data)
            verify_input = json.dumps({
                "narrative": narrative,
                "provided_data": prompt_data,
            }, indent=2)

            verify_response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=400,
                system=VERIFY_PROMPT,
                messages=[{"role": "user", "content": verify_input}],
            )
            verification = verify_response.content[0].text
            grounded, ungrounded = parse_verdict(verification)

            # Track stats
            if cell not in stats[variant_name]:
                stats[variant_name][cell] = {"grounded": 0, "ungrounded": 0, "pairs": 0}
            stats[variant_name][cell]["grounded"] += grounded
            stats[variant_name][cell]["ungrounded"] += ungrounded
            stats[variant_name][cell]["pairs"] += 1

            # Print
            total = grounded + ungrounded
            pct = ungrounded / total * 100 if total else 0
            print(f"  [{variant_name}] ({grounded}G/{ungrounded}U = {pct:.0f}% halluc)")
            print(f"    {narrative}")
            print()

            time.sleep(0.2)

        print(f"  ({pair_idx + 1}/{len(all_pairs)} pairs done)", file=sys.stderr)

    # Summary tables
    print(f"\n{'=' * 70}")
    print("SUMMARY: HALLUCINATION RATE BY VARIANT")
    print(f"{'=' * 70}")

    # Aggregate across all cells per variant
    print(f"\n{'Variant':<22s} {'Pairs':>5s} {'Grounded':>8s} {'Ungnd':>6s} {'Halluc%':>8s}")
    print("-" * 52)
    for variant in PROMPTS:
        total_g = sum(s["grounded"] for s in stats[variant].values())
        total_u = sum(s["ungrounded"] for s in stats[variant].values())
        total_p = sum(s["pairs"] for s in stats[variant].values())
        total = total_g + total_u
        pct = total_u / total * 100 if total else 0
        print(f"{variant:<22s} {total_p:>5d} {total_g:>8d} {total_u:>6d} {pct:>7.0f}%")

    # Per-cell breakdown for each variant
    print(f"\n{'=' * 70}")
    print("BREAKDOWN BY CELL")
    print(f"{'=' * 70}")

    all_cells = sorted(set(pair["cell"] for pair in all_pairs))
    for cell in all_cells:
        print(f"\n  {cell}")
        print(f"  {'Variant':<22s} {'G':>4s} {'U':>4s} {'%':>6s}")
        print(f"  {'-' * 38}")
        for variant in PROMPTS:
            s = stats[variant].get(cell, {"grounded": 0, "ungrounded": 0})
            total = s["grounded"] + s["ungrounded"]
            pct = s["ungrounded"] / total * 100 if total else 0
            print(f"  {variant:<22s} {s['grounded']:>4d} {s['ungrounded']:>4d} {pct:>5.0f}%")

    db.close()


if __name__ == "__main__":
    main()
