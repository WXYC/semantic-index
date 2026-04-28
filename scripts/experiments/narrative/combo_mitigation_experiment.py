"""Experiment: test promising mitigation combinations.

Variants:
1. BASELINE — current anti-hallucination prompt (control)
2. ANON+FEWSHOT — anonymized names + few-shot examples
3. FEWSHOT+NAMING — few-shot examples + naming-only instruction
4. ANON+FEWSHOT+NAMING — all three winners combined

Same 2×2×2 matrix (fame × data × genre) with 3 pairs per cell.
Reuses the same pair-finding logic as prior experiments.
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

# --- Few-shot examples (used in anonymized form for ANON variants) ---

FEWSHOT_REAL = (
    "\n\nHere are examples of well-grounded narratives:\n\n"
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

NAMING_INSTRUCTION = (
    "When naming shared set neighbors, state ONLY their names. Do not describe, characterize, or "
    "categorize the neighbors in any way — you have no data about them. Say 'both appear in sets "
    "alongside X, Y, and Z' and stop there. Do not call them 'experimental,' 'introspective,' "
    "'boundary-pushing,' or any other adjective."
)

BASE_INSTRUCTIONS = (
    "You are a music knowledge assistant for WXYC 89.3 FM, a freeform college radio station. "
    "Given structured data about two artists, write 2-3 sentences (under 80 words) explaining "
    "their connection. "
    "CRITICAL: describe each artist ONLY using the styles, audio, and genre fields provided. "
    "Do not draw on outside knowledge about these artists. If a field is missing, do not guess "
    "what it might contain. If you lack data to describe an artist's sound, focus on the "
    "sequential_context instead. "
    "Describe what an artist's music IS, not what it isn't. "
    "Do not quote numerical values from the data. "
    "Africa is a continent, not a genre. Use the specific tradition from the styles when possible."
)

ANON_BASE_INSTRUCTIONS = (
    "You are a music knowledge assistant for WXYC 89.3 FM, a freeform college radio station. "
    "Given structured data about two artists (labeled Artist A and Artist B), write 2-3 sentences "
    "(under 80 words) explaining their connection. "
    "CRITICAL: describe each artist ONLY using the styles, audio, and genre fields provided. "
    "Do not try to identify who Artist A or Artist B might be. Do not draw on outside knowledge. "
    "If a field is missing, do not guess what it might contain. "
    "Describe what an artist's music IS, not what it isn't. "
    "Do not quote numerical values from the data. "
    "Africa is a continent, not a genre. Use the specific tradition from the styles when possible."
)

# Build the four variants
BASELINE = BASE_INSTRUCTIONS

ANON_FEWSHOT = ANON_BASE_INSTRUCTIONS + FEWSHOT_REAL

FEWSHOT_NAMING = BASE_INSTRUCTIONS + " " + NAMING_INSTRUCTION + FEWSHOT_REAL

ANON_FEWSHOT_NAMING = ANON_BASE_INSTRUCTIONS + " " + NAMING_INSTRUCTION + FEWSHOT_REAL

PROMPTS = {
    "BASELINE": {"prompt": BASELINE, "anon": False},
    "ANON+FEWSHOT": {"prompt": ANON_FEWSHOT, "anon": True},
    "FEWSHOT+NAMING": {"prompt": FEWSHOT_NAMING, "anon": False},
    "ANON+FEWSHOT+NAMING": {"prompt": ANON_FEWSHOT_NAMING, "anon": True},
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
    play_clause = "a.total_plays > 800" if fame == "HIGH" else "a.total_plays BETWEEN 150 AND 400"
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


def anonymize(prompt_data):
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


def deanonymize(narrative, name_a, name_b, neighbor_map):
    text = narrative.replace("Artist A", name_a).replace("Artist B", name_b)
    for label, real_name in neighbor_map.items():
        text = text.replace(label, real_name)
    return text


def parse_verdict(text):
    for line in text.strip().split("\n"):
        if line.strip().startswith("VERDICT:"):
            grounded = ungrounded = 0
            for part in line.strip().replace("VERDICT:", "").split(","):
                part = part.strip().lower()
                if "ungrounded" in part:
                    try:
                        ungrounded = int(part.split()[0])
                    except (ValueError, IndexError):
                        pass
                elif "grounded" in part:
                    try:
                        grounded = int(part.split()[0])
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
    n_variants = len(PROMPTS)
    print(f"Running {len(all_pairs)} pairs × {n_variants} variants = {len(all_pairs) * n_variants} narratives\n", file=sys.stderr)

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

        for variant_name, config in PROMPTS.items():
            system_prompt = config["prompt"]
            use_anon = config["anon"]

            if use_anon:
                anon_data, name_a, name_b, neighbor_map = anonymize(prompt_data)
                user_message = json.dumps(anon_data, separators=(",", ":"))
            else:
                user_message = json.dumps(prompt_data, separators=(",", ":"))

            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=150,
                system=system_prompt,
                messages=[{"role": "user", "content": user_message}],
            )
            narrative = response.content[0].text

            if use_anon:
                narrative = deanonymize(narrative, name_a, name_b, neighbor_map)

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

            if cell not in stats[variant_name]:
                stats[variant_name][cell] = {"grounded": 0, "ungrounded": 0, "pairs": 0}
            stats[variant_name][cell]["grounded"] += grounded
            stats[variant_name][cell]["ungrounded"] += ungrounded
            stats[variant_name][cell]["pairs"] += 1

            total = grounded + ungrounded
            pct = ungrounded / total * 100 if total else 0
            print(f"  [{variant_name}] ({grounded}G/{ungrounded}U = {pct:.0f}% halluc)")
            print(f"    {narrative}")
            print()
            time.sleep(0.2)

        # Running totals for this pair
        pair_results = []
        for v in PROMPTS:
            s = stats[v].get(cell, {"grounded": 0, "ungrounded": 0})
            t = s["grounded"] + s["ungrounded"]
            p = s["ungrounded"] / t * 100 if t else 0
            pair_results.append(f"{v}={p:.0f}%")
        print(f"  ({pair_idx + 1}/{len(all_pairs)}) {a['name']} / {b['name']}  [{', '.join(pair_results)}]", file=sys.stderr)

    # Summary
    print(f"\n{'=' * 70}")
    print("SUMMARY: HALLUCINATION RATE BY VARIANT")
    print(f"{'=' * 70}")
    print(f"\n{'Variant':<24s} {'Pairs':>5s} {'Grounded':>8s} {'Ungnd':>6s} {'Halluc%':>8s}")
    print("-" * 54)
    for variant in PROMPTS:
        total_g = sum(s["grounded"] for s in stats[variant].values())
        total_u = sum(s["ungrounded"] for s in stats[variant].values())
        total_p = sum(s["pairs"] for s in stats[variant].values())
        total = total_g + total_u
        pct = total_u / total * 100 if total else 0
        print(f"{variant:<24s} {total_p:>5d} {total_g:>8d} {total_u:>6d} {pct:>7.0f}%")

    # Prior best results for comparison
    print(f"\n  (Prior results for comparison:)")
    print(f"  {'NAMING-ONLY (prior)':<24s} {'23':>5s} {'139':>8s} {'34':>6s} {'20':>7s}%")
    print(f"  {'ANONYMIZED (prior)':<24s} {'23':>5s} {'166':>8s} {'41':>6s} {'20':>7s}%")
    print(f"  {'FEW-SHOT (prior)':<24s} {'23':>5s} {'137':>8s} {'36':>6s} {'21':>7s}%")

    # Per-cell breakdown
    print(f"\n{'=' * 70}")
    print("BREAKDOWN BY CELL")
    print(f"{'=' * 70}")
    all_cells = sorted(set(pair["cell"] for pair in all_pairs))
    for cell in all_cells:
        print(f"\n  {cell}")
        print(f"  {'Variant':<24s} {'G':>4s} {'U':>4s} {'%':>6s}")
        print(f"  {'-' * 40}")
        for variant in PROMPTS:
            s = stats[variant].get(cell, {"grounded": 0, "ungrounded": 0})
            total = s["grounded"] + s["ungrounded"]
            pct = s["ungrounded"] / total * 100 if total else 0
            print(f"  {variant:<24s} {s['grounded']:>4d} {s['ungrounded']:>4d} {pct:>5.0f}%")

    db.close()


if __name__ == "__main__":
    main()
