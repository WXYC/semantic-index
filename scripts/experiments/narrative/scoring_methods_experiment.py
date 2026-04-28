"""Experiment: compare four truthiness scoring methods.

Scores the same narratives with four different grounding methods:
1. CLAIM-RATIO — decompose into claims, binary grounded/ungrounded, compute ratio
2. WEIGHTED-CLAIMS — same but weight by claim type (artist=3, neighbor=1, dj_intent=1)
3. ENTAILMENT — per-claim "does the data entail this? YES/NO" (constrained task)
4. TOKEN-MATCH — mechanical string matching, no model needed

Generates narratives with both BASELINE and ANON+FEWSHOT+NAMING to calibrate:
good narratives should score low, bad ones should score high.
"""

import json
import math
import os
import re
import sqlite3
import sys
import time
import unicodedata
from itertools import product

import anthropic

DB_PATH = "data/wxyc_artist_graph.db"

# --- Prompts (reused from combo experiment) ---

ANON_FEWSHOT_NAMING = (
    "You are a music knowledge assistant for WXYC 89.3 FM, a freeform college radio station. "
    "Given structured data about two artists (labeled Artist A and Artist B), write 2-3 sentences "
    "(under 80 words) explaining their connection. "
    "CRITICAL: describe each artist ONLY using the styles, audio, and genre fields provided. "
    "Do not try to identify who Artist A or Artist B might be. Do not draw on outside knowledge. "
    "If a field is missing, do not guess what it might contain. "
    "When naming shared set neighbors, state ONLY their names. Do not describe, characterize, or "
    "categorize the neighbors in any way — you have no data about them. Say 'both appear in sets "
    "alongside X, Y, and Z' and stop there. Do not call them 'experimental,' 'introspective,' "
    "'boundary-pushing,' or any other adjective. "
    "Describe what an artist's music IS, not what it isn't. "
    "Do not quote numerical values from the data. "
    "Africa is a continent, not a genre. Use the specific tradition from the styles when possible."
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

BASELINE = (
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

# --- Scoring prompts ---

CLAIM_DECOMPOSE_PROMPT = (
    "You are a fact-checking assistant. Decompose the following narrative into individual factual "
    "claims (one per line). For each claim, check whether it is grounded in the provided data.\n\n"
    "Output format — one claim per line:\n"
    "  G: <claim>\n"
    "  U: <claim>\n\n"
    "G = grounded (the claim is stated or directly implied by a data field).\n"
    "U = ungrounded (the claim is not in the provided data).\n\n"
    "Be strict. Describing a neighbor with any adjective is U. Inferring DJ intent is U. "
    "Stating an artist quality not in the styles/audio/genre fields is U.\n\n"
    "End with a count line: COUNTS: Xg Yu"
)

WEIGHTED_DECOMPOSE_PROMPT = (
    "You are a fact-checking assistant. Decompose the following narrative into individual factual "
    "claims (one per line). For each claim, check whether it is grounded and categorize the claim "
    "type.\n\n"
    "Output format — one claim per line:\n"
    "  G|TYPE: <claim>\n"
    "  U|TYPE: <claim>\n\n"
    "G = grounded, U = ungrounded.\n"
    "TYPE is one of:\n"
    "  ARTIST — describes a subject artist's sound, style, or genre\n"
    "  NEIGHBOR — describes or characterizes a shared neighbor\n"
    "  CONTEXT — describes co-occurrence pattern (e.g. 'both appear alongside X')\n"
    "  INTENT — attributes DJ motivation or curation philosophy\n"
    "  STRUCTURE — describes the relationship structure between the artists\n\n"
    "Be strict. Any adjective applied to a neighbor is U|NEIGHBOR. "
    "Any inference about why DJs program artists is U|INTENT.\n\n"
    "End with: COUNTS: Xg Yu"
)

ENTAILMENT_PROMPT = (
    "You are a textual entailment checker. For each claim extracted from the narrative below, "
    "determine: does the provided data ENTAIL this claim?\n\n"
    "ENTAIL means the claim follows directly from the data with no inference or outside knowledge. "
    "If the data says styles include 'Acid Rock' and the claim says 'acid rock textures,' that is "
    "ENTAILED. If the claim says 'boundary-pushing' and nothing in the data mentions boundaries, "
    "that is NOT ENTAILED.\n\n"
    "First, list each claim from the narrative. Then for each:\n"
    "  YES: <claim> — <data field that entails it>\n"
    "  NO: <claim> — <not entailed by any data field>\n\n"
    "End with: COUNTS: X yes, Y no"
)


# --- Shared infrastructure ---

STOP_WORDS = {
    "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for", "of", "with",
    "by", "from", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had",
    "do", "does", "did", "will", "would", "could", "should", "may", "might", "shall",
    "can", "that", "this", "these", "those", "it", "its", "they", "them", "their", "both",
    "each", "all", "any", "some", "no", "not", "more", "most", "other", "into", "over",
    "such", "than", "too", "very", "just", "also", "about", "up", "out", "so", "if",
    "when", "where", "how", "what", "which", "who", "whom", "while", "as", "yet",
    "between", "through", "during", "before", "after", "above", "below", "here", "there",
    "then", "once", "again", "further", "same", "own", "s", "t", "re", "ve", "ll", "d",
    "one", "two", "three", "near", "set", "sets", "music", "artists", "artist", "appear",
    "alongside", "wxyc", "station", "radio", "plays", "work", "genre",
}


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
        "SELECT id, canonical_name, genre, total_plays FROM artist WHERE id = ?", (artist_id,),
    ).fetchone()
    if not row:
        return None
    styles = []
    try:
        style_rows = db.execute(
            "SELECT style_tag FROM artist_style WHERE artist_id = ? ORDER BY style_tag", (artist_id,),
        ).fetchall()
        styles = [r["style_tag"] for r in style_rows]
    except sqlite3.OperationalError:
        pass
    has_audio = False
    audio = {}
    try:
        profile = db.execute(
            "SELECT avg_danceability, voice_instrumental_ratio, recording_count "
            "FROM audio_profile WHERE artist_id = ?", (artist_id,),
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
          AND a.canonical_name != 'various' AND a.canonical_name != 'Unknown'
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


def find_pairs_for_cell(db, degree, fame, data, genre, count=2):
    play_clause = "a.total_plays > 800" if fame == "HIGH" else "a.total_plays BETWEEN 150 AND 400"
    candidates = db.execute(
        f"""
        SELECT a.id FROM artist a
        JOIN dj_transition dt ON (dt.source_id = a.id OR dt.target_id = a.id) AND dt.source_id != dt.target_id
        WHERE {play_clause}
          AND a.canonical_name NOT LIKE 'Various%' AND a.canonical_name NOT LIKE 'V/A%'
          AND a.canonical_name != 'various' AND a.canonical_name != 'Unknown' AND a.genre IS NOT NULL
        GROUP BY a.id
        HAVING COUNT(DISTINCT CASE WHEN dt.source_id = a.id THEN dt.target_id ELSE dt.source_id END) >= 8
        ORDER BY RANDOM() LIMIT 120
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
    pairs, used = [], set()
    for i, a in enumerate(profiles):
        if len(pairs) >= count or a["id"] in used:
            continue
        for b in profiles[i + 1:]:
            if len(pairs) >= count or b["id"] in used:
                break
            if cross and a["genre"] == b["genre"]:
                continue
            if not cross and a["genre"] != b["genre"]:
                continue
            edge = db.execute(
                "SELECT 1 FROM dj_transition WHERE "
                "(source_id=? AND target_id=?) OR (source_id=? AND target_id=?)",
                (a["id"], b["id"], b["id"], a["id"]),
            ).fetchone()
            if edge:
                continue
            neighbors, aa_total = get_aa_neighbors(db, a["id"], b["id"], degree)
            if aa_total < 0.6 or len(neighbors) < 2:
                continue
            pairs.append({"a": a, "b": b, "neighbors": neighbors, "cell": f"fame={fame} data={data} genre={genre}"})
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
    name_a, name_b = anon["source"]["name"], anon["target"]["name"]
    anon["source"]["name"], anon["target"]["name"] = "Artist A", "Artist B"
    nmap = {}
    new_n = []
    for i, n in enumerate(anon["sequential_context"]["shared_set_neighbors"]):
        label = f"Neighbor {i + 1}"
        nmap[label] = n
        new_n.append(label)
    anon["sequential_context"]["shared_set_neighbors"] = new_n
    return anon, name_a, name_b, nmap


def deanonymize(text, name_a, name_b, nmap):
    text = text.replace("Artist A", name_a).replace("Artist B", name_b)
    for label, real in nmap.items():
        text = text.replace(label, real)
    return text


def parse_counts(text):
    """Extract grounded/ungrounded counts from COUNTS line."""
    for line in text.strip().split("\n"):
        line = line.strip().upper()
        if line.startswith("COUNTS:"):
            g = u = 0
            nums = re.findall(r"(\d+)\s*[GY]", line, re.IGNORECASE)
            if nums:
                g = int(nums[0])
            nums_u = re.findall(r"(\d+)\s*[UN]", line, re.IGNORECASE)
            if nums_u:
                u = int(nums_u[0])
            return g, u
    # Fallback: count G:/U: or YES:/NO: lines
    g = len(re.findall(r"^  *(G\||G:| *YES:)", text, re.MULTILINE))
    u = len(re.findall(r"^  *(U\||U:| *NO:)", text, re.MULTILINE))
    return g, u


def score_token_match(narrative: str, prompt_data: dict) -> float:
    """Mechanical token-level grounding score. Returns ungrounded ratio (0=perfect, 1=all ungrounded)."""
    # Build set of grounded terms from input data
    grounded_terms = set()

    for side in ["source", "target"]:
        d = prompt_data[side]
        if d.get("name"):
            for word in d["name"].lower().split():
                if word not in STOP_WORDS and len(word) > 2:
                    grounded_terms.add(word)
        if d.get("genre"):
            grounded_terms.add(d["genre"].lower())
        for style in d.get("styles", []):
            for word in style.lower().split():
                if word not in STOP_WORDS and len(word) > 2:
                    grounded_terms.add(word)
            # Also add the full style as a phrase
            grounded_terms.add(style.lower())
        if d.get("audio"):
            vi = d["audio"].get("voice_instrumental", "")
            if vi:
                grounded_terms.add(vi)
                if vi == "vocal":
                    grounded_terms.update(["vocal", "vocals", "vocal-driven", "singer", "voice"])
                else:
                    grounded_terms.update(["instrumental", "instrument"])

    for neighbor in prompt_data.get("sequential_context", {}).get("shared_set_neighbors", []):
        for word in neighbor.lower().split():
            if word not in STOP_WORDS and len(word) > 2:
                grounded_terms.add(word)

    # Extract content words from narrative
    narrative_lower = narrative.lower()
    # Remove punctuation
    narrative_clean = re.sub(r"[^\w\s-]", " ", narrative_lower)
    words = narrative_clean.split()
    content_words = [w for w in words if w not in STOP_WORDS and len(w) > 2]

    if not content_words:
        return 0.0

    ungrounded = sum(1 for w in content_words if w not in grounded_terms)
    return ungrounded / len(content_words)


def score_weighted_claims(text: str) -> float:
    """Compute weighted score from categorized claims. Returns 0-1."""
    weights = {"ARTIST": 3.0, "NEIGHBOR": 1.0, "CONTEXT": 1.0, "INTENT": 1.0, "STRUCTURE": 2.0}
    total_weight = 0.0
    ungrounded_weight = 0.0
    for line in text.strip().split("\n"):
        line = line.strip()
        match = re.match(r"^(G|U)\|(\w+):", line)
        if match:
            status, claim_type = match.group(1), match.group(2)
            w = weights.get(claim_type, 1.0)
            total_weight += w
            if status == "U":
                ungrounded_weight += w
    if total_weight == 0:
        return 0.0
    return ungrounded_weight / total_weight


def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("Set ANTHROPIC_API_KEY.", file=sys.stderr)
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)
    db = get_db()
    degree = compute_degrees(db)

    # Find pairs: 2 per cell = 16 pairs
    all_pairs = []
    for fame, data, genre in product(["HIGH", "LOW"], ["RICH", "THIN"], ["CROSS", "SAME"]):
        cell = f"fame={fame} data={data} genre={genre}"
        print(f"Finding pairs for [{cell}]...", file=sys.stderr)
        pairs = find_pairs_for_cell(db, degree, fame, data, genre, count=2)
        print(f"  Found {len(pairs)}", file=sys.stderr)
        all_pairs.extend(pairs)

    print(f"\nTotal pairs: {len(all_pairs)}", file=sys.stderr)
    print(f"Generating 2 narratives per pair (BASELINE + BEST), scoring with 4 methods each\n", file=sys.stderr)

    # Collect all results for summary
    results = []

    for pair_idx, pair in enumerate(all_pairs):
        a, b = pair["a"], pair["b"]
        cell = pair["cell"]
        prompt_data = build_prompt_data(pair)

        print(f"{'=' * 70}")
        print(f"[{cell}] {a['name']} / {b['name']}")
        print(f"  styles A: {', '.join(a['styles'][:5]) if a['styles'] else '(none)'}")
        print(f"  styles B: {', '.join(b['styles'][:5]) if b['styles'] else '(none)'}")
        print(f"  neighbors: {', '.join(pair['neighbors'])}")

        # Generate both variants
        narratives = {}

        # BASELINE (no anonymization)
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001", max_tokens=150,
            system=BASELINE,
            messages=[{"role": "user", "content": json.dumps(prompt_data, separators=(",", ":"))}],
        )
        narratives["BASELINE"] = resp.content[0].text
        time.sleep(0.2)

        # BEST (anonymized)
        anon_data, name_a, name_b, nmap = anonymize(prompt_data)
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001", max_tokens=150,
            system=ANON_FEWSHOT_NAMING,
            messages=[{"role": "user", "content": json.dumps(anon_data, separators=(",", ":"))}],
        )
        narratives["BEST"] = deanonymize(resp.content[0].text, name_a, name_b, nmap)
        time.sleep(0.2)

        for variant_name, narrative in narratives.items():
            print(f"\n  [{variant_name}]")
            print(f"  {narrative}")

            verify_data = json.dumps({"narrative": narrative, "provided_data": prompt_data}, indent=2)
            scores = {}

            # Method 1: Claim-ratio
            resp = client.messages.create(
                model="claude-haiku-4-5-20251001", max_tokens=400,
                system=CLAIM_DECOMPOSE_PROMPT,
                messages=[{"role": "user", "content": verify_data}],
            )
            g, u = parse_counts(resp.content[0].text)
            scores["claim_ratio"] = u / (g + u) if (g + u) > 0 else 0.0
            time.sleep(0.15)

            # Method 2: Weighted claims
            resp = client.messages.create(
                model="claude-haiku-4-5-20251001", max_tokens=500,
                system=WEIGHTED_DECOMPOSE_PROMPT,
                messages=[{"role": "user", "content": verify_data}],
            )
            scores["weighted"] = score_weighted_claims(resp.content[0].text)
            time.sleep(0.15)

            # Method 3: Entailment
            resp = client.messages.create(
                model="claude-haiku-4-5-20251001", max_tokens=400,
                system=ENTAILMENT_PROMPT,
                messages=[{"role": "user", "content": verify_data}],
            )
            g_e, u_e = parse_counts(resp.content[0].text)
            scores["entailment"] = u_e / (g_e + u_e) if (g_e + u_e) > 0 else 0.0
            time.sleep(0.15)

            # Method 4: Token match (no API call)
            scores["token_match"] = score_token_match(narrative, prompt_data)

            print(f"  Scores: claim_ratio={scores['claim_ratio']:.2f}  weighted={scores['weighted']:.2f}  "
                  f"entailment={scores['entailment']:.2f}  token_match={scores['token_match']:.2f}")

            results.append({
                "pair": f"{a['name']} / {b['name']}",
                "cell": cell,
                "variant": variant_name,
                "narrative": narrative,
                **scores,
            })

        print(f"\n  ({pair_idx + 1}/{len(all_pairs)} pairs done)", file=sys.stderr)

    # Summary
    print(f"\n{'=' * 70}")
    print("SUMMARY: MEAN SCORES BY VARIANT AND METHOD")
    print(f"{'=' * 70}")
    print(f"\n{'Variant':<10s} {'claim_ratio':>12s} {'weighted':>10s} {'entailment':>12s} {'token_match':>12s}")
    print("-" * 58)

    for variant in ["BASELINE", "BEST"]:
        vr = [r for r in results if r["variant"] == variant]
        if not vr:
            continue
        means = {
            "claim_ratio": sum(r["claim_ratio"] for r in vr) / len(vr),
            "weighted": sum(r["weighted"] for r in vr) / len(vr),
            "entailment": sum(r["entailment"] for r in vr) / len(vr),
            "token_match": sum(r["token_match"] for r in vr) / len(vr),
        }
        print(f"{variant:<10s} {means['claim_ratio']:>12.3f} {means['weighted']:>10.3f} "
              f"{means['entailment']:>12.3f} {means['token_match']:>12.3f}")

    # Correlation: do methods agree on which narratives are worse?
    print(f"\n{'=' * 70}")
    print("CALIBRATION: DOES BASELINE SCORE HIGHER (WORSE) THAN BEST?")
    print(f"{'=' * 70}")
    for method in ["claim_ratio", "weighted", "entailment", "token_match"]:
        baseline_scores = [r[method] for r in results if r["variant"] == "BASELINE"]
        best_scores = [r[method] for r in results if r["variant"] == "BEST"]
        if baseline_scores and best_scores:
            b_mean = sum(baseline_scores) / len(baseline_scores)
            best_mean = sum(best_scores) / len(best_scores)
            correct = sum(1 for bs, be in zip(baseline_scores, best_scores) if bs >= be)
            total = min(len(baseline_scores), len(best_scores))
            print(f"  {method:<14s}: BASELINE={b_mean:.3f}  BEST={best_mean:.3f}  "
                  f"baseline≥best in {correct}/{total} pairs ({correct / total * 100:.0f}%)")

    # Per-pair detail
    print(f"\n{'=' * 70}")
    print("PER-PAIR SCORES")
    print(f"{'=' * 70}")
    print(f"{'Pair':<40s} {'Var':<8s} {'claim':>6s} {'wght':>6s} {'entl':>6s} {'tokn':>6s}")
    print("-" * 74)
    for r in results:
        pair_short = r["pair"][:38]
        print(f"{pair_short:<40s} {r['variant']:<8s} {r['claim_ratio']:>5.2f} {r['weighted']:>6.2f} "
              f"{r['entailment']:>5.2f} {r['token_match']:>6.2f}")

    db.close()


if __name__ == "__main__":
    main()
