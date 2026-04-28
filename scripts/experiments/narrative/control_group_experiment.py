"""Control group: score narratives for pairs expected to be highly grounded.

Selects pairs with:
- Direct DJ transition edge (real relationship, not inferred)
- Rich data on both sides (3+ styles, audio profile)
- Same genre
- Moderate fame (400-1200 plays — well-known enough to have data, not so famous
  that pretraining dominates)

Generates narratives with ANON+FEWSHOT+NAMING and scores with both token-match
and claim-ratio. These should score LOW (good). If they don't, the scoring
methods are miscalibrated.
"""

import json
import math
import os
import re
import sqlite3
import sys
import time

import anthropic

DB_PATH = "data/wxyc_artist_graph.db"

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
    "alongside", "wxyc", "station", "radio", "plays", "work", "genre", "both", "shared",
}

ANON_FEWSHOT_NAMING = (
    "You are a music knowledge assistant for WXYC 89.3 FM, a freeform college radio station. "
    "Given structured data about two artists (labeled Artist A and Artist B), write 2-3 sentences "
    "(under 80 words) explaining their connection. "
    "CRITICAL: describe each artist ONLY using the styles, audio, and genre fields provided. "
    "Do not try to identify who Artist A or Artist B might be. Do not draw on outside knowledge. "
    "If a field is missing, do not guess what it might contain. "
    "When naming shared set neighbors, state ONLY their names. Do not describe, characterize, or "
    "categorize the neighbors in any way — you have no data about them. "
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

CLAIM_DECOMPOSE_PROMPT = (
    "You are a strict fact-checking assistant. Decompose the following narrative into individual "
    "factual claims (one per line). For each claim, check whether it is grounded in the provided "
    "data.\n\n"
    "Output format — one claim per line:\n"
    "  G: <claim>\n"
    "  U: <claim>\n\n"
    "G = grounded (stated or directly implied by a data field).\n"
    "U = ungrounded (not in the provided data).\n\n"
    "Be strict. Describing a neighbor with any adjective is U. Inferring DJ intent is U. "
    "Stating an artist quality not in the styles/audio/genre fields is U.\n\n"
    "End with: COUNTS: Xg Yu"
)


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


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
        "audio": audio,
    }


def find_control_pairs(db, count=20):
    """Find 'easy' pairs: direct edge, rich data, same genre, moderate fame."""
    rows = db.execute(
        """
        SELECT dt.source_id, dt.target_id, dt.pmi, dt.raw_count,
               a1.canonical_name AS name_a, a1.genre AS genre_a, a1.total_plays AS plays_a,
               a2.canonical_name AS name_b, a2.genre AS genre_b, a2.total_plays AS plays_b
        FROM dj_transition dt
        JOIN artist a1 ON a1.id = dt.source_id
        JOIN artist a2 ON a2.id = dt.target_id
        WHERE dt.source_id != dt.target_id
          AND dt.raw_count >= 3
          AND a1.genre = a2.genre
          AND a1.genre IS NOT NULL
          AND a1.total_plays BETWEEN 400 AND 1200
          AND a2.total_plays BETWEEN 400 AND 1200
          AND a1.canonical_name NOT LIKE 'Various%'
          AND a2.canonical_name NOT LIKE 'Various%'
        ORDER BY RANDOM()
        LIMIT 200
        """,
    ).fetchall()

    pairs = []
    used = set()
    for r in rows:
        if len(pairs) >= count:
            break
        if r["source_id"] in used or r["target_id"] in used:
            continue

        a = get_artist_profile(db, r["source_id"])
        b = get_artist_profile(db, r["target_id"])
        if not a or not b:
            continue

        # Both must be rich
        if len(a["styles"]) < 3 or not a["has_audio"]:
            continue
        if len(b["styles"]) < 3 or not b["has_audio"]:
            continue

        pairs.append({
            "a": a, "b": b,
            "pmi": r["pmi"], "raw_count": r["raw_count"],
        })
        used.add(r["source_id"])
        used.add(r["target_id"])

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

    # Include the actual relationship data (these pairs HAVE a direct edge)
    return {
        "source": source, "target": target,
        "relationships": [{"type": "djTransition", "raw_count": pair["raw_count"], "pmi": round(pair["pmi"], 2)}],
    }


def anonymize(prompt_data):
    anon = json.loads(json.dumps(prompt_data))
    name_a, name_b = anon["source"]["name"], anon["target"]["name"]
    anon["source"]["name"], anon["target"]["name"] = "Artist A", "Artist B"
    return anon, name_a, name_b


def deanonymize(text, name_a, name_b):
    return text.replace("Artist A", name_a).replace("Artist B", name_b)


def build_grounded_terms(prompt_data):
    terms = set()
    for side in ["source", "target"]:
        d = prompt_data[side]
        if d.get("name"):
            for word in d["name"].lower().split():
                if word not in STOP_WORDS and len(word) > 2:
                    terms.add(word)
        if d.get("genre"):
            terms.add(d["genre"].lower())
        for style in d.get("styles", []):
            for word in style.lower().split():
                if word not in STOP_WORDS and len(word) > 2:
                    terms.add(word)
            terms.add(style.lower())
        if d.get("audio"):
            vi = d["audio"].get("voice_instrumental", "")
            if vi:
                terms.add(vi)
                if vi == "vocal":
                    terms.update(["vocal", "vocals", "vocal-driven", "singer", "voice"])
                else:
                    terms.update(["instrumental", "instrument"])
    # Add relationship terms
    for rel in prompt_data.get("relationships", []):
        terms.update(["transition", "transitions", "back-to-back", "times", "appeared"])
        if rel.get("raw_count"):
            terms.add(str(rel["raw_count"]))
    return terms


def score_token_match(narrative, grounded_terms):
    narrative_clean = re.sub(r"[^\w\s-]", " ", narrative.lower())
    words = narrative_clean.split()
    content_words = [w for w in words if w not in STOP_WORDS and len(w) > 2]
    if not content_words:
        return 0.0
    ungrounded = sum(1 for w in content_words if w not in grounded_terms)
    return ungrounded / len(content_words)


def parse_claim_counts(text):
    for line in text.strip().split("\n"):
        line_up = line.strip().upper()
        if line_up.startswith("COUNTS:"):
            g = u = 0
            nums_g = re.findall(r"(\d+)\s*G", line_up)
            nums_u = re.findall(r"(\d+)\s*U", line_up)
            if nums_g:
                g = int(nums_g[0])
            if nums_u:
                u = int(nums_u[0])
            return g, u
    g = len(re.findall(r"^ *G:", text, re.MULTILINE))
    u = len(re.findall(r"^ *U:", text, re.MULTILINE))
    return g, u


def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("Set ANTHROPIC_API_KEY.", file=sys.stderr)
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)
    db = get_db()

    print("Finding control pairs (direct edge, rich data, same genre, moderate fame)...", file=sys.stderr)
    pairs = find_control_pairs(db, count=20)
    print(f"Found {len(pairs)} pairs\n", file=sys.stderr)

    results = []

    for pair_idx, pair in enumerate(pairs):
        a, b = pair["a"], pair["b"]
        prompt_data = build_prompt_data(pair)
        grounded_terms = build_grounded_terms(prompt_data)

        print(f"{'=' * 70}")
        print(f"{a['name']} / {b['name']}")
        print(f"  genre: {a['genre']} / {b['genre']}")
        print(f"  plays: {a['total_plays']} / {b['total_plays']}")
        print(f"  edge: {pair['raw_count']} times, PMI {pair['pmi']:.2f}")
        print(f"  styles A: {', '.join(a['styles'][:5])}")
        print(f"  styles B: {', '.join(b['styles'][:5])}")
        print(f"  audio: {a['has_audio']} / {b['has_audio']}")

        # Generate (anonymized)
        anon_data, name_a, name_b = anonymize(prompt_data)
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001", max_tokens=150,
            system=ANON_FEWSHOT_NAMING,
            messages=[{"role": "user", "content": json.dumps(anon_data, separators=(",", ":"))}],
        )
        narrative = deanonymize(resp.content[0].text, name_a, name_b)
        time.sleep(0.2)

        # Token-match score
        token_score = score_token_match(narrative, grounded_terms)

        # Claim-ratio score
        verify_data = json.dumps({"narrative": narrative, "provided_data": prompt_data}, indent=2)
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001", max_tokens=400,
            system=CLAIM_DECOMPOSE_PROMPT,
            messages=[{"role": "user", "content": verify_data}],
        )
        verification = resp.content[0].text
        g, u = parse_claim_counts(verification)
        claim_score = u / (g + u) if (g + u) > 0 else 0.0
        time.sleep(0.2)

        print(f"\n  NARRATIVE:")
        print(f"  {narrative}")
        print(f"\n  SCORES: token={token_score:.2f}  claim={claim_score:.2f}  ({g}G/{u}U)")
        print(f"\n  VERIFICATION:")
        for line in verification.strip().split("\n"):
            print(f"  {line}")
        print()

        results.append({
            "pair": f"{a['name']} / {b['name']}",
            "genre": a["genre"],
            "token_score": token_score,
            "claim_score": claim_score,
            "grounded": g,
            "ungrounded": u,
        })

        print(f"  ({pair_idx + 1}/{len(pairs)} done)", file=sys.stderr)

    # Summary
    print(f"\n{'=' * 70}")
    print("CONTROL GROUP SUMMARY")
    print(f"{'=' * 70}")

    n = len(results)
    mean_token = sum(r["token_score"] for r in results) / n
    mean_claim = sum(r["claim_score"] for r in results) / n
    below_token_thresh = sum(1 for r in results if r["token_score"] < 0.50)
    below_claim_thresh = sum(1 for r in results if r["claim_score"] < 0.20)

    print(f"\n  Pairs: {n}")
    print(f"  Mean token score: {mean_token:.3f}  (prior experimental mean: 0.362)")
    print(f"  Mean claim score: {mean_claim:.3f}  (prior experimental mean: 0.252)")
    print(f"  Below token threshold (0.50): {below_token_thresh}/{n} ({below_token_thresh/n*100:.0f}%)")
    print(f"  Below claim threshold (0.20): {below_claim_thresh}/{n} ({below_claim_thresh/n*100:.0f}%)")

    # Compare with prior experimental results
    print(f"\n  Comparison with experimental (no-edge) pairs:")
    print(f"  {'':>20s} {'Control':>10s} {'Experimental':>12s}")
    print(f"  {'Mean token':>20s} {mean_token:>10.3f} {'0.362':>12s}")
    print(f"  {'Mean claim':>20s} {mean_claim:>10.3f} {'0.252':>12s}")

    # Per-pair detail
    print(f"\n{'=' * 70}")
    print(f"{'Pair':<45s} {'Genre':<12s} {'Token':>6s} {'Claim':>6s}")
    print("-" * 75)
    for r in sorted(results, key=lambda x: x["token_score"]):
        pair_short = r["pair"][:43]
        print(f"{pair_short:<45s} {r['genre']:<12s} {r['token_score']:>5.2f} {r['claim_score']:>6.2f}")

    db.close()


if __name__ == "__main__":
    main()
