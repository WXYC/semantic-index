"""Experiment: calibrated scoring methods against control + experimental pairs.

Fixes from control group findings:
1. Claim-ratio v2: verifier prompt allows reasonable paraphrasing, qualitative
   descriptions of numeric fields, and summaries of style lists.
2. Token-match v2: adds an allowlist of common narrative prose terms, plus
   stemming-aware matching (e.g. "ambient" grounds "ambience", "atmospheric").

Runs both v1 (original) and v2 (calibrated) on the same pairs so we can
compare directly.
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

# --- Prose allowlist: words that are acceptable narrative vocabulary ---
# These are common descriptive terms used to paraphrase data fields.
# They should not count as ungrounded.
PROSE_ALLOWLIST = {
    # Descriptive texture words
    "textures", "texture", "textural", "soundscapes", "soundscape", "landscapes",
    "atmospherics", "atmospheric", "ambience", "arrangements", "arrangement",
    "sensibilities", "sensibility", "aesthetics", "aesthetic",
    # Common music description
    "grooves", "groove", "rhythmic", "rhythms", "rhythm", "melodic", "melodies",
    "melody", "harmonic", "harmonies", "harmony", "sonic", "sonically",
    "layered", "layers", "layer", "driven", "rooted", "infused", "inflected",
    "oriented", "leaning", "tinged", "informed", "influenced",
    # Structure/role words
    "blending", "blend", "blends", "channels", "channel", "crafts", "craft",
    "delivers", "explores", "exploring", "spans", "spanning", "pursues",
    "fusing", "fusion", "merging", "combines", "combining",
    # Degree/comparison
    "moderate", "moderately", "restrained", "higher", "lower", "similarly",
    "notably", "prominent", "subtle", "heavy", "light", "dense", "sparse",
    "propulsive", "subdued", "energetic", "contemplative", "meditative",
    # Structural
    "frameworks", "framework", "foundation", "foundations", "tradition",
    "traditions", "traditional", "approach", "approaches", "elements",
    "characteristics", "qualities", "components",
    # Connection/relationship
    "complementary", "contrasting", "distinct", "overlapping", "parallel",
    "different", "similar", "comparable", "shared", "common",
    # DJ/programming (these are part of the task context)
    "programming", "rotation", "transitions", "transition", "sets",
    "freeform", "programming", "dj", "djs",
    # Voice descriptions (reasonable from vocal/instrumental field)
    "vocal-driven", "voice", "singer", "vocals", "vocal",
    "production", "producer", "produced",
}

# --- Stem mappings: data term -> additional grounded narrative terms ---
STEM_MAP = {
    "ambient": {"ambience", "atmospheric", "atmospherics", "atmosphere"},
    "acoustic": {"acoustics", "unplugged"},
    "experimental": {"experimentation", "experiments", "experimenting"},
    "electronic": {"electronics", "electro-acoustic"},
    "instrumental": {"instrument", "instruments", "instrumentation"},
    "alternative": {"alt", "alternative"},
    "psychedelic": {"psychedelia", "psych"},
    "folk": {"folk-inflected", "folksy"},
    "jazz": {"jazzy", "jazz-inflected", "jazz-informed"},
    "blues": {"bluesy", "blues-inflected"},
    "punk": {"punky", "punk-inflected"},
    "funk": {"funky", "funk-inflected"},
    "soul": {"soulful", "soul-inflected"},
    "rock": {"rock-oriented", "rocking"},
    "pop": {"poppy", "pop-oriented"},
    "dance": {"danceable", "dance-oriented"},
    "noise": {"noisy", "noise-inflected"},
    "drone": {"droning", "drone-based"},
    "disco": {"disco-inflected", "discotheque"},
    "dub": {"dubby", "dub-inflected"},
    "minimal": {"minimalist", "minimalism"},
    "abstract": {"abstraction", "abstractionist"},
    "avantgarde": {"avant-garde", "avant"},
    "downtempo": {"down-tempo", "slow-tempo"},
    "lo-fi": {"lofi", "lo-fi"},
    "vocal": {"vocal-driven", "vocals", "voice", "singer", "singing"},
}

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

CLAIM_V1_PROMPT = (
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

CLAIM_V2_PROMPT = (
    "You are a fact-checking assistant. Decompose the following narrative into individual "
    "factual claims (one per line). For each claim, check whether it is grounded in the provided "
    "data.\n\n"
    "Output format — one claim per line:\n"
    "  G: <claim>\n"
    "  U: <claim>\n\n"
    "G = grounded. U = ungrounded.\n\n"
    "Grounding rules:\n"
    "- A claim is GROUNDED if it paraphrases, summarizes, or qualitatively describes a data field. "
    "'Vocal-driven rock' from genre=Rock + audio=vocal is G. "
    "'Restrained danceability' from danceability=0.22 is G. "
    "'Similarly low danceability' when both values are under 0.3 is G. "
    "'Blends ambient and acid textures' from styles=[Ambient, Acid] is G. "
    "'Soundscapes' from styles containing Ambient or Drone is G.\n"
    "- A claim is UNGROUNDED if it adds factual information not derivable from any data field. "
    "Describing what a shared neighbor sounds like is U (only their names are data). "
    "Attributing specific DJ motivation or curation philosophy is U. "
    "Stating an artist's historical significance, cultural impact, or biographical facts is U.\n\n"
    "The key question for each claim: could this claim be written by someone who ONLY saw the "
    "data fields and knew nothing else about these artists? If yes, G. If it requires outside "
    "knowledge, U.\n\n"
    "End with: COUNTS: Xg Yu"
)

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


def build_grounded_terms(prompt_data):
    """Build grounded terms set from input data."""
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
    for n in prompt_data.get("sequential_context", {}).get("shared_set_neighbors", []):
        for word in n.lower().split():
            if word not in STOP_WORDS and len(word) > 2:
                terms.add(word)
    for rel in prompt_data.get("relationships", []):
        terms.update(["transition", "transitions", "back-to-back", "times", "appeared"])
        if rel.get("raw_count"):
            terms.add(str(rel["raw_count"]))
    return terms


def build_grounded_terms_v2(prompt_data):
    """V2: grounded terms + allowlist + stem expansions."""
    terms = build_grounded_terms(prompt_data)

    # Add stem expansions for every grounded term
    for term in list(terms):
        term_lower = term.lower().replace("-", "").replace(" ", "")
        for stem, expansions in STEM_MAP.items():
            if stem in term_lower or term_lower in stem:
                terms.update(expansions)

    # Add prose allowlist
    terms.update(PROSE_ALLOWLIST)

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


def build_prompt_data_control(pair):
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
        "source": source, "target": target,
        "relationships": [{"type": "djTransition", "raw_count": pair["raw_count"], "pmi": round(pair["pmi"], 2)}],
    }


def build_prompt_data_experimental(pair):
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
    if "sequential_context" in anon:
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


def find_control_pairs(db, count=10):
    rows = db.execute(
        """
        SELECT dt.source_id, dt.target_id, dt.pmi, dt.raw_count
        FROM dj_transition dt
        JOIN artist a1 ON a1.id = dt.source_id
        JOIN artist a2 ON a2.id = dt.target_id
        WHERE dt.source_id != dt.target_id
          AND dt.raw_count >= 3
          AND a1.genre = a2.genre AND a1.genre IS NOT NULL
          AND a1.total_plays BETWEEN 400 AND 1200
          AND a2.total_plays BETWEEN 400 AND 1200
          AND a1.canonical_name NOT LIKE 'Various%'
          AND a2.canonical_name NOT LIKE 'Various%'
        ORDER BY RANDOM() LIMIT 200
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
        if len(a["styles"]) < 3 or not a["has_audio"]:
            continue
        if len(b["styles"]) < 3 or not b["has_audio"]:
            continue
        pairs.append({"a": a, "b": b, "pmi": r["pmi"], "raw_count": r["raw_count"], "group": "CONTROL"})
        used.add(r["source_id"])
        used.add(r["target_id"])
    return pairs


def find_experimental_pairs(db, degree, count=10):
    pairs = []
    for fame, data, genre in product(["HIGH", "LOW"], ["RICH", "THIN"], ["CROSS", "SAME"]):
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
            ORDER BY RANDOM() LIMIT 80
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
        used = {p["a"]["id"] for p in pairs} | {p["b"]["id"] for p in pairs}
        for i, a in enumerate(profiles):
            if len(pairs) >= count:
                break
            if a["id"] in used:
                continue
            for b in profiles[i + 1:]:
                if b["id"] in used:
                    continue
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
                pairs.append({"a": a, "b": b, "neighbors": neighbors, "group": "EXPERIMENTAL"})
                used.add(a["id"])
                used.add(b["id"])
                break
            if len(pairs) >= count:
                break
    return pairs


def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("Set ANTHROPIC_API_KEY.", file=sys.stderr)
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)
    db = get_db()
    degree = compute_degrees(db)

    print("Finding control pairs...", file=sys.stderr)
    control = find_control_pairs(db, count=10)
    print(f"  Found {len(control)}", file=sys.stderr)

    print("Finding experimental pairs...", file=sys.stderr)
    experimental = find_experimental_pairs(db, degree, count=10)
    print(f"  Found {len(experimental)}", file=sys.stderr)

    all_pairs = control + experimental
    print(f"\nTotal: {len(all_pairs)} pairs ({len(control)} control + {len(experimental)} experimental)\n",
          file=sys.stderr)

    results = []

    for pair_idx, pair in enumerate(all_pairs):
        a, b = pair["a"], pair["b"]
        group = pair["group"]

        if group == "CONTROL":
            prompt_data = build_prompt_data_control(pair)
        else:
            prompt_data = build_prompt_data_experimental(pair)

        grounded_v1 = build_grounded_terms(prompt_data)
        grounded_v2 = build_grounded_terms_v2(prompt_data)

        print(f"{'=' * 70}")
        print(f"[{group}] {a['name']} / {b['name']}")
        print(f"  genre: {a['genre']} / {b['genre']}")
        print(f"  styles A: {', '.join(a['styles'][:5]) if a['styles'] else '(none)'}")
        print(f"  styles B: {', '.join(b['styles'][:5]) if b['styles'] else '(none)'}")

        # Generate
        anon_data, name_a, name_b, nmap = anonymize(prompt_data)
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001", max_tokens=150,
            system=ANON_FEWSHOT_NAMING,
            messages=[{"role": "user", "content": json.dumps(anon_data, separators=(",", ":"))}],
        )
        narrative = deanonymize(resp.content[0].text, name_a, name_b, nmap)
        time.sleep(0.2)

        print(f"\n  {narrative}")

        # Token-match v1 and v2
        token_v1 = score_token_match(narrative, grounded_v1)
        token_v2 = score_token_match(narrative, grounded_v2)

        # Claim-ratio v1
        verify_data = json.dumps({"narrative": narrative, "provided_data": prompt_data}, indent=2)
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001", max_tokens=400,
            system=CLAIM_V1_PROMPT,
            messages=[{"role": "user", "content": verify_data}],
        )
        g1, u1 = parse_claim_counts(resp.content[0].text)
        claim_v1 = u1 / (g1 + u1) if (g1 + u1) > 0 else 0.0
        time.sleep(0.2)

        # Claim-ratio v2
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001", max_tokens=400,
            system=CLAIM_V2_PROMPT,
            messages=[{"role": "user", "content": verify_data}],
        )
        g2, u2 = parse_claim_counts(resp.content[0].text)
        claim_v2 = u2 / (g2 + u2) if (g2 + u2) > 0 else 0.0
        time.sleep(0.2)

        print(f"  token_v1={token_v1:.2f} → token_v2={token_v2:.2f}  |  "
              f"claim_v1={claim_v1:.2f} ({g1}G/{u1}U) → claim_v2={claim_v2:.2f} ({g2}G/{u2}U)")

        results.append({
            "pair": f"{a['name']} / {b['name']}", "group": group,
            "token_v1": token_v1, "token_v2": token_v2,
            "claim_v1": claim_v1, "claim_v2": claim_v2,
        })

        print(f"  ({pair_idx + 1}/{len(all_pairs)} done)", file=sys.stderr)

    # Summary
    print(f"\n{'=' * 70}")
    print("SUMMARY")
    print(f"{'=' * 70}")

    for group in ["CONTROL", "EXPERIMENTAL"]:
        gr = [r for r in results if r["group"] == group]
        if not gr:
            continue
        n = len(gr)
        print(f"\n  {group} ({n} pairs):")
        print(f"  {'Method':<14s} {'Mean':>6s}  {'<0.20':>6s}  {'<0.50':>6s}")
        print(f"  {'-' * 34}")
        for method in ["token_v1", "token_v2", "claim_v1", "claim_v2"]:
            mean = sum(r[method] for r in gr) / n
            below_20 = sum(1 for r in gr if r[method] < 0.20) / n * 100
            below_50 = sum(1 for r in gr if r[method] < 0.50) / n * 100
            print(f"  {method:<14s} {mean:>6.3f}  {below_20:>5.0f}%  {below_50:>5.0f}%")

    # Discrimination: does each method correctly rank control < experimental?
    print(f"\n  DISCRIMINATION (control should score LOWER than experimental):")
    for method in ["token_v1", "token_v2", "claim_v1", "claim_v2"]:
        ctrl_mean = sum(r[method] for r in results if r["group"] == "CONTROL") / max(1, len(control))
        exp_mean = sum(r[method] for r in results if r["group"] == "EXPERIMENTAL") / max(1, len(experimental))
        correct = "YES" if ctrl_mean < exp_mean else "NO"
        gap = exp_mean - ctrl_mean
        print(f"  {method:<14s}: control={ctrl_mean:.3f}  experimental={exp_mean:.3f}  "
              f"gap={gap:+.3f}  correct={correct}")

    # Per-pair
    print(f"\n{'=' * 70}")
    print(f"{'Pair':<42s} {'Group':<6s} {'Tv1':>5s} {'Tv2':>5s} {'Cv1':>5s} {'Cv2':>5s}")
    print("-" * 72)
    for r in results:
        p = r["pair"][:40]
        g = "CTRL" if r["group"] == "CONTROL" else "EXP"
        print(f"{p:<42s} {g:<6s} {r['token_v1']:>4.2f} {r['token_v2']:>4.2f} "
              f"{r['claim_v1']:>5.2f} {r['claim_v2']:>5.2f}")

    db.close()


if __name__ == "__main__":
    main()
