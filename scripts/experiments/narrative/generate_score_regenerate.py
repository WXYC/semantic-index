"""Experiment: generate-score-regenerate loop.

Tests whether scoring feedback produces convergence:
1. Generate narrative with ANON+FEWSHOT+NAMING
2. Score with token-match (instant)
3. If above threshold, identify ungrounded terms and regenerate with constraints
4. Score again
5. Repeat up to 3 iterations

Tracks: how many iterations to converge, does the score actually drop,
what's the final distribution.
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

TOKEN_THRESHOLD = 0.50
CLAIM_THRESHOLD = 0.20
MAX_RETRIES = 3

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

ANON_FEWSHOT_NAMING_BASE = (
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


def find_pairs(db, degree, count=20):
    all_pairs = []
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
            ORDER BY RANDOM() LIMIT 100
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
        used = set()
        cell_count = 0
        for i, a in enumerate(profiles):
            if cell_count >= 3 or a["id"] in used:
                continue
            for b in profiles[i + 1:]:
                if cell_count >= 3 or b["id"] in used:
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
                all_pairs.append({
                    "a": a, "b": b, "neighbors": neighbors,
                    "cell": f"fame={fame} data={data} genre={genre}",
                })
                used.add(a["id"])
                used.add(b["id"])
                cell_count += 1
                break
    return all_pairs


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


def build_grounded_terms(prompt_data):
    """Build set of grounded terms from input data."""
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
    return terms


def score_token_match(narrative, grounded_terms):
    """Returns (score, list of ungrounded content words)."""
    narrative_clean = re.sub(r"[^\w\s-]", " ", narrative.lower())
    words = narrative_clean.split()
    content_words = [w for w in words if w not in STOP_WORDS and len(w) > 2]
    if not content_words:
        return 0.0, []
    ungrounded = [w for w in content_words if w not in grounded_terms]
    # Deduplicate while preserving order
    seen = set()
    unique_ungrounded = []
    for w in ungrounded:
        if w not in seen:
            seen.add(w)
            unique_ungrounded.append(w)
    return len(ungrounded) / len(content_words), unique_ungrounded


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
    degree = compute_degrees(db)

    print("Finding pairs...", file=sys.stderr)
    pairs = find_pairs(db, degree)
    print(f"Found {len(pairs)} pairs\n", file=sys.stderr)

    # Track results
    all_results = []

    for pair_idx, pair in enumerate(pairs):
        a, b = pair["a"], pair["b"]
        prompt_data = build_prompt_data(pair)
        grounded_terms = build_grounded_terms(prompt_data)

        print(f"{'=' * 70}")
        print(f"[{pair['cell']}] {a['name']} / {b['name']}")
        print(f"  styles A: {', '.join(a['styles'][:5]) if a['styles'] else '(none)'}")
        print(f"  styles B: {', '.join(b['styles'][:5]) if b['styles'] else '(none)'}")
        print(f"  neighbors: {', '.join(pair['neighbors'])}")

        iteration = 0
        constraint_terms = []
        history = []

        while iteration <= MAX_RETRIES:
            # Build prompt with constraints from prior iterations
            if constraint_terms:
                constraint_clause = (
                    f"\n\nDo NOT use these words or concepts (they are not in the data): "
                    f"{', '.join(constraint_terms[:15])}"
                )
                system_prompt = ANON_FEWSHOT_NAMING_BASE + constraint_clause
            else:
                system_prompt = ANON_FEWSHOT_NAMING_BASE

            # Generate (anonymized)
            anon_data, name_a, name_b, nmap = anonymize(prompt_data)
            resp = client.messages.create(
                model="claude-haiku-4-5-20251001", max_tokens=150,
                system=system_prompt,
                messages=[{"role": "user", "content": json.dumps(anon_data, separators=(",", ":"))}],
            )
            narrative = deanonymize(resp.content[0].text, name_a, name_b, nmap)
            time.sleep(0.2)

            # Score with token-match
            token_score, ungrounded_words = score_token_match(narrative, grounded_terms)

            # Score with claim-ratio
            verify_data = json.dumps({"narrative": narrative, "provided_data": prompt_data}, indent=2)
            resp = client.messages.create(
                model="claude-haiku-4-5-20251001", max_tokens=400,
                system=CLAIM_DECOMPOSE_PROMPT,
                messages=[{"role": "user", "content": verify_data}],
            )
            g, u = parse_claim_counts(resp.content[0].text)
            claim_score = u / (g + u) if (g + u) > 0 else 0.0
            time.sleep(0.2)

            status = "PASS" if token_score < TOKEN_THRESHOLD else "RETRY"
            if iteration == MAX_RETRIES and status == "RETRY":
                status = "GIVE UP"

            print(f"\n  Iteration {iteration}: token={token_score:.2f} claim={claim_score:.2f} [{status}]")
            print(f"    {narrative}")
            if ungrounded_words and status == "RETRY":
                top_ungrounded = ungrounded_words[:10]
                print(f"    ungrounded terms: {', '.join(top_ungrounded)}")

            history.append({
                "iteration": iteration,
                "narrative": narrative,
                "token_score": token_score,
                "claim_score": claim_score,
                "status": status,
            })

            if token_score < TOKEN_THRESHOLD:
                break

            # Add ungrounded words as constraints for next iteration
            constraint_terms.extend(ungrounded_words[:10])
            # Deduplicate
            constraint_terms = list(dict.fromkeys(constraint_terms))
            iteration += 1

        all_results.append({
            "pair": f"{a['name']} / {b['name']}",
            "cell": pair["cell"],
            "iterations": len(history),
            "history": history,
            "final_token": history[-1]["token_score"],
            "final_claim": history[-1]["claim_score"],
            "initial_token": history[0]["token_score"],
            "initial_claim": history[0]["claim_score"],
            "converged": history[-1]["status"] == "PASS",
        })

        converged = history[-1]["status"] == "PASS"
        iters = len(history)
        delta_t = history[0]["token_score"] - history[-1]["token_score"]
        print(f"\n  Result: {'CONVERGED' if converged else 'DID NOT CONVERGE'} "
              f"in {iters} iteration(s), token delta={delta_t:+.2f}")

        print(f"  ({pair_idx + 1}/{len(pairs)} pairs done)", file=sys.stderr)

    # Summary
    print(f"\n{'=' * 70}")
    print("SUMMARY")
    print(f"{'=' * 70}")

    total = len(all_results)
    converged = sum(1 for r in all_results if r["converged"])
    first_try = sum(1 for r in all_results if r["iterations"] == 1)
    needed_retry = sum(1 for r in all_results if r["iterations"] > 1)
    gave_up = total - converged

    print(f"\n  Total pairs: {total}")
    print(f"  Passed on first try: {first_try} ({first_try / total * 100:.0f}%)")
    print(f"  Needed retry: {needed_retry} ({needed_retry / total * 100:.0f}%)")
    print(f"  Converged after retry: {converged - first_try}")
    print(f"  Did not converge: {gave_up} ({gave_up / total * 100:.0f}%)")

    if all_results:
        mean_initial_token = sum(r["initial_token"] for r in all_results) / total
        mean_final_token = sum(r["final_token"] for r in all_results) / total
        mean_initial_claim = sum(r["initial_claim"] for r in all_results) / total
        mean_final_claim = sum(r["final_claim"] for r in all_results) / total

        print(f"\n  Mean token score: {mean_initial_token:.3f} → {mean_final_token:.3f}")
        print(f"  Mean claim score: {mean_initial_claim:.3f} → {mean_final_claim:.3f}")

    # Distribution of iterations needed
    iter_dist = {}
    for r in all_results:
        n = r["iterations"]
        label = f"{n} iter" if r["converged"] else f"{n} iter (gave up)"
        iter_dist[label] = iter_dist.get(label, 0) + 1

    print(f"\n  Iteration distribution:")
    for label, count in sorted(iter_dist.items()):
        print(f"    {label}: {count}")

    # Per-pair detail
    print(f"\n{'=' * 70}")
    print("PER-PAIR DETAIL")
    print(f"{'=' * 70}")
    print(f"{'Pair':<42s} {'Iters':>5s} {'T0':>5s} {'Tf':>5s} {'C0':>5s} {'Cf':>5s} {'Result':<10s}")
    print("-" * 80)
    for r in all_results:
        pair_short = r["pair"][:40]
        result = "PASS" if r["converged"] else "FAIL"
        print(f"{pair_short:<42s} {r['iterations']:>5d} {r['initial_token']:>5.2f} {r['final_token']:>5.2f} "
              f"{r['initial_claim']:>5.2f} {r['final_claim']:>5.2f} {result:<10s}")

    db.close()


if __name__ == "__main__":
    main()
