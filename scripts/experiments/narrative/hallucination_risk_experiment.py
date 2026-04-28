"""Experiment: hallucination risk factors and verification.

Scores artist pairs on hallucination risk, generates narratives, then runs
a verification pass to check whether claims are grounded in the provided data.

Risk factors:
- fame: high total_plays = model likely has pretraining knowledge
- data_sparsity: few/no styles, no audio profile
- style_noise: many styles relative to plays (noisy metadata)
- genre_distance: different genres between pair members

Outputs pairs grouped by risk tier (HIGH / MEDIUM / LOW) with narratives
and verification results.
"""

import json
import math
import os
import sqlite3
import sys
import time

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


def get_artist_profile(db: sqlite3.Connection, artist_id: int) -> dict:
    """Get artist metadata with risk factor scores."""
    row = db.execute(
        "SELECT id, canonical_name, genre, total_plays FROM artist WHERE id = ?",
        (artist_id,),
    ).fetchone()
    if not row:
        return {}

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
        # Risk factors
        "fame": row["total_plays"],
        "style_count": len(styles),
        "data_richness": len(styles) + (3 if has_audio else 0),
    }


def score_pair_risk(a: dict, b: dict) -> tuple[float, dict]:
    """Score hallucination risk for a pair. Higher = riskier."""
    # Fame: well-known artists have more pretraining interference
    fame_score = (min(a["fame"], b["fame"]) + max(a["fame"], b["fame"])) / 2
    fame_risk = 1.0 if fame_score > 1000 else (0.5 if fame_score > 500 else 0.0)

    # Data sparsity: less data = more gap-filling
    min_richness = min(a["data_richness"], b["data_richness"])
    sparsity_risk = 1.0 if min_richness == 0 else (0.5 if min_richness < 3 else 0.0)

    # Style noise: many styles = noisy
    max_styles = max(a["style_count"], b["style_count"])
    noise_risk = 1.0 if max_styles > 20 else (0.5 if max_styles > 10 else 0.0)

    # Genre distance
    genre_risk = 1.0 if a["genre"] != b["genre"] else 0.0

    total = fame_risk + sparsity_risk + noise_risk + genre_risk
    breakdown = {
        "fame": fame_risk,
        "sparsity": sparsity_risk,
        "noise": noise_risk,
        "genre_distance": genre_risk,
    }
    return total, breakdown


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


def find_pairs_by_risk(db, degree, tier: str, count: int = 5) -> list[dict]:
    """Find pairs matching a risk tier."""
    if tier == "HIGH":
        # Well-known, cross-genre, some data sparsity
        min_plays, genre_match = 800, False
    elif tier == "LOW":
        # Lesser-known, same genre, rich data
        min_plays, genre_match = 150, True
    else:
        # Medium: moderate fame, mixed
        min_plays, genre_match = 400, None  # either

    candidates = db.execute(
        """
        SELECT a.id, a.canonical_name, a.genre, a.total_plays
        FROM artist a
        JOIN dj_transition dt ON (dt.source_id = a.id OR dt.target_id = a.id) AND dt.source_id != dt.target_id
        WHERE a.total_plays >= :min_plays
          AND a.canonical_name NOT LIKE 'Various%'
          AND a.canonical_name NOT LIKE 'V/A%'
          AND a.canonical_name != 'various'
        GROUP BY a.id
        HAVING COUNT(DISTINCT CASE WHEN dt.source_id = a.id THEN dt.target_id ELSE dt.source_id END) >= 10
        ORDER BY RANDOM()
        LIMIT 80
        """,
        {"min_plays": min_plays},
    ).fetchall()

    pairs = []
    used: set[int] = set()

    for i, a in enumerate(candidates):
        if len(pairs) >= count:
            break
        if a["id"] in used:
            continue

        a_profile = get_artist_profile(db, a["id"])

        for b in candidates[i + 1:]:
            if len(pairs) >= count:
                break
            if b["id"] in used:
                continue

            # Genre filter
            if genre_match is True and a["genre"] != b["genre"]:
                continue
            if genre_match is False and a["genre"] == b["genre"]:
                continue

            # No direct edge
            edge = db.execute(
                "SELECT 1 FROM dj_transition WHERE "
                "(source_id = ? AND target_id = ?) OR (source_id = ? AND target_id = ?)",
                (a["id"], b["id"], b["id"], a["id"]),
            ).fetchone()
            if edge:
                continue

            b_profile = get_artist_profile(db, b["id"])
            neighbors, aa_total = get_aa_neighbors(db, a["id"], b["id"], degree)
            if aa_total < 0.8 or len(neighbors) < 3:
                continue

            risk_score, risk_breakdown = score_pair_risk(a_profile, b_profile)

            # Filter by tier
            if tier == "HIGH" and risk_score < 2.0:
                continue
            if tier == "LOW" and risk_score > 1.0:
                continue
            if tier == "MEDIUM" and (risk_score < 1.0 or risk_score > 2.5):
                continue

            pairs.append({
                "a": a_profile,
                "b": b_profile,
                "neighbors": neighbors,
                "aa_total": aa_total,
                "risk_score": risk_score,
                "risk_breakdown": risk_breakdown,
                "tier": tier,
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

    all_pairs = []
    for tier in ["HIGH", "MEDIUM", "LOW"]:
        print(f"Finding {tier} risk pairs...", file=sys.stderr)
        pairs = find_pairs_by_risk(db, degree, tier, count=5)
        print(f"  Found {len(pairs)}", file=sys.stderr)
        all_pairs.extend(pairs)

    print(f"\nTotal pairs: {len(all_pairs)}", file=sys.stderr)
    print(f"Generating narratives and verifying...\n", file=sys.stderr)

    for pair in all_pairs:
        a, b = pair["a"], pair["b"]
        prompt_data = build_prompt_data(pair)
        user_message = json.dumps(prompt_data, separators=(",", ":"))

        print(f"{'=' * 70}")
        print(f"[{pair['tier']} RISK — score {pair['risk_score']:.1f}] {a['name']} / {b['name']}")
        print(f"  risk: fame={pair['risk_breakdown']['fame']:.1f} "
              f"sparsity={pair['risk_breakdown']['sparsity']:.1f} "
              f"noise={pair['risk_breakdown']['noise']:.1f} "
              f"genre_dist={pair['risk_breakdown']['genre_distance']:.1f}")
        print(f"  genre: {a['genre']} / {b['genre']}")
        print(f"  plays: {a['total_plays']} / {b['total_plays']}")
        print(f"  styles A ({a['style_count']} total): {', '.join(a['styles'][:5]) if a['styles'] else '(none)'}")
        print(f"  styles B ({b['style_count']} total): {', '.join(b['styles'][:5]) if b['styles'] else '(none)'}")
        print(f"  audio A: {'yes' if a['has_audio'] else 'no'} | audio B: {'yes' if b['has_audio'] else 'no'}")
        print(f"  neighbors: {', '.join(pair['neighbors'])}")

        # Generate narrative
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
        time.sleep(0.3)

    print(f"{'=' * 70}")
    db.close()


if __name__ == "__main__":
    main()
