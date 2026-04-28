"""Generate 20 narrative samples from real database pairs.

Finds artist pairs that illustrate the three embedding-enriched narrative scenarios:
1. No direct edge but many shared neighbors
2. Cross-genre pairs with shared sequential context
3. Sparse-neighborhood artists paired with likely peers

Outputs narratives to stdout (pipe to file).
"""

import json
import os
import sqlite3
import sys
import time

import anthropic

DB_PATH = "data/wxyc_artist_graph.db"

SYSTEM_PROMPT = (
    "You are a music knowledge assistant for WXYC 89.3 FM, a freeform college radio station. "
    "Given structured data about the relationship between two artists in the station's play "
    "history, write 2-3 sentences (under 80 words) explaining their connection in plain "
    "English. Be specific — mention shared styles, personnel names, labels, or play patterns "
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


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_artist_meta(db: sqlite3.Connection, artist_id: int) -> dict:
    """Build artist metadata dict."""
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

    meta = {
        "name": row["canonical_name"],
        "genre": row["genre"],
        "total_plays": row["total_plays"],
        "styles": styles,
    }

    try:
        profile = db.execute(
            "SELECT avg_danceability, primary_genre, voice_instrumental_ratio, recording_count "
            "FROM audio_profile WHERE artist_id = ?",
            (artist_id,),
        ).fetchone()
        if profile and profile["recording_count"] and profile["recording_count"] > 0:
            meta["audio"] = {
                "danceability": round(profile["avg_danceability"], 2),
                "voice_instrumental": (
                    "vocal" if profile["voice_instrumental_ratio"] > 0.5 else "instrumental"
                ),
            }
    except sqlite3.OperationalError:
        pass

    return meta


def get_shared_neighbors(db: sqlite3.Connection, id_a: int, id_b: int) -> list[str]:
    """Find shared DJ transition neighbors, excluding VA/compilation entries."""
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
        SELECT DISTINCT a.canonical_name
        FROM a_neighbors an
        JOIN b_neighbors bn ON an.nid = bn.nid
        JOIN artist a ON a.id = an.nid
        WHERE a.canonical_name NOT LIKE 'Various%'
          AND a.canonical_name NOT LIKE 'V/A%'
          AND a.canonical_name != 'various'
          AND a.canonical_name != 'Unknown'
        ORDER BY a.total_plays DESC
        LIMIT 8
        """,
        {"a": id_a, "b": id_b},
    ).fetchall()
    return [r["canonical_name"] for r in rows]


def get_direct_neighbors(db: sqlite3.Connection, artist_id: int, limit: int = 6) -> list[str]:
    """Get an artist's direct DJ transition neighbors."""
    rows = db.execute(
        """
        SELECT a.canonical_name
        FROM dj_transition dt
        JOIN artist a ON a.id = CASE WHEN dt.source_id = :id THEN dt.target_id ELSE dt.source_id END
        WHERE (dt.source_id = :id OR dt.target_id = :id)
          AND dt.source_id != dt.target_id
          AND a.canonical_name NOT LIKE 'Various%'
          AND a.canonical_name NOT LIKE 'V/A%'
          AND a.canonical_name != 'various'
          AND a.canonical_name != 'Unknown'
        ORDER BY dt.pmi DESC
        LIMIT :limit
        """,
        {"id": artist_id, "limit": limit},
    ).fetchall()
    return [r["canonical_name"] for r in rows]


def get_same_show_count(db: sqlite3.Connection, id_a: int, id_b: int) -> int:
    """Count shows where both artists appeared (not necessarily back-to-back)."""
    try:
        row = db.execute(
            """
            SELECT COUNT(DISTINCT p1.show_id)
            FROM play p1
            JOIN play p2 ON p1.show_id = p2.show_id
            WHERE p1.artist_id = :a AND p2.artist_id = :b AND p1.id != p2.id
            """,
            {"a": id_a, "b": id_b},
        ).fetchone()
        return row[0] if row else 0
    except sqlite3.OperationalError:
        return 0


def find_no_edge_shared_context_pairs(db: sqlite3.Connection, limit: int = 8) -> list[tuple]:
    """Find pairs with no direct edge but many shared neighbors.

    Strategy: sample well-played artists, check pairwise for no direct edge
    but high shared-neighbor count.
    """
    # Get artists with 400+ plays and 10+ edges (well-connected)
    candidates = db.execute(
        """
        SELECT a.id, a.canonical_name, a.genre, COUNT(dt.source_id) AS edge_count
        FROM artist a
        JOIN dj_transition dt ON (dt.source_id = a.id OR dt.target_id = a.id)
            AND dt.source_id != dt.target_id
        WHERE a.total_plays >= 400
          AND a.canonical_name NOT LIKE 'Various%'
          AND a.canonical_name NOT LIKE 'V/A%'
          AND a.canonical_name != 'various'
        GROUP BY a.id
        HAVING edge_count >= 10
        ORDER BY RANDOM()
        LIMIT 60
        """,
    ).fetchall()

    pairs = []
    used_ids: set[int] = set()
    for i, a in enumerate(candidates):
        if len(pairs) >= limit:
            break
        if a["id"] in used_ids:
            continue
        for b in candidates[i + 1 :]:
            if len(pairs) >= limit:
                break
            if b["id"] in used_ids:
                continue

            # Check no direct edge
            edge = db.execute(
                "SELECT 1 FROM dj_transition WHERE "
                "(source_id = ? AND target_id = ?) OR (source_id = ? AND target_id = ?)",
                (a["id"], b["id"], b["id"], a["id"]),
            ).fetchone()
            if edge:
                continue

            shared = get_shared_neighbors(db, a["id"], b["id"])
            if len(shared) >= 4:
                pairs.append((a["id"], b["id"], "no_edge_shared_context", shared))
                used_ids.add(a["id"])
                used_ids.add(b["id"])
                break  # move on to next source artist

    return pairs


def find_cross_genre_pairs(db: sqlite3.Connection, limit: int = 6) -> list[tuple]:
    """Find pairs from different genres with shared sequential context."""
    candidates = db.execute(
        """
        SELECT a.id, a.canonical_name, a.genre
        FROM artist a
        JOIN dj_transition dt ON (dt.source_id = a.id OR dt.target_id = a.id)
            AND dt.source_id != dt.target_id
        WHERE a.total_plays >= 500
          AND a.genre IS NOT NULL
          AND a.canonical_name NOT LIKE 'Various%'
          AND a.canonical_name NOT LIKE 'V/A%'
        GROUP BY a.id
        HAVING COUNT(dt.source_id) >= 10
        ORDER BY RANDOM()
        LIMIT 50
        """,
    ).fetchall()

    pairs = []
    used_ids: set[int] = set()
    for i, a in enumerate(candidates):
        if len(pairs) >= limit:
            break
        if a["id"] in used_ids:
            continue
        for b in candidates[i + 1 :]:
            if len(pairs) >= limit:
                break
            if b["id"] in used_ids:
                continue
            if a["genre"] == b["genre"]:
                continue

            edge = db.execute(
                "SELECT 1 FROM dj_transition WHERE "
                "(source_id = ? AND target_id = ?) OR (source_id = ? AND target_id = ?)",
                (a["id"], b["id"], b["id"], a["id"]),
            ).fetchone()
            if edge:
                continue

            shared = get_shared_neighbors(db, a["id"], b["id"])
            if len(shared) >= 3:
                pairs.append((a["id"], b["id"], "cross_genre", shared))
                used_ids.add(a["id"])
                used_ids.add(b["id"])
                break

    return pairs


def find_sparse_neighborhood_pairs(db: sqlite3.Connection, limit: int = 6) -> list[tuple]:
    """Find sparse artists paired with genre peers they lack edges to."""
    sparse = db.execute(
        """
        SELECT a.id, a.canonical_name, a.genre, a.total_plays,
               COUNT(dt.source_id) AS edge_count
        FROM artist a
        LEFT JOIN dj_transition dt ON (dt.source_id = a.id OR dt.target_id = a.id)
            AND dt.source_id != dt.target_id
        WHERE a.total_plays >= 150
          AND a.canonical_name NOT LIKE 'Various%'
          AND a.canonical_name NOT LIKE 'V/A%'
          AND a.canonical_name != 'various'
          AND a.genre IS NOT NULL
        GROUP BY a.id
        HAVING edge_count BETWEEN 1 AND 5
        ORDER BY a.total_plays DESC
        LIMIT 30
        """,
    ).fetchall()

    pairs = []
    for artist in sparse:
        if len(pairs) >= limit:
            break
        # Find a well-connected peer in the same genre
        peer = db.execute(
            """
            SELECT a.id, a.canonical_name
            FROM artist a
            JOIN dj_transition dt ON (dt.source_id = a.id OR dt.target_id = a.id)
                AND dt.source_id != dt.target_id
            WHERE a.genre = :genre
              AND a.total_plays >= 300
              AND a.id != :id
              AND a.canonical_name NOT LIKE 'Various%'
            GROUP BY a.id
            HAVING COUNT(dt.source_id) >= 10
            ORDER BY RANDOM()
            LIMIT 1
            """,
            {"genre": artist["genre"], "id": artist["id"]},
        ).fetchone()
        if not peer:
            continue

        # Verify no direct edge
        edge = db.execute(
            "SELECT 1 FROM dj_transition WHERE "
            "(source_id = ? AND target_id = ?) OR (source_id = ? AND target_id = ?)",
            (artist["id"], peer["id"], peer["id"], artist["id"]),
        ).fetchone()
        if edge:
            continue

        pairs.append((artist["id"], peer["id"], "sparse_neighborhood", []))

    return pairs


def build_prompt(db: sqlite3.Connection, id_a: int, id_b: int, case_type: str, shared: list[str]) -> dict:
    """Build the full prompt dict for a pair."""
    source_meta = get_artist_meta(db, id_a)
    target_meta = get_artist_meta(db, id_b)

    prompt = {
        "source": source_meta,
        "target": target_meta,
        "relationships": [],
    }

    if case_type == "sparse_neighborhood":
        source_neighbors = get_direct_neighbors(db, id_a)
        target_neighbors = get_direct_neighbors(db, id_b)
        same_shows = get_same_show_count(db, id_a, id_b)
        prompt["sequential_context"] = {
            "source_direct_neighbors": source_neighbors,
            "target_direct_neighbors": target_neighbors,
            "shared_set_neighbors": shared,
            "same_show_count": same_shows,
        }
    else:
        prompt["sequential_context"] = {
            "shared_set_neighbors": shared,
        }

    return prompt


def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("Set ANTHROPIC_API_KEY to run this script.", file=sys.stderr)
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)
    db = get_db()

    print("Finding test cases...", file=sys.stderr)
    pairs = []
    pairs.extend(find_no_edge_shared_context_pairs(db, limit=8))
    pairs.extend(find_cross_genre_pairs(db, limit=6))
    pairs.extend(find_sparse_neighborhood_pairs(db, limit=6))

    print(f"Found {len(pairs)} pairs, generating narratives...", file=sys.stderr)

    for i, (id_a, id_b, case_type, shared) in enumerate(pairs):
        prompt = build_prompt(db, id_a, id_b, case_type, shared)
        source_name = prompt["source"]["name"]
        target_name = prompt["target"]["name"]
        label = case_type.replace("_", " ").upper()

        print(f"  [{i + 1}/{len(pairs)}] {source_name} / {target_name} ({label})", file=sys.stderr)

        user_message = json.dumps(prompt, separators=(",", ":"))

        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=150,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )

        narrative = response.content[0].text

        # Output to stdout
        print(f"{'=' * 70}")
        print(f"[{label}] {source_name} / {target_name}")
        print(f"  genre: {prompt['source'].get('genre', '?')} / {prompt['target'].get('genre', '?')}")
        print(f"  plays: {prompt['source']['total_plays']} / {prompt['target']['total_plays']}")
        if shared:
            print(f"  shared neighbors: {', '.join(shared[:6])}")
        print()
        print(f"  {narrative}")
        print()

        # Rate limit courtesy
        time.sleep(0.5)

    print(f"{'=' * 70}")
    db.close()


if __name__ == "__main__":
    main()
