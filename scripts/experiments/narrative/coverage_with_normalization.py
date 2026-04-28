"""Improved review-to-artist matching with name normalization.

Fixes:
- Split canonical names on " / ", " & ", " and " to match components
- Strip leading "The " for matching
- Normalize unicode (NFKD + strip diacritics)
- Skip "Various Artists" entries entirely
- Case-insensitive matching against review titles
"""

import json
import re
import sqlite3
import unicodedata
from collections import defaultdict
from pathlib import Path

DB_PATH = "data/wxyc_artist_graph.db"
REVIEW_DIR = Path("data/reviews")


def normalize(name: str) -> str:
    """NFKD decomposition + strip diacritics + lowercase + trim."""
    nfkd = unicodedata.normalize("NFKD", name)
    stripped = "".join(c for c in nfkd if not unicodedata.combining(c))
    return stripped.lower().strip()


def generate_variants(canonical: str) -> set[str]:
    """Generate matchable name variants from a canonical name."""
    variants = set()
    norm = normalize(canonical)

    # Skip too-short names
    if len(norm) < 4:
        return variants

    variants.add(norm)

    # Strip leading "the "
    if norm.startswith("the "):
        variants.add(norm[4:])

    # Split on separators and add components
    for sep in [" / ", " & ", " and ", " + ", " with ", " vs. ", " vs "]:
        if sep in norm:
            for part in norm.split(sep):
                part = part.strip()
                if len(part) >= 4:
                    variants.add(part)
                    if part.startswith("the "):
                        variants.add(part[4:])

    # Handle "Jay Dee" from "J Dilla / Jay Dee"
    # Handle "Mikal Cronin" from "Ty Segall & Mikal Cronin"
    # (covered by the split above)

    # Strip common suffixes
    for suffix in [", Jr.", ", III", ", II"]:
        cleaned = norm.replace(suffix.lower(), "").strip()
        if len(cleaned) >= 4 and cleaned != norm:
            variants.add(cleaned)

    return variants


def main():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row

    # Load artists with 100+ plays, skip VA entries
    artists = {}
    for r in db.execute(
        "SELECT id, canonical_name, genre, total_plays FROM artist WHERE total_plays >= 100"
    ):
        name = r["canonical_name"]
        if name.startswith("Various Artists") or name in ("V/A", "various", "Unknown"):
            continue
        artists[r["id"]] = {
            "name": name,
            "genre": r["genre"],
            "plays": r["total_plays"],
        }

    print(f"Artists with 100+ plays (excl. VA): {len(artists)}")

    # Build variant -> artist ID mapping
    variant_to_ids: dict[str, set[int]] = defaultdict(set)
    for aid, info in artists.items():
        for v in generate_variants(info["name"]):
            variant_to_ids[v].add(aid)

    # Remove overly common variants that would cause false matches
    # (variants matching 10+ artists are probably too generic)
    variant_to_ids = {k: v for k, v in variant_to_ids.items() if len(v) <= 5}

    print(f"Unique match variants: {len(variant_to_ids)}")

    # Sort variants longest-first for greedy matching
    sorted_variants = sorted(variant_to_ids.keys(), key=len, reverse=True)

    # Scan reviews
    source_matched: dict[str, set[int]] = defaultdict(set)
    all_matched: set[int] = set()

    sources = [d.name for d in REVIEW_DIR.iterdir() if d.is_dir() and (d / "reviews.jsonl").exists()]

    for source in sorted(sources):
        path = REVIEW_DIR / source / "reviews.jsonl"
        with open(path) as f:
            for line in f:
                rec = json.loads(line)
                title = normalize(rec.get("title", "") or "")
                if not title:
                    continue
                for variant in sorted_variants:
                    if variant in title:
                        for aid in variant_to_ids[variant]:
                            source_matched[source].add(aid)
                            all_matched.add(aid)

    print(f"\nMatched artists: {len(all_matched)} / {len(artists)} ({len(all_matched)/len(artists)*100:.0f}%)")
    for source in sorted(source_matched):
        print(f"  {source}: {len(source_matched[source])}")

    # Before/after comparison for specific problem cases
    print("\nSpot checks:")
    problem_names = [
        "J Dilla / Jay Dee", "Ty Segall & Mikal Cronin", "Lindstrom & Prins Thomas",
        "Weyes Blood & Dark Juices", "Duke Ellington", "Ella Fitzgerald",
        "Ali Farka Toure", "Konono No 1", "Omar S.",
    ]
    for name in problem_names:
        for aid, info in artists.items():
            if info["name"] == name:
                status = "MATCHED" if aid in all_matched else "UNMATCHED"
                variants = generate_variants(name)
                print(f"  {name}: {status} (variants: {', '.join(sorted(variants)[:5])})")
                break

    # Genre breakdown of remaining unmatched
    unmatched_by_genre = defaultdict(list)
    for aid, info in artists.items():
        if aid not in all_matched:
            unmatched_by_genre[info["genre"] or "None"].append((info["name"], info["plays"]))

    print(f"\nRemaining unmatched by genre:")
    for genre, items in sorted(unmatched_by_genre.items(), key=lambda x: -len(x[1])):
        print(f"  {genre}: {len(items)}")
        top = sorted(items, key=lambda x: -x[1])[:3]
        for name, plays in top:
            print(f"    {name} ({plays} plays)")

    total_unmatched = sum(len(v) for v in unmatched_by_genre.values())
    print(f"\nTotal unmatched: {total_unmatched} / {len(artists)} ({total_unmatched/len(artists)*100:.0f}%)")

    db.close()


if __name__ == "__main__":
    main()
