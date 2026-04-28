"""Test augmented narrative generation with hand-crafted sequential context.

Sends three test cases to Claude Haiku to evaluate whether sequential context
(the kind embeddings would provide) produces better narratives. No embeddings
needed — the context is constructed from real database queries.
"""

import json
import os
import sys

import anthropic

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

TEST_CASES = [
    {
        "label": "NO DIRECT EDGE, SHARED CONTEXT (Tinariwen / Konono No 1)",
        "prompt": {
            "source": {
                "name": "Tinariwen",
                "genre": "Africa",
                "total_plays": 844,
                "styles": [
                    "African", "Blues Rock", "Desert Blues", "Electric Blues",
                    "Experimental", "Folk", "Psychedelic Rock",
                ],
                "region": "Mali / Algeria (Saharan Tuareg)",
            },
            "target": {
                "name": "Konono No 1",
                "genre": "Africa",
                "total_plays": 695,
                "styles": [],
                "region": "DR Congo (Kinshasa, likembe/thumb piano ensemble)",
            },
            "relationships": [],
            "sequential_context": {
                "shared_set_neighbors": [
                    "Ali Farka Toure", "Mdou Moctar", "William Parker",
                    "Duke Ellington", "LCD Soundsystem", "Gilberto Gil",
                    "The Microphones", "People Like Us",
                ],
                "note": "These artists have never appeared back-to-back, but DJs place them near the same artists in their shows.",
            },
        },
    },
    {
        "label": "CONTEXTUAL SIMILARITY DESPITE SURFACE DIFFERENCE (Outkast / Dam-Funk)",
        "prompt": {
            "source": {
                "name": "Outkast",
                "genre": "Hiphop",
                "total_plays": 1780,
                "styles": [
                    "Boom Bap", "Conscious", "Funk", "G-Funk", "Hip Hop",
                    "Neo Soul", "P.Funk", "Soul",
                ],
                "audio": {
                    "danceability": 0.58,
                    "voice_instrumental": "vocal",
                    "top_moods": ["happy", "party"],
                },
            },
            "target": {
                "name": "Dam-Funk",
                "genre": "Electronic",
                "total_plays": 834,
                "styles": [
                    "Boogie", "Deep House", "Electro", "Free Funk", "Funk",
                    "G-Funk", "Neo Soul", "P.Funk", "Synth-pop",
                ],
                "audio": {
                    "danceability": 0.74,
                    "voice_instrumental": "vocal",
                    "top_moods": ["electronic", "happy"],
                },
            },
            "relationships": [],
            "sequential_context": {
                "shared_set_neighbors": [
                    "Miles Davis", "Nina Simone", "A Tribe Called Quest",
                    "Gil Scott-Heron", "Madlib", "Omar S.", "Four Tet",
                    "Octo Octa", "Animal Collective", "Pharoah Sanders",
                    "Marvin Gaye", "Stevie Wonder",
                ],
                "note": "These artists have never appeared back-to-back, but DJs place them near the same artists in their shows.",
            },
        },
    },
    {
        "label": "SPARSE NEIGHBORHOOD ENRICHMENT (Michael Nyman)",
        "prompt": {
            "source": {
                "name": "Michael Nyman",
                "genre": "OCS",
                "total_plays": 214,
                "styles": [
                    "Ambient", "Avantgarde", "Chamber Music", "Classical",
                    "Minimal", "Minimalism", "Modern Classical",
                    "Neo-Classical", "Opera", "Score", "Soundtrack",
                ],
                "audio": {
                    "danceability": 0.14,
                    "voice_instrumental": "instrumental",
                    "top_moods": ["relaxed", "acoustic", "sad"],
                },
            },
            "target": {
                "name": "Philip Glass",
                "genre": "OCS",
                "total_plays": 595,
                "styles": [],
            },
            "relationships": [],
            "sequential_context": {
                "source_direct_neighbors": ["Bitchin' Bajas"],
                "target_direct_neighbors": [
                    "Steve Reich", "Nils Frahm", "Ennio Morricone",
                    "Laurie Anderson", "Loscil", "Actress",
                ],
                "shared_set_neighbors": [],
                "same_show_count": 2,
                "note": "Michael Nyman has only 1 non-compilation edge (Bitchin' Bajas) despite 214 plays. Philip Glass connects to Steve Reich, Nils Frahm, and Ennio Morricone. Both are minimalist composers shelved under OCS. They appeared in the same show twice but never back-to-back.",
            },
        },
    },
]


def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("Set ANTHROPIC_API_KEY to run this script.", file=sys.stderr)
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)

    for case in TEST_CASES:
        print(f"\n{'=' * 70}")
        print(f"  {case['label']}")
        print(f"{'=' * 70}\n")

        user_message = json.dumps(case["prompt"], separators=(",", ":"))

        print(f"Prompt ({len(user_message)} chars):")
        print(json.dumps(case["prompt"], indent=2))
        print()

        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=150,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )

        narrative = response.content[0].text
        usage = response.usage

        print(f"Narrative ({len(narrative.split())} words):")
        print(f"  {narrative}")
        print(f"\n  tokens: {usage.input_tokens} in / {usage.output_tokens} out")

    print(f"\n{'=' * 70}")
    print("Done.")


if __name__ == "__main__":
    main()
