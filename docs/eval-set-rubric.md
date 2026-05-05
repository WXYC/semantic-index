# Narrative Eval-Set Labeling Rubric

## What you're labeling

Each row is a 2-3 sentence narrative the WXYC Freeform Map produced (or could have produced) about a relationship between two artists in the WXYC library. Alongside each narrative, you see the structured input data the model was working from — both artists' WXYC genre, total play count, Discogs styles, and (when present) audio profile + shared transition neighbors.

Your job is to read the narrative and decide whether it gets the music **wrong**. You are not judging prose quality, brevity, or whether the narrative is interesting — only whether it makes a claim about the music or the relationship that is **untrue or unsupported in a way a knowledgeable WXYC listener would push back on**.

## Decision sequence

1. **Read the narrative once, normally** — as a listener would.
2. **Note any specific claim that struck you as off** while reading. Trust your instinct here; the rubric is for the second pass.
3. **For each off-feeling claim, ask:**
   - *Could a knowledgeable music listener defend it?* — if yes, not wrong.
   - *Is it stated about the wrong artist?* — wrong (severe).
   - *Does it contradict the data fields?* — wrong (severe).
   - *Does it characterize a shared neighbor with adjectives the data doesn't supply?* — wrong (minor).
   - *Does it claim DJ intent ("DJs pair them to ...", "valued for ...")?* — wrong (minor).
4. **Apply the most serious applicable label.** Severe trumps minor; if any claim is severe, the row is severe.

## Severity

- **severe** — a knowledgeable listener would say "that's just wrong" or "that's about a different artist." Examples: calling a folk singer a jazz innovator; describing the wrong Waters; stating "electronic" when audio + styles are all acoustic. These are the failures that erode trust.
- **minor** — the narrative says something the data doesn't support, but the claim isn't outright contradicted by the music. Most neighbor-characterization and DJ-intent fall here. Examples: "introspective indie voices like Jamila Woods" (data names Jamila Woods but doesn't characterize her), "DJs value them for lyrical depth" (data shows co-occurrence, not curator motive).
- **not_wrong** — every claim either traces to the data or is a defensible inference about the music. Paraphrase is fine. Reasonable genre umbrellas are fine.

A row with only minor wrongness still gets labeled `severity = minor`. A row with no wrongness at all is `not_wrong`.

## Failure mode

Pick the *one* category that best describes the wrongness. If multiple apply, pick the most severe; ties go to whichever the labeler thinks is the most diagnostic for fixing prompts later.

| Code | Name | What it looks like |
|---|---|---|
| `subject_hallucination` | Subject artist hallucination | A fact about one of the two named artists that isn't in the input data and contradicts what the music actually is. The model leaned on pretraining knowledge and got it wrong. |
| `neighbor_characterization` | Neighbor characterization | The narrative describes a shared neighbor with adjectives, traditions, or roles the data doesn't supply. ("X like the experimental jazz innovator Y") |
| `dj_intent` | DJ intent attribution | Claims about *why* DJs played them together. ("DJs pair them to bridge ...", "valued for their willingness to ...") |
| `data_noise` | Data-noise propagation | The narrative is grounded in the input data but the input data is itself wrong (an outlier Discogs style from a minor release pulled into the top 5; a stale audio profile). The narrative is technically faithful and substantively misleading. |
| `other` | Other | Anything else worth flagging — note it in the freeform notes column. |
| — | (blank when severity is `not_wrong`) | |

## Worked examples

These come from the whitepaper's Section 6 plus a few drawn from the eval-set corpus itself.

### Example 1 — severe / subject_hallucination

> "The Smiths and Dam-Funk represent different entry points into electronically-inflected funk and soul."

Why severe: The Smiths are a jangle-pop / post-punk band. They have no funk lineage. The narrative is asserting a category about the music that is plainly wrong; the input data did not say "funk" anywhere for The Smiths.

### Example 2 — severe / subject_hallucination

> "Waters pioneered electric Chicago blues and jump blues with raw harmonica and driving grooves."

Why severe: This was generated for **Crystal Waters** (a house singer). The model conflated her with Muddy Waters. Stating things about a completely different artist is the worst failure mode.

### Example 3 — severe / data_noise

> "Alex G channels dance-pop and Euro House into experimental rock territory."

Why severe (data_noise variant): Alex G's input data did say `styles: [Dance-pop, Euro House, Makina]` — the narrative is technically grounded. But those styles came from minor releases on a Discogs page; they are not what Alex G's music sounds like. A knowledgeable listener would call this wrong, even though the model was faithful to a misleading input.

### Example 4 — minor / neighbor_characterization

> "Outkast and Dam-Funk both connect through introspective indie voices like Jamila Woods and art-rock innovators like U.S. Maple."

Why minor: The data names Jamila Woods and U.S. Maple as shared neighbors but does not characterize them. "Introspective indie" and "art-rock innovators" come from pretraining knowledge. The neighbors *are* shared, so the narrative isn't lying about the relationship; it's just decorating with unsupported adjectives.

### Example 5 — minor / dj_intent

> "DJs pair Joanna Newsom and Colleen to create sets that prioritize lyrical depth and acoustic intimacy."

Why minor: The input data shows a co-occurrence count, not a curator motive. The narrative invents a reason. It's a minor wrongness because the underlying fact (they co-occur, both have acoustic styles) is true; the framing is what's off.

### Example 6 — not_wrong (paraphrase)

> "Animal Collective and Tinariwen share relaxed, electronic moods in WXYC's collection, with DJs occasionally pairing them across 2 transitions."

Why not_wrong: "Relaxed" is in the audio profile; "electronic" maps to Tinariwen's audio.primary_genre; "2 transitions" matches raw_count. Paraphrase of the data, not invention.

### Example 7 — not_wrong (reasonable inference)

> "Both Konono No. 1 and Tinariwen ground their sound in African traditions while pushing toward amplified, hypnotic textures."

Why not_wrong: Even though "African traditions" is broader than the data, both artists' genre fields and styles support it. "Amplified, hypnotic textures" is defensible from styles like Likembe and Tuareg Guitar. A knowledgeable listener would not push back.

## Edge cases

- **Vagueness vs. wrongness.** A vague but technically accurate sentence ("both make compelling music") is not wrong, just unhelpful. Don't flag prose quality.
- **Reasonable umbrellas.** "African music" for an artist with `genre = Africa + styles = [Saharan Rock]` is acceptable. "European music" for an artist with `genre = Asia` is wrong (subject_hallucination).
- **Numbers in prose.** If the narrative says "2 transitions" and the data shows raw_count = 2, that's not_wrong. If it says "frequent transitions" and raw_count = 2, mark as minor / data_noise (overstating the signal).
- **Insufficient signal narratives.** A narrative reading "WXYC DJs occasionally play these artists together, but they don't share enough specific musical context …" is the canned placeholder. Always label `not_wrong`.
- **You don't know the artist.** If you can't tell whether a claim about an artist's music is true (e.g., obscure release on Reggae shelf), default to `not_wrong` and add a note in the freeform column. Don't guess.

## Calibration

Before the bulk pass, every labeler should label the same 20 rows from the calibration set. We compare results, discuss disagreements, and refine examples in this rubric before splitting the rest of the work. If two labelers disagree on whether a row is severe vs. minor, neither is wrong — the rubric needs sharpening. Notes in the calibration round are doubly valuable.
