# Narrative Enrichment Plan

The narrative endpoint at `explore.wxyc.org` generates natural-language explanations of artist relationships using Claude Haiku. Today it works well when two artists share a direct DJ transition edge with rich metadata — Discogs styles, audio profiles, cross-references. But it falls silent when artists lack a direct edge, and even when it speaks, it often describes roles and genres rather than what the music actually sounds like.

The goal: **make the narrative endpoint say true, specific, musical things about artist relationships — including relationships the graph can't currently see.**

This plan addresses that goal across four fronts: better data selection, richer source material, new relationships via embeddings, and accuracy within the model.

## 1. Better Data Selection

The narrative endpoint currently passes shared DJ transition neighbors to the prompt ranked by play count. This surfaces generic hub artists (Miles Davis, The Beatles, Yo La Tengo) that appear in hundreds of neighborhoods and prove nothing about the specific pair being narrated.

### Adamic-Adar Weighted Neighbors

We tested four neighbor weighting methods — raw play count, Adamic-Adar (1/log(degree)), Resource Allocation (1/degree), and a degree ceiling (exclude above 95th percentile) — across eight artist pairs with no direct edge.

Adamic-Adar and Resource Allocation both rerank neighbors by informativeness. RA produces sharper separation: for the Caetano Veloso / Don Cherry pair, Dennis Bovell (degree 37, a dub reggae producer) scores 9x higher than Sonic Youth (degree 309) under RA, versus 1.6x under AA. The degree ceiling was too aggressive — it eliminated all shared neighbors for 5 of 8 pairs at the 95th percentile cutoff (degree 43).

**Concrete improvements from AA reranking:**

- Peter Brötzmann / Angel Olsen: raw ranking leads with Arthur Russell and Dan Melchior (generic, high-degree hubs). AA surfaces Dum Dum Girls (degree 27) and Joe McPhee (degree 36) — a garage pop band and a free jazz saxophonist. Joe McPhee sharing a neighborhood with both Brötzmann and Olsen is genuinely informative.
- Caetano Veloso / Don Cherry: Dennis Bovell (degree 37) rises from last to first. Both Veloso and Cherry worked across Brazilian/jazz/dub lines — Bovell as a shared neighbor makes the pair legible.
- Religious Knives / Pastor T.L. Barrett: United Waters (degree 27) rises to the top; Can (degree 204) drops.

### Minimum Score Threshold

Some pairs are spuriously connected through generic hubs. Bob Marley and the Wailers / Les Rallizes Dénudés share five neighbors (Björk, The Velvet Underground, Tim Hecker, Frank Sinatra, Hiatus Kaiyote) but even after AA reranking, the top neighbor (Frank Sinatra, AA 0.23) is uninformative. No weighting method saves a pair that shouldn't surface at all. A minimum total AA score threshold (we used 0.8 in experiments) filters these out.

### Data Field Pruning

Experiments revealed several data hygiene issues:

- **Overly long style lists invite hallucination.** Outkast has 53 Discogs styles. Destroyer's top 5 include "Breakbeat" and "Techno" from minor releases, which the model seized on to describe an indie rock artist. Capping to the top 5 by release count, or omitting the field entirely when styles are unreliable, reduces hallucination surface.
- **Raw danceability numbers leak into prose.** The Gang Gang Dance / Matmos narrative quoted "(0.68 danceability)" and "(0.34 danceability)" — users don't want to see decimal scores. Either convert to qualitative labels (omit when unremarkable, say "highly danceable" only when notably high) or omit the number and keep only the label.
- **Empty fields tempt the model to compensate.** Konono No 1 had zero Discogs styles. Including an empty `styles: []` in the prompt tempts the model to hallucinate genre descriptions. Better to omit the field entirely.

### Recommendation

Use Adamic-Adar for neighbor ranking, with a minimum total AA score of 0.8 and a top-k of 4-5 neighbors per pair. Cap style lists to the 5 most representative. Convert audio profile numbers to qualitative labels. Omit empty or unreliable fields.

## 2. Richer Source Material

The current narrative data (Discogs styles, genre labels, audio profile numbers) tells the model *what category* an artist falls into but not *what the music sounds like*. The narratives talk about roles, contexts, and genres but rarely about rhythm, texture, instrumentation, or atmosphere. Richer text-based source material fills this gap.

### Three Complementary Text Sources

Each source provides a different kind of evidence and should be extracted and surfaced differently in the narrative prompt:

**Professional reviews** (Pitchfork, Stereogum, The Quietus, Tiny Mix Tapes, Bandcamp Daily + new sources) provide critical characterization — a trained listener's description of what the music sounds like, its mood, and how it relates to other artists. These are the richest source of sonic descriptors, mood language, and comparative references.

**Artist and label bios** (Bandcamp release pages, potentially other platforms) provide first-party factual descriptions — what instruments were used, how it was recorded, self-described influences. These are the most reliable source of instrumentation and production details. An artist saying "recorded on a 4-track with lap steel and Farfisa organ" is a factual statement, not a critical judgment.

**Encyclopedic descriptions** (Wikipedia artist articles, sourced via Wikidata QIDs already in the pipeline) provide edited, sourced descriptions of musical style with very broad coverage. These fill the historical and world music gaps where no review publication has covered an artist. Wikipedia has rich "Musical style" sections for canonical figures like Duke Ellington, Ravi Shankar, and Ali Farka Toure who are absent from indie review corpora.

### Extraction Schema

Each source type gets its own extraction schema, reflecting the different kinds of evidence it provides:

**Reviews → five fields:**
- `sonic_descriptors` — phrases describing what the music sounds like: "shimmering guitars," "thudding kick drum," "nasal vocal delivery"
- `mood_atmosphere` — evocative terms: "claustrophobic," "sun-drenched," "nocturnal," "anthemic"
- `instrumentation` — instruments and production tools mentioned: "thumb piano," "analog synth," "upright bass"
- `comparative_artists` — explicit comparisons: "recalls early Talk Talk," "indebted to Lee Perry's dub experiments"
- `genre_descriptors` — finer-grained genre terms than Discogs: "spiritual jazz," "Saharan rock," "zolo"

**Artist bios → three fields:**
- `instrumentation` — self-described instruments and tools
- `influences` — self-described influences and reference points
- `production_details` — recording methods, studio context, factual production information

**Wikipedia → two fields:**
- `style_description` — prose from "Musical style" / "Artistry" sections
- `genre_context` — regional, historical, or cultural context for the artist's tradition

Extraction method: structured LLM extraction via Haiku. Each review or bio is passed through with a prompt asking for JSON output in the relevant schema. At ~37K existing reviews averaging 700 words, extraction costs roughly $2 in Haiku input tokens. The prompt should instruct: extract only what's explicitly stated, don't infer.

### Narrative Prompt Integration

In the narrative prompt, these appear as separate, labeled fields so the model knows how to weight each:

```json
{
  "source": {
    "name": "Tinariwen",
    "review_descriptors": {
      "sonic": ["hypnotic guitar interplay", "pentatonic scales over driving rhythms"],
      "mood": ["desert heat", "meditative"],
      "instrumentation": ["electric guitar", "tehardent"]
    },
    "bio": {
      "instrumentation": ["electric guitar", "bass", "calabash"],
      "production_details": ["recorded in the Sahara"]
    }
  }
}
```

The system prompt should direct the model: use review language for characterization, bio details for concrete facts, and Wikipedia context for cultural/historical framing. Don't attribute — don't say "critics describe" or "the artist describes," just use the language naturally.

### Current Corpus and Coverage

We have 37,422 reviews across four sources:

| Source | Reviews |
|--------|---------|
| The Quietus | 15,845 |
| Stereogum | 7,346 |
| Bandcamp Daily | 7,180 |
| Tiny Mix Tapes | 7,051 |

Full-text matching against the artist graph (136,675 total artists) yields 20,068 matched artists. For artists with 100+ plays (the ones that matter for narratives), the remaining unmatched breakdown by genre:

| Genre | Unmatched (100+ plays) | Best source to close gap |
|-------|----------------------|--------------------------|
| Rock | 495 | Name matching fixes (many are VA entries or name variants) |
| Hiphop | 186 | Name matching fixes (J Dilla / Jay Dee, etc.) |
| Jazz | 127 | All About Jazz, Wikipedia |
| Electronic | 127 | Resident Advisor |
| OCS | 119 | Aquarium Drunkard |
| Africa | 79 | Songlines, Afropop Worldwide, Wikipedia |
| Asia | 62 | Wikipedia, Songlines |
| Latin | 60 | Wikipedia |
| Blues | 51 | Wikipedia |
| Reggae | 40 | Wikipedia |
| Classical | 42 | The Wire (partially covered by The Quietus) |

A significant portion of the "unmatched" count is a name matching problem: "J Dilla / Jay Dee" doesn't match a review titled "J Dilla"; "Ty Segall & Mikal Cronin" doesn't match "Ty Segall." Normalizing matches by splitting on `/`, `&`, and stripping "Various Artists" entries improved coverage from 53% to 56% in title-only matching. Full-text matching (searching the first 500 characters of review body) brought it further — Ella Fitzgerald, Duke Ellington, and Herbie Hancock were all found mentioned in review body text as reference points, even without dedicated reviews.

### New Sources to Crawl

Priority order by coverage impact:

1. **Wikipedia** — broadest coverage gain. Covers the blues, Latin, reggae, jazz, and Asia gaps (canonical artists with rich articles but no indie review coverage). Accessible via Wikimedia API using Wikidata QIDs already in the pipeline. Estimated crawl time: ~3 hours.
2. **All About Jazz** — deep jazz archive (30K+ reviews). Covers the free jazz and world-jazz crossover gap (Slavic Soul Party, Kali Fasteau, Rahsaan Roland Kirk). Estimated crawl time: ~12 hours.
3. **Resident Advisor** — electronic/dance coverage (15K reviews). Covers Omar S., Rhythm & Sound, Fingers Inc. Estimated crawl time: ~6 hours.
4. **Aquarium Drunkard** — psych/folk/outsider coverage (5K articles). Covers Doc Watson, Hazel Dickens, Charlie Parr. Estimated crawl time: ~2 hours.
5. **Songlines** — world music reviews. Covers Ali Farka Toure, Konono No 1, Orchestre Poly-Rhythmo, Khun Narin Electric Phin Band. Smaller corpus but uniquely fills a gap no other source touches. Estimated crawl time: ~1 hour.
6. **Bandcamp release pages** — first-party artist/label descriptions. Covers the deepest long tail of independent artists. We already have `bandcamp_id` for some artists in the entity table.

All sources use the same crawler infrastructure already built in `scripts/crawl_reviews.py`: sitemap or CDX discovery → URL cache → trafilatura extraction → JSONL output. Crawlers are resumable via the existing `load_done` pattern. Each new source needs a source-specific discovery function (~30-50 lines) following the existing templates.

Crawl times are bounded by polite rate limiting (1-2 second delays). Sources are independent and can run in parallel.

### Artist Matching

Matching reviews to graph artists requires:

1. **Source-specific title parsing** — review titles follow patterns like "Artist - Album" or "Review: Artist — Album" that vary by publication. Regex per source to extract the artist name from the title.
2. **Name normalization** — NFKD decomposition, diacritics stripping, lowercase, splitting on `/` and `&` separators. The same logic already exists in `artist_resolver.py` via `wxyc_etl.text.normalize_artist_name`.
3. **Fuzzy matching** — Jaro-Winkler against canonical names for cases where the review uses a variant spelling (e.g., "Björk" vs "Bjork," "MF DOOM" vs "MF Doom").
4. **Body text matching** — for reviews that mention artists as reference points (not the review subject), the comparative_artists field from the extraction step serves as a cross-reference signal.

### Graph Propagation for Remaining Gaps

For artists with no review text, no bio, and no Wikipedia article, review descriptors can be propagated through the graph. If an artist has strong PMI edges to three artists that all carry the review descriptor "shimmering guitars," that descriptor can be inferred with diminishing confidence. Weight propagated descriptors by edge strength. This is a fallback, not a primary source — the narrative prompt should not present propagated descriptors as if they were directly observed.

For artists with no data at all, the narrative should lean on the sequential context (shared neighbors, same-show counts) and not attempt to describe the sound. "DJs place X near Y and Z" is true and useful even without sonic descriptors.

## 3. New Relationships via Embeddings

The pairwise PMI graph requires two artists to appear back-to-back at least twice (with the default `min_count=2`) to create an edge. This misses artists that occupy similar roles in DJ sets without ever being directly juxtaposed — the "similar position in context" relationship that linguists call distributional similarity.

### Skip-Gram Embeddings over Show Sequences

Treat each radio show as a sentence of artists and train word2vec-style skip-gram embeddings. The show segmentation already exists in `adjacency.py`. The result is a per-artist vector that encodes sequential co-occurrence patterns across all context window sizes simultaneously. Artists that appear in similar positions within sets (surrounded by similar neighbors) end up close in embedding space, even if they've never appeared back-to-back.

### What Embeddings Surface

Three narrative scenarios that embeddings enable, demonstrated with real data from the production database:

**No direct edge, shared context.** Tinariwen (844 plays) and Konono No 1 (695 plays) have never appeared back-to-back in 22 years of flowsheets, but DJs place them near the same artists — Ali Farka Toure, Mdou Moctar, William Parker, Duke Ellington. Embeddings would surface this pair; the current graph cannot.

**Sparse neighborhood enrichment.** Michael Nyman (214 plays) has only 1 non-compilation DJ transition edge (Bitchin' Bajas) despite being a canonical minimalist composer. He has zero edges to Philip Glass, Steve Reich, or Terry Riley — all fellow minimalists shelved under OCS. He appeared in the same show as Glass twice, never back-to-back. Embeddings would place Nyman near Glass by learning from the broader context of how minimalist composers are sequenced, even without direct adjacency evidence.

**Contextual similarity despite surface difference.** Outkast (1,780 plays, hip-hop) and Dam-Funk (834 plays, electronic) have no direct edge but share 32 non-VA neighbors including Miles Davis, Nina Simone, A Tribe Called Quest, Gil Scott-Heron, and Madlib. DJs use them in similar set positions despite different sounds. Embedding proximity would capture this; the current graph shows no connection.

### Embeddings as Infrastructure, Not a Feature

The embeddings do not become a new edge type, a new neighbor list, or anything the user sees directly. They are query-time infrastructure: when the narrative endpoint receives a pair of artists with no direct relationship, it computes cosine similarity between their embedding vectors. If the similarity is high, it retrieves the shared sequential context (common neighbors from the embedding space) and passes it to the narrative prompt. The narrative describes DJs using the artists in similar ways — observable behavior, not math. The word "embedding" never appears in user-facing output.

### Evaluating Embeddings vs. Reviews

Embeddings and reviews are complementary, not competing. Reviews improve narrative *quality* — what the model can say about a pair it already knows about. Embeddings improve narrative *coverage* — which pairs surface at all. To measure relative value, we would generate narratives for 50 artist pairs under four conditions: (a) current data only, (b) current + review descriptors, (c) current + embedding context, (d) both. Rating each on accuracy and informativeness would quantify the marginal improvement from each addition.

The prediction: reviews produce the larger quality improvement per pair; embeddings produce the larger coverage improvement across the graph. Both are needed.

## 4. Accuracy Within the Model

Experiments with three prompt variants across 10 artist pairs revealed specific hallucination patterns and effective mitigations.

### Observed Hallucination Patterns

- **Inferring outside the data.** Baseline prompt: The Smiths described as "different entry points into electronically-inflected funk and soul." Josephine Foster described as sharing "roots in experimental approaches to jazz and blues traditions." Neither characterization comes from the provided data; the model draws on its own knowledge and gets it wrong.
- **Seizing on misleading styles.** Destroyer's Discogs styles include "Breakbeat" and "Techno" from minor releases. The model described an indie rock artist as channeling "breakbeat and techno elements" — technically grounded in the data, but misleading because the styles aren't representative.
- **Negation descriptions.** "Low-danceability" appeared in multiple narratives despite explicit instruction to describe what music IS, not what it isn't.
- **Raw numbers in prose.** Danceability scores like "(0.68)" surfaced in narrative text — the model quoted the data rather than interpreting it.
- **Filling gaps with outside knowledge.** Muddy Waters was conflated with Crystal Waters (a house singer). Pastor T.L. Barrett was described as "instrumental and experimental" when he sings religious soul and gospel. When the model lacks sufficient data, it guesses.
- **Continental generalization.** Artists from specific African traditions were described as "African artists" rather than by their specific tradition (Saharan Tuareg, Congolese likembe music). Adding region data to the prompt and instructing the model not to generalize across the continent fixed this.

### Prompt Variant Results

**Baseline:** Fluent but frequently inaccurate. Overuses "occupy," "reach for," "represent," "suggest," "anchor," "bridge." Hallucinations in ~40% of narratives.

**Anti-hallucination:** Added "describe each artist ONLY using the styles, audio, and genre fields provided. Do not draw on outside knowledge." Reduced hallucination rate but didn't eliminate it. The model still infers when it has thin data.

**Anti-hallucination + varied language:** Added a banned word list (occupy, reach, represent, suggest, anchor, bridge, curate, sonic, territory, sensibility, touchstone) and "find fresher ways to say things." Produced more varied prose but introduced new tics: "rather than" appears in nearly every narrative as a contrast device. "Straightforward" became filler.

### Hallucination Risk Matrix

A systematic experiment tested hallucination rates across a 2×2×2 matrix of risk factors — Fame (HIGH >800 plays / LOW <400), Data richness (RICH: 3+ styles and audio profile / THIN: ≤2 styles or no audio), and Genre distance (CROSS: different genres / SAME: same genre) — with 3 pairs per cell (19 pairs total, as the LOW+THIN+SAME cell had no qualifying pairs) and automated verification.

| Cell | Pairs | Grounded | Ungrounded | Ambiguous | Halluc% |
|------|-------|----------|------------|-----------|---------|
| HIGH fame + RICH data + CROSS genre | 3 | 20 | 5 | 5 | 33% |
| HIGH fame + RICH data + SAME genre | 3 | 20 | 2 | 7 | 31% |
| HIGH fame + THIN data + CROSS genre | 3 | 27 | 8 | 3 | 29% |
| HIGH fame + THIN data + SAME genre | 3 | 23 | 6 | 4 | 30% |
| LOW fame + RICH data + CROSS genre | 3 | 21 | 4 | 3 | 25% |
| LOW fame + RICH data + SAME genre | 3 | 22 | 3 | 5 | 27% |
| LOW fame + THIN data + CROSS genre | 1 | 6 | 2 | 1 | 33% |

**Fame is the strongest predictor.** HIGH fame cells average ~31% hallucination. LOW fame + RICH data cells average ~26%. The model hallucinates more about artists it "knows" from pretraining — its own knowledge overrides the provided data. Data richness helps but less than expected: HIGH+RICH (32%) vs HIGH+THIN (30%) is barely different. Genre distance shows no consistent effect.

### Four Distinct Failure Modes

Manual review of all 19 pairs and their verification results revealed that hallucination is not one problem but four, each requiring a different solution:

**1. Subject artist hallucination (fame-driven).** The model draws on pretraining knowledge about well-known artists and states things not in the provided data. Examples: "Dylan's lyrical innovation and unconventional song structure" (no lyrical data provided), "Elvis Costello's literary approach" (inferred from "Art Song" style tag), Omar S. described as having a "harder electronic stance" (zero styles data for Omar S.). This was the dominant failure mode for HIGH fame pairs.

**2. Neighbor characterization (all tiers).** The most consistent hallucination across every risk tier. The model describes shared neighbors with adjectives not present in the data: "folk innovators like Astor Piazzolla," "introspective indie voices like Jamila Woods," "art-rock innovators like U.S. Maple." The data provides only the neighbor names; everything about what kind of artists they are comes from pretraining knowledge. This happens equally in HIGH and LOW fame pairs.

**3. DJ intent attribution (all tiers).** The model infers motivations for co-occurrence: "DJs pair them to create sets that prioritize lyrical depth," "DJs value them for their willingness to push beyond conventional rock structures." The data shows co-occurrence patterns, not curator intent. This appeared in nearly every narrative regardless of risk tier.

**4. Garbage-in data (style noise).** Alex G.'s top 3 Discogs styles came back as "Dance-pop, Euro House, Makina" — clearly from minor releases, not representative of the artist. The model dutifully described Alex G. as "channeling dance-pop and Euro House into experimental rock territory." The narrative was technically grounded in the provided data but substantively wrong. The automated verifier cannot catch this because it checks against the provided data, not against reality.

### Solutions by Failure Mode

Each failure mode needs a targeted fix:

| Failure mode | Solution |
|-------------|----------|
| **Subject artist hallucination** | Anonymize artist names in the prompt ("Artist A" / "Artist B"), generate from data alone, then substitute names back. Or use a fine-tuned smaller model that hasn't memorized artist knowledge. Or use a stronger model (Sonnet/Opus) that follows the "only use provided data" instruction more reliably. |
| **Neighbor characterization** | Either include neighbor metadata in the prompt (genre, styles) so the model has grounded data to draw from, or instruct the model to only name neighbors without characterizing them ("both appear near X, Y, and Z" — stop there). |
| **DJ intent attribution** | Prompt instruction to describe observed patterns, not infer motivations. Replace "DJs value them for..." with "both tend to appear in sets alongside..." Frame as correlation, not causation. |
| **Garbage-in styles** | Better style curation upstream: filter styles by release count or prominence, exclude styles from minor/compilation appearances. This is a data pipeline fix, not a model fix. |

### Mitigation Experiment Results

A controlled experiment tested six prompt variants across the same 2×2×2 matrix (23 pairs, 138 narratives total, each with automated verification). The stricter verifier used in this round removed the AMBIGUOUS category entirely, classifying every claim as GROUNDED or UNGROUNDED.

| Variant | Halluc% | vs Baseline |
|---------|---------|-------------|
| **BASELINE** | **45%** | — |
| **NAMING-ONLY** | **20%** | **-25 pts** |
| **ANONYMIZED** | **20%** | **-25 pts** |
| **FEW-SHOT** | **21%** | **-24 pts** |
| **COMBINED** (naming-only + pattern-not-intent) | **31%** | -14 pts |
| **PATTERN-NOT-INTENT** | **40%** | -5 pts |

**Three mitigations cut hallucination by more than half.** Naming-only, anonymization, and few-shot each independently achieve ~20% hallucination, down from 45% baseline. Each addresses a different failure mode but produces a similar aggregate improvement.

**NAMING-ONLY is the highest-impact, lowest-cost change.** Adding one sentence ("do not describe or characterize neighbors — state only their names") to the system prompt dropped hallucination from 45% to 20%. It hit 0% on LOW fame + RICH data + SAME genre pairs. Neighbor characterization was the single largest hallucination source; eliminating it costs nothing.

**ANONYMIZED confirms the fame hypothesis.** It performs best on HIGH fame cells (13-17% hallucination) because the model can't activate pretraining knowledge about "Aphex Twin" when it only sees "Artist A." It's less effective on LOW fame + THIN data (36-42%) where the model hallucinates about the *structure* of the connection rather than about specific artists. The technique requires name-substitution machinery (anonymize before generation, deanonymize after) but is straightforward to implement.

**FEW-SHOT works through demonstration, not prohibition.** Two gold-standard example narratives — one with rich data, one with thin data — taught the model by showing what grounded output looks like. Hit 7% hallucination on HIGH fame + RICH data + SAME genre, the best single-cell result in the entire experiment. Models pattern-match on examples more reliably than they follow negative instructions.

**PATTERN-NOT-INTENT barely helps.** 40% vs 45% baseline. The model rephrases DJ intent into slightly different language ("both serve as," "suggesting they function as") that still constitutes ungrounded inference. The instruction to avoid specific phrases doesn't prevent the underlying reasoning pattern.

**COMBINED is worse than its components.** 31% vs 20% for naming-only alone. Longer, more complex prompts dilute the effectiveness of each individual instruction. Simpler, more focused prompts work better with Haiku.

**Per-cell breakdown:**

| Cell | Baseline | Naming-Only | Anonymized | Few-Shot |
|------|----------|-------------|------------|----------|
| HIGH fame + RICH + CROSS | 43% | 24% | 13% | 18% |
| HIGH fame + RICH + SAME | 38% | 10% | 16% | 7% |
| HIGH fame + THIN + CROSS | 47% | 25% | 29% | 21% |
| HIGH fame + THIN + SAME | 43% | 15% | 17% | 25% |
| LOW fame + RICH + CROSS | 38% | 22% | 13% | 26% |
| LOW fame + RICH + SAME | 34% | 0% | 17% | 11% |
| LOW fame + THIN + CROSS | 60% | 38% | 36% | 40% |
| LOW fame + THIN + SAME | 60% | 12% | 42% | 42% |

The LOW fame + THIN data cells remain the hardest — 36-42% hallucination even with mitigations. When the model has neither pretraining knowledge nor provided data, it still fabricates structural claims about the connection. This is the cell where richer source material (reviews, bios, Wikipedia) would have the most impact.

### Recommended Production Prompt Changes

Based on the experiment results, in priority order:

1. **Add naming-only instruction immediately.** One sentence added to the existing system prompt: "When naming shared set neighbors, state ONLY their names. Do not describe, characterize, or categorize the neighbors in any way." Zero cost, 25-point hallucination reduction.

2. **Add few-shot examples.** Two gold-standard narratives (one rich-data, one thin-data) appended to the system prompt. Adds ~200 tokens to the system prompt but further reduces hallucination, especially for HIGH fame + RICH data pairs (down to 7-18%).

3. **Implement anonymization for HIGH fame pairs.** When either artist has >800 total plays, strip names before generation and substitute back after. Requires a small code change in the narrative endpoint. Most effective for the pairs where the model's pretraining knowledge is strongest.

### Combination Experiment Results

A follow-up experiment tested whether the three winning mitigations could be combined without the dilution seen in the earlier COMBINED (naming-only + pattern-not-intent) variant. Four variants were tested across the same 2×2×2 matrix (20 pairs):

| Variant | Halluc% | vs Baseline | vs Best Single |
|---------|---------|-------------|----------------|
| **BASELINE** | **33%** | — | — |
| **ANON+FEWSHOT** | **20%** | -13 pts | same as singles |
| **FEWSHOT+NAMING** | **14%** | -19 pts | -6 pts better |
| **ANON+FEWSHOT+NAMING** | **9%** | **-24 pts** | **-11 pts better** |

The combinations are additive, not dilutive — the opposite of the earlier COMBINED result. The difference: the earlier failure combined two prohibitive instructions (don't characterize + don't infer intent) that competed for the model's attention. These combinations use three distinct mechanisms (structural, demonstrative, prohibitive) that target different failure modes without interfering.

**Per-cell breakdown for ANON+FEWSHOT+NAMING:**

| Cell | Baseline | ANON+FEWSHOT+NAMING |
|------|----------|---------------------|
| HIGH fame + RICH + CROSS | 41% | **0%** |
| HIGH fame + RICH + SAME | 21% | **0%** |
| HIGH fame + THIN + CROSS | 42% | **12%** |
| HIGH fame + THIN + SAME | 32% | **6%** |
| LOW fame + RICH + CROSS | 17% | **0%** |
| LOW fame + RICH + SAME | 40% | 23% |
| LOW fame + THIN + CROSS | 60% | **0%** |
| LOW fame + THIN + SAME | 30% | 29% |

Zero hallucination in 4 of 8 cells. The remaining hallucination concentrates in SAME-genre pairs where the model has less structural contrast to work with. The hardest cells (LOW+RICH+SAME at 23%, LOW+THIN+SAME at 29%) are where richer source material would help most.

**FEWSHOT+NAMING at 14% is the best no-anonymization option.** It requires only prompt changes — no name-substitution code. It hit 0% on HIGH+RICH+CROSS and 4% on HIGH+RICH+SAME.

### Cost of the Three-Way Combo

The ANON+FEWSHOT+NAMING variant adds cost in two dimensions:

**Prompt tokens.** The few-shot examples add ~200 tokens to the system prompt. The anonymization instructions add ~30 tokens. The naming-only instruction adds ~50 tokens. Total overhead: ~280 tokens per narrative call. At Haiku's input pricing ($0.80/MTok), that's $0.000224 per narrative — negligible.

**Code complexity.** Anonymization requires pre-processing (strip names → "Artist A"/"Artist B", neighbors → "Neighbor 1"/"Neighbor 2") and post-processing (substitute real names back into the generated text). This is ~20 lines of code in the narrative endpoint. The few-shot examples and naming-only instruction are just prompt text, no code change beyond the system prompt string.

**Latency.** The longer system prompt adds a few milliseconds to each Haiku call. Anonymization adds two string-replacement passes (pre and post). Neither is perceptible given that the Haiku call itself takes ~500ms. And narratives are cached — each pair is generated once and served from the sidecar database for all subsequent requests.

**Net cost: ~$0.0002 per narrative in additional tokens, ~20 lines of code, no perceptible latency impact.** For a 9% hallucination rate down from 33-45%, this is an excellent trade.

### Generate-Score-Regenerate Loop

A closed-loop experiment tested whether token-match scoring can drive iterative improvement: generate a narrative with ANON+FEWSHOT+NAMING, score it with token-match, and if above threshold (0.5), feed the ungrounded terms back as constraints ("do NOT use these words") and regenerate. Up to 3 retries allowed.

Results across 17 pairs:

| Metric | Value |
|--------|-------|
| Passed on first try | 14 / 17 (82%) |
| Needed retry | 3 / 17 (18%) |
| Converged after retry | 3 / 3 (100%) |
| Did not converge | 0 (0%) |
| Max iterations needed | 2 |
| Mean token score | 0.380 → 0.362 |
| Mean claim score | 0.263 → 0.252 |

The loop works but is rarely needed. The ANON+FEWSHOT+NAMING prompt already produces narratives below the token-match threshold 82% of the time. The 3 pairs that retried all converged on the second attempt — no pair needed a third try.

The three pairs that needed retry (Destroyer/Kelela, Madlib/Kelela, Magnetic Fields/Grateful Dead) dropped meaningfully on the second pass: token scores went from 0.50-0.53 down to 0.38-0.45 after the ungrounded terms were fed back as constraints.

**Production cost of the loop:** negligible. 82% of narratives incur zero extra cost. The remaining 18% need one additional generation + scoring call (~$0.0004). At the top-5-per-artist scale (34,402 pairs), the loop adds roughly $2.50 total on top of the $36 base generation cost.

**Token-match vs claim-ratio divergence:** some narratives pass token-match (grounded vocabulary) but score high on claim-ratio (ungrounded assertions). For example, Madlib/Prince: token=0.25 (PASS) but claim=0.36. This happens when a narrative uses words from the data but makes structural claims about the relationship that aren't grounded. This reinforces using both methods: token-match as the fast production gate, claim-ratio as a periodic quality audit on cached narratives.

### Additional Mitigation Strategies (Not Yet Tested)

**Structured output with grounding citations.** Ask for JSON with each claim paired with the data fields it drew from, then post-process into prose. If a claim can't cite a source field, it gets dropped. This makes hallucination structurally impossible rather than relying on self-restraint.

**Two-pass generation.** First pass: generate the narrative. Second pass: a separate call verifies each claim against the input data and flags ungrounded claims. If the verifier flags issues, regenerate with the specific flagged claims appended as "do not include" instructions. Cost is 2-3x per narrative, but narratives are cached.

**Temperature reduction.** Default temperature allows creative gap-filling. Dropping to 0 or near-0 produces more formulaic but more grounded prose.

**Local model fine-tuning.** Fine-tune a 7B-14B model on verified gold-standard narratives. A model trained specifically on "given this structured data, produce this narrative" may hallucinate less than a general-purpose model, because it learns the task pattern rather than relying on general instruction following. The training set would be small (hundreds of examples) but the task is narrow. Hugging Face provides PEFT/LoRA tooling for consumer hardware. Larger local models (70B, e.g. Llama 3.1 70B or Qwen 2.5 72B) approach Haiku's instruction-following quality and could replace the API dependency entirely.

### Truthiness Scoring Methods

Four scoring methods were tested on 13 pairs, each scored with both BASELINE and BEST (ANON+FEWSHOT+NAMING) narratives as calibration. The key question: does each method consistently score BASELINE higher (worse) than BEST?

| Method | BASELINE mean | BEST mean | Correct ranking | API cost |
|--------|--------------|-----------|-----------------|----------|
| **Claim-ratio** | 0.430 | 0.135 | 100% (13/13) | 1 Haiku call |
| **Weighted claims** | 0.240 | 0.091 | 77% (10/13) | 1 Haiku call |
| **Entailment** | 0.000 | 0.000 | — (no signal) | 1 Haiku call |
| **Token-match** | 0.626 | 0.359 | 100% (13/13) | **zero** |

**Claim-ratio** decomposes the narrative into individual claims, checks each grounded/ungrounded, and computes the ungrounded fraction. Most reliable model-based method — wide spread between good and bad narratives, perfect calibration. Threshold: ~0.2 for a quality gate.

**Token-match** extracts content words from the narrative and checks each against the input data fields with string matching. No model needed, instant results. Absolute scores are higher (flags valid paraphrases like "soundscapes" for "Ambient") but relative ranking is perfect. Threshold: ~0.5 for a flag.

**Weighted claims** adds a categorization step (ARTIST/NEIGHBOR/CONTEXT/INTENT) and applies weights (ARTIST=3, STRUCTURE=2, others=1). The extra model judgment introduces noise — in one case it scored BEST worse than BASELINE. The marginal benefit over unweighted claim-ratio doesn't justify the added complexity.

**Entailment** scored 0.000 for everything. The "does the data entail this?" framing produced no discrimination between good and bad narratives. Dead method.

**Recommended production setup:**
- **Token-match as the always-on scorer.** Free, instant. Run on every generated narrative. Flag anything above 0.5 for regeneration.
- **Claim-ratio as the quality gate.** One Haiku call per flagged narrative, or as a periodic audit on cached narratives. Reject anything above 0.2.

### Verifier Calibration

The first-round verifier (3-category: GROUNDED/UNGROUNDED/AMBIGUOUS) was systematically generous — it marked genuine hallucinations as AMBIGUOUS rather than UNGROUNDED (e.g., characterizing shared neighbors as "introspective, genre-fluid" when only their names are provided). The second-round verifier used a stricter 2-category prompt (GROUNDED/UNGROUNDED only) with explicit instructions that neighbor characterization and DJ intent attribution are UNGROUNDED. This produced cleaner signal and more actionable results. For production use, the strict 2-category verifier is recommended.

### Root Cause

The hallucination problem is not primarily a prompt engineering problem. It is four distinct problems that happen to manifest as the same symptom (the model stating things not in the data). The subject artist and neighbor characterization modes are knowledge-contamination problems — the model knows too much. The DJ intent mode is a reasoning problem — the model infers causation from correlation. The garbage-in mode is a data quality problem upstream of the model entirely.

The mitigation experiments showed that targeted, simple interventions (one-sentence naming-only instruction, two few-shot examples) are more effective than complex combined prompts. The remaining ~20% hallucination floor is structural — it represents the model's tendency to construct narrative bridges even when instructed not to. Breaking through that floor likely requires either richer input data (so the model doesn't need to bridge gaps) or architectural changes (structured output, two-pass verification, or fine-tuning).

The real fix is the combination of:

1. **Better data selection** (less room to fill gaps — omit empty fields, curate style lists by prominence, convert numbers to qualitative labels)
2. **Richer source material** (less need to fill gaps — review descriptors, artist bios, and Wikipedia context give the model concrete things to say about what the music sounds like)
3. **Targeted prompt mitigations** (naming-only instruction + few-shot examples as the primary intervention; anonymization for high-fame pairs)
4. **Honest gap acknowledgment** (when data is truly thin, the narrative should describe the co-occurrence pattern and not attempt to characterize the sound)

### Verb Monotony

The baseline prompt's example phrasing ("DJs reach for both at similar moments") was adopted as a template by Haiku, producing monotonous narratives where every pair is "reached for" by DJs who "occupy" spaces and "suggest" roles. The banned word list improved variety but introduced substitution tics. A better approach may be to remove all example phrasing from the system prompt and test whether Haiku produces more natural variety without a template to latch onto.

### Accuracy Evaluation Method

For ongoing evaluation, a second LLM pass can fact-check generated narratives against the input data: for each factual claim in the narrative, verify it appears in the provided fields. Claims not grounded in the data are flagged. This could be automated as a CI-style check on narrative quality, running periodically against a sample of cached narratives.

## Sequence of Work

These four fronts have natural dependencies that suggest an ordering:

1. **Data selection improvements** — cheapest, improves existing narratives immediately. Implement AA-weighted neighbors, minimum score threshold, style list capping, field pruning. No new infrastructure needed.
2. **Review corpus expansion** — fix name matching, crawl Wikipedia + 3-4 specialized sources, build extraction pipeline. Enriches narrative quality for covered artists.
3. **Bandcamp bio extraction** — separate pipeline for first-party descriptions. Fills the independent/underground long tail that reviews miss.
4. **Embedding training** — skip-gram over show sequences. Expands which artist pairs can be narrated at all. Most valuable after review descriptors exist, so the newly surfaced pairs have rich data to narrate with.

Each step improves narratives independently, and each makes the subsequent steps more effective.

## Artifacts from This Investigation

The following scripts in `scripts/` were produced during this investigation and can be used for further iteration:

| Script | Purpose |
|--------|---------|
| `test_narrative_augmentation.py` | Three hand-crafted test cases with augmented sequential context. Tests the prompt with real data. |
| `generate_narrative_samples.py` | Generates 20 narrative samples from database pairs across three scenarios (no edge, cross-genre, sparse neighborhood). |
| `compare_neighbor_weighting.py` | Compares four neighbor weighting methods (raw, AA, RA, degree ceiling) across eight pairs. |
| `experiment_narrative_variants.py` | A/B/C test of three prompt variants (baseline, anti-hallucination, anti-hallucination + varied language) across 10 AA-filtered pairs. |
| `coverage_with_normalization.py` | Improved review-to-artist matching with name normalization (split on `/` and `&`, diacritic stripping, "The" removal). |
| `hallucination_risk_experiment.py` | Earlier risk-tier experiment (HIGH/MEDIUM/LOW aggregate scoring) with generation and verification. |
| `hallucination_matrix_experiment.py` | Full 2×2×2 matrix experiment (fame × data richness × genre distance) with 3 pairs per cell, generation, verification, and summary table. |
| `mitigation_experiment.py` | Six prompt variants (baseline, naming-only, pattern-not-intent, anonymized, few-shot, combined) tested across the same 2×2×2 matrix with strict 2-category verification. |
| `combo_mitigation_experiment.py` | Combination variants (anon+fewshot, fewshot+naming, anon+fewshot+naming) tested across the 2×2×2 matrix. |
| `scoring_methods_experiment.py` | Four truthiness scoring methods (claim-ratio, weighted, entailment, token-match) compared across 13 pairs with BASELINE vs BEST narratives. |
| `generate_score_regenerate.py` | Closed-loop experiment: generate → token-match score → feed ungrounded terms back → regenerate. Tests convergence. |

Output files in `output/`:

| File | Contents |
|------|----------|
| `narrative_samples.txt` | 20 generated narratives across three categories |
| `narrative_experiments.txt` | 10 pairs × 3 prompt variants = 30 narratives |
| `hallucination_matrix.txt` | 19 pairs across 7 matrix cells with narratives, verification, and summary table |
| `mitigation_experiment.txt` | 23 pairs × 6 variants = 138 narratives with verification and summary tables |
| `combo_mitigation_experiment.txt` | 20 pairs × 4 variants = 80 narratives with verification and summary tables |
| `scoring_methods_experiment.txt` | 13 pairs × 2 variants × 4 scoring methods with calibration results |
| `generate_score_regenerate.txt` | 17 pairs through generate-score-regenerate loop with per-iteration scores |
