# Enriching Narrative Generation in the WXYC Freeform Map

## Abstract

The WXYC Freeform Map (explore.wxyc.org) visualizes over two decades of DJ transition data as a semantic artist graph, with an LLM-powered narrative endpoint that explains artist relationships in plain English. This paper documents research into making those narratives more accurate, more musically specific, and capable of describing relationships the graph cannot currently see. The work decomposes into four fronts: better selection of the data we already pass to the model, richer source material drawn from professional music criticism, new relationships surfaced via skip-gram embeddings over show sequences, and accuracy mitigations within the model itself.

The accuracy work is the most developed. Controlled experiments across a 2×2×2 risk-factor matrix (artist fame × data richness × genre distance) identified four distinct hallucination failure modes — fame-driven subject hallucination, neighbor characterization, DJ-intent attribution, and upstream style-tag noise. Three targeted mitigations using different mechanisms (structural anonymization, demonstrative few-shot examples, prohibitive naming-only constraint) combine additively to reduce hallucination from 45% to 9%, with a closed generate-score-regenerate loop pushing all sampled narratives below threshold within two iterations. Two complementary scoring methods (mechanical token-match and LLM-driven claim-ratio) act as quality gates, though both measure grounding fidelity rather than truthiness — a gap addressed by the proposed constraint-based validation grammar derived dimensionally from the data itself.

The data-selection and accuracy mitigations described here have shipped to production; the remaining fronts (source material expansion, skip-gram embeddings, the constraint ontology) are scoped but not yet implemented. The paper closes with future directions: multi-modal grounding from audio embeddings, RAG-style review injection, wrongness detection beyond grounding fidelity, and listener-feedback evaluation.

## 1. Introduction

WXYC 89.3 FM is the student-run freeform radio station at UNC Chapel Hill. Since 2003, every song played on air has been logged in a digital flowsheet, producing a dataset of approximately 2 million entries spanning over two decades. The Freeform Map pipeline extracts consecutive artist pairs within radio shows, computes Pointwise Mutual Information (PMI) to identify statistically significant co-occurrences, and exports a graph where nodes are artists and edges encode the strength of DJ-curated transitions.

The Graph API at explore.wxyc.org serves this graph through an interactive D3.js visualization. Among its endpoints is a narrative generator (`/graph/artists/{id}/explain/{target_id}/narrative`) that calls Claude Haiku to produce 2–3 sentence natural-language explanations of artist relationships. The narrative endpoint receives structured data about both artists (genre, Discogs styles, audio profile features) and any relationships between them (DJ transition count, PMI score, shared personnel, etc.), then prompts Haiku to synthesize a human-readable explanation.

A note on what the data fields mean. WXYC's "genre" labels (Rock, Hiphop, Electronic, OCS, Africa, Asia, Latin, Blues, Reggae, Classical, Jazz) are physical-shelving codes for the station's record library, not descriptions of sound. "Rock" includes Bob Dylan and Black Dice; "OCS" includes both spoken word and minimalist composition. Discogs styles are richer but inconsistently applied — long-tail releases push noisy tags into an artist's top five. Audio profile features come from Essentia classifiers run over the WXYC audio archive, currently covering only a fraction of the artist graph. None of these fields is a clean musical description on its own; the narrative model has to triangulate.

The narrative endpoint works well when two artists share a direct DJ transition edge with rich metadata. But it falls silent when artists lack a direct edge, and even when it speaks, it often describes categorical roles and genres rather than what the music actually sounds like. The research presented here addresses both problems.

## 2. Problem Statement

The goal is to make the narrative endpoint say true, specific, musical things about artist relationships — including relationships the graph cannot currently see.

This decomposes into four subproblems:

1. **Data selection.** The narrative prompt previously received shared neighbors ranked by play count, surfacing generic hub artists (Miles Davis, The Beatles) that appear in hundreds of neighborhoods and prove nothing about the specific pair being narrated. The data passed to the model needs better curation.

2. **Source material.** The available data (Discogs style tags, WXYC genre labels, audio profile numbers) tells the model what category an artist falls into but not what the music sounds like. The narratives talk about roles and contexts but rarely about rhythm, texture, instrumentation, or atmosphere.

3. **Coverage.** The pairwise PMI graph requires two artists to appear back-to-back at least twice to create an edge. Many meaningful relationships are invisible because the artists occupy similar sequential contexts without ever being directly juxtaposed.

4. **Accuracy.** The model hallucinates — describing The Smiths as "electronically-inflected funk and soul," calling Josephine Foster an experimental jazz artist, confusing Muddy Waters with Crystal Waters (a house singer). These errors damage trust and must be systematically reduced.

The data-selection and accuracy fronts have largely shipped; sections below mark each finding with its current implementation status. Source material and embeddings remain scoped but unimplemented.

## 3. Data Selection: Neighbor Weighting

> **Status:** Shipped. Adamic-Adar reranking, the 0.8 minimum-score threshold, the 5-style cap, qualitative audio descriptors, empty-field omission, and a UI-side heat slider that modulates the DJ-vs-enrichment weight balance during neighbor selection are all live in the production prompt assembly.

### 3.1 The Problem with Play-Count Ranking

When the narrative endpoint describes how two artists relate through shared sequential context, it passes shared DJ transition neighbors. The original ranking was by total play count, which consistently surfaced the same high-degree hub artists. For example, Bob Marley and the Wailers and Les Rallizes Dénudés share five neighbors: Björk, The Velvet Underground, Tim Hecker, Frank Sinatra, and Hiatus Kaiyote — every one of them a high-degree hub appearing in hundreds of other neighborhoods. Surfacing them tells us nothing about the specific Marley/Rallizes relationship, which is itself a spurious co-occurrence.

### 3.2 Weighting Methods Compared

We evaluated four neighbor weighting methods across eight artist pairs with no direct DJ transition edge:

**Raw (unweighted, by play count).** The baseline. Surfaces hub artists that dilute signal.

**Adamic-Adar.** Weights each shared neighbor by 1/log(degree). A shared neighbor connected to 27 artists contributes heavily; one connected to 346 contributes almost nothing. From link prediction literature, originally designed to predict missing edges in social networks.

**Resource Allocation.** Weights each shared neighbor by 1/degree. More aggressive than Adamic-Adar — produces sharper separation between informative and uninformative neighbors.

**Degree Ceiling.** Excludes any shared neighbor above the 95th percentile in degree (43 edges in the graph at experiment time). A crude filter.

> Degree counts cited below are as of the experiment run (April 2026). The production graph grows daily; absolute numbers have drifted but the relative rankings — the substance of the AA argument — are stable.

### 3.3 Results

The degree ceiling was too aggressive, eliminating all shared neighbors for 5 of 8 test pairs. The remaining three methods all produced valid rankings, with Adamic-Adar and Resource Allocation agreeing on order but differing in separation magnitude.

Concrete improvements from Adamic-Adar reranking:

- **Peter Brötzmann / Angel Olsen.** Raw ranking leads with Arthur Russell (degree 429) and Dan Melchior (degree 379), both pan-genre hubs. Adamic-Adar surfaces Dum Dum Girls (degree 27) and Joe McPhee (degree 36). McPhee is a free jazz saxophonist who frequently played with Brötzmann; Dum Dum Girls is a lo-fi indie pop band whose neighborhood overlaps with Olsen's. Each low-degree neighbor speaks to one side of the pair — together they make the cross-genre bridge legible in a way that "they both share Arthur Russell as a neighbor" doesn't.

- **Caetano Veloso / Don Cherry.** Dennis Bovell (degree 37), a UK dub reggae producer (Linton Kwesi Johnson, The Slits), rises from last place to first under Adamic-Adar. Veloso (Brazilian tropicália) and Cherry (free jazz with a strong world-music turn) are both eclectic, internationalist artists whose work sits at folk/jazz/world intersections. Bovell occupies a similar left-field-internationalist position in DJs' programming — that he surfaces as a shared neighbor is more telling than Miles Davis (degree 346), Animal Collective (degree 392), or Sonic Youth (degree 309), each of which appears in hundreds of unrelated neighborhoods.

- **Religious Knives / Pastor T.L. Barrett.** United Waters (degree 27) rises to the top; Can (degree 204) drops. The low-degree shared neighbor is the more distinctive signal.

Resource Allocation produced even sharper separation: for Caetano Veloso / Don Cherry, Dennis Bovell scores 9× higher than Sonic Youth under RA, versus 1.6× under Adamic-Adar. Either method is a substantial improvement over raw ranking; production uses Adamic-Adar for its less aggressive weighting and clearer interpretability (1/log(degree) is a familiar quantity).

### 3.4 Minimum Score Threshold

Some pairs are spuriously connected through generic hubs regardless of weighting. Bob Marley / Les Rallizes Dénudés remains incoherent even after Adamic-Adar reranking — the top neighbor (Frank Sinatra, AA score 0.23) is uninformative. A minimum total Adamic-Adar score threshold of 0.8 across all shared neighbors filters these pairs. In production, when the threshold is not met, the endpoint returns a canned "insufficient signal" narrative ("WXYC DJs occasionally play these artists together, but they don't share enough specific musical context …") rather than fabricating a connection. This same threshold gates the experimental sample selection in later sections.

### 3.5 Data Field Pruning

Beyond neighbor selection, experiments revealed several data hygiene issues:

- **Overly long style lists.** Outkast has 53 Discogs styles. Destroyer's top 5 include "Breakbeat" and "Techno" from minor releases, leading the model to describe an indie rock artist as "channeling breakbeat and techno elements." Capping to the top 5 styles by release count reduces this surface. (The current production cap is alphabetical-top-5, since the upstream `artist_style` table does not yet persist a release-count column; proper release-count ranking is a pipeline-side follow-up.)

- **Raw numeric leakage.** Danceability scores like "(0.68)" surfaced verbatim in narrative text when passed as floats in the prompt data. Converting to qualitative labels at extremes only (with unremarkable middles omitted entirely so the model has no number to quote) prevents this.

- **Empty field compensation.** When Konono No 1's styles array was empty (`styles: []`), the model compensated by hallucinating genre descriptions. Omitting empty and placeholder fields ("unknown", "various", "n/a") entirely from the prompt removes the temptation.

## 4. Source Material: Review Text and Artist Bios

> **Status:** Scoped, not implemented. The review corpus exists locally at `data/reviews/<source>/reviews.jsonl` (37,422 articles, written by `scripts/crawl_reviews.py`), with consolidated single-file copies mirrored into the org-level `research-data/reviews/` repo for archival; the extraction pipeline, name-matching fixes, and prompt integration are next.

### 4.1 The Musicality Gap

The current narrative data (Discogs style tags, WXYC genre labels, audio profile numbers from Essentia classifiers) tells the model what category an artist falls into but not what the music sounds like. A narrative can say "both share ambient and acoustic styles" but not "shimmering guitar arpeggios over a motorik beat" or "amplified thumb piano patterns." The gap between categorical description and sonic characterization is where narratives feel generic. WXYC's genre labels make this gap structural rather than incidental: "Rock" is a shelving code that yields the same prompt-level description for Bob Dylan and Black Dice.

### 4.2 Three Complementary Text Sources

Three types of text sources fill this gap differently, and each should be extracted and surfaced differently in the narrative prompt:

**Professional reviews** (Pitchfork, Stereogum, The Quietus, Tiny Mix Tapes, Bandcamp Daily, plus specialized sources) provide critical characterization — a trained listener's description of what the music sounds like, its mood, and how it relates to other artists. These are the richest source of sonic descriptors, mood language, and comparative references.

**Artist and label bios** (Bandcamp release pages) provide first-party factual descriptions — what instruments were used, how it was recorded, self-described influences. A Pitchfork review saying "shimmering guitars over a motorik beat" is a critic's characterization. A Bandcamp description saying "recorded on a 4-track with lap steel and Farfisa organ" is a factual statement about instrumentation.

**Encyclopedic descriptions** (Wikipedia, sourced via Wikidata QIDs already in the pipeline database) provide edited, sourced descriptions of musical style with broad coverage. These fill the historical and world music gaps where no review publication has covered an artist.

The three types are not interchangeable. Use review language for characterization, bio details for concrete production facts, and Wikipedia context for cultural and historical framing.

### 4.3 Extraction Schema

Each source type yields its own schema, reflecting the different evidence it provides.

**Reviews → five fields:**
- `sonic_descriptors` — phrases describing what the music sounds like: "shimmering guitars," "thudding kick drum," "nasal vocal delivery"
- `mood_atmosphere` — evocative terms: "claustrophobic," "sun-drenched," "nocturnal," "anthemic"
- `instrumentation` — instruments and production tools: "thumb piano," "analog synth," "upright bass"
- `comparative_artists` — explicit comparisons: "recalls early Talk Talk," "indebted to Lee Perry's dub experiments"
- `genre_descriptors` — finer-grained genre terms: "spiritual jazz," "Saharan rock," "zolo"

**Artist bios → three fields:**
- `instrumentation` — self-described instruments and tools
- `influences` — self-described influences and reference points
- `production_details` — recording methods, studio context

**Wikipedia → two fields:**
- `style_description` — prose from "Musical style" / "Artistry" sections
- `genre_context` — regional, historical, or cultural context

Extraction method: structured LLM extraction via Haiku. Each review or bio is passed through with a prompt asking for JSON output in the relevant schema, with explicit instruction to extract only what is stated, not to infer. At approximately 37,000 existing reviews averaging 700 words, extraction costs roughly $2 in Haiku input tokens.

The extracted fields surface in the narrative prompt as a labeled per-artist subobject, so the model knows how to weight each:

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

The system-prompt instruction is to use the language naturally rather than attribute it ("critics describe …", "the artist describes …"); the source labels are for the model's internal disambiguation, not for surfacing in the output.

### 4.4 Current Corpus and Coverage

The existing review corpus contains 37,422 articles across four sources:

| Source | Articles |
|--------|----------|
| The Quietus | 15,845 |
| Stereogum | 7,346 |
| Bandcamp Daily | 7,180 |
| Tiny Mix Tapes | 7,051 |

Full-text matching against the artist graph (136,675 total artists) yields 20,068 matched artists. For artists with 100+ total plays (the ones that matter for narratives), the remaining unmatched count by genre:

| Genre | Unmatched (100+ plays) | Recommended source |
|-------|----------------------|---------------------|
| Rock | 495 | Name matching fixes (VA entries, slash variants) |
| Hiphop | 186 | Name matching fixes ("J Dilla / Jay Dee" → "J Dilla") |
| Jazz | 127 | All About Jazz, Wikipedia |
| Electronic | 127 | Resident Advisor |
| OCS | 119 | Aquarium Drunkard |
| Africa | 79 | Songlines, Afropop Worldwide, Wikipedia |
| Asia | 62 | Wikipedia, Songlines |
| Latin | 60 | Wikipedia |
| Blues | 51 | Wikipedia |
| Classical | 42 | The Wire |
| Reggae | 40 | Wikipedia |

A significant portion of the "unmatched" count is a name matching problem, not a corpus gap. "J Dilla / Jay Dee" doesn't match a review titled "J Dilla"; "Ty Segall & Mikal Cronin" doesn't match "Ty Segall." Normalizing matches by splitting on `/`, `&`, and stripping "Various Artists" entries improved title-only coverage from 53% to 56%. Full-text matching (searching the first 500 characters of review body text) brought it further — Ella Fitzgerald, Duke Ellington, and Herbie Hancock were all found mentioned in review body text as reference points, even without dedicated reviews.

### 4.5 New Sources to Crawl

The four sources already in the corpus — TMT (Wayback), The Quietus (WordPress sitemap), Bandcamp Daily (Wayback CDX), Stereogum (Next.js `__NEXT_DATA__`) — each have a discovery function in `scripts/crawl_reviews.py` and write JSONL via the same trafilatura extraction path. Priority order for the next sources, by coverage impact:

1. **Wikipedia** — broadest coverage. Covers blues, Latin, reggae, jazz, Asia gaps via Wikimedia API using Wikidata QIDs already in the pipeline. Estimated: ~3 hours.
2. **All About Jazz** — 30K+ reviews covering the jazz gap. Estimated: ~12 hours.
3. **Resident Advisor** — 15K reviews covering electronic/dance. Estimated: ~6 hours.
4. **Aquarium Drunkard** — 5K articles covering OCS/folk/outsider. Estimated: ~2 hours.
5. **Songlines** — world music reviews for the Africa/Asia/world gap. Estimated: ~1 hour.
6. **Bandcamp release pages** — first-party artist/label descriptions for the independent long tail.

None of these have discovery functions yet; each needs a 30–50-line addition to `crawl_reviews.py` (sitemap or CDX query → URL cache → reuse the shared `fetch_live` + `extract_text` path). All crawl times are bounded by polite rate limiting (1-2 second delays). Sources are independent and can run in parallel.

### 4.6 Graph Propagation for Remaining Gaps

For artists with no review text, no bio, and no Wikipedia article, descriptors can be propagated through the graph. If an artist has strong PMI edges to three artists that all carry the review descriptor "shimmering guitars," that descriptor can be inferred with diminishing confidence, weighted by edge strength. This is a fallback — the narrative prompt should not present propagated descriptors with the same confidence as directly observed ones.

For artists with no data at all, the narrative should lean on the sequential context (shared neighbors, same-show counts) and not attempt to describe the sound. "Both appear in sets alongside X and Y" is true and useful even without sonic descriptors.

## 5. Embeddings: Expanding Graph Coverage

> **Status:** Scoped, not implemented. The three scenarios below were validated by hand-crafting the sequential context that embeddings would surface and verifying that the narrative prompt can produce grounded output from it. Training the embeddings themselves is the next step.

### 5.1 The Pairwise PMI Limitation

The current graph requires two artists to appear back-to-back at least twice (with `min_count=2`) to create an edge. This misses artists that occupy similar roles in DJ sets without ever being directly juxtaposed — the distributional similarity that linguists call "words that appear in similar contexts have similar meanings."

### 5.2 Skip-Gram Embeddings over Show Sequences

Treating each radio show as a sentence of artists and training word2vec-style skip-gram embeddings would produce per-artist vectors encoding sequential co-occurrence patterns across all context window sizes simultaneously. Artists that appear in similar positions within sets — surrounded by similar neighbors — end up close in embedding space, even if they've never appeared back-to-back. The show segmentation already exists in the pipeline's `semantic_index/adjacency.py` module.

### 5.3 Three Narrative Scenarios

Experiments with hand-crafted sequential context data demonstrated three scenarios where embeddings would enable narratives that the current graph cannot produce.

**Scenario 1: No direct edge, shared context.** Tinariwen (844 plays, Saharan Tuareg desert blues) and Konono No 1 (695 plays, Congolese likembe ensemble) have never appeared back-to-back in over two decades of flowsheets. But DJs place them near the same artists — Ali Farka Toure, Mdou Moctar, William Parker, Duke Ellington. With region data added to the prompt, the narrative correctly distinguished their specific traditions rather than generalizing across the African continent:

> "Tinariwen and Konono No. 1 represent distinct but complementary approaches to electric music rooted in African traditions. Tinariwen channels Saharan Tuareg guitar blues with psychedelic and experimental textures, while Konono No. 1 builds hypnotic grooves from amplified likembe (thumb piano) patterns in Kinshasa."

**Scenario 2: Contextual similarity despite surface difference.** Outkast (1,780 plays, hip-hop) and Dam-Funk (834 plays, electronic) have no direct edge but share 32 non-VA neighbors including Miles Davis, Nina Simone, A Tribe Called Quest, and Madlib. The narrative leveraged both the shared funk/P-Funk styles in the data and the sequential context:

> "Outkast and Dam-Funk both draw from funk and P-Funk traditions while incorporating modern production — Outkast through hip-hop's boom-bap and neo-soul layers, Dam-Funk through electronic and deep house frameworks."

**Scenario 3: Sparse neighborhood enrichment.** Michael Nyman (214 plays) has only 1 non-compilation DJ transition edge (Bitchin' Bajas) despite being a canonical minimalist composer. He has zero edges to Philip Glass, Steve Reich, or Terry Riley. He appeared in the same show as Glass twice, never back-to-back. The narrative used neighborhood asymmetry and same-show count to produce a grounded explanation:

> "Michael Nyman and Philip Glass are both foundational minimalist composers working in instrumental, score-based music — a connection reflected in their shared OCS classification and two appearances in the same WXYC shows."

### 5.4 Embeddings as Infrastructure

The embeddings would not become a new edge type, a new neighbor list, or anything the user sees. They are query-time infrastructure: when the narrative endpoint receives a pair with no direct relationship, it computes cosine similarity between embedding vectors. If similarity is high, it retrieves the shared sequential context and passes it to the narrative prompt. The word "embedding" never appears in user-facing output.

### 5.5 Relative Value: Embeddings vs. Reviews

Embeddings and reviews are complementary. Reviews improve narrative *quality* — what the model can say about a pair it already knows about. Embeddings improve narrative *coverage* — which pairs surface at all. The natural sequence puts reviews first (richer text for already-narratable pairs), then embeddings (newly surfaceable pairs that can be narrated with the now-richer text). Doing embeddings first risks generating shallow narratives for surfaced-but-thin pairs, training listeners to ignore the new connections.

## 6. Hallucination Analysis

> **Status:** Shipped. ANON+FEWSHOT+NAMING is live in the production prompt (`semantic_index/api/narrative.py`, prompt version 11). Anonymization triggers above an 800-play threshold; the few-shot examples and naming-only constraint are unconditional.

### 6.1 Initial Observations

Early narrative experiments revealed systematic hallucination patterns:

- "The Smiths and Dam-Funk represent different entry points into electronically-inflected funk and soul." The Smiths are a jangle-pop/post-punk band with no connection to funk.
- "Josephine Foster and Billie Holiday share roots in experimental approaches to jazz and blues traditions." Josephine Foster is a folk singer; neither artist is experimental.
- "Waters pioneered electric Chicago blues and jump blues with raw harmonica and driving grooves." This was about Crystal Waters, a house singer, not Muddy Waters.
- "Pastor T.L. Barrett represent distinctly different sonic territories — one instrumental and experimental." Pastor T.L. Barrett sings religious soul and gospel.

These four examples were drawn from incidental observations during the matrix experiment. A curated bait set — pairs known to trigger pretraining hallucinations, distributed above and below the 800-play anonymization threshold — lets us measure ANON+FEWSHOT+NAMING's suppression of these failure modes directly rather than by chance. That construction shipped as `scripts/eval/build_bait_set.py` (closed WXYC/semantic-index#278); the curated pairs live in `scripts/eval/bait_pairs.json`, split into above/below the 800-play threshold.

### 6.2 Risk Factor Matrix

To understand what predicts hallucination, we tested narratives across a 2×2×2 matrix of risk factors:

- **Fame:** HIGH (>800 total plays) / LOW (150–400 total plays)
- **Data richness:** RICH (3+ Discogs styles AND audio profile) / THIN (≤2 styles OR no audio)
- **Genre distance:** CROSS (different genres) / SAME (same genre)

Each cell was populated with 3 artist pairs (19 pairs total, as LOW+THIN+SAME had insufficient qualifying pairs). Every narrative was verified by a separate Haiku call that decomposed the text into individual claims and labeled each as GROUNDED or UNGROUNDED.

| Cell | Pairs | Grounded | Ungrounded | Halluc% |
|------|-------|----------|------------|---------|
| HIGH fame + RICH data + CROSS genre | 3 | 20 | 5 | 33% |
| HIGH fame + RICH data + SAME genre | 3 | 20 | 2 | 31% |
| HIGH fame + THIN data + CROSS genre | 3 | 27 | 8 | 29% |
| HIGH fame + THIN data + SAME genre | 3 | 23 | 6 | 30% |
| LOW fame + RICH data + CROSS genre | 3 | 21 | 4 | 25% |
| LOW fame + RICH data + SAME genre | 3 | 22 | 3 | 27% |
| LOW fame + THIN data + CROSS genre | 1 | 6 | 2 | 33% |

**Fame is the strongest predictor.** HIGH fame cells average ~31% hallucination; LOW fame + RICH data cells average ~26%. The model hallucinates more about artists it "knows" from pretraining — its own knowledge overrides the provided data.

**Data richness helps but less than expected.** HIGH+RICH (32%) vs HIGH+THIN (30%) is barely different. Rich data doesn't prevent hallucination; it just gives the model more grounded material alongside the hallucinated material.

**Genre distance is not a strong factor.** CROSS vs SAME shows no consistent pattern.

### 6.3 Four Distinct Failure Modes

Manual review of all 19 pairs and their verification results revealed that hallucination is not one problem but four:

**1. Subject artist hallucination (fame-driven).** The model draws on pretraining knowledge about well-known artists and states things not in the provided data. "Dylan's lyrical innovation and unconventional song structure" (no lyrical data provided), "Elvis Costello's literary approach" (inferred from an "Art Song" style tag), Omar S. described as having a "harder electronic stance" (zero styles data for Omar S.).

**2. Neighbor characterization (all tiers).** The most consistent hallucination across every risk tier. The model describes shared neighbors with adjectives not present in the data: "folk innovators like Astor Piazzolla," "introspective indie voices like Jamila Woods," "art-rock innovators like U.S. Maple." The data provides only neighbor names; everything about what kind of artists they are comes from pretraining knowledge. This occurs equally in HIGH and LOW fame pairs.

**3. DJ intent attribution (all tiers).** The model infers motivations for co-occurrence: "DJs pair them to create sets that prioritize lyrical depth," "DJs value them for their willingness to push beyond conventional rock structures." The data shows co-occurrence patterns, not curator intent. This appeared in nearly every narrative regardless of risk tier.

**4. Garbage-in data (style noise).** Alex G.'s top 3 Discogs styles came back as "Dance-pop, Euro House, Makina" — from minor releases, not representative. The model dutifully described Alex G. as "channeling dance-pop and Euro House into experimental rock territory." The narrative was technically grounded in the provided data but substantively wrong. The automated verifier cannot catch this because it checks against provided data, not reality.

A grounding-fidelity scorer (token-match, claim-ratio) is structurally blind to this failure mode by construction — the constraint ontology proposed in §8 is the mechanism designed to catch it. Measuring that gap requires gold positives where the narrative is faithful to a deliberately-corrupted input, so the labeler can flag it against the *real* metadata they see. Forty such rows ship in `scripts/eval/build_wrong_set.py --mode field_corruption` (`construction_method=field_corruption`, `failure_mode=data_noise`); see WXYC/semantic-index#277.

### 6.4 Targeted Mitigations

Each failure mode suggested a specific mitigation. We tested six prompt variants across the same matrix structure (23 pairs, 138 narratives), sorted here by aggregate effectiveness:

| Variant | Mechanism | Halluc% | vs Baseline |
|---------|-----------|---------|-------------|
| **BASELINE** | Prohibitive ("do not draw on outside knowledge") | **45%** | — |
| **NAMING-ONLY** | Prohibitive ("do not characterize neighbors") | **20%** | −25 pts |
| **ANONYMIZED** | Structural (strip artist names → "Artist A/B") | **20%** | −25 pts |
| **FEW-SHOT** | Demonstrative (two gold-standard example narratives) | **21%** | −24 pts |
| **COMBINED** | Two prohibitions (naming-only + pattern-not-intent) | **31%** | −14 pts |
| **PATTERN-NOT-INTENT** | Prohibitive ("do not infer DJ motivation") | **40%** | −5 pts |

Three mitigations independently cut hallucination by more than half:

**NAMING-ONLY** — adding one sentence ("do not describe, characterize, or categorize the neighbors in any way — state only their names") dropped hallucination from 45% to 20%. It hit 0% on LOW fame + RICH data + SAME genre pairs. This is the highest-impact, lowest-cost intervention.

**ANONYMIZED** — replacing artist names with "Artist A" / "Artist B" and neighbor names with "Neighbor 1" / "Neighbor 2" before generation, then substituting real names back after. The model cannot activate pretraining knowledge about "Aphex Twin" when it only sees "Artist A." Most effective on HIGH fame cells (13–17%).

**FEW-SHOT** — two gold-standard example narratives (one with rich data, one with thin data) appended to the system prompt. Teaches by demonstration rather than prohibition. Hit 7% hallucination on HIGH fame + RICH data + SAME genre — the best single-cell result.

**PATTERN-NOT-INTENT** barely helped (40% vs 45%). The model rephrases DJ intent into slightly different language that still constitutes ungrounded inference. Telling it not to say "DJs value" doesn't stop it from saying "both serve as" or "suggesting they function as."

**COMBINED was worse than its components** (31% vs 20% for naming-only alone). Longer, more complex prompts with two same-mechanism prohibitions dilute the effectiveness of each instruction. The takeaway: combinations have to draw on different mechanisms, not stack the same one.

### 6.5 Combination Experiments

Since the three effective mitigations use different mechanisms (structural, demonstrative, prohibitive), we hypothesized they could be combined without the dilution seen in the COMBINED variant. A follow-up experiment tested three combinations across 20 pairs:

| Variant | Halluc% | vs Baseline | vs Best Single |
|---------|---------|-------------|----------------|
| **BASELINE** | **33%** | — | — |
| **ANON+FEWSHOT** | **20%** | −13 pts | same as singles |
| **FEWSHOT+NAMING** | **14%** | −19 pts | −6 pts better |
| **ANON+FEWSHOT+NAMING** | **9%** | **−24 pts** | **−11 pts better** |

The three-way combination achieved 9% hallucination — zero in four of eight matrix cells. The combinations are additive because each mechanism targets a different failure mode without competing for the model's attention: anonymization blocks subject-artist hallucination at the input level, few-shot examples teach the grounded output pattern by demonstration, naming-only prevents neighbor characterization through prohibition. Three mechanisms, three failure modes — no overlap, no competition.

Per-cell breakdown for ANON+FEWSHOT+NAMING:

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

The remaining hallucination concentrates in SAME-genre pairs where the model has less structural contrast to work with. The hardest cells (LOW+RICH+SAME, LOW+THIN+SAME) are where richer source material — the work of Section 4 — would help most. This is the most concrete bridge between the accuracy work and the source-material work: thinning the hardest cells requires giving the model something specific to say.

### 6.6 Cost of the Three-Way Combination

The ANON+FEWSHOT+NAMING prompt adds approximately 280 tokens to the system prompt (few-shot examples ≈200, anonymization instructions ≈30, naming-only ≈50). At Haiku input pricing ($0.80/MTok), the additional cost is $0.000224 per narrative. Anonymization requires ≈20 lines of pre/post-processing code. Narratives are cached, so each pair pays this cost once.

## 7. Truthiness Scoring

> **Status:** Partially shipped. Token-match v1 runs as the always-on production gate (threshold 0.50), driving the generate-score-regenerate loop with up to two retries. Claim-ratio v1 runs as a periodic offline audit via `scripts/audit_narratives.py`, persisting flags to a sidecar `.narrative-audit-cache.db` exposed at `GET /graph/narrative-audit/recent`. The v2 calibrations (prose allowlist, lenient verifier prompt) were prototyped but not promoted: as Section 7.5 explains, they don't fix the underlying grounding-vs-truthiness gap.

### 7.1 Scoring Method Comparison

Four methods for scoring narrative truthiness were tested on 13 pairs, each scored with both BASELINE and BEST (ANON+FEWSHOT+NAMING) narratives as calibration:

| Method | Mechanism | BASELINE | BEST | Correct ranking | API cost |
|--------|-----------|----------|------|-----------------|----------|
| **Claim-ratio** | Decompose into claims, binary G/U, compute ratio | 0.430 | 0.135 | 100% | 1 Haiku call |
| **Weighted claims** | Categorize claims (ARTIST/NEIGHBOR/CONTEXT/INTENT), weight | 0.240 | 0.091 | 77% | 1 Haiku call |
| **Entailment** | Per-claim "does the data entail this?" | 0.000 | 0.000 | — | 1 Haiku call |
| **Token-match** | Mechanical string matching, no model | 0.626 | 0.359 | 100% | **zero** |

**Claim-ratio** emerged as the most reliable model-based scorer. Wide spread between good and bad narratives, perfect calibration (100% correct ranking of BASELINE worse than BEST).

**Token-match** — the crudest method — correlated perfectly with claim-ratio on directionality despite using zero API calls. It checks whether content words from the narrative appear verbatim in the input data fields.

**Entailment** produced no signal at all (0.000 for both variants). The strict "does the data entail this?" framing was too binary for Haiku to operationalize usefully.

**Weighted claims** added noise. The categorization step (ARTIST/NEIGHBOR/CONTEXT/INTENT) introduced model judgment errors that degraded calibration from 100% to 77%.

### 7.2 The Generate-Score-Regenerate Loop

Having established scoring methods, we tested a closed loop: generate a narrative with ANON+FEWSHOT+NAMING, score it with token-match, and if above threshold (0.50), feed the ungrounded terms back as constraints ("do NOT use these words") and regenerate. The experiment allowed up to 3 retries; production deploys with 2 (`_DEFAULT_MAX_REGEN_RETRIES`), since the third retry never fired in the experiment runs below.

Results across 17 pairs:

| Metric | Value |
|--------|-------|
| Passed on first try | 14 / 17 (82%) |
| Needed retry | 3 / 17 (18%) |
| Converged after retry | 3 / 3 (100%) |
| Did not converge | 0 (0%) |
| Max iterations needed | 2 |

The loop works but is rarely needed. The ANON+FEWSHOT+NAMING prompt already produces narratives below threshold 82% of the time. The 3 pairs that retried all converged on the second attempt. The production cost of the loop is negligible: 82% of narratives incur zero extra cost; the remaining 18% need one additional generation + scoring call.

### 7.3 Control Group Calibration

All prior experiments tested narratives we expected to have problems — pairs with no direct edge, cross-genre pairs, sparse neighborhoods. We ran a control group of 20 "easy" pairs: direct DJ transition edges (3+ co-occurrences), rich metadata on both sides, same genre, moderate fame (400-1200 plays).

The results revealed a fundamental issue:

| | Control (easy pairs) | Experimental (hard pairs) |
|---|---|---|
| Mean token-match v1 | **0.477** | 0.362 |
| Mean claim-ratio v1 | **0.267** | 0.252 |

The control group scored *worse* than the experimental group on both methods. The "easy" pairs produced worse scores than the hard pairs.

The explanation: **token-match penalizes richer narratives.** When the model has more data, it writes more detailed prose with more varied vocabulary, and more of those words don't appear verbatim in the input fields. "Scratchy lo-fi guitar textures" has 3 ungrounded words even though it's a reasonable description of a Lo-Fi + Garage Rock artist. The experimental pairs had thinner data, so narratives leaned more on neighbor names (which are grounded terms), producing lower token scores.

Claim-ratio showed the same issue: the verifier flagged "vocal-driven rock" as ungrounded even though the data says genre=Rock and audio=vocal. "Restrained danceability" was flagged even though danceability=0.22. "Similarly low danceability" was flagged even though both values were under 0.3.

### 7.4 Calibrated Scoring (v2)

We recalibrated both methods:

**Token-match v2** adds a prose allowlist (common descriptive terms like "textures," "soundscapes," "grooves," "framework") and stem-aware matching (so "ambient" in the data grounds "atmospheric," "ambience," "atmosphere" in the narrative).

**Claim-ratio v2** uses a revised verifier prompt that explicitly allows paraphrasing: "'Vocal-driven rock' from genre=Rock + audio=vocal is GROUNDED. 'Restrained danceability' from danceability=0.22 is GROUNDED. 'Soundscapes' from styles containing Ambient or Drone is GROUNDED." The key question becomes: "Could this claim be written by someone who ONLY saw the data fields and knew nothing else about these artists?"

Results on 10 control + 10 experimental pairs:

| Method | Control mean | Experimental mean | Correct direction? |
|--------|-------------|-------------------|-------------------|
| token_v1 | 0.474 | 0.389 | NO |
| **token_v2** | **0.282** | **0.213** | NO |
| claim_v1 | 0.319 | 0.199 | NO |
| **claim_v2** | **0.246** | **0.153** | NO |

The v2 calibration substantially reduced false positives — token-match dropped control mean from 0.474 to 0.282 (40% reduction), and all 20 pairs now score below 0.50 (100%, up from 60%). But the discrimination direction remains reversed: control pairs still score higher than experimental.

### 7.5 The Fundamental Tension

The scoring methods measure **grounding fidelity** — how closely the narrative hews to the literal data fields. But grounding fidelity is not the same as truthiness. A terse narrative that names three neighbors and stops is maximally grounded but not useful. A rich narrative that paraphrases styles, describes texture, and draws reasonable inferences is better prose but scores worse. The control-group inversion in Section 7.3 isn't a calibration bug — it is the methods doing what they were designed to do, and what they were designed to do is the wrong thing.

This means these scoring methods are useful as a *floor detector* (catching the worst slice of narratives — the egregious "The Smiths as funk" hallucinations) but cannot rank narrative quality. They cannot distinguish a great narrative from a workmanlike one. The current production deployment uses them as floor detectors only:

- **Token-match v1** as an always-on fast gate at threshold 0.50, driving regeneration through the closed loop. With ANON+FEWSHOT+NAMING upstream, the gate rarely fires — which is itself a sign that the upstream prompt is doing most of the work.
- **Claim-ratio v1** as a periodic offline audit on cached narratives, surfacing flagged rows via the `/graph/narrative-audit/recent` endpoint for human review.
- **Human spot-check** on the audited sample for everything below threshold; the floor detectors don't certify a narrative as good, only as not-egregiously-bad.

### 7.6 Toward Wrongness Detection

The scoring methods we tested ask "is this grounded in the data?" The better question is "is this *wrong*?" — a factual error detector rather than a grounding detector.

"Vocal-driven rock" for a genre=Rock + audio=vocal artist isn't wrong, it's just inferred. "Electronically-inflected funk" for The Smiths is wrong. The scoring method should distinguish these.

Several approaches target wrongness rather than groundedness:

**Contradiction detection.** Instead of checking whether each claim appears in the data, check whether any claim *contradicts* the data. "Instrumental" when audio=vocal is a contradiction. "Electronic textures" when styles are all acoustic/folk is a contradiction. Claims not addressed by the data (neither supported nor contradicted) get a pass. This is a much tighter filter — only catches things demonstrably wrong.

**Adversarial cross-examination.** Ask a second model: "If you were a knowledgeable music listener reading this, would anything make you say 'that's not right'?" This leans on the model's knowledge as a *validator* rather than a *generator* — the same asymmetry as anonymization (distrusting knowledge for generation while trusting it for validation).

**Semantic alignment.** Embed both the input data and the narrative, compute cosine similarity. A narrative that drifts far from the data semantically (talking about funk when the data says post-punk) would have lower alignment. Runs locally with a sentence embedding model.

**Constraint satisfaction.** Define a set of hard constraints derived from the data that must not be violated, then check mechanically. This is developed further in Section 8.

## 8. Constraint-Based Validation

> **Status:** Proposed, not implemented. The dimensional model and grammar below are a design sketch. The first concrete milestone would be a small hand-built ontology (~10 dimensions) with a mechanical checker, evaluated against a labeled set of known-wrong narratives.

### 8.1 Constraints from Data Fields

Rather than asking a model to judge whether a narrative is grounded, we can derive constraints mechanically from the input data and check whether the narrative violates any of them. Each data field implies boundaries on what the narrative can and cannot say.

The constraints are not a static list. They are generated dynamically from the input data for each specific narrative, because each artist's data places them at specific positions along musical dimensions that determine what descriptions are contradictory.

### 8.2 Dimensional Model

The constraints operate along musical dimensions — axes of description where terms at opposite ends are incompatible:

**Voice presence.** The audio field's `voice_instrumental` value places the artist at one pole. Terms distributed along this dimension:
- HIGH: vocal, singer, singing, lyrical, voice, vocal-driven
- LOW: instrumental, instrument, instrumentals

The constraint: if the data says `audio.voice_instrumental = "vocal"`, the narrative must not use LOW terms, and vice versa.

**Energy.** The danceability value maps to a continuous axis:
- HIGH (>0.7): danceable, propulsive, driving, high-energy, energetic, groovy
- LOW (<0.3): subdued, restrained, contemplative, meditative, still, sparse
- NEUTRAL (0.3-0.7): no constraint — any characterization is defensible

**Acoustic/electronic spectrum.** The combination of styles and genre maps to a position:
- ACOUSTIC: styles containing Acoustic, Folk, Ballad, Country + genre not Electronic
- ELECTRONIC: styles containing Electronic, Electro, House, Techno, Synth-pop + genre Electronic
- MIXED: both present — no constraint

The constraints become incompatibility rules: an artist at the ACOUSTIC pole must not be described with terms like "synth-driven," "programmed," "electronic production." An artist at the ELECTRONIC pole must not be described as "acoustic," "unplugged," "folk."

### 8.3 N-ary Constraints

Not all constraints are binary pairs. Some emerge only from the combination of multiple fields:

- `styles=[Ambient, Drone] + audio.instrumental + danceability=0.15` → forbids "danceable," "groovy," "propulsive," "vocal," "singer." No single field generates this full constraint set — it's the three-way combination that creates the profile.

- `genre=Jazz + styles=[Free Improvisation, Free Jazz] + audio.instrumental` → forbids "pop," "catchy," "hook-driven," "commercial." The genre alone doesn't constrain (jazz includes smooth jazz and bebop); the styles alone don't (free improvisation can be vocal); but the combination does.

- `genre=Rock + styles=[Garage Rock, Lo-Fi, Punk] + audio.vocal` → forbids "orchestral," "chamber," "symphonic," "polished production." Again, the combination constrains more than any single field.

### 8.4 The Grammar

The constraint system can be expressed as a formal grammar:

```
dimension := (name, terms_high, terms_low, data_source, mapping, rule)

mapping := data_value → {high, low, neutral}

rule := position=high FORBIDS terms_low
      | position=low FORBIDS terms_high
      | position=neutral FORBIDS nothing

constraints(artist_data) := for each dimension d:
    position = d.mapping(artist_data[d.data_source])
    forbidden_terms = d.rule(position)
    yield forbidden_terms

violation(narrative, artist_data) := narrative contains any term in constraints(artist_data)
```

The grammar has two layers:

1. **An ontology of music description dimensions.** Each dimension has a name, two sets of pole terms, a data source, a mapping function, and a constraint rule. Adding a dimension extends the grammar.

2. **A mapping from data fields to positions on those dimensions.** The mapping functions translate data values (style lists, audio numbers, genre labels) into positions (high, low, neutral) that activate the constraint rules.

### 8.5 Populating the Ontology

The ontology can be populated from three sources:

**Hand-built core dimensions.** Voice presence, energy, acoustic/electronic, rhythmic density — these are well-understood and can be specified immediately. They provide immediate precision.

**Embedding-derived expansions.** Embed all Discogs style tags and common narrative terms with a sentence embedding model. Terms that are far apart in embedding space on relevant axes are potential contradiction pairs. "Ambient" and "thrash" are distant. "Lo-fi" and "polished" are distant. Dimensional reduction (PCA on the style embeddings) can reveal the natural axes of the descriptor space, suggesting new dimensions to add to the ontology.

**Learned from human judgments.** Given 50-100 narrative + data pairs with human truthiness labels, learn which term/data-field combinations humans flag as wrong. This is a classification problem: given (data fields, narrative term), predict whether a human would say "that's not right."

The hand-built core gives precision now. The embedding-derived expansions give coverage later. The human-judgment layer calibrates the thresholds.

### 8.6 Constraint Checking in Production

Constraint checking is mechanical — no model needed, instant, no probabilistic false positives. False positives are only possible from misspecified constraints (e.g., a constraint that fires on "instrumental" when the data field actually says "mostly instrumental"); the checker itself is deterministic. The check scans the narrative for any forbidden term given the artist's data profile.

The coverage is narrow: it only catches violations of constraints present in the ontology. But it catches them with certainty. This complements the probabilistic scoring methods (token-match, claim-ratio): constraints catch the mechanical violations with 100% precision, scoring methods provide broader but noisier coverage.

The target production stack, once the constraint ontology is built:

1. **Constraint check** (instant, free, deterministic) — reject any narrative that violates a hard constraint, then regenerate.
2. **Token-match** (instant, free, some false positives) — already live as the always-on gate at threshold 0.50.
3. **Claim-ratio audit** (one Haiku call, periodic) — already live as the offline audit, surfaced via `/graph/narrative-audit/recent`.

The constraint check would slot in *before* token-match in the generate-score-regenerate loop, since it is cheaper and more precise. Failures from either gate trigger the same regeneration path with the offending terms appended as "do NOT use these words."

## 9. Cost Analysis

All figures use Haiku 4.5 pricing ($0.80/MTok input, $4/MTok output) as of April 2026.

### 9.1 Narrative Generation

| Strategy | Pairs | Generation | + Verification | Total |
|----------|-------|-----------|----------------|-------|
| All DJ transition edges | 71,619 | $74 | +$97 | $172 |
| Top-5 neighbors per artist | 34,402 | $36 | +$47 | $83 |
| On-demand (current) | dozens | $0.02 | +$0.02 | $0.04 |

The practical sweet spot for cache pre-population is top-5 neighbors per artist at $36 without verification, $83 with. This warms the cache for the most likely-to-be-viewed narratives; the long tail generates on-demand and stays cached for subsequent views. The current on-demand-only deployment costs cents per month at current traffic — pre-population is an investment in p99 latency, not a cost concern.

### 9.2 Review Descriptor Extraction

At approximately 37,000 existing reviews averaging 700 words, structured extraction via Haiku costs roughly $2 in input tokens. The four new crawl sources (Wikipedia, All About Jazz, Resident Advisor, Aquarium Drunkard, Songlines) would add proportionally — call it $5 total for the full corpus.

### 9.3 Ongoing Costs

Narratives are cached in a sidecar SQLite database keyed by `(source_id, target_id, month, dj_id, edge_type, prompt_version)`. Each artist pair pays the generation cost once per prompt-version and is served from cache thereafter; bumping `_PROMPT_VERSION` invalidates by exclusion (old rows stay on disk but are filtered out of reads). The generate-score-regenerate loop adds approximately $2.50 total on top of the $36 base cost at top-5-per-artist scale (82% pass on first try, 18% need one retry, none need a second).

## 10. Sequence of Work

The four fronts decompose into discrete deliverables with natural dependencies. Work to date has prioritized data selection and accuracy mitigations because they are zero-infrastructure prompt changes; source material and embeddings are larger lifts and remain ahead.

### 10.1 Shipped

The production endpoint at `/graph/artists/{id}/explain/{target_id}/narrative` now incorporates the following (referenced commits are on `main` as of May 2026):

- **Adamic-Adar reranking of shared neighbors** (`034a5cf`), with per-neighbor scores returned to the prompt.
- **Minimum 0.8 AA-score threshold** for narrating a pair (`9b08458`); below threshold, an "insufficient signal" canned narrative is returned rather than a fabricated one.
- **Top-5 cap on Discogs styles** per artist (`bdb2bda`); ordering is alphabetical pending an upstream `release_count` column.
- **Qualitative audio-profile descriptors** at extremes only (`df6a59e`), suppressing decimal leakage in prose.
- **Empty / placeholder field omission** (`a601d07`), removing the model's temptation to fill blanks.
- **Naming-only constraint** for shared neighbors (`cba910f`).
- **Anonymization above an 800-play threshold** (`9b0ee21`), applied before generation and reversed after.
- **Two few-shot examples** (rich-data and thin-data) appended to the system prompt (`6fae095`).
- **Token-match scorer as the always-on production gate** at threshold 0.50 (`334ccbc`).
- **Generate-score-regenerate loop** with two retries (`1dab529`); the closed loop converges 100% of the 18% that retry.
- **Periodic claim-ratio audit** with a sidecar audit DB and the `/graph/narrative-audit/recent` endpoint (`e63fadd`).
- **UI-side heat slider** in `routes.py` that modulates the DJ-vs-enrichment weight balance during neighbor selection (`ed9eac6`).
- **Narrative eval-set scaffolding**: stratified pair sampler across the 2×2×2 risk matrix, three deliberately-wrong narrative constructors (data-shuffle for `subject_hallucination`, field-corruption for `data_noise` per #277, pretraining-bait for `subject_hallucination` per #278), bulk generator over the production endpoint, exporter that produces a labeling CSV/JSONL, label merger, and backscorer (`scripts/eval/backscore.py metrics`) that reports per-failure-mode and per-construction-method recall against gold labels (`scripts/eval/`). The 30 data-shuffle and 40 field-corruption rows are in the labeling pool (`output/eval/labeling.jsonl`); the 10 pretraining-bait rows from `bait_pairs.json` still need to be generated through the production endpoint and merged in before the labeling round covers all three failure modes.
- **Standalone labeling web UI** (`semantic_index/labeling_app/`) — a single-page FastAPI app that reads the eval-set JSONL, persists per-labeler labels to a SQLite sidecar, and exports a merge-ready CSV. Deployed for human labeling.

The accuracy delta from baseline to current production is the headline 45% → 9% hallucination reduction, achieved with no new infrastructure and roughly 280 prompt-token overhead per call.

### 10.2 In Progress

- **Release-count column for `artist_style`.** The 5-style cap is currently alphabetical; switching to release-count ranking is a pipeline-side schema change.
- **Audit-driven prompt evolution.** The audit endpoint surfaces flagged narratives but the loop back to "add a few-shot example or a constraint rule" is still manual.
- **Narrative labeling round.** The eval-set scaffolding and labeling UI are deployed; what remains is recruiting labelers, running the calibration round (~20 rows), then the bulk pass, and merging the resulting labels into the gold set so backscore can run.

### 10.3 Next

Work remaining, in dependency order:

1. **Review corpus integration.** Fix name matching (`coverage_with_normalization.py` outline), build the structured extraction pipeline, wire the fields into the prompt schema. The corpus already exists locally at `data/reviews/<source>/reviews.jsonl` (37,422 articles, mirrored to the org-level `research-data/reviews/` repo); the extraction itself is one Haiku pass over the corpus.
2. **New crawl sources.** Wikipedia (broadest coverage), then All About Jazz, Resident Advisor, Aquarium Drunkard, Songlines. The crawler infrastructure (`scripts/crawl_reviews.py`) is built and resumable; each new source needs a 30–50-line discovery function.
3. **Bandcamp bio extraction.** Separate from reviews, since the schema and source URL discovery are different. Fills the independent/underground long tail.
4. **Skip-gram embedding training.** Per Section 5. Most valuable after review descriptors exist, so the newly surfaceable pairs have rich data to narrate with.
5. **Constraint ontology.** Per Section 8. Hand-built core first, embedding-derived expansion second, human-judgment calibration third.

Each step improves narratives independently, and each makes subsequent steps more effective.

## 11. Future Directions

These are research directions surfaced by the work to date but not pursued. Each could be its own investigation.

### 11.1 Multi-Modal Grounding from Audio Embeddings

The pipeline ingests Essentia VGGish embeddings during archive processing (`scripts/process_archive.py`) but currently consumes only the derived qualitative descriptors (genre label, mood, danceability bucket). The raw 128-dim VGGish vectors per recording are dropped. Surfacing them as an artist-level mean and computing nearest-neighbor retrieval would provide a sound-similarity axis orthogonal to both DJ-transition adjacency and Discogs styles. The narrative could ground "both have a dense, lo-fi production aesthetic" in a measured embedding distance rather than in shared style tags. This is the single largest accuracy lever still on the table, because it speaks to the gap that motivated Section 4.

### 11.2 RAG-Style Review Injection

Section 4 plans structured-field extraction from reviews, but a complementary approach is retrieval-augmented generation: index review sentences in a vector store and inject the most-relevant 3–5 sentences directly into the narrative prompt as evidence. Less brittle than extraction (no schema to maintain, no extraction failures), more grounded than free text (the model has actual quoted text to paraphrase). The two approaches could coexist: structured fields for predictable categorization, retrieval for surfacing surprising sentences.

### 11.3 Wrongness Detection vs. Grounding Detection

Section 7 ends with the recognition that the scoring methods measure grounding fidelity, not truthiness, and Section 7.6 sketches four approaches to wrongness detection (contradiction, adversarial cross-examination, semantic alignment, constraint satisfaction). The first prerequisite — an evaluation set of narratives with human-applied "wrong / not wrong" labels — has been scaffolded (`scripts/eval/`, `semantic_index/labeling_app/`): a stratified sample across the 2×2×2 matrix combined with three deliberately-wrong constructions — data shuffle (real names + mismatched metadata, 30 rows, `subject_hallucination`), field corruption (real pair + one deliberately corrupted field, 40 rows, `data_noise`; #277), and pretraining bait (curated confusable-name / strong-prior pairs driven through the production endpoint, 10 rows split between the anonymized and unanonymized branches at the 800-play threshold, `subject_hallucination`; #278). The data-shuffle and field-corruption rows are in the labeling pool today; the pretraining-bait pairs are curated but their rows have not yet been generated through the production endpoint or merged into the pool. The remaining work is generating the bait rows, then the labeling round itself, then measuring each candidate scoring method's precision and recall against the gold labels via `scripts/eval/backscore.py`, whose `metrics` subcommand already reports per-construction-method recall — the load-bearing comparison for surfacing the data_noise gap that the constraint ontology should close, and for distinguishing whether the anonymization branch successfully suppresses the pretraining bait the unanonymized branch is vulnerable to.

### 11.4 Constraint Ontology Bootstrap

Section 8 proposes the dimensional grammar but does not build it. The minimum viable ontology is roughly ten dimensions (voice presence, energy/danceability, acoustic↔electronic, rhythmic density, genre region, instrumentation density, structural conventionality, vocal style, era, fidelity). Implementing the mechanical checker against this set, then evaluating its precision and recall against the wrongness eval set above, would close the loop. The hand-built ontology gives a firm precision floor; the embedding-derived expansion (PCA over Discogs style embeddings) extends coverage at the cost of precision; the calibration layer manages the trade.

### 11.5 Narrative as Discovery

Currently narratives explain known relationships. They can also become a discovery surface: given an artist, ask the system to generate narratives between that artist and its 5 most semantically distant *connected* artists, surfacing surprising bridges that the user wouldn't have clicked into. This is a one-line UI change on top of existing endpoints, but it reframes the narrative from explanation to recommendation.

### 11.6 Listener-Feedback Evaluation

All evaluation in this paper is offline (auto-verifier or hand inspection). The production deployment surfaces narratives to real listeners whose dwell, click-through, and explore-further behaviors are observable. Logging these signals against narrative ID would let us empirically distinguish narratives that listeners read and pursue from ones they bounce off — a signal qualitatively different from grounding scores. It also creates a path to A/B-test prompt versions on the live audience rather than against a synthetic verifier.

### 11.7 Model Choice as a Lever

The experiments use Haiku because its price makes the cost analysis in Section 9 tractable. Sonnet or Opus likely hallucinate less (anecdotally, in spot checks they did) but cost 5–15× more per call. The unstudied question is the ROI: with ANON+FEWSHOT+NAMING dropping Haiku to 9%, is a stronger model worth the price for the remaining 9%? The right experiment is the same matrix on Sonnet, with the same scoring methods. A negative result (Sonnet not meaningfully better) is itself valuable: it would say the residual hallucination is structural to the task setup, not to the model.

### 11.8 Local Fine-Tuned Model

Inverse of the above: a small open-weights model (Llama 3.1 8B, Qwen 2.5 7B) fine-tuned on a few hundred gold narratives may match Haiku at zero per-call cost and zero data egress. The fine-tuning corpus is the production cache plus the closed-loop regenerated outputs. The interesting unknown is whether a model trained specifically on the (data → narrative) mapping is more reliable than a general instruction-follower, even at smaller scale.

### 11.9 Per-DJ and Per-Listener Variants

A narrative explaining a transition for the DJ who programmed it ("you played them together four times in 2024") is different from one for a listener encountering the pair fresh. The current single-output design collapses these. Per-DJ narratives would draw from `play.dj_id` already in the schema; per-listener variants would require listener identity, which the current explore.wxyc.org does not collect.

### 11.10 Cross-Script Artist Matching

A meaningful slice of unmatched artists in the review corpus are non-Latin-script (Cyrillic, CJK, Arabic, Devanagari). Wikidata has multilingual labels for these via P407/P1813 and aliases via P1448; the pipeline already stores Wikidata QIDs. A cross-script matcher that compares against all language-tagged labels would lift coverage on Africa, Asia, and Latin tiers materially. This is a lookup-table change, not a model change.

### 11.11 The Genre Field Itself

The largest unaddressed data-quality issue in this paper is that WXYC's "genre" is a shelving code (Rock, OCS, Hiphop, Africa, Asia, Latin, Blues, Reggae, Classical, Jazz, Electronic), not a sound description. Every narrative that says "both are rock" inherits that ambiguity. Two corrections are possible: drop the field and rely on Discogs styles + audio profile only, or replace it with a learned cluster label derived from audio embeddings + style embeddings. The first is a one-line prompt change with measurable downside (loss of fall-back categorical for thin pairs). The second is a research project. Deciding between them deserves a focused experiment.

### 11.12 Highlighting Provenance in the UI

The narrative is prose. The graph node card is data. Listeners reading a narrative cannot tell which data fields it drew from. A UI affordance — hover over a phrase to highlight the originating data field, or footnote-style citations — would let listeners verify the model's reasoning at a glance. This complements the wrongness-detection work in Section 11.3 by making provenance visible to humans, who are still the most reliable judges.

## 12. Experimental Artifacts

The experiment scripts in `scripts/experiments/narrative/` are frozen artifacts of this investigation, not maintained pipeline tooling. See `scripts/experiments/narrative/README.md` for the canonical map.

### Scripts (under `scripts/experiments/narrative/`)

| Script | Purpose |
|--------|---------|
| `test_narrative_augmentation.py` | Three hand-crafted test cases with augmented sequential context |
| `generate_narrative_samples.py` | 20 narrative samples across three scenarios (no edge, cross-genre, sparse) |
| `compare_neighbor_weighting.py` | Four neighbor weighting methods across eight pairs |
| `experiment_narrative_variants.py` | A/B/C test of three prompt variants across 10 AA-filtered pairs |
| `coverage_with_normalization.py` | Review-to-artist matching with name normalization |
| `hallucination_risk_experiment.py` | Risk-tier experiment (HIGH/MEDIUM/LOW aggregate scoring) |
| `hallucination_matrix_experiment.py` | Full 2×2×2 matrix with generation and verification |
| `mitigation_experiment.py` | Six prompt variants tested across the matrix |
| `combo_mitigation_experiment.py` | Combination variants (anon+fewshot, fewshot+naming, all three) |
| `scoring_methods_experiment.py` | Four scoring methods compared with BASELINE vs BEST calibration |
| `generate_score_regenerate.py` | Closed-loop generate → score → regenerate convergence test |
| `control_group_experiment.py` | Control group (easy pairs with direct edges) |
| `calibrated_scoring_experiment.py` | v1 vs v2 scoring on control + experimental pairs |

### Output files (under `output/`)

| File | Contents |
|------|----------|
| `narrative_samples.txt` | 20 generated narratives across three categories |
| `narrative_experiments.txt` | 10 pairs × 3 prompt variants = 30 narratives |
| `hallucination_matrix.txt` | 19 pairs across 7 matrix cells with verification |
| `mitigation_experiment.txt` | 23 pairs × 6 variants = 138 narratives with verification |
| `combo_mitigation_experiment.txt` | 20 pairs × 4 variants = 80 narratives with verification |
| `scoring_methods_experiment.txt` | 13 pairs × 2 variants × 4 scoring methods |
| `generate_score_regenerate.txt` | 17 pairs through generate-score-regenerate loop |
| `control_group_experiment.txt` | 20 control pairs with scoring |
| `calibrated_scoring_experiment.txt` | 20 pairs with v1 vs v2 scoring comparison |

### Production code

| Module | Role |
|--------|------|
| `semantic_index/api/narrative.py` | Narrative endpoint, prompt assembly, anonymization, scoring loop, sidecar cache |
| `semantic_index/narrative_audit.py` | Periodic claim-ratio audit and audit-DB schema |
| `semantic_index/api/narrative_audit_routes.py` | `/graph/narrative-audit/recent` read endpoint |
| `scripts/audit_narratives.py` | CLI entry point for the claim-ratio audit |

### Eval-set tooling

| Module | Role |
|--------|------|
| `scripts/eval/sample_pairs.py` | Stratified sample across the 2×2×2 matrix, ensuring per-cell minimums |
| `scripts/eval/generate_narratives.py` | Bulk narrative generation through the production endpoint via TestClient |
| `scripts/eval/build_wrong_set.py` | Deliberately-wrong narratives, two modes: `data_shuffle` (real names + mismatched metadata, 30 rows, `subject_hallucination`) and `field_corruption` (real pair + one corrupted field, 40 rows, `data_noise`; #277) |
| `scripts/eval/build_bait_set.py` | Pretraining-bait rows generated through the production endpoint with curated confusable-name / strong-prior pairs (10 rows split above/below the 800-play anonymization threshold, `subject_hallucination`; #278) |
| `scripts/eval/bait_pairs.json` | Curated pretraining-bait pair list read by `build_bait_set.py`, partitioned by the 800-play threshold |
| `scripts/eval/export_labeling.py` | Combines real + wrong + bait rows into a labeling CSV/JSONL with redaction of construction metadata |
| `scripts/eval/merge_labels.py` | Merges per-labeler CSVs into a single gold-label set |
| `scripts/eval/backscore.py` | Per-method recall on the gold set, broken out by failure mode and construction method |
| `semantic_index/labeling_app/` | Standalone single-page FastAPI labeling UI; SQLite-backed per-labeler persistence |
| `docs/eval-set-rubric.md` | Severity ladder, failure-mode definitions, and worked examples for human labelers |
