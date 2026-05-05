# Narrative Eval Set

Tooling for building a labeled corpus of WXYC narrative-endpoint outputs. The eval set is the yardstick that lets every downstream narrative-enrichment epic (review descriptors, embeddings, constraint ontology) claim a real lift in narrative wrongness rather than only nudging proxy scores like token-match or claim-ratio.

See `docs/narrative-enrichment-whitepaper.md` Section 11.3 for the motivation: existing scoring methods measure *grounding fidelity* (how closely a narrative hews to the literal data fields), not *truthiness*. Section 7.3's control-group inversion isn't a calibration bug — it's the methods doing what they were designed to do, which is the wrong thing. A human-labeled set lets us measure what we actually care about.

## Pipeline

```
sample_pairs.py        -> output/eval/eval_pairs.jsonl        (stratified candidates)
generate_narratives.py -> output/eval/eval_narratives.jsonl   (production narratives)
build_wrong_set.py     -> output/eval/eval_wrong.jsonl        (data-shuffle gold positives)
export_labeling.py     -> output/eval/labeling.csv + .jsonl   (labeler-ready sheet)
```

The backscoring pass (`backscore.py`, planned) is not in the MVP — it's gated on the labeling round itself, since until we have human labels there is nothing to backscore against. Field-corruption and pretraining-bait wrong-set constructions are also deferred; they're a smaller increment than data-shuffle and can be added once labeling proves the rubric is consistent.

## Stratification

Pairs are stratified across 16 cells:

- **fame**: HIGH (`total_plays > 800`) | LOW (100..400 inclusive)
- **richness**: RICH (≥3 styles AND audio profile) | THIN (≤2 styles OR no audio)
- **genre**: CROSS (different genre) | SAME (same genre)
- **edge**: DIRECT (`dj_transition.raw_count >= 2`) | INDIRECT (no edge, AA-sum ≥ 0.8)

The whitepaper's Section 6.2 used a 2×2×2 matrix (fame × richness × genre). We added the edge axis because a narrative for a directly-transitioning pair has access to a `relationships.djTransition` field while an indirect pair does not — the prompt shape is meaningfully different and any wrongness analysis should be able to distinguish the two.

The fame band's mid-range (`401..800`) is intentionally excluded so the HIGH/LOW distinction stays sharp. Mid-band artists are common (~half the catalog) but they obscure whether fame is actually the operative variable.

## Per-cell capacity

The four LOW-INDIRECT cells starve at sample time — LOW-fame artists (100-400 plays) typically don't have the rich shared-neighbor structure to clear the AA-sum threshold of 0.8. Several LOW-DIRECT cells also produce `insufficient_signal` canned narratives because their direct edges are not backed by enough shared-neighbor structure to clear the same threshold. Both behaviors mirror what the production endpoint does, so the eval set faithfully represents user-visible reality.

The current corpus has 252 production narratives spread across all 16 cells (HIGH cells average ~16 each, LOW cells more variable, with ~80 `insufficient_signal` canned placeholders concentrated in LOW-DIRECT). The cell counts are uneven because the corpus accumulated across two sampler runs (the second after the self-loop bug fix); rather than re-trim, we kept the full pool because more labelable data is good. Plus 30 deliberately-wrong rows from `build_wrong_set.py`, total 282 labelable rows.

## Running it

```bash
# 1. Sample pairs across the matrix.
python -m scripts.eval.sample_pairs \
    --db-path data/wxyc_artist_graph.db \
    --out output/eval/eval_pairs.jsonl \
    --per-cell 12

# 2. Generate narratives via the production endpoint (cache populates as a side
#    effect — same path users hit). Requires ANTHROPIC_API_KEY.
ANTHROPIC_API_KEY=sk-... python -m scripts.eval.generate_narratives \
    --db-path data/wxyc_artist_graph.db \
    --pairs output/eval/eval_pairs.jsonl \
    --out output/eval/eval_narratives.jsonl

# 3. Build deliberately-wrong narratives (data-shuffle: real names + mismatched
#    metadata). These are gold positives — every row is wrong by construction.
ANTHROPIC_API_KEY=sk-... python -m scripts.eval.build_wrong_set \
    --db-path data/wxyc_artist_graph.db \
    --narratives output/eval/eval_narratives.jsonl \
    --out output/eval/eval_wrong.jsonl \
    --n 30

# 4. Export a labeling sheet (CSV for Sheets + JSONL for downstream analysis).
python -m scripts.eval.export_labeling \
    --db-path data/wxyc_artist_graph.db \
    --narratives output/eval/eval_narratives.jsonl \
    --wrong output/eval/eval_wrong.jsonl \
    --csv-out output/eval/labeling.csv \
    --jsonl-out output/eval/labeling.jsonl
```

Re-runs are safe: `generate_narratives.py` supports `--skip-cached`, and the underlying narrative cache uses prompt-version-keyed entries so old rows from earlier prompts stay on disk but don't interfere.

## Labeling

The CSV is sized for Google Sheets. Columns:

- `row_id` — stable identifier; preserves identity across reshuffles or label-merge passes.
- `cell_id`, `pair`, `narrative` — the row a labeler reads.
- `source_data`, `target_data`, `shared_neighbors` — the input the model saw.
- `raw_count` / `aa_sum` / `insufficient_signal` / `token_match_score` — diagnostic context (don't use to set the label, but useful for spot-checking edge cases).
- `severity`, `failure_mode`, `notes` — empty; the labeler fills these.

Rubric and worked examples: `docs/eval-set-rubric.md`. Labelers should read it once before starting and refer back when in doubt.

For multi-labeler runs:

1. Calibration round — every labeler labels the same first 20 rows. Compare results, refine rubric examples for any rows where labelers disagreed.
2. Bulk pass — partition the remaining rows. Optionally by genre wheelhouse if labelers have specialized expertise.
3. Self-IRR — re-label a 10% subset a week later (single-labeler quality check).

After labels merge back into a single CSV, a follow-up script (`merge_labels.py`, not yet built) will join them onto the JSONL backing file by `row_id`, producing the labeled eval set proper.

## What this enables

Once the eval set has human labels:

1. **Backscore prior interventions.** Replay all six prompt variants from Section 6.4 of the whitepaper plus the v1/v2 scoring methods against the labeled set. Report precision/recall/F1 per failure mode. This is the first apples-to-apples table showing which interventions reduce wrongness vs. only reducing proxy scores.
2. **Measure new interventions.** Review-corpus integration, skip-gram embeddings, the constraint ontology — each claims a hallucination reduction in the whitepaper. The eval set converts those claims into measurable lifts.
3. **Detect drift.** Re-run the production prompt against the eval pairs periodically and compare. If a prompt change quietly raises wrongness on a stable input set, we see it before listeners do.

## Files

| File | Purpose |
|------|---------|
| `scripts/eval/sample_pairs.py` | Stratified candidate-pair sampler. |
| `scripts/eval/generate_narratives.py` | Drives the production `/narrative` endpoint via TestClient. |
| `scripts/eval/build_wrong_set.py` | Generates data-shuffle gold positives (real names + mismatched metadata). |
| `scripts/eval/export_labeling.py` | Joins narratives with input data; emits CSV + JSONL. Hides `construction_method` and `expected_label` from the CSV so the labeler doesn't see the answer. |
| `scripts/eval/merge_labels.py` | Joins a filled-in labeler CSV back onto `labeling.jsonl` by `row_id`. Validates against the rubric vocabulary; supports per-labeler outputs for IRR work. |
| `scripts/eval/backscore.py` | Two subcommands: `score` runs token-match v1 and claim-ratio v1 against every eval-set row (real metadata, not the model's prompt input — mirrors what a labeler judges); `metrics` reports precision/recall/F1 of each scorer once labels exist. |
| `tests/unit/test_eval_sample_pairs.py` | Sampler unit tests (cell classification, edge enumeration, self-loop exclusion). |
| `tests/unit/test_eval_merge_labels.py` | Label-merge validation (vocabulary, severity-without-mode error, unknown row_id rejection). |
| `docs/eval-set-rubric.md` | Labeling rubric: severity, failure modes, worked examples. |
| `output/eval/eval_pairs.jsonl` | Sampled candidate pairs. |
| `output/eval/eval_narratives.jsonl` | Generated production narratives + endpoint scoring metadata. |
| `output/eval/eval_wrong.jsonl` | Data-shuffle wrong narratives, each annotated with `construction_method` and `expected_label`. |
| `output/eval/labeling.csv` | Labeler-ready sheet (production + wrong rows interleaved, answer key hidden). |
| `output/eval/labeling.jsonl` | Same data, JSON form, with input data + answer-key fields preserved. |
| `output/eval/eval_scored.jsonl` | Per-row token-match v1 + claim-ratio v1 scores (built by `backscore.py score`). |
