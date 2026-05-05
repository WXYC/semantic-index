# Baseline Backscore Findings — Pre-Label

This is a snapshot of what the production scoring methods (token-match v1 and claim-ratio v1) say about the eval-set corpus *before* human labels arrive. The 30 deliberately-wrong rows from `eval_wrong.jsonl` (data-shuffle: real artist names paired with mismatched metadata) act as gold positives — every row is wrong by construction.

## Setup

- 252 production narratives generated through the live `/graph/.../narrative` endpoint at prompt version 11 (the current ANON+FEWSHOT+NAMING+regen-loop pipeline).
- 30 data-shuffle wrong narratives: real artist names + mismatched metadata, generated via direct Haiku call against the same system prompt, no endpoint mediation.
- Both pools scored against the *real metadata* of the named artists (which is what the labeler sees), not the model's prompt input. This is intentional: the score should mirror the labeler's judgment.

## Score distributions

|  | Production (n=252) | Wrong (n=30) |
|---|---|---|
| `token_match_v1` mean ± sd | **0.753 ± 0.144** | **0.685 ± 0.088** |
| `token_match_v1` range | 0.448 .. 1.000 | 0.526 .. 0.917 |
| `claim_ratio_v1` mean ± sd | **0.578 ± 0.201** | **0.646 ± 0.177** |
| `claim_ratio_v1` range | 0.111 .. 0.889 | 0.000 .. 0.889 |

Token-match actually scores production rows *higher* than wrong rows on average. The two distributions overlap heavily on both methods.

## Treating wrong-set as positive class

Threshold 0.5 (the production gate value):

| Method | TP | FP | TN | FN | Precision | Recall | F1 |
|---|---|---|---|---|---|---|---|
| `token_match_v1` | 30 | 244 | 8 | 0 | **0.11** | **1.00** | **0.20** |
| `claim_ratio_v1` | 25 | 141 | 111 | 5 | **0.15** | **0.83** | **0.26** |

Threshold 0.7:

| Method | TP | FP | TN | FN | Precision | Recall | F1 |
|---|---|---|---|---|---|---|---|
| `token_match_v1` | 11 | 137 | 115 | 19 | 0.07 | 0.37 | 0.12 |
| `claim_ratio_v1` | 13 | 89 | 163 | 17 | 0.13 | 0.43 | 0.20 |

## What this confirms

Section 7.5 of the whitepaper argued qualitatively that the scoring methods measure *grounding fidelity*, not *truthiness* — that they're useful as a floor detector only. This is the first quantitative confirmation. With 30 wrong rows acting as gold positives:

- **Token-match cannot discriminate.** A 0.07 precision (threshold 0.7) means 93% of rows the gate would catch are not in fact deliberately wrong. The score reflects how much paraphrase a narrative does, not whether the paraphrase is misleading.
- **Claim-ratio is marginally better but still poor.** The Haiku verifier's bias toward calling paraphrase "ungrounded" (Section 7.3 of the paper) drowns out the genuine signal from wrongness.
- **Both methods invert on direction.** Production rows score *worse* than wrong rows on token-match, despite being mostly right by construction.

## What this doesn't tell us yet

The 252 production rows are not all "right" — some fraction will be flagged as wrong by human labelers (Section 6.3's neighbor characterization, DJ intent attribution, etc.). Until labels arrive, we cannot know:

- The true wrongness rate in the production pool.
- Whether token-match's high false-positive count includes the actually-wrong production rows (in which case its recall on real-world wrongness might be better than this synthetic comparison suggests).
- Per-failure-mode discrimination — does either method catch `subject_hallucination` better than `neighbor_characterization`?

Those questions all become measurable once `merge_labels.py` produces a labeled JSONL and `backscore metrics` joins the two.

## Implications for the constraint ontology

Section 8 of the whitepaper proposes a constraint-based wrongness detector. The numbers above set the bar: *any* mechanism that achieves precision > 0.20 at recall > 0.50 against this eval set beats both probabilistic scorers. That's a low bar to clear — the constraint approach should clear it easily for the contradiction-detectable subset of failures (voice/instrumental flips, electronic/acoustic incompatibilities), and it sets up a decisive comparison once the eval set is fully labeled.

## Pilot self-labeling pass (n=20)

Before recruiting external labelers, I self-labeled a stratified 20-row sample (8 wrong + 12 production, mixed) to validate the rubric and surface the first end-to-end metrics. Labels: 10 not_wrong, 8 severe (all `subject_hallucination`), 2 minor (1 `neighbor_characterization`, 1 `dj_intent`). Notably, 4 of 12 production rows (33%) carried some wrongness — one severe (invented personnel + label families that the model leaned on from pretraining despite ANON+FEWSHOT+NAMING).

| Method @ t=0.5 | Precision | Recall | F1 | mean(pos) | mean(neg) |
|---|---|---|---|---|---|
| `token_match_v1` | 0.50 | 1.00 | 0.67 | 0.721 | 0.718 |
| `claim_ratio_v1` | 0.70 | 0.70 | 0.70 | 0.639 | 0.439 |

Token-match catches everything because its mean is essentially identical between positive and negative — F1 looks decent only because the sample is artificially balanced (n=10/10). Claim-ratio v1 shows actual separation between the two distributions on this richer (real wrongness, not just data-shuffle) sample.

Per-failure-mode recall on claim-ratio:
- `subject_hallucination` 0.75 (6/8) — model misses some severe wrongness
- `neighbor_characterization` 1.00 (1/1) — one-row sample
- **`dj_intent` 0.00** (0/1) — the verifier prompt does not flag DJ intent attribution as ungrounded, even though the rubric does

The `dj_intent` miss is the most actionable per-mode finding. The Haiku verifier's `_CLAIM_DECOMPOSE_PROMPT` says "Inferring DJ intent is U" but the model didn't apply it on the one row that exhibited it (`R0269`, "reflect the station's appetite for aggressive, dance-adjacent avant-garde material"). Either the prompt instruction needs strengthening, or this category is best caught by a different mechanism — exactly the kind of gap the constraint-based detector in §8 would close mechanically.

Caveats: n=20 is tiny. Per-mode counts of 1 are illustrative only. The 33% production-wrongness rate I labeled is in the same ballpark as Section 6 of the whitepaper but on a different sample; nothing here either confirms or contradicts the headline 9%. The point of the pilot is to validate the rubric and the pipeline before bulk labeling, not to establish ground-truth metrics.

## Reproduction

```bash
python -m scripts.eval.backscore score \
    --db-path data/wxyc_artist_graph.db \
    --labeling-jsonl output/eval/labeling.jsonl \
    --out output/eval/eval_scored.jsonl

# After labels arrive:
python -m scripts.eval.merge_labels \
    --labeling-jsonl output/eval/labeling.jsonl \
    --labels-csv path/to/filled.csv \
    --labeler <name> \
    --out output/eval/labeling_labeled.<name>.jsonl

python -m scripts.eval.backscore metrics \
    --scored output/eval/eval_scored.jsonl \
    --labeled output/eval/labeling_labeled.<name>.jsonl \
    --threshold 0.5
```
