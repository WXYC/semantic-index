# Narrative-enrichment investigation scripts

Frozen artifacts from the narrative-enrichment investigation. These scripts produced the empirical results cited in [`docs/narrative-enrichment-plan.md`](../../../docs/narrative-enrichment-plan.md) (see the table of investigation artifacts at the end of that document).

They are **not** maintained pipeline tooling. They were one-off runners against a local `data/wxyc_artist_graph.db`, the narrative endpoint, and Claude Haiku. Expect drift: paths, prompt versions, and DB schema may have moved on. Re-run only to reproduce a specific result, and read the script before doing so.

For maintained scripts (the nightly sync, archive processor, AcousticBrainz import, etc.), see the parent `scripts/` directory.

## Map of scripts to plan sections

| Script | Investigated |
|---|---|
| `test_narrative_augmentation.py` | Initial prompt-augmentation feasibility |
| `generate_narrative_samples.py` | Bulk sampler used to drive the audits below |
| `compare_neighbor_weighting.py` | Adamic-Adar vs raw PMI neighbor reranking (Plan §1) |
| `coverage_with_normalization.py` | How much normalization lifts pair coverage (Plan §1) |
| `experiment_narrative_variants.py` | Prompt variant matrix (Plan §1, §4) |
| `hallucination_risk_experiment.py` | Per-axis risk factor screening (Plan §4) |
| `hallucination_matrix_experiment.py` | 2×2×2 risk matrix (fame × richness × genre distance) |
| `mitigation_experiment.py` | Single-mitigation A/B against the matrix |
| `combo_mitigation_experiment.py` | Stacked mitigations — measured 45% → 9% (Plan §4) |
| `scoring_methods_experiment.py` | Token-match vs claim-ratio scorers (Plan §4) |
| `calibrated_scoring_experiment.py` | Calibrating the token-match threshold |
| `control_group_experiment.py` | Sanity control for scorer drift |
| `generate_score_regenerate.py` | Generate → score → regenerate loop prototype |
