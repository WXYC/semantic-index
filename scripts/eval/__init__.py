"""Tooling for the narrative eval set.

The eval set is a labeled corpus of narratives — production-shape outputs paired
with the structured input data the model saw — used to measure whether scoring
methods, prompt changes, or downstream enrichment epics actually reduce
narrative wrongness rather than just nudging proxy metrics.

Pipeline:
  sample_pairs   → eval_pairs.jsonl    (stratified candidate pairs)
  generate       → eval_narratives.jsonl (production narratives via TestClient)
  build_wrong    → eval_wrong.jsonl    (deliberately-wrong constructions)
  export         → labeling.csv / .jsonl (combined sheet for human labelers)
"""
