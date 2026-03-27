# semantic-index

Builds a semantic artist graph from WXYC DJ transition data. When DJs curate transitions between artists during their shows, those adjacency relationships encode latent genre, mood, and style similarity. This project extracts that signal using Pointwise Mutual Information (PMI) and produces a graph for visualization and downstream use.

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
python run_pipeline.py /path/to/wxycmusic.sql
```

This parses the SQL dump, computes PMI for all artist co-occurrences, extracts cross-reference edges from the catalog, and writes a GEXF graph + SQLite database to `output/`.

## Options

```
python run_pipeline.py <dump_path> [--output-dir DIR] [--min-count N] [--no-sqlite]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--output-dir` | `output/` | Directory for output files |
| `--min-count` | `2` | Minimum co-occurrence count for graph edges |
| `--no-sqlite` | disabled | Skip SQLite database export |

## How it works

1. **Parse** the tubafrenzy MySQL dump directly (no database required)
2. **Resolve** artist names via the library catalog FK chain (LIBRARY_RELEASE → LIBRARY_CODE)
3. **Extract** consecutive artist pairs within each radio show
4. **Compute** PMI: `log2(P(a,b) / (P(a) * P(b)))` — high PMI means two artists appear together more than chance predicts
5. **Extract** cross-reference edges from catalog tables (LIBRARY_CODE_CROSS_REFERENCE, RELEASE_CROSS_REFERENCE)
6. **Export** a GEXF graph loadable in [Gephi](https://gephi.org/) and a SQLite database for querying

## Development

```bash
pytest                    # unit tests
pytest -m integration     # integration tests (needs fixture dump)
ruff check .              # lint
black --check .           # format check
mypy .                    # type check
```

See [CLAUDE.md](CLAUDE.md) for detailed development patterns, column mappings, and SQLite schema.
