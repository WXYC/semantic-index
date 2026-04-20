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
python run_pipeline.py <dump_path> [--output-dir DIR] [--min-count N] [--no-sqlite] [--db-path PATH]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--output-dir` | `output/` | Directory for output files |
| `--min-count` | `2` | Minimum co-occurrence count for graph edges |
| `--no-sqlite` | disabled | Skip SQLite database export |
| `--db-path` | none | Path to pipeline SQLite database with persistent identity resolution from LML |

## How it works

1. **Parse** the tubafrenzy MySQL dump directly (no database required)
2. **Resolve** artist names via the library catalog FK chain (LIBRARY_RELEASE → LIBRARY_CODE)
3. **Extract** consecutive artist pairs within each radio show
4. **Compute** PMI: `log2(P(a,b) / (P(a) * P(b)))` — high PMI means two artists appear together more than chance predicts
5. **Extract** cross-reference edges from catalog tables (LIBRARY_CODE_CROSS_REFERENCE, RELEASE_CROSS_REFERENCE)
6. **Export** a GEXF graph loadable in [Gephi](https://gephi.org/) and a SQLite database for querying

## Graph API

A read-only FastAPI service that queries the SQLite database produced by the pipeline.

### Running locally

```bash
pip install -e ".[api]"
DB_PATH=output/wxyc_artist_graph.db python -m semantic_index.api
```

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_PATH` | `output/wxyc_artist_graph.db` | Path to the SQLite graph database |
| `HOST` | `0.0.0.0` | Host to bind the server to |
| `PORT` | `8000` | Server port |

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check — returns 200 with artist count, or 503 if DB unavailable |
| GET | `/graph/artists/search?q=autechre&limit=10` | Case-insensitive artist name search, ordered by total_plays descending |
| GET | `/graph/artists/{id}/neighbors?type=djTransition&limit=20` | Neighbors by edge type: `djTransition`, `sharedPersonnel`, `sharedStyle`, `labelFamily`, `compilation`, `crossReference` |
| GET | `/graph/artists/{id}/explain/{target_id}` | All relationship types between two artists with weights and details |

### Deployment (Railway)

The API is deployed to Railway. Configuration lives in `railway.toml`:

- **Builder**: nixpacks (auto-detects Python from `pyproject.toml`)
- **Start command**: `python -m semantic_index.api`
- **Health check**: `GET /health` with 300s timeout
- **Restart policy**: on failure, max 10 retries

Railway sets the `PORT` environment variable automatically. Set `DB_PATH` to point to the SQLite database file (e.g. via a Railway volume mount or persistent storage).

## Dependencies

The pipeline depends on `wxyc-etl`, a shared Rust/PyO3 package providing text normalization (`normalize_artist_name`, `is_compilation_artist`, `split_artist_name`) and discogs-cache schema constants. All discogs-cache table names in SQL queries come from `wxyc_etl.schema` constants rather than hardcoded strings. See [CLAUDE.md](CLAUDE.md) for the full list of shared functions used.

## Development

```bash
pytest                    # unit tests
pytest -m integration     # integration tests (needs fixture dump)
ruff check .              # lint
ruff format --check .     # format check
mypy .                    # type check
```

See [CLAUDE.md](CLAUDE.md) for detailed development patterns, column mappings, and SQLite schema.
