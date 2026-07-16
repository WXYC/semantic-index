# semantic-index

WXYC 89.3 FM is the student-run freeform radio station at UNC Chapel Hill. Since 2003, every song played on air has been logged track-by-track in the station's [flowsheet](docs/glossary.md#flowsheet) — about 2.6 million entries and counting. Every time a DJ follows one artist with another, they're making a small editorial statement: *these two belong together*. No genre taxonomy predicts most of these pairings; they come from a human hearing a connection.

This project mines those millions of [DJ transitions](docs/glossary.md#dj-transition) for the structure they encode. It parses the flowsheet history, resolves the free-text artist names DJs typed into canonical artists, and scores each recurring artist pairing with [PMI](docs/glossary.md#pmi) — a statistic that separates deliberate curatorial patterns from coincidences of heavy rotation. Around that core it layers relationship edges from external sources: [Discogs](docs/glossary.md#discogs) credits and styles, [Wikidata](docs/glossary.md#wikidata) influence claims, and [audio analysis](docs/glossary.md#audio-profile) of the station's own broadcast archive. The result is a [semantic artist graph](docs/glossary.md#semantic-artist-graph) — the data behind the interactive Freeform Map at [explore.wxyc.org](https://explore.wxyc.org).

The system has three parts, all in this repo:

1. **The pipeline** (`run_pipeline.py`, `scripts/nightly_sync.py`) — batch jobs that read flowsheet data (from a [tubafrenzy](docs/glossary.md#tubafrenzy) MySQL dump, or nightly from [Backend-Service](docs/glossary.md#backend-service) PostgreSQL) and build the graph as a SQLite database.
2. **The Graph API** (`semantic_index/api/`) — a read-only FastAPI service that serves that SQLite database as JSON.
3. **The explorer** — a D3.js visualization served by the API at its root URL; it's what visitors to explore.wxyc.org see.

Terms you don't recognize (PMI, LML, MBID, VA, Fargate...) are defined in the [glossary](docs/glossary.md).

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
python run_pipeline.py /path/to/wxycmusic.sql
```

This parses a tubafrenzy MySQL dump (no database server needed), resolves artist names, computes PMI for all artist co-occurrences, extracts curated [cross-reference](docs/glossary.md#cross-reference) edges from the catalog, and writes two files to `output/`: a SQLite graph database and a [GEXF](docs/glossary.md#gexf) graph you can open in Gephi.

## Options

```
python run_pipeline.py <dump_path> [--output-dir DIR] [--min-count N] [--no-sqlite] [--db-path PATH] [--entity-source {local,lml}]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--output-dir` | `output/` | Directory for output files |
| `--min-count` | `2` | Minimum co-occurrence count for graph edges — a pairing must recur at least this many times to become an edge |
| `--no-sqlite` | disabled | Skip the SQLite database export |
| `--db-path` | none | Path to a pipeline SQLite database with persistent identity resolution (see [docs/pipeline.md](docs/pipeline.md)) |
| `--entity-source` | see docs | Where artist identities come from: `local` (this repo's own resolution) or `lml` (import pre-resolved identities from [LML](docs/glossary.md#lml)). The flag combinations have sharp edges — see the decision guide in [docs/pipeline.md](docs/pipeline.md) |

These are the everyday flags; the pipeline has more (compilation resolution, Discogs/Wikidata enrichment, audio features), all documented in [docs/pipeline.md](docs/pipeline.md).

## How it works

1. **Parse** the tubafrenzy MySQL dump directly — a Rust extension makes this take seconds rather than minutes
2. **Resolve** each entry's artist name to a canonical artist, trying the most reliable evidence first (the [FK chain](docs/glossary.md#fk-chain) through the library catalog) and falling back through progressively fuzzier matching — see [artist resolution](docs/glossary.md#artist-resolution)
3. **Extract** consecutive artist pairs within each [radio show](docs/glossary.md#radio-show)
4. **Compute** PMI for every recurring pair: `log2(P(a,b) / (P(a) * P(b)))` — high PMI means two artists appear together more than chance predicts
5. **Extract** the cross-reference edges music directors curated in the catalog
6. **Export** the SQLite graph database, and optionally a GEXF graph loadable in [Gephi](https://gephi.org/)

## Graph API

A read-only FastAPI service that queries the SQLite database produced by the pipeline and serves the D3.js explorer at its root URL. The full endpoint reference lives in [docs/graph-api.md](docs/graph-api.md).

### Running locally

```bash
pip install -e ".[api]"
DB_PATH=output/wxyc_artist_graph.db python -m semantic_index.api
```

`DB_PATH=output/...` points the API at what the pipeline just wrote — the pipeline's default output directory (`output/`) and the API's default database path (`data/`) are two different subsystems' defaults, so wire them together explicitly.

### Environment variables (local defaults)

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_PATH` | `data/wxyc_artist_graph.db` | Path to the SQLite graph database |
| `HOST` | `0.0.0.0` | Host to bind the server to |
| `PORT` | `8000` | Server port |

These are the code defaults for local development. Production runs on EC2 behind nginx at explore.wxyc.org with its own overrides (port 8083, a `/data` volume) — see [docs/deployment.md](docs/deployment.md).

## Dependencies

The pipeline depends on [`wxyc-etl`](https://github.com/WXYC/wxyc-etl), a Rust package with Python bindings shared across WXYC's data projects. It provides the text-normalization functions used in artist matching (`to_match_form`, `to_identity_match_form`, `is_compilation_artist`, `split_artist_name`) and the discogs-cache schema constants — all discogs-cache table names in SQL queries come from `wxyc_etl.schema` constants rather than hardcoded strings. See [docs/development.md](docs/development.md) for what each shared function does.

## Development

```bash
pytest                    # unit tests
pytest -m integration     # integration tests (needs fixture dump)
ruff check .              # lint
ruff format --check .     # format check
mypy .                    # type check
```

See [docs/development.md](docs/development.md) for the test-marker layout and code style.

## Documentation

[CLAUDE.md](CLAUDE.md) is the index of the topic guides in `docs/` (architecture, pipeline usage, development, audio ingest, Graph API, deployment). If you're new, a good reading order: this README → skim [docs/glossary.md](docs/glossary.md) → [docs/architecture.md](docs/architecture.md) → whichever topic guide covers your task.
