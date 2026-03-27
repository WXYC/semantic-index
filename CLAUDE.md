# Semantic Index

Builds a semantic artist graph from WXYC DJ transition data. DJs curate transitions between artists during their shows — these adjacency relationships encode latent genre/mood/style similarity that PMI (Pointwise Mutual Information) can surface.

## Architecture

Phase 0 is a batch pipeline that parses a tubafrenzy MySQL dump, extracts artist adjacency pairs from flowsheet data, computes PMI, and exports a GEXF graph for Gephi visualization.

```
SQL dump → sql_parser → models → artist_resolver → adjacency → pmi → graph_export → GEXF
```

### Modules

| Module | Responsibility |
|--------|---------------|
| `semantic_index/sql_parser.py` | Parse MySQL INSERT statements from SQL dump files. Streaming interface for large files. |
| `semantic_index/models.py` | Pydantic data models for flowsheet entries, library records, adjacency pairs, PMI edges. |
| `semantic_index/artist_resolver.py` | Tier 1 artist name resolution via catalog FK chain (LIBRARY_RELEASE → LIBRARY_CODE). |
| `semantic_index/adjacency.py` | Extract consecutive artist pairs within radio shows. |
| `semantic_index/pmi.py` | Compute Pointwise Mutual Information for artist co-occurrences. |
| `semantic_index/graph_export.py` | Build NetworkX graph and export GEXF. |
| `run_phase0.py` | CLI entry point wiring the pipeline. |

### Column Mappings (0-indexed from SQL INSERT order)

| Table | Key columns |
|-------|------------|
| FLOWSHEET_ENTRY_PROD | 0=ID, 1=ARTIST_NAME, 3=SONG_TITLE, 4=RELEASE_TITLE, 6=LIBRARY_RELEASE_ID, 8=LABEL_NAME, 12=RADIO_SHOW_ID, 13=SEQUENCE_WITHIN_SHOW, 15=FLOWSHEET_ENTRY_TYPE_CODE_ID |
| LIBRARY_RELEASE | 0=ID, 8=LIBRARY_CODE_ID |
| LIBRARY_CODE | 0=ID, 1=GENRE_ID, 7=PRESENTATION_NAME |
| GENRE | 0=ID, 1=NAME |

## Development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest                          # unit tests only
pytest -m integration           # needs fixture dump
pytest -m slow                  # needs production dump
```

### Code Style

- Python 3.12+
- black (100 char line length)
- ruff (100 char, rules: E, W, F, I, N, UP, B, C4)
- mypy with pydantic plugin
- TDD: write failing test first, then implement

### Testing

- Unit tests in `tests/unit/` — hand-crafted data via factory functions, no SQL files
- Integration tests in `tests/integration/` — run against fixture dump, marked `@pytest.mark.integration`
- Slow tests marked `@pytest.mark.slow` — run against production dump, manual only
- Use WXYC example artists (Autechre, Stereolab, Father John Misty, etc.) in test fixtures

## Usage

```bash
python run_phase0.py /path/to/wxycmusic.sql [--output-dir output/] [--min-count 2]
```

## Data

The pipeline parses tubafrenzy MySQL dump files directly (no database required). Production dumps are not committed to git — pass the path as a CLI argument. The fixture dump at `tubafrenzy/scripts/dev/fixtures/wxycmusic-fixture.sql` has minimal data suitable for structural testing only.
