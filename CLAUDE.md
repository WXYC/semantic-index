# Semantic Index

Builds a semantic artist graph from WXYC DJ transition data. DJs curate transitions between artists during their shows — these adjacency relationships encode latent genre/mood/style similarity that PMI (Pointwise Mutual Information) can surface.

## Architecture

A batch pipeline that parses a tubafrenzy MySQL dump, extracts artist adjacency pairs from flowsheet data, computes PMI, and exports a GEXF graph and SQLite database.

```
SQL dump → sql_parser → models → artist_resolver → adjacency → pmi → graph_export → GEXF
                                                  → cross_reference →──────────────→ SQLite
                                                  → node_attributes →──────────────→
```

### Modules

| Module | Responsibility |
|--------|---------------|
| `semantic_index/sql_parser.py` | Parse MySQL INSERT statements from SQL dump files. Streaming interface for large files. |
| `semantic_index/models.py` | Pydantic data models for flowsheet entries, library records, adjacency pairs, PMI edges. |
| `semantic_index/artist_resolver.py` | Tier 1 artist name resolution via catalog FK chain (LIBRARY_RELEASE → LIBRARY_CODE). |
| `semantic_index/adjacency.py` | Extract consecutive artist pairs within radio shows. |
| `semantic_index/pmi.py` | Compute Pointwise Mutual Information for artist co-occurrences. |
| `semantic_index/node_attributes.py` | Extract and compute per-artist temporal, DJ, and request statistics. |
| `semantic_index/cross_reference.py` | Extract cross-reference edges from catalog cross-reference tables. |
| `semantic_index/graph_export.py` | Build NetworkX graph and export GEXF. |
| `semantic_index/sqlite_export.py` | Build and export SQLite graph database. |
| `run_pipeline.py` | CLI entry point wiring the pipeline. |

### Column Mappings (0-indexed from SQL INSERT order)

| Table | Key columns |
|-------|------------|
| FLOWSHEET_ENTRY_PROD | 0=ID, 1=ARTIST_NAME, 3=SONG_TITLE, 4=RELEASE_TITLE, 6=LIBRARY_RELEASE_ID, 8=LABEL_NAME, 10=START_TIME, 12=RADIO_SHOW_ID, 13=SEQUENCE_WITHIN_SHOW, 15=FLOWSHEET_ENTRY_TYPE_CODE_ID, 18=REQUEST_FLAG |
| FLOWSHEET_RADIO_SHOW_PROD | 0=ID, 2=DJ_NAME, 3=DJ_ID |
| LIBRARY_RELEASE | 0=ID, 8=LIBRARY_CODE_ID |
| LIBRARY_CODE | 0=ID, 1=GENRE_ID, 7=PRESENTATION_NAME |
| LIBRARY_CODE_CROSS_REFERENCE | 1=CROSS_REFERENCING_ARTIST_ID (→ LIBRARY_CODE.ID), 2=CROSS_REFERENCED_LIBRARY_CODE_ID, 3=COMMENT |
| RELEASE_CROSS_REFERENCE | 1=CROSS_REFERENCING_ARTIST_ID (→ LIBRARY_CODE.ID), 2=CROSS_REFERENCED_RELEASE_ID, 3=COMMENT |
| GENRE | 0=ID, 1=NAME |

### SQLite Schema

```sql
CREATE TABLE artist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_name TEXT NOT NULL UNIQUE,
    genre TEXT,
    total_plays INTEGER NOT NULL DEFAULT 0,
    active_first_year INTEGER,
    active_last_year INTEGER,
    dj_count INTEGER NOT NULL DEFAULT 0,
    request_ratio REAL NOT NULL DEFAULT 0.0,
    show_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE dj_transition (
    source_id INTEGER NOT NULL REFERENCES artist(id),
    target_id INTEGER NOT NULL REFERENCES artist(id),
    raw_count INTEGER NOT NULL,
    pmi REAL NOT NULL,
    PRIMARY KEY (source_id, target_id)
);

CREATE TABLE cross_reference (
    artist_a_id INTEGER NOT NULL REFERENCES artist(id),
    artist_b_id INTEGER NOT NULL REFERENCES artist(id),
    comment TEXT,
    source TEXT NOT NULL,
    PRIMARY KEY (artist_a_id, artist_b_id, source)
);
```

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
python run_pipeline.py /path/to/wxycmusic.sql [--output-dir output/] [--min-count 2]
```

Output: `output/wxyc_artist_pmi.gexf` (Gephi graph) + `output/wxyc_artist_graph.db` (SQLite database).

Use `--no-sqlite` to skip the SQLite export.

## Data

The pipeline parses tubafrenzy MySQL dump files directly (no database required). Production dumps are not committed to git — pass the path as a CLI argument. The fixture dump at `tubafrenzy/scripts/dev/fixtures/wxycmusic-fixture.sql` has minimal data suitable for structural testing only.
