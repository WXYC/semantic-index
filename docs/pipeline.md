# Pipeline Usage

```bash
python run_pipeline.py /path/to/wxycmusic.sql [--output-dir output/] [--min-count 2]
```

Output: `output/wxyc_artist_pmi.gexf` (Gephi graph) + `output/wxyc_artist_graph.db` (SQLite database).

Use `--no-sqlite` to skip the SQLite export.

## Pipeline DB mode

Pass `--db-path` to enable the pipeline database: artists are managed with persistent identity resolution rather than created fresh on each run. The pipeline database becomes the SQLite output.

```bash
python run_pipeline.py dump.sql --db-path output/wxyc_artist_graph.db --entity-source lml --discogs-cache-dsn postgresql://...
```

- `--db-path PATH` — Path to pipeline SQLite database. Creates it if needed.
- `--entity-source {local,lml}` — Where to source artist identity from. `local` uses only the local SQLite pipeline DB and skips LML. `lml` reads identities from LML's `entity.identity` PG table and **requires both `--discogs-cache-dsn` (for the PG connection) and `--db-path` (the destination for imported identities)**; the early validator raises `LmlEntitySourceError` if either is missing or LML PG is unreachable, so silent failures cannot hide. To skip LML when PG is down, re-run with `--entity-source=local`. **When both `--db-path` and `--discogs-cache-dsn` are set, `--entity-source` is required** — the pipeline refuses to start without an explicit choice, since pre-PR #184 that flag combo silently triggered LML import and now silently skips it. Operators upgrading past PR #184 must pick `--entity-source=lml` (preserve the old behavior) or `--entity-source=local` (skip LML). Other flag combos default to `local`.
- `--compilation-track-artist-dump PATH` — Path to a SQL dump containing the `COMPILATION_TRACK_ARTIST` table. When provided, VA/compilation entries are resolved to per-track artists (Tier 0) before the FK chain.
- `--compute-discogs-edges` — Compute Discogs-derived edges (shared personnel, styles, labels, compilations). Off by default.
- `--compute-wikidata-influences` — Query Wikidata P737 (influenced by) and create directed influence edges. Requires `--db-path` with reconciled Wikidata QIDs.
- `--populate-label-hierarchy` — Populate label and label_hierarchy tables from Wikidata P749/P355. Requires `--db-path` and enrichment data.
- `--discogs-track-json PATH` — Path to `compilation_track_artists.json` (from LML `match_compilations.py`). Provides a Discogs-derived fallback (Tier 0b) for VA entries not matched by the CTA table. JSON format: `[{comp_id, discogs_release_id, tracks: [{position, title, artists: [str]}]}]` where `comp_id` = WXYC `LIBRARY_RELEASE_ID`.
- `--musicbrainz-cache-dsn` — When set (without `--acousticbrainz-dir`), uses the PostgreSQL `ab_recording` table for audio features. This is the preferred path — a single JOIN query replaces the two-step MusicBrainzClient + tar loader flow. Requires `import_acousticbrainz.py` to have populated `ab_recording`.
- `--acousticbrainz-dir` — **(Deprecated)** Path to AcousticBrainz tar archives. Requires `--musicbrainz-cache-dsn`. When both `--acousticbrainz-dir` and `--musicbrainz-cache-dsn` are set, the PG path is used and the tar dir is ignored.

## Nightly sync mode

The nightly sync queries Backend-Service PostgreSQL directly instead of parsing a SQL dump. It recomputes the core graph (resolution, PMI, stats, facets, graph metrics) while preserving enrichment data (Discogs, Wikidata, AcousticBrainz) from the existing production database via an atomic copy-and-swap.

```bash
python scripts/nightly_sync.py --dsn postgresql://... --db-path data/wxyc_artist_graph.db
```

Or via environment variables (for Railway cron):

```bash
DATABASE_URL_BACKEND=postgresql://... DB_PATH=data/wxyc_artist_graph.db python scripts/nightly_sync.py
```

- `--dsn` / `DATABASE_URL_BACKEND` — PostgreSQL DSN for Backend-Service (required).
- `--db-path` / `DB_PATH` — Production SQLite database path (default: `data/wxyc_artist_graph.db`).
- `--min-count` / `MIN_COUNT` — Minimum co-occurrence count for DJ transition edges (default: 2).
- `--dry-run` — Run the full pipeline but skip the atomic swap (writes to a temp file instead).
- `--verbose` — Enable debug logging.

**PG schema mappings (`wxyc_schema.*` → pipeline types):**
- `artists` → `LibraryCode` (id, genre_id from `genre_artist_crossreference`, artist_name → presentation_name)
- `library` → `LibraryRelease` (id, artist_id → library_code_id)
- `flowsheet` → `FlowsheetEntry` (filtered to `entry_type = 'track'`, `add_time` → epoch, `request_flag` boolean → int)
- `shows` → show-to-DJ mapping (keyed by `shows.id`, `primary_dj_id` as value)
