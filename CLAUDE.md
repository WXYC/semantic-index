# Semantic Index

Builds a semantic artist graph from WXYC DJ transition data. DJs curate transitions between artists during their shows — these adjacency relationships encode latent genre/mood/style similarity that PMI (Pointwise Mutual Information) can surface. The pipeline parses tubafrenzy MySQL dumps (or queries Backend-Service PG), resolves artist names, computes PMI + Discogs/Wikidata/AcousticBrainz edges, exports a SQLite database, and serves the Graph API at `explore.wxyc.org`.

## Topic guides

CLAUDE.md is a router for the always-loaded reference card. Topic depth lives in `docs/`:

- **[`docs/architecture.md`](docs/architecture.md)** — Pipeline diagram, per-module responsibilities (~30 modules across resolution, enrichment, edges, API), MySQL column mappings, SQLite schema for `artist`, `dj_transition`, `cross_reference`, `wikidata_influence`, `audio_profile`, `acoustic_similarity`, graph-metrics, entity-store, and facet tables. The committed `data/` snapshot.
- **[`docs/development.md`](docs/development.md)** — Local dev setup, pytest marker layout (default / `pg` / `slow`), `wxyc-etl` shared functions (`to_identity_match_form`, `to_match_form`, `is_compilation_artist`, `split_artist_name`, schema constants), Sentry + JSON-log observability for the pipeline entrypoints and the API service (including the post-#322 scheduler hardening), code style.
- **[`docs/pipeline.md`](docs/pipeline.md)** — `run_pipeline.py` CLI (SQL-dump mode), pipeline-DB mode flags (`--db-path`, `--entity-source`, `--compilation-track-artist-dump`, `--compute-discogs-edges`, `--compute-wikidata-influences`, `--populate-label-hierarchy`, `--discogs-track-json`, `--musicbrainz-cache-dsn`, `--acousticbrainz-dir`), `scripts/nightly_sync.py` (Backend-Service PG mode), PG-schema mappings.
- **[`docs/audio-ingest.md`](docs/audio-ingest.md)** — Two audio-feature paths: `import_acousticbrainz.py` (precomputed features into `ab_recording`) and `process_archive.py` (Essentia TF on the WXYC S3 hourly archive). Model setup, processing estimates.
- **[`docs/graph-api.md`](docs/graph-api.md)** — Read-only FastAPI surface backing `explore.wxyc.org`: search / artists / neighbors / explain / entities / facets / communities / narrative / preview / narrative-audit. Claim-ratio audit CLI.
- **[`docs/deployment.md`](docs/deployment.md)** — EC2 + ECR Docker deploy (`deploy.yml`), all `.env.semantic-index` variables, cross-cache-identity feature flags (`SI_USE_NEW_HOOK_*`, asserted against §4.2 by `scripts/check_cross_cache_identity_flags.sh`), CI pin maintenance (workflow `permissions:`, `@gha/v1` reusable refs), in-process nightly sync scheduler.

Read the relevant topic doc before doing work in that area.

## Always-loaded rules

- **TDD** — Write a failing test first, then implement. Applies to every code change.
- **Code style** — Python 3.12+, `ruff format` (100-char lines), `ruff check` (E/W/F/I/N/UP/B/C4), `mypy` with the pydantic plugin.
- **Test data** — Use WXYC example artists (Autechre, Stereolab, Father John Misty, etc.) in fixtures, not mainstream acts.
- **Branches** — Feature branches off `main`. CI auto-deploys `main` to EC2 production.
- **Cross-cache-identity flag drift** — Any change to `SI_USE_NEW_HOOK_*` flag names or defaults must update both `docs/deployment.md` AND the canonical inventory in `WXYC/Backend-Service/CLAUDE.md` in the same PR. The `cross-cache-identity-flags.yml` workflow grep-asserts the lists match §4.2 of `WXYC/wiki/plans/library-hook-canonicalization-plan.md`.

## Relationship to other repos

- **[Backend-Service](https://github.com/WXYC/Backend-Service)** — Source of nightly-sync data via PG (`wxyc_schema.*`). Owns the canonical cross-cache-identity flag inventory.
- **[library-metadata-lookup](https://github.com/WXYC/library-metadata-lookup)** — Source of pre-resolved identities via `entity.identity` PG table (`--entity-source=lml`); also the fallback Discogs API path for `discogs_client.py`.
- **[wxyc-etl](https://github.com/WXYC/wxyc-etl)** — Shared Rust/PyO3 package providing text normalization (`to_identity_match_form`, `to_match_form`, `is_compilation_artist`, `split_artist_name`), parser (`wxyc_etl.parser`), schema constants (`wxyc_etl.schema.*`), and the JSON-logger / Sentry init (`wxyc_etl.logger`).
- **[wxyc-fastapi](https://github.com/WXYC/wxyc-fastapi)** — Shared FastAPI observability helpers (`init_sentry`) used by the Graph API service.
- **[discogs-cache](https://github.com/WXYC/discogs-etl)** — PG cache backing `discogs_client.py` (and `populate_label_hierarchy.py` via P1902). Schema constants come from `wxyc_etl.schema.*`.
- **[musicbrainz-cache](https://github.com/WXYC/musicbrainz-cache)** — PG cache backing `musicbrainz_client.py` and the AcousticBrainz `ab_recording` table populated by `import_acousticbrainz.py`.
- **[wikidata-cache](https://github.com/WXYC/wikidata-cache)** — PG cache backing `wikidata_client.py` (influence P737, label hierarchy P749/P355, P1902 bridge).
