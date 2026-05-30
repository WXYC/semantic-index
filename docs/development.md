# Development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest                          # default (no-marker) tests: unit + unmarked integration/e2e (which self-skip without fixtures)
pytest -m pg                    # PG-backed tests (needs DATABASE_URL_DISCOGS / DATABASE_URL_TEST)
pytest -m slow                  # slow tests, e.g. the artist-resolver-rust perf benchmark (manual-only)
```

## Shared Dependencies (wxyc-etl)

The pipeline uses `wxyc-etl` (a Rust/PyO3 package) for shared text normalization, compilation detection, and schema constants:

- **`wxyc_etl.text.to_identity_match_form(name)`** -- Cross-cache-identity normalizer per `library-hook-canonicalization-plan` §3.3.2 steps 4+5+7: NFKC + lowercase + enclosing paren/bracket strip + leading-article drop ("the ", "a ") + whitespace collapse. Used as the body of `artist_resolver._normalize()` (with a leading `&` -> `and` shim applied first, since the canonical step 6 collapses `&` to a space rather than the word "and").
- **`wxyc_etl.text.to_match_form(name)`** -- WX-2 Normalizer Charter match form (NFKD + diacritics-strip + Cf-strip + Greek sigma fold + lowercase + whitespace collapse). Still imported by call sites that don't need cross-cache identity.
- **`wxyc_etl.text.is_compilation_artist(name)`** -- Compilation/VA detection covering "Various Artists", "V/A", "v.a.", "Soundtrack", "Compilation". Replaces the old narrow `is_various_artists()` in `utils.py`.
- **`wxyc_etl.text.split_artist_name(name)`** -- Context-free multi-artist splitting on `, `, ` / `, ` + `. Used in `artist_resolver._normalized_forms()`.
- **`wxyc_etl.schema.*`** -- Table name constants (`RELEASE_TABLE`, `RELEASE_ARTIST_TABLE`, `RELEASE_LABEL_TABLE`, `RELEASE_STYLE_TABLE`, `RELEASE_TRACK_TABLE`, `RELEASE_TRACK_ARTIST_TABLE`) for all discogs-cache SQL queries in `discogs_client.py` and `reconciliation.py`.

Summary tables (`artist_style_summary`, `artist_personnel_summary`, `artist_label_summary`, `artist_compilation_summary`) are materialized views created by discogs-cache and are not part of the wxyc-etl schema constants.

## Observability (Sentry + JSON logs)

Both pipeline entrypoints (`run_pipeline.py` and `scripts/nightly_sync.py`) initialize the shared `wxyc_etl.logger` at the top of `main()` so logs come out as one JSON object per line on stderr and unhandled exceptions land in Sentry. Every log line carries the four standard tags:

- `repo` — `"semantic-index"`
- `tool` — `"semantic-index run_pipeline"` or `"semantic-index nightly_sync"`
- `step` — supplied per-call via `logger.info("...", extra={"step": "resolve"})`
- `run_id` — UUIDv4 generated at `init_logger` time, shared across all log lines for a single invocation

Sentry activates automatically when `SENTRY_DSN` is set in the environment; without it, JSON logging still initializes and Sentry stays inactive. TODO: provision `SENTRY_DSN` in the EC2 `.env.semantic-index` and the GitHub Actions deploy workflow (separate child task — see Phase A epic).

The Graph API service (`semantic_index/api/app.py`) initializes JSON logging and Sentry in `_create_app_from_settings()`, in that order. First `wxyc_etl.logger.init_logger(repo="semantic-index", tool="semantic-index api", sentry_dsn="")` installs the JSON-on-stderr handler with the standard tags so module loggers under `semantic_index.*` (including the sync scheduler) are visible from the first line of process lifetime. The explicit `sentry_dsn=""` skips Sentry init inside `init_logger` so Sentry is owned by the next call. Then `wxyc_fastapi.observability.init_sentry` reads `SENTRY_DSN`, `SENTRY_ENVIRONMENT`, and `SENTRY_RELEASE` from the environment via `Settings`. `service.name` is set to `"semantic-index"`. The default `HttpxIntegration` is on, so outbound calls (Anthropic, iTunes, Spotify, Bandcamp, Deezer) are traced. Pass `integrations=[FastApiIntegration()]` to opt out if quota becomes a concern.

The in-process nightly sync scheduler (`semantic_index/api/sync_scheduler.py`) emits a heartbeat log every `_HEARTBEAT_INTERVAL_SECONDS` (4h) inside the daily sleep so `docker logs semantic-index` reflects thread liveness without inspecting `/proc/1/task`. The `_scheduler_loop` body is wrapped in an outer `try`/`except BaseException` that calls `logger.exception` before re-raising, so a thread-killing exception lands in both `docker logs` and (via Sentry's logging integration) Sentry. This is the post-incident hardening from WXYC/semantic-index#322 (16 days of silent sync failure because the daemon thread died without a visible record).

## Code Style

- Python 3.12+
- ruff format (100 char line length)
- ruff (100 char, rules: E, W, F, I, N, UP, B, C4)
- mypy with pydantic plugin
- TDD: write failing test first, then implement

## Testing

Markers follow architecture A from [the wiki test-patterns doc](https://github.com/WXYC/wiki/blob/main/plans/test-patterns.md), Section 3 — they route CI by infrastructure, not by tier. The directory layout (`tests/unit/`, `tests/integration/`, `tests/e2e/`) documents tier; markers describe operational requirements.

- **Default (no marker)** — pure logic tests plus the in-memory pipeline tests in `tests/integration/test_pipeline.py`, `tests/integration/test_entity_source_fallback.py`, and `tests/e2e/test_full_pipeline.py`. These self-skip when the tubafrenzy fixture (`tubafrenzy/scripts/dev/fixtures/wxycmusic-fixture.sql`) is not on disk.
- **`pg`** — needs a PostgreSQL service. Currently the discogs-edges SQL tests in `tests/integration/test_discogs_edges_sql.py`, which query the discogs-cache PG via `DATABASE_URL_DISCOGS`. Self-skip when the DSN is unreachable.
- **`slow`** — orthogonal cost dimension; takes longer than ~10s. Currently the Rust resolver perf benchmark in `tests/unit/test_artist_resolver_rust.py`. Manual-only via `# ci-sync-skip: slow` in `pyproject.toml`.

Use WXYC example artists (Autechre, Stereolab, Father John Misty, etc.) in test fixtures.
