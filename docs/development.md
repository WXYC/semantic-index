# Development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest                          # default (no-marker) tests: unit + unmarked integration/e2e (which self-skip without fixtures)
pytest -m pg                    # PG-backed tests (needs DATABASE_URL_DISCOGS / DATABASE_URL_TEST)
pytest -m slow                  # slow tests, e.g. the artist-resolver-rust perf benchmark (manual-only)
```

## Testing

Test *markers* describe what infrastructure a test needs, so CI knows which tests can run in which environment; test *directories* (`tests/unit/`, `tests/integration/`, `tests/e2e/`) describe what kind of test it is. This split is "architecture A" from [the org test-patterns doc](https://github.com/WXYC/wiki/blob/main/plans/test-patterns.md), Section 3.

- **Default (no marker)** — pure logic tests plus the in-memory pipeline tests in `tests/integration/test_pipeline.py`, `tests/integration/test_entity_source_fallback.py`, and `tests/e2e/test_full_pipeline.py`. These self-skip when the tubafrenzy fixture (`tubafrenzy/scripts/dev/fixtures/wxycmusic-fixture.sql`) is not on disk.
- **`pg`** — needs a PostgreSQL server. Currently the discogs-edges SQL tests in `tests/integration/test_discogs_edges_sql.py`, which query the discogs-cache PostgreSQL via `DATABASE_URL_DISCOGS`. Self-skip when the DSN is unreachable.
- **`slow`** — an orthogonal cost dimension: anything taking longer than ~10s. Currently the Rust resolver perf benchmark in `tests/unit/test_artist_resolver_rust.py`. Manual-only, via `# ci-sync-skip: slow` in `pyproject.toml`.

Use WXYC example artists (Autechre, Stereolab, Father John Misty, etc.) in test fixtures — see the org-wide example-data conventions.

## Code Style

- Python 3.12+
- ruff format (100 char line length)
- ruff (100 char, rules: E, W, F, I, N, UP, B, C4)
- mypy with pydantic plugin
- TDD: write a failing test first, then implement

## Shared Dependencies (wxyc-etl)

Artist-name matching has to behave identically across all WXYC data projects — a name normalized one way in this repo and another way in [LML](glossary.md#lml) would fracture identities. So the [normalization](glossary.md#normalization) functions, compilation detection, and schema constants live in [`wxyc-etl`](https://github.com/WXYC/wxyc-etl) (a Rust package with Python bindings) rather than being reimplemented per repo. What the pipeline uses:

- **`wxyc_etl.text.to_identity_match_form(name)`** — the strict, org-wide **cross-cache identity** form: Unicode NFKC normalization + lowercase + strip enclosing parens/brackets + drop leading articles ("the ", "a ") + collapse whitespace. This is the form every WXYC cache uses to agree on which names are "the same artist". (Spec citation: steps 4+5+7 of `library-hook-canonicalization-plan` §3.3.2.) In this repo it is the body of `artist_resolver._normalize()`, with one shim applied first — `&` → `and` — because the spec's step 6 collapses `&` to a space rather than the word "and".
- **`wxyc_etl.text.to_match_form(name)`** — the general-purpose match form: Unicode NFKD + strip diacritics + strip invisible format characters + fold Greek final sigma + lowercase + collapse whitespace. (Spec citation: the WX-2 Normalizer Charter.) Still used by call sites that don't need cross-cache identity semantics.
- **`wxyc_etl.text.is_compilation_artist(name)`** — detects [Various-Artists-style credits](glossary.md#compilation-artist-va): "Various Artists", "V/A", "v.a.", "Soundtrack", "Compilation". (Replaced the older, narrower `is_various_artists()` in `utils.py`.)
- **`wxyc_etl.text.split_artist_name(name)`** — splits multi-artist credits on `, `, ` / `, ` + ` without needing context. Used in `artist_resolver._normalized_forms()`.
- **`wxyc_etl.schema.*`** — table-name constants (`RELEASE_TABLE`, `RELEASE_ARTIST_TABLE`, `RELEASE_LABEL_TABLE`, `RELEASE_STYLE_TABLE`, `RELEASE_TRACK_TABLE`, `RELEASE_TRACK_ARTIST_TABLE`) for all discogs-cache SQL queries in `discogs_client.py` and `reconciliation.py`.

The [summary tables](glossary.md#summary-table) (`artist_style_summary`, `artist_personnel_summary`, `artist_label_summary`, `artist_compilation_summary`) are materialized views created by discogs-cache and are not part of the wxyc-etl schema constants.

## Observability (Sentry + JSON logs)

Both pipeline entrypoints (`run_pipeline.py` and `scripts/nightly_sync.py`) initialize the shared `wxyc_etl.logger` at the top of `main()`, which gets you two things: logs come out as one JSON object per line on stderr, and unhandled exceptions land in Sentry. Every log line carries the four standard tags:

- `repo` — `"semantic-index"`
- `tool` — `"semantic-index run_pipeline"` or `"semantic-index nightly_sync"`
- `step` — supplied per-call via `logger.info("...", extra={"step": "resolve"})`
- `run_id` — a UUIDv4 generated at `init_logger` time, shared across all log lines of a single invocation (so one run's lines can be grepped together)

Sentry activates automatically when `SENTRY_DSN` is set in the environment; without it, JSON logging still initializes and Sentry stays inactive. TODO: provision `SENTRY_DSN` in the EC2 `.env.semantic-index` and the GitHub Actions deploy workflow (separate child task — see the Phase A epic).

The Graph API service (`semantic_index/api/app.py`) initializes JSON logging and Sentry in `_create_app_from_settings()`, in that order, and the order matters. First, `wxyc_etl.logger.init_logger(repo="semantic-index", tool="semantic-index api", sentry_dsn="")` installs the JSON-on-stderr handler with the standard tags, so module loggers under `semantic_index.*` (including the sync scheduler) are visible from the first line of process lifetime; the explicit `sentry_dsn=""` skips Sentry init inside `init_logger` so that Sentry is owned by the next call. Then `wxyc_fastapi.observability.init_sentry` reads `SENTRY_DSN`, `SENTRY_ENVIRONMENT`, and `SENTRY_RELEASE` from the environment via `Settings`, with `service.name` set to `"semantic-index"`. The default `HttpxIntegration` is on, so outbound HTTP calls (Anthropic, iTunes, Spotify, Bandcamp, Deezer) are traced; pass `integrations=[FastApiIntegration()]` to opt out if quota becomes a concern.

The in-process nightly sync scheduler (`semantic_index/api/sync_scheduler.py`) is hardened so that a dying scheduler thread can never be invisible: it emits a heartbeat log every 4 hours (`_HEARTBEAT_INTERVAL_SECONDS`) inside its daily sleep, so `docker logs semantic-index` reflects thread liveness, and its `_scheduler_loop` body is wrapped in an outer `try`/`except BaseException` that calls `logger.exception` before re-raising, so a thread-killing exception lands in both `docker logs` and Sentry. (Why: the scheduler thread once died silently and syncs stopped for 16 days before anyone noticed — [#322](https://github.com/WXYC/semantic-index/issues/322).)
