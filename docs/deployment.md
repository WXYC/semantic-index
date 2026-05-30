# Deployment

Deployed on EC2 (us-east-1) as a Docker container alongside Backend-Service. The container runs on port 8083, with nginx reverse proxy and Let's Encrypt TLS for `explore.wxyc.org`.

**GitHub Actions** (`.github/workflows/deploy.yml`) auto-deploys on push to main: builds a Docker image, pushes to ECR, SSHs to EC2 to pull and restart the container. Manual deploys via `workflow_dispatch`.

**EC2 container setup:**

```bash
docker run -d \
  --name semantic-index \
  -p 8083:8083 \
  -v /home/ec2-user/semantic-index-data:/data \
  --restart unless-stopped \
  --env-file .env.semantic-index \
  $ECR_URI/semantic-index:$TAG
```

The SQLite database and sidecar caches live in the bind-mounted `/data` directory and persist across deploys.

**Configuration** via environment variables (`.env.semantic-index` on EC2):
- `DB_PATH` — path to SQLite database (default: `/data/wxyc_artist_graph.db`)
- `HOST` — bind address (default: `0.0.0.0`)
- `PORT` — port (default: `8083`)
- `ANTHROPIC_API_KEY` — Anthropic API key for narrative generation (optional; narrative endpoint returns 501 when not set)
- `SPOTIFY_CLIENT_ID` / `SPOTIFY_CLIENT_SECRET` — Spotify API credentials for preview URL lookups (optional; Spotify tier in the preview fallback chain is skipped when not set)
- `SYNC_ENABLED` — enable the in-process nightly sync scheduler (default: `false`)
- `SYNC_HOUR_UTC` — hour (UTC) to run the daily sync (default: `9`, i.e. 5:00 AM ET)
- `DATABASE_URL_BACKEND` — Backend-Service PostgreSQL DSN for nightly sync (required when `SYNC_ENABLED=true`; uses the RDS private endpoint since EC2 and RDS share a VPC)
- `SYNC_MIN_COUNT` — minimum co-occurrence count for DJ transition edges (default: `2`)
- `ENRICHMENT_TOP_K` — per-artist neighbor cap applied to `shared_personnel` and `label_family` on every nightly sync (default: `50`, `0` disables). Without it both tables grow into the 10M+ row range and stall the affinity composite-edge endpoint on cold cache.

**Cross-cache-identity feature flags.** Per-cache toggles for which `wxyc_library` hook table the resolver reads (legacy schema vs. new normalized schema). All default `false`. The **canonical inventory** (with naming-convention rationale and approval gates) lives in `WXYC/Backend-Service/CLAUDE.md` "Cross-cache-identity feature flags (canonical inventory)". When a flag is renamed or its default changes, both the canonical Backend section AND this list must update in the same PR; CI on this repo grep-asserts the names listed here match the §4.2 inventory.

| Flag | Scope | Default | Set true when |
|---|---|---|---|
| `SI_USE_NEW_HOOK_DISCOGS` | per-cache (Docker discogs, port 5433) | `false` | LML cuts over for that cache + 7 days clean |
| `SI_USE_NEW_HOOK_MUSICBRAINZ` | per-cache | `false` | LML cuts over for that cache + 7 days clean |
| `SI_USE_NEW_HOOK_WIKIDATA` | per-cache | `false` | LML cuts over for that cache + 7 days clean |

Production location: EC2 systemd unit env file (`.env.semantic-index`). Updater is Jake via SSH + edit env file + container restart. Plan reference: `WXYC/wiki/plans/library-hook-canonicalization-plan.md` §4.2.

**GitHub Actions secrets** (shared with Backend-Service, same GitHub org):
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_ECR_URI`
- `EC2_HOST`, `EC2_USER`, `EC2_SSH_KEY`

## CI pin maintenance

Two classes of pin in `.github/workflows/*.yml` exist for supply-chain reasons (mirrors WXYC/request-o-matic#124's free-tier hardening; see WXYC/wiki#67 for the org-wide rollout). They will bit-rot and need occasional bumps:

- **Workflow-level `permissions:`** scoped to the minimum each workflow needs:
  - `ci.yml`, `cross-cache-identity-flags.yml`, `deploy.yml`: `contents: read` (no GITHUB_TOKEN writes — `deploy.yml` ECR push uses `AWS_*` static keys, not OIDC, so no `id-token: write` is needed).
  - `charset-corpus-drift.yml`: `contents: read` plus `packages: read` (the reusable workflow pulls `@wxyc/shared` from `npm.pkg.github.com`).
  Failure mode is silent — a job that needs a missing scope (e.g. `pull-requests: write`) fails its API call but the workflow stays green. When adding a step that needs to comment on PRs, push tags, mint releases, etc., explicitly grant the scope at the job level (or widen the workflow-level floor only if every job in the file needs it). If `deploy.yml` is ever migrated to AWS OIDC, add `id-token: write` at the job level — not at the workflow level — so other jobs can't mint OIDC tokens by accident.
- **Reusable-workflow refs pinned to `@gha/v1`**, not `@main` — `WXYC/wxyc-etl/.github/workflows/check-ci-marker-sync.yml@gha/v1` (in `ci.yml`) and `WXYC/wxyc-shared/.github/workflows/check-charset-corpus-drift.yml@gha/v1` (in `charset-corpus-drift.yml`). The publishing repos treat `gha/v1` as a moving major tag — re-pointed forward on non-breaking changes, frozen on breaking changes (which get a fresh `gha/v2`). Don't downgrade either to `@main`; if a `gha/v2` migration arrives, follow the procedure at the top of the publishing repo's CLAUDE.md.

Run `actionlint .github/workflows/*.yml` locally before pushing workflow changes; it validates `permissions:` syntax, action-version pins, and shell-script blocks (via shellcheck), and catches the silent-mistake class of errors above before CI does. The current `deploy.yml` has 8 pre-existing SC2086 info-level shellcheck warnings that have been deferred — they're info, not error, and predate the pin work.

## Nightly sync scheduler (in-process)

The API service includes a built-in sync scheduler that runs `nightly_sync()` as a background daemon thread. Enable it by setting env vars in `.env.semantic-index`:

- `SYNC_ENABLED=true` — enable the scheduler (default: false)
- `SYNC_HOUR_UTC=9` — hour to run daily sync (default: 9 = 5:00 AM ET)
- `DATABASE_URL_BACKEND=postgresql://...` — Backend-Service PG DSN (required when sync enabled)
- `SYNC_MIN_COUNT=2` — minimum co-occurrence count for DJ transition edges
- `ENRICHMENT_TOP_K=50` — per-artist neighbor cap for `shared_personnel` and `label_family` applied as Step 7c of every sync; 0 disables.

The scheduler sleeps until the configured hour, runs the full pipeline (PG → resolve → PMI → export → entity dedup → enrichment-edge prune → facets → graph metrics), atomically swaps the database, then sleeps until the next day. The API continues serving requests during the rebuild. Runtime is ~5 minutes.

The sync can also be run manually via CLI: `python scripts/nightly_sync.py --dsn postgresql://... --verbose`
