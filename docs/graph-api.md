# Graph API

A read-only FastAPI service that queries the SQLite database produced by the pipeline. Serves the D3.js graph explorer at the root URL and the JSON API at `/graph/*`.

```bash
python -m semantic_index.api
```

Or programmatically:

```python
from semantic_index.api.app import create_app
app = create_app("data/wxyc_artist_graph.db")
```

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | D3.js graph explorer (interactive visualization). |
| `GET` | `/health` | Health check — returns `status`, `artist_count`, and `graph_db_age_seconds` (age in seconds of the serving DB file's mtime, or `null` if the file is briefly absent mid atomic-swap), or 503 if the database is unreachable. `graph_db_age_seconds` is the freshness signal the WXYC synthetic-DJ canary reads to catch SIGKILL-class silent nightly-sync failures (WXYC/semantic-index#348). |
| `GET` | `/graph/artists/search?q=autechre&limit=10` | Case-insensitive LIKE search, ordered by total_plays descending. |
| `GET` | `/graph/artists/{id}` | Full artist detail including external IDs (Discogs, MusicBrainz, Wikidata QID) and streaming service IDs (Spotify, Apple Music, Bandcamp) joined from the entity table. Gracefully degrades on old-schema databases. |
| `GET` | `/graph/artists/{id}/neighbors?type=djTransition&limit=20` | Neighbors by edge type. Types: `djTransition`, `sharedPersonnel`, `sharedStyle`, `labelFamily`, `compilation`, `crossReference`, `wikidataInfluence`. Supports optional `month` (1-12) and `dj_id` facet filters for `djTransition` — computes PMI dynamically from play-level data. `min_raw_count` (default 1) filters DJ transition edges by minimum co-occurrence count; applies to `djTransition` and `affinity` edge types. |
| `GET` | `/graph/artists/{id}/explain/{target_id}` | All relationship types between two artists with weights and details. |
| `GET` | `/graph/entities/{id}/artists` | All artists sharing an entity (alias group). Returns entity metadata and a list of artist summaries. |
| `GET` | `/graph/facets` | Available facet values (months with data, DJ list) for filtering. Gracefully returns empty lists on databases without facet tables. |
| `GET` | `/graph/communities?min_size=5&limit=50` | Louvain community metadata (size, label, top genres, top artists). Gracefully returns empty on databases without the `community` table. |
| `GET` | `/graph/artists/{id}/explain/{target_id}/narrative?month=&dj_id=` | LLM-generated natural-language explanation of the relationship between two artists. Uses Claude Haiku. Cached in sidecar SQLite DB. Returns 501 when `ANTHROPIC_API_KEY` is not set. |
| `GET` | `/graph/artists/{id}/preview` | Audio preview URL for an artist. Multi-source fallback: iTunes lookup (by Apple Music ID) -> Spotify top tracks (by Spotify ID, requires credentials) -> Bandcamp (by bandcamp_id, scrapes track stream) -> Deezer search (by name) -> iTunes search (by name). Cached in sidecar `.preview-cache.db`. |
| `GET` | `/graph/narrative-audit/recent?limit=50&flagged_only=false` | Most-recent narrative-audit rows from the audit sidecar (`<db>.narrative-audit-cache.db`). Returns an empty list when no audits have run yet. |

## Narrative claim-ratio audit

Periodic offline check that catches structural-claim hallucinations the always-on token-match gate can miss. Samples N cached narratives, opens a read-only connection to the production DB to reconstruct each pair's source/target metadata (the same shape the live narrative endpoint scored against), runs each narrative through a Haiku verifier prompt that decomposes it into grounded vs ungrounded claims, and records the resulting ratio to `<db_path>.narrative-audit-cache.db`. Narratives with `ungrounded / total > threshold` are flagged for review or regeneration.

```bash
ANTHROPIC_API_KEY=sk-... python scripts/audit_narratives.py \
    --db-path data/wxyc_artist_graph.db \
    [--n 100] [--threshold 0.2]
```

- `--db-path` / `DB_PATH` — production SQLite database (the narrative cache lives at `<db-path>.narrative-cache.db`).
- `--n` — sample size (default `100`).
- `--threshold` / `NARRATIVE_AUDIT_CLAIM_THRESHOLD` — claim-ratio above which a narrative is flagged (default `0.2`, strict `>` boundary).

The audit DB is a separate sidecar from the narrative cache so audit history survives cache-version bumps. Recent rows are exposed via `GET /graph/narrative-audit/recent`. Scheduling (nightly or periodic invocation) is a follow-up; for now the script is run manually or by external cron.
