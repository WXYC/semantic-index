# Audio Feature Ingest

The graph's audio features — what an artist actually *sounds* like, as opposed to how they're shelved or tagged — live in the [`audio_profile`](glossary.md#audio-profile) table, which feeds acoustic-similarity edges and enriches narrative generation with genre, mood, and danceability data. Two pathways populate it:

1. **[AcousticBrainz](glossary.md#acousticbrainz) import** — bulk-load features that the (now discontinued) AcousticBrainz project already computed. Cheap, but covers only ~13% of WXYC artists: AcousticBrainz skews toward well-known recordings, and WXYC doesn't play mostly well-known recordings.
2. **Archive classification** — run audio classifiers ourselves over the [WXYC audio archive](glossary.md#wxyc-audio-archive), the station's own hourly broadcast recordings. Costs real compute, but covers exactly what the station actually played.

Both pathways produce profiles in the same 59-dimension layout, so downstream code doesn't care which source an artist's profile came from.

## AcousticBrainz import

A one-time ETL that populates the `ab_recording` table in the musicbrainz-cache PostgreSQL from the AcousticBrainz data-dump tar archives. The import is resumable — per-tar [checkpointing](glossary.md#checkpoint) skips completed tars, and `ON CONFLICT DO NOTHING` handles duplicate [MBIDs](glossary.md#musicbrainz) — so an interrupted run picks up where it left off.

```bash
python scripts/import_acousticbrainz.py \
    --tar-dir "/Volumes/Peak Twins/acousticbrainz/" \
    --dsn postgresql://localhost/musicbrainz \
    --checkpoint output/ab_import_progress.db \
    [--retry-failed]
```

The `ab_recording` table stores all 18 AcousticBrainz classifiers as structured columns plus JSONB for probability distributions and metadata tags. The feature vector uses all 18 classifiers for the 59-dimension representation.

## Archive audio classification

The pipeline knows *when* each flowsheet entry was played, and the archive stores *what was on air* at every moment since June 2021 — so each play can be located inside its archive hour. The script extracts 30-second segments at those timestamps, runs them through [Essentia](glossary.md#essentia) TF classifiers ([VGGish](glossary.md#vggish) embeddings + 15 [classification heads](glossary.md#classification-head)) to produce per-segment features, then aggregates the segments per artist and writes the resulting profiles to `audio_profile`.

```bash
python scripts/process_archive.py \
    --backend-dsn postgresql://... \
    --model-dir /path/to/essentia-models \
    --db-path data/wxyc_artist_graph.db \
    --checkpoint output/archive_progress.db \
    --date-range 2021-06-01:2026-01-01 \
    --max-hours 100 \
    [--segment-duration 30] \
    [--retry-failed] \
    [--dry-run]
```

- `--backend-dsn` / `DATABASE_URL_BACKEND` — Backend-Service PostgreSQL [DSN](glossary.md#dsn) (required). Queries `wxyc_schema.flowsheet` for entry timestamps.
- `--model-dir` / `ESSENTIA_MODEL_DIR` — Directory containing the Essentia TF models: `audioset-vggish-3.pb` (the 275 MB feature extractor) + 15 classification heads (~50 KB each). Download commands below.
- `--db-path` / `DB_PATH` — Pipeline SQLite database for writing aggregated audio profiles (optional; omit to skip aggregation).
- `--checkpoint` / `ARCHIVE_CHECKPOINT` — Path to the checkpoint SQLite database (default: `output/archive_progress.db`).
- `--bucket` — S3 bucket name (default: `wxyc-archive`).
- `--date-range` — Date range to process as `START:END` (YYYY-MM-DD:YYYY-MM-DD, required unless `--aggregate-only`).
- `--max-hours` — Maximum archive hours to process (0 = unlimited).
- `--segment-duration` — Duration of each segment in seconds (default: 30).
- `--aggregate-only` — Skip processing; aggregate existing checkpoint data into the DB.
- `--retry-failed` — Re-attempt previously failed archive hours.
- `--dry-run` — Log what would be processed without downloading audio.

System dependencies: `ffmpeg` (for MP3 → PCM decoding). Python: `pip install -e ".[archive]"` — note this optional extra is stricter about interpreter version than the rest of the project (which needs only 3.12+): essentia-tensorflow currently requires Python 3.13 and does not support 3.14.

**Essentia model setup:**

```bash
# Download VGGish feature extractor (275 MB)
curl -o models/audioset-vggish-3.pb https://essentia.upf.edu/models/feature-extractors/vggish/audioset-vggish-3.pb

# Download 15 classification heads (~50 KB each)
for cat in danceability genre_dortmund mood_acoustic mood_aggressive mood_electronic \
  mood_happy mood_party mood_relaxed mood_sad moods_mirex tonal_atonal \
  voice_instrumental gender genre_rosamerica genre_tzanetakis; do
  curl -o "models/${cat}-audioset-vggish-1.pb" \
    "https://essentia.upf.edu/models/classification-heads/${cat}/${cat}-audioset-vggish-1.pb"
done
```

**Processing estimate:** 41,578 hourly MP3s (June 2021–present), 330K–620K segments at ~3s each. 8-core EC2: 1.5–3 days, ~$12–22.
