# Audio Feature Ingest

Two pathways for populating the `audio_profile` table: AcousticBrainz (precomputed features for ~13% of WXYC artists) and direct archive classification (Essentia TF on WXYC's S3 hourly archive).

## AcousticBrainz import

One-time ETL to populate the `ab_recording` table in the musicbrainz PostgreSQL database from the AcousticBrainz data dump tar archives. The import is resumable — per-tar checkpointing skips completed tars, and `ON CONFLICT DO NOTHING` handles duplicate MBIDs.

```bash
python scripts/import_acousticbrainz.py \
    --tar-dir "/Volumes/Peak Twins/acousticbrainz/" \
    --dsn postgresql://localhost/musicbrainz \
    --checkpoint output/ab_import_progress.db \
    [--retry-failed]
```

The `ab_recording` table stores all 18 AcousticBrainz classifiers as structured columns plus JSONB for probability distributions and metadata tags. The feature vector uses all 18 classifiers for a 59-dimension representation.

## Archive audio classification

Extends audio feature coverage beyond AcousticBrainz (which covers only ~13% of WXYC artists) by classifying WXYC's hourly audio archives directly. Uses flowsheet timestamps to locate each play within the S3 archive, extracts 30-second segments, and runs Essentia TF classifiers (VGGish + 15 classification heads) to produce per-segment features. Results are aggregated per-artist and written to the `audio_profile` table, enriching narrative generation with genre, mood, and danceability data.

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

- `--backend-dsn` / `DATABASE_URL_BACKEND` — Backend-Service PostgreSQL DSN (required). Queries `wxyc_schema.flowsheet` for entry timestamps.
- `--model-dir` / `ESSENTIA_MODEL_DIR` — Directory containing Essentia TF models: `audioset-vggish-3.pb` (275 MB feature extractor) + 15 classification heads (~50 KB each).
- `--db-path` / `DB_PATH` — Pipeline SQLite database for writing aggregated audio profiles (optional; omit to skip aggregation).
- `--checkpoint` / `ARCHIVE_CHECKPOINT` — Path to checkpoint SQLite database (default: `output/archive_progress.db`).
- `--bucket` — S3 bucket name (default: `wxyc-archive`).
- `--date-range` — Date range to process as `START:END` (YYYY-MM-DD:YYYY-MM-DD, required unless `--aggregate-only`).
- `--max-hours` — Maximum archive hours to process (0 = unlimited).
- `--segment-duration` — Duration of each segment in seconds (default: 30).
- `--aggregate-only` — Skip processing; aggregate existing checkpoint data into the DB.
- `--retry-failed` — Re-attempt previously failed archive hours.
- `--dry-run` — Log what would be processed without downloading audio.

System dependencies: `ffmpeg`. Python: `pip install -e ".[archive]"` (essentia-tensorflow requires Python 3.13, not 3.14).

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
