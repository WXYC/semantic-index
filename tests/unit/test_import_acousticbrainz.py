"""Tests for AcousticBrainz ETL import script."""

import io
import json
import sqlite3
import tarfile
from pathlib import Path

import pytest

# --- Fixtures ---


def _make_ab_json(
    *,
    danceability: float = 0.75,
    genre: str = "electronic",
    genre_prob: float = 0.90,
) -> dict:
    """Build a minimal AcousticBrainz high-level JSON for import testing."""
    return {
        "highlevel": {
            "danceability": {
                "all": {"danceable": danceability, "not_danceable": 1 - danceability},
                "probability": max(danceability, 1 - danceability),
                "value": "danceable" if danceability > 0.5 else "not_danceable",
            },
            "gender": {
                "all": {"female": 0.4, "male": 0.6},
                "probability": 0.6,
                "value": "male",
            },
            "genre_dortmund": {
                "all": {
                    "alternative": 0.01,
                    "blues": 0.01,
                    "electronic": genre_prob if genre == "electronic" else 0.01,
                    "folkcountry": 0.01,
                    "funksoulrnb": 0.01,
                    "jazz": genre_prob if genre == "jazz" else 0.01,
                    "pop": 0.01,
                    "raphiphop": 0.01,
                    "rock": genre_prob if genre == "rock" else 0.01,
                },
                "probability": genre_prob,
                "value": genre,
            },
            "genre_electronic": {
                "all": {
                    "ambient": 0.10,
                    "dnb": 0.05,
                    "house": 0.60,
                    "techno": 0.20,
                    "trance": 0.05,
                },
                "probability": 0.60,
                "value": "house",
            },
            "genre_rosamerica": {
                "all": {
                    "cla": 0.03,
                    "dan": 0.40,
                    "hip": 0.02,
                    "jaz": 0.05,
                    "pop": 0.20,
                    "rhy": 0.10,
                    "roc": 0.10,
                    "spe": 0.10,
                },
                "probability": 0.40,
                "value": "dan",
            },
            "genre_tzanetakis": {
                "all": {
                    "blu": 0.03,
                    "cla": 0.02,
                    "cou": 0.02,
                    "dis": 0.30,
                    "hip": 0.05,
                    "jaz": 0.03,
                    "met": 0.02,
                    "pop": 0.30,
                    "reg": 0.03,
                    "roc": 0.20,
                },
                "probability": 0.30,
                "value": "dis",
            },
            "ismir04_rhythm": {
                "all": {
                    "ChaChaCha": 0.05,
                    "Jive": 0.15,
                    "Quickstep": 0.10,
                    "Rumba-American": 0.05,
                    "Rumba-International": 0.05,
                    "Rumba-Misc": 0.05,
                    "Samba": 0.20,
                    "Tango": 0.15,
                    "VienneseWaltz": 0.10,
                    "Waltz": 0.10,
                },
                "probability": 0.20,
                "value": "Samba",
            },
            "mood_acoustic": {
                "all": {"acoustic": 0.3, "not_acoustic": 0.7},
                "probability": 0.7,
                "value": "not_acoustic",
            },
            "mood_aggressive": {
                "all": {"aggressive": 0.1, "not_aggressive": 0.9},
                "probability": 0.9,
                "value": "not_aggressive",
            },
            "mood_electronic": {
                "all": {"electronic": 0.8, "not_electronic": 0.2},
                "probability": 0.8,
                "value": "electronic",
            },
            "mood_happy": {
                "all": {"happy": 0.6, "not_happy": 0.4},
                "probability": 0.6,
                "value": "happy",
            },
            "mood_party": {
                "all": {"party": 0.7, "not_party": 0.3},
                "probability": 0.7,
                "value": "party",
            },
            "mood_relaxed": {
                "all": {"relaxed": 0.3, "not_relaxed": 0.7},
                "probability": 0.7,
                "value": "not_relaxed",
            },
            "mood_sad": {
                "all": {"sad": 0.15, "not_sad": 0.85},
                "probability": 0.85,
                "value": "not_sad",
            },
            "moods_mirex": {
                "all": {
                    "Cluster1": 0.15,
                    "Cluster2": 0.25,
                    "Cluster3": 0.20,
                    "Cluster4": 0.30,
                    "Cluster5": 0.10,
                },
                "probability": 0.30,
                "value": "Cluster4",
            },
            "timbre": {
                "all": {"bright": 0.85, "dark": 0.15},
                "probability": 0.85,
                "value": "bright",
            },
            "tonal_atonal": {
                "all": {"tonal": 0.65, "atonal": 0.35},
                "probability": 0.65,
                "value": "tonal",
            },
            "voice_instrumental": {
                "all": {"voice": 0.80, "instrumental": 0.20},
                "probability": 0.80,
                "value": "voice",
            },
        },
        "metadata": {
            "audio_properties": {
                "length": 240.5,
                "codec": "mp3",
                "analysis_sample_rate": 44100,
                "bit_rate": 320000,
                "replay_gain": -8.5,
            },
            "tags": {
                "artist": ["Autechre"],
                "title": ["Gantz Graf"],
                "musicbrainz_recordingid": ["0e11c0fd-a1da-4b88-a438-7ef55c5809ec"],
            },
        },
    }


def _make_tar_with_recordings(tar_path: Path, recordings: dict[str, dict]) -> None:
    """Create a tar file with AB JSON files keyed by MBID."""
    with tarfile.open(tar_path, "w") as tf:
        for mbid, data in recordings.items():
            content = json.dumps(data).encode()
            member_name = f"highlevel/{mbid[:2]}/{mbid[2]}/{mbid}-0.json"
            info = tarfile.TarInfo(name=member_name)
            info.size = len(content)
            tf.addfile(info, io.BytesIO(content))


class TestParseRecordingJson:
    """Test JSON parsing for import."""

    def test_parse_extracts_all_classifiers(self) -> None:
        from scripts.import_acousticbrainz import parse_recording_json

        data = _make_ab_json()
        mbid = "0e11c0fd-a1da-4b88-a438-7ef55c5809ec"
        row = parse_recording_json(mbid, data, "test.tar")

        assert row["recording_mbid"] == mbid
        assert row["danceability"] == pytest.approx(0.75)
        assert row["genre_dortmund_value"] == "electronic"
        assert row["genre_dortmund_prob"] == pytest.approx(0.90)
        assert row["genre_electronic_value"] == "house"
        assert row["genre_rosamerica_value"] == "dan"
        assert row["genre_tzanetakis_value"] == "dis"
        assert row["timbre_value"] == "bright"
        assert row["voice_instrumental_value"] == "voice"
        assert row["mood_acoustic"] == pytest.approx(0.3)
        assert row["mood_electronic"] == pytest.approx(0.8)
        assert row["tar_file"] == "test.tar"

    def test_parse_extracts_audio_properties(self) -> None:
        from scripts.import_acousticbrainz import parse_recording_json

        data = _make_ab_json()
        row = parse_recording_json("abc", data, "test.tar")

        assert row["audio_length"] == pytest.approx(240.5)
        assert row["audio_codec"] == "mp3"
        assert row["audio_sample_rate"] == 44100
        assert row["audio_bit_rate"] == 320000
        assert row["replay_gain"] == pytest.approx(-8.5)

    def test_parse_extracts_classifier_distributions(self) -> None:
        from scripts.import_acousticbrainz import parse_recording_json

        data = _make_ab_json()
        row = parse_recording_json("abc", data, "test.tar")

        dists = json.loads(row["classifier_distributions"])
        assert "genre_dortmund" in dists
        assert "genre_electronic" in dists
        assert "genre_rosamerica" in dists
        assert "genre_tzanetakis" in dists
        assert "moods_mirex" in dists
        assert "ismir04_rhythm" in dists
        assert "gender" in dists

    def test_parse_extracts_metadata_tags(self) -> None:
        from scripts.import_acousticbrainz import parse_recording_json

        data = _make_ab_json()
        row = parse_recording_json("abc", data, "test.tar")

        tags = json.loads(row["metadata_tags"])
        assert tags["artist"] == ["Autechre"]
        assert tags["title"] == ["Gantz Graf"]


class TestCheckpointLogic:
    """Test checkpoint skip and tracking logic."""

    def test_completed_tar_is_skipped(self, tmp_path: Path) -> None:
        from scripts.import_acousticbrainz import get_completed_tars

        checkpoint_path = tmp_path / "checkpoint.db"
        conn = sqlite3.connect(str(checkpoint_path))
        conn.execute(
            "CREATE TABLE progress (tar_file TEXT PRIMARY KEY, status TEXT, "
            "rows_imported INTEGER, completed_at TEXT)"
        )
        conn.execute("INSERT INTO progress VALUES ('done.tar', 'complete', 1000, '2025-01-01')")
        conn.commit()
        conn.close()

        completed = get_completed_tars(str(checkpoint_path))
        assert "done.tar" in completed

    def test_mark_tar_complete(self, tmp_path: Path) -> None:
        from scripts.import_acousticbrainz import (
            get_completed_tars,
            init_checkpoint,
            mark_tar_complete,
        )

        checkpoint_path = str(tmp_path / "checkpoint.db")
        init_checkpoint(checkpoint_path)
        mark_tar_complete(checkpoint_path, "test.tar", 500)

        completed = get_completed_tars(checkpoint_path)
        assert "test.tar" in completed

    def test_failed_tar_not_in_completed(self, tmp_path: Path) -> None:
        from scripts.import_acousticbrainz import (
            get_completed_tars,
            init_checkpoint,
            mark_tar_failed,
        )

        checkpoint_path = str(tmp_path / "checkpoint.db")
        init_checkpoint(checkpoint_path)
        mark_tar_failed(checkpoint_path, "bad.tar", "OSError: NAS dropped")

        completed = get_completed_tars(checkpoint_path)
        assert "bad.tar" not in completed


class TestTarProcessing:
    """Test processing recordings from tar files."""

    def test_process_tar_extracts_recordings(self, tmp_path: Path) -> None:
        from scripts.import_acousticbrainz import process_tar

        mbid = "0e11c0fd-a1da-4b88-a438-7ef55c5809ec"
        tar_path = tmp_path / "test.tar"
        _make_tar_with_recordings(tar_path, {mbid: _make_ab_json()})

        rows = process_tar(str(tar_path))
        assert len(rows) == 1
        assert rows[0]["recording_mbid"] == mbid

    def test_process_tar_multiple_recordings(self, tmp_path: Path) -> None:
        from scripts.import_acousticbrainz import process_tar

        mbids = {
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee": _make_ab_json(genre="rock"),
            "11111111-2222-3333-4444-555555555555": _make_ab_json(genre="jazz"),
        }
        tar_path = tmp_path / "multi.tar"
        _make_tar_with_recordings(tar_path, mbids)

        rows = process_tar(str(tar_path))
        assert len(rows) == 2
        extracted_mbids = {r["recording_mbid"] for r in rows}
        assert extracted_mbids == set(mbids.keys())
