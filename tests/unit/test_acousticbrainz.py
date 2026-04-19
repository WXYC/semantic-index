"""Tests for AcousticBrainz feature loader."""

import json
import sqlite3
import tarfile
from pathlib import Path

import pytest

from semantic_index.acousticbrainz import (
    FEATURE_VECTOR_DIM,
    GENRE_ELECTRONIC_LABELS,
    GENRE_ROSAMERICA_LABELS,
    GENRE_TZANETAKIS_LABELS,
    AcousticBrainzLoader,
    ArtistAudioProfile,
    TarAcousticBrainzLoader,
    build_audio_profiles,
    cosine_similarity,
    load_audio_profiles,
    store_audio_profiles,
)

# --- Fixtures ---


def _make_highlevel_json(
    *,
    danceability: float = 0.6,
    genre: str = "electronic",
    genre_prob: float = 0.95,
    electronic_subgenre: str = "ambient",
    electronic_prob: float = 0.7,
    mood_acoustic: float = 0.3,
    mood_aggressive: float = 0.1,
    mood_electronic: float = 0.8,
    mood_happy: float = 0.4,
    mood_party: float = 0.2,
    mood_relaxed: float = 0.6,
    mood_sad: float = 0.3,
    timbre: str = "dark",
    timbre_prob: float = 0.65,
    tonal: float = 0.8,
    voice_instrumental: str = "instrumental",
    voice_prob: float = 0.9,
) -> dict:
    """Build a minimal AcousticBrainz high-level JSON structure."""
    return {
        "highlevel": {
            "danceability": {
                "all": {"danceable": danceability, "not_danceable": 1 - danceability},
                "probability": max(danceability, 1 - danceability),
                "value": "danceable" if danceability > 0.5 else "not_danceable",
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
                    "ambient": electronic_prob if electronic_subgenre == "ambient" else 0.1,
                    "dnb": 0.05,
                    "house": electronic_prob if electronic_subgenre == "house" else 0.1,
                    "techno": 0.1,
                    "trance": 0.05,
                },
                "probability": electronic_prob,
                "value": electronic_subgenre,
            },
            "genre_rosamerica": {
                "all": {
                    "cla": 0.03,
                    "dan": 0.05,
                    "hip": 0.02,
                    "jaz": 0.10,
                    "pop": 0.15,
                    "rhy": 0.05,
                    "roc": 0.50,
                    "spe": 0.10,
                },
                "probability": 0.50,
                "value": "roc",
            },
            "genre_tzanetakis": {
                "all": {
                    "blu": 0.06,
                    "cla": 0.04,
                    "cou": 0.03,
                    "dis": 0.05,
                    "hip": 0.02,
                    "jaz": 0.10,
                    "met": 0.05,
                    "pop": 0.15,
                    "reg": 0.10,
                    "roc": 0.40,
                },
                "probability": 0.40,
                "value": "roc",
            },
            "mood_acoustic": {
                "all": {"acoustic": mood_acoustic, "not_acoustic": 1 - mood_acoustic},
                "probability": max(mood_acoustic, 1 - mood_acoustic),
                "value": "acoustic" if mood_acoustic > 0.5 else "not_acoustic",
            },
            "mood_aggressive": {
                "all": {"aggressive": mood_aggressive, "not_aggressive": 1 - mood_aggressive},
                "probability": max(mood_aggressive, 1 - mood_aggressive),
                "value": "aggressive" if mood_aggressive > 0.5 else "not_aggressive",
            },
            "mood_electronic": {
                "all": {"electronic": mood_electronic, "not_electronic": 1 - mood_electronic},
                "probability": max(mood_electronic, 1 - mood_electronic),
                "value": "electronic" if mood_electronic > 0.5 else "not_electronic",
            },
            "mood_happy": {
                "all": {"happy": mood_happy, "not_happy": 1 - mood_happy},
                "probability": max(mood_happy, 1 - mood_happy),
                "value": "happy" if mood_happy > 0.5 else "not_happy",
            },
            "mood_party": {
                "all": {"party": mood_party, "not_party": 1 - mood_party},
                "probability": max(mood_party, 1 - mood_party),
                "value": "party" if mood_party > 0.5 else "not_party",
            },
            "mood_relaxed": {
                "all": {"relaxed": mood_relaxed, "not_relaxed": 1 - mood_relaxed},
                "probability": max(mood_relaxed, 1 - mood_relaxed),
                "value": "relaxed" if mood_relaxed > 0.5 else "not_relaxed",
            },
            "mood_sad": {
                "all": {"sad": mood_sad, "not_sad": 1 - mood_sad},
                "probability": max(mood_sad, 1 - mood_sad),
                "value": "sad" if mood_sad > 0.5 else "not_sad",
            },
            "timbre": {
                "all": {
                    "bright": 1 - timbre_prob if timbre == "dark" else timbre_prob,
                    "dark": timbre_prob if timbre == "dark" else 1 - timbre_prob,
                },
                "probability": timbre_prob,
                "value": timbre,
            },
            "tonal_atonal": {
                "all": {"tonal": tonal, "atonal": 1 - tonal},
                "probability": max(tonal, 1 - tonal),
                "value": "tonal" if tonal > 0.5 else "atonal",
            },
            "voice_instrumental": {
                "all": {
                    "voice": 1 - voice_prob if voice_instrumental == "instrumental" else voice_prob,
                    "instrumental": (
                        voice_prob if voice_instrumental == "instrumental" else 1 - voice_prob
                    ),
                },
                "probability": voice_prob,
                "value": voice_instrumental,
            },
            "moods_mirex": {
                "all": {
                    "Cluster1": 0.2,
                    "Cluster2": 0.3,
                    "Cluster3": 0.15,
                    "Cluster4": 0.25,
                    "Cluster5": 0.1,
                },
                "probability": 0.3,
                "value": "Cluster2",
            },
            "ismir04_rhythm": {
                "all": {
                    "ChaChaCha": 0.05,
                    "Jive": 0.05,
                    "Quickstep": 0.05,
                    "Rumba-American": 0.05,
                    "Rumba-International": 0.05,
                    "Rumba-Misc": 0.05,
                    "Samba": 0.1,
                    "Tango": 0.4,
                    "VienneseWaltz": 0.1,
                    "Waltz": 0.1,
                },
                "probability": 0.4,
                "value": "Tango",
            },
            "gender": {
                "all": {"female": 0.4, "male": 0.6},
                "probability": 0.6,
                "value": "male",
            },
        }
    }


def _write_ab_file(data_dir: Path, mbid: str, data: dict, submission: int = 0) -> Path:
    """Write a JSON file in AcousticBrainz directory layout."""
    prefix1 = mbid[:2]
    prefix2 = mbid[2]
    dest = data_dir / "highlevel" / prefix1 / prefix2
    dest.mkdir(parents=True, exist_ok=True)
    path = dest / f"{mbid}-{submission}.json"
    path.write_text(json.dumps(data))
    return path


MBID_AUTECHRE_1 = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
MBID_AUTECHRE_2 = "a1b2c3d4-e5f6-7890-abcd-ef1234567891"
MBID_STEREOLAB = "b2c3d4e5-f6a7-8901-bcde-f12345678901"
MBID_MISSING = "ffffffff-ffff-ffff-ffff-ffffffffffff"


@pytest.fixture
def ab_data_dir(tmp_path: Path) -> Path:
    """Create a mock AcousticBrainz data directory with sample files."""
    _write_ab_file(
        tmp_path,
        MBID_AUTECHRE_1,
        _make_highlevel_json(
            danceability=0.3,
            genre="electronic",
            genre_prob=0.99,
            electronic_subgenre="ambient",
            mood_electronic=0.9,
            mood_relaxed=0.7,
            voice_instrumental="instrumental",
            voice_prob=0.95,
        ),
    )
    _write_ab_file(
        tmp_path,
        MBID_AUTECHRE_2,
        _make_highlevel_json(
            danceability=0.5,
            genre="electronic",
            genre_prob=0.97,
            electronic_subgenre="techno",
            mood_electronic=0.85,
            mood_relaxed=0.4,
            voice_instrumental="instrumental",
            voice_prob=0.92,
        ),
    )
    _write_ab_file(
        tmp_path,
        MBID_STEREOLAB,
        _make_highlevel_json(
            danceability=0.65,
            genre="rock",
            genre_prob=0.75,
            mood_electronic=0.5,
            mood_happy=0.7,
            voice_instrumental="voice",
            voice_prob=0.8,
        ),
    )
    return tmp_path


@pytest.fixture
def ab_tar_dir(tmp_path: Path, ab_data_dir: Path) -> Path:
    """Create a tar archive from the mock AcousticBrainz data for tar loader tests."""
    tar_dir = tmp_path / "tar_dir"
    tar_dir.mkdir()
    tar_path = tar_dir / "shard-0.tar"
    with tarfile.open(tar_path, "w") as tf:
        for json_file in ab_data_dir.rglob("*.json"):
            arcname = str(json_file.relative_to(ab_data_dir))
            tf.add(json_file, arcname=arcname)
    return tar_dir


# --- RecordingFeatures parsing ---


class TestRecordingFeaturesParsing:
    """Test parsing AcousticBrainz JSON into RecordingFeatures."""

    def test_parse_from_json(self, ab_data_dir: Path) -> None:
        loader = AcousticBrainzLoader(str(ab_data_dir))
        features = loader.get_features(MBID_AUTECHRE_1)

        assert features is not None
        assert features.recording_mbid == MBID_AUTECHRE_1
        assert features.danceability == pytest.approx(0.3, abs=0.01)
        assert features.genre == "electronic"
        assert features.genre_probability == pytest.approx(0.99, abs=0.01)
        assert features.voice_instrumental == "instrumental"

    def test_mood_vector(self, ab_data_dir: Path) -> None:
        loader = AcousticBrainzLoader(str(ab_data_dir))
        features = loader.get_features(MBID_AUTECHRE_1)

        assert features is not None
        assert len(features.mood_vector) == 7
        assert features.mood_vector[0] == pytest.approx(0.3, abs=0.01)  # acoustic
        assert features.mood_vector[2] == pytest.approx(0.9, abs=0.01)  # electronic

    def test_genre_vector(self, ab_data_dir: Path) -> None:
        loader = AcousticBrainzLoader(str(ab_data_dir))
        features = loader.get_features(MBID_AUTECHRE_1)

        assert features is not None
        assert len(features.genre_vector) == 9
        # Electronic should be the dominant genre
        electronic_idx = features.GENRE_LABELS.index("electronic")
        assert features.genre_vector[electronic_idx] == pytest.approx(0.99, abs=0.01)

    def test_missing_mbid_returns_none(self, ab_data_dir: Path) -> None:
        loader = AcousticBrainzLoader(str(ab_data_dir))
        assert loader.get_features(MBID_MISSING) is None

    def test_feature_vector(self, ab_data_dir: Path) -> None:
        """The full feature vector concatenates genre + mood + scalar features."""
        loader = AcousticBrainzLoader(str(ab_data_dir))
        features = loader.get_features(MBID_AUTECHRE_1)

        assert features is not None
        vec = features.feature_vector()
        # 9 genre + 7 mood + 5 mirex + 10 rhythm + 5 scalar + 5 electronic + 8 rosamerica + 10 tzanetakis
        assert len(vec) == 59

    def test_genre_electronic_vector(self, ab_data_dir: Path) -> None:
        loader = AcousticBrainzLoader(str(ab_data_dir))
        features = loader.get_features(MBID_AUTECHRE_1)

        assert features is not None
        assert len(features.genre_electronic_vector) == len(GENRE_ELECTRONIC_LABELS)
        # Ambient is dominant for this fixture
        ambient_idx = GENRE_ELECTRONIC_LABELS.index("ambient")
        assert features.genre_electronic_vector[ambient_idx] == pytest.approx(0.7, abs=0.01)

    def test_genre_rosamerica_vector(self, ab_data_dir: Path) -> None:
        loader = AcousticBrainzLoader(str(ab_data_dir))
        features = loader.get_features(MBID_AUTECHRE_1)

        assert features is not None
        assert len(features.genre_rosamerica_vector) == len(GENRE_ROSAMERICA_LABELS)
        roc_idx = GENRE_ROSAMERICA_LABELS.index("roc")
        assert features.genre_rosamerica_vector[roc_idx] == pytest.approx(0.50, abs=0.01)

    def test_genre_tzanetakis_vector(self, ab_data_dir: Path) -> None:
        loader = AcousticBrainzLoader(str(ab_data_dir))
        features = loader.get_features(MBID_AUTECHRE_1)

        assert features is not None
        assert len(features.genre_tzanetakis_vector) == len(GENRE_TZANETAKIS_LABELS)
        roc_idx = GENRE_TZANETAKIS_LABELS.index("roc")
        assert features.genre_tzanetakis_vector[roc_idx] == pytest.approx(0.40, abs=0.01)

    def test_feature_vector_layout(self, ab_data_dir: Path) -> None:
        """New genre classifiers occupy positions [36:59] in the feature vector."""
        loader = AcousticBrainzLoader(str(ab_data_dir))
        features = loader.get_features(MBID_AUTECHRE_1)
        assert features is not None

        vec = features.feature_vector()
        # Positions [36:41] = genre_electronic (5 elements)
        assert vec[36:41] == features.genre_electronic_vector
        # Positions [41:49] = genre_rosamerica (8 elements)
        assert vec[41:49] == features.genre_rosamerica_vector
        # Positions [49:59] = genre_tzanetakis (10 elements)
        assert vec[49:59] == features.genre_tzanetakis_vector


# --- Batch lookup ---


class TestBatchGetFeatures:
    """Test batch feature retrieval."""

    def test_batch_get(self, ab_data_dir: Path) -> None:
        loader = AcousticBrainzLoader(str(ab_data_dir))
        results = loader.batch_get_features([MBID_AUTECHRE_1, MBID_STEREOLAB, MBID_MISSING])

        assert len(results) == 2
        assert MBID_AUTECHRE_1 in results
        assert MBID_STEREOLAB in results
        assert MBID_MISSING not in results

    def test_batch_empty_list(self, ab_data_dir: Path) -> None:
        loader = AcousticBrainzLoader(str(ab_data_dir))
        assert loader.batch_get_features([]) == {}


# --- Artist audio profile aggregation ---


class TestArtistAudioProfile:
    """Test aggregating recording features into artist profiles."""

    def test_aggregate_two_recordings(self, ab_data_dir: Path) -> None:
        loader = AcousticBrainzLoader(str(ab_data_dir))
        features = loader.batch_get_features([MBID_AUTECHRE_1, MBID_AUTECHRE_2])

        from semantic_index.acousticbrainz import ArtistAudioProfile

        profile = ArtistAudioProfile.from_recordings(list(features.values()))

        assert profile.recording_count == 2
        # Danceability should be average of 0.3 and 0.5
        assert profile.avg_danceability == pytest.approx(0.4, abs=0.01)
        # Both recordings are instrumental
        assert profile.voice_instrumental_ratio == pytest.approx(0.0, abs=0.01)
        # Feature centroid should be length 59
        assert len(profile.feature_centroid) == 59

    def test_single_recording_profile(self, ab_data_dir: Path) -> None:
        loader = AcousticBrainzLoader(str(ab_data_dir))
        features = loader.get_features(MBID_STEREOLAB)

        from semantic_index.acousticbrainz import ArtistAudioProfile

        profile = ArtistAudioProfile.from_recordings([features])

        assert profile.recording_count == 1
        assert profile.primary_genre == "rock"
        # Stereolab has voice
        assert profile.voice_instrumental_ratio == pytest.approx(1.0, abs=0.01)

    def test_empty_recordings_raises(self) -> None:
        from semantic_index.acousticbrainz import ArtistAudioProfile

        with pytest.raises(ValueError, match="at least one"):
            ArtistAudioProfile.from_recordings([])


# --- Cosine similarity ---


class TestAcousticSimilarity:
    """Test cosine similarity between artist audio profiles."""

    def test_identical_profiles_similarity_1(self, ab_data_dir: Path) -> None:
        loader = AcousticBrainzLoader(str(ab_data_dir))
        f1 = loader.get_features(MBID_AUTECHRE_1)

        p1 = ArtistAudioProfile.from_recordings([f1])
        assert cosine_similarity(p1.feature_centroid, p1.feature_centroid) == pytest.approx(
            1.0, abs=0.001
        )

    def test_different_profiles_lower_similarity(self, ab_data_dir: Path) -> None:
        loader = AcousticBrainzLoader(str(ab_data_dir))
        f_autechre = loader.get_features(MBID_AUTECHRE_1)
        f_stereolab = loader.get_features(MBID_STEREOLAB)

        p_autechre = ArtistAudioProfile.from_recordings([f_autechre])
        p_stereolab = ArtistAudioProfile.from_recordings([f_stereolab])

        sim = cosine_similarity(p_autechre.feature_centroid, p_stereolab.feature_centroid)
        assert 0.0 < sim < 1.0

    def test_similar_recordings_higher_similarity(self, ab_data_dir: Path) -> None:
        """Two Autechre recordings should be more similar to each other than to Stereolab."""
        loader = AcousticBrainzLoader(str(ab_data_dir))
        f1 = loader.get_features(MBID_AUTECHRE_1)
        f2 = loader.get_features(MBID_AUTECHRE_2)
        f_stereolab = loader.get_features(MBID_STEREOLAB)

        p_ae = ArtistAudioProfile.from_recordings([f1, f2])
        p_ae1 = ArtistAudioProfile.from_recordings([f1])
        p_sl = ArtistAudioProfile.from_recordings([f_stereolab])

        sim_same = cosine_similarity(p_ae.feature_centroid, p_ae1.feature_centroid)
        sim_diff = cosine_similarity(p_ae.feature_centroid, p_sl.feature_centroid)
        assert sim_same > sim_diff


# --- SQLite storage ---


@pytest.fixture
def graph_db(tmp_path: Path) -> sqlite3.Connection:
    """Create a minimal graph DB with artist rows for storage tests."""
    conn = sqlite3.connect(str(tmp_path / "test.db"))
    conn.execute(
        "CREATE TABLE artist ("
        "  id INTEGER PRIMARY KEY,"
        "  canonical_name TEXT NOT NULL UNIQUE,"
        "  musicbrainz_artist_id TEXT"
        ")"
    )
    conn.execute("INSERT INTO artist VALUES (1, 'Autechre', '100')")
    conn.execute("INSERT INTO artist VALUES (2, 'Stereolab', '200')")
    conn.execute("INSERT INTO artist VALUES (3, 'Cat Power', '300')")
    conn.commit()
    return conn


class TestStoreAudioProfiles:
    """Test persisting audio profiles to SQLite."""

    def test_store_and_read_back(self, ab_data_dir: Path, graph_db: sqlite3.Connection) -> None:
        loader = AcousticBrainzLoader(str(ab_data_dir))
        f1 = loader.get_features(MBID_AUTECHRE_1)
        profile = ArtistAudioProfile.from_recordings([f1])

        store_audio_profiles(graph_db, {1: profile})

        row = graph_db.execute("SELECT * FROM audio_profile WHERE artist_id = 1").fetchone()
        assert row is not None
        # artist_id, avg_danceability, primary_genre, ..., recording_count
        assert row[0] == 1  # artist_id
        assert row[6] == 1  # recording_count

    def test_store_creates_table(self, ab_data_dir: Path, tmp_path: Path) -> None:
        """store_audio_profiles creates the table if it doesn't exist."""
        conn = sqlite3.connect(str(tmp_path / "empty.db"))
        conn.execute("CREATE TABLE artist (id INTEGER PRIMARY KEY, canonical_name TEXT)")
        conn.commit()

        loader = AcousticBrainzLoader(str(ab_data_dir))
        f1 = loader.get_features(MBID_AUTECHRE_1)
        profile = ArtistAudioProfile.from_recordings([f1])

        store_audio_profiles(conn, {1: profile})

        tables = [
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        ]
        assert "audio_profile" in tables

    def test_store_multiple_profiles(self, ab_data_dir: Path, graph_db: sqlite3.Connection) -> None:
        loader = AcousticBrainzLoader(str(ab_data_dir))
        f_ae = loader.get_features(MBID_AUTECHRE_1)
        f_sl = loader.get_features(MBID_STEREOLAB)

        profiles = {
            1: ArtistAudioProfile.from_recordings([f_ae]),
            2: ArtistAudioProfile.from_recordings([f_sl]),
        }
        store_audio_profiles(graph_db, profiles)

        count = graph_db.execute("SELECT COUNT(*) FROM audio_profile").fetchone()[0]
        assert count == 2


class TestLoadAudioProfiles:
    """Test loading persisted audio profiles from SQLite."""

    def test_load_round_trip(self, ab_data_dir: Path, graph_db: sqlite3.Connection) -> None:
        """Profiles stored via store_audio_profiles can be loaded back identically."""
        loader = AcousticBrainzLoader(str(ab_data_dir))
        f_ae = loader.get_features(MBID_AUTECHRE_1)
        f_sl = loader.get_features(MBID_STEREOLAB)

        original = {
            1: ArtistAudioProfile.from_recordings([f_ae]),
            2: ArtistAudioProfile.from_recordings([f_sl]),
        }
        store_audio_profiles(graph_db, original)

        loaded = load_audio_profiles(graph_db)

        assert set(loaded.keys()) == {1, 2}
        for artist_id in (1, 2):
            orig = original[artist_id]
            read = loaded[artist_id]
            assert read.recording_count == orig.recording_count
            assert read.avg_danceability == pytest.approx(orig.avg_danceability)
            assert read.primary_genre == orig.primary_genre
            assert read.primary_genre_probability == pytest.approx(orig.primary_genre_probability)
            assert read.voice_instrumental_ratio == pytest.approx(orig.voice_instrumental_ratio)
            assert len(read.feature_centroid) == FEATURE_VECTOR_DIM
            for a, b in zip(read.feature_centroid, orig.feature_centroid, strict=True):
                assert a == pytest.approx(b)


# --- Build profiles pipeline ---


class TestBuildAudioProfiles:
    """Test the full pipeline: artist → MB recordings → AcousticBrainz → profiles."""

    def test_build_profiles(self, ab_data_dir: Path, graph_db: sqlite3.Connection) -> None:
        # Simulate artist → recording MBID mapping (what MB database provides)
        artist_recordings = {
            1: [MBID_AUTECHRE_1, MBID_AUTECHRE_2],  # Autechre has 2 recordings
            2: [MBID_STEREOLAB],  # Stereolab has 1
            3: [MBID_MISSING],  # Cat Power has no AB data
        }

        loader = AcousticBrainzLoader(str(ab_data_dir))
        profiles = build_audio_profiles(loader, artist_recordings, min_recordings=1)

        assert len(profiles) == 2
        assert 1 in profiles  # Autechre
        assert 2 in profiles  # Stereolab
        assert 3 not in profiles  # Cat Power (no AB data)

        assert profiles[1].recording_count == 2
        assert profiles[2].recording_count == 1

    def test_min_recordings_filter(self, ab_data_dir: Path) -> None:
        artist_recordings = {
            1: [MBID_AUTECHRE_1, MBID_AUTECHRE_2],
            2: [MBID_STEREOLAB],  # Only 1 recording
        }

        loader = AcousticBrainzLoader(str(ab_data_dir))
        profiles = build_audio_profiles(loader, artist_recordings, min_recordings=2)

        assert len(profiles) == 1
        assert 1 in profiles  # 2 recordings
        assert 2 not in profiles  # filtered out


# --- Tar loader ---


class TestTarAcousticBrainzLoader:
    """Test loading features directly from tar archives."""

    def test_get_features_from_tar(self, ab_tar_dir: Path, tmp_path: Path) -> None:
        loader = TarAcousticBrainzLoader(str(ab_tar_dir), index_path=str(tmp_path / "idx.db"))
        features = loader.get_features(MBID_AUTECHRE_1)

        assert features is not None
        assert features.recording_mbid == MBID_AUTECHRE_1
        assert features.genre == "electronic"

    def test_missing_mbid_returns_none(self, ab_tar_dir: Path, tmp_path: Path) -> None:
        loader = TarAcousticBrainzLoader(str(ab_tar_dir), index_path=str(tmp_path / "idx.db"))
        assert loader.get_features(MBID_MISSING) is None

    def test_batch_get_from_tar(self, ab_tar_dir: Path, tmp_path: Path) -> None:
        loader = TarAcousticBrainzLoader(str(ab_tar_dir), index_path=str(tmp_path / "idx.db"))
        results = loader.batch_get_features([MBID_AUTECHRE_1, MBID_STEREOLAB, MBID_MISSING])

        assert len(results) == 2
        assert MBID_AUTECHRE_1 in results
        assert MBID_STEREOLAB in results

    def test_wanted_mbids_filter(self, ab_tar_dir: Path, tmp_path: Path) -> None:
        """Only index MBIDs in the wanted set."""
        loader = TarAcousticBrainzLoader(
            str(ab_tar_dir), wanted_mbids={MBID_AUTECHRE_1}, index_path=str(tmp_path / "idx.db")
        )

        assert loader.get_features(MBID_AUTECHRE_1) is not None
        assert loader.get_features(MBID_STEREOLAB) is None  # not in wanted set

    def test_index_persisted_and_reloaded(self, ab_tar_dir: Path, tmp_path: Path) -> None:
        """Index is persisted to SQLite and reloaded on next instantiation."""
        idx_path = str(tmp_path / "idx.db")
        loader1 = TarAcousticBrainzLoader(str(ab_tar_dir), index_path=idx_path)
        assert len(loader1._index) == 3  # 3 test MBIDs

        # Second load should use cached index (no tar scan)
        loader2 = TarAcousticBrainzLoader(str(ab_tar_dir), index_path=idx_path)
        assert len(loader2._index) == 3
        assert loader2.get_features(MBID_AUTECHRE_1) is not None

    def test_build_profiles_with_tar_loader(self, ab_tar_dir: Path, tmp_path: Path) -> None:
        """TarAcousticBrainzLoader works with build_audio_profiles."""
        loader = TarAcousticBrainzLoader(str(ab_tar_dir), index_path=str(tmp_path / "idx.db"))
        artist_recordings = {
            1: [MBID_AUTECHRE_1, MBID_AUTECHRE_2],
            2: [MBID_STEREOLAB],
        }
        profiles = build_audio_profiles(loader, artist_recordings, min_recordings=1)

        assert len(profiles) == 2
        assert profiles[1].recording_count == 2
