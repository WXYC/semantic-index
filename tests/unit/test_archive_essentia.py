"""Tests for Essentia TF audio classification module."""

from __future__ import annotations

import pytest

from semantic_index.acousticbrainz import FEATURE_VECTOR_DIM
from semantic_index.archive_essentia import (
    CLASSIFIERS,
    SegmentFeatures,
    _build_recording_features,
    _parse_classifier_output,
    aggregate_artist_profile,
)

# ---------------------------------------------------------------------------
# Classifier output parsing
# ---------------------------------------------------------------------------


class TestParseClassifierOutput:
    def test_binary_classifier(self):
        """Binary classifier returns dict with two keys summing to ~1."""
        labels = ["danceable", "not_danceable"]
        predictions = [[0.7, 0.3], [0.6, 0.4]]  # two time frames
        result = _parse_classifier_output(predictions, labels)
        assert set(result.keys()) == {"danceable", "not_danceable"}
        assert abs(result["danceable"] - 0.65) < 0.01
        assert abs(result["not_danceable"] - 0.35) < 0.01

    def test_multiclass_classifier(self):
        """Multi-class classifier returns dict with one key per label."""
        labels = ["alternative", "blues", "electronic"]
        predictions = [[0.2, 0.5, 0.3], [0.4, 0.3, 0.3]]
        result = _parse_classifier_output(predictions, labels)
        assert len(result) == 3
        assert abs(result["alternative"] - 0.3) < 0.01
        assert abs(result["blues"] - 0.4) < 0.01

    def test_single_frame(self):
        """Single time frame returns exact probabilities."""
        labels = ["tonal", "atonal"]
        predictions = [[0.8, 0.2]]
        result = _parse_classifier_output(predictions, labels)
        assert abs(result["tonal"] - 0.8) < 0.01


# ---------------------------------------------------------------------------
# RecordingFeatures construction
# ---------------------------------------------------------------------------


class TestBuildRecordingFeatures:
    @pytest.fixture()
    def classifier_results(self) -> dict[str, dict[str, float]]:
        """Realistic classifier output from a rock segment."""
        return {
            "danceability": {"danceable": 0.4, "not_danceable": 0.6},
            "genre_dortmund": {
                "alternative": 0.1,
                "blues": 0.2,
                "electronic": 0.05,
                "folkcountry": 0.15,
                "funksoulrnb": 0.03,
                "jazz": 0.1,
                "pop": 0.05,
                "raphiphop": 0.02,
                "rock": 0.3,
            },
            "mood_acoustic": {"acoustic": 0.4, "not_acoustic": 0.6},
            "mood_aggressive": {"aggressive": 0.2, "not_aggressive": 0.8},
            "mood_electronic": {"electronic": 0.3, "not_electronic": 0.7},
            "mood_happy": {"happy": 0.5, "not_happy": 0.5},
            "mood_party": {"party": 0.7, "not_party": 0.3},
            "mood_relaxed": {"relaxed": 0.6, "not_relaxed": 0.4},
            "mood_sad": {"sad": 0.4, "not_sad": 0.6},
            "moods_mirex": {
                "Cluster1": 0.3,
                "Cluster2": 0.25,
                "Cluster3": 0.2,
                "Cluster4": 0.15,
                "Cluster5": 0.1,
            },
            "tonal_atonal": {"tonal": 0.8, "atonal": 0.2},
            "voice_instrumental": {"voice": 0.3, "instrumental": 0.7},
            "gender": {"female": 0.3, "male": 0.7},
            "genre_rosamerica": {
                "cla": 0.01,
                "dan": 0.02,
                "hip": 0.05,
                "jaz": 0.05,
                "pop": 0.2,
                "rhy": 0.3,
                "roc": 0.35,
                "spe": 0.02,
            },
            "genre_tzanetakis": {
                "blu": 0.15,
                "cla": 0.01,
                "cou": 0.15,
                "dis": 0.05,
                "hip": 0.02,
                "jaz": 0.01,
                "met": 0.05,
                "pop": 0.02,
                "reg": 0.1,
                "roc": 0.44,
            },
        }

    def test_feature_vector_length(self, classifier_results):
        """Output matches the 59-dim RecordingFeatures layout."""
        rf = _build_recording_features(classifier_results)
        fv = rf.feature_vector()
        assert len(fv) == FEATURE_VECTOR_DIM

    def test_genre_extracted(self, classifier_results):
        """Primary genre is the argmax of genre_dortmund."""
        rf = _build_recording_features(classifier_results)
        assert rf.genre == "rock"
        assert abs(rf.genre_probability - 0.3) < 0.01

    def test_danceability(self, classifier_results):
        """Danceability maps to the 'danceable' probability."""
        rf = _build_recording_features(classifier_results)
        assert abs(rf.danceability - 0.4) < 0.01

    def test_voice_instrumental(self, classifier_results):
        """Voice/instrumental uses argmax for label, probability for confidence."""
        rf = _build_recording_features(classifier_results)
        assert rf.voice_instrumental == "instrumental"
        assert abs(rf.voice_instrumental_probability - 0.7) < 0.01

    def test_tonal(self, classifier_results):
        """Tonal probability is extracted correctly."""
        rf = _build_recording_features(classifier_results)
        assert abs(rf.tonal - 0.8) < 0.01

    def test_mood_vector(self, classifier_results):
        """Mood vector has 7 elements in the correct order."""
        rf = _build_recording_features(classifier_results)
        assert len(rf.mood_vector) == 7
        assert abs(rf.mood_vector[0] - 0.4) < 0.01  # acoustic
        assert abs(rf.mood_vector[1] - 0.2) < 0.01  # aggressive

    def test_unavailable_classifiers_zeroed(self, classifier_results):
        """Rhythm and genre_electronic are zero-filled."""
        rf = _build_recording_features(classifier_results)
        assert rf.rhythm_vector == [0.0] * 10
        assert rf.genre_electronic_vector == [0.0] * 5

    def test_timbre_neutral(self, classifier_results):
        """Timbre defaults to 'dark' at 0.5 probability (neutral)."""
        rf = _build_recording_features(classifier_results)
        assert rf.timbre == "dark"
        assert abs(rf.timbre_probability - 0.5) < 0.01

    def test_recording_mbid_is_archive_source(self, classifier_results):
        """Recording MBID is 'archive' since there's no MusicBrainz ID."""
        rf = _build_recording_features(classifier_results)
        assert rf.recording_mbid == "archive"


# ---------------------------------------------------------------------------
# Artist profile aggregation
# ---------------------------------------------------------------------------


class TestAggregateArtistProfile:
    def test_single_segment(self):
        """Single segment becomes the profile directly."""
        seg = SegmentFeatures(
            artist_name="Autechre",
            danceability=0.3,
            genre="electronic",
            genre_probability=0.7,
            genre_vector=[0.0, 0.0, 0.7, 0.0, 0.0, 0.0, 0.0, 0.0, 0.3],
            mood_vector=[0.2, 0.6, 0.8, 0.1, 0.3, 0.1, 0.2],
            voice_instrumental="instrumental",
            voice_instrumental_probability=0.9,
            feature_vector=[0.1] * FEATURE_VECTOR_DIM,
        )
        profile = aggregate_artist_profile("Autechre", [seg])
        assert profile["recording_count"] == 1
        assert abs(profile["avg_danceability"] - 0.3) < 0.01
        assert profile["primary_genre"] == "electronic"

    def test_multiple_segments_averaged(self):
        """Multiple segments are averaged for continuous features."""
        seg1 = SegmentFeatures(
            artist_name="Stereolab",
            danceability=0.6,
            genre="rock",
            genre_probability=0.5,
            genre_vector=[0.1, 0.0, 0.2, 0.0, 0.0, 0.0, 0.1, 0.0, 0.5] + [0.1],
            mood_vector=[0.3, 0.1, 0.2, 0.6, 0.7, 0.4, 0.2],
            voice_instrumental="voice",
            voice_instrumental_probability=0.6,
            feature_vector=[0.2] * FEATURE_VECTOR_DIM,
        )
        seg2 = SegmentFeatures(
            artist_name="Stereolab",
            danceability=0.8,
            genre="electronic",
            genre_probability=0.6,
            genre_vector=[0.05, 0.0, 0.6, 0.0, 0.0, 0.0, 0.05, 0.0, 0.3],
            mood_vector=[0.1, 0.2, 0.5, 0.7, 0.8, 0.3, 0.1],
            voice_instrumental="instrumental",
            voice_instrumental_probability=0.55,
            feature_vector=[0.4] * FEATURE_VECTOR_DIM,
        )
        profile = aggregate_artist_profile("Stereolab", [seg1, seg2])
        assert profile["recording_count"] == 2
        assert abs(profile["avg_danceability"] - 0.7) < 0.01
        # Primary genre from averaged genre vector
        assert profile["primary_genre"] in ("rock", "electronic")
        # Feature centroid is averaged
        centroid = profile["feature_centroid"]
        assert len(centroid) == FEATURE_VECTOR_DIM
        assert abs(centroid[0] - 0.3) < 0.01

    def test_voice_instrumental_ratio(self):
        """Voice/instrumental ratio reflects segment proportions."""
        voice_seg = SegmentFeatures(
            artist_name="Cat Power",
            danceability=0.3,
            genre="rock",
            genre_probability=0.4,
            genre_vector=[0.0] * 9,
            mood_vector=[0.0] * 7,
            voice_instrumental="voice",
            voice_instrumental_probability=0.8,
            feature_vector=[0.0] * FEATURE_VECTOR_DIM,
        )
        inst_seg = SegmentFeatures(
            artist_name="Cat Power",
            danceability=0.3,
            genre="rock",
            genre_probability=0.4,
            genre_vector=[0.0] * 9,
            mood_vector=[0.0] * 7,
            voice_instrumental="instrumental",
            voice_instrumental_probability=0.6,
            feature_vector=[0.0] * FEATURE_VECTOR_DIM,
        )
        profile = aggregate_artist_profile("Cat Power", [voice_seg, inst_seg])
        # voice_instrumental_ratio: average of voice probabilities
        # voice=0.8 and instrumental=0.6 → voice_prob = 0.8 and 1-0.6=0.4 → avg = 0.6
        assert abs(profile["voice_instrumental_ratio"] - 0.6) < 0.01


# ---------------------------------------------------------------------------
# Classifier registry
# ---------------------------------------------------------------------------


class TestClassifierRegistry:
    def test_all_classifiers_present(self):
        """All 15 VGGish-compatible classifiers are registered."""
        assert len(CLASSIFIERS) == 15

    def test_classifier_structure(self):
        """Each classifier has model_file, labels, input_node, output_node."""
        for name, info in CLASSIFIERS.items():
            assert "model_file" in info, f"{name} missing model_file"
            assert "labels" in info, f"{name} missing labels"
            assert "input_node" in info, f"{name} missing input_node"
            assert "output_node" in info, f"{name} missing output_node"
            assert len(info["labels"]) >= 2, f"{name} needs at least 2 labels"

    def test_moods_mirex_uses_different_nodes(self):
        """moods_mirex uses SavedModel-style node names."""
        mirex = CLASSIFIERS["moods_mirex"]
        assert mirex["input_node"] == "serving_default_model_Placeholder"
        assert mirex["output_node"] == "PartitionedCall"
