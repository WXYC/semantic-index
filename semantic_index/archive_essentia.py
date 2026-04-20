"""Essentia TF audio classification for WXYC archive segments.

Runs pre-trained Essentia classification heads on VGGish embeddings
extracted from archive audio segments. Produces per-segment features
compatible with the existing :class:`RecordingFeatures` schema from
:mod:`acousticbrainz`, enabling the same 59-dimension feature vector
for acoustic similarity and narrative enrichment.

Requires ``essentia-tensorflow`` (Python 3.13, not 3.14) and
pre-downloaded model files from ``essentia.upf.edu/models/``.

VGGish feature extractor:
    ``audioset-vggish-3.pb`` (275 MB)

Classification heads (15 × ~50 KB each):
    ``{category}/{category}-audioset-vggish-1.pb``

Three AcousticBrainz classifiers lack VGGish heads and are zero-filled:
    - ``ismir04_rhythm`` (10 dims)
    - ``genre_electronic`` (5 dims)
    - ``timbre`` (1 dim, set to 0.5 as neutral)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from semantic_index.acousticbrainz import (
    FEATURE_VECTOR_DIM,
    GENRE_LABELS,
    GENRE_ROSAMERICA_LABELS,
    GENRE_TZANETAKIS_LABELS,
    MIREX_LABELS,
    MOOD_LABELS,
    RecordingFeatures,
)

logger = logging.getLogger(__name__)

# Default TF graph node names for most classification heads
_DEFAULT_INPUT = "model/Placeholder"
_DEFAULT_OUTPUT = "model/Softmax"

# Classification head registry: name -> {model_file, labels, input_node, output_node}
CLASSIFIERS: dict[str, dict] = {
    "danceability": {
        "model_file": "danceability-audioset-vggish-1.pb",
        "labels": ["danceable", "not_danceable"],
        "input_node": _DEFAULT_INPUT,
        "output_node": _DEFAULT_OUTPUT,
    },
    "genre_dortmund": {
        "model_file": "genre_dortmund-audioset-vggish-1.pb",
        "labels": list(GENRE_LABELS),
        "input_node": _DEFAULT_INPUT,
        "output_node": _DEFAULT_OUTPUT,
    },
    "mood_acoustic": {
        "model_file": "mood_acoustic-audioset-vggish-1.pb",
        "labels": ["acoustic", "not_acoustic"],
        "input_node": _DEFAULT_INPUT,
        "output_node": _DEFAULT_OUTPUT,
    },
    "mood_aggressive": {
        "model_file": "mood_aggressive-audioset-vggish-1.pb",
        "labels": ["aggressive", "not_aggressive"],
        "input_node": _DEFAULT_INPUT,
        "output_node": _DEFAULT_OUTPUT,
    },
    "mood_electronic": {
        "model_file": "mood_electronic-audioset-vggish-1.pb",
        "labels": ["electronic", "not_electronic"],
        "input_node": _DEFAULT_INPUT,
        "output_node": _DEFAULT_OUTPUT,
    },
    "mood_happy": {
        "model_file": "mood_happy-audioset-vggish-1.pb",
        "labels": ["happy", "not_happy"],
        "input_node": _DEFAULT_INPUT,
        "output_node": _DEFAULT_OUTPUT,
    },
    "mood_party": {
        "model_file": "mood_party-audioset-vggish-1.pb",
        "labels": ["party", "not_party"],
        "input_node": _DEFAULT_INPUT,
        "output_node": _DEFAULT_OUTPUT,
    },
    "mood_relaxed": {
        "model_file": "mood_relaxed-audioset-vggish-1.pb",
        "labels": ["relaxed", "not_relaxed"],
        "input_node": _DEFAULT_INPUT,
        "output_node": _DEFAULT_OUTPUT,
    },
    "mood_sad": {
        "model_file": "mood_sad-audioset-vggish-1.pb",
        "labels": ["sad", "not_sad"],
        "input_node": _DEFAULT_INPUT,
        "output_node": _DEFAULT_OUTPUT,
    },
    "moods_mirex": {
        "model_file": "moods_mirex-audioset-vggish-1.pb",
        "labels": list(MIREX_LABELS),
        "input_node": "serving_default_model_Placeholder",
        "output_node": "PartitionedCall",
    },
    "tonal_atonal": {
        "model_file": "tonal_atonal-audioset-vggish-1.pb",
        "labels": ["tonal", "atonal"],
        "input_node": _DEFAULT_INPUT,
        "output_node": _DEFAULT_OUTPUT,
    },
    "voice_instrumental": {
        "model_file": "voice_instrumental-audioset-vggish-1.pb",
        "labels": ["voice", "instrumental"],
        "input_node": _DEFAULT_INPUT,
        "output_node": _DEFAULT_OUTPUT,
    },
    "gender": {
        "model_file": "gender-audioset-vggish-1.pb",
        "labels": ["female", "male"],
        "input_node": _DEFAULT_INPUT,
        "output_node": _DEFAULT_OUTPUT,
    },
    "genre_rosamerica": {
        "model_file": "genre_rosamerica-audioset-vggish-1.pb",
        "labels": list(GENRE_ROSAMERICA_LABELS),
        "input_node": _DEFAULT_INPUT,
        "output_node": _DEFAULT_OUTPUT,
    },
    "genre_tzanetakis": {
        "model_file": "genre_tzanetakis-audioset-vggish-1.pb",
        "labels": list(GENRE_TZANETAKIS_LABELS),
        "input_node": _DEFAULT_INPUT,
        "output_node": _DEFAULT_OUTPUT,
    },
}

VGGISH_MODEL_FILE = "audioset-vggish-3.pb"
VGGISH_SAMPLE_RATE = 16000


@dataclass
class SegmentFeatures:
    """Condensed classification results for a single audio segment.

    Stores the narratively useful fields plus the full feature vector
    for aggregation into per-artist profiles.

    Attributes:
        artist_name: Artist name from the flowsheet entry.
        danceability: Probability of being danceable (0-1).
        genre: Top genre label from genre_dortmund classifier.
        genre_probability: Confidence of the top genre prediction.
        genre_vector: Full 9-element genre distribution.
        mood_vector: 7-element mood probability vector.
        voice_instrumental: "voice" or "instrumental".
        voice_instrumental_probability: Confidence of the prediction.
        feature_vector: Full 59-dim feature vector for similarity.
        bpm: Estimated tempo in beats per minute.
        key: Musical key (e.g. "A", "C#").
        scale: Musical scale ("major" or "minor").
        key_strength: Confidence of key estimation (0-1).
    """

    artist_name: str
    danceability: float
    genre: str
    genre_probability: float
    genre_vector: list[float]
    mood_vector: list[float]
    voice_instrumental: str
    voice_instrumental_probability: float
    feature_vector: list[float]
    bpm: float = 0.0
    key: str = ""
    scale: str = ""
    key_strength: float = 0.0


def _parse_classifier_output(
    predictions: list[list[float]],
    labels: list[str],
) -> dict[str, float]:
    """Average classifier predictions across time frames.

    Args:
        predictions: 2D array of shape (num_frames, num_labels).
        labels: Label names corresponding to the second axis.

    Returns:
        Dict mapping each label to its mean probability.
    """
    num_frames = len(predictions)
    result = {}
    for i, label in enumerate(labels):
        total = sum(predictions[frame][i] for frame in range(num_frames))
        result[label] = total / num_frames
    return result


def _build_recording_features(
    classifier_results: dict[str, dict[str, float]],
) -> RecordingFeatures:
    """Convert classifier output dicts into a RecordingFeatures instance.

    Maps the 15 available Essentia classifiers into the 18-classifier
    RecordingFeatures layout, zero-filling the 3 unavailable classifiers
    (ismir04_rhythm, genre_electronic, timbre).

    Args:
        classifier_results: Maps classifier name to label→probability dict.

    Returns:
        RecordingFeatures with ``recording_mbid="archive"``.
    """
    # Danceability
    dance = classifier_results["danceability"]
    danceability = dance["danceable"]

    # Genre (genre_dortmund)
    genre_dist = classifier_results["genre_dortmund"]
    genre = max(genre_dist, key=genre_dist.get)  # type: ignore[arg-type]
    genre_probability = genre_dist[genre]
    genre_vector = [genre_dist.get(label, 0.0) for label in GENRE_LABELS]

    # Moods (7 binary classifiers)
    mood_vector = []
    for mood_key in MOOD_LABELS:
        mood_dist = classifier_results[mood_key]
        positive_label = mood_key.replace("mood_", "")
        mood_vector.append(mood_dist.get(positive_label, 0.0))

    # MIREX compound mood
    mirex_dist = classifier_results["moods_mirex"]
    mirex_vector = [mirex_dist.get(label, 0.0) for label in MIREX_LABELS]

    # Rhythm — unavailable, zero-fill
    rhythm_vector = [0.0] * 10

    # Tonal/atonal
    tonal_dist = classifier_results["tonal_atonal"]
    tonal = tonal_dist["tonal"]

    # Voice/instrumental
    vi_dist = classifier_results["voice_instrumental"]
    if vi_dist["voice"] >= vi_dist["instrumental"]:
        voice_instrumental = "voice"
        voice_instrumental_probability = vi_dist["voice"]
    else:
        voice_instrumental = "instrumental"
        voice_instrumental_probability = vi_dist["instrumental"]

    # Gender
    gender_dist = classifier_results["gender"]
    gender_female = gender_dist["female"]

    # Timbre — unavailable, neutral defaults
    timbre = "dark"
    timbre_probability = 0.5

    # Genre electronic — unavailable, zero-fill
    genre_electronic_vector = [0.0] * 5

    # Genre rosamerica
    rosa_dist = classifier_results["genre_rosamerica"]
    genre_rosamerica_vector = [rosa_dist.get(label, 0.0) for label in GENRE_ROSAMERICA_LABELS]

    # Genre tzanetakis
    tzan_dist = classifier_results["genre_tzanetakis"]
    genre_tzanetakis_vector = [tzan_dist.get(label, 0.0) for label in GENRE_TZANETAKIS_LABELS]

    return RecordingFeatures(
        recording_mbid="archive",
        danceability=danceability,
        genre=genre,
        genre_probability=genre_probability,
        genre_vector=genre_vector,
        mood_vector=mood_vector,
        mirex_vector=mirex_vector,
        rhythm_vector=rhythm_vector,
        gender_female=gender_female,
        timbre=timbre,
        timbre_probability=timbre_probability,
        tonal=tonal,
        voice_instrumental=voice_instrumental,
        voice_instrumental_probability=voice_instrumental_probability,
        genre_electronic_vector=genre_electronic_vector,
        genre_rosamerica_vector=genre_rosamerica_vector,
        genre_tzanetakis_vector=genre_tzanetakis_vector,
    )


def extract_rhythm_and_key(audio_array, sample_rate: int = VGGISH_SAMPLE_RATE) -> dict:
    """Extract BPM and musical key from an audio array via Essentia DSP.

    Uses ``RhythmExtractor2013`` for tempo and ``KeyExtractor`` for
    tonal analysis. Both operate on the raw audio signal (no TF models).

    Args:
        audio_array: Mono float32 audio array.
        sample_rate: Sample rate in Hz (default 16000).

    Returns:
        Dict with ``bpm``, ``key``, ``scale``, ``key_strength``.
    """
    from essentia.standard import KeyExtractor, RhythmExtractor2013

    result: dict = {"bpm": 0.0, "key": "", "scale": "", "key_strength": 0.0}

    try:
        rhythm = RhythmExtractor2013()
        bpm, _beats, _confidence, _estimates, _intervals = rhythm(audio_array)
        result["bpm"] = float(bpm)
    except Exception:
        logger.debug("RhythmExtractor2013 failed", exc_info=True)

    try:
        key_ext = KeyExtractor(sampleRate=sample_rate)
        key, scale, strength = key_ext(audio_array)
        result["key"] = key
        result["scale"] = scale
        result["key_strength"] = float(strength)
    except Exception:
        logger.debug("KeyExtractor failed", exc_info=True)

    return result


def aggregate_artist_profile(
    artist_name: str,
    segments: list[SegmentFeatures],
) -> dict:
    """Aggregate per-segment features into an artist audio profile.

    Averages continuous features (danceability, genre vector, mood vector,
    feature centroid) and computes derived fields (primary genre,
    voice/instrumental ratio).

    Args:
        artist_name: Canonical artist name.
        segments: List of classified segments for this artist.

    Returns:
        Dict matching the ``audio_profile`` table schema:
        ``avg_danceability``, ``primary_genre``, ``primary_genre_probability``,
        ``voice_instrumental_ratio``, ``feature_centroid``, ``recording_count``.
    """
    n = len(segments)

    # Average danceability
    avg_danceability = sum(s.danceability for s in segments) / n

    # Average genre vector → primary genre
    avg_genre_vector = [
        sum(s.genre_vector[i] for s in segments) / n for i in range(len(GENRE_LABELS))
    ]
    primary_idx = max(range(len(avg_genre_vector)), key=lambda i: avg_genre_vector[i])
    primary_genre = GENRE_LABELS[primary_idx]
    primary_genre_probability = avg_genre_vector[primary_idx]

    # Voice/instrumental ratio: average voice probability
    voice_probs = []
    for s in segments:
        if s.voice_instrumental == "voice":
            voice_probs.append(s.voice_instrumental_probability)
        else:
            voice_probs.append(1.0 - s.voice_instrumental_probability)
    voice_instrumental_ratio = sum(voice_probs) / n

    # Feature centroid: element-wise average
    feature_centroid = [
        sum(s.feature_vector[i] for s in segments) / n for i in range(FEATURE_VECTOR_DIM)
    ]

    # Average BPM (weighted by key_strength as a proxy for estimate quality)
    bpm_segments = [s for s in segments if s.bpm > 0]
    avg_bpm = sum(s.bpm for s in bpm_segments) / len(bpm_segments) if bpm_segments else 0.0

    # Most common key: vote weighted by key_strength
    key_votes: dict[str, float] = {}
    for s in segments:
        if s.key and s.key_strength > 0:
            label = f"{s.key} {s.scale}"
            key_votes[label] = key_votes.get(label, 0.0) + s.key_strength
    primary_key = max(key_votes, key=key_votes.get) if key_votes else ""  # type: ignore[arg-type]

    return {
        "avg_danceability": avg_danceability,
        "primary_genre": primary_genre,
        "primary_genre_probability": primary_genre_probability,
        "voice_instrumental_ratio": voice_instrumental_ratio,
        "feature_centroid": feature_centroid,
        "recording_count": n,
        "avg_bpm": avg_bpm,
        "primary_key": primary_key,
    }


class EssentiaClassifier:
    """Essentia TF classifier pipeline: audio → VGGish → classification heads.

    Loads models lazily on first use. Thread-safe for single-threaded
    processing (Essentia TF is not thread-safe).

    Args:
        model_dir: Directory containing VGGish and classification head ``.pb`` files.
    """

    def __init__(self, model_dir: str | Path) -> None:
        self._model_dir = Path(model_dir)
        self._vggish = None
        self._heads: dict[str, object] = {}

    def _ensure_vggish(self):
        """Lazily load the VGGish feature extractor."""
        if self._vggish is not None:
            return

        from essentia.standard import TensorflowPredictVGGish

        vggish_path = self._model_dir / VGGISH_MODEL_FILE
        if not vggish_path.exists():
            raise FileNotFoundError(
                f"VGGish model not found: {vggish_path}. "
                f"Download from essentia.upf.edu/models/feature-extractors/vggish/"
            )
        self._vggish = TensorflowPredictVGGish(
            graphFilename=str(vggish_path),
            output="model/vggish/embeddings",
        )
        logger.info("Loaded VGGish feature extractor")

    def _get_head(self, name: str):
        """Lazily load a classification head model."""
        if name in self._heads:
            return self._heads[name]

        from essentia.standard import TensorflowPredict2D

        info = CLASSIFIERS[name]
        model_path = self._model_dir / info["model_file"]
        if not model_path.exists():
            raise FileNotFoundError(f"Classification head not found: {model_path}")

        head = TensorflowPredict2D(
            graphFilename=str(model_path),
            input=info["input_node"],
            output=info["output_node"],
        )
        self._heads[name] = head
        return head

    def _classify_embeddings(
        self, embeddings, source_label: str = "<unknown>"
    ) -> dict[str, dict[str, float]]:
        """Run all classification heads on pre-computed VGGish embeddings.

        Args:
            embeddings: VGGish embedding array (num_frames × 128).
            source_label: Label for logging (file path or offset).

        Returns:
            Dict mapping classifier name to label→probability dict.
        """
        results = {}
        for name, info in CLASSIFIERS.items():
            try:
                head = self._get_head(name)
                predictions = head(embeddings)
                results[name] = _parse_classifier_output(predictions.tolist(), info["labels"])
            except Exception:
                logger.warning("Classifier %s failed on %s", name, source_label, exc_info=True)
        return results

    def classify_audio(self, audio_path: str | Path) -> dict[str, dict[str, float]]:
        """Run all classification heads on an audio file.

        Loads audio at 16 kHz mono via Essentia, extracts VGGish embeddings,
        and feeds them through each classification head.

        Args:
            audio_path: Path to a WAV or MP3 file.

        Returns:
            Dict mapping classifier name to label→probability dict.
        """
        from essentia.standard import MonoLoader

        self._ensure_vggish()
        assert self._vggish is not None

        audio = MonoLoader(filename=str(audio_path), sampleRate=VGGISH_SAMPLE_RATE)()
        embeddings = self._vggish(audio)
        return self._classify_embeddings(embeddings, source_label=str(audio_path))

    def classify_array(self, audio_array) -> dict[str, dict[str, float]]:
        """Run all classification heads on a pre-loaded audio array.

        Use this when slicing segments from a pre-loaded hour file to
        avoid repeated disk I/O.

        Args:
            audio_array: Mono 16 kHz float32 audio array (e.g. numpy).

        Returns:
            Dict mapping classifier name to label→probability dict.
        """
        self._ensure_vggish()
        assert self._vggish is not None
        embeddings = self._vggish(audio_array)
        return self._classify_embeddings(embeddings, source_label="<array>")

    def classify_segment(
        self,
        audio_path: str | Path,
        artist_name: str,
    ) -> SegmentFeatures | None:
        """Classify an audio segment and return condensed features.

        Args:
            audio_path: Path to a WAV or MP3 file (single segment).
            artist_name: Artist name from the flowsheet entry.

        Returns:
            SegmentFeatures if classification succeeds, None otherwise.
        """
        results = self.classify_audio(audio_path)

        # Need all classifiers to build a valid feature vector
        missing = set(CLASSIFIERS) - set(results)
        if missing:
            logger.warning("Skipping segment %s: missing classifiers %s", audio_path, missing)
            return None

        rf = _build_recording_features(results)
        fv = rf.feature_vector()

        return SegmentFeatures(
            artist_name=artist_name,
            danceability=rf.danceability,
            genre=rf.genre,
            genre_probability=rf.genre_probability,
            genre_vector=rf.genre_vector,
            mood_vector=rf.mood_vector,
            voice_instrumental=rf.voice_instrumental,
            voice_instrumental_probability=rf.voice_instrumental_probability,
            feature_vector=fv,
        )
