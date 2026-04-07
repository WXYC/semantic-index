"""AcousticBrainz feature loader and artist audio profile aggregation.

Loads high-level audio features from the AcousticBrainz data dump (CC0,
archived at data.metabrainz.org). Features are keyed by MusicBrainz
recording MBID and organized in a directory tree:

    highlevel/{first2}/{char3}/{mbid}-{submission}.json

Supports two loading modes:
- **Extracted**: reads JSON files from an extracted directory tree
- **Tar-indexed**: scans .tar archives, builds an in-memory MBID index,
  and reads features directly from the tar without extraction (much faster
  on network-attached storage where creating millions of small files is slow)

Each JSON file contains classifier outputs (probability distributions) for
danceability, genre, mood, timbre, tonality, and voice/instrumental.
"""

from __future__ import annotations

import io
import json
import logging
import math
import tarfile
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


# Ordered labels matching the AcousticBrainz genre_dortmund classifier output
GENRE_LABELS = [
    "alternative",
    "blues",
    "electronic",
    "folkcountry",
    "funksoulrnb",
    "jazz",
    "pop",
    "raphiphop",
    "rock",
]

# Ordered labels matching the mood classifiers (each is a binary classifier)
MOOD_LABELS = [
    "mood_acoustic",
    "mood_aggressive",
    "mood_electronic",
    "mood_happy",
    "mood_party",
    "mood_relaxed",
    "mood_sad",
]


@dataclass
class RecordingFeatures:
    """Parsed audio features for a single recording from AcousticBrainz.

    Attributes:
        recording_mbid: MusicBrainz recording UUID.
        danceability: Probability of being danceable (0-1).
        genre: Top genre label from genre_dortmund classifier.
        genre_probability: Confidence of the top genre prediction.
        genre_vector: Full 9-element probability distribution over genres.
        mood_vector: 7-element vector of mood probabilities (acoustic, aggressive,
            electronic, happy, party, relaxed, sad).
        timbre: "bright" or "dark".
        timbre_probability: Confidence of timbre prediction.
        tonal: Probability of being tonal (vs atonal).
        voice_instrumental: "voice" or "instrumental".
        voice_instrumental_probability: Confidence of voice/instrumental prediction.
    """

    GENRE_LABELS = GENRE_LABELS

    recording_mbid: str
    danceability: float
    genre: str
    genre_probability: float
    genre_vector: list[float]
    mood_vector: list[float]
    timbre: str
    timbre_probability: float
    tonal: float
    voice_instrumental: str
    voice_instrumental_probability: float

    def feature_vector(self) -> list[float]:
        """Build a fixed-length numeric feature vector for similarity computation.

        Layout (20 dimensions):
            [0:9]   genre_dortmund probability distribution (9 genres)
            [9:16]  mood probabilities (7 moods)
            [16]    danceability probability
            [17]    timbre (bright=1, dark=0)
            [18]    tonal probability
            [19]    voice probability (1=voice, 0=instrumental)

        Returns:
            List of 20 floats.
        """
        timbre_val = 1.0 if self.timbre == "bright" else 0.0
        voice_val = 1.0 - self.voice_instrumental_probability if self.voice_instrumental == "instrumental" else self.voice_instrumental_probability
        return self.genre_vector + self.mood_vector + [
            self.danceability,
            timbre_val,
            self.tonal,
            voice_val,
        ]


def _parse_highlevel(mbid: str, data: dict) -> RecordingFeatures:
    """Parse an AcousticBrainz high-level JSON into RecordingFeatures."""
    hl = data["highlevel"]

    # Danceability: probability of "danceable"
    danceability = hl["danceability"]["all"]["danceable"]

    # Genre (genre_dortmund classifier)
    genre_all = hl["genre_dortmund"]["all"]
    genre = hl["genre_dortmund"]["value"]
    genre_probability = hl["genre_dortmund"]["probability"]
    genre_vector = [genre_all.get(label, 0.0) for label in GENRE_LABELS]

    # Mood vector: extract positive probability from each binary mood classifier
    mood_vector = []
    for mood_key in MOOD_LABELS:
        mood_data = hl[mood_key]["all"]
        # The positive label is the mood name without the "mood_" prefix
        positive_label = mood_key.replace("mood_", "")
        mood_vector.append(mood_data.get(positive_label, 0.0))

    # Timbre
    timbre = hl["timbre"]["value"]
    timbre_probability = hl["timbre"]["probability"]

    # Tonal/atonal
    tonal = hl["tonal_atonal"]["all"]["tonal"]

    # Voice/instrumental
    voice_instrumental = hl["voice_instrumental"]["value"]
    voice_instrumental_probability = hl["voice_instrumental"]["probability"]

    return RecordingFeatures(
        recording_mbid=mbid,
        danceability=danceability,
        genre=genre,
        genre_probability=genre_probability,
        genre_vector=genre_vector,
        mood_vector=mood_vector,
        timbre=timbre,
        timbre_probability=timbre_probability,
        tonal=tonal,
        voice_instrumental=voice_instrumental,
        voice_instrumental_probability=voice_instrumental_probability,
    )


class AcousticBrainzLoader:
    """Load audio features from a local AcousticBrainz data dump.

    Expects the dump to be extracted into a directory with the layout:
        {data_dir}/highlevel/{first2chars}/{char3}/{mbid}-{submission}.json

    Args:
        data_dir: Path to the extracted AcousticBrainz dump directory.
    """

    def __init__(self, data_dir: str) -> None:
        self._data_dir = Path(data_dir)

    def _feature_path(self, mbid: str, submission: int = 0) -> Path:
        """Build the file path for a recording MBID."""
        prefix1 = mbid[:2]
        prefix2 = mbid[2]
        return self._data_dir / "highlevel" / prefix1 / prefix2 / f"{mbid}-{submission}.json"

    def get_features(self, recording_mbid: str) -> RecordingFeatures | None:
        """Load features for a single recording MBID.

        Tries submission 0 first. Returns None if the file doesn't exist
        or can't be parsed.

        Args:
            recording_mbid: MusicBrainz recording UUID string.

        Returns:
            Parsed RecordingFeatures, or None if not found.
        """
        path = self._feature_path(recording_mbid)
        if not path.exists():
            return None

        try:
            with open(path) as f:
                data = json.load(f)
            return _parse_highlevel(recording_mbid, data)
        except (json.JSONDecodeError, KeyError):
            logger.warning("Failed to parse AcousticBrainz JSON: %s", path)
            return None

    def batch_get_features(self, mbids: list[str]) -> dict[str, RecordingFeatures]:
        """Load features for multiple recording MBIDs.

        Args:
            mbids: List of MusicBrainz recording UUID strings.

        Returns:
            Dict mapping MBID to RecordingFeatures (only for found recordings).
        """
        results: dict[str, RecordingFeatures] = {}
        for mbid in mbids:
            features = self.get_features(mbid)
            if features is not None:
                results[mbid] = features
        return results


class TarAcousticBrainzLoader:
    """Load audio features directly from AcousticBrainz tar archives.

    Scans tar files to build an in-memory index of MBID → (tar_path, member_name),
    then reads features on demand without extracting. This avoids creating millions
    of small files on disk, which is critical for network-attached storage.

    Only indexes MBIDs present in the ``wanted_mbids`` set (if provided),
    keeping memory usage proportional to the number of WXYC artists rather
    than the full 4M-recording archive.

    Args:
        tar_dir: Directory containing .tar files from the AcousticBrainz dump.
        wanted_mbids: Optional set of MBIDs to index. If None, indexes everything.
    """

    def __init__(self, tar_dir: str, wanted_mbids: set[str] | None = None) -> None:
        self._tar_dir = Path(tar_dir)
        self._wanted = wanted_mbids
        # MBID → (tar_path, member_name)
        self._index: dict[str, tuple[Path, str]] = {}
        self._build_index()

    def _build_index(self) -> None:
        """Scan all tar files and index MBID → location."""
        tar_files = sorted(self._tar_dir.glob("*.tar"))
        logger.info("Indexing %d tar files in %s...", len(tar_files), self._tar_dir)

        for tar_path in tar_files:
            try:
                with tarfile.open(tar_path) as tf:
                    for member in tf.getmembers():
                        if not member.isfile() or not member.name.endswith(".json"):
                            continue
                        # Extract MBID from path like highlevel/0e/1/0e11...-0.json
                        filename = member.name.rsplit("/", 1)[-1]
                        mbid = filename.rsplit("-", 1)[0]
                        if self._wanted is not None and mbid not in self._wanted:
                            continue
                        if mbid not in self._index:
                            self._index[mbid] = (tar_path, member.name)
            except (tarfile.TarError, OSError):
                logger.warning("Failed to index tar: %s", tar_path, exc_info=True)

            logger.info("  %s: indexed, %d MBIDs total", tar_path.name, len(self._index))

        logger.info("Index complete: %d MBIDs across %d tar files", len(self._index), len(tar_files))

    def get_features(self, recording_mbid: str) -> RecordingFeatures | None:
        """Load features for a single recording MBID from the tar index.

        Args:
            recording_mbid: MusicBrainz recording UUID string.

        Returns:
            Parsed RecordingFeatures, or None if not in the index.
        """
        entry = self._index.get(recording_mbid)
        if entry is None:
            return None

        tar_path, member_name = entry
        try:
            with tarfile.open(tar_path) as tf:
                f = tf.extractfile(member_name)
                if f is None:
                    return None
                data = json.load(f)
            return _parse_highlevel(recording_mbid, data)
        except (tarfile.TarError, json.JSONDecodeError, KeyError, OSError):
            logger.warning("Failed to read %s from %s", member_name, tar_path)
            return None

    def batch_get_features(self, mbids: list[str]) -> dict[str, RecordingFeatures]:
        """Load features for multiple recording MBIDs.

        Groups lookups by tar file to minimize archive opens.

        Args:
            mbids: List of MusicBrainz recording UUID strings.

        Returns:
            Dict mapping MBID to RecordingFeatures (only for found MBIDs).
        """
        # Group by tar file for efficiency
        by_tar: dict[Path, list[tuple[str, str]]] = {}
        for mbid in mbids:
            entry = self._index.get(mbid)
            if entry is not None:
                tar_path, member_name = entry
                by_tar.setdefault(tar_path, []).append((mbid, member_name))

        results: dict[str, RecordingFeatures] = {}
        for tar_path, items in by_tar.items():
            try:
                with tarfile.open(tar_path) as tf:
                    for mbid, member_name in items:
                        try:
                            f = tf.extractfile(member_name)
                            if f is None:
                                continue
                            data = json.load(f)
                            features = _parse_highlevel(mbid, data)
                            results[mbid] = features
                        except (json.JSONDecodeError, KeyError):
                            continue
            except (tarfile.TarError, OSError):
                logger.warning("Failed to open tar: %s", tar_path)

        return results


@dataclass
class ArtistAudioProfile:
    """Aggregated audio features for an artist across all their recordings.

    Built by averaging per-recording feature vectors into a single centroid.
    """

    recording_count: int
    avg_danceability: float
    primary_genre: str
    primary_genre_probability: float
    voice_instrumental_ratio: float  # fraction of recordings that are vocal
    feature_centroid: list[float]  # averaged 20-element feature vector

    @classmethod
    def from_recordings(cls, recordings: list[RecordingFeatures]) -> ArtistAudioProfile:
        """Aggregate multiple recording features into an artist profile.

        Args:
            recordings: List of RecordingFeatures for this artist.

        Returns:
            Aggregated ArtistAudioProfile.

        Raises:
            ValueError: If recordings list is empty.
        """
        if not recordings:
            raise ValueError("Need at least one recording to build a profile")

        n = len(recordings)

        # Average danceability
        avg_danceability = sum(r.danceability for r in recordings) / n

        # Voice/instrumental ratio: fraction that are vocal
        vocal_count = sum(1 for r in recordings if r.voice_instrumental == "voice")
        voice_ratio = vocal_count / n

        # Average feature vectors
        vec_len = 20
        centroid = [0.0] * vec_len
        for r in recordings:
            vec = r.feature_vector()
            for i in range(vec_len):
                centroid[i] += vec[i]
        centroid = [v / n for v in centroid]

        # Primary genre: majority vote
        genre_counts: dict[str, float] = {}
        for r in recordings:
            genre_counts[r.genre] = genre_counts.get(r.genre, 0) + r.genre_probability
        primary_genre = max(genre_counts, key=genre_counts.get)  # type: ignore[arg-type]
        primary_genre_probability = genre_counts[primary_genre] / n

        return cls(
            recording_count=n,
            avg_danceability=avg_danceability,
            primary_genre=primary_genre,
            primary_genre_probability=primary_genre_probability,
            voice_instrumental_ratio=voice_ratio,
            feature_centroid=centroid,
        )


def cosine_similarity(a: list[float], b: list[float]) -> float:
    """Compute cosine similarity between two feature vectors.

    Args:
        a: First feature vector.
        b: Second feature vector (same length as a).

    Returns:
        Cosine similarity in range [-1, 1]. Returns 0.0 for zero vectors.
    """
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)


def build_audio_profiles(
    loader: AcousticBrainzLoader,
    artist_recordings: dict[int, list[str]],
    min_recordings: int = 3,
) -> dict[int, ArtistAudioProfile]:
    """Build aggregated audio profiles for artists with AcousticBrainz data.

    For each artist, looks up all their recording MBIDs in the AcousticBrainz
    dump. Only creates profiles for artists with at least ``min_recordings``
    successfully loaded.

    Args:
        loader: AcousticBrainzLoader pointed at the extracted dump.
        artist_recordings: Mapping of artist ID → list of recording MBIDs.
        min_recordings: Minimum recordings required to create a profile.

    Returns:
        Dict mapping artist ID to ArtistAudioProfile.
    """
    profiles: dict[int, ArtistAudioProfile] = {}
    total = len(artist_recordings)

    for i, (artist_id, mbids) in enumerate(artist_recordings.items(), 1):
        features_map = loader.batch_get_features(mbids)
        recordings = list(features_map.values())

        if len(recordings) < min_recordings:
            continue

        profiles[artist_id] = ArtistAudioProfile.from_recordings(recordings)

        if i % 500 == 0:
            logger.info("  Audio profiles: %d/%d artists processed, %d profiles built", i, total, len(profiles))

    logger.info("Audio profiles complete: %d/%d artists have profiles (min_recordings=%d)", len(profiles), total, min_recordings)
    return profiles


_AUDIO_PROFILE_SCHEMA = """
CREATE TABLE IF NOT EXISTS audio_profile (
    artist_id INTEGER PRIMARY KEY REFERENCES artist(id),
    avg_danceability REAL,
    primary_genre TEXT,
    primary_genre_probability REAL,
    voice_instrumental_ratio REAL,
    feature_centroid TEXT,
    recording_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
"""

_ACOUSTIC_SIMILARITY_SCHEMA = """
CREATE TABLE IF NOT EXISTS acoustic_similarity (
    artist_a_id INTEGER NOT NULL REFERENCES artist(id),
    artist_b_id INTEGER NOT NULL REFERENCES artist(id),
    similarity REAL NOT NULL,
    PRIMARY KEY (artist_a_id, artist_b_id)
);
CREATE INDEX IF NOT EXISTS idx_acoustic_sim_a ON acoustic_similarity(artist_a_id);
CREATE INDEX IF NOT EXISTS idx_acoustic_sim_b ON acoustic_similarity(artist_b_id);
"""


def store_audio_profiles(
    conn: sqlite3.Connection,
    profiles: dict[int, ArtistAudioProfile],
) -> int:
    """Persist audio profiles to the SQLite graph database.

    Creates the ``audio_profile`` table if it doesn't exist, then
    inserts or replaces all profiles.

    Args:
        conn: SQLite connection to the graph database.
        profiles: Mapping of artist ID → ArtistAudioProfile.

    Returns:
        Number of profiles stored.
    """
    import sqlite3 as _sqlite3

    conn.executescript(_AUDIO_PROFILE_SCHEMA)

    for artist_id, profile in profiles.items():
        conn.execute(
            "INSERT OR REPLACE INTO audio_profile "
            "(artist_id, avg_danceability, primary_genre, primary_genre_probability, "
            "voice_instrumental_ratio, feature_centroid, recording_count) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                artist_id,
                profile.avg_danceability,
                profile.primary_genre,
                profile.primary_genre_probability,
                profile.voice_instrumental_ratio,
                json.dumps(profile.feature_centroid),
                profile.recording_count,
            ),
        )
    conn.commit()
    return len(profiles)


def compute_acoustic_similarity(
    conn: sqlite3.Connection,
    profiles: dict[int, ArtistAudioProfile],
    threshold: float = 0.85,
) -> int:
    """Compute pairwise cosine similarity between artist audio profiles.

    Only stores edges where similarity >= threshold. Stores edges in
    canonical order (artist_a_id < artist_b_id).

    Args:
        conn: SQLite connection to the graph database.
        profiles: Mapping of artist ID → ArtistAudioProfile.
        threshold: Minimum similarity to emit an edge.

    Returns:
        Number of similarity edges created.
    """
    conn.executescript(_ACOUSTIC_SIMILARITY_SCHEMA)

    artist_ids = sorted(profiles.keys())
    edge_count = 0

    for i, aid in enumerate(artist_ids):
        for j in range(i + 1, len(artist_ids)):
            bid = artist_ids[j]
            sim = cosine_similarity(
                profiles[aid].feature_centroid, profiles[bid].feature_centroid
            )
            if sim >= threshold:
                conn.execute(
                    "INSERT OR REPLACE INTO acoustic_similarity "
                    "(artist_a_id, artist_b_id, similarity) VALUES (?, ?, ?)",
                    (aid, bid, sim),
                )
                edge_count += 1

        if (i + 1) % 100 == 0:
            logger.info("  Acoustic similarity: %d/%d artists compared, %d edges", i + 1, len(artist_ids), edge_count)

    conn.commit()
    logger.info("Acoustic similarity complete: %d edges (threshold=%.2f)", edge_count, threshold)
    return edge_count
