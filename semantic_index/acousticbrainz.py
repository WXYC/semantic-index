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

import json
import logging
import math
import sqlite3
import tarfile
from dataclasses import dataclass
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

# Ordered labels for the moods_mirex compound mood classifier
MIREX_LABELS = ["Cluster1", "Cluster2", "Cluster3", "Cluster4", "Cluster5"]

# Ordered labels for the ismir04_rhythm dance rhythm classifier
RHYTHM_LABELS = [
    "ChaChaCha",
    "Jive",
    "Quickstep",
    "Rumba-American",
    "Rumba-International",
    "Rumba-Misc",
    "Samba",
    "Tango",
    "VienneseWaltz",
    "Waltz",
]

# Labels from AcousticBrainz classifier outputs (verified against data dump)
GENRE_ELECTRONIC_LABELS = ["ambient", "dnb", "house", "techno", "trance"]
GENRE_ROSAMERICA_LABELS = ["cla", "dan", "hip", "jaz", "pop", "rhy", "roc", "spe"]
GENRE_TZANETAKIS_LABELS = ["blu", "cla", "cou", "dis", "hip", "jaz", "met", "pop", "reg", "roc"]

FEATURE_VECTOR_DIM = 59


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
    mirex_vector: list[float]  # 5-element MIREX compound mood distribution
    rhythm_vector: list[float]  # 10-element dance rhythm distribution
    gender_female: float  # probability of female vocal
    timbre: str
    timbre_probability: float
    tonal: float
    voice_instrumental: str
    voice_instrumental_probability: float
    genre_electronic_vector: list[float]  # 5-element electronic subgenre distribution
    genre_rosamerica_vector: list[float]  # 8-element rosamerica genre distribution
    genre_tzanetakis_vector: list[float]  # 10-element tzanetakis genre distribution

    def feature_vector(self) -> list[float]:
        """Build a fixed-length numeric feature vector for similarity computation.

        Layout (59 dimensions):
            [0:9]   genre_dortmund probability distribution (9 genres)
            [9:16]  mood probabilities (7 binary mood classifiers)
            [16:21] moods_mirex compound mood distribution (5 clusters)
            [21:31] ismir04_rhythm dance rhythm distribution (10 rhythms)
            [31]    danceability probability
            [32]    timbre (bright=1, dark=0)
            [33]    tonal probability
            [34]    voice probability (1=voice, 0=instrumental)
            [35]    gender (female probability)
            [36:41] genre_electronic probability distribution (5 subgenres)
            [41:49] genre_rosamerica probability distribution (8 genres)
            [49:59] genre_tzanetakis probability distribution (10 genres)

        Returns:
            List of 59 floats.
        """
        timbre_val = 1.0 if self.timbre == "bright" else 0.0
        voice_val = (
            1.0 - self.voice_instrumental_probability
            if self.voice_instrumental == "instrumental"
            else self.voice_instrumental_probability
        )
        return (
            self.genre_vector
            + self.mood_vector
            + self.mirex_vector
            + self.rhythm_vector
            + [
                self.danceability,
                timbre_val,
                self.tonal,
                voice_val,
                self.gender_female,
            ]
            + self.genre_electronic_vector
            + self.genre_rosamerica_vector
            + self.genre_tzanetakis_vector
        )


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

    # MIREX compound mood
    mirex_all = hl.get("moods_mirex", {}).get("all", {})
    mirex_vector = [mirex_all.get(label, 0.0) for label in MIREX_LABELS]

    # Rhythm
    rhythm_all = hl.get("ismir04_rhythm", {}).get("all", {})
    rhythm_vector = [rhythm_all.get(label, 0.0) for label in RHYTHM_LABELS]

    # Genre electronic subgenre
    genre_electronic_all = hl.get("genre_electronic", {}).get("all", {})
    genre_electronic_vector = [
        genre_electronic_all.get(label, 0.0) for label in GENRE_ELECTRONIC_LABELS
    ]

    # Genre rosamerica
    genre_rosamerica_all = hl.get("genre_rosamerica", {}).get("all", {})
    genre_rosamerica_vector = [
        genre_rosamerica_all.get(label, 0.0) for label in GENRE_ROSAMERICA_LABELS
    ]

    # Genre tzanetakis
    genre_tzanetakis_all = hl.get("genre_tzanetakis", {}).get("all", {})
    genre_tzanetakis_vector = [
        genre_tzanetakis_all.get(label, 0.0) for label in GENRE_TZANETAKIS_LABELS
    ]

    # Gender
    gender_all = hl.get("gender", {}).get("all", {})
    gender_female = gender_all.get("female", 0.5)

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

    Scans tar files to build an index of MBID → (tar_filename, member_name),
    then reads features on demand without extracting. The index is persisted
    to a local SQLite file so subsequent runs skip the slow NAS scan.

    Only indexes MBIDs present in the ``wanted_mbids`` set (if provided),
    keeping memory usage proportional to the number of WXYC artists rather
    than the full 4M-recording archive.

    Args:
        tar_dir: Directory containing .tar files from the AcousticBrainz dump.
        wanted_mbids: Optional set of MBIDs to index. If None, indexes everything.
        index_path: Path to persist the index SQLite file. Defaults to
            ``{tar_dir}/ab_index.db``. Stored locally (not on NAS) for speed.
    """

    def __init__(
        self,
        tar_dir: str,
        wanted_mbids: set[str] | None = None,
        index_path: str | None = None,
    ) -> None:
        self._tar_dir = Path(tar_dir)
        self._wanted = wanted_mbids
        self._index_path = Path(index_path) if index_path else Path("output/ab_index.db")
        # MBID → (tar_filename, member_name) — loaded from SQLite or built from tar scan
        self._index: dict[str, tuple[str, str]] = {}
        self._load_or_build_index()

    def _load_or_build_index(self) -> None:
        """Load index from SQLite cache, or build it by scanning tars."""
        import sqlite3

        if self._index_path.exists():
            conn = sqlite3.connect(str(self._index_path))
            rows = conn.execute("SELECT mbid, tar_file, member_name FROM ab_index").fetchall()
            conn.close()
            for mbid, tar_file, member_name in rows:
                if self._wanted is None or mbid in self._wanted:
                    self._index[mbid] = (tar_file, member_name)
            logger.info("Loaded index from %s: %d MBIDs", self._index_path, len(self._index))
            return

        self._build_index()

    def _build_index(self) -> None:
        """Scan all tar files and build a persistent index."""
        import sqlite3

        tar_files = sorted(self._tar_dir.glob("*.tar"))
        logger.info("Indexing %d tar files in %s...", len(tar_files), self._tar_dir)

        # Build full index (all MBIDs) and persist, then filter in memory
        all_entries: list[tuple[str, str, str]] = []

        for tar_path in tar_files:
            shard_count = 0
            try:
                with tarfile.open(tar_path) as tf:
                    for member in tf:
                        if not member.isfile() or not member.name.endswith(".json"):
                            continue
                        filename = member.name.rsplit("/", 1)[-1]
                        mbid = filename.rsplit("-", 1)[0]
                        all_entries.append((mbid, tar_path.name, member.name))
                        shard_count += 1
            except (tarfile.TarError, OSError):
                logger.warning("Failed to index tar: %s", tar_path, exc_info=True)

            logger.info("  %s: %d files indexed", tar_path.name, shard_count)

        # Persist to SQLite
        self._index_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self._index_path))
        conn.execute(
            "CREATE TABLE IF NOT EXISTS ab_index (mbid TEXT PRIMARY KEY, tar_file TEXT NOT NULL, member_name TEXT NOT NULL)"
        )
        conn.executemany(
            "INSERT OR IGNORE INTO ab_index (mbid, tar_file, member_name) VALUES (?, ?, ?)",
            all_entries,
        )
        conn.commit()
        conn.close()
        logger.info("Index persisted to %s: %d total MBIDs", self._index_path, len(all_entries))

        # Load into memory with wanted filter
        for mbid, tar_file, member_name in all_entries:
            if self._wanted is None or mbid in self._wanted:
                if mbid not in self._index:
                    self._index[mbid] = (tar_file, member_name)

        logger.info("Index loaded: %d MBIDs matching wanted set", len(self._index))

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

        tar_file, member_name = entry
        tar_path = self._tar_dir / tar_file
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

        Groups lookups by tar file to minimize archive opens. Resilient to
        individual tar read failures — skips failed shards and continues.

        Args:
            mbids: List of MusicBrainz recording UUID strings.

        Returns:
            Dict mapping MBID to RecordingFeatures (only for found MBIDs).
        """
        # Group by tar file for efficiency
        by_tar: dict[str, list[tuple[str, str]]] = {}
        for mbid in mbids:
            entry = self._index.get(mbid)
            if entry is not None:
                tar_file, member_name = entry
                by_tar.setdefault(tar_file, []).append((mbid, member_name))

        results: dict[str, RecordingFeatures] = {}
        for tar_file, items in by_tar.items():
            tar_path = self._tar_dir / tar_file
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
                logger.warning("Failed to open tar: %s (skipping %d MBIDs)", tar_path, len(items))

        return results

    def bulk_load_all_features(self) -> dict[str, RecordingFeatures]:
        """Load features for ALL indexed MBIDs in a single pass per tar.

        Instead of opening each tar multiple times (once per artist), this
        method reads each tar file exactly once, extracting every matching
        recording in one sequential scan. This is orders of magnitude faster
        over NAS where tar seek overhead dominates.

        Returns:
            Dict mapping MBID to RecordingFeatures for all indexed recordings.
        """
        # Group index by tar file
        by_tar: dict[str, list[tuple[str, str]]] = {}
        for mbid, (tar_file, member_name) in self._index.items():
            by_tar.setdefault(tar_file, []).append((mbid, member_name))

        results: dict[str, RecordingFeatures] = {}
        total_tars = len(by_tar)

        for i, (tar_file, items) in enumerate(sorted(by_tar.items()), 1):
            tar_path = self._tar_dir / tar_file
            member_lookup = {member_name: mbid for mbid, member_name in items}
            shard_count = 0

            try:
                with tarfile.open(tar_path) as tf:
                    for member in tf:
                        if member.name in member_lookup:
                            try:
                                f = tf.extractfile(member)
                                if f is None:
                                    continue
                                data = json.load(f)
                                mbid = member_lookup[member.name]
                                results[mbid] = _parse_highlevel(mbid, data)
                                shard_count += 1
                            except (json.JSONDecodeError, KeyError):
                                continue
            except (tarfile.TarError, OSError):
                logger.warning("Failed to read tar: %s (skipping)", tar_path)

            logger.info(
                "  %s: %d features loaded (%d/%d tars, %d total)",
                tar_file,
                shard_count,
                i,
                total_tars,
                len(results),
            )

        logger.info("Bulk load complete: %d features from %d tar files", len(results), total_tars)
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
    feature_centroid: list[float]  # averaged 59-element feature vector

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
        vec_len = FEATURE_VECTOR_DIM
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
    dot = sum(x * y for x, y in zip(a, b, strict=True))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)


def build_audio_profiles(
    loader: AcousticBrainzLoader | TarAcousticBrainzLoader,
    artist_recordings: dict[int, list[str]],
    min_recordings: int = 3,
    preloaded: dict[str, RecordingFeatures] | None = None,
) -> dict[int, ArtistAudioProfile]:
    """Build aggregated audio profiles for artists with AcousticBrainz data.

    For each artist, looks up all their recording MBIDs in the AcousticBrainz
    dump. Only creates profiles for artists with at least ``min_recordings``
    successfully loaded.

    When ``preloaded`` is provided, features are looked up from the dict
    instead of calling the loader per-artist. Use this with
    ``TarAcousticBrainzLoader.bulk_load_all_features()`` for best NAS performance.

    Args:
        loader: AcousticBrainzLoader or TarAcousticBrainzLoader.
        artist_recordings: Mapping of artist ID → list of recording MBIDs.
        min_recordings: Minimum recordings required to create a profile.
        preloaded: Optional pre-loaded features dict (MBID → RecordingFeatures).

    Returns:
        Dict mapping artist ID to ArtistAudioProfile.
    """
    profiles: dict[int, ArtistAudioProfile] = {}
    total = len(artist_recordings)

    for i, (artist_id, mbids) in enumerate(artist_recordings.items(), 1):
        if preloaded is not None:
            recordings = [preloaded[m] for m in mbids if m in preloaded]
        else:
            features_map = loader.batch_get_features(mbids)
            recordings = list(features_map.values())

        if len(recordings) < min_recordings:
            continue

        profiles[artist_id] = ArtistAudioProfile.from_recordings(recordings)

        if i % 500 == 0:
            logger.info(
                "  Audio profiles: %d/%d artists processed, %d profiles built",
                i,
                total,
                len(profiles),
            )

    logger.info(
        "Audio profiles complete: %d/%d artists have profiles (min_recordings=%d)",
        len(profiles),
        total,
        min_recordings,
    )
    return profiles


def build_audio_profiles_from_features(
    artist_features: dict[int, list[RecordingFeatures]],
    min_recordings: int = 3,
) -> dict[int, ArtistAudioProfile]:
    """Build audio profiles from pre-resolved per-artist feature lists.

    Used by the PG-based AcousticBrainz path where features are already
    grouped by artist from the JOIN query.

    Args:
        artist_features: Mapping of artist ID → list of RecordingFeatures.
        min_recordings: Minimum recordings required to create a profile.

    Returns:
        Dict mapping artist ID to ArtistAudioProfile.
    """
    profiles: dict[int, ArtistAudioProfile] = {}
    total = len(artist_features)

    for i, (artist_id, recordings) in enumerate(artist_features.items(), 1):
        if len(recordings) < min_recordings:
            continue

        profiles[artist_id] = ArtistAudioProfile.from_recordings(recordings)

        if i % 500 == 0:
            logger.info(
                "  Audio profiles: %d/%d artists processed, %d profiles built",
                i,
                total,
                len(profiles),
            )

    logger.info(
        "Audio profiles complete: %d/%d artists have profiles (min_recordings=%d)",
        len(profiles),
        total,
        min_recordings,
    )
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


def load_audio_profiles(conn: sqlite3.Connection) -> dict[int, ArtistAudioProfile]:
    """Load all persisted audio profiles from SQLite.

    Reconstructs ``ArtistAudioProfile`` objects from the ``audio_profile``
    table, deserializing the ``feature_centroid`` JSON column back to a
    list of floats for similarity computation.

    Args:
        conn: SQLite connection to the graph database.

    Returns:
        Dict mapping artist ID to ArtistAudioProfile.
    """
    rows = conn.execute(
        "SELECT artist_id, avg_danceability, primary_genre, "
        "primary_genre_probability, voice_instrumental_ratio, "
        "feature_centroid, recording_count FROM audio_profile"
    ).fetchall()
    return {
        row[0]: ArtistAudioProfile(
            recording_count=row[6],
            avg_danceability=row[1],
            primary_genre=row[2],
            primary_genre_probability=row[3],
            voice_instrumental_ratio=row[4],
            feature_centroid=json.loads(row[5]),
        )
        for row in rows
    }


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
    import time as _time

    conn.executescript(_ACOUSTIC_SIMILARITY_SCHEMA)

    artist_ids = sorted(profiles.keys())
    n = len(artist_ids)
    total_pairs = n * (n - 1) // 2
    edge_count = 0

    # Estimate runtime from a calibration batch (first 50 artists)
    t0 = _time.monotonic()
    calibration_size = min(50, n)
    calibration_pairs = 0
    for i in range(calibration_size):
        for j in range(i + 1, calibration_size):
            cosine_similarity(
                profiles[artist_ids[i]].feature_centroid,
                profiles[artist_ids[j]].feature_centroid,
            )
            calibration_pairs += 1
    calibration_elapsed = _time.monotonic() - t0

    if calibration_pairs > 0 and total_pairs > 0:
        rate = calibration_pairs / calibration_elapsed if calibration_elapsed > 0 else 0
        est_seconds = total_pairs / rate if rate > 0 else 0
        if est_seconds < 60:
            est_str = f"{est_seconds:.0f}s"
        else:
            est_str = f"{est_seconds / 60:.1f}m"
        logger.info(
            "Acoustic similarity: %d profiles, %s pairwise comparisons, estimated %s",
            n,
            f"{total_pairs:,}",
            est_str,
        )
    else:
        logger.info(
            "Acoustic similarity: %d profiles, %s pairwise comparisons", n, f"{total_pairs:,}"
        )

    t_start = _time.monotonic()
    for i, aid in enumerate(artist_ids):
        for j in range(i + 1, n):
            bid = artist_ids[j]
            sim = cosine_similarity(profiles[aid].feature_centroid, profiles[bid].feature_centroid)
            if sim >= threshold:
                conn.execute(
                    "INSERT OR REPLACE INTO acoustic_similarity "
                    "(artist_a_id, artist_b_id, similarity) VALUES (?, ?, ?)",
                    (aid, bid, sim),
                )
                edge_count += 1

        if (i + 1) % 500 == 0:
            elapsed = _time.monotonic() - t_start
            pct = (i + 1) / n * 100
            logger.info(
                "  Acoustic similarity: %d/%d artists (%.0f%%), %d edges, %.0fs elapsed",
                i + 1,
                n,
                pct,
                edge_count,
                elapsed,
            )

    elapsed = _time.monotonic() - t_start
    conn.commit()
    logger.info(
        "Acoustic similarity complete: %d edges (threshold=%.2f) in %.0fs",
        edge_count,
        threshold,
        elapsed,
    )
    return edge_count
