"""S3 archive client for WXYC hourly audio recordings.

Downloads hourly MP3 files from the ``wxyc-archive`` S3 bucket, decodes
them to PCM WAV via ffmpeg, and extracts audio segments at specified offsets.
Archive files are organized as ``YYYY/MM/DD/YYYYMMDDHH00.mp3``.

The :func:`timestamp_to_s3_key` helper converts a UTC datetime to the
corresponding S3 object key. Search window computation (:func:`compute_search_windows`,
:func:`merge_overlapping_windows`) uses flowsheet timestamps to narrow
the audio region that needs fingerprinting.
"""

from __future__ import annotations

import logging
import subprocess
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

HOUR_DURATION_MS = 3_600_000
DEFAULT_WINDOW_HALF_WIDTH_MS = 300_000  # ± 5 minutes


@dataclass
class SearchWindow:
    """A contiguous time range within an archive hour to fingerprint.

    Attributes:
        start_ms: Start offset within the hour file (milliseconds).
        end_ms: End offset within the hour file (milliseconds).
        play_ids: Flowsheet entry IDs that contributed to this window.
    """

    start_ms: int
    end_ms: int
    play_ids: list[int] = field(default_factory=list)

    @property
    def duration_ms(self) -> int:
        return self.end_ms - self.start_ms


def timestamp_to_s3_key(ts: datetime) -> str:
    """Convert a UTC datetime to the S3 object key for its archive hour.

    Args:
        ts: UTC datetime of a flowsheet entry.

    Returns:
        S3 key like ``2020/03/14/202003141900.mp3``.
    """
    return ts.strftime("%Y/%m/%d/%Y%m%d%H00.mp3")


def compute_search_windows(
    play_offsets_ms: list[int],
    window_half_width_ms: int = DEFAULT_WINDOW_HALF_WIDTH_MS,
    hour_duration_ms: int = HOUR_DURATION_MS,
    play_ids: list[int] | None = None,
) -> list[SearchWindow]:
    """Compute search windows centered on flowsheet play offsets.

    Each play offset (milliseconds from the start of the hour) gets a
    window of ``offset ± window_half_width_ms``, clamped to
    ``[0, hour_duration_ms]``.

    Args:
        play_offsets_ms: Offsets within the hour for each play entry.
        window_half_width_ms: Half-width of the search window.
        hour_duration_ms: Total duration of the hour file.
        play_ids: Optional parallel list of play IDs (same length as offsets).

    Returns:
        List of :class:`SearchWindow` instances, one per play entry.
    """
    if play_ids is None:
        play_ids = list(range(len(play_offsets_ms)))

    windows = []
    for offset, pid in zip(play_offsets_ms, play_ids, strict=True):
        start = max(0, offset - window_half_width_ms)
        end = min(hour_duration_ms, offset + window_half_width_ms)
        windows.append(SearchWindow(start_ms=start, end_ms=end, play_ids=[pid]))
    return windows


def merge_overlapping_windows(windows: list[SearchWindow]) -> list[SearchWindow]:
    """Merge overlapping or adjacent search windows.

    Args:
        windows: List of search windows, need not be sorted.

    Returns:
        Sorted, non-overlapping list of merged windows.
    """
    if not windows:
        return []

    sorted_windows = sorted(windows, key=lambda w: w.start_ms)
    merged: list[SearchWindow] = [
        SearchWindow(
            start_ms=sorted_windows[0].start_ms,
            end_ms=sorted_windows[0].end_ms,
            play_ids=list(sorted_windows[0].play_ids),
        )
    ]

    for w in sorted_windows[1:]:
        prev = merged[-1]
        if w.start_ms <= prev.end_ms:
            prev.end_ms = max(prev.end_ms, w.end_ms)
            prev.play_ids.extend(w.play_ids)
        else:
            merged.append(
                SearchWindow(
                    start_ms=w.start_ms,
                    end_ms=w.end_ms,
                    play_ids=list(w.play_ids),
                )
            )

    return merged


class ArchiveClient:
    """S3 client for downloading and processing WXYC archive audio.

    Downloads hourly MP3 files from S3, decodes them to PCM WAV, and
    extracts audio segments at specified offsets using ffmpeg.

    Args:
        bucket: S3 bucket name.
        temp_dir: Directory for temporary files. Uses system temp if None.
    """

    def __init__(self, bucket: str = "wxyc-archive", temp_dir: str | None = None) -> None:
        import boto3

        self._s3 = boto3.client("s3")
        self._bucket = bucket
        self._temp_dir = temp_dir

    def download_hour(self, s3_key: str) -> Path:
        """Download an hourly archive MP3 from S3.

        Args:
            s3_key: S3 object key (e.g. ``2020/03/14/202003141900.mp3``).

        Returns:
            Path to the downloaded MP3 file. Caller is responsible for cleanup.

        Raises:
            botocore.exceptions.ClientError: If the S3 key does not exist.
        """
        suffix = Path(s3_key).suffix
        fd = tempfile.NamedTemporaryFile(suffix=suffix, dir=self._temp_dir, delete=False)
        local_path = Path(fd.name)
        fd.close()

        logger.info("Downloading s3://%s/%s -> %s", self._bucket, s3_key, local_path)
        self._s3.download_file(self._bucket, s3_key, str(local_path))
        return local_path

    @staticmethod
    def decode_to_wav(mp3_path: Path, sample_rate: int = 16000) -> Path:
        """Decode an MP3 file to mono PCM WAV using ffmpeg.

        Args:
            mp3_path: Path to the input MP3 file.
            sample_rate: Output sample rate in Hz (default 16000 for Chromaprint).

        Returns:
            Path to the output WAV file (same directory as input, ``.wav`` suffix).

        Raises:
            subprocess.CalledProcessError: If ffmpeg fails.
        """
        wav_path = mp3_path.with_suffix(".wav")
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            str(mp3_path),
            "-ac",
            "1",
            "-ar",
            str(sample_rate),
            "-f",
            "wav",
            str(wav_path),
        ]
        logger.debug("Decoding: %s", " ".join(cmd))
        subprocess.run(cmd, check=True, capture_output=True)
        return wav_path

    @staticmethod
    def extract_segment(wav_path: Path, offset_ms: int, duration_ms: int) -> Path:
        """Extract an audio segment from a WAV file using ffmpeg.

        Args:
            wav_path: Path to the source WAV file.
            offset_ms: Start offset in milliseconds.
            duration_ms: Duration in milliseconds.

        Returns:
            Path to the extracted segment WAV file.

        Raises:
            subprocess.CalledProcessError: If ffmpeg fails.
        """
        segment_path = wav_path.parent / f"segment_{offset_ms}_{duration_ms}.wav"
        offset_s = offset_ms / 1000.0
        duration_s = duration_ms / 1000.0
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            str(wav_path),
            "-ss",
            f"{offset_s:.3f}",
            "-t",
            f"{duration_s:.3f}",
            "-c",
            "copy",
            str(segment_path),
        ]
        logger.debug("Extracting segment: %s", " ".join(cmd))
        subprocess.run(cmd, check=True, capture_output=True)
        return segment_path
