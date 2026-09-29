#!/usr/bin/env python3
"""Disk-space and stale-artifact preflight checks for the nightly rebuild conductor.

``scripts/ec2-build-conductor.sh`` (#347) writes two database-sized files to
local disk every run (the live-DB snapshot and the downloaded build artifact)
without checking for room first, so a near-full host failed closed with a raw
``sqlite3.OperationalError: database or disk is full`` instead of a clear,
alertable message -- and a run killed before its exit trap ran left its
working files behind to compound the next night's shortfall (#385).

Plain Python, not bash, so it's unit-testable the way ``validate_graph_db.py``
and ``run_build_job.py`` already are; the conductor shells out to it the same
way. Two CLI subcommands:

``check-space``
    Compare a caller-supplied payload size (``stat`` on the live DB, or
    ``aws s3api head-object`` on the build artifact -- the conductor already
    knows both without downloading anything) against free space on the target
    filesystem, plus a margin that scales with the payload rather than a fixed
    constant. Fails loud, naming both numbers.

``clear-stale``
    Remove the conductor's own leftover working files from a run that never
    reached its exit trap. Scoped tightly to the conductor's own
    ``.seed``/``.incoming``/``.preprune-*`` naming -- NOT a blanket "anything
    after the db name" sweep, because the API's permanent cache sidecars
    (``semantic_index/api/database.py``'s ``f".{suffix}-cache.db"`` --
    narrative-cache.db, bio-cache.db, preview-cache.db) share that exact
    dotted-suffix shape and must never be swept up here.
"""

from __future__ import annotations

import argparse
import math
import shutil
import sys
from pathlib import Path

DEFAULT_MARGIN = 0.1  # 10% headroom beyond the known payload size

# Roots the conductor itself ever writes under DATA_DIR (see PROD_DB/SEED_DB/
# INCOMING_DB in ec2-build-conductor.sh), each of which may also leave SQLite
# -wal/-shm companions behind if a snapshot or validation step opened it.
_STALE_ROOTS = ("seed", "incoming")
_STALE_EXTRA_SUFFIXES = (".seed-counts.json",)


class InsufficientDiskSpaceError(RuntimeError):
    """Raised by check_free_space when the target filesystem lacks headroom."""


def _human(num_bytes: float) -> str:
    """Render a byte count as a short human-readable string (e.g. '1.4 GB')."""
    size = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def required_bytes(payload_bytes: int, margin: float = DEFAULT_MARGIN) -> int:
    """Free space required for a payload of ``payload_bytes``, plus ``margin``.

    ``margin`` is a fraction of the known payload, not a fixed magic constant --
    the threshold this returns is always sized to the actual footprint the
    conductor is about to write.
    """
    return math.ceil(payload_bytes * (1 + margin))


def check_free_space(
    target: str | Path,
    payload_bytes: int,
    step: str,
    margin: float = DEFAULT_MARGIN,
) -> None:
    """Raise :class:`InsufficientDiskSpaceError` unless ``target``'s filesystem
    has at least :func:`required_bytes` free for ``payload_bytes``.
    """
    _total, _used, free = shutil.disk_usage(target)
    needed = required_bytes(payload_bytes, margin)
    if free < needed:
        raise InsufficientDiskSpaceError(
            f"insufficient free space for {step} step on {target}: need "
            f"{_human(needed)} ({_human(payload_bytes)} payload + {margin:.0%} "
            f"margin), only {_human(free)} available"
        )


def find_stale_artifacts(data_dir: Path, db_name: str) -> list[Path]:
    """Return this conductor's own leftover working files under ``data_dir``.

    Matches ``{db_name}.seed`` / ``{db_name}.incoming`` (and their SQLite
    ``-wal`` / ``-shm`` companions), ``{db_name}.seed-counts.json``, and the
    generalized ``{db_name}.preprune-*`` one-off-backup shape (the exact
    pattern of the 3.1 GB orphan from #385). Deliberately narrow, not a
    blanket ``{db_name}.*`` glob: the API's permanent cache sidecars use the
    identical dotted-suffix shape and must never match here.
    """
    names = [f"{db_name}.{root}{tail}" for root in _STALE_ROOTS for tail in ("", "-wal", "-shm")]
    names += [f"{db_name}{suffix}" for suffix in _STALE_EXTRA_SUFFIXES]
    found = [data_dir / name for name in names if (data_dir / name).exists()]
    found.extend(sorted(data_dir.glob(f"{db_name}.preprune-*")))
    return found


def clear_stale_artifacts(data_dir: Path, db_name: str) -> list[tuple[Path, int]]:
    """Remove every :func:`find_stale_artifacts` result; return ``[(path, size)]``."""
    removed = []
    for path in find_stale_artifacts(data_dir, db_name):
        size = path.stat().st_size
        path.unlink()
        removed.append((path, size))
    return removed


def _cmd_check_space(args: argparse.Namespace) -> int:
    try:
        check_free_space(args.path, args.needed_bytes, args.step, args.margin)
    except InsufficientDiskSpaceError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(f"OK: sufficient free space for {args.step} ({_human(args.needed_bytes)} payload)")
    return 0


def _cmd_clear_stale(args: argparse.Namespace) -> int:
    removed = clear_stale_artifacts(Path(args.data_dir), args.db_name)
    if not removed:
        print("no stale artifacts found")
        return 0
    for path, size in removed:
        print(f"removed stale artifact: {path} ({_human(size)})")
    total = sum(size for _, size in removed)
    print(f"reclaimed {_human(total)} from {len(removed)} file(s)")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_space = sub.add_parser(
        "check-space",
        help="Fail if the target filesystem lacks room for a known payload size",
    )
    p_space.add_argument("--path", required=True, help="Directory on the filesystem to check")
    p_space.add_argument(
        "--needed-bytes", type=int, required=True, help="Known payload size in bytes"
    )
    p_space.add_argument("--step", required=True, help="Conductor step name (for the message)")
    p_space.add_argument("--margin", type=float, default=DEFAULT_MARGIN)
    p_space.set_defaults(func=_cmd_check_space)

    p_clear = sub.add_parser(
        "clear-stale",
        help="Remove the conductor's own leftover artifacts from a prior run",
    )
    p_clear.add_argument("--data-dir", required=True)
    p_clear.add_argument("--db-name", required=True)
    p_clear.set_defaults(func=_cmd_clear_stale)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
