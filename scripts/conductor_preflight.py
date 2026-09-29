#!/usr/bin/env python3
"""Disk-space and stale-artifact preflight checks for the nightly rebuild conductor.

``scripts/ec2-build-conductor.sh`` (#347) writes two database-sized files to
local disk every run (the live-DB snapshot and the downloaded build artifact)
without checking for room first, so a near-full host failed closed with a raw
``sqlite3.OperationalError: database or disk is full`` instead of a clear,
alertable message -- and a run killed before its exit trap ran left its
working files behind to compound the next night's shortfall (#385).

Plain Python, not bash, so it's unit-testable the way ``validate_graph_db.py``
and ``run_build_job.py`` already are. Unlike those, this one runs with the
HOST's own ``python3`` rather than through the conductor's ``in_image`` docker
wrapper: it needs only the standard library, and `in_image` runs whatever
image tag happens to be on the host (which can be a stale build that predates
this script entirely, or fail outright on its own at 100% disk before a check
can even speak -- see #385, #371). Two CLI subcommands:

``check-space``
    Compare a caller-supplied payload size (``stat`` on the live DB, or
    ``aws s3api head-object`` on the build artifact -- the conductor already
    knows both without downloading anything) against free space on the target
    filesystem, plus a margin that scales with the payload rather than a fixed
    constant. Fails loud, naming both numbers.

``clear-stale``
    Remove the conductor's OWN leftover working files from a run that never
    reached its exit trap: the ``.seed``/``.incoming`` shapes (and their
    SQLite ``-wal``/``-shm`` companions) and ``.seed-counts.json``. Deliberately
    narrow, not a blanket "anything after the db name" sweep -- the API's
    permanent cache sidecars (``semantic_index/api/database.py``'s
    ``f".{suffix}-cache.db"`` -- narrative-cache.db, bio-cache.db,
    preview-cache.db) share that exact dotted-suffix shape and must never be
    swept up here. Separately, a ``.preprune-*`` file is a deliberate OPERATOR
    backup -- nothing in this repo ever creates one -- so it is only detected
    and loudly flagged, never auto-deleted (data-safety policy).

Exit codes (the conductor's bash checks these, not just "nonzero"):
    0   success
    1   an expected, actionable failure -- insufficient disk space, or a
        stale artifact could not be removed. The printed message says which.
    2   CLI usage error (argparse's own default) -- never this module's
        message, so the conductor must not relabel it.
    3   an unexpected/internal failure (the CLI boundary catch-all).
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

EXIT_OK = 0
EXIT_ACTIONABLE_FAILURE = 1
EXIT_UNEXPECTED_FAILURE = 3


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


def _human_and_raw(num_bytes: int) -> str:
    """Human-readable size plus the exact raw byte count, e.g. '1.4 GB (1503238553 bytes)'.

    The raw count is what an operator greps a log for; the human-readable form
    is what they read at a glance. Neither alone is enough (#385 review).
    """
    return f"{_human(num_bytes)} ({num_bytes} bytes)"


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

    Any other failure (e.g. ``target`` doesn't exist) propagates as-is -- it is
    not disk-space-related and must not be reported as if it were.
    """
    _total, _used, free = shutil.disk_usage(target)
    needed = required_bytes(payload_bytes, margin)
    if free < needed:
        raise InsufficientDiskSpaceError(
            f"insufficient free space for {step} step on {target}: need "
            f"{_human_and_raw(needed)} ({_human_and_raw(payload_bytes)} payload + "
            f"{margin:.0%} margin), only {_human_and_raw(free)} available"
        )


def find_stale_artifacts(data_dir: Path, db_name: str) -> list[Path]:
    """Return this conductor's own leftover working files under ``data_dir``.

    Matches ``{db_name}.seed`` / ``{db_name}.incoming`` (and their SQLite
    ``-wal`` / ``-shm`` companions) and ``{db_name}.seed-counts.json`` --
    exactly the shapes the conductor itself creates. Deliberately narrow, not
    a blanket ``{db_name}.*`` glob: the API's permanent cache sidecars use the
    identical dotted-suffix shape and must never match here. Does NOT include
    ``{db_name}.preprune-*`` -- see :func:`find_operator_backups`.
    """
    names = [f"{db_name}.{root}{tail}" for root in _STALE_ROOTS for tail in ("", "-wal", "-shm")]
    names += [f"{db_name}{suffix}" for suffix in _STALE_EXTRA_SUFFIXES]
    return [data_dir / name for name in names if (data_dir / name).exists()]


def find_operator_backups(data_dir: Path, db_name: str) -> list[Path]:
    """Return deliberate operator-made backups under ``data_dir``.

    The ``{db_name}.preprune-<timestamp>`` shape (the 3.1 GB orphan from #385)
    is never created by anything in this repo -- it's a manual, one-off backup
    a human made on purpose. Per the data-safety policy, this is detected and
    loudly flagged (see the ``clear-stale`` CLI command), never auto-deleted.
    """
    return sorted(data_dir.glob(f"{db_name}.preprune-*"))


def clear_stale_artifacts(
    data_dir: Path, db_name: str
) -> tuple[list[tuple[Path, int]], list[tuple[Path, OSError]]]:
    """Remove every :func:`find_stale_artifacts` result.

    Returns ``(removed, errors)``: ``removed`` is ``[(path, size)]`` for each
    file actually deleted, ``errors`` is ``[(path, exception)]`` for any that
    could not be removed (e.g. a directory sitting where a file was expected).
    One bad entry is reported, not raised -- it must not abort the rest of the
    sweep or crash with a traceback the conductor's log can't attribute.
    """
    removed: list[tuple[Path, int]] = []
    errors: list[tuple[Path, OSError]] = []
    for path in find_stale_artifacts(data_dir, db_name):
        try:
            size = path.stat().st_size
            path.unlink()
        except OSError as exc:
            errors.append((path, exc))
            continue
        removed.append((path, size))
    return removed, errors


def _cmd_check_space(args: argparse.Namespace) -> int:
    try:
        check_free_space(args.path, args.needed_bytes, args.step, args.margin)
    except InsufficientDiskSpaceError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return EXIT_ACTIONABLE_FAILURE
    print(
        f"OK: sufficient free space for {args.step} ({_human_and_raw(args.needed_bytes)} payload)"
    )
    return EXIT_OK


def _cmd_clear_stale(args: argparse.Namespace) -> int:
    data_dir = Path(args.data_dir)
    removed, errors = clear_stale_artifacts(data_dir, args.db_name)

    if not removed and not errors:
        print("no stale artifacts found")
    for path, size in removed:
        print(f"removed stale artifact: {path} ({_human_and_raw(size)})")
    if removed:
        total = sum(size for _, size in removed)
        print(f"reclaimed {_human_and_raw(total)} from {len(removed)} file(s)")
    for path, exc in errors:
        print(f"ERROR: could not remove stale artifact {path}: {exc}", file=sys.stderr)

    for path in find_operator_backups(data_dir, args.db_name):
        size = path.stat().st_size
        print(
            f"WARNING: operator backup present and NOT removed automatically "
            f"(see WXYC/semantic-index#385): {path} ({_human_and_raw(size)})",
            file=sys.stderr,
        )

    return EXIT_ACTIONABLE_FAILURE if errors else EXIT_OK


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

    # parse_args raises SystemExit(2) on a usage error (missing/bad flags) --
    # let that propagate untouched. It must never be relabeled by the caller as
    # one of this module's own domain failures.
    args = parser.parse_args(argv)

    try:
        return args.func(args)
    except Exception as exc:
        # CLI boundary of last resort: an exception here is a bug or an
        # environmental surprise (e.g. an unreadable path), not the specific
        # "insufficient disk space" or "couldn't remove a file" conditions
        # the subcommands already handle and report clearly themselves.
        print(f"ERROR: unexpected failure running '{args.command}': {exc}", file=sys.stderr)
        return EXIT_UNEXPECTED_FAILURE


if __name__ == "__main__":
    sys.exit(main())
