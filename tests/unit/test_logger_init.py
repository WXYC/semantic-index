"""Smoke tests for wxyc_etl.logger wireup in semantic-index entrypoints."""

from __future__ import annotations

import json
import logging
from unittest.mock import patch

import pytest


def test_logger_init_emits_json_with_repo_tag(monkeypatch, capsys):
    """init_logger produces a JSON line tagged with repo='semantic-index'."""
    monkeypatch.delenv("SENTRY_DSN", raising=False)

    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)

    from wxyc_etl import logger as wxyc_logger

    wxyc_logger._INITIALIZED = False
    wxyc_logger.init_logger(repo="semantic-index", tool="semantic-index test")
    logging.getLogger("semantic_index.test").info("smoke", extra={"step": "smoke"})

    captured = capsys.readouterr()
    line = next(line for line in captured.err.splitlines() if line.strip().startswith("{"))
    payload = json.loads(line)
    assert payload["repo"] == "semantic-index"
    assert payload["tool"] == "semantic-index test"
    assert payload["step"] == "smoke"
    assert payload["message"] == "smoke"


def test_run_pipeline_main_calls_init_logger():
    """run_pipeline.main() invokes wxyc_etl.logger.init_logger with the
    semantic-index repo tag before any pipeline work."""
    import run_pipeline

    with (
        patch.object(run_pipeline, "init_logger") as mock_init,
        patch.object(run_pipeline, "run") as mock_run,
    ):
        run_pipeline.main(["dump.sql"])

    mock_init.assert_called_once()
    kwargs = mock_init.call_args.kwargs or {}
    args_pos = mock_init.call_args.args
    repo = kwargs.get("repo") or (args_pos[0] if args_pos else None)
    assert repo == "semantic-index"
    mock_run.assert_called_once()


def test_nightly_sync_main_calls_init_logger():
    """nightly_sync.main() invokes wxyc_etl.logger.init_logger with the
    semantic-index repo tag before running."""
    from semantic_index import nightly_sync

    with (
        patch.object(nightly_sync, "init_logger") as mock_init,
        patch.object(nightly_sync, "nightly_sync") as mock_run,
    ):
        with pytest.raises(SystemExit):
            nightly_sync.main([])  # missing --dsn -> exit 1, after init_logger

    mock_init.assert_called_once()
    kwargs = mock_init.call_args.kwargs or {}
    args_pos = mock_init.call_args.args
    repo = kwargs.get("repo") or (args_pos[0] if args_pos else None)
    assert repo == "semantic-index"
    mock_run.assert_not_called()
