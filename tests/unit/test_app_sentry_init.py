"""Tests for Sentry + logging initialization in the Graph API factory."""

from __future__ import annotations

import json
import logging
from unittest.mock import patch

import pytest


def test_create_app_from_settings_initializes_sentry(monkeypatch) -> None:
    """``_create_app_from_settings`` calls ``init_sentry`` with the service name."""
    monkeypatch.setenv("SENTRY_DSN", "https://public@sentry.example.com/1")
    monkeypatch.setenv("SENTRY_ENVIRONMENT", "test")
    monkeypatch.setenv("SENTRY_RELEASE", "test-1.2.3")

    with (
        patch("semantic_index.api.app.init_sentry") as mock_init,
        patch("semantic_index.api.app.create_app"),
    ):
        from semantic_index.api.app import _create_app_from_settings

        _create_app_from_settings()

    mock_init.assert_called_once()
    kwargs = mock_init.call_args.kwargs
    assert kwargs["dsn"] == "https://public@sentry.example.com/1"
    assert kwargs["service_name"] == "semantic-index"
    assert kwargs["environment"] == "test"
    assert kwargs["release"] == "test-1.2.3"


def test_create_app_from_settings_passes_none_dsn_when_unset(monkeypatch) -> None:
    """Without ``SENTRY_DSN``, ``init_sentry`` is still called (it no-ops on None)."""
    monkeypatch.delenv("SENTRY_DSN", raising=False)
    monkeypatch.delenv("SENTRY_ENVIRONMENT", raising=False)
    monkeypatch.delenv("SENTRY_RELEASE", raising=False)

    with (
        patch("semantic_index.api.app.init_sentry") as mock_init,
        patch("semantic_index.api.app.create_app"),
    ):
        from semantic_index.api.app import _create_app_from_settings

        _create_app_from_settings()

    mock_init.assert_called_once()
    assert mock_init.call_args.kwargs["dsn"] is None
    assert mock_init.call_args.kwargs["service_name"] == "semantic-index"


def test_create_app_from_settings_calls_init_logger_before_sentry(monkeypatch) -> None:
    """The Graph API factory must call ``wxyc_etl.logger.init_logger`` before
    ``init_sentry`` so module loggers under ``semantic_index.*`` emit to stderr
    from the first line of process lifetime. Without this, ``sync_scheduler``
    and ``nightly_sync`` log calls vanish — the silent-sync-for-16-days root
    cause from WXYC/semantic-index#322.

    Ordering matters: ``init_logger`` installs the JSON handler on the root
    logger before any other component (Sentry, uvicorn) gets a chance to
    configure logging in a way that suppresses our handler.
    """
    monkeypatch.delenv("SENTRY_DSN", raising=False)

    call_order: list[str] = []
    with (
        patch("semantic_index.api.app.init_logger") as mock_logger,
        patch("semantic_index.api.app.init_sentry") as mock_sentry,
        patch("semantic_index.api.app.create_app"),
    ):
        mock_logger.side_effect = lambda *a, **kw: call_order.append("logger")
        mock_sentry.side_effect = lambda *a, **kw: call_order.append("sentry")

        from semantic_index.api.app import _create_app_from_settings

        _create_app_from_settings()

    mock_logger.assert_called_once()
    kwargs = mock_logger.call_args.kwargs or {}
    args_pos = mock_logger.call_args.args
    repo = kwargs.get("repo") or (args_pos[0] if args_pos else None)
    tool = kwargs.get("tool") or (args_pos[1] if len(args_pos) > 1 else None)
    assert repo == "semantic-index"
    assert tool == "semantic-index api"

    assert call_order == ["logger", "sentry"], (
        f"init_logger must run before init_sentry, got: {call_order}"
    )


def test_create_app_from_settings_emits_json_to_stderr(monkeypatch, capsys) -> None:
    """After ``_create_app_from_settings`` runs, any ``semantic_index.*``
    logger writes a JSON line on stderr. This is the end-to-end visibility
    contract that closes WXYC/semantic-index#322.
    """
    monkeypatch.delenv("SENTRY_DSN", raising=False)
    monkeypatch.delenv("SENTRY_ENVIRONMENT", raising=False)
    monkeypatch.delenv("SENTRY_RELEASE", raising=False)

    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
    from wxyc_etl import logger as wxyc_logger

    wxyc_logger._INITIALIZED = False

    with patch("semantic_index.api.app.create_app"):
        from semantic_index.api.app import _create_app_from_settings

        _create_app_from_settings()

    logging.getLogger("semantic_index.api.sync_scheduler").info(
        "scheduler started", extra={"step": "startup"}
    )

    captured = capsys.readouterr()
    json_lines = [line for line in captured.err.splitlines() if line.strip().startswith("{")]
    assert json_lines, (
        "expected at least one JSON log line from semantic_index.* logger after app init"
    )
    payload = json.loads(json_lines[-1])
    assert payload["repo"] == "semantic-index"
    assert payload["tool"] == "semantic-index api"
    assert payload["name"] == "semantic_index.api.sync_scheduler"
    assert payload["message"] == "scheduler started"


@pytest.fixture(autouse=True)
def _reset_logger_init_state():
    """Reset ``wxyc_etl.logger._INITIALIZED`` so each test can re-init cleanly."""
    from wxyc_etl import logger as wxyc_logger

    saved = wxyc_logger._INITIALIZED
    yield
    wxyc_logger._INITIALIZED = saved
