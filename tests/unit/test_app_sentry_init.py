"""Tests for Sentry initialization in the Graph API factory."""

from __future__ import annotations

from unittest.mock import patch


def test_create_app_from_settings_initializes_sentry(monkeypatch, tmp_path) -> None:
    """``_create_app_from_settings`` calls ``init_sentry`` with the service name."""
    db_path = tmp_path / "graph.db"
    db_path.touch()
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("SENTRY_DSN", "https://public@sentry.example.com/1")
    monkeypatch.setenv("SENTRY_ENVIRONMENT", "test")
    monkeypatch.setenv("SENTRY_RELEASE", "test-1.2.3")

    with patch("semantic_index.api.app.init_sentry") as mock_init:
        from semantic_index.api.app import _create_app_from_settings

        _create_app_from_settings()

    mock_init.assert_called_once()
    kwargs = mock_init.call_args.kwargs
    assert kwargs["dsn"] == "https://public@sentry.example.com/1"
    assert kwargs["service_name"] == "semantic-index"
    assert kwargs["environment"] == "test"
    assert kwargs["release"] == "test-1.2.3"


def test_create_app_from_settings_passes_none_dsn_when_unset(monkeypatch, tmp_path) -> None:
    """Without ``SENTRY_DSN``, ``init_sentry`` is still called (it no-ops on None)."""
    db_path = tmp_path / "graph.db"
    db_path.touch()
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.delenv("SENTRY_DSN", raising=False)
    monkeypatch.delenv("SENTRY_ENVIRONMENT", raising=False)
    monkeypatch.delenv("SENTRY_RELEASE", raising=False)

    with patch("semantic_index.api.app.init_sentry") as mock_init:
        from semantic_index.api.app import _create_app_from_settings

        _create_app_from_settings()

    mock_init.assert_called_once()
    assert mock_init.call_args.kwargs["dsn"] is None
    assert mock_init.call_args.kwargs["service_name"] == "semantic-index"
