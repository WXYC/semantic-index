"""Guards against the Railway-era deploy path being reintroduced.

The Railway deployment was retired in 2026-09 (WXYC/semantic-index#389); EC2
via .github/workflows/deploy.yml is the sole deploy path now.
"""

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

FORBIDDEN_STRINGS = (
    "railway ssh",
    "railway redeploy",
    "media.githubusercontent.com/media/WXYC/semantic-index",
)

SCAN_DIRS = ("scripts", "deploy", "infra", ".github")


def _tracked_files_under(directory: str) -> list[Path]:
    import subprocess

    result = subprocess.run(
        ["git", "ls-files", directory],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return [REPO_ROOT / line for line in result.stdout.splitlines() if line]


def test_railway_deploy_script_removed():
    assert not (REPO_ROOT / "scripts" / "deploy.sh").exists()


def test_railway_cron_sync_script_removed():
    assert not (REPO_ROOT / "scripts" / "cron_sync.sh").exists()


@pytest.mark.parametrize("directory", SCAN_DIRS)
def test_no_railway_deploy_strings_in_tracked_files(directory):
    for path in _tracked_files_under(directory):
        if not path.is_file():
            continue
        try:
            text = path.read_text()
        except (UnicodeDecodeError, OSError):
            continue
        for forbidden in FORBIDDEN_STRINGS:
            assert forbidden not in text, f"{forbidden!r} found in {path.relative_to(REPO_ROOT)}"
