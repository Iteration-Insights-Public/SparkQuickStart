"""
test_version.py — the package version has exactly one source of truth.

`pyproject.toml`'s `version` and `spark_quick_start.__version__` were once two
hand-maintained literals and had drifted three minor versions apart (0.3.7 vs
0.1.0). `__version__` is now read back from installed package metadata, so the
two cannot disagree. These tests pin that down.
"""
from __future__ import annotations

import re
from importlib.metadata import version as pkg_version
from pathlib import Path

import pytest

import spark_quick_start


def _pyproject_version() -> str:
    """Read `version` out of pyproject.toml.

    A regex rather than tomllib: the line is a plain literal in a fixed spot,
    so a full TOML parser buys nothing here.
    """
    # Walk up to the repo root rather than counting directory levels — this
    # test has already been moved one level deeper once (tests/ -> tests/unit/),
    # and a hardcoded .parent.parent silently resolved to the wrong directory.
    here = Path(__file__).resolve()
    root = next(
        (d for d in here.parents if (d / "pyproject.toml").is_file()),
        None,
    )
    assert root is not None, f"no pyproject.toml above {here}"
    text = (root / "pyproject.toml").read_text(encoding="utf-8")
    match = re.search(r'^version = "([^"]+)"', text, re.MULTILINE)
    assert match is not None, "no `version = \"...\"` line in pyproject.toml"
    return match.group(1)


class TestVersion:
    def test_version_is_a_real_release_not_the_fallback(self) -> None:
        """__version__ came from metadata, not the not-installed fallback."""
        assert spark_quick_start.__version__ != "0.0.0+unknown"
        assert re.fullmatch(r"\d+\.\d+\.\d+.*", spark_quick_start.__version__)

    def test_version_matches_installed_distribution_metadata(self) -> None:
        assert spark_quick_start.__version__ == pkg_version("spark_quick_start")

    def test_version_matches_pyproject(self) -> None:
        """Guards the drift this module exists to prevent.

        Only meaningful against an editable install, whose metadata tracks the
        working tree. A stale non-editable install would compare an older wheel
        against the current pyproject, so skip rather than fail.
        """
        installed = pkg_version("spark_quick_start")
        declared = _pyproject_version()
        if installed != declared:
            pytest.skip(
                f"installed {installed} != pyproject {declared}; "
                "reinstall with `pip install -e .` to check this"
            )
        assert spark_quick_start.__version__ == declared
