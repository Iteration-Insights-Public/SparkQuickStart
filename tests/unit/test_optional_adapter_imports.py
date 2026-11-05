"""Tests for the defensive adapter imports in ``spark_quick_start.adapters``.

``adapters/__init__.py`` imports each concrete adapter inside ``try/except
ImportError``, resolving the name to ``None`` and dropping it from ``__all__``
when the module is absent. That is not defensive padding: platform-filtered
release builds physically delete adapter files (see
``scripts/filter_platform_release.sh``), and the same holds for a source-copy
deployment made from one. A plain import there would look correct in every
local suite and break on first import of the shipped Fabric or Databricks wheel.

Missing modules are simulated by setting the ``sys.modules`` entry to ``None``,
which makes Python raise ``ImportError`` for that name, then re-importing the
package.
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

_PKG = "spark_quick_start.adapters"


@pytest.fixture
def reimport_adapters():
    """Re-import the adapters package cleanly after a test has mangled it."""
    yield
    sys.modules.pop(_PKG, None)
    importlib.import_module(_PKG)


@pytest.mark.parametrize(
    ("module_suffix", "attr"),
    [
        ("fabric_adapter", "FabricAdapter"),
        ("databricks_adapter", "DatabricksAdapter"),
        ("local_adapter", "LocalAdapter"),
    ],
)
def test_absent_adapter_resolves_to_none_and_leaves_all(
    module_suffix, attr, monkeypatch, reimport_adapters
):
    """A stripped adapter module must not break importing the package."""
    monkeypatch.setitem(sys.modules, f"{_PKG}.{module_suffix}", None)
    monkeypatch.delitem(sys.modules, _PKG, raising=False)

    pkg = importlib.import_module(_PKG)

    assert getattr(pkg, attr) is None, f"{attr} should resolve to None when its module is absent"
    assert attr not in pkg.__all__, f"{attr} should be dropped from __all__ when unavailable"
    # The ABC and the test double are platform-neutral and always present.
    assert "PlatformAdapter" in pkg.__all__
    assert "MockPlatformAdapter" in pkg.__all__


def test_all_adapters_present_in_a_full_checkout(reimport_adapters):
    """The trunk ships every adapter — the negative tests above must not be vacuous.

    Skips on a platform-filtered checkout, where the assertion is false by construction:
    `scripts/filter_platform_release.sh` physically deletes the other platform's adapter,
    so a fabric-only tree has no `DatabricksAdapter` to find and this test asserted the
    opposite of what that tree is.

    That mattered. `platform-builds.yaml` filters a checkout for each platform and runs the
    whole suite on it, with the stated purpose of catching breakage before it reaches a release
    branch — and this test failed it, for both platforms, on every PR at the time, when the PR
    workflow still ran those builds, for an unrelated reason. A job that is always red proves nothing about the thing it exists to check
    (F16).

    Detected from the source tree rather than from the imported package, deliberately:
    `pkg.DatabricksAdapter is None` is also what a genuine import failure looks like, and
    conflating the two is how this test would stop failing for the right reasons too.
    The sibling test above covers the absent-adapter path, so nothing is lost on a
    filtered tree; `tests/architecture/test_release_filter.py` and
    `scripts/verify_trunk_complete.sh` both assert trunk completeness independently.
    """
    here = Path(__file__).resolve()
    root = next(p for p in here.parents if (p / "pyproject.toml").is_file())
    adapters_dir = root / "src" / "spark_quick_start" / "adapters"
    stripped = [
        name for name in ("fabric_adapter.py", "databricks_adapter.py")
        if not (adapters_dir / name).is_file()
    ]
    if stripped:
        pytest.skip(
            f"platform-filtered checkout — {stripped} deleted by "
            "scripts/filter_platform_release.sh, so a complete trunk is not what this "
            "tree is"
        )

    sys.modules.pop(_PKG, None)
    pkg = importlib.import_module(_PKG)

    for attr in ("FabricAdapter", "DatabricksAdapter", "LocalAdapter"):
        assert getattr(pkg, attr) is not None
        assert attr in pkg.__all__
