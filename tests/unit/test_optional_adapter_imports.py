"""Tests for the defensive adapter imports in ``spark_quick_start.adapters``.

``adapters/__init__.py`` imports each concrete adapter inside ``try/except
ImportError``, resolving the name to ``None`` and dropping it from ``__all__``
when the module is absent. That is not defensive padding: a source-copy
deployment may have adapter files removed (``.claude/rules/dual-shipping.md``).
A plain import there would look correct in every local suite and break on
first import of that copy.

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
    """The trunk ships every adapter, and the negative tests above must not be vacuous.

    Fails, never skips, when an adapter file is missing from the checkout (F218). The repo is
    never a platform-filtered tree any more, so a missing file is a deletion, the August 2026
    failure that `verify_trunk_complete.sh` caught before it was removed.

    Read from the source tree rather than from the imported package, deliberately:
    `pkg.DatabricksAdapter is None` is also what a genuine import failure looks like, and
    the two failures must be told apart.
    """
    here = Path(__file__).resolve()
    root = next(p for p in here.parents if (p / "pyproject.toml").is_file())
    adapters_dir = root / "src" / "spark_quick_start" / "adapters"
    missing = [
        name for name in ("fabric_adapter.py", "databricks_adapter.py", "local_adapter.py")
        if not (adapters_dir / name).is_file()
    ]
    assert not missing, f"adapter file(s) missing from the trunk: {missing}"

    sys.modules.pop(_PKG, None)
    pkg = importlib.import_module(_PKG)

    for attr in ("FabricAdapter", "DatabricksAdapter", "LocalAdapter"):
        assert getattr(pkg, attr) is not None
        assert attr in pkg.__all__
