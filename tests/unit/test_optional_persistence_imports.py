"""Tests for the defensive persistence-adapter imports in ``spark_quick_start.spark_logging``.

``spark_logging/__init__.py`` imports each platform's persistence adapter inside
``try/except ImportError``: the name resolves to ``None`` and stays out of ``__all__`` when
the module is absent, as a platform-filtered build leaves it (F121, platform-boundary rule 4).
On a full checkout every module imports, so only a simulated absence reaches that branch.

The absence is simulated by setting the ``sys.modules`` entry to ``None`` and importing the
package afresh. The original module object is put back afterwards, not re-imported: other
modules hold references to its classes, and a second copy would split their identity.
"""
from __future__ import annotations

import importlib
import sys

import pytest

_PKG = "spark_quick_start.spark_logging"


@pytest.fixture
def restore_spark_logging():
    """Put the original package back, in sys.modules and on its parent, after the test."""
    import spark_quick_start

    original = importlib.import_module(_PKG)
    yield original
    sys.modules[_PKG] = original
    spark_quick_start.spark_logging = original


@pytest.mark.parametrize(
    ("module_suffix", "attr"),
    [
        ("fabric_persistence", "FabricPersistenceAdapter"),
        ("databricks_persistence", "DatabricksPersistenceAdapter"),
        ("local_persistence", "LocalFilePersistenceAdapter"),
    ],
)
def test_an_absent_persistence_adapter_is_none_and_left_out_of_all(
    module_suffix, attr, monkeypatch, restore_spark_logging
):
    """F121 — a stripped persistence module neither breaks the import nor reaches `__all__`."""
    monkeypatch.setitem(sys.modules, f"{_PKG}.{module_suffix}", None)
    monkeypatch.delitem(sys.modules, _PKG)

    pkg = importlib.import_module(_PKG)

    assert pkg is not restore_spark_logging, "the package was not imported afresh"
    assert getattr(pkg, attr) is None, f"{attr} should resolve to None when its module is absent"
    assert attr not in pkg.__all__, f"{attr} should be left out of __all__ when unavailable"
    others = {"FabricPersistenceAdapter", "DatabricksPersistenceAdapter", "LocalFilePersistenceAdapter"} - {attr}
    assert others <= set(pkg.__all__), "the adapters that did import must stay in __all__"
