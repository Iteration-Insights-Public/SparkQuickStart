"""The package refuses an interpreter below its floor with a message that says so (F191).

A source copy has no install step, so nothing applies `requires-python`; without this guard
a 3.10 interpreter failed at import with an unrelated-looking error (`cannot import name 'UTC'
from 'datetime'`). The dual-shipping rule asks for a clear message instead.

The package is imported afresh under a patched `sys.version_info`, then the original module
objects are put back, so the rest of the run keeps one copy of every class.
"""
from __future__ import annotations

import importlib
import sys

import pytest

_PKG = "spark_quick_start"


@pytest.fixture
def restore_package():
    saved = {name: mod for name, mod in sys.modules.items() if name == _PKG or name.startswith(_PKG + ".")}
    yield
    for name in [n for n in sys.modules if n == _PKG or n.startswith(_PKG + ".")]:
        del sys.modules[name]
    sys.modules.update(saved)


def test_an_interpreter_below_the_floor_is_refused_by_name(monkeypatch, restore_package):
    monkeypatch.setattr(sys, "version_info", (3, 10, 12, "final", 0))
    for name in [n for n in sys.modules if n == _PKG or n.startswith(_PKG + ".")]:
        monkeypatch.delitem(sys.modules, name)
    with pytest.raises(ImportError, match=r"requires Python 3\.11 or later; this interpreter is 3\.10"):
        importlib.import_module(_PKG)


def test_the_floor_itself_imports():
    assert importlib.import_module(_PKG).__name__ == _PKG
