"""F155 — the SCD-specific steps carry `SCD` in their names; the old names still work, with a warning.

Data Vault steps are named by style (`DVHub…`, `DVSatellite…`), so the five steps that only make
sense for an SCD target say so too. Notebooks outside the repo import the old names; each keeps
resolving to the same class, and warns once per lookup with the name to use.
"""
from __future__ import annotations

import importlib
import warnings

import pytest

_RENAMED = {
    "TargetTableTransformer": "SCDTargetTableTransformer",
    "IncrementalTargetTableTransformer": "SCDIncrementalTargetTableTransformer",
    "ChangeDetectorTransformer": "SCDChangeDetectorTransformer",
    "GracefulChangeDetectorTransformer": "SCDGracefulChangeDetectorTransformer",
    "CandidateVersionGuardTransformer": "SCDCandidateVersionGuardTransformer",
}
# The module each name is defined in.
_HOMES = {
    "TargetTableTransformer": "spark_quick_start.orchestration.transformers.target",
    "IncrementalTargetTableTransformer": "spark_quick_start.orchestration.transformers.target",
    "ChangeDetectorTransformer": "spark_quick_start.orchestration.transformers.change_detector",
    "GracefulChangeDetectorTransformer": "spark_quick_start.orchestration.transformers.change_detector",
    "CandidateVersionGuardTransformer": "spark_quick_start.orchestration.transformers.candidate_version_guard",
}
_TRANSFORMERS = "spark_quick_start.orchestration.transformers"
_ORCHESTRATION = "spark_quick_start.orchestration"
# The packages that re-exported each old name before the rename; `orchestration` never exported
# IncrementalTargetTableTransformer, and does not start now (F155 renames, it does not widen).
_PACKAGES = {old: (_TRANSFORMERS,) if old == "IncrementalTargetTableTransformer"
             else (_TRANSFORMERS, _ORCHESTRATION) for old in _RENAMED}


@pytest.mark.parametrize("old,new", sorted(_RENAMED.items()))
def test_the_new_name_is_exported_and_the_old_one_is_not(old: str, new: str) -> None:
    for package in _PACKAGES[old]:
        module = importlib.import_module(package)
        assert new in module.__all__, f"{package} does not export {new}"
        assert old not in module.__all__, f"{package} still exports the deprecated {old}"


@pytest.mark.parametrize("old,new", sorted(_RENAMED.items()))
def test_the_old_name_resolves_to_the_new_class_with_a_warning(old: str, new: str) -> None:
    for where in (*_PACKAGES[old], _HOMES[old]):
        module = importlib.import_module(where)
        with pytest.warns(DeprecationWarning, match=rf"{old} is deprecated; use {new}\. The old name is removed in 0\.5\.0"):
            alias = getattr(module, old)
        assert alias is getattr(module, new) if hasattr(module, new) else alias.__name__ == new


@pytest.mark.parametrize("where", [_ORCHESTRATION, _HOMES["ChangeDetectorTransformer"]])
def test_a_from_import_warns_at_the_line_that_imports(where: str) -> None:
    """How notebooks use it. A package's `from … import` looks the name up twice
    (`_handle_fromlist`, then `IMPORT_FROM`), so it can warn twice, but every warning points
    at the importing line, and the default filters show one warning per location."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        namespace: dict[str, object] = {"__name__": "__main__"}
        exec(compile(f"from {where} import ChangeDetectorTransformer", "<cell>", "exec"), namespace)
    assert namespace["ChangeDetectorTransformer"].__name__ == "SCDChangeDetectorTransformer"  # type: ignore[attr-defined]
    assert caught, "no DeprecationWarning on a from-import"
    assert {(w.category, w.filename, w.lineno) for w in caught} == {(DeprecationWarning, "<cell>", 1)}


def test_the_new_classes_are_named_as_they_are_exported() -> None:
    """The stage name in `ExecutionResult.stages` is the class name, so it changes with it."""
    module = importlib.import_module(_TRANSFORMERS)
    for new in _RENAMED.values():
        assert getattr(module, new).__name__ == new


def test_an_unknown_name_still_raises_attribute_error() -> None:
    """Including an old name a package never exported: the alias does not widen the API."""
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        for package, name in ((_TRANSFORMERS, "NoSuchTransformer"),
                              (_ORCHESTRATION, "IncrementalTargetTableTransformer")):
            module = importlib.import_module(package)
            with pytest.raises(AttributeError, match=name):
                getattr(module, name)
