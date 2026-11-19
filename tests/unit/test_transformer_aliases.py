"""F155, F235 — the SCD-specific steps carry `SCD` in their names, and the old names are gone.

Data Vault steps are named by style (`DVHub…`, `DVSatellite…`), so the five steps that only make
sense for an SCD target say so too. F155 kept the old names importable, with a warning, until a
removal release; F235 removed them in 0.6.0, so an old name fails as any missing name does.
"""
from __future__ import annotations

import importlib

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


@pytest.mark.parametrize("old", sorted(_RENAMED))
def test_the_old_name_no_longer_resolves(old: str) -> None:
    """Neither as an attribute of the module it was defined in or a package that exported it,
    nor by a from-import, which is how notebooks used it."""
    for where in (*_PACKAGES[old], _HOMES[old]):
        module = importlib.import_module(where)
        with pytest.raises(AttributeError, match=old):
            getattr(module, old)
        with pytest.raises(ImportError, match=old):
            exec(f"from {where} import {old}", {"__name__": "__main__"})


def test_the_new_classes_are_named_as_they_are_exported() -> None:
    """The stage name in `ExecutionResult.stages` is the class name, so it changes with it."""
    module = importlib.import_module(_TRANSFORMERS)
    for new in _RENAMED.values():
        assert getattr(module, new).__name__ == new
