"""F197 — the "append" load style is not part of the library.

`NativeSCDAppendTransformer`, `append_pipeline` and `execute_scd_append` do not exist. A history
of changes is a merge with `scd2`; an append-only load is built by hand from
`NativeSCDAppendOnlyTransformer`, which stays, as do `SCDCandidateVersionGuardTransformer` and
`execute_append_only`.

The check is a word-boundary text search over every `.py` under `src/`, so a stale docstring or
comment naming one of them fails as a leftover import would. `NativeSCDAppendOnlyTransformer`
does not match: the boundary sits after `Append`, not inside the longer name.
"""
from __future__ import annotations

import re
from collections.abc import Iterable

import _arch

_REMOVED = ("NativeSCDAppendTransformer", "append_pipeline", "execute_scd_append")
_PATTERN = re.compile(r"\b(" + "|".join(_REMOVED) + r")\b")


def _leftovers(sources: Iterable[tuple[str, str]]) -> list[str]:
    """Every line of every ``(name, text)`` pair naming a removed identifier, as ``name:line: id``."""
    return [
        f"{name}:{number}: {match.group(1)}"
        for name, text in sources
        for number, line in enumerate(text.splitlines(), start=1)
        for match in _PATTERN.finditer(line)
    ]


def _src_leftovers() -> list[str]:
    """`_leftovers` over every `.py` under `src/`, egg-info and bytecode directories skipped."""
    paths = (p for p in sorted(_arch.SRC.rglob("*.py"))
             if "__pycache__" not in p.parts and not any(part.endswith(".egg-info") for part in p.parts))
    return _leftovers((p.relative_to(_arch.SRC).as_posix(), p.read_text(encoding="utf-8")) for p in paths)


def test_no_source_file_names_a_removed_append_identifier() -> None:
    found = _src_leftovers()
    assert not found, f"the append load style is back (F197): {found}"


def test_the_removed_names_are_not_attributes_of_the_packages() -> None:
    import spark_quick_start.orchestration as orchestration
    import spark_quick_start.orchestration.transformers as transformers

    back = [(m.__name__, n) for m in (orchestration, transformers) for n in _REMOVED if hasattr(m, n)]
    assert not back, f"the append load style is exported again (F197): {back}"


def test_the_names_kept_are_still_there() -> None:
    """The append-only load is built by hand from these; removing the style must not take them."""
    import spark_quick_start.orchestration as orchestration
    import spark_quick_start.orchestration.transformers as transformers

    assert hasattr(orchestration, "default_pipeline")
    assert hasattr(transformers, "NativeSCDAppendOnlyTransformer")
    assert hasattr(transformers, "SCDCandidateVersionGuardTransformer")


def test_the_search_can_fail() -> None:
    """Anti-vacuity: each removed name is reported; the kept near-names are not."""
    dirty = [(f"{name}.py", f"x = {name}\n") for name in _REMOVED]
    assert len(_leftovers(dirty)) == len(_REMOVED)
    clean = [("ok.py", "NativeSCDAppendOnlyTransformer = execute_append_only = default_pipeline = 1\n")]
    assert not _leftovers(clean)
