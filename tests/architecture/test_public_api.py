"""
test_public_api.py — the core's public API matches its committed snapshot (F220).

Contains: the snapshot comparison, its anti-vacuity twin, and the refusal to render a snapshot
    from a checkout missing an adapter file.
Does NOT contain: the rendering itself, which is scripts/public_api_snapshot.py, so the test
    and the `--write` that regenerates the snapshot cannot disagree about what a line holds.

A removed or changed line is a breaking change to a package that extends the core; an added
line is a minor release. Whether the version was bumped to match is checked by /review-pr and
at release, not here: the version moves once per release, the snapshot with every change
(docs/architecture/decisions/2026-11-13-core-extension-readiness-design.md, change 3).
"""
from __future__ import annotations

import importlib.util
from types import ModuleType

from _arch import ROOT

_SNAPSHOT = ROOT / "tests" / "architecture" / "public_api.txt"


def _script() -> ModuleType:
    spec = importlib.util.spec_from_file_location("public_api_snapshot", ROOT / "scripts" / "public_api_snapshot.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_public_api_matches_its_snapshot() -> None:
    """F220 — every public name and signature is the committed snapshot's."""
    script = _script()
    problems = script.compare(_SNAPSHOT.read_text(encoding="utf-8"), script.render())
    assert not problems, (
        "the public API differs from tests/architecture/public_api.txt. A removed or changed line "
        "breaks a package that extends the core: bump the version as .claude/standards/"
        "tooling-and-gates.md says and mark it Breaking in CHANGELOG.md. Then, and for added lines, run "
        f"`python scripts/public_api_snapshot.py --write` and commit the file: {problems}"
    )


def test_every_snapshot_check_can_fail() -> None:
    """F220 — anti-vacuity: each departure from the snapshot, alone, is reported."""
    script = _script()
    current = script.render()
    lines = current.splitlines()
    assert script.compare(current, current) == []
    write = next(line for line in lines if line.startswith(
        "spark_quick_start.adapters.PlatformAdapter.write_delta_append method"))
    adapter = next(line for line in lines if line.startswith(
        "spark_quick_start.orchestration.transformers.SourceTransformer._adapter annotation"))
    mutations = {
        "a line removed": "\n".join(line for line in lines if line != write) + "\n",
        "a line added": current + "spark_quick_start.orchestration.Extra function()\n",
        "a default changed": current.replace(write, write.replace("identifier", "identifier=None", 1)),
        "an injected service retyped": current.replace(adapter, adapter + " | None"),
    }
    wrong = {label: snapshot for label, snapshot in mutations.items()
             if snapshot == current or not script.compare(snapshot, current)}
    # Read as the snapshot holding the old signature and the code the new one.
    signatures = {
        "a required parameter added": ("f function(a, b=1)", "f function(a, b=1, c)", "Breaking"),
        "a required keyword-only parameter added": ("f function(a, *, b=1)", "f function(a, *, b=1, c)", "Breaking"),
        "an optional parameter inserted before a positional one": ("f function(a, b)", "f function(a, x=1, b)", "Breaking"),
        "a parameter renamed": ("f function(a, b=1)", "f function(a, c=1)", "Breaking"),
        "a return type changed": ("f function(a) -> 'int'", "f function(a) -> 'str'", "Breaking"),
        "an optional parameter appended": ("f function(a)", "f function(a, b=1)", "minor"),
        "an optional keyword-only parameter added": ("f function(a, b=1)", "f function(a, b=1, *, c=None)", "minor"),
        "an optional parameter before *args": ("f function(a, *args)", "f function(a, b=1, *args)", "Breaking"),
        "a bare * before an existing parameter": ("f function(a, b=1)", "f function(a, *, b=1)", "Breaking"),
        "an optional parameter after *args": ("f function(a, *args)", "f function(a, *args, b=1)", "minor"),
        "an optional parameter on an overridable method": (
            "C abstract (m)\nC.m method(self, a)", "C abstract (m)\nC.m method(self, a, b=1)", "Breaking"),
        "an optional parameter on a hook": ("C._run method(self, s)", "C._run method(self, s, x=1)", "Breaking"),
        "an optional parameter on a method of a plain class": ("C.m method(self, a)", "C.m method(self, a, b=1)", "minor"),
    }
    for label, (old, new, verdict) in signatures.items():
        got = script.compare(old + "\n", new + "\n")
        if not got or not all(verdict in p for p in got if not p.startswith("added")) or (
                verdict == "Breaking" and not any("Breaking" in p for p in got)):
            wrong[label] = got
    assert not wrong, f"these departures were not reported: {sorted(wrong)}"
    removed = script.compare(mutations["a line removed"], current)
    added = script.compare(mutations["a line added"], current)
    assert all(p.startswith("added") for p in removed) and all(p.startswith("removed") for p in added), (
        "the snapshot holds the old API and the code the new: a line only the snapshot has was "
        f"removed, a line only the code has was added; got {removed} and {added}")


def test_the_snapshot_refuses_a_checkout_missing_an_adapter() -> None:
    """F220 — a missing adapter file is an error, not a snapshot with its lines left out."""
    import spark_quick_start.adapters as adapters

    script = _script()
    saved = adapters.FabricAdapter
    adapters.FabricAdapter = None  # type: ignore[assignment,misc]
    try:
        try:
            script.render()
        except RuntimeError as error:
            assert "FabricAdapter" in str(error)
        else:
            raise AssertionError("render() produced a snapshot without FabricAdapter")
    finally:
        adapters.FabricAdapter = saved  # type: ignore[misc]


_ABSENT = object()


def _patched(owner: object, name: str, value: object) -> object:
    """Set or delete *name* on *owner*; return what to restore (``_ABSENT`` when it was unset)."""
    # `__bases__` is a type slot, not an entry in the class's namespace.
    saved = getattr(owner, name) if name == "__bases__" else vars(owner).get(name, _ABSENT)
    if value is _ABSENT:
        delattr(owner, name)
    else:
        setattr(owner, name, value)
    return saved


def _restore(owner: object, name: str, saved: object) -> None:
    if saved is _ABSENT:
        if name in vars(owner):
            delattr(owner, name)
    else:
        setattr(owner, name, saved)


def _params(cls: type, **change: bool) -> object:
    """A copy of a dataclass's parameters with *change* applied."""
    params = cls.__dataclass_params__  # type: ignore[attr-defined]
    kind = type(params)
    return kind(**{k: change.get(k, getattr(params, k)) for k in kind.__slots__})


def test_every_api_change_the_adversary_found_is_rendered() -> None:
    """F220 — anti-vacuity on the code side: each breaking change, alone, changes a line.

    The check-adversary run of 2026-11-13 found these left the render unchanged, or reported
    only as added; each is now a mutation that must remove or change a snapshot line.
    """
    import functools

    from spark_quick_start.adapters import PlatformAdapter
    from spark_quick_start.orchestration import (
        ExtensionsContext,
        GlobalContext,
        LoadOrchestrator,
        PipelineState,
        SCDContext,
    )
    from spark_quick_start.orchestration.transformers import (
        AlwaysRunTransformer,
        DVHubWriterTransformer,
        HWMProvider,
        LoadContext,
        MetricsChartTransformer,
        ProcessTransformer,
    )
    from spark_quick_start.spark_logging import LogSession

    def str_timestamp(self: object) -> str:
        return ""

    def bare_init(self: object) -> None:
        self.inserted = None

    def run_with_extra(self: object, *, extra: int) -> None:
        return None

    def call_with_extra(self: object, state: object, ctx: object, *, extra: int) -> object:
        return state

    script = _script()
    baseline = script.render()
    mutations = {
        "an existing method made abstract": (
            PlatformAdapter, "__abstractmethods__",
            PlatformAdapter.__abstractmethods__ | {"write_delta_append"}),
        "a new abstract method": (
            PlatformAdapter, "__abstractmethods__",
            PlatformAdapter.__abstractmethods__ | {"must_implement"}),
        "a protocol no longer runtime-checkable": (HWMProvider, "_is_runtime_protocol", False),
        "a frozen dataclass thawed": (LoadContext, "__dataclass_params__", _params(LoadContext, frozen=False)),
        "a dataclass frozen": (PipelineState, "__dataclass_params__", _params(PipelineState, frozen=True)),
        "a property's return type changed": (GlobalContext, "load_timestamp_utc", property(str_timestamp)),
        "an instance attribute no longer set": (DVHubWriterTransformer, "__init__", bare_init),
        "a base class changed": (MetricsChartTransformer, "__bases__", (ProcessTransformer,)),
        "a template's call signature changed": (ProcessTransformer, "__call__", call_with_extra),
        "a context manager no longer one": (LogSession, "__enter__", _ABSENT),
        "a mapping no longer subscriptable": (ExtensionsContext, "__getitem__", _ABSENT),
        "a property made cached": (SCDContext, "is_data_vault",
                                   functools.cached_property(SCDContext.is_data_vault.fget)),
        "a wrapped method's signature changed": (LoadOrchestrator, "run", functools.lru_cache(run_with_extra)),
    }
    assert MetricsChartTransformer.__bases__ == (AlwaysRunTransformer,)
    unreported = {}
    for label, (owner, name, value) in mutations.items():
        saved = _patched(owner, name, value)
        try:
            problems = script.compare(baseline, script.render())
        finally:
            _restore(owner, name, saved)
        if not any(p.startswith("removed") for p in problems):
            unreported[label] = problems
    assert script.render() == baseline, "a mutation was not restored"
    assert not unreported, f"these breaking changes did not remove or change a line: {unreported}"


def test_the_render_is_the_same_in_every_run() -> None:
    """F220 — no memory address, and a set renders sorted, so the hash seed cannot move a line."""
    script = _script()
    assert " at 0x" not in script.render()
    assert script._stable(frozenset({"beta", "alpha", "gamma"})) == "frozenset({'alpha', 'beta', 'gamma'})"
    assert script._stable({"b": 1, "a": {2, 1}}) == "{'a': set({1, 2}), 'b': 1}"
    assert script._stable(lambda: None).startswith("<function test_the_render_is_the_same_in_every_run.<locals>.<lambda>")
    assert script._stable(object()) == "<object>"


def test_moving_a_base_between_internal_modules_changes_no_line() -> None:
    """F220 — control: a refactor that moves an exported base class is not a breaking change."""
    from spark_quick_start.orchestration.transformers import ProcessTransformer

    script = _script()
    baseline = script.render()
    saved = ProcessTransformer.__module__
    ProcessTransformer.__module__ = "spark_quick_start.orchestration.transformers.moved"
    try:
        moved = script.render()
    finally:
        ProcessTransformer.__module__ = saved
    assert script.compare(baseline, moved) == []
