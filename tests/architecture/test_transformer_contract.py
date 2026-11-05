"""
test_transformer_contract.py — executable form of .claude/rules/transformer-contract.md.

Contains: template-override, constructor-purity, wrapper registration and
    protocol-completeness assertions.
Does NOT contain: cost rules — spark-delta-cost has no enforcing test by design.
    Also does not contain a "concrete transformer implements its hook" check —
    deleted; see the comment above where it used to sit for why it cannot fail.
"""
from __future__ import annotations

import ast
import inspect
import textwrap

import _arch

from spark_quick_start.orchestration.transformers.base import (
    AlwaysRunTransformer,
    ProcessTransformer,
    SourceTransformer,
)
from spark_quick_start.orchestration.transformers.conditional import ConditionalTransformer
from spark_quick_start.orchestration.transformers.conditional_source import (
    ConditionalSourceTransformer,
)
from spark_quick_start.orchestration.transformers.protocols import (
    HWMProvider,
    SCDMetricsProvider,
    SourceMetricsProvider,
)

# The classes that own a template and are allowed to define it.
_TEMPLATE_OWNERS = {
    SourceTransformer,
    ProcessTransformer,
    AlwaysRunTransformer,
    ConditionalTransformer,
    ConditionalSourceTransformer,
}

# Every wrapper that holds an inner transformer. LoadOrchestrator._unwrap must
# resolve each one, or the inner transformer's metrics vanish silently.
_WRAPPERS = {ConditionalTransformer, ConditionalSourceTransformer}

# Services injected at .run() time. A constructor must not require any of them.
# orchestrator.py's _run_pipeline sets _spark/_adapter/_logger/_metrics_collector and
# _stage_index unconditionally on every transformer; _ctx_manager only conditionally
# (behind a hasattr guard) — which is exactly why it must not be constructor-required
# either: the sanctioned shape is a class attribute default (see StagingTransformer),
# filled in by the orchestrator when present.
_SERVICE_PARAMS = {
    "spark",
    "adapter",
    "logger",
    "metrics_collector",
    "stage_index",
    "ctx_manager",
}


def _all_subclasses(cls: type) -> set[type]:
    found: set[type] = set()
    for sub in cls.__subclasses__():
        found.add(sub)
        found |= _all_subclasses(sub)
    return found


def _transformers() -> set[type]:
    # Importing the package registers every shipped transformer as a subclass.
    import spark_quick_start.orchestration.transformers  # noqa: F401

    found = _all_subclasses(SourceTransformer) | _all_subclasses(ProcessTransformer)
    # __subclasses__() returns every subclass the interpreter has ever seen, not just
    # this package's. The test suite defines dozens of ad-hoc SourceTransformer/
    # ProcessTransformer subclasses inline (tests/unit/test_transformer_base.py alone
    # has around twenty), and pytest's collection order decides which of those test
    # modules have been imported by the time this runs. Without this filter, the set
    # — and therefore this file's results — depends on module import order: running
    # this file alone never imports tests/unit/test_conditional_source_transformer.py,
    # so its `_HookedSource` fixture (a SourceTransformer that reuses the name
    # `inserted` for something unrelated to SCDMetricsProvider) is invisible; running
    # the full suite does import it, and it showed up as a partial SCDMetricsProvider
    # claimant, failing a test that had nothing to do with it. transformer-contract.md
    # governs the shipped library, not test fixtures, so keep only classes actually
    # defined in the package.
    return {cls for cls in found if cls.__module__.startswith("spark_quick_start")}


def test_no_subclass_overrides_the_templates() -> None:
    """produce() and __call__() own before_run -> work -> after_run and the metrics contract."""
    offenders = []
    for cls in sorted(_transformers(), key=lambda c: c.__name__):
        if cls in _TEMPLATE_OWNERS:
            continue
        offenders.extend(
            f"{cls.__module__}.{cls.__name__} overrides {template}()"
            for template in ("produce", "__call__")
            if template in cls.__dict__
        )
    assert not offenders, (
        "a transformer overrides its template instead of implementing _produce()/_run(). "
        "The template owns the metrics contract, so an override silently drops the "
        f"stage's StageMetrics entry (rule: transformer-contract): {offenders}"
    )


# No test_every_concrete_transformer_implements_its_hook here. It used to check
# "is _produce/_run anywhere in cls.__mro__", which is vacuous — that name is an
# @abstractmethod on SourceTransformer/ProcessTransformer, so it is present in
# every subclass's MRO via the ABC itself regardless of whether any concrete
# class overrides it. Reviewer I1 (2026-09-25 whole-branch review) proposed
# tightening it to search only ancestors below the two hook-declaring ABCs.
# That version was written and proven against this exact suite: renaming
# CandidateGuardTransformer._run to _run_renamed (temporarily, reverted) does
# NOT make the tightened test fail — it makes inspect.isabstract(cls) True for
# that class instead, so the test's own "skip abstract classes" guard removes
# it from consideration before the hook search ever runs. Confirmed directly:
#     class Broken(ProcessTransformer): pass
#     inspect.isabstract(Broken)  # True
#     Broken()  # TypeError: Can't instantiate abstract class Broken with
#               # abstract method _run
# A class that skips the hook is, by definition of ABCMeta, abstract and
# uninstantiable — Python already enforces this rule at class-creation time,
# for every subclass, with no way to construct a counterexample. There is no
# tightening of this test that can make it fail on real code, so per the
# review's own instruction ("either outcome is acceptable; a test that cannot
# fail is not") the assertion is deleted rather than kept in a form that only
# *looks* meaningful.


def test_no_transformer_requires_a_service_in_its_constructor() -> None:
    """Services are injected at .run() time, never demanded by __init__."""
    offenders = []
    for cls in sorted(_transformers(), key=lambda c: c.__name__):
        try:
            params = inspect.signature(cls.__init__).parameters
        except (ValueError, TypeError):  # pragma: no cover — C-level __init__
            continue
        for name, param in params.items():
            if name.lstrip("_") in _SERVICE_PARAMS and param.default is inspect.Parameter.empty:
                offenders.append(f"{cls.__name__}.__init__ requires {name!r}")
    assert not offenders, (
        "a transformer requires an injected service in its constructor. They are "
        "wired by LoadOrchestrator._run_pipeline at .run() time, and a transformer "
        f"must tolerate self._logger is None (rule: transformer-contract): {offenders}"
    )


def test_every_wrapper_is_resolved_by_unwrap() -> None:
    """A new wrapper not added to _unwrap() makes its inner transformer's metrics vanish.

    _unwrap is defined inside LoadOrchestrator.run(), so it is not importable.
    This asserts on the source text — weaker than calling it, and still the only
    check available without restructuring run().
    """
    # The function's exact extent, via AST. This used to slice `body[1][:1200]` — a magic
    # window with roughly 512 characters of slack, which fails safe but silently narrows as
    # _unwrap() grows (F19 item 6). A text split on the next `def ` is no better here:
    # _unwrap is a *nested* function, so the next 4-space def is the enclosing method's
    # sibling and the window would cover the whole of run(), letting a wrapper mentioned
    # anywhere in that method satisfy the assertion falsely.
    #
    # Body only, not the whole node: the surrounding comments name both wrappers in prose,
    # so including them would make this pass on documentation rather than on code — which
    # is exactly how a first attempt at proving this assertion fires fooled itself.
    path = _arch.SRC / "orchestration" / "orchestrator.py"
    source = path.read_text(encoding="utf-8")
    unwrap = next(
        (n for n in ast.walk(_arch.tree(path))
         if isinstance(n, ast.FunctionDef) and n.name == "_unwrap"),
        None,
    )
    assert unwrap is not None, "LoadOrchestrator.run() no longer defines _unwrap()"
    unwrap_code = "\n".join(
        ast.get_source_segment(source, stmt) or "" for stmt in unwrap.body
    )
    missing = [cls.__name__ for cls in _WRAPPERS if cls.__name__ not in unwrap_code]
    assert not missing, (
        "a wrapper transformer is not resolved by LoadOrchestrator's _unwrap(). Its "
        f"inner transformer's HWM and SCD metrics will be silently dropped: {missing}"
    )


def _declared_attributes(cls: type) -> set[str]:
    """Every attribute name *cls* declares, across its MRO.

    Three surfaces count as a declaration, because protocols.py's own examples
    use two of them: a class-level annotation, a class attribute, and an
    assignment to ``self`` in ``__init__``. Every shipped provider uses the
    third — ``NativeSCDMergeTransformer.__init__`` does ``self.inserted: int | None = None``
    — and an annotated assignment inside a method body never reaches the class's
    ``__annotations__``, so a check reading only the class body sees zero
    claimants for SCDMetricsProvider and SourceMetricsProvider and silently
    enforces nothing.

    Deliberately NOT included: assignments in ``_run()``/``_produce()``. The
    orchestrator reads these attributes whether or not the transformer ran, so
    an attribute that only comes into existence after ``_run()`` is not
    declared at all.
    """
    declared: set[str] = set()
    for ancestor in cls.__mro__:
        declared |= set(getattr(ancestor, "__annotations__", {}))
        declared |= set(vars(ancestor))
        init = ancestor.__dict__.get("__init__")
        if init is None:
            continue
        try:
            source = textwrap.dedent(inspect.getsource(init))
        except (OSError, TypeError):  # pragma: no cover — C-level or no source
            continue
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.Assign):
                targets = list(node.targets)
            elif isinstance(node, ast.AnnAssign):
                targets = [node.target]
            else:
                continue
            for target in targets:
                if (
                    isinstance(target, ast.Attribute)
                    and isinstance(target.value, ast.Name)
                    and target.value.id == "self"
                ):
                    declared.add(target.attr)
    return declared


def test_protocol_implementors_declare_every_attribute() -> None:
    """Declared, not probed — the orchestrator must never conflate missing or not-run with zero.

    An append-only writer declares updated as None and sets it to 0 inside ``_run()``.
    """
    required = {
        HWMProvider: ("hwm_before", "hwm_after", "hwm_column"),
        SCDMetricsProvider: ("inserted", "updated"),
        SourceMetricsProvider: ("source_count", "target_count"),
    }
    offenders = []
    claimants = {protocol.__name__: 0 for protocol in required}
    for cls in sorted(_transformers(), key=lambda c: c.__name__):
        if inspect.isabstract(cls):
            continue
        declared = _declared_attributes(cls)
        for protocol, attributes in required.items():
            # Claiming is ANY of the protocol's attributes, not the first one:
            # a class declaring `updated` but not `inserted` must be caught, and
            # a first-attribute-only check would skip it entirely.
            if not declared & set(attributes):
                continue
            claimants[protocol.__name__] += 1
            missing = [a for a in attributes if a not in declared]
            if missing:
                offenders.append(f"{cls.__name__} claims {protocol.__name__} but lacks {missing}")
    assert not offenders, (
        "a transformer declares part of a metrics protocol. Declare every attribute, "
        "in __init__ (counts as None; _run() sets the structurally zero ones), so the "
        "orchestrator reads a declared attribute rather than probing with getattr "
        "(rule: transformer-contract): "
        f"{offenders}"
    )
    # Self-defence against vacuity: a protocol with no declaring transformer means
    # the loop above enforced nothing for it, which is exactly how this assertion
    # was silently hollow before.
    assert all(count > 0 for count in claimants.values()), (
        "a metrics protocol has no declaring transformer, so this assertion enforced "
        f"nothing for it: {claimants}"
    )


def _eager_count_offenders(classes: set[type]) -> list[str]:
    """Names of SCDMetricsProvider classes whose fresh instance holds a count (F117).

    A class that cannot be built with no arguments is skipped: its constructor is not the
    shipped shape, and the orchestrator-facing providers all take none.
    """
    offenders = []
    for cls in sorted(classes, key=lambda c: c.__name__):
        if inspect.isabstract(cls):
            continue
        try:
            fresh = cls()
        except TypeError:
            continue
        if isinstance(fresh, SCDMetricsProvider) and (fresh.inserted is not None or fresh.updated is not None):
            offenders.append(cls.__name__)
    return offenders


def test_a_fresh_scd_metrics_provider_reports_no_counts() -> None:
    """None until `_run()` sets them: a count of 0 at construction makes a skipped branch look run."""
    classes = _transformers()
    assert _eager_count_offenders(classes) == []
    # Self-defence against vacuity: the check must have found providers to look at.
    assert any(isinstance(cls(), SCDMetricsProvider) for cls in classes if _builds(cls))


def _builds(cls: type) -> bool:
    try:
        return not inspect.isabstract(cls) and cls() is not None
    except TypeError:
        return False


def test_a_provider_defaulting_to_zero_is_reported() -> None:
    """Mutation: the shape F117 removed."""

    class Eager:
        def __init__(self) -> None:
            self.inserted: int | None = 0
            self.updated: int | None = 0

    assert _eager_count_offenders({Eager}) == ["Eager"]


def test_a_provider_defaulting_to_none_is_not_reported() -> None:
    """Control for the mutation above."""

    class Lazy:
        def __init__(self) -> None:
            self.inserted: int | None = None
            self.updated: int | None = None

    assert _eager_count_offenders({Lazy}) == []
