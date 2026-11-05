"""conditional.py — ConditionalTransformer.

Wraps any ProcessTransformer and skips it when a predicate is False.
Predicate is evaluated once per pipeline run, before the inner transformer.
"""
from __future__ import annotations

from collections.abc import Callable
from typing import Union

from spark_quick_start.orchestration.transformers.base import (
    AlwaysRunTransformer,
    LoadContext,
    ProcessTransformer,
)
from spark_quick_start.orchestration.transformers.state import PipelineState

Predicate = Union[bool, Callable[[LoadContext], bool]]


class ConditionalTransformer(ProcessTransformer):
    """Wraps a ProcessTransformer and skips it when a predicate is False.

    When the predicate evaluates to False the input ``state`` is returned
    unchanged (identity passthrough) and a DEBUG log entry is emitted.
    When True the inner transformer is called normally.

    Args:
        predicate: A plain ``bool`` or ``Callable[[LoadContext], bool]``.
            Evaluated once per ``run()`` call after ``LoadContext`` is built.
            A plain ``bool`` is normalised to a constant lambda at construction.
        transformer: The ``ProcessTransformer`` to conditionally apply.
        label: Optional human-readable name for this conditional step.
            Used in log messages and ``__repr__`` instead of the inner
            transformer's class name.

    Raises:
        TypeError: If ``predicate`` is neither ``bool`` nor callable.
        RuntimeError: If ``predicate`` itself raises; wraps the original
            exception and includes the step name for diagnostics.

    Example::

        .add_if(
            lambda ctx: ctx.scd_ctx.load_type == "Incremental",
            SCDIncrementalTargetTableTransformer(),
            label="target filtered to incoming keys",
        )
        .add_if(
            lambda ctx: ctx.scd_ctx.load_type == "Full",
            SCDTargetTableTransformer(),
            label="full target",
        )
    """

    def __init__(
        self,
        predicate: Predicate,
        transformer: ProcessTransformer,
        label: str | None = None,
    ) -> None:
        if isinstance(predicate, bool):
            self._predicate: Callable[[LoadContext], bool] = lambda _: predicate
        elif callable(predicate):
            self._predicate = predicate
        else:
            raise TypeError(
                f"ConditionalTransformer: predicate must be bool or "
                f"Callable[[LoadContext], bool], got {type(predicate).__name__}"
            )
        self._transformer = transformer
        self._label = label

    @property
    def _name(self) -> str:
        return self._label or type(self._transformer).__name__

    def __repr__(self) -> str:
        return f"ConditionalTransformer({self._name})"

    def __call__(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
        """Route to the inner transformer without running a lifecycle of our own.

        This deliberately does not call ``super().__call__``. A conditional is
        pure routing, not a pipeline stage: running
        ``ProcessTransformer.__call__`` here would wrap the inner transformer in
        a *second* before_run/after_run pair, so we would either double-record
        StageMetrics or (as was previously the case, by calling the inner's
        ``_run`` directly to avoid that) suppress the inner's hooks entirely.
        Suppressing them silently dropped whatever an inner ``after_run``
        override contributes — ``NativeSCDMergeTransformer`` attaches
        ``inserted``/``updated`` there, so ``.add_if(pred, NativeSCDMerge...)``
        lost both from ``ExecutionResult.stages``.

        Delegating the whole call gives exactly one lifecycle — the inner's —
        with its own name, its own extra metrics, and its own halted semantics.

        The halted short-circuit is also the inner transformer's to make:
        ``AlwaysRunTransformer`` subclasses (``MetricsChartTransformer``,
        ``PlanReportTransformer``) exist precisely to run on a halted pipeline,
        and short-circuiting here would silently downgrade one to a
        skip-on-halt step just because it was wrapped in ``.add_if()``.

        A skipped conditional records no StageMetrics entry (it emits the DEBUG
        skip log only). Previously the wrapper's own hooks left a zero-duration
        entry carrying the inner's name, which read as though the step had run.
        """
        if state.halted and not isinstance(self._transformer, AlwaysRunTransformer):
            return state
        return self._run(state, ctx)

    def _run(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
        # Forward injected services to the inner transformer before any branch.
        # Inner _run() bodies commonly use self._logger/self._spark/self._adapter
        # directly (e.g. CandidateGuardTransformer, NativeSCDMergeTransformer),
        # and the inner's own before_run/after_run need _metrics_collector.
        self._transformer._spark = self._spark
        self._transformer._adapter = self._adapter
        self._transformer._logger = self._logger
        self._transformer._metrics_collector = self._metrics_collector
        # The wrapper holds the pipeline position; the inner transformer is
        # what derives a scratch table name from it.
        self._transformer._stage_index = self._stage_index

        try:
            should_run = self._predicate(ctx)
        except Exception as exc:
            raise RuntimeError(
                f"ConditionalTransformer: predicate raised while evaluating "
                f"whether to run {self._name}: {exc}"
            ) from exc

        if not should_run:
            if self._logger:
                self._logger.debug(f"Skipping {self._name}: predicate evaluated to False")
            return state
        # Full inner lifecycle: before_run -> _run -> after_run. See __call__.
        return self._transformer(state, ctx)
