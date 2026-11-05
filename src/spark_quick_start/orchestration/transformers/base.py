"""
base.py — Core transformer abstractions for the pipeline architecture.

LoadContext: frozen config envelope passed to every transformer.
_TransformerBase: shared service attributes, _name, and lifecycle hooks.
SourceTransformer: ABC for transformers that produce an initial PipelineState.
ProcessTransformer: ABC for transformers that receive and return a PipelineState.
"""
from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pyspark.sql import SparkSession

from spark_quick_start.orchestration.metrics import StageMetricsCollector
from spark_quick_start.orchestration.models import ExtensionsContext, PlatformContext
from spark_quick_start.orchestration.transformers.state import PipelineState

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from spark_quick_start.adapters.platform_adapter import PlatformAdapter
    from spark_quick_start.logger import OrchestratorLogger
    from spark_quick_start.orchestration.models import (
        DQDContext,
        GlobalContext,
        HWMContext,
        LoggingContext,
        RuntimeContext,
        SCDContext,
        StagingContext,
    )


def _safe_num_partitions(df: DataFrame | None, enabled: bool = True) -> int | None:
    """Return df.rdd.getNumPartitions(), or None when disabled/unavailable.

    Cheap for a narrow plan (scan, projection, filter) — reading the partition
    count off the RDD needs no Spark job. NOT cheap when the plan contains a
    shuffle: ``df.rdd`` goes through ``QueryExecution.toRdd``, and an AQE
    ``AdaptiveSparkPlanExec`` cannot report a final partition count until its
    shuffle stages have materialised, so asking runs them.

    Observed: a SCDChangeDetectorTransformer stage whose partition count changed
    16 -> 98 (AQE coalescing after the target join) spent 117s here, against
    <=170ms for every stage with an unchanged count. after_run() reports that
    cost as ``measure_ms`` so it is attributable rather than hidden.

    Args:
        df: DataFrame to inspect. None returns None.
        enabled: When False, skip the call entirely and return None. Defaults
            to True — partition counts are the cheapest useful volume signal
            on a narrow plan, and dropping them by default would remove the
            metric that makes shuffle behaviour visible at all.
    """
    if not enabled or df is None:
        return None
    try:
        return int(df.rdd.getNumPartitions())
    except Exception:
        return None


def _safe_row_count(df: DataFrame | None, enabled: bool) -> int | None:
    """Return df.count() only when enabled — this is a real Spark action."""
    if not enabled or df is None:
        return None
    try:
        return df.count()
    except Exception:
        return None


def _safe_explain_string(df: DataFrame | None, enabled: bool, mode: str = "extended") -> str | None:
    """Return the Spark query plan for df as a string, only when enabled.

    Calls the same internal PythonSQLUtils.explainString() that DataFrame.explain()
    uses, without the print() — this is plan analysis only, no Spark action is
    triggered. Spark Connect DataFrames (Databricks serverless) have no
    _sc/_jdf, so those fall through to Connect's own internal _explain_string(),
    which explain() calls before printing there too. Returns None when
    disabled, df is None, or both attempts fail (e.g. a detached/mocked
    DataFrame in unit tests).
    """
    if not enabled or df is None:
        return None
    try:
        jvm = df._sc._jvm
        return jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), mode)
    except Exception:
        pass
    try:
        return df._explain_string(mode=mode)
    except Exception:
        return None


@dataclass(frozen=True)
class LoadContext:
    """Read-only config envelope shared across all pipeline transformers.

    frozen=True prevents reassignment of LoadContext fields themselves.
    The nested context objects are mutable; transformers must not modify them.
    """
    global_ctx: GlobalContext
    staging_ctx: StagingContext
    scd_ctx: SCDContext
    runtime_ctx: RuntimeContext
    logging_ctx: LoggingContext
    # Optional: a load without hwm_context has none (F138).
    hwm_ctx: HWMContext | None = None
    # Optional: a load without dqd_context has none (F139).
    dqd_ctx: DQDContext | None = None
    # Never None: a load without extensions_context has an empty one, so a step may read it
    # without a guard. A LoadContext built by hand without either gets an empty one.
    extensions_ctx: ExtensionsContext = field(default_factory=ExtensionsContext)
    platform_ctx: PlatformContext = field(default_factory=PlatformContext)


class _TransformerBase(ABC):
    """Shared service attributes, name resolution, and lifecycle hooks.

    Provides ``before_run(ctx)`` and ``after_run(ctx)`` with default logging.
    Subclasses may override them — call ``super()`` to preserve base logging.

    Subclasses receive spark, adapter, and logger via class attributes,
    which are set by the pipeline runner before any method is called.
    """

    _spark: SparkSession | None = None
    _adapter: PlatformAdapter | None = None
    _logger: OrchestratorLogger | None = None
    _metrics_collector: StageMetricsCollector | None = None
    # Position in the orchestrator's transformer list, injected by run()
    # alongside the services above. Transformers that derive a scratch table
    # name use it so two instances of the same transformer in one pipeline
    # cannot collide on a single path (see MaterializeTransformer).
    _stage_index: int = 0
    _run_start: float = 0.0
    # Raw seconds, deliberately not pre-rounded milliseconds. after_run adds
    # this to its own measuring span and rounds the total once. Rounding here as
    # well made measure_ms the sum of two independently rounded values, and two
    # roundings can each gain half a millisecond — so measure_ms could come out
    # 1ms larger than the duration_ms containing it, which it is a part of.
    # Reproduced at ~7.5% of runs before the fix; see backlog F14.
    _measure_s: float = 0.0
    _partitions_in: int | None = None
    _row_count_in: int | None = None

    @property
    def _name(self) -> str:
        return type(self).__name__

    def before_run(self, ctx: LoadContext, df: DataFrame | None = None) -> None:
        """Record start time, input partitions/row-count, and log. Override and call super() to extend."""
        self._run_start = time.monotonic()
        measure_start = self._run_start

        # Emit the start event *before* measuring, not after. duration_ms is
        # read at the end of after_run, so it spans this whole method too — if
        # the start line were logged after the measurements below, duration_ms
        # would exceed the gap between the start and end log timestamps by
        # however long they took. That inverts the very diff used to find the
        # original bug: a stage where measuring the *input* is the expensive
        # part would report a duration larger than its own logged wall time.
        if self._logger:
            self._logger.info("transformer", tags={"event": "start", "transformer": self._name})

        self._partitions_in = _safe_num_partitions(
            df, getattr(ctx.runtime_ctx, "capture_partitions", True)
        )
        self._row_count_in = _safe_row_count(df, getattr(ctx.runtime_ctx, "capture_row_counts", False))
        self._measure_s = time.monotonic() - measure_start

    def after_run(
        self,
        ctx: LoadContext,
        df: DataFrame | None = None,
        *,
        extra_metrics: dict[str, Any] | None = None,
    ) -> None:
        """Log completion with duration_ms and volume metrics. Override and call super() to extend."""
        # Measure first, read the clock second. Reading it first excluded the
        # measurement block from duration_ms, so a stage that took 238s wall
        # reported 120s — the 117s _safe_num_partitions() spent materialising
        # an AQE shuffle landed in no metric at all. duration_ms now covers the
        # same span as the start/end log timestamps, and measure_ms says how
        # much of it was instrumentation rather than the stage's own work.
        measure_start = time.monotonic()
        partitions_out = _safe_num_partitions(
            df, getattr(ctx.runtime_ctx, "capture_partitions", True)
        )
        row_count_out = _safe_row_count(df, getattr(ctx.runtime_ctx, "capture_row_counts", False))
        plans_enabled = getattr(ctx.runtime_ctx, "capture_plans", False)
        plan = _safe_explain_string(df, plans_enabled)
        now = time.monotonic()

        duration_ms = round((now - self._run_start) * 1000)
        measure_ms = round((self._measure_s + (now - measure_start)) * 1000)

        metrics_list = [{"metricName": "duration_ms", "metricValue": duration_ms}]
        for name, value in (
            ("measure_ms", measure_ms or None),
            ("partitions_in", self._partitions_in),
            ("partitions_out", partitions_out),
            ("row_count_in", self._row_count_in),
            ("row_count_out", row_count_out),
        ):
            if value is not None:
                metrics_list.append({"metricName": name, "metricValue": value})
        for name, value in (extra_metrics or {}).items():
            metrics_list.append({"metricName": name, "metricValue": value})

        if self._logger:
            self._logger.info(
                "transformer",
                tags={"event": "end", "transformer": self._name},
                metrics=metrics_list,
            )
            # Plan strings can be large — kept out of metrics_list (numeric-only
            # by convention) and logged separately at DEBUG so it's suppressed
            # unless logging_context.verbose=True.
            if plan is not None:
                self._logger.debug(
                    f"{self._name} query plan\n{plan}",
                    tags={"event": "execution_plan", "transformer": self._name},
                )
            elif plans_enabled:
                # Distinguishes "capture_plans is off" (no log line at all) from
                # "capture_plans is on but the JVM explain call failed" — without
                # this, both cases silently produce zero plan-related output.
                self._logger.debug(
                    f"{self._name}: capture_plans is enabled but plan capture failed "
                    "(df may be a Spark Connect / detached DataFrame)",
                    tags={"event": "execution_plan_failed", "transformer": self._name},
                )

        if self._metrics_collector is not None:
            self._metrics_collector.record(
                stage=self._name,
                duration_ms=duration_ms,
                measure_ms=measure_ms,
                partitions_in=self._partitions_in,
                partitions_out=partitions_out,
                row_count_in=self._row_count_in,
                row_count_out=row_count_out,
                plan=plan,
                **(extra_metrics or {}),
            )


class SourceTransformer(_TransformerBase):
    """Produces the initial PipelineState — takes no input state.

    Subclasses implement ``_produce(ctx)``. The public ``produce(ctx)``
    is a template that calls:

        before_run(ctx) → _produce(ctx) → validate → after_run(ctx)

    Override ``before_run`` / ``after_run`` and call ``super()`` to extend
    without losing base logging.

    Required columns (always):
    - ``ctx.scd_ctx.business_key_column``

    The deleted flag column is optional (F127): when the source carries it, the detector
    reads it as the delete signal; when it does not, the detector writes ``scd_ctx.not_deleted_flag_value`` ("N" by default).

    Required when ``ctx.scd_ctx.source_system_from_source is True``:
    - ``ctx.scd_ctx.source_system_column``
    """

    @abstractmethod
    def _produce(self, ctx: LoadContext) -> PipelineState:
        """Subclass implementation — produce the initial PipelineState."""

    def produce(self, ctx: LoadContext) -> PipelineState:
        """Template: before_run → _produce → validate → after_run.

        Raises:
            ValueError: If required SCD columns are missing from ``state.df``.
        """
        self.before_run(ctx)
        state = self._produce(ctx)
        self._validate_source_columns(state, ctx)
        self.after_run(ctx, df=state.df)
        return state

    def _validate_source_columns(self, state: PipelineState, ctx: LoadContext) -> None:
        """Raise ValueError if required SCD columns are absent from state.df."""
        columns = set(state.df.columns)
        required = {ctx.scd_ctx.business_key_column}
        if ctx.scd_ctx.source_system_from_source:
            required.add(ctx.scd_ctx.source_system_column)

        missing = required - columns
        if missing:
            raise ValueError(
                f"{type(self).__name__}.produce() output is missing required SCD columns: "
                f"{sorted(missing)}. DataFrame columns: {sorted(columns)}"
            )


class ProcessTransformer(_TransformerBase):
    """Transforms a PipelineState — receives state + ctx, returns state.

    Subclasses implement ``_run(state, ctx)`` for main logic, and may override
    ``before_run`` / ``after_run`` for lifecycle hooks.

    Execution order: before_run → _run → after_run.

    When overriding hooks, call ``super().before_run(ctx)`` /
    ``super().after_run(ctx)`` to preserve base-class logging.
    """

    @abstractmethod
    def _run(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
        """Main transform logic.

        To stop the pipeline cleanly, return ``state.with_halted(reason)``: later
        steps short-circuit and the run reports ``early_exit``. Raising is a
        failure, not a halt — ``LoadOrchestrator.run()`` returns
        ``status="error"``.
        """

    def before_run(self, state: PipelineState, ctx: LoadContext) -> None:  # type: ignore[override]
        """Log transformer start. Override and call super() to extend."""
        super().before_run(ctx, df=state.df)

    def after_run(  # type: ignore[override]
        self,
        state: PipelineState,
        ctx: LoadContext,
        *,
        extra_metrics: dict[str, Any] | None = None,
    ) -> None:
        """Log transformer completion. Override and call super() to extend."""
        super().after_run(ctx, df=state.df, extra_metrics=extra_metrics)

    def __call__(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
        """Template: before_run → _run → after_run. Passes through if state.halted."""
        if state.halted:
            return state
        self.before_run(state, ctx)
        result = self._run(state, ctx)
        self.after_run(result, ctx)
        return result


class AlwaysRunTransformer(ProcessTransformer):
    """A ProcessTransformer that runs even when state.halted is True.

    ProcessTransformer.__call__ passes through unchanged on a halted pipeline
    (see above). Reporting-only transformers that never mutate state — printing
    a metrics chart or a plan report, for example — are exactly the ones where
    that information is most useful on an early-exit run, so they need to
    bypass the halted short-circuit rather than go silent along with it.

    Subclasses implement ``_run(state, ctx)`` exactly like ProcessTransformer;
    the only difference is this class's ``__call__`` always executes it.
    """

    def __call__(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
        """Template: before_run → _run → after_run. Ignores state.halted."""
        self.before_run(state, ctx)
        result = self._run(state, ctx)
        self.after_run(result, ctx)
        return result
