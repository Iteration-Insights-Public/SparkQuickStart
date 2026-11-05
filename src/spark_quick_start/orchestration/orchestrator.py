"""
orchestrator.py — LoadOrchestrator entry point.

Contains: LoadOrchestrator class only.
Does NOT contain: platform-specific imports, Spark logic, SCD logic,
    logging session management, or any import from spark_load_orchestration.

Usage (Fabric notebook)::

    from spark_quick_start.orchestration import LoadOrchestrator

    result = LoadOrchestrator(
        load_context_json=load_context,
        spark=spark,
        platform="fabric",
        data_storage_path=mssparkutils.lakehouse.get("DataStorage").abfsPath,
        data_eng_path=mssparkutils.lakehouse.get("DataEngineering").abfsPath,
    ).run()
    mssparkutils.notebook.exit(result.to_json())

Usage (Databricks job)::

    result = LoadOrchestrator(
        load_context_json=load_context,
        spark=spark,
        platform="databricks",
        data_storage_path=storage_path,
        data_eng_path=eng_path,
    ).run()

Usage (local / testing)::

    from spark_quick_start.adapters import LocalAdapter
    adapter = LocalAdapter(notebook_registry={"NB_Stage": my_fn})
    result = LoadOrchestrator(
        load_context_json=ctx,
        spark=spark,
        adapter=adapter,
        data_storage_path="/tmp/storage",
        data_eng_path="/tmp/eng",
    ).run()
    assert result.status == "success"
"""
from __future__ import annotations

import contextlib
import importlib
import traceback
import uuid
from dataclasses import asdict
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from pyspark.sql import SparkSession

from spark_quick_start.adapters.platform_adapter import PlatformAdapter
from spark_quick_start.logger import OrchestratorLogger
from spark_quick_start.orchestration.context_manager import LoadContextManager
from spark_quick_start.orchestration.metrics import StageMetricsCollector
from spark_quick_start.orchestration.models import ErrorDetails, ExecutionMetrics, ExecutionResult

if TYPE_CHECKING:
    # _run_pipeline's return annotation referenced PipelineState without it ever
    # being imported at module scope — the name is only bound inside the method
    # body. `from __future__ import annotations` keeps the annotation a string,
    # so this never failed at runtime, but it left the signature unresolvable
    # for both ruff (F821) and mypy ("Name 'PipelineState' is not defined").
    from spark_quick_start.orchestration.transformers.state import PipelineState

# Maps platform string → dotted class path for lazy adapter resolution.
_PLATFORM_MAP = {
    "fabric": "spark_quick_start.adapters.fabric_adapter.FabricAdapter",
    "databricks": "spark_quick_start.adapters.databricks_adapter.DatabricksAdapter",
    "local": "spark_quick_start.adapters.local_adapter.LocalAdapter",
}


class LoadOrchestrator:
    """Entry point for a single entity load run.

    Instantiated by the Main Load Notebook. Wires up LoadContextManager,
    OrchestratorLogger and the appropriate PlatformAdapter; the NotebookExecutor
    that runs staging notebooks is built by StagingTransformer, not here.
    Steps are registered with add(), add_if() and set_source(); run() executes
    the registered chain (a SourceTransformer first, then ProcessTransformers)
    and returns an ExecutionResult.

    The orchestrator never imports notebookutils, mssparkutils, or dbutils
    directly. All platform-specific behaviour is delegated to PlatformAdapter.

    Args:
        load_context_json: JSON string containing every context section
            (global_context, staging_context, scd_context, runtime_context,
            logging_context). Optionally also hwm_context, dqd_context and
            extensions_context (the caller's own data). A platform_context or
            dqf_context section is refused with ValueError when run() parses
            the JSON: the library fills platform_context from data_storage_path
            and data_eng_path, and DQF was removed (F146).
        spark: Active SparkSession for Delta I/O.
        adapter: Platform adapter. When None, resolved from ``platform``.
            Explicit injection takes priority.
        platform: String code for adapter resolution when adapter is None.
            Accepted values: "fabric", "databricks", "local".
            Ignored when adapter is provided.
        data_storage_path: Resolved value for {DataStorageLakehouse}.
            Pass "" when no placeholder is used.
        data_eng_path: Resolved value for {DataEngineeringLakehouse}.
            Pass "" when no placeholder is used.
        logger: Pre-constructed OrchestratorLogger. When None, built from
            logging_context inside the JSON. Explicit injection is for testing.
    """

    def __init__(
        self,
        load_context_json: str,
        spark: SparkSession,
        *,
        adapter: PlatformAdapter | None = None,
        platform: str = "fabric",
        data_storage_path: str = "",
        data_eng_path: str = "",
        logger: OrchestratorLogger | None = None,
    ) -> None:
        self._load_context_json = load_context_json
        self._spark = spark
        self._adapter = adapter if adapter is not None else self._resolve_adapter(platform)
        self._data_storage_path = data_storage_path
        self._data_eng_path = data_eng_path
        self._injected_logger = logger
        self._transformers: list[Any] = []
        # The last PipelineState a step returned in the current run(); what the `finally`
        # reads to release cached frames, including when a later step raised.
        self._last_state: PipelineState | None = None

    # ── Public API ────────────────────────────────────────────────────────────

    def add(self, transformer: Any) -> LoadOrchestrator:
        """Add a transformer to the pipeline. Returns self for chaining.

        The first transformer must be a SourceTransformer.
        Subsequent entries must be ProcessTransformer instances or plain
        callables with signature (DataFrame, LoadContext) -> DataFrame.
        spark and adapter are injected into base-class transformers at
        .run() time — not at .add() time.
        """
        self._transformers.append(transformer)
        return self

    def add_if(
        self,
        predicate: Any,
        transformer: Any,
        label: str | None = None,
    ) -> LoadOrchestrator:
        """Add a transformer that only runs when predicate is True.

        Equivalent to .add(ConditionalTransformer(predicate, transformer, label)).
        When the predicate is False the step is skipped and a DEBUG log entry
        is emitted using label (or the inner transformer's class name).

        Args:
            predicate: A plain bool or Callable[[LoadContext], bool].
                Evaluated at run() time after LoadContext is resolved.
            transformer: The ProcessTransformer to conditionally apply.
            label: Optional human-readable name shown in logs and __repr__.

        Returns:
            self, for chaining.
        """
        from spark_quick_start.orchestration.transformers.conditional import ConditionalTransformer
        return self.add(ConditionalTransformer(predicate, transformer, label=label))

    def set_source(
        self,
        predicate: Any,
        source_if_true: Any,
        source_if_false: Any,
        label: str | None = None,
    ) -> LoadOrchestrator:
        """Set the pipeline source, choosing between two SourceTransformers at runtime.

        Equivalent to .add(ConditionalSourceTransformer(predicate, source_if_true,
        source_if_false, label)). Must be the first call in the chain.

        Args:
            predicate: A plain bool or Callable[[LoadContext], bool].
            source_if_true: SourceTransformer used when predicate is True.
            source_if_false: SourceTransformer used when predicate is False.
            label: Optional name for log messages.

        Example::

            .set_source(
                lambda ctx: ctx.dqd_ctx is not None,
                DQDTransformer(),
                StagingTransformer(),
            )
        """
        from spark_quick_start.orchestration.transformers.conditional_source import (
            ConditionalSourceTransformer,
        )
        return self.add(ConditionalSourceTransformer(predicate, source_if_true, source_if_false, label=label))

    def run(self) -> ExecutionResult:
        """Execute the complete entity load pipeline.

        Steps:
        1. Parse and validate every context section.
        2. Initialize OrchestratorLogger (unless pre-injected).
        3. Log Spark runtime configuration (pre-staging).
        4. Execute the registered transformer chain (_run_pipeline).
        5. Harvest metrics from the steps that ran: inserted/updated counts
           from the SCDMetricsProvider whose counts are set; the watermark
           from the HWMProvider reporting for hwm_context.hwm_column_name
           when hwm_context is present (it wins even when it found none,
           F138), else from the first one with hwm_after set; source/target
           counts from the SourceMetricsProvider. When no HWMProvider
           matched, the watermark is read from the target table (load
           timestamp for Data Vault, effective-from otherwise), with
           before == after.
        6. Log Spark runtime configuration (post-merge).
        7. Release every frame a step cached (PipelineState.cached), then
           finalize and persist logs — both on success and on failure.
        8. Return ExecutionResult.

        On any unhandled exception, including no transformers registered:
        captures traceback, finalizes logs with success=False, and returns
        ExecutionResult(status="error"). Does not re-raise.

        Returns:
            ExecutionResult with status, metrics (including hwm_value_before /
            hwm_value_after), logs JSON, and correlation_id.
        """
        start_dt = datetime.now(UTC)
        metrics = ExecutionMetrics(start_time=start_dt.isoformat())
        correlation_id = str(uuid.uuid4())
        logger: OrchestratorLogger = self._injected_logger or OrchestratorLogger()
        collector = StageMetricsCollector()
        self._last_state = None

        _success = False
        _error_details: dict | None = None
        _early_exit = False
        _early_exit_reason = ""
        logs_json = ""

        try:
            # ── 1. Parse and validate ────────────────────────────────────────
            ctx = LoadContextManager(self._load_context_json)
            ctx.parse(self._data_storage_path, self._data_eng_path)
            ctx.validate_all()

            # Narrow once rather than dereferencing ctx.runtime_ctx repeatedly.
            # LoadContextManager declares its contexts Optional because they are
            # populated by parse(), which validate_all() above requires — see
            # issue #34. Checking here keeps the invariant in one place and
            # keeps both reads below type-safe.
            runtime_ctx = ctx.runtime_ctx
            if runtime_ctx is None:  # pragma: no cover — validate_all() guarantees parse
                raise RuntimeError(
                    "LoadContextManager.validate_all() returned with runtime_ctx unset."
                )

            correlation_id = runtime_ctx.correlation_id or correlation_id

            # ── 2. Initialize logger ─────────────────────────────────────────
            if self._injected_logger is None:
                logger = OrchestratorLogger.from_logging_context(
                    ctx.logging_ctx,
                    correlation_id,
                    self._adapter,
                    pipeline_name=runtime_ctx.pipeline_name,
                )

            logger.info(
                f"LoadOrchestrator.run() started | entity={ctx.global_ctx.entity_name}"
                f" | correlation_id={correlation_id}"
            )

            # ── 3. Pre-staging runtime log ───────────────────────────────────
            from spark_quick_start.orchestration.load_pipeline import log_runtime
            log_runtime(self._spark, "pre-staging", logger)

            # ── 4–11. Transformer pipeline + SCD metrics ─────────────────────
            hwm_before: str | None = None
            hwm_after: str | None = None
            inserted = 0
            updated = 0
            source_count = 0
            target_count = 0

            if not self._transformers:
                raise ValueError(
                    "LoadOrchestrator.run(): no transformers registered. "
                    "Call .add() with a SourceTransformer before .run()."
                )

            _pipeline_state = self._run_pipeline(ctx, logger, collector)

            if _pipeline_state.halted:
                _early_exit = True
                _early_exit_reason = _pipeline_state.halt_reason
                logger.info(f"Pipeline halted early: {_early_exit_reason}")

            from spark_quick_start.orchestration.transformers.conditional import (
                ConditionalTransformer,
            )
            from spark_quick_start.orchestration.transformers.conditional_source import (
                ConditionalSourceTransformer,
            )
            from spark_quick_start.orchestration.transformers.protocols import (
                HWMProvider,
                SCDMetricsProvider,
                SourceMetricsProvider,
            )

            def _unwrap(t: Any) -> Any:
                # ConditionalTransformer wraps a single fixed inner ProcessTransformer.
                if isinstance(t, ConditionalTransformer):
                    return t._transformer
                # ConditionalSourceTransformer picks between two SourceTransformers
                # at runtime (e.g. .set_source(..., DQDTransformer(), ...)) — its
                # HWM/metrics live on whichever one selected_source points to, not
                # on the wrapper itself (it doesn't implement HWMProvider).
                if isinstance(t, ConditionalSourceTransformer) and t.selected_source is not None:
                    return t.selected_source
                return t

            # Counts that are not None prove the transformer executed (a provider declares
            # them None in __init__ and sets them inside _run(), F117) — without this guard,
            # when multiple SCD transformers are registered via mutually-exclusive
            # .add_if() branches (e.g. NativeSCDAppendOnlyTransformer for append_only
            # and NativeSCDMergeTransformer for not append_only), next() picks
            # whichever was registered first regardless of which one's predicate
            # actually ran. The proof is the counts, not hwm_after: the post-write watermark
            # read may fail and be swallowed, and the counts are still real.
            scd_tx = next(
                (_unwrap(t) for t in self._transformers
                 if isinstance(_unwrap(t), SCDMetricsProvider)
                 and _unwrap(t).inserted is not None
                 and _unwrap(t).updated is not None),
                None,
            )
            if scd_tx is not None:
                inserted = scd_tx.inserted
                updated = scd_tx.updated

            # F138: with a load-context hwm_context, the step that reported for its column
            # owns the watermark even when it found none (a first run that staged nothing):
            # falling through to the writer or the target would report a load time from
            # another column, and fed back as hwm_value it would skip older source rows.
            hwm_owner = ctx.hwm_ctx.hwm_column_name if ctx.hwm_ctx is not None else None
            hwm_tx = next(
                (_unwrap(t) for t in self._transformers
                 if hwm_owner is not None and isinstance(_unwrap(t), HWMProvider)
                 and _unwrap(t).hwm_column == hwm_owner),
                None,
            ) or next(
                (_unwrap(t) for t in self._transformers
                 if isinstance(_unwrap(t), HWMProvider) and _unwrap(t).hwm_after is not None),
                None,
            )
            hwm_before = hwm_tx.hwm_before if hwm_tx else None
            hwm_after = hwm_tx.hwm_after if hwm_tx else None

            # Unlike scd_tx, there's exactly one active source per run (picked by
            # ConditionalSourceTransformer at .set_source() time if used), so no
            # "which of several candidates actually ran" guard is needed here —
            # isinstance is enough. source_count/target_count may be None (not
            # computed, e.g. DQDTransformer without hwm_context) — collapse
            # to 0 here since ExecutionMetrics's fields are typed int, not
            # Optional[int]; see SourceMetricsProvider's docstring.
            source_tx = next(
                (_unwrap(t) for t in self._transformers if isinstance(_unwrap(t), SourceMetricsProvider)),
                None,
            )
            if source_tx is not None:
                source_count = source_tx.source_count or 0
                target_count = source_tx.target_count or 0

            if hwm_tx is None:
                # No provider ran (early exit or first load) — read current HWM directly.
                # hwm_before == hwm_after because nothing changed.
                from spark_quick_start.orchestration.transformers._pipeline import (
                    get_high_watermark,
                )
                target_path = ctx.global_ctx.target_table_path
                if self._adapter.delta_table_exists(self._spark, target_path):
                    try:
                        # A Data Vault table has no effective-from column; its load
                        # timestamp is the nearest thing (F159).
                        scd = ctx.scd_ctx
                        assert scd is not None  # validate_all() parsed it
                        current_hwm = get_high_watermark(
                            self._spark, target_path, self._adapter,
                            hwm_column=scd.load_ts_column if scd.is_data_vault else scd.effective_from_column,
                        )
                        hwm_before = current_hwm
                        hwm_after = current_hwm
                    except Exception:
                        pass

            # ── 12. Post-merge runtime log ───────────────────────────────────
            log_runtime(self._spark, "post-merge", logger)

            # ── 13. Finalise metrics ─────────────────────────────────────────
            end_dt = datetime.now(UTC)
            metrics.end_time = end_dt.isoformat()
            metrics.duration_seconds = (end_dt - start_dt).total_seconds()
            metrics.inserted_count = inserted
            metrics.updated_count = updated
            metrics.source_count = source_count
            metrics.target_count = target_count
            metrics.hwm_value_before = hwm_before
            metrics.hwm_value_after = hwm_after

            logger.info(
                f"LoadOrchestrator.run() complete"
                f" | hwm_before={hwm_before} | hwm_after={hwm_after}"
                f" | duration={metrics.duration_seconds:.2f}s"
            )
            _success = True

        except Exception as exc:
            tb = traceback.format_exc()
            try:
                logger.error(
                    f"LoadOrchestrator.run() failed: {type(exc).__name__} — {exc}\n{tb}"
                )
            except Exception:
                pass
            end_dt = datetime.now(UTC)
            metrics.end_time = end_dt.isoformat()
            metrics.duration_seconds = (end_dt - start_dt).total_seconds()
            _error_details = ErrorDetails.from_exception(exc, tb).to_dict()

        finally:
            # Release first, so a failed release's warning is in the log that follows.
            self._release_cached(logger)
            # Always finalize logs — exactly once, regardless of success or failure.
            # Mirrors the legacy notebook pattern: finalize_logs(success=_success)
            # called in the finally block so logs are never lost.
            try:
                logs_json = logger.finalize(success=_success)
            except Exception:
                logs_json = ""

        if _success:
            return ExecutionResult(
                status="success",
                has_error=False,
                metrics=asdict(metrics),
                logs=logs_json,
                correlation_id=correlation_id,
                early_exit=_early_exit,
                early_exit_reason=_early_exit_reason,
                stages=collector.stages,
            )
        return ExecutionResult(
            status="error",
            has_error=True,
            error_details=_error_details,
            metrics=asdict(metrics),
            logs=logs_json,
            correlation_id=correlation_id,
            stages=collector.stages,
        )

    # ── Private helpers ───────────────────────────────────────────────────────

    def _run_pipeline(self, ctx, logger, collector: StageMetricsCollector) -> PipelineState:
        """Execute the transformer list and return the final PipelineState.

        Validates pipeline structure, wires spark/adapter/logger into base-class
        transformers, calls produce() on the source, then chains all
        process transformers in order. Plain callables with signature
        (DataFrame, LoadContext) -> DataFrame are shimmed automatically.
        Raises ValueError on structural errors; other exceptions propagate
        to run() which wraps them.
        """
        from spark_quick_start.orchestration.transformers.base import (
            LoadContext,
            ProcessTransformer,
            SourceTransformer,
        )

        if not self._transformers:
            raise ValueError("_run_pipeline called with empty transformer list.")

        source = self._transformers[0]
        if not isinstance(source, SourceTransformer):
            raise ValueError(
                "LoadOrchestrator: first transformer must be a SourceTransformer, "
                f"got {type(source).__name__}. Add a StagingTransformer or "
                "DQDTransformer as the first .add() call."
            )

        for i, t in enumerate(self._transformers[1:], start=1):
            if isinstance(t, SourceTransformer):
                raise ValueError(
                    f"LoadOrchestrator: SourceTransformer found at position {i} "
                    "(only position 0 is valid for source transformers)."
                )

        # F156: an SCD step under a Data Vault style is refused here, before the source runs
        # staging notebooks or reads anything. A step inside .add_if() may never run, so it is
        # left to its own check at the start of _run.
        for t in self._transformers:
            if getattr(type(t), "scd_style_only", False):
                ctx.scd_ctx.require_scd_style(type(t).__name__)
            data_vault_style = getattr(type(t), "data_vault_style", None)
            if data_vault_style is not None:
                ctx.scd_ctx.require_data_vault_style(type(t).__name__, data_vault_style)

        load_ctx = LoadContext(
            global_ctx=ctx.global_ctx,
            staging_ctx=ctx.staging_ctx,
            scd_ctx=ctx.scd_ctx,
            runtime_ctx=ctx.runtime_ctx,
            logging_ctx=ctx.logging_ctx,
            hwm_ctx=ctx.hwm_ctx,
            dqd_ctx=ctx.dqd_ctx,
            extensions_ctx=ctx.extensions_ctx,
            platform_ctx=ctx.platform_ctx,
        )

        for i, t in enumerate(self._transformers):
            if isinstance(t, (SourceTransformer, ProcessTransformer)):
                t._spark = self._spark
                t._adapter = self._adapter
                t._logger = logger
                t._metrics_collector = collector
                t._stage_index = i
                if hasattr(t, '_ctx_manager'):
                    t._ctx_manager = ctx

        state = source.produce(load_ctx)
        self._last_state = state

        for t in self._transformers[1:]:
            if isinstance(t, ProcessTransformer):
                state = t(state, load_ctx)
            else:
                # Plain callable shim: (DataFrame, LoadContext) -> DataFrame
                if not callable(t):
                    raise TypeError(
                        f"LoadOrchestrator: transformer at position "
                        f"{self._transformers.index(t)} is neither a "
                        f"ProcessTransformer nor a callable, got {type(t).__name__}."
                    )
                state = state.with_df(t(state.df, load_ctx))
            self._last_state = state

        return state

    def _release_cached(self, logger: OrchestratorLogger) -> None:
        """Unpersist every frame the last state saw declared in ``PipelineState.cached``.

        Called from ``run()``'s ``finally`` on every outcome. Spark cost: one asynchronous
        ``unpersist()`` per cached frame, no action.

        Reached only from ``run()``'s ``finally``, so it is part of ``run()``'s swallow point
        (errors-and-logging), not a second one: an ``unpersist`` that raises is logged as a
        warning and the next frame is still released, because freeing memory must never replace
        the run's real result or error.
        """
        state = self._last_state
        self._last_state = None
        if state is None:
            return
        for frame in state.cached:
            try:
                frame.unpersist()
            except Exception as exc:
                # Logging a release failure must not mask the run's outcome either.
                with contextlib.suppress(Exception):
                    logger.warning(
                        f"LoadOrchestrator: could not release a cached frame: "
                        f"{type(exc).__name__}: {exc}",
                        exc_info=True,
                    )

    @staticmethod
    def _resolve_adapter(platform: str) -> PlatformAdapter:
        """Resolve a PlatformAdapter from a platform string via lazy import.

        Args:
            platform: One of "fabric", "databricks", "local".

        Returns:
            Instantiated PlatformAdapter for the given platform.

        Raises:
            ValueError: If the platform string is not recognised, or if its adapter
                module is absent because this is a platform-filtered build.
        """
        dotted = _PLATFORM_MAP.get(platform.lower())
        if dotted is None:
            raise ValueError(
                f"LoadOrchestrator: unknown platform {platform!r}. "
                f"Valid values: {sorted(_PLATFORM_MAP.keys())}"
            )
        module_path, class_name = dotted.rsplit(".", 1)
        try:
            mod = importlib.import_module(module_path)
        except ModuleNotFoundError as exc:
            # `adapters/__init__.py` degrades gracefully when an adapter file has been
            # stripped; this path did not, and it is the only one that matters for a
            # platform-filtered build. scripts/filter_platform_release.sh physically
            # deletes the other platform's adapter, so a context asking for it here
            # produced a bare "No module named
            # spark_quick_start.adapters.databricks_adapter" — true, and silent about
            # the one cause that explains it. This is the exact scenario
            # dual-shipping.md is written for (F5).
            #
            # Re-raised as ValueError rather than a new exception type: the unknown-
            # platform case above already raises ValueError for the same class of
            # problem — this context names a platform this build cannot serve — and a
            # caller that handles one should not have to learn a second.
            raise ValueError(
                f"LoadOrchestrator: no adapter module for platform {platform!r} "
                f"({module_path}). This is expected on a platform-filtered build, "
                "where scripts/filter_platform_release.sh deletes the other platform's "
                "adapter — a fabric-only build cannot serve platform='databricks' and "
                "vice versa. Check the load context's platform against the build you "
                f"deployed. Valid values for a complete build: "
                f"{sorted(_PLATFORM_MAP.keys())}"
            ) from exc
        return getattr(mod, class_name)()


__all__ = ["LoadOrchestrator"]
