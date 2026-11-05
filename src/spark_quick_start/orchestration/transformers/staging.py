"""
staging.py — StagingTransformer.

Wraps the staging notebook execution branch previously in
LoadOrchestrator._execute_data_acquisition() (Branch A).

StagingTransformer requires a LoadContextManager instance (it calls
ctx_manager.build_staging_phase_context(idx, nb_name)). The pipeline
runner wires _ctx_manager at .run() time alongside _spark/_adapter.

Structural exception: this transformer intentionally imports
notebook_executor.NotebookExecutor and staging_pipeline.execute_staging_phases
from the wider orchestration layer. These are orchestration wiring, not data
pipeline logic. StagingTransformer bridges the transformer pipeline and the
notebook execution layer. If full isolation is ever needed, inject executor
and staging_phases_fn as callables at __init__ time instead.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from spark_quick_start.orchestration.notebook_executor import NotebookExecutor
from spark_quick_start.orchestration.staging_pipeline import execute_staging_phases
from spark_quick_start.orchestration.transformers._pipeline import (
    load_staged_dataframe,
    validate_and_merge_staging_tables,
)
from spark_quick_start.orchestration.transformers.base import LoadContext, SourceTransformer
from spark_quick_start.orchestration.transformers.state import PipelineState

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from spark_quick_start.orchestration.context_manager import LoadContextManager


class StagingTransformer(SourceTransformer):
    """Execute staging notebooks and return the merged staged DataFrame.

    Supports sequential, parallel, and batched-parallel execution modes
    as configured in the LoadContextManager's runtime_ctx.

    Requires _ctx_manager to be wired by the pipeline runner before produce()
    is called. _ctx_manager provides build_staging_phase_context() used by
    execute_staging_phases.

    After produce(): .source_count/.target_count are set from the sum of
    every staging notebook's own metrics (each notebook computes these for
    free from its write's Delta commit metadata — see
    delta_utils.write_staging_data — no extra cost to capture here).
    LoadOrchestrator reads them via the SourceMetricsProvider scan, same
    pattern as HWMProvider.

    It is also an HWMProvider (F138). When the load context has an `hwm_context`, it reports
    `hwm_before` as its `hwm_value` and `hwm_after` as the latest `hwm_column_name` among the
    staged rows, keeping `hwm_before` when nothing was staged. That costs one Spark action, a
    single `max` over the staged rows, and only when `hwm_context` is set. Without it all
    three attributes stay None, so the orchestrator takes the watermark from a later
    provider, as before.
    """

    _ctx_manager: LoadContextManager | None = None

    def __init__(self) -> None:
        self.source_count: int | None = None
        self.target_count: int | None = None
        # HWMProvider: declared always, set only in _produce (rule: transformer-contract).
        self.hwm_before: str | None = None
        self.hwm_after: str | None = None
        self.hwm_column: str | None = None

    def _produce(self, ctx: LoadContext) -> PipelineState:
        from spark_quick_start.logger import OrchestratorLogger

        logger = self._logger or OrchestratorLogger()
        executor = NotebookExecutor(self._adapter, logger)

        table_paths, self.source_count, self.target_count = execute_staging_phases(
            self._ctx_manager, executor, logger
        )

        # staging_table_schema is required whenever there are staging stages: validate_all()
        # refuses a context without it before any notebook runs (F107), so the merge is unconditional.
        validate_and_merge_staging_tables(
            self._spark,
            table_paths,
            self._ctx_manager.global_ctx.staging_table_schema,
            self._ctx_manager.global_ctx.staging_table_path,
            self._adapter,
            logger,
        )

        df = load_staged_dataframe(
            self._spark, self._ctx_manager.global_ctx.staging_table_path, self._adapter, logger
        )
        self._report_watermark(df, ctx)
        return PipelineState(df=df)

    def _report_watermark(self, df: DataFrame, ctx: LoadContext) -> None:
        """Set the HWMProvider attributes from `hwm_context`, when the load has one.

        One Spark action: `max(hwm_column_name)` over the staged rows, formatted by Spark with
        `.cast("string")`, so a timestamp keeps the session time zone it is read back in.
        """
        hwm = ctx.hwm_ctx
        if hwm is None:
            self.hwm_before = self.hwm_after = self.hwm_column = None
            return
        column = hwm.hwm_column_name
        resolved = self._staged_column(df, column)
        self.hwm_column = column
        self.hwm_before = None if hwm.hwm_value is None else str(hwm.hwm_value)
        quoted = "`" + resolved.replace("`", "``") + "`"
        latest = df.agg(F.max(F.col(quoted)).cast("string")).collect()[0][0]
        self.hwm_after = self.hwm_before if latest is None else latest

    def _staged_column(self, df: DataFrame, column: str) -> str:
        """The staged column `column` names, matched as the session matches names."""
        case_sensitive = False
        if self._spark is not None:
            case_sensitive = str(self._spark.conf.get("spark.sql.caseSensitive", "false")).lower() == "true"
        matches = [c for c in df.columns if c == column] or (
            [] if case_sensitive else [c for c in df.columns if c.lower() == column.lower()])
        if len(matches) != 1:
            raise ValueError(
                f"hwm_context: hwm_column_name '{column}' is "
                f"{'not a column' if not matches else 'more than one column'} of the staged rows; "
                f"they have {df.columns}"
            )
        return matches[0]
