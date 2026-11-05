"""
target.py — SCDTargetTableTransformer, SCDIncrementalTargetTableTransformer.

Loads the current Silver/target DataFrame and stashes it in
PipelineState.frames["target"] for use by SCDChangeDetectorTransformer.
"""
from __future__ import annotations

from spark_quick_start.orchestration.transformers._aliases import SCD_RENAMES, deprecated_aliases
from spark_quick_start.orchestration.transformers._pipeline import load_target_dataframe
from spark_quick_start.orchestration.transformers.base import LoadContext, ProcessTransformer
from spark_quick_start.orchestration.transformers.state import PipelineState


class SCDTargetTableTransformer(ProcessTransformer):
    """Load the target (Silver) table and stash it as frames["target"].

    The primary DataFrame (staged data) passes through unchanged.
    SCDChangeDetectorTransformer reads frames["target"] in the next step. When
    the table does not exist the key is left out and ``state.target_exists`` is
    False, set by ``PipelineState.with_target(None)`` (F28); an existing but
    empty table is a present frame and ``target_exists`` is True.

    Example usage::

        transformer = SCDTargetTableTransformer()
        transformer._spark = spark
        state = transformer(state, ctx)
        # state.frames["target"] is now the Silver Delta table DataFrame
        # state.target_exists is True; state.df is unchanged (the staged DataFrame)
    """

    # F156: refused under a Data Vault style, at composition (LoadOrchestrator) and in _run.
    scd_style_only = True

    def _run(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
        """Load the target table and return updated state with frames["target"] set.

        Args:
            state: Current pipeline state carrying the staged DataFrame as ``state.df``.
            ctx: Load context supplying ``scd_ctx`` and ``global_ctx.target_table_path``.

        Returns:
            A new :class:`PipelineState` with ``frames["target"]`` set to the Silver
            DataFrame and ``target_exists`` True, or, when the table does not exist,
            no ``"target"`` key and ``target_exists`` False; ``df`` left unchanged.
        """
        ctx.scd_ctx.require_scd_style(type(self).__name__)
        from spark_quick_start.logger import OrchestratorLogger

        logger = self._logger or OrchestratorLogger()
        _, target_df = load_target_dataframe(
            self._spark, ctx.scd_ctx,
            ctx.global_ctx.target_table_path, self._adapter, logger,
        )
        # target_df is None on first load (table not yet created): with_target(None)
        # leaves the key out and states target_exists=False, which
        # SCDChangeDetectorTransformer reads as an empty target.
        return state.with_target(target_df)


class SCDIncrementalTargetTableTransformer(ProcessTransformer):
    """Load the target table, filtered to (business_key, source_system) values
    present in the incoming staged data.

    Incremental loads only — raises if load_type == 'Full'. Filtering would
    silently break full-load delete detection: SCDChangeDetectorTransformer finds
    deletes under a Full load via a left_anti join against target keys NOT
    present in the incoming data; pre-filtering the target to only keys that
    ARE present would make that anti-join permanently empty.

    Args:
        broadcast: When True (default), the incoming-keys side of the join is
            wrapped in F.broadcast() — deterministic, doesn't depend on Delta
            table statistics for Spark's auto-broadcast-threshold heuristic.
            When False, no hint is applied; Spark's own
            spark.sql.autoBroadcastJoinThreshold / AQE decides the strategy.
    """

    # F156: refused under a Data Vault style, at composition (LoadOrchestrator) and in _run.
    scd_style_only = True

    def __init__(self, *, broadcast: bool = True) -> None:
        self._broadcast = broadcast

    def _run(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
        ctx.scd_ctx.require_scd_style(type(self).__name__)
        from pyspark.sql import functions as F

        from spark_quick_start.logger import OrchestratorLogger

        logger = self._logger or OrchestratorLogger()
        s = ctx.scd_ctx

        if s.load_type == "Full":
            raise ValueError(
                f"{type(self).__name__} cannot be used with load_type='Full' — "
                "filtering the target to incoming keys would silently break "
                "full-load delete detection (deletes are found via target keys "
                "NOT present in the incoming data — see SCDChangeDetectorTransformer's "
                "left_anti join). Use SCDTargetTableTransformer for Full loads instead."
            )

        _, target_df = load_target_dataframe(
            self._spark, s, ctx.global_ctx.target_table_path, self._adapter, logger,
        )
        if target_df is None:
            # First load — table doesn't exist yet, nothing to filter; state it absent.
            return state.with_target(None)

        bk, ss = s.business_key_column, s.source_system_column
        if s.source_system_from_source:
            incoming_keys = state.df.select(bk, ss).distinct()
        else:
            # Mirror SCDChangeDetectorTransformer's own stamping (change_detector.py) —
            # ss doesn't exist on state.df yet at this point in the pipeline.
            incoming_keys = state.df.select(bk).distinct().withColumn(ss, F.lit(ctx.global_ctx.source_system))

        if self._broadcast:
            incoming_keys = F.broadcast(incoming_keys)

        filtered_target = target_df.join(incoming_keys, on=[bk, ss], how="left_semi")
        logger.info(f"{type(self).__name__}: target filtered to incoming keys.")
        return state.with_target(filtered_target)


# F155: the pre-rename names still resolve, with a DeprecationWarning (see _aliases.py).
__getattr__ = deprecated_aliases(__name__, globals(), SCD_RENAMES)
