"""materialize.py — MaterializeFrameTransformer / MaterializeTransformer.

Both force a lazy DataFrame to materialize via a write-then-read round trip
through a scratch Delta table, instead of .cache()/.persist() (unavailable/
ineffective under Fabric serverless capacity). With
``runtime_context.materialize_strategy = "cache"`` and not serverless they persist the frame in
memory instead and write no table; on serverless that setting falls back to the round trip. _materialize() is the shared
drop-write-read primitive both transformers call; each transformer owns a
distinct scratch table, named by scratch_table_name():
`{staging_table_path}_{id}_{stage_index}_{name}`, where `id` is global_context.id (unique per
entity), stage_index the step's pipeline position and `name` a fixed token per transformer
(`materialize`, `materialize_{frame_key}`, `dqd`). DQDTransformer uses the same helper for its
eager_materialize_source scratch, at position 0.

Contains: scratch_table_name, MaterializeFrameTransformer, MaterializeTransformer.
Does NOT contain: a caller-chosen scratch path (F129: none can be passed).
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark import StorageLevel

from spark_quick_start.orchestration.transformers._constants import Constants
from spark_quick_start.orchestration.transformers.base import ProcessTransformer

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from spark_quick_start.adapters.platform_adapter import PlatformAdapter
    from spark_quick_start.logger import OrchestratorLogger
    from spark_quick_start.orchestration.transformers.base import LoadContext
    from spark_quick_start.orchestration.transformers.state import PipelineState


def scratch_table_name(ctx: LoadContext, stage_index: int, name: str) -> str:
    """The scratch table of one step: ``{staging_table_path}_{id}_{stage_index}_{name}``.

    The single place a scratch name is built (F129), used by both Materialize steps and by
    DQDTransformer. Two steps of one pipeline differ in ``stage_index`` or ``name``; two
    loads sharing a ``staging_table_path`` differ in ``global_context.id`` (compared ignoring case). No Spark work.

    Args:
        ctx: The load context, for ``staging_table_path`` and ``id``.
        stage_index: The step's position in the pipeline.
        name: A fixed lowercase token naming the kind of step.

    Raises:
        ValueError: if ``staging_table_path`` or ``id`` is not set (a validated context has both).
    """
    base, load_id = ctx.global_ctx.staging_table_path, ctx.global_ctx.id
    if not isinstance(base, str) or not base.strip() or not load_id:
        raise ValueError(
            "scratch table name needs global_context.staging_table_path and global_context.id; "
            f"got staging_table_path={base!r}, id={load_id!r}"
        )
    # Lower-cased: Unity Catalog and Hive compare names ignoring case, so `Cust_A` and `cust_a`
    # would be one table.
    return f"{base}_{str(load_id).lower()}_{stage_index}_{name}"


def _materialize(
    spark: SparkSession | None, adapter: PlatformAdapter | None, df: DataFrame, path: str,
) -> DataFrame:
    """Drop path, write df to it, and read it back — the shared eager-materialize step.

    spark/adapter are typed Optional because that is how ``_TransformerBase``
    declares them: the pipeline runner injects them at ``.run()`` time, so they
    are always set by the time a transformer's ``_run`` executes. Checking once
    here rather than at each call site keeps the guard in one place. Issue #34
    tracks dropping the Optional declarations across the board, at which point
    this check becomes redundant and the annotations can tighten again.

    Raises:
        RuntimeError: if called outside a pipeline run, before injection.
    """
    if spark is None or adapter is None:
        raise RuntimeError(
            "_materialize() called before spark/adapter were injected — a "
            "Materialize transformer was used outside LoadOrchestrator.run()."
        )
    # Drop rather than overwrite. A Delta overwrite only *marks* the previous
    # files removed; they stay on disk until VACUUM, so a scratch path grows by
    # a full copy every run — one was measured at 45 GB holding 2.3 GB of live
    # data across 22 versions. Dropping also discards the old schema outright,
    # so a column change during development needs no schema-evolution step.
    #
    # This is only safe because scratch_table_name() gives every step its own
    # path: the stage index and the step's token separate the steps of one
    # pipeline, global_context.id separates loads sharing a staging path.
    # Dropping a table that df is still reading from yields an EMPTY result,
    # silently and without raising (F129).
    adapter.drop_delta(spark, path)
    adapter.write_delta_overwrite(df, path)
    return adapter.read_delta(spark, path)


def _use_cache(ctx: LoadContext, logger: OrchestratorLogger | None) -> bool:
    """Whether this step caches in memory (True) or takes the Delta round trip (False).

    ``materialize_strategy == "cache"`` caches unless ``serverless``, where persisting is
    unavailable or ineffective: that falls back to the round trip and says so once, at INFO.
    No Spark work.
    """
    rt = ctx.runtime_ctx
    if rt.materialize_strategy != "cache":
        return False
    if rt.serverless:
        if logger is not None:
            logger.info(
                "runtime_context.materialize_strategy is 'cache' but serverless is true, where "
                "persist is unavailable or ineffective: falling back to the Delta round trip."
            )
        return False
    return True


def _cache_frame(df: DataFrame) -> DataFrame:
    """Persist *df* (MEMORY_AND_DISK) and fill the cache with one action; return it.

    Spark cost: one full pass over *df*'s lineage, the same pass the Delta round trip's write
    makes, minus the write and the read-back. The action is materialization, not a metrics
    count (spark-delta-cost rule 2): it is what makes later reads cheap. If it raises, the
    frame is unpersisted before the error propagates, so a failed fill leaks nothing.
    """
    df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        df.count()
    except BaseException:
        df.unpersist()
        raise
    return df


class MaterializeFrameTransformer(ProcessTransformer):
    """Force a named frame in PipelineState to materialize via a write-then-read
    round trip through Delta.

    Spark DataFrames are lazy — a frame read by multiple downstream transformers
    gets its whole lineage recomputed on each read. .cache()/.persist() would
    normally fix this, but those are unavailable/ineffective under Fabric
    serverless capacity. Writing forces eager computation once; the read-back
    DataFrame has a trivially cheap lineage (a small-file scan) no matter how
    many times it's triggered downstream.

    Passes through unchanged if the named frame is absent (e.g. the first-load
    case, where a target step has stated the target does not exist and left
    the key out of state.frames).

    See MaterializeTransformer for the primary-df equivalent of this transformer.

    The scratch table is ``{staging_table_path}_{id}_{stage_index}_materialize_{frame_key}``
    and cannot be chosen: see scratch_table_name().

    Spark cost: with ``runtime_context.materialize_strategy`` ``"delta"`` (the default), one write
    of the frame and one read-back, dropped and rewritten on every run. With ``"cache"`` off
    serverless, one action that fills an in-memory cache (``MEMORY_AND_DISK``): a full pass over
    the lineage, once, no table written or dropped. That action is materialization, not a metrics
    count. On serverless ``"cache"`` falls back to the round trip and logs it. The cached frame
    is declared with ``PipelineState.with_cached`` and released when ``LoadOrchestrator.run()``
    ends, not earlier.

    Args:
        frame_key: Name of the entry in state.frames to materialize. Defaults
            to "target".
    """

    def __init__(self, frame_key: str = Constants.TARGET_FRAME_KEY) -> None:
        self._frame_key = frame_key

    def _run(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
        df = state.frames.get(self._frame_key)
        if df is None:
            return state

        if _use_cache(ctx, self._logger):
            return state.with_frame(self._frame_key, _cache_frame(df)).with_cached(df)
        path = scratch_table_name(ctx, self._stage_index, f"materialize_{self._frame_key}")
        materialized = _materialize(self._spark, self._adapter, df, path)
        return state.with_frame(self._frame_key, materialized)


class MaterializeTransformer(ProcessTransformer):
    """Force the primary DataFrame (state.df) to materialize via a write-then-read
    round trip through Delta.

    Same mechanism as MaterializeFrameTransformer, applied to state.df instead of
    a named side frame. Typical placement: right after the last transform-heavy
    stage (e.g. SCDChangeDetectorTransformer, which computes scdHash/scdVersion and
    may sit downstream of an expensive source read) and before any transformer that reads
    state.df more than once — e.g. CandidateGuardTransformer's duplicate-key
    check, followed by the final merge/append write. Without materializing,
    each of those reads independently re-walks the *entire* upstream lineage
    (source scan, joins, hash computation) from scratch.

    This is a genuine cost trade-off, not a strict win, which is why it's opt-in
    rather than wired into default_pipeline(): materializing
    trades "recompute the transform chain N times" for "1 scratch write + N cheap
    reads." For a small/cheap source (a lightly-transformed few-thousand-row
    dimension table), the fixed cost of a Delta write-then-read round trip (log
    commit, file listing) can exceed what it saves. For a large or
    expensively-transformed source (tens of millions of rows, heavy staging
    queries, wide hash computation), the recomputation avoided dominates and this
    is a clear win. Judge it per-pipeline based on source size and transform
    complexity — there's no universal threshold.

    Scratch table lifecycle
    -----------------------
    The scratch table is dropped and recreated on every run rather than
    overwritten. A Delta overwrite leaves every superseded file on disk until
    VACUUM runs, so an overwrite-only scratch path grows by a full copy per
    run — one was measured at 45 GB holding 2.3 GB of live data. Dropping also
    discards the previous schema, so adding or retyping a column mid-development
    needs no schema-evolution handling.

    The scratch table is ``{staging_table_path}_{id}_{stage_index}_materialize`` and
    cannot be chosen (F129). Two steps writing one scratch table would have the
    second drop the table its own input is still reading, and the write would
    produce an EMPTY table, silently and without raising: CandidateGuardTransformer
    would report zero candidates and halt the pipeline as a clean success.
    The stage index and token keep the steps of one pipeline apart; ``id``
    (global_context.id, lower-cased; unique, ignoring case, among the contexts sharing a staging
    path) keeps those loads apart; two concurrent runs of one context still share scratch tables.

    Spark cost: with ``runtime_context.materialize_strategy`` ``"delta"`` (the default), one write
    of the frame and one read-back, dropped and rewritten on every run. With ``"cache"`` off
    serverless, one action that fills an in-memory cache (``MEMORY_AND_DISK``): a full pass over
    the lineage, once, no table written or dropped. That action is materialization, not a metrics
    count. On serverless ``"cache"`` falls back to the round trip and logs it. The cached frame
    is declared with ``PipelineState.with_cached`` and released when ``LoadOrchestrator.run()``
    ends, not earlier.
    """

    def _run(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
        if _use_cache(ctx, self._logger):
            return state.with_df(_cache_frame(state.df)).with_cached(state.df)
        path = scratch_table_name(ctx, self._stage_index, "materialize")
        materialized = _materialize(self._spark, self._adapter, state.df, path)
        return state.with_df(materialized)
