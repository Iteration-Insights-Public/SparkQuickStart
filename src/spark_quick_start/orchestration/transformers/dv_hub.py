"""
dv_hub.py — DVHubTargetTransformer, DVHubWriterTransformer.

Contains: the two steps of a Data Vault hub load (the accepted design of 2026-10-01): reading
    the hub's keys the batch carries, and inserting the keys it lacks.
Does NOT contain: the hash key, which the staged rows carry under `business_key_column`, built
    upstream (decision 5); the satellite (dv_satellite.py); or a factory (pipeline_factory.py).

A hub holds one row per business key, ever: no update, no delete (decision 9, canonical Data
Vault 2.0). Several sources load one hub; each run inserts the keys the hub does not have yet,
stamping the source that saw the key first as its record source.
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_quick_start.orchestration.transformers._constants import Constants
from spark_quick_start.orchestration.transformers._hashing import (
    check_hash_seal,
    hash_settings,
    write_hash_seal,
)
from spark_quick_start.orchestration.transformers._pipeline import (
    capture_merge_statistics,
    capture_write_statistics,
    execute_dv_insert_only,
)
from spark_quick_start.orchestration.transformers.base import LoadContext, ProcessTransformer
from spark_quick_start.orchestration.transformers.state import PipelineState

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from spark_quick_start.adapters.platform_adapter import PlatformAdapter

__all__ = [
    "DVHubTargetTransformer",
    "DVHubWriterTransformer",
    "insert_new_keys",
    "keys_in_target",
    "stamp_data_vault_columns",
]

_STYLE = "dv_hub"


class DVHubTargetTransformer(ProcessTransformer):
    """Read the hub's keys that the staged batch carries into `state.frames["target"]`.

    A semi-join against the staged keys whatever the `load_type`: a hub never deletes, so a key
    absent from the batch means nothing and the hub is never read as a whole (the owner,
    2026-10-01). When the table does not exist yet the key is absent and `state.target_exists` is False (F28). Lazy: no Spark action here; the read
    and the semi-join run inside the writer's anti-join.
    """

    data_vault_style = _STYLE

    def _run(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
        ctx.scd_ctx.require_data_vault_style(type(self).__name__, _STYLE)
        frame = keys_in_target(self._adapter, self._spark, ctx, state.df)
        return state.with_target(frame)


class DVHubWriterTransformer(ProcessTransformer):
    """Insert the staged keys the hub lacks, stamped with the load columns; never update.

    The rows are the staged rows whose key the target frame does not hold, every staged column
    kept (the hub's source step selects the key and the business key columns), plus
    `RecordSource`, `LoadTS`, `LoadEndTS`, the correlation id and, when named, the pipeline run
    id. The hash settings are sealed on the table at its first write and checked on every later
    one (F158). The write is an insert-only MERGE, or `write_delta_create`, which refuses a table
    already there.

    Reports `inserted` from the write's own Delta commit, `updated` 0 (`SCDMetricsProvider`).
    As an `HWMProvider` it reports this run's `LoadTS` as `hwm_after` on the load-timestamp
    column, and no `hwm_before`: a hub has no watermark of its own, and with a load-context
    `hwm_context` the source step owns the run's watermark anyway (F138). No extra read.

    `LoadTS` is the run's load time, `global_context.load_timestamp` (F106): when the key was
    first seen. A hub takes any load time, a backfilled one included, because an insert-only
    write for keys the hub lacks cannot put history behind a row it holds. The satellite can,
    and does not refuse one either: keeping its load times increasing is the user's
    responsibility (`DVSatelliteWriterTransformer`). `LoadEndTS` stays the clock at stamping,
    so with a backfilled load time `LoadEndTS - LoadTS` is not a duration.

    The anti-join against the target frame is not needed for correctness, which the insert-only
    MERGE holds on its own; it keeps the MERGE's source to the batch's new keys, so the MERGE
    inserts what it is given and touches no more of the hub than the batch's keys.

    Cost: one existence check and one metadata read for the seal (plus one `isEmpty()` on an
    unsealed existing table), the anti-join and write, and one read of the commit's metrics.
    """

    data_vault_style = _STYLE

    def __init__(self) -> None:
        self.inserted: int | None = None  # None until _run sets it (F117)
        self.updated: int | None = None
        self.hwm_before: str | None = None
        self.hwm_after: str | None = None
        self.hwm_column: str | None = None

    def _run(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
        ctx.scd_ctx.require_data_vault_style(type(self).__name__, _STYLE)
        self.inserted, load_ts = insert_new_keys(self._adapter, self._spark, self._logger, ctx, state)
        self.updated = 0
        self.hwm_column = ctx.scd_ctx.load_ts_column
        self.hwm_before = None
        self.hwm_after = load_ts.strftime("%Y-%m-%d %H:%M:%S.%f")
        return state


def keys_in_target(adapter: PlatformAdapter | None, spark: SparkSession | None, ctx: LoadContext,
                   staged: DataFrame) -> DataFrame | None:
    """The target's keys that *staged* carries, or `None` when the target does not exist yet.

    Shared by the hub and link target steps (F168): both are keyed by `business_key_column`
    alone and never delete. A semi-join; no Spark action.
    """
    assert adapter is not None and spark is not None
    target = _target(ctx)
    if not adapter.delta_table_exists(spark, target):
        return None
    key = ctx.scd_ctx.business_key_column
    batch_keys = staged.select(key).distinct()
    return adapter.read_delta(spark, target).select(key).join(batch_keys, key, "left_semi")


def insert_new_keys(adapter: PlatformAdapter | None, spark: SparkSession | None, logger: Any,
                    ctx: LoadContext, state: PipelineState) -> tuple[int, datetime]:
    """Insert the staged rows whose key the target lacks; return (inserted, the run's LoadTS).

    Shared by the hub and link writers (F168): seal check, anti-join against the target frame,
    the load columns stamped, the insert-only write on the key alone with its retry (safe: the
    MERGE's own predicate re-checks the live table), the commit's count, then the seal.
    """
    from spark_quick_start.logger import OrchestratorLogger
    logger = logger or OrchestratorLogger()
    assert adapter is not None and spark is not None
    s = ctx.scd_ctx
    target = _target(ctx)
    key = s.business_key_column

    settings = hash_settings(s, spark)
    needs_seal = check_hash_seal(adapter, spark, target, settings)

    existing = state.frames.get(Constants.TARGET_FRAME_KEY)
    new_keys = state.df if existing is None else state.df.join(existing, key, "left_anti")
    load_ts = ctx.global_ctx.load_timestamp_utc
    rows = stamp_data_vault_columns(new_keys, ctx, load_ts)

    pre_version, created = execute_dv_insert_only(spark, target, rows, [key], adapter, logger)
    if created:
        inserted = capture_write_statistics(spark, target, pre_version, adapter, logger)
    else:
        inserted = capture_merge_statistics(spark, target, pre_version, adapter, logger)[0]
    if needs_seal:
        write_hash_seal(adapter, spark, target, settings)
    logger.info(f"{s.scd_type}: {inserted} new key(s) in {target}.")
    return inserted, load_ts


def _target(ctx: LoadContext) -> str:
    """The hub's table; `GlobalContext.validate` has refused a load context without one."""
    target = ctx.global_ctx.target_table_path
    if not target:
        raise ValueError("global_context.target_table_path is required for a Data Vault load")
    return target


def stamp_data_vault_columns(rows: DataFrame, ctx: LoadContext, load_ts: datetime) -> DataFrame:
    """Add the Data Vault load columns every hub and satellite row carries (F159).

    `RecordSource` comes from the rows under `source_system_from_source`, else from
    `global_context.source_system`; `LoadTS` is the run's load time, `global_context.load_timestamp`, and
    `LoadEndTS` this call's clock, both timezone-aware UTC literals (spark-delta-cost 6); then the correlation id and,
    when named, the pipeline run id. No Spark action.
    """
    s = ctx.scd_ctx
    if not s.source_system_from_source:
        rows = rows.withColumn(s.source_system_column, F.lit(ctx.global_ctx.source_system))
    rows = (rows
            .withColumn(s.load_ts_column, F.lit(load_ts))
            .withColumn(s.load_end_ts_column, F.lit(datetime.now(UTC)))
            .withColumn(s.correlation_id_column, F.lit(ctx.runtime_ctx.correlation_id)))
    if s.pipeline_run_id_column:
        rows = rows.withColumn(s.pipeline_run_id_column, F.lit(ctx.runtime_ctx.pipeline_run_id))
    return rows
