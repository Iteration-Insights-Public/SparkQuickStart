"""
dv_satellite.py — the Data Vault satellite's steps.

Contains: DVSatelliteTargetTransformer and DVSatelliteIncrementalTargetTransformer, which read
    the latest satellite row per key into `state.frames["target"]` (F160), and
    DVSatelliteChangeDetectorTransformer, which classifies the staged rows against it (F161).
    DVSatelliteWriterTransformer, which inserts its rows (F162).
Does NOT contain: the hub (dv_hub.py) or the hash diff and its seal (_hashing.py).

A satellite is insert-only (the accepted design, decision 9): a change, a delete and a
reappearance are each a new row, and a key's current state is its row with the latest `LoadTS`.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spark_quick_start.adapters.platform_adapter import PlatformAdapter
from spark_quick_start.orchestration.transformers._constants import (
    CHANGE_TYPE,
    INTERNAL_COLUMNS,
    Constants,
)
from spark_quick_start.orchestration.transformers._hashing import (
    check_hash_seal,
    hash_hex,
    hash_settings,
    write_hash_seal,
)
from spark_quick_start.orchestration.transformers._pipeline import (
    capture_merge_statistics,
    capture_write_statistics,
    execute_dv_insert_only,
)
from spark_quick_start.orchestration.transformers.base import LoadContext, ProcessTransformer
from spark_quick_start.orchestration.transformers.dv_hub import stamp_data_vault_columns
from spark_quick_start.orchestration.transformers.state import PipelineState

__all__ = [
    "DVSatelliteChangeDetectorTransformer",
    "DVSatelliteIncrementalTargetTransformer",
    "DVSatelliteTargetTransformer",
    "DVSatelliteWriterTransformer",
    "latest_rows",
]

_STYLE = "dv_satellite"
_LATEST = "_latest"


def _orderable(data_type: T.DataType) -> bool:
    """Whether Spark can order a value of this type: not a map or variant, nor one nested."""
    if isinstance(data_type, T.MapType) or data_type.typeName() == "variant":
        return False
    if isinstance(data_type, T.ArrayType):
        return _orderable(data_type.elementType)
    if isinstance(data_type, T.StructType):
        return all(_orderable(f.dataType) for f in data_type.fields)
    return True


def latest_rows(satellite: DataFrame, key: str, load_ts: str) -> DataFrame:
    """Each key's row with the latest load timestamp, every column kept.

    One aggregation, `max_by` with map-side partial aggregation and no join back (the
    delta-lake-specialist, 2026-10-01): a window would sort every row, and a `max` plus a join
    back reads twice and duplicates on a tie. The value is a struct of every column, of any
    type; the ordering is a struct of the load timestamp then the orderable columns in table
    order, because `max` refuses a struct holding a map (F160's review). A tie on the load
    timestamp is broken by those later columns, deterministically; a row with no load
    timestamp orders below every row with one. No Spark action: a plan.
    """
    rest = [c for c in satellite.columns if c not in (key, load_ts)]
    order = [load_ts, *(f.name for f in satellite.schema.fields if f.name in rest and _orderable(f.dataType))]
    latest = satellite.groupBy(key).agg(F.max_by(F.struct(load_ts, *rest), F.struct(*order)).alias(_LATEST))
    return latest.select(key, *[F.col(f"{_LATEST}.{c}").alias(c) for c in (load_ts, *rest)])


def _target_latest(adapter: PlatformAdapter | None, spark: SparkSession | None, ctx: LoadContext,
                   batch: DataFrame | None) -> DataFrame | None:
    """The satellite's latest rows, for the keys of `batch` when one is given; `None` when the
    satellite does not exist yet, which the caller states with `with_target(None)` (F28)."""
    assert adapter is not None and spark is not None
    target = ctx.global_ctx.target_table_path
    if not target:
        raise ValueError("global_context.target_table_path is required for a Data Vault load")
    if not adapter.delta_table_exists(spark, target):
        return None
    s = ctx.scd_ctx
    satellite = adapter.read_delta(spark, target)
    if batch is not None:
        satellite = satellite.join(batch.select(s.business_key_column).distinct(), s.business_key_column, "left_semi")
    return latest_rows(satellite, s.business_key_column, s.load_ts_column)


class DVSatelliteTargetTransformer(ProcessTransformer):
    """The latest row of every key in the satellite, into `state.frames["target"]`.

    The step for a `Full` load, which needs the keys absent from the source to record their
    deletion. Cost: one aggregation over the whole satellite, growing with its history, where
    an SCD target reads current rows by a flag; measure before reaching for a helper table
    (the design). When the satellite does not exist yet the key is absent and
    `state.target_exists` is False (F28).
    """

    data_vault_style = _STYLE

    def _run(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
        ctx.scd_ctx.require_data_vault_style(type(self).__name__, _STYLE)
        frame = _target_latest(self._adapter, self._spark, ctx, batch=None)
        return state.with_target(frame)


class DVSatelliteIncrementalTargetTransformer(ProcessTransformer):
    """As `DVSatelliteTargetTransformer`, for the keys the staged batch carries only.

    Semi-joins the satellite to the staged keys before the aggregation, so its cost follows
    the batch, not the history. Refuses a `Full` load, which needs every key's latest row, as
    `SCDIncrementalTargetTableTransformer` does.
    """

    data_vault_style = _STYLE

    def _run(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
        s = ctx.scd_ctx
        s.require_data_vault_style(type(self).__name__, _STYLE)
        if s.load_type == "Full":
            raise ValueError(
                f"{type(self).__name__} reads only the batch's keys, and a Full load needs every "
                "key's latest row to find the ones absent from the source: use "
                "DVSatelliteTargetTransformer."
            )
        frame = _target_latest(self._adapter, self._spark, ctx, batch=state.df)
        return state.with_target(frame)


class DVSatelliteChangeDetectorTransformer(ProcessTransformer):
    """Classify the staged rows against each key's latest satellite row (the design's table).

    Reads `state.df` and the target frame of either satellite target step. Writes the rows the
    satellite must take, each with its hash diff, its deleted flag and `_change_type`:

    - a key the satellite lacks: `new`;
    - a key whose pair (hash diff, deleted flag) differs from its latest row, compared
      null-safely: `changed`, so a deleted key that reappears unchanged is `changed` too;
    - a source delete of a live key, or under `Full` a live key absent from the source:
      `deleted`, carrying the latest row's attributes and hash diff forward, so a reappearance
      is seen by the flag alone and a blank delete row never reads as an attribute change.

    Nothing for an equal pair, a delete of a key already deleted (as the SCD detector
    does since F142), or a delete of a key the satellite lacks. The hash diff is `hash_hex` over the staged
    descriptive columns in alphabetical order, leaving out the key, the control columns, the
    columns the load stamps and `exclude_from_hash`. `RecordSource` is this run's source, as the
    writer stamps it, unless `source_system_from_source`.

    Cost: no action on the target; an absent or empty frame joins to nothing, so every live row
    is `new`. Under `Full` only, one `isEmpty` on the source, to refuse deleting every live key
    the satellite holds, and when the source is empty one on the satellite's live rows. A shared
    column typed differently in the staged rows and the satellite is refused, from the schemas.
    """

    data_vault_style = _STYLE

    def _run(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
        s = ctx.scd_ctx
        s.require_data_vault_style(type(self).__name__, _STYLE)
        assert self._spark is not None
        key, ss, flag = s.business_key_column, s.source_system_column, s.deleted_flag_column
        written = [s.hash_column, s.load_ts_column, s.load_end_ts_column]

        staged = state.df
        collisions = [c for c in written if c in staged.columns]
        if collisions:
            raise ValueError(
                f"{type(self).__name__}: the staged DataFrame carries {collisions}, which the load "
                "writes itself. Remove them upstream."
            )
        required = [key, s.source_deleted_flag_column, ss if s.source_system_from_source else None]
        missing = [c for c in required if c is not None and c not in staged.columns]
        if missing:
            raise ValueError(
                f"{type(self).__name__}: required source column(s) missing from the staged "
                f"DataFrame: {missing}. Available: {list(staged.columns)}"
            )

        # The delete signal, as the SCD detector reads it (F127): the named column, else a staged
        # deleted flag; a row is deleted only where it equals source_deleted_flag_value, null-safely.
        staged_flag = next((c for c in staged.columns if c.lower() == flag.lower()), None)
        signal = s.source_deleted_flag_column or staged_flag
        control = {flag} | {c for c in (staged_flag, signal) if c}
        is_signalled = (F.coalesce(F.col(signal) == F.lit(s.source_deleted_flag_value), F.lit(False))
                        if signal else F.lit(False))

        src = staged if s.source_system_from_source else staged.withColumn(ss, F.lit(ctx.global_ctx.source_system))
        unhashed = ({key, ss, s.row_key_column, s.correlation_id_column, s.pipeline_run_id_column}
                    | set(written) | control | set(s.exclude_from_hash))
        hashed = sorted(c for c in src.columns if c not in unhashed)
        src = src.withColumn(s.hash_column, hash_hex(hashed, hash_settings(s, self._spark)))
        columns = [c for c in src.columns if c not in control]

        live = src.filter(~is_signalled)
        tgt = state.frames.get(Constants.TARGET_FRAME_KEY)
        if tgt is None:
            return state.with_df(_classified(live.select(*columns), flag, s.not_deleted_flag_value, "new"))

        # A Data Vault table never evolves implicitly: a shared column typed differently would be
        # settled by the union's coercion, which differs between runtimes. Schema only, no action.
        tgt_types = {c.lower(): t for c, t in tgt.dtypes}
        drift = [c for c, t in src.select(*columns).dtypes if tgt_types.get(c.lower(), t) != t]
        if drift:
            raise ValueError(
                f"{type(self).__name__}: column(s) {drift} are typed differently in the staged rows "
                f"and in the satellite ({ {c: tgt_types[c.lower()] for c in drift} }). Cast them "
                "upstream to the satellite's types."
            )

        # Both delete paths are rows of the latest satellite row, the live ones only.
        tgt_live = tgt.filter(~F.col(flag).eqNullSafe(F.lit(s.deleted_flag_value)))
        if s.load_type == "Full" and src.isEmpty() and not tgt_live.isEmpty():
            raise ValueError(
                f"{type(self).__name__}: Full load aborted, the source has 0 rows and the satellite "
                "has live keys: every one would be recorded deleted. Fix staging before re-running."
            )

        latest = tgt.select(key, F.col(s.hash_column).alias("_tgt_diff"), F.col(flag).alias("_tgt_deleted"))
        joined = live.join(latest, key, "left")
        same_pair = (F.col(s.hash_column).eqNullSafe(F.col("_tgt_diff"))
                     & F.col("_tgt_deleted").eqNullSafe(F.lit(s.not_deleted_flag_value)))
        known = F.col("_tgt_diff").isNotNull() | F.col("_tgt_deleted").isNotNull()
        active = (joined.filter(~known | ~same_pair)
                  .withColumn(CHANGE_TYPE, F.when(known, F.lit("changed")).otherwise(F.lit("new")))
                  .withColumn(flag, F.lit(s.not_deleted_flag_value))
                  .select(*columns, flag, CHANGE_TYPE))

        signalled = src.filter(is_signalled).select(key, ss)
        deletes = tgt_live.drop(ss).join(signalled, key, "inner")
        if s.load_type == "Full":
            absent = tgt_live.join(src.select(key), key, "left_anti")
            if not s.source_system_from_source:
                absent = absent.withColumn(ss, F.lit(ctx.global_ctx.source_system))
            deletes = deletes.unionByName(absent.select(*deletes.columns))
        # Matched by name case-insensitively, as Spark and Delta match them; a staged column the
        # satellite lacks is NULL, and the writer refuses that drift before anything is written.
        types, have = dict(src.dtypes), {c.lower(): c for c in deletes.columns}
        carried = deletes.select(*[F.col(have[c.lower()]).alias(c) if c.lower() in have
                                   else F.lit(None).cast(types[c]).alias(c) for c in columns])
        return state.with_df(active.unionByName(_classified(carried, flag, s.deleted_flag_value, "deleted")))


def _classified(rows: DataFrame, flag: str, deleted: str, change: str) -> DataFrame:
    """*rows* with the deleted flag column and `_change_type` set to the given values."""
    return rows.withColumn(flag, F.lit(deleted)).withColumn(CHANGE_TYPE, F.lit(change))


class DVSatelliteWriterTransformer(ProcessTransformer):
    """Insert the detector's rows into the satellite, stamped with the load columns; never update.

    Strips the routing columns (`_change_type`), stamps `RecordSource`, `LoadTS`, `LoadEndTS`,
    the correlation id and, when named, the pipeline run id, as the hub writer does, and writes
    an insert-only MERGE on (key, `LoadTS`), or `write_delta_create`, which refuses a table
    already there. The hash settings the detector used are sealed on the table at its first
    write and checked before every later one (F158), so a run hashing another way is refused
    before it writes.

    The predicate on (key, `LoadTS`) never matches a row of an earlier run, whose `LoadTS`
    differs; it makes the MERGE read the satellite, so a concurrent load's commit conflicts with
    this one instead of both appending. The MERGE is not retried: the rows come from the
    detector's read of the satellite, and a retry would re-run them on that old snapshot,
    inserting a change the winning load already wrote. The conflict fails this load, and its
    rerun classifies against the satellite as it then is (F162's review). Whether Delta raises
    the conflict on a live metastore is a check the design owes.

    Reports `inserted` from the write's own Delta commit, `updated` 0 (`SCDMetricsProvider`),
    and, as an `HWMProvider`, this run's `LoadTS` as `hwm_after` and no `hwm_before`, as the hub
    writer does: the load-context watermark stays the source step's (F138).

    `LoadTS` is the run's load time, `global_context.load_timestamp` (F106). The step does not
    check that it increases: an equal one makes a real change match an existing (key, `LoadTS`)
    row and be dropped silently, and an earlier one makes a key's "latest row" the wrong one.
    Passing a later load time is the user's responsibility. `LoadEndTS` stays the clock at
    stamping, so with a backfilled load time `LoadEndTS - LoadTS` is not a duration.

    The target step's answer (`state.target_exists`) decides between creating and merging
    (F210): False creates through `errorifexists`, so a satellite the step wrongly missed is
    refused and the load fails with it untouched, rather than every key being recorded again.

    Cost: one existence check and one metadata read for the seal (plus one `isEmpty()` on an
    unsealed existing table); the MERGE, which evaluates the detector's plan once and joins it
    to the whole satellite, a read that grows with its history on every load on top of the
    target step's; and one read of the commit's metrics.
    """

    data_vault_style = _STYLE

    def __init__(self) -> None:
        self.inserted: int | None = None  # None until _run sets it (F117)
        self.updated: int | None = None
        self.hwm_before: str | None = None
        self.hwm_after: str | None = None
        self.hwm_column: str | None = None

    def _run(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
        from spark_quick_start.logger import OrchestratorLogger
        logger = self._logger or OrchestratorLogger()

        s = ctx.scd_ctx
        s.require_data_vault_style(type(self).__name__, _STYLE)
        assert self._adapter is not None and self._spark is not None
        spark, adapter = self._spark, self._adapter
        target = ctx.global_ctx.target_table_path
        if not target:
            raise ValueError("global_context.target_table_path is required for a Data Vault load")

        settings = hash_settings(s, spark)
        needs_seal = check_hash_seal(adapter, spark, target, settings)

        load_ts = ctx.global_ctx.load_timestamp_utc  # the run's load time T (F106)
        rows = state.df.drop(*[c for c in INTERNAL_COLUMNS if c in state.df.columns])
        rows = stamp_data_vault_columns(rows, ctx, load_ts)

        pre_version, created = execute_dv_insert_only(
            spark, target, rows, [s.business_key_column, s.load_ts_column], adapter, logger, retry=False,
            target_exists=state.target_exists)
        if created:
            self.inserted = capture_write_statistics(spark, target, pre_version, adapter, logger)
        else:
            self.inserted = capture_merge_statistics(spark, target, pre_version, adapter, logger)[0]
        self.updated = 0
        if needs_seal:
            write_hash_seal(adapter, spark, target, settings)

        self.hwm_column = s.load_ts_column
        self.hwm_before = None
        self.hwm_after = load_ts.strftime("%Y-%m-%d %H:%M:%S.%f")
        logger.info(f"{type(self).__name__}: {self.inserted} row(s) into {target}.")
        return state
