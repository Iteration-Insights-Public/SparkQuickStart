"""
load_pipeline.py — EDW load pipeline functions.

Contains: validate_and_merge_staging_tables, load_staged_dataframe,
    load_target_dataframe, execute_scd_merge, execute_append_only,
    execute_dv_insert_only, capture_write_statistics,
    capture_merge_statistics, get_high_watermark, log_runtime.
Does NOT contain: staging orchestration, platform-specific code,
    or any import from spark_load_orchestration.

Usage::

    from spark_quick_start.orchestration.load_pipeline import (
        load_staged_dataframe, execute_scd_merge, get_high_watermark,
    )
"""
from __future__ import annotations

import dataclasses
import json
from collections.abc import Sequence
from datetime import UTC, datetime
from functools import reduce as _reduce
from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType

from spark_quick_start.adapters.platform_adapter import PlatformAdapter
from spark_quick_start.orchestration.column_policy import ColumnNamingPolicy
from spark_quick_start.orchestration.models import Constants, SCDContext
from spark_quick_start.orchestration.validation import compare_schemas_primitive_ignore_order

__all__ = [
    "capture_merge_statistics",
    "capture_write_statistics",
    "execute_append_only",
    "execute_scd_merge",
    "get_high_watermark",
    "load_staged_dataframe",
    "load_target_dataframe",
    "log_runtime",
    "validate_and_merge_staging_tables",
]


def validate_and_merge_staging_tables(
    spark: SparkSession,
    table_paths: list[str],
    expected_schema_json: dict[str, Any],
    staging_table_path: str,
    adapter: PlatformAdapter,
    logger: Any,
) -> None:
    """Validate each staging table against the expected schema, then merge into one Delta path.

    Raises:
        TypeError: list of schema-mismatch messages if any table fails validation.
    """
    expected_schema = StructType.fromJson(expected_schema_json)
    errors: list[str] = []

    for path in table_paths:
        actual = adapter.read_delta(spark, path).schema
        actual = StructType([f for f in actual.fields if f.name not in Constants.EXCLUDE_COLUMNS])
        ok, report = compare_schemas_primitive_ignore_order(actual, expected_schema)
        if not ok:
            errors.append(f"Schema mismatch for '{path}': {report}")

    if errors:
        raise TypeError(errors)

    dfs = [adapter.read_delta(spark, p) for p in table_paths]
    merged_df = _reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)

    expected_column_order = [
        f.name for f in expected_schema.fields
        if f.name not in Constants.EXCLUDE_COLUMNS and f.name in merged_df.columns
    ]
    merged_df = merged_df.select(*expected_column_order)

    adapter.write_delta_overwrite(merged_df, staging_table_path, overwrite_schema=True)

    logger.info(f"✓ {len(table_paths)} staging table(s) merged into: {staging_table_path}")
    logger.info(f"  Column order enforced: {', '.join(expected_column_order)}")

def load_staged_dataframe(
    spark: SparkSession,
    staging_table_path: str,
    adapter: PlatformAdapter,
    logger: Any,
) -> DataFrame:
    """Read the merged staging Delta table and strip technical orchestration columns."""
    df = adapter.read_delta(spark, staging_table_path)
    for col_name in Constants.EXCLUDE_COLUMNS:
        if col_name in df.columns:
            df = df.drop(col_name)
    logger.info(f"Staged data loaded — {len(df.columns)} columns")
    return df


def load_target_dataframe(
    spark: SparkSession,
    scd_ctx: SCDContext,
    target_table: str,
    adapter: PlatformAdapter,
    logger: Any,
) -> tuple[Any, DataFrame | None]:
    """Read current-flag records from the Silver Delta table."""
    if not adapter.delta_table_exists(spark, target_table):
        logger.warning(f"Target table does not exist yet: {target_table}")
        return None, None

    dt = adapter.resolve_delta_table(spark, target_table)
    silver_df = dt.toDF().filter(
        col(scd_ctx.current_flag_column) == scd_ctx.current_flag_value
    )
    logger.info("Target table loaded.")
    return dt, silver_df


def _build_merge_condition(scd_ctx: SCDContext) -> str:
    """Build the MERGE ON condition string used by the SCD merge."""
    bk = scd_ctx.business_key_column
    cond = (
        f"tgt.`{bk}` = src.`{bk}` "
        f"AND tgt.`{scd_ctx.source_system_column}` = src.`{scd_ctx.source_system_column}`"
    )
    if scd_ctx.scd_type == "scd2":
        cond += f" AND tgt.`{scd_ctx.version_column}` = src.`{scd_ctx.version_column}`"
        # Only match ACTIVE rows (currentFlag=Y). Closed-out rows (N) must never be matched
        # again — without this guard, "new" candidates with scdVersion=1 accidentally match
        # previously closed-out scdVersion=1 rows, producing MATCHED UPDATEs instead of
        # NOT MATCHED INSERTs and causing an infinite close-out cycle.
        cond += f" AND tgt.`{scd_ctx.current_flag_column}` = '{scd_ctx.current_flag_value}'"
    return cond


def _scd1_update_columns(scd_ctx: SCDContext, candidate_columns: Sequence[str]) -> list[str]:
    """Return the columns an SCD1 MERGE writes on a matched row (F150).

    The business columns (every candidate column that is not one of the context's nine meta
    columns, by the context's names, never by a prefix) plus the current flag, effective-from,
    hash, deleted flag, version and correlation id, which describe the latest change. The row
    key, effective-to and source system columns are the row's identity or constant under SCD1
    and are never updated. Pure Python: no Spark action.
    """
    # Each ColumnNamingPolicy field is an SCDContext attribute holding the resolved name, so a
    # field added to the policy is excluded here without an edit.
    meta = {getattr(scd_ctx, f.name) for f in dataclasses.fields(ColumnNamingPolicy)}
    rewritten = (
        scd_ctx.current_flag_column, scd_ctx.effective_from_column, scd_ctx.hash_column,
        scd_ctx.deleted_flag_column, scd_ctx.version_column, scd_ctx.correlation_id_column,
    )
    return [c for c in candidate_columns if c not in meta] + list(rewritten)


def execute_scd_merge(
    spark: SparkSession,
    scd_ctx: SCDContext,
    target_table: str,
    staging_view_name: str,
    adapter: PlatformAdapter,
    logger: Any,
    *,
    target_exists: bool | None = None,
) -> int:
    """Create the target table if absent, then execute SCD MERGE using the DeltaTable API.

    The table is created with `write_delta_create` (`errorifexists`), not an overwrite (F209).
    `delta_table_exists` answers False on any error but a path checked without delta-spark. An empty overwrite of a table that does
    exist replaced its current version, after which the MERGE kept only the batch.

    `target_exists` is the target step's answer (`PipelineState.target_exists`). When it is
    given it decides, and the existence is not asked again: the candidates were classified
    against that answer, so a table the target step did not see must not be merged into even
    if a second check would find it. False creates the table, and `errorifexists` refuses one
    that is in fact there; the load fails with the table untouched. None, a pipeline with no
    target step, keeps the check here.
    """
    candidates = spark.table(staging_view_name)

    try:
        absent = (not adapter.delta_table_exists(spark, target_table)
                  if target_exists is None else not target_exists)
        if absent:
            logger.info(f"Creating target table: {target_table}")
            adapter.write_delta_create(candidates.limit(0), target_table)

        dt = adapter.resolve_delta_table(spark, target_table)
        pre_version = int(dt.history(1).selectExpr("max(version)").collect()[0][0] or -1)
        logger.info(f"Pre-merge version: {pre_version}")

        merge_cond = _build_merge_condition(scd_ctx)

        if scd_ctx.scd_type == "scd2":
            update_set = {
                scd_ctx.current_flag_column: F.col(f"src.{scd_ctx.current_flag_column}"),
                scd_ctx.effective_to_column: F.col(f"src.{scd_ctx.effective_to_column}"),
                scd_ctx.hash_column: F.col(f"src.{scd_ctx.hash_column}"),
                scd_ctx.deleted_flag_column: F.col(f"src.{scd_ctx.deleted_flag_column}"),
            }
        else:  # scd1
            update_set = {c: F.col(f"src.{c}") for c in _scd1_update_columns(scd_ctx, candidates.columns)}

        builder = dt.alias("tgt").merge(candidates.alias("src"), merge_cond)
        not_matched_condition = None
        if scd_ctx.scd_type == "scd1":
            # F110: an SCD1 delete is a soft delete. A deleted candidate (the only kind carrying
            # the deleted flag value; new and changed ones carry the not-deleted one) flags the matched row and stamps
            # effective-from and the correlation id, and nothing else: the row keeps its business
            # columns, hash and version. The clause precedes the update, which would overwrite them.
            # Null-safe: a NULL flag is not a delete, and NOT (NULL) would drop the row from the insert.
            # The value is the context's, validated to carry no quote (SCDContext.validate, F106).
            deleted = f"src.`{scd_ctx.deleted_flag_column}` <=> '{scd_ctx.deleted_flag_value}'"
            soft_delete_set = {
                c: F.col(f"src.{c}")
                for c in (scd_ctx.deleted_flag_column, scd_ctx.effective_from_column, scd_ctx.correlation_id_column)
            }
            builder = builder.whenMatchedUpdate(condition=deleted, set=soft_delete_set)
            # A deleted candidate with no target row is never inserted as a flagged row.
            not_matched_condition = f"NOT ({deleted})"
        builder = builder.whenMatchedUpdate(set=update_set)
        if not_matched_condition is None:
            builder = builder.whenNotMatchedInsertAll()
        else:
            builder = builder.whenNotMatchedInsertAll(condition=not_matched_condition)
        builder.execute()
        return pre_version
    finally:
        spark.catalog.dropTempView(staging_view_name)


def execute_append_only(
    spark: SparkSession,
    target_table: str,
    staging_view_name: str,
    adapter: PlatformAdapter,
    logger: Any,
    *,
    target_exists: bool | None = None,
) -> int:
    """Raw Delta append — no ``.merge()``, no target-table scan.

    `target_exists` is the target step's answer (`PipelineState.target_exists`), as for
    `execute_scd_merge` (F210). `delta_table_exists` answers False on any error but a path checked without delta-spark,
    and the candidates were classified against that answer: every staged row `new`. When it is given it
    decides and the existence is not asked again. False creates the table through
    `write_delta_create` (`errorifexists`) rather than an append, so a table that is in fact
    there is refused and the load fails with it untouched, where an append would have written
    rows it already holds with status success. True appends. None, a pipeline with no target
    step, keeps the check here and appends either way.

    Cost: one existence check when `target_exists` is None, none otherwise; one history read
    for an existing table; the write.

    Returns:
        pre_version (or -1 if the table was created by this call), so
        ``capture_write_statistics`` can read only this write's commit
        metadata afterward.
    """
    candidates = spark.table(staging_view_name)
    try:
        exists = adapter.delta_table_exists(spark, target_table) if target_exists is None else target_exists
        if exists:
            dt = adapter.resolve_delta_table(spark, target_table)
            pre_version = int(dt.history(1).selectExpr("max(version)").collect()[0][0] or -1)
            adapter.write_delta_append(candidates, target_table)
        elif target_exists is False:
            logger.info(f"Creating target table: {target_table}")
            adapter.write_delta_create(candidates, target_table)
            pre_version = -1
        else:
            pre_version = -1
            adapter.write_delta_append(candidates, target_table)
        return pre_version
    finally:
        spark.catalog.dropTempView(staging_view_name)


def execute_dv_insert_only(
    spark: SparkSession,
    target_table: str,
    rows: DataFrame,
    key_columns: Sequence[str],
    adapter: PlatformAdapter,
    logger: Any,
    *,
    retry: bool = True,
    target_exists: bool | None = None,
) -> tuple[int, bool]:
    """Insert the rows whose key the Data Vault table lacks; never update, never overwrite (F159).

    A missing table is created by `write_delta_create`, one commit with its data, which fails if
    anything is already there: an existence check that missed a populated table cannot turn the
    first write into an append over its rows (F158's and F159's reviews). An existing table takes
    an insert-only MERGE on *key_columns* (a hub's key; a satellite's key and load
    timestamp) through `run_with_delta_retry`, so two loads of one
    table at once conflict on the append and retry rather than duplicating a key; a retried
    MERGE re-checks the key. Columns must match the table's exactly: a staged column the table
    lacks, or the reverse, is refused before the write rather than left to schema evolution,
    which differs between Delta runtimes.

    *retry* False runs the MERGE once, so a concurrent commit fails the load instead: right when
    *rows* were derived from a read of the table, which a retry re-runs on its old snapshot (a
    DataFrame over a Delta table keeps the snapshot it was defined on, measured locally), so it
    would insert again what the winning load wrote. A hub's rows keep the default: its predicate
    is the key alone, checked against the table as it is when the MERGE runs.

    *target_exists* is the target step's answer (`PipelineState.target_exists`), as for
    `execute_scd_merge` (F210). When it is given it decides and the existence is not asked
    again: False creates through `write_delta_create`, which refuses a table that is in fact
    there, so a satellite whose detector classified every key new against a false "absent"
    fails the load instead of recording every key again under a new load timestamp. None, no
    target step, keeps the check here. The hub and link writers do not pass it, on purpose: the
    MERGE on the key checks every key against the table, so a false "absent" from their target
    step inserts nothing twice.

    Raises:
        ValueError: when the rows' columns differ from the existing table's.

    Returns:
        (pre_version, created): the version before the write, -1 when the table was created,
        for `capture_write_statistics` (created) or `capture_merge_statistics` (merged).
    """
    # Function-local: delta_utils imports this module at its top, so a module-level import
    # here would be circular.
    from spark_quick_start.orchestration.delta_utils import run_with_delta_retry

    absent = not adapter.delta_table_exists(spark, target_table) if target_exists is None else not target_exists
    if absent:
        logger.info(f"Creating Data Vault table: {target_table}")
        adapter.write_delta_create(rows, target_table)
        return -1, True
    existing = adapter.read_delta(spark, target_table).columns
    if sorted(existing) != sorted(rows.columns):
        raise ValueError(
            f"{target_table!r} has columns {sorted(existing)} and this load writes "
            f"{sorted(rows.columns)}: only in the table {sorted(set(existing) - set(rows.columns))}, "
            f"only in the load {sorted(set(rows.columns) - set(existing))}. A Data Vault table is "
            "not evolved implicitly; change the table or the load's selection."
        )
    dt = adapter.resolve_delta_table(spark, target_table)
    pre_version = int(dt.history(1).selectExpr("max(version)").collect()[0][0] or -1)
    condition = " AND ".join(f"tgt.`{c}` = src.`{c}`" for c in key_columns)
    def merge() -> Any:
        return dt.alias("tgt").merge(rows.alias("src"), condition).whenNotMatchedInsertAll().execute()

    run_with_delta_retry(merge) if retry else merge()
    return pre_version, False


def capture_write_statistics(
    spark: SparkSession,
    target_table: str,
    pre_version: int,
    adapter: PlatformAdapter,
    logger: Any,
) -> int:
    """Read numOutputRows from Delta history after a write — mirrors capture_merge_statistics for MERGE.

    A write to a path is recorded as `WRITE`; one that creates a table by catalog name, through
    `saveAsTable` with append or errorifexists, as `CREATE TABLE AS SELECT` (seen on Spark 3.5.1
    with Delta 3.2.0, F159's review). Both are read, or the first load of a table by name would
    report 0 rows written.
    """
    dt = adapter.resolve_delta_table(spark, target_table)
    post_hist = (
        dt.history()
        .where(col("version") > lit(pre_version))
        .where(col("operation").isin("WRITE", "CREATE TABLE AS SELECT"))
        .orderBy(col("version").desc())
    )
    rows = (
        post_hist
        .selectExpr("operationMetrics['numOutputRows'] as rows_written")
        .selectExpr("cast(coalesce(rows_written, '0') as int) as rows_written")
        .limit(1)
        .collect()
    )
    if not rows:
        logger.warning("No WRITE operation found in Delta history — count defaults to 0")
        return 0
    return rows[0].rows_written


def capture_merge_statistics(
    spark: SparkSession,
    target_table: str,
    pre_version: int,
    adapter: PlatformAdapter,
    logger: Any,
) -> tuple[int, int, int]:
    """Read insert/update/delete counts from Delta table history after a MERGE.

    Returns:
        (inserted, updated, deleted)
    """
    dt = adapter.resolve_delta_table(spark, target_table)
    post_hist = (
        dt.history()
        .where(col("version") > lit(pre_version))
        .where(col("operation") == lit("MERGE"))
        .orderBy(col("version").desc())
    )

    rows = (
        post_hist
        .selectExpr(
            "operationMetrics['numTargetRowsInserted'] as inserted",
            "operationMetrics['numTargetRowsUpdated'] as updated",
            "operationMetrics['numTargetRowsDeleted'] as deleted",
        )
        .selectExpr(
            "cast(coalesce(inserted, '0') as int) as inserted",
            "cast(coalesce(updated, '0') as int) as updated",
            "cast(coalesce(deleted, '0') as int) as deleted",
        )
        .limit(1)
        .collect()
    )
    if not rows:
        logger.warning("No MERGE operation found in Delta history — counts default to 0")
        return 0, 0, 0
    row = rows[0]
    return row.inserted, row.updated, row.deleted


def get_high_watermark(
    spark: SparkSession,
    target_table: str,
    adapter: PlatformAdapter,
    hwm_column: str,
) -> str:
    """Return the maximum watermark column value as a string (empty string if table is empty).

    hwm_column has no default deliberately — callers must pass the active
    ColumnNamingPolicy's effective_from_column (or another watermark column).
    A hardcoded "scdEffectiveFrom" default previously caused watermarks to
    silently fall back to the wrong column under a custom naming policy.
    """
    dt = adapter.resolve_delta_table(spark, target_table)
    hwm = dt.toDF().agg({hwm_column: "max"}).collect()[0][0]
    return str(hwm) if hwm is not None else ""


def log_runtime(spark: SparkSession, execution_stage: str, logger: Any) -> None:
    """Log Spark runtime configuration for the given execution stage.

    Compatible with both classic Spark (JVM-backed ``sparkContext``) and
    Spark Connect (Databricks serverless / remote sessions).  When
    ``spark.sparkContext`` is unavailable (Spark Connect raises
    ``PySparkAttributeError``), configuration values are read via
    ``spark.conf.get()`` which is supported on all session types.

    Args:
        spark: Active SparkSession.
        execution_stage: Label identifying the pipeline stage (e.g. "pre-merge").
        logger: Logger instance (standard Python logger or spark_logging equivalent).

    Returns:
        None
    """
    def _conf_get(key: str) -> str | None:
        """Read one config key — works on classic Spark and Spark Connect."""
        try:
            return spark.conf.get(key)
        except Exception:
            return None

    # application_id and app_name require the JVM-backed sparkContext.
    # Fall back gracefully when running under Spark Connect.
    application_id: str | None = None
    app_name: str | None = None
    try:
        application_id = spark.sparkContext.applicationId
        app_name = spark.sparkContext.appName
    except Exception:
        # Spark Connect: sparkContext is not available.
        # spark.conf.get does not expose applicationId or appName,
        # so we leave them as None rather than failing the pipeline.
        pass

    logger.info(json.dumps({
        "execution_stage": execution_stage,
        "utc_time": datetime.now(UTC).isoformat(),
        "spark_application_id": application_id,
        "spark_app_name": app_name,
        "high_concurrency_max": _conf_get("spark.highConcurrency.max"),
        "executor_instances": _conf_get("spark.executor.instances"),
        "executor_cores": _conf_get("spark.executor.cores"),
        "executor_memory": _conf_get("spark.executor.memory"),
        "adaptive_enabled": _conf_get("spark.sql.adaptive.enabled"),
        "adaptive_coalesce_partitions_enabled": _conf_get("spark.sql.adaptive.coalescePartitions.enabled"),
        "shuffle_partitions": _conf_get("spark.sql.shuffle.partitions"),
    }, indent=2))
