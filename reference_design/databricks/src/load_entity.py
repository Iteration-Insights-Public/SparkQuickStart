# Databricks notebook source
# MAGIC %md
# MAGIC # Load an entity into bronze (DQD → SCD2)
# MAGIC
# MAGIC One task of the `load_bronze` or `load_iot` job. Reads one load-context JSON file, fills its
# MAGIC `${...}` placeholders from the task parameters, runs the load, records the outcome
# MAGIC in the control schema's `load_runs` table, and fails the task when the load failed,
# MAGIC so the chained tasks after it do not start.
# MAGIC
# MAGIC An incremental context (one with `hwm_context` and a null `hwm_value`) gets its
# MAGIC watermark from `load_runs`: the `hwm_value_after` of the entity's latest successful run.

# COMMAND ----------

# ruff: noqa: F821 - spark and dbutils are Databricks notebook globals

import json
from datetime import UTC, datetime
from string import Template

from pyspark.errors import AnalysisException
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spark_quick_start.orchestration import (
    CandidateGuardTransformer,
    DQDTransformer,
    LoadOrchestrator,
    NativeSCDMergeTransformer,
    SCDChangeDetectorTransformer,
    SCDTargetTableTransformer,
)

# COMMAND ----------

PARAMETERS = (
    "context_file",
    "catalog",
    "landing_schema",
    "bronze_schema",
    "stage_schema",
    "control_schema",
    "logs_volume",
    "run_date",
    "job_run_id",
)
for name in PARAMETERS:
    dbutils.widgets.text(name, "")
params = {name: dbutils.widgets.get(name) for name in PARAMETERS}
missing = [name for name, value in params.items() if not value]
if missing:
    raise ValueError(f"load_entity: task parameters not set: {missing}")

# COMMAND ----------

with open(params["context_file"], encoding="utf-8") as f:
    # substitute() raises KeyError on a placeholder no parameter fills, so a typo in a
    # context file fails here rather than as a table name with "${" in it.
    load_context_json = Template(f.read()).substitute(params)
load_context = json.loads(load_context_json)
entity = load_context["global_context"]["entity_name"]
load_runs = f"`{params['catalog']}`.`{params['control_schema']}`.`load_runs`"

# COMMAND ----------


def previous_watermark(table: str, entity: str) -> str | None:
    """The `hwm_value_after` of `entity`'s latest successful run in `table`, or None.

    Only a successful run counts, so a failed load never moves the watermark; a run that read
    no new rows reports its incoming value as `hwm_value_after`, so it holds. None also when
    the table is missing or predates the watermark columns: the load then reads everything.
    One filtered read of `load_runs`, a small table.
    """
    try:
        runs = spark.table(table)
    except AnalysisException:
        return None
    if "hwm_value_after" not in runs.columns:
        return None
    row = (
        runs.filter(
            (F.col("entity") == entity)
            & (F.col("status") == "success")
            & F.col("hwm_value_after").isNotNull()
            & (F.col("hwm_value_after") != "")
        )
        .orderBy(F.col("recorded_at").desc())
        .select("hwm_value_after")
        .first()
    )
    return None if row is None else row["hwm_value_after"]


hwm_context = load_context.get("hwm_context")
if hwm_context is not None and hwm_context.get("hwm_value") is None:
    hwm_context["hwm_value"] = previous_watermark(load_runs, entity)
    load_context_json = json.dumps(load_context)
if hwm_context is not None:
    print(f"load_entity: {entity} watermark {hwm_context['hwm_column_name']} > {hwm_context['hwm_value']}")

# COMMAND ----------

result = (
    LoadOrchestrator(load_context_json, spark, platform="databricks")
    .add(DQDTransformer())
    .add(SCDTargetTableTransformer())
    .add(SCDChangeDetectorTransformer())
    .add(CandidateGuardTransformer())
    .add(NativeSCDMergeTransformer())
    .run()
)

# COMMAND ----------

# The run is recorded before the task is failed, so a failed load leaves its row too.
metrics = result.metrics or {}
error_message = (result.error_details or {}).get("error_message")
LOAD_RUNS_SCHEMA = T.StructType(
    [
        T.StructField("entity", T.StringType()),
        T.StructField("correlation_id", T.StringType()),
        T.StructField("run_date", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("early_exit", T.BooleanType()),
        T.StructField("early_exit_reason", T.StringType()),
        T.StructField("inserted_count", T.LongType()),
        T.StructField("updated_count", T.LongType()),
        T.StructField("duration_seconds", T.DoubleType()),
        T.StructField("error_message", T.StringType()),
        T.StructField("recorded_at", T.TimestampType()),
        T.StructField("hwm_value_before", T.StringType()),
        T.StructField("hwm_value_after", T.StringType()),
    ]
)
row = (
    entity,
    result.correlation_id,
    params["run_date"],
    result.status,
    result.early_exit,
    result.early_exit_reason,
    metrics.get("inserted_count"),
    metrics.get("updated_count"),
    float(metrics["duration_seconds"]) if metrics.get("duration_seconds") is not None else None,
    error_message,
    datetime.now(UTC),
    *(None if metrics.get(k) is None else str(metrics[k]) for k in ("hwm_value_before", "hwm_value_after")),
)
# mergeSchema: a load_runs created before the watermark columns gains them on this append.
(
    spark.createDataFrame([row], LOAD_RUNS_SCHEMA)
    .write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(load_runs)
)

# COMMAND ----------

print(result.to_json())
if result.status == "error":
    raise RuntimeError(f"load_entity: {entity} failed: {error_message}")
