# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6c9a4b42-877d-4898-b018-941aa5ad2571",
# META       "default_lakehouse_name": "lh_quickstart",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000",
# META       "known_lakehouses": [
# META         {
# META           "id": "6c9a4b42-877d-4898-b018-941aa5ad2571"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "d8ed079d-f1ab-4838-9c70-4a380e5e2cb0",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Load an entity into bronze (DQD → SCD2)
#
# One activity of `nb_orchestrate`, or a run of its own. Reads one load-context file from the
# lakehouse's `Files/contexts/`, fills its `${run_date}` and `${job_run_id}` placeholders,
# runs the load, records the outcome in `control.load_runs`, and fails when the load failed,
# so the activities after it in the orchestrator's chain do not start.
#
# An incremental context (one with `hwm_context` and a null `hwm_value`) gets its watermark
# from `load_runs`: the `hwm_value_after` of the entity's latest successful run.
#
# `lh_quickstart` and `env_quickstart` are bound by their logical IDs, which `fabric-cicd`
# replaces with the deployed items' IDs.

# PARAMETERS CELL ********************

# The file name under Files/contexts/, such as person_person.json.
context_file = ""
# The day the orchestrator started, YYYY-MM-DD; empty: today, UTC.
run_date = ""
# The orchestrator's run; empty: this notebook's own.
job_run_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ruff: noqa: F821, E402 - spark and notebookutils are Fabric notebook globals; the parameters cell comes before the imports

import json
from datetime import UTC, datetime
from string import Template

from pyspark.errors import AnalysisException
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spark_quick_start.adapters import FabricAdapter
from spark_quick_start.orchestration import (
    CandidateGuardTransformer,
    DQDTransformer,
    LoadOrchestrator,
    NativeSCDMergeTransformer,
    SCDChangeDetectorTransformer,
    SCDTargetTableTransformer,
)

if not context_file:
    raise ValueError("load_entity: the context_file parameter is not set")
params = {
    "run_date": run_date or datetime.now(UTC).date().isoformat(),
    "job_run_id": job_run_id or notebookutils.runtime.context["activityId"],
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# The default lakehouse's Files, mounted on the driver.
with open(f"/lakehouse/default/Files/contexts/{context_file}", encoding="utf-8") as f:
    # substitute() raises KeyError on a placeholder no parameter fills, so a typo in a
    # context file fails here rather than as a table name with "${" in it.
    load_context_json = Template(f.read()).substitute(params)
load_context = json.loads(load_context_json)
entity = load_context["global_context"]["entity_name"]
load_runs = "control.load_runs"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result = (
    LoadOrchestrator(load_context_json, spark, platform="fabric")
    .add(DQDTransformer())
    .add(SCDTargetTableTransformer())
    .add(SCDChangeDetectorTransformer())
    .add(CandidateGuardTransformer())
    .add(NativeSCDMergeTransformer())
    .run()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# The run is recorded before the notebook is failed, so a failed load leaves its row too.
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(result.to_json())
if result.status == "error":
    raise RuntimeError(f"load_entity: {entity} failed: {error_message}")
# The result goes back to nb_orchestrate as this activity's exitVal. Last, and outside any
# try: notebook exit ends the notebook, and some runtimes raise it as an exception.
FabricAdapter().notebook_exit(result.to_json())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
