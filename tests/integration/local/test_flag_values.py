"""F106: the flag vocabularies and the open-row sentinel, end to end on local Delta.

`current_flag_value` / `inactive_flag_value` are the current-row flag pair the writer stamps and
closes out with; `deleted_flag_value` / `not_deleted_flag_value` are the deleted flag pair written
and compared with the target's; `source_deleted_flag_value` is what the delete signal is compared
with; `default_max_timestamp` is the open row's effective-to, an instant in UTC. Requires a
working Delta install.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from pyspark.sql import functions as F

from spark_quick_start.adapters.local_adapter import LocalAdapter
from spark_quick_start.orchestration import default_pipeline
from spark_quick_start.orchestration.staging_pipeline import create_staging_main_function

_VOCAB = {"current_flag_value": "1", "inactive_flag_value": "0", "deleted_flag_value": "D",
          "not_deleted_flag_value": "A", "source_deleted_flag_column": "is_deleted",
          "source_deleted_flag_value": "X"}
_T0 = datetime(2026, 10, 1, 8, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _require_working_delta(spark, tmp_path_factory):
    """Skip unless this session can round-trip a Delta table; see test_dqd_load."""
    pytest.importorskip("delta", reason="delta-spark not installed")
    probe = str(tmp_path_factory.mktemp("delta_probe") / "t")
    try:
        spark.range(1).write.format("delta").mode("overwrite").save(probe)
    except Exception as exc:  # pragma: no cover — environment-dependent
        pytest.skip(f"Delta not usable in this Spark session: {type(exc).__name__}")


def _context(root, run, scd_type, **scd):
    return json.dumps({
        "global_context": {
            "id": "customer", "entity_name": "Customer", "source_system": "CRM",
            "target_table_path": f"{root}/silver/customer",
            "staging_table_path": f"{root}/staging/customer",
            "load_timestamp": (_T0 + timedelta(hours=run)).isoformat(),
            "staging_table_schema": {"type": "struct", "fields": [
                {"name": "customer_id", "type": "integer", "nullable": True, "metadata": {}},
                {"name": "name", "type": "string", "nullable": True, "metadata": {}},
                {"name": "is_deleted", "type": "string", "nullable": True, "metadata": {}}]},
        },
        "staging_context": {"stages": [{"nb_name": "NB_Stage_Customer"}]},
        "scd_context": {"business_key_column": "customer_id", "scd_type": scd_type, **scd},
        "runtime_context": {"correlation_id": f"run-{run}", "pipeline_name": "customer", "application_name": "crm"},
        "logging_context": {"logging_level": "INFO", "log_file_name": "customer", "log_file_dir": f"{root}/logs"},
    })


@pytest.fixture
def run(spark, tmp_path):
    """run(rows, scd_type, **scd) -> ExecutionResult; rows are (customer_id, name, is_deleted).

    Each call is the next hour's load, so successive loads have distinct effective-from times.
    """
    staged = {"rows": [], "n": 0}

    def stage(storage_path, staging_context, global_context, notebook_name):
        return spark.createDataFrame(staged["rows"], "customer_id int, name string, is_deleted string")

    main = create_staging_main_function(stage, adapter=LocalAdapter(), spark=spark)
    adapter = LocalAdapter(notebook_registry={"NB_Stage_Customer": lambda a: main(a["stage_context_str"]).to_json()})

    def _run(rows, scd_type="scd2", **scd):
        staged["rows"], staged["n"] = rows, staged["n"] + 1
        result = default_pipeline(_context(tmp_path, staged["n"], scd_type, **scd), spark, adapter=adapter).run()
        assert result.status == "success", result.error_details
        return result

    return _run


def _history(spark, root):
    rows = spark.read.format("delta").load(f"{root}/silver/customer").collect()
    return sorted((r["customer_id"], r["scdVersion"], r["scdCurrentFlag"], r["scdDeletedFlag"]) for r in rows)


def test_scd2_writes_and_compares_the_custom_vocabulary_through_a_change_a_delete_and_a_return(spark, tmp_path, run):
    run([(1, "Ada", None), (2, "Grace", None)], **_VOCAB)
    assert _history(spark, tmp_path) == [(1, 1, "1", "A"), (2, 1, "1", "A")]

    run([(1, "Ada Lovelace", None)], **_VOCAB)
    assert _history(spark, tmp_path) == [(1, 1, "0", "A"), (1, 2, "1", "A"), (2, 1, "1", "A")], \
        "the close-out takes the inactive value, the new version the current one"

    run([(2, "ignored", "X")], **_VOCAB)
    assert _history(spark, tmp_path) == [(1, 1, "0", "A"), (1, 2, "1", "A"), (2, 1, "0", "A"), (2, 2, "1", "D")]

    again = run([(2, "ignored", "X")], **_VOCAB)
    assert again.early_exit, "a repeated delete is recognised by the written deleted value and adds no version"

    run([(2, "Grace", None)], **_VOCAB)
    assert _history(spark, tmp_path) == [(1, 1, "0", "A"), (1, 2, "1", "A"), (2, 1, "0", "A"),
                                         (2, 2, "0", "D"), (2, 3, "1", "A")]


def test_scd2_full_load_absence_is_written_with_the_deleted_value(spark, tmp_path, run):
    run([(1, "Ada", None), (2, "Grace", None)], **_VOCAB)
    run([(1, "Ada", None)], load_type="Full", **_VOCAB)
    assert (2, 2, "1", "D") in _history(spark, tmp_path)


def test_scd1_soft_delete_with_a_custom_deleted_value(spark, tmp_path, run):
    """The merge SQL of the SCD1 soft delete (load_pipeline) takes the deleted value from the context."""
    run([(1, "Ada", None), (2, "Grace", None)], "scd1", **_VOCAB)
    run([(2, "ignored", "X")], "scd1", **_VOCAB)
    table = {r["customer_id"]: r for r in spark.read.format("delta").load(f"{tmp_path}/silver/customer").collect()}
    assert (table[2]["scdDeletedFlag"], table[2]["name"], table[1]["scdDeletedFlag"]) == ("D", "Grace", "A")

    run([(2, "Grace", None)], "scd1", **_VOCAB)
    table = {r["customer_id"]: r for r in spark.read.format("delta").load(f"{tmp_path}/silver/customer").collect()}
    assert table[2]["scdDeletedFlag"] == "A"


def test_an_open_row_ends_at_the_given_default_max_timestamp_in_utc(spark, tmp_path, run):
    run([(1, "Ada", None)], default_max_timestamp="2500-06-01T12:00:00Z")
    row = spark.read.format("delta").load(f"{tmp_path}/silver/customer") \
        .select(F.unix_micros("scdEffectiveTo")).first()[0]
    assert row == (datetime(2500, 6, 1, 12, tzinfo=UTC) - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)
