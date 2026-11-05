"""F159 end to end: a Data Vault hub loaded by dv_hub_pipeline from a DQD source, on local Delta.

Two runs from one source, then a second source loading the same hub; the seal of the hash
settings; and the refusals that must come before anything is written. Requires a working Delta
install.
"""
from __future__ import annotations

import json

import pytest

from spark_quick_start.adapters.local_adapter import LocalAdapter
from spark_quick_start.orchestration import dv_hub_pipeline


@pytest.fixture(autouse=True)
def _require_working_delta(spark, tmp_path_factory):
    """Skip unless this session can round-trip a Delta table; see test_dqd_load."""
    pytest.importorskip("delta", reason="delta-spark not installed")
    probe = str(tmp_path_factory.mktemp("delta_probe") / "t")
    try:
        spark.range(1).write.format("delta").mode("overwrite").save(probe)
    except Exception as exc:  # pragma: no cover — environment-dependent
        pytest.skip(f"Delta not usable in this Spark session: {type(exc).__name__}")


_KEY = {"name": "HashKey", "expression": {"function": "sha2", "arguments": [
    {"function": "upper", "arguments": [{"function": "trim", "arguments": [{"column": "CUST_ID"}]}]},
    {"literal": 256}]}}


def _context(root, source_system, load_timestamp=None, **scd):
    return json.dumps({
        "global_context": {"id": "ent_1", "entity_name": "Customer", "source_system": source_system,
                           "target_table_path": f"{root}/vault/customer_h", "staging_table_path": f"{root}/unused",
                           **({"load_timestamp": load_timestamp} if load_timestamp else {})},
        "scd_context": {"scd_type": "dv_hub", "business_key_column": "HashKey", **scd},
        "runtime_context": {"correlation_id": f"run-{source_system}", "pipeline_name": "customer_h",
                            "application_name": "vault"},
        "logging_context": {"logging_level": "INFO", "log_file_name": "customer_h", "log_file_dir": f"{root}/logs"},
        "dqd_context": {"source_kind": "delta", "source_identifier": f"{root}/bronze/{source_system}",
                        "transformations": [_KEY],
                        "selection": [{"name": "HashKey"}, {"name": "CustomerNo", "expression": {"column": "CUST_ID"}}]},
    })


def _load(spark, root, source_system, ids, load_timestamp=None, **scd):
    spark.createDataFrame([(i,) for i in ids], "CUST_ID string").write.format("delta").mode("overwrite") \
        .save(f"{root}/bronze/{source_system}")
    return dv_hub_pipeline(_context(root, source_system, load_timestamp, **scd), spark,
                           adapter=LocalAdapter()).run()


def _hub(spark, root):
    return spark.read.format("delta").load(f"{root}/vault/customer_h")


def test_a_hub_takes_each_key_once_across_runs_and_sources(spark, tmp_path):
    first = _load(spark, tmp_path, "CRM", ["1", "2"])
    assert first.status == "success", first.error_details
    assert (first.metrics["inserted_count"], first.metrics["updated_count"]) == (2, 0)

    second = _load(spark, tmp_path, "CRM", ["2", "3"])
    assert second.status == "success", second.error_details
    assert second.metrics["inserted_count"] == 1

    erp = _load(spark, tmp_path, "ERP", [" 3 ", "4"])  # " 3 " is customer 3 after trim and upper
    assert erp.status == "success", erp.error_details
    assert erp.metrics["inserted_count"] == 1

    rows = {r.CustomerNo.strip(): r for r in _hub(spark, tmp_path).collect()}
    assert sorted(rows) == ["1", "2", "3", "4"]
    assert (rows["3"].RecordSource, rows["4"].RecordSource) == ("CRM", "ERP"), "the first source to see a key keeps it"
    assert {"HashKey", "CustomerNo", "RecordSource", "LoadTS", "LoadEndTS", "CorrelationId"} <= set(rows["1"].asDict())
    assert not {"scdVersion", "scdCurrentFlag", "scdEffectiveFrom", "scdHash"} & set(rows["1"].asDict())


def test_every_row_of_a_run_carries_the_load_timestamp_as_load_ts(spark, tmp_path):
    """F106: LoadTS is global_context.load_timestamp, not when the run started."""
    from datetime import UTC, datetime, timedelta

    from pyspark.sql import functions as F
    when = "2026-10-01T08:00:00.123456+00:00"
    result = _load(spark, tmp_path, "CRM", ["1", "2", "3"], load_timestamp=when)
    stamps = {r[0] for r in _hub(spark, tmp_path).select(F.unix_micros("LoadTS")).collect()}
    assert stamps == {(datetime.fromisoformat(when) - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)}
    assert result.metrics["hwm_value_after"] == "2026-10-01 08:00:00.123456"


def test_a_load_whose_columns_differ_from_the_hub_is_refused(spark, tmp_path):
    """A Data Vault table is never evolved implicitly; runtimes differ on schema evolution."""
    assert _load(spark, tmp_path, "CRM", ["1"]).status == "success"
    drifted = _load(spark, tmp_path, "CRM", ["2"], pipeline_run_id_column="PipelineRunId")
    assert drifted.status == "error"
    assert "only in the load ['PipelineRunId']" in drifted.error_details["error_message"]
    assert _hub(spark, tmp_path).count() == 1


def test_the_hash_settings_are_sealed_and_a_load_with_others_is_refused(spark, tmp_path):
    assert _load(spark, tmp_path, "CRM", ["1"]).status == "success"
    properties = spark.sql(f"DESCRIBE DETAIL delta.`{tmp_path}/vault/customer_h`").first()["properties"]
    assert properties["spark_quick_start.hash_algorithm"] == "sha256"

    md5 = _load(spark, tmp_path, "CRM", ["2"], hash_algorithm="md5")
    assert md5.status == "error"
    assert "sealed" in md5.error_details["error_message"]
    assert _hub(spark, tmp_path).count() == 1


def test_a_duplicate_staged_key_is_refused_before_the_hub_exists(spark, tmp_path):
    result = _load(spark, tmp_path, "CRM", ["1", " 1"])  # one key after trim
    assert result.status == "error"
    assert "staged more than once" in result.error_details["error_message"]
    assert not (tmp_path / "vault" / "customer_h").exists()


def test_a_missed_existence_check_cannot_append_the_hub_a_second_time(spark, tmp_path):
    """F159's review: delta_table_exists swallows errors and answers False. Before the fix the
    writer then appended every batch key over the populated hub; now the create refuses."""
    from unittest.mock import patch
    assert _load(spark, tmp_path, "CRM", ["1", "2"]).status == "success"
    with patch.object(LocalAdapter, "delta_table_exists", return_value=False):
        result = _load(spark, tmp_path, "CRM", ["1", "2"])
    assert result.status == "error"
    assert "exist" in result.error_details["error_message"].lower()
    assert _hub(spark, tmp_path).count() == 2

