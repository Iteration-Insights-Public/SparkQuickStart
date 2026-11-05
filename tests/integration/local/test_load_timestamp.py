"""F106: `global_context.load_timestamp` is the run's load time T, end to end on local Delta.

T is the SCD effective-from (both writers), the SCD2 close-out's effective-to less one
microsecond, and the Data Vault `LoadTS`. The library does not check that T increases (the owner's
decision, 2026-10-04): a T equal to or earlier than what the target holds is the user's
responsibility, and the tests below pin that it is not refused. Requires a working Delta install.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from pyspark.sql import functions as F

from spark_quick_start.adapters.local_adapter import LocalAdapter
from spark_quick_start.orchestration import default_pipeline, dv_hub_pipeline, dv_satellite_pipeline
from spark_quick_start.orchestration.staging_pipeline import create_staging_main_function

_T1 = datetime(2026, 10, 1, 8, 0, 0, 123456, tzinfo=UTC)
_T2 = _T1 + timedelta(hours=1)


@pytest.fixture(autouse=True)
def _require_working_delta(spark, tmp_path_factory):
    """Skip unless this session can round-trip a Delta table; see test_dqd_load."""
    pytest.importorskip("delta", reason="delta-spark not installed")
    probe = str(tmp_path_factory.mktemp("delta_probe") / "t")
    try:
        spark.range(1).write.format("delta").mode("overwrite").save(probe)
    except Exception as exc:  # pragma: no cover — environment-dependent
        pytest.skip(f"Delta not usable in this Spark session: {type(exc).__name__}")


def _micros(moment: datetime) -> int:
    return (moment - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)


def _instants(frame, column):
    """A column's values as epoch microseconds: free of the session's time zone."""
    return sorted(r[0] for r in frame.select(F.unix_micros(F.col(column))).collect())


# ── SCD2 through a staging notebook ──────────────────────────────────────────

def _scd_context(root, when):
    return json.dumps({
        "global_context": {
            "id": "customer", "entity_name": "Customer", "source_system": "CRM",
            "target_table_path": f"{root}/silver/customer",
            "staging_table_path": f"{root}/staging/customer",
            "load_timestamp": when.isoformat(),
            "staging_table_schema": {"type": "struct", "fields": [
                {"name": "customer_id", "type": "integer", "nullable": True, "metadata": {}},
                {"name": "name", "type": "string", "nullable": True, "metadata": {}}]},
        },
        "staging_context": {"stages": [{"nb_name": "NB_Stage_Customer"}]},
        "scd_context": {"business_key_column": "customer_id", "scd_type": "scd2"},
        "runtime_context": {"correlation_id": "run", "pipeline_name": "customer", "application_name": "crm"},
        "logging_context": {"logging_level": "INFO", "log_file_name": "customer", "log_file_dir": f"{root}/logs"},
    })


@pytest.fixture
def run_scd(spark, tmp_path):
    """run_scd(rows, when) -> ExecutionResult; rows are (customer_id, name)."""
    staged = {"rows": []}

    def stage(storage_path, staging_context, global_context, notebook_name):
        return spark.createDataFrame(staged["rows"], "customer_id int, name string")

    main = create_staging_main_function(stage, adapter=LocalAdapter(), spark=spark)
    adapter = LocalAdapter(notebook_registry={"NB_Stage_Customer": lambda a: main(a["stage_context_str"]).to_json()})

    def _run(rows, when):
        staged["rows"] = rows
        return default_pipeline(_scd_context(tmp_path, when), spark, adapter=adapter).run()

    return _run


def _silver(spark, root):
    return spark.read.format("delta").load(f"{root}/silver/customer")


def test_scd2_effective_from_is_the_load_time_and_the_close_out_ends_a_microsecond_before_it(spark, tmp_path, run_scd):
    first = run_scd([(1, "Ada")], _T1)
    assert first.status == "success", first.error_details
    assert _instants(_silver(spark, tmp_path), "scdEffectiveFrom") == [_micros(_T1)]

    second = run_scd([(1, "Ada Lovelace")], _T2)
    assert second.status == "success", second.error_details
    table = _silver(spark, tmp_path)
    assert _instants(table.filter("scdVersion = 2"), "scdEffectiveFrom") == [_micros(_T2)]
    closed = table.filter("scdVersion = 1")
    assert _instants(closed, "scdEffectiveTo") == [_micros(_T2) - 1]
    assert closed.first()["scdCurrentFlag"] == "N"


def test_a_second_load_with_the_same_load_time_succeeds_under_scd2(spark, tmp_path, run_scd):
    """No backfill guard: the user owns the ordering. The close-out ends one microsecond before T,
    so the closed row's effective-to precedes its effective-from; the documented consequence."""
    assert run_scd([(1, "Ada")], _T1).status == "success"

    result = run_scd([(1, "Ada Lovelace")], _T1)

    assert result.status == "success", result.error_details
    table = _silver(spark, tmp_path)
    assert table.count() == 2
    assert _instants(table.filter("scdVersion = 2"), "scdEffectiveFrom") == [_micros(_T1)]
    closed = table.filter("scdVersion = 1")
    assert _instants(closed, "scdEffectiveTo") == [_micros(_T1) - 1]


# ── Data Vault ───────────────────────────────────────────────────────────────

_KEY = {"name": "HashKey", "expression": {"function": "sha2", "arguments": [
    {"function": "upper", "arguments": [{"function": "trim", "arguments": [{"column": "CUST_ID"}]}]},
    {"literal": 256}]}}


def _dv_context(root, style, table, when):
    selection = [{"name": "HashKey"}, {"name": "CustomerNo", "expression": {"column": "CUST_ID"}}]
    if style == "dv_satellite":
        selection = [{"name": "HashKey"}, {"name": "Name", "expression": {"column": "NAME"}}]
    return json.dumps({
        "global_context": {"id": "ent_1", "entity_name": "Customer", "source_system": "CRM",
                           "target_table_path": f"{root}/vault/{table}", "staging_table_path": f"{root}/unused",
                           "load_timestamp": when.isoformat()},
        "scd_context": {"scd_type": style, "business_key_column": "HashKey"},
        "runtime_context": {"correlation_id": "run", "pipeline_name": table, "application_name": "vault"},
        "logging_context": {"logging_level": "INFO", "log_file_name": table, "log_file_dir": f"{root}/logs"},
        "dqd_context": {"source_kind": "delta", "source_identifier": f"{root}/bronze/customers",
                        "transformations": [_KEY], "selection": selection},
    })


def _bronze(spark, root, rows):
    spark.createDataFrame(rows, "CUST_ID string, NAME string") \
        .write.format("delta").mode("overwrite").save(f"{root}/bronze/customers")


def _hub(spark, root, when):
    return dv_hub_pipeline(_dv_context(root, "dv_hub", "customer_h", when), spark, adapter=LocalAdapter()).run()


def _satellite(spark, root, when):
    return dv_satellite_pipeline(_dv_context(root, "dv_satellite", "customer_s", when), spark,
                                 adapter=LocalAdapter()).run()


def test_hub_and_satellite_load_ts_is_the_load_time(spark, tmp_path):
    _bronze(spark, tmp_path, [("1", "Ada")])
    assert _hub(spark, tmp_path, _T1).status == "success"
    assert _satellite(spark, tmp_path, _T1).status == "success"
    for table in ("customer_h", "customer_s"):
        assert _instants(spark.read.format("delta").load(f"{tmp_path}/vault/{table}"), "LoadTS") == [_micros(_T1)]


def test_a_satellite_load_with_the_same_load_time_is_not_refused_and_drops_the_change(spark, tmp_path):
    """The documented hazard: an equal LoadTS makes a real change match the (key, LoadTS) row."""
    _bronze(spark, tmp_path, [("1", "Ada")])
    assert _satellite(spark, tmp_path, _T1).status == "success"
    _bronze(spark, tmp_path, [("1", "Ada Lovelace")])

    result = _satellite(spark, tmp_path, _T1)

    assert result.status == "success", result.error_details
    assert spark.read.format("delta").load(f"{tmp_path}/vault/customer_s").count() == 1


def test_a_hub_takes_a_backfilled_load_time_for_a_key_first_seen_now(spark, tmp_path):
    _bronze(spark, tmp_path, [("1", "Ada")])
    assert _hub(spark, tmp_path, _T2).status == "success"
    _bronze(spark, tmp_path, [("1", "Ada"), ("2", "Grace")])

    result = _hub(spark, tmp_path, _T1)

    assert result.status == "success", result.error_details
    hub = spark.read.format("delta").load(f"{tmp_path}/vault/customer_h")
    assert _instants(hub, "LoadTS") == [_micros(_T1), _micros(_T2)], "LoadTS is when the key was first seen"
