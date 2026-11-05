"""F162 end to end: a Data Vault hub and satellite loaded from one DQD source, on local Delta.

The satellite takes a change, a source delete, a reappearance and a `Full` delete as new rows,
never updating one; a no-change run adds nothing; a duplicate staged key and a run hashing
another way are refused before anything is written. Requires a working Delta install.
"""
from __future__ import annotations

import json

import pytest

from spark_quick_start.adapters.local_adapter import LocalAdapter
from spark_quick_start.orchestration import dv_hub_pipeline, dv_satellite_pipeline


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


def _context(root, style, table, load_type="Incremental", **scd):
    scd_context = {"scd_type": style, "business_key_column": "HashKey", "load_type": load_type, **scd}
    selection = [{"name": "HashKey"}, {"name": "CustomerNo", "expression": {"column": "CUST_ID"}}]
    if style == "dv_satellite":
        scd_context["source_deleted_flag_column"] = "Op"
        selection = [{"name": "HashKey"}, {"name": "Name", "expression": {"column": "NAME"}},
                     {"name": "City", "expression": {"column": "CITY"}}, {"name": "Op", "expression": {"column": "OP"}}]
    return json.dumps({
        "global_context": {"id": "ent_1", "entity_name": "Customer", "source_system": "CRM",
                           "target_table_path": f"{root}/vault/{table}", "staging_table_path": f"{root}/unused"},
        "scd_context": scd_context,
        "runtime_context": {"correlation_id": "run", "pipeline_name": table, "application_name": "vault"},
        "logging_context": {"logging_level": "INFO", "log_file_name": table, "log_file_dir": f"{root}/logs"},
        "dqd_context": {"source_kind": "delta", "source_identifier": f"{root}/bronze/customers",
                        "transformations": [_KEY], "selection": selection},
    })


def _load(spark, root, rows, load_type="Incremental", **scd):
    """Land *rows* (id, name, city, op) in bronze, then run the hub and the satellite on them."""
    spark.createDataFrame(rows, "CUST_ID string, NAME string, CITY string, OP string") \
        .write.format("delta").mode("overwrite").save(f"{root}/bronze/customers")
    hub = dv_hub_pipeline(_context(root, "dv_hub", "customer_h", load_type), spark, adapter=LocalAdapter()).run()
    assert hub.status == "success", hub.error_details
    return dv_satellite_pipeline(_context(root, "dv_satellite", "customer_crm_s", load_type, **scd), spark,
                                 adapter=LocalAdapter()).run()


def _history(spark, root, customer):
    """One customer's satellite rows, oldest first, as (name, city, deleted)."""
    rows = spark.read.format("delta").load(f"{root}/vault/customer_crm_s").collect()
    hub = {r.CustomerNo: r.HashKey for r in spark.read.format("delta").load(f"{root}/vault/customer_h").collect()}
    mine = sorted((r for r in rows if r.HashKey == hub[customer]), key=lambda r: r.LoadTS)
    return [(r.Name, r.City, r.IsDeleted) for r in mine]


def test_a_satellite_records_each_change_as_a_new_row_and_never_updates(spark, tmp_path):
    first = _load(spark, tmp_path, [("1", "Ada", "Oslo", None), ("2", "Grace", "Rome", None)])
    assert first.status == "success", first.error_details
    assert first.metrics["inserted_count"] == 2

    same = _load(spark, tmp_path, [("1", "Ada", "Oslo", None), ("2", "Grace", "Rome", None)])
    assert same.status == "success", same.error_details
    assert same.metrics["inserted_count"] == 0

    changed = _load(spark, tmp_path, [("1", "Ada", "Bergen", None), ("2", None, None, "D")],
                    source_deleted_flag_value="D")
    assert changed.status == "success", changed.error_details
    assert changed.metrics["inserted_count"] == 2

    back = _load(spark, tmp_path, [("2", "Grace", "Rome", None)])
    assert back.status == "success", back.error_details

    assert _history(spark, tmp_path, "1") == [("Ada", "Oslo", "N"), ("Ada", "Bergen", "N")]
    assert _history(spark, tmp_path, "2") == [("Grace", "Rome", "N"), ("Grace", "Rome", "Y"), ("Grace", "Rome", "N")]
    row = spark.read.format("delta").load(f"{tmp_path}/vault/customer_crm_s").first().asDict()
    assert {"HashKey", "HashDiff", "IsDeleted", "LoadTS", "LoadEndTS", "RecordSource", "CorrelationId"} <= set(row)
    assert "Op" not in row and "_change_type" not in row


def test_a_full_load_records_an_absent_key_as_deleted(spark, tmp_path):
    assert _load(spark, tmp_path, [("1", "Ada", "Oslo", None), ("2", "Grace", "Rome", None)]).status == "success"
    full = _load(spark, tmp_path, [("1", "Ada", "Oslo", None)], load_type="Full")
    assert full.status == "success", full.error_details
    assert full.metrics["inserted_count"] == 1
    assert _history(spark, tmp_path, "2") == [("Grace", "Rome", "N"), ("Grace", "Rome", "Y")]


def test_a_duplicate_staged_key_is_refused_before_the_satellite_exists(spark, tmp_path):
    spark.createDataFrame([("1", "Ada", "Oslo", None), (" 1", "Ada", "Oslo", None)],
                          "CUST_ID string, NAME string, CITY string, OP string") \
        .write.format("delta").mode("overwrite").save(f"{tmp_path}/bronze/customers")
    result = dv_satellite_pipeline(_context(tmp_path, "dv_satellite", "customer_crm_s"), spark,
                                   adapter=LocalAdapter()).run()
    assert result.status == "error"
    assert "staged more than once" in result.error_details["error_message"]
    assert not (tmp_path / "vault" / "customer_crm_s").exists()


def test_a_satellite_sealed_with_sha256_refuses_an_md5_run(spark, tmp_path):
    assert _load(spark, tmp_path, [("1", "Ada", "Oslo", None)]).status == "success"
    md5 = _load(spark, tmp_path, [("1", "Ada", "Bergen", None)], hash_algorithm="md5")
    assert md5.status == "error"
    assert "sealed" in md5.error_details["error_message"]
    assert len(_history(spark, tmp_path, "1")) == 1
