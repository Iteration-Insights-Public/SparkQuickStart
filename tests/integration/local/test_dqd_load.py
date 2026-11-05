"""End to end: a DQD load from a Delta table, with every SCD column renamed, run twice (F139).

The second run is filtered by the first run's watermark from `hwm_context`. The renamed columns
prove decision 11 of the design: the DQD reaches every SCD-managed column through the context's
map, never by a literal name. Requires a working Delta install.
"""
from __future__ import annotations

import json

import pytest

from spark_quick_start.adapters.local_adapter import LocalAdapter
from spark_quick_start.orchestration import (
    CandidateGuardTransformer,
    DQDTransformer,
    LoadOrchestrator,
    NativeSCDMergeTransformer,
    SCDChangeDetectorTransformer,
    SCDTargetTableTransformer,
)


@pytest.fixture(autouse=True)
def _require_working_delta(spark, tmp_path_factory):
    """Skip unless this session can round-trip a Delta table; see test_materialize_scratch_integration."""
    pytest.importorskip("delta", reason="delta-spark not installed")
    probe = str(tmp_path_factory.mktemp("delta_probe") / "t")
    try:
        spark.range(1).write.format("delta").mode("overwrite").save(probe)
    except Exception as exc:  # pragma: no cover — environment-dependent
        pytest.skip(f"Delta not usable in this Spark session: {type(exc).__name__}")


_RENAMED = {
    "row_key_column": "rk", "hash_column": "h", "version_column": "ver", "effective_from_column": "valid_from",
    "effective_to_column": "valid_to", "current_flag_column": "is_current", "deleted_flag_column": "is_deleted",
    "source_system_column": "src_sys", "correlation_id_column": "run_id",
}


def _context(root, hwm_value):
    return json.dumps({
        "global_context": {"id": "ent_1", "entity_name": "Customer", "source_system": "CRM",
                           "target_table_path": f"{root}/silver/customer", "staging_table_path": f"{root}/unused"},
        "scd_context": {"business_key_column": "customer_id", **_RENAMED},
        "runtime_context": {"correlation_id": "dqd-1", "pipeline_name": "customer", "application_name": "crm"},
        "logging_context": {"logging_level": "INFO", "log_file_name": "customer", "log_file_dir": f"{root}/logs"},
        "hwm_context": {"hwm_column_name": "MODIFIED_TS", **({"hwm_value": hwm_value} if hwm_value else {})},
        "dqd_context": {
            "source_kind": "delta", "source_identifier": f"{root}/bronze/customers",
            "filters": [{"operator": "!=", "left": {"column": "STATUS"}, "right": {"literal": "test"}}],
            "transformations": [{"name": "full_name", "expression": {"function": "concat_ws", "arguments": [
                {"literal": " "}, {"column": "FIRST"}, {"column": "LAST"}]}}],
            "selection": [{"name": "customer_id", "expression": {"column": "CUST_ID"}}, {"name": "full_name"}],
        },
    })


def _run(spark, root, hwm_value):
    return (LoadOrchestrator(_context(root, hwm_value), spark, adapter=LocalAdapter())
            .add(DQDTransformer()).add(SCDTargetTableTransformer()).add(SCDChangeDetectorTransformer())
            .add(CandidateGuardTransformer()).add(NativeSCDMergeTransformer()).run())


def test_a_dqd_load_with_renamed_scd_columns_runs_twice(spark, tmp_path):
    def write_source(rows):
        spark.createDataFrame(rows, "CUST_ID int, FIRST string, LAST string, STATUS string, MODIFIED string") \
            .selectExpr("*", "timestamp(MODIFIED) AS MODIFIED_TS").drop("MODIFIED") \
            .write.format("delta").mode("overwrite").save(f"{tmp_path}/bronze/customers")

    write_source([(1, "Ada", "Lovelace", "ok", "2026-09-01 10:00:00"),
                  (2, "Grace", "Hopper", "ok", "2026-09-02 10:00:00"),
                  (9, "Test", "Row", "test", "2026-09-02 11:00:00")])
    first = _run(spark, tmp_path, None)
    assert first.status == "success", first.error_details
    assert first.metrics["inserted_count"] == 2
    assert first.metrics["source_count"] == 2  # F144: the rows the DQD passed on, the test row filtered out
    assert first.metrics["hwm_value_after"] == "2026-09-02 10:00:00"

    write_source([(1, "Ada", "King", "ok", "2026-09-03 10:00:00"),
                  (2, "Grace", "Hopper", "ok", "2026-09-02 10:00:00"),
                  (3, "Alan", "Turing", "ok", "2026-09-03 11:00:00")])
    second = _run(spark, tmp_path, first.metrics["hwm_value_after"])
    assert second.status == "success", second.error_details
    assert (second.metrics["inserted_count"], second.metrics["updated_count"]) == (2, 1)
    assert (second.metrics["hwm_value_before"], second.metrics["hwm_value_after"]) == \
        ("2026-09-02 10:00:00", "2026-09-03 11:00:00")

    target = spark.read.format("delta").load(f"{tmp_path}/silver/customer")
    assert set(_RENAMED.values()) <= set(target.columns)
    assert not {"scdHash", "scdVersion", "scdDeletedFlag", "scdCurrentFlag"} & set(target.columns)
    current = sorted((r.customer_id, r.full_name, r.ver, r.is_deleted)
                     for r in target.filter("is_current = 'Y'").collect())
    assert current == [(1, "Ada King", 2, "N"), (2, "Grace Hopper", 1, "N"), (3, "Alan Turing", 1, "N")]
