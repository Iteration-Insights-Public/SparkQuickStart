"""SCD1 MERGE: which columns an update writes (F150), against a real local Delta target.

The update set is the business columns plus the current flag, effective-from, hash, deleted flag,
version and correlation id, named by the context, not by a "scd" prefix. Requires a working Delta install.
"""
from __future__ import annotations

import json

import pytest

from spark_quick_start.adapters.local_adapter import LocalAdapter
from spark_quick_start.orchestration import default_pipeline
from spark_quick_start.orchestration.staging_pipeline import create_staging_main_function


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


def _context(root, correlation_id, scd_extra):
    return json.dumps({
        "global_context": {
            "id": "ent_1",
            "entity_name": "Customer", "source_system": "CRM",
            "target_table_path": f"{root}/silver/customer",
            "staging_table_path": f"{root}/staging/customer",
            "staging_table_schema": {"type": "struct", "fields": [
                {"name": "customer_id", "type": "integer", "nullable": True, "metadata": {}},
                {"name": "name", "type": "string", "nullable": True, "metadata": {}},
                {"name": "scd_region", "type": "string", "nullable": True, "metadata": {}}]},
        },
        "staging_context": {"stages": [{"nb_name": "NB_Stage_Customer"}]},
        "scd_context": {"business_key_column": "customer_id", "load_type": "Incremental",
                        "scd_type": "scd1", **scd_extra},
        "runtime_context": {"correlation_id": correlation_id, "pipeline_name": "customer", "application_name": "crm"},
        "logging_context": {"logging_level": "INFO", "log_file_name": "customer", "log_file_dir": f"{root}/logs"},
    })


@pytest.fixture
def run(spark, tmp_path):
    """run(rows, correlation_id, scd_extra=None) -> ExecutionResult; rows are (customer_id, name, scd_region)."""
    staged = {"rows": []}

    def stage(storage_path, staging_context, global_context, notebook_name):
        return spark.createDataFrame(staged["rows"], "customer_id int, name string, scd_region string")

    main = create_staging_main_function(stage, adapter=LocalAdapter(), spark=spark)
    adapter = LocalAdapter(notebook_registry={"NB_Stage_Customer": lambda a: main(a["stage_context_str"]).to_json()})

    def _run(rows, correlation_id, scd_extra=None):
        staged["rows"] = rows
        result = default_pipeline(_context(tmp_path, correlation_id, scd_extra or {}), spark, adapter=adapter).run()
        assert result.status == "success", result.error_details
        return result

    return _run


def _rows(spark, root):
    return {r["customer_id"]: r.asDict()
            for r in spark.read.format("delta").load(f"{root}/silver/customer").collect()}


def test_a_business_column_starting_with_scd_is_updated(spark, tmp_path, run):
    run([(1, "Ada", "north")], "run-1")

    result = run([(1, "Ada L.", "south")], "run-2")

    row = _rows(spark, tmp_path)[1]
    assert (row["name"], row["scd_region"]) == ("Ada L.", "south")
    assert result.metrics["updated_count"] == 1


def test_the_correlation_id_is_the_last_run_that_changed_the_row(spark, tmp_path, run):
    run([(1, "Ada", "north"), (2, "Grace", "east")], "run-1")

    run([(1, "Ada L.", "north"), (2, "Grace", "east")], "run-2")

    rows = _rows(spark, tmp_path)
    assert rows[1]["scdCorrelationId"] == "run-2"
    assert rows[2]["scdCorrelationId"] == "run-1"


def test_renamed_meta_columns_keep_identity_and_take_the_updating_runs_correlation_id(spark, tmp_path, run):
    """Row key and valid_to are set to sentinels between the runs; neither is in the MERGE ON clause, so
    run-2 still matches the row, and an update set that included them would overwrite the sentinels.

    The source system column cannot be tested this way: it is in the ON clause, so changing it
    would stop the row matching.
    """
    run([(1, "Ada", "north")], "run-1", _RENAMED)
    spark.sql(f"UPDATE delta.`{tmp_path}/silver/customer` SET rk = 'sentinel-rk', valid_to = timestamp'2001-01-01 00:00:00'")
    first = _rows(spark, tmp_path)[1]
    assert first["rk"] == "sentinel-rk"

    run([(1, "Ada L.", "north")], "run-2", _RENAMED)

    row = _rows(spark, tmp_path)[1]
    assert row["name"] == "Ada L."
    assert (row["rk"], row["valid_to"]) == ("sentinel-rk", first["valid_to"])
    assert (first["run_id"], row["run_id"]) == ("run-1", "run-2")
    assert row["ver"] == 1
