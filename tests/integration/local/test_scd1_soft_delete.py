"""SCD1 soft delete (F110), against a real local Delta target.

The owner's decision (2026-10-03): under SCD1 a `deleted` candidate flags the matched target row,
keeps its business columns, stamps effective-from and the correlation id, and adds no version and
no row. A key that returns is updated and un-flagged. Requires a working Delta install.
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


def _context(root, correlation_id, load_type):
    return json.dumps({
        "global_context": {
            "id": "customer", "entity_name": "Customer", "source_system": "CRM",
            "target_table_path": f"{root}/silver/customer",
            "staging_table_path": f"{root}/staging/customer",
            "staging_table_schema": {"type": "struct", "fields": [
                {"name": "customer_id", "type": "integer", "nullable": True, "metadata": {}},
                {"name": "name", "type": "string", "nullable": True, "metadata": {}},
                {"name": "scdDeletedFlag", "type": "string", "nullable": True, "metadata": {}}]},
        },
        "staging_context": {"stages": [{"nb_name": "NB_Stage_Customer"}]},
        "scd_context": {"business_key_column": "customer_id", "load_type": load_type, "scd_type": "scd1"},
        "runtime_context": {"correlation_id": correlation_id, "pipeline_name": "customer", "application_name": "crm"},
        "logging_context": {"logging_level": "INFO", "log_file_name": "customer", "log_file_dir": f"{root}/logs"},
    })


@pytest.fixture
def run(spark, tmp_path):
    """run(rows, correlation_id, load_type="Incremental") -> ExecutionResult; rows are (id, name, flag)."""
    staged = {"rows": []}

    def stage(storage_path, staging_context, global_context, notebook_name):
        return spark.createDataFrame(staged["rows"], "customer_id int, name string, scdDeletedFlag string")

    main = create_staging_main_function(stage, adapter=LocalAdapter(), spark=spark)
    adapter = LocalAdapter(notebook_registry={"NB_Stage_Customer": lambda a: main(a["stage_context_str"]).to_json()})

    def _run(rows, correlation_id, load_type="Incremental"):
        staged["rows"] = rows
        result = default_pipeline(_context(tmp_path, correlation_id, load_type), spark, adapter=adapter).run()
        assert result.status == "success", result.error_details
        return result

    return _run


def _table(spark, root):
    return {r["customer_id"]: r.asDict()
            for r in spark.read.format("delta").load(f"{root}/silver/customer").collect()}


def test_a_source_delete_flags_the_row_and_keeps_its_business_columns(spark, tmp_path, run):
    run([(1, "Ada", "N"), (2, "Grace", "N")], "run-1")
    before = _table(spark, tmp_path)

    result = run([(2, "ignored by a delete", "Y")], "run-2")

    after = _table(spark, tmp_path)
    assert set(after) == {1, 2}, "no row is added or removed"
    assert after[2]["scdDeletedFlag"] == "Y"
    assert after[2]["name"] == "Grace"
    assert after[2]["scdVersion"] == before[2]["scdVersion"] == 1
    assert after[2]["scdCorrelationId"] == "run-2"
    assert after[2]["scdEffectiveFrom"] > before[2]["scdEffectiveFrom"]
    assert after[2]["scdCurrentFlag"] == "Y"
    assert after[1] == before[1]
    assert result.metrics["updated_count"] == 1 and result.metrics["inserted_count"] == 0


def test_a_full_load_absence_flags_the_row(spark, tmp_path, run):
    run([(1, "Ada", "N"), (2, "Grace", "N")], "run-1")

    run([(1, "Ada", "N")], "run-2", load_type="Full")

    after = _table(spark, tmp_path)
    assert (after[2]["scdDeletedFlag"], after[2]["name"], after[2]["scdCorrelationId"]) == ("Y", "Grace", "run-2")
    assert (after[1]["scdDeletedFlag"], after[1]["scdCorrelationId"]) == ("N", "run-1")


def test_a_key_that_comes_back_is_updated_and_unflagged(spark, tmp_path, run):
    run([(1, "Ada", "N")], "run-1")
    run([(1, "x", "Y")], "run-2")

    run([(1, "Ada", "N")], "run-3")

    row = _table(spark, tmp_path)[1]
    assert (row["scdDeletedFlag"], row["name"], row["scdCorrelationId"], row["scdVersion"]) == ("N", "Ada", "run-3", 1)


def test_a_repeated_delete_writes_nothing(spark, tmp_path, run):
    run([(1, "Ada", "N")], "run-1")
    run([(1, "x", "Y")], "run-2")

    result = run([(1, "x", "Y")], "run-3")

    assert result.early_exit
    assert _table(spark, tmp_path)[1]["scdCorrelationId"] == "run-2"


def test_a_batch_of_a_delete_a_new_key_and_a_change(spark, tmp_path, run):
    run([(1, "Ada", "N"), (2, "Grace", "N")], "run-1")

    result = run([(2, "gone", "Y"), (3, "Alan", "N"), (1, "Ada L.", "N")], "run-2")

    after = _table(spark, tmp_path)
    assert set(after) == {1, 2, 3}
    assert (after[3]["name"], after[3]["scdDeletedFlag"], after[3]["scdCorrelationId"]) == ("Alan", "N", "run-2")
    assert (after[1]["name"], after[1]["scdDeletedFlag"]) == ("Ada L.", "N")
    assert (after[2]["name"], after[2]["scdDeletedFlag"]) == ("Grace", "Y")
    assert (result.metrics["inserted_count"], result.metrics["updated_count"]) == (1, 2)
