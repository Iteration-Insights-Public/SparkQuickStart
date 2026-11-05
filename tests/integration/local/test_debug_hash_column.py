"""A debug_hash_string_column never reaches the target (F111), on a real local Delta table.

The detector adds the column and declares it on the state; the writer drops it before the
MERGE, so the target schema does not gain it however the Delta session treats schema evolution.
Requires a working Delta install.
"""
from __future__ import annotations

import json

import pytest

from spark_quick_start.adapters.local_adapter import LocalAdapter
from spark_quick_start.orchestration import LoadOrchestrator
from spark_quick_start.orchestration.staging_pipeline import create_staging_main_function
from spark_quick_start.orchestration.transformers import (
    CandidateGuardTransformer,
    NativeSCDAppendOnlyTransformer,
    NativeSCDMergeTransformer,
    SCDChangeDetectorTransformer,
    SCDTargetTableTransformer,
    StagingTransformer,
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


def _context(root, correlation_id, load_type="Incremental"):
    return json.dumps({
        "global_context": {
            "id": "customer", "entity_name": "Customer", "source_system": "CRM",
            "target_table_path": f"{root}/silver/customer",
            "staging_table_path": f"{root}/staging/customer",
            "staging_table_schema": {"type": "struct", "fields": [
                {"name": "customer_id", "type": "integer", "nullable": True, "metadata": {}},
                {"name": "name", "type": "string", "nullable": True, "metadata": {}}]},
        },
        "staging_context": {"stages": [{"nb_name": "NB_Stage_Customer"}]},
        "scd_context": {"business_key_column": "customer_id", "load_type": load_type, "scd_type": "scd2"},
        "runtime_context": {"correlation_id": correlation_id, "pipeline_name": "customer", "application_name": "crm"},
        "logging_context": {"logging_level": "INFO", "log_file_name": "customer", "log_file_dir": f"{root}/logs"},
    })


@pytest.fixture
def load(spark, tmp_path):
    """load(rows, correlation_id, load_type="Incremental", debug="dbg_hash") -> ExecutionResult."""
    staged = {"rows": []}

    def stage(storage_path, staging_context, global_context, notebook_name):
        return spark.createDataFrame(staged["rows"], "customer_id int, name string")

    main = create_staging_main_function(stage, adapter=LocalAdapter(), spark=spark)
    adapter = LocalAdapter(notebook_registry={"NB_Stage_Customer": lambda a: main(a["stage_context_str"]).to_json()})

    def _load(rows, correlation_id, load_type="Incremental", debug="dbg_hash", writer=NativeSCDMergeTransformer):
        staged["rows"] = rows
        result = (
            LoadOrchestrator(
                load_context_json=_context(tmp_path, correlation_id, load_type), spark=spark, adapter=adapter)
            .add(StagingTransformer())
            .add(SCDTargetTableTransformer())
            .add(SCDChangeDetectorTransformer(debug_hash_string_column=debug))
            .add(CandidateGuardTransformer())
            .add(writer())
            .run()
        )
        assert result.status == "success", result.error_details
        return result

    return _load


def _table(spark, root):
    return spark.read.format("delta").load(f"{root}/silver/customer")


def test_the_target_schema_does_not_gain_the_debug_column(spark, tmp_path, load):
    load([(1, "Ada"), (2, "Grace")], "run-1")
    load([(1, "Ada L."), (2, "Grace")], "run-2")  # an SCD2 change: close-out plus new version

    assert "dbg_hash" not in _table(spark, tmp_path).columns
    assert _table(spark, tmp_path).filter("customer_id = 1").count() == 2


def test_a_full_load_with_a_change_and_no_delete_does_not_gain_the_column(spark, tmp_path, load):
    load([(1, "Ada"), (2, "Grace")], "run-1")

    load([(1, "Ada L."), (2, "Grace")], "run-2", load_type="Full")

    assert "dbg_hash" not in _table(spark, tmp_path).columns
    assert _table(spark, tmp_path).filter("customer_id = 1").count() == 2


def test_a_full_load_with_a_delete_succeeds_and_does_not_gain_the_column(spark, tmp_path, load):
    load([(1, "Ada"), (2, "Grace")], "run-1")

    load([(1, "Ada")], "run-2", load_type="Full")  # 2 is absent: deleted

    table = _table(spark, tmp_path)
    assert "dbg_hash" not in table.columns
    assert table.filter("customer_id = 2 AND scdCurrentFlag = 'Y'").first()["scdDeletedFlag"] == "Y"


def test_a_target_that_already_holds_the_column_keeps_it_and_later_loads_leave_it_null(spark, tmp_path, load):
    """A target written before F111 can hold the debug column; a load whose source lacks it still works.

    Observed on local Delta: the MERGE's insert and update take the target's column as NULL for the
    new rows, and the column stays in the schema. The remedy is `ALTER TABLE ... DROP COLUMN` (needs
    column mapping) or leaving it, since it is NULL from then on. Unproven on Fabric/Databricks.
    """
    load([(1, "Ada")], "run-1")
    spark.sql(f"ALTER TABLE delta.`{tmp_path}/silver/customer` ADD COLUMNS (dbg_hash string)")
    spark.sql(f"UPDATE delta.`{tmp_path}/silver/customer` SET dbg_hash = 'old'")

    load([(1, "Ada L."), (2, "Grace")], "run-2", debug=None)

    rows = {(r["customer_id"], r["scdVersion"]): r["dbg_hash"] for r in _table(spark, tmp_path).collect()}
    assert rows == {(1, 1): "old", (1, 2): None, (2, 1): None}


def test_the_append_only_writer_into_a_target_holding_the_column_leaves_it_null(spark, tmp_path, load):
    """As above for the append: Delta fills the target column the source lacks with NULL. Unproven on Fabric/Databricks."""
    load([(1, "Ada")], "run-1")
    spark.sql(f"ALTER TABLE delta.`{tmp_path}/silver/customer` ADD COLUMNS (dbg_hash string)")

    load([(2, "Grace")], "run-2", debug=None, writer=NativeSCDAppendOnlyTransformer)

    rows = {r["customer_id"]: r["dbg_hash"] for r in _table(spark, tmp_path).collect()}
    assert rows == {1: None, 2: None}
