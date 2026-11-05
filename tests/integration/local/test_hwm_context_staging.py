"""End to end: a staging load driven by `hwm_context`, run twice (F138).

The first load stages the whole source and reports its latest `modified_at`; the second, given
that value in `hwm_context`, stages only the rows changed since, and the watermark moves on. The
staging function reads `hwm_context` because its signature declares it. Requires a working
Delta install, like the other tests in this directory.
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


def _load_context(root, hwm_context):
    fields = [("customer_id", "integer"), ("name", "string"), ("modified_at", "timestamp"),
              ("scdDeletedFlag", "string")]
    return json.dumps({
        "global_context": {
            "id": "ent_1",
            "entity_name": "Customer", "source_system": "CRM",
            "target_table_path": f"{root}/silver/customer",
            "staging_table_path": f"{root}/staging/customer",
            "staging_table_schema": {"type": "struct", "fields": [
                {"name": n, "type": t, "nullable": True, "metadata": {}} for n, t in fields]},
        },
        "staging_context": {"stages": [{"nb_name": "NB_Stage_Customer"}]},
        # source_deleted_flag_column names the staged flag until F127 lands.
        "scd_context": {"business_key_column": "customer_id", "source_deleted_flag_column": "scdDeletedFlag"},
        "runtime_context": {"correlation_id": "hwm-1", "pipeline_name": "customer", "application_name": "crm"},
        "logging_context": {"logging_level": "INFO", "log_file_name": "customer", "log_file_dir": f"{root}/logs"},
        **({"hwm_context": hwm_context} if hwm_context is not None else {}),
    })


def _stage_main(spark, rows):
    def stage(storage_path, staging_context, global_context, notebook_name, hwm_context):
        return spark.createDataFrame(rows["rows"], "customer_id int, name string, modified_at string") \
            .selectExpr("customer_id", "name", "timestamp(modified_at) AS modified_at", "'N' AS scdDeletedFlag")

    main = create_staging_main_function(stage, adapter=LocalAdapter(), spark=spark)
    return LocalAdapter(notebook_registry={"NB_Stage_Customer": lambda a: main(a["stage_context_str"]).to_json()})


def test_a_first_watermark_run_that_stages_nothing_does_not_report_the_load_time(spark, tmp_path):
    """Moving an existing entity onto hwm_context: the target exists, the first run with the
    section stages nothing. The writer's or the target's effective-from is a load time from
    another column; fed back as hwm_value it would skip older source rows (F138 review)."""
    rows = {"rows": [(1, "Ada", "2026-09-02 10:00:00")]}
    adapter = _stage_main(spark, rows)
    before = default_pipeline(_load_context(tmp_path, None), spark, adapter=adapter).run()
    assert before.status == "success" and before.metrics["hwm_value_after"] is not None  # the load time

    rows["rows"] = []
    after = default_pipeline(_load_context(tmp_path, {"hwm_column_name": "modified_at"}), spark,
                             adapter=adapter).run()
    assert after.status == "success", after.error_details
    assert after.early_exit
    assert (after.metrics["hwm_value_before"], after.metrics["hwm_value_after"]) == (None, None)


def test_a_staging_load_filters_by_the_previous_runs_watermark(spark, tmp_path):
    source = {"rows": [(1, "Ada", "2026-09-02 10:00:00"), (2, "Grace", "2026-09-02 11:00:00")]}
    staged_counts = []

    def stage(storage_path, staging_context, global_context, notebook_name, hwm_context):
        frame = spark.createDataFrame(source["rows"], "customer_id int, name string, modified_at string") \
            .selectExpr("customer_id", "name", "timestamp(modified_at) AS modified_at", "'N' AS scdDeletedFlag")
        if hwm_context["hwm_value"] is not None:
            frame = frame.filter(f"{hwm_context['hwm_column_name']} > timestamp('{hwm_context['hwm_value']}')")
        staged_counts.append(frame.count())
        return frame

    main = create_staging_main_function(stage, adapter=LocalAdapter(), spark=spark)
    adapter = LocalAdapter(notebook_registry={"NB_Stage_Customer": lambda a: main(a["stage_context_str"]).to_json()})

    first = default_pipeline(_load_context(tmp_path, {"hwm_column_name": "modified_at"}), spark, adapter=adapter).run()
    assert first.status == "success", first.error_details
    assert (first.metrics["hwm_value_before"], first.metrics["hwm_value_after"]) == (None, "2026-09-02 11:00:00")

    source["rows"] = [(1, "Ada L.", "2026-09-03 09:00:00"), (2, "Grace", "2026-09-02 11:00:00"),
                      (3, "Alan", "2026-09-03 10:00:00")]
    second = default_pipeline(
        _load_context(tmp_path, {"hwm_column_name": "modified_at", "hwm_value": first.metrics["hwm_value_after"]}),
        spark, adapter=adapter).run()
    assert second.status == "success", second.error_details
    assert staged_counts == [2, 2]
    assert (second.metrics["inserted_count"], second.metrics["updated_count"]) == (2, 1)
    assert (second.metrics["hwm_value_before"], second.metrics["hwm_value_after"]) == \
        ("2026-09-02 11:00:00", "2026-09-03 10:00:00")
