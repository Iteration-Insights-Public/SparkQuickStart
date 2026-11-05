"""Characterization of the SCD2 output of the shipped pipelines, written before F28.

F28 changes how the target step reports "the table does not exist" (a PipelineState flag instead
of a None in `frames`). SCD output must not move, so this pins what `default_pipeline` writes today over the four first-load shapes: an absent
target, a second run with one changed row, an existing empty Delta table under a Full load with an empty source, and
an Incremental load whose keys match nothing in the target, the same through
`SCDIncrementalTargetTableTransformer` composed by hand, and a Full load of two rows into an
existing empty table. Rows are compared without their
timestamps. Requires a working Delta install.
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


_PIPELINES = {"default": default_pipeline}


def _incremental_target_pipeline(context_json, spark, *, adapter):
    """default_pipeline with SCDIncrementalTargetTableTransformer in place of the full target step.

    `default_pipeline` does not compose it, so the filtered-target path is only reachable by hand.
    """
    from spark_quick_start.orchestration.orchestrator import LoadOrchestrator
    from spark_quick_start.orchestration.transformers import (
        CandidateGuardTransformer,
        NativeSCDMergeTransformer,
        SCDChangeDetectorTransformer,
        SCDIncrementalTargetTableTransformer,
        StagingTransformer,
    )

    return (
        LoadOrchestrator(load_context_json=context_json, spark=spark, adapter=adapter)
        .add(StagingTransformer())
        .add(SCDIncrementalTargetTableTransformer())
        .add(SCDChangeDetectorTransformer())
        .add(CandidateGuardTransformer())
        .add(NativeSCDMergeTransformer())
    )


def _context(root, load_type):
    return json.dumps({
        "global_context": {
            "id": "ent_1",
            "entity_name": "Customer", "source_system": "CRM",
            "target_table_path": f"{root}/silver/customer",
            "staging_table_path": f"{root}/staging/customer",
            "staging_table_schema": {"type": "struct", "fields": [
                {"name": "customer_id", "type": "integer", "nullable": True, "metadata": {}},
                {"name": "name", "type": "string", "nullable": True, "metadata": {}}]},
        },
        "staging_context": {"stages": [{"nb_name": "NB_Stage_Customer"}]},
        "scd_context": {"business_key_column": "customer_id", "load_type": load_type, "scd_type": "scd2"},
        "runtime_context": {"correlation_id": "f28", "pipeline_name": "customer", "application_name": "crm"},
        "logging_context": {"logging_level": "INFO", "log_file_name": "customer", "log_file_dir": f"{root}/logs"},
    })


@pytest.fixture
def run(spark, tmp_path):
    """run(pipeline, rows, load_type) -> ExecutionResult, staging `rows` as (customer_id, name)."""
    staged = {"rows": []}

    def stage(storage_path, staging_context, global_context, notebook_name):
        return spark.createDataFrame(staged["rows"], "customer_id int, name string")

    main = create_staging_main_function(stage, adapter=LocalAdapter(), spark=spark)
    adapter = LocalAdapter(notebook_registry={"NB_Stage_Customer": lambda a: main(a["stage_context_str"]).to_json()})

    def _run(pipeline, rows, load_type="Incremental"):
        staged["rows"] = rows
        return _PIPELINES.get(pipeline, pipeline)(_context(tmp_path, load_type), spark, adapter=adapter).run()

    return _run


def _table(spark, root):
    """Every Silver row except its timestamps, sorted."""
    df = spark.read.format("delta").load(f"{root}/silver/customer")
    cols = ["customer_id", "name", "scdVersion", "scdCurrentFlag", "scdDeletedFlag"]
    return sorted(tuple(r) for r in df.select(*cols).collect())


def _metrics(result):
    m = result.metrics or {}
    return {k: m.get(k) for k in ("inserted_count", "updated_count")}


@pytest.mark.parametrize("pipeline", sorted(_PIPELINES))
def test_an_absent_target_is_a_first_load(spark, tmp_path, run, pipeline):
    result = run(pipeline, [(1, "Ada"), (2, "Grace")])

    assert result.status == "success", result.error_details
    assert _table(spark, tmp_path) == [(1, "Ada", 1, "Y", "N"), (2, "Grace", 1, "Y", "N")]
    assert _metrics(result) == {"inserted_count": 2, "updated_count": 0}


@pytest.mark.parametrize("pipeline", sorted(_PIPELINES))
def test_a_second_run_with_one_changed_row(spark, tmp_path, run, pipeline):
    run(pipeline, [(1, "Ada"), (2, "Grace")])

    result = run(pipeline, [(1, "Ada L."), (2, "Grace")])

    assert result.status == "success", result.error_details
    expected = [(1, "Ada", 1, "N", "N"), (1, "Ada L.", 2, "Y", "N"), (2, "Grace", 1, "Y", "N")]
    assert _table(spark, tmp_path) == expected
    assert _metrics(result) == {"inserted_count": 1, "updated_count": 1}


@pytest.mark.parametrize("pipeline", sorted(_PIPELINES))
def test_an_existing_empty_table_under_a_full_load_of_nothing(spark, tmp_path, run, pipeline):
    run(pipeline, [(1, "Ada")])
    spark.sql(f"DELETE FROM delta.`{tmp_path}/silver/customer`")
    assert _table(spark, tmp_path) == []

    result = run(pipeline, [], load_type="Full")

    assert result.status == "success", result.error_details
    assert _table(spark, tmp_path) == []
    # default_pipeline's CandidateGuardTransformer halts on zero candidates.
    assert result.early_exit is True
    assert _metrics(result) == {"inserted_count": 0, "updated_count": 0}


@pytest.mark.parametrize("pipeline", sorted(_PIPELINES))
def test_an_incremental_load_whose_keys_match_nothing_in_the_target(spark, tmp_path, run, pipeline):
    run(pipeline, [(1, "Ada")])

    result = run(pipeline, [(2, "Grace"), (3, "Alan")], load_type="Incremental")

    assert result.status == "success", result.error_details
    assert _table(spark, tmp_path) == [
        (1, "Ada", 1, "Y", "N"), (2, "Grace", 1, "Y", "N"), (3, "Alan", 1, "Y", "N")]
    assert _metrics(result) == {"inserted_count": 2, "updated_count": 0}


@pytest.mark.parametrize("pipeline", sorted(_PIPELINES))
def test_an_existing_empty_table_under_a_full_load_of_two_rows(spark, tmp_path, run, pipeline):
    run(pipeline, [(1, "Ada")])
    spark.sql(f"DELETE FROM delta.`{tmp_path}/silver/customer`")
    assert _table(spark, tmp_path) == []

    result = run(pipeline, [(1, "Ada"), (2, "Grace")], load_type="Full")

    assert result.status == "success", result.error_details
    assert result.early_exit is False
    assert _table(spark, tmp_path) == [(1, "Ada", 1, "Y", "N"), (2, "Grace", 1, "Y", "N")]
    assert _metrics(result) == {"inserted_count": 2, "updated_count": 0}


def test_the_incremental_target_step_filtering_to_an_empty_present_frame(spark, tmp_path, run):
    """The target exists, but none of the incoming keys are in it, so the filtered frame is empty."""
    run("default", [(1, "Ada")])

    result = run(_incremental_target_pipeline, [(2, "Grace"), (3, "Alan")], load_type="Incremental")

    assert result.status == "success", result.error_details
    assert _table(spark, tmp_path) == [
        (1, "Ada", 1, "Y", "N"), (2, "Grace", 1, "Y", "N"), (3, "Alan", 1, "Y", "N")]
    assert _metrics(result) == {"inserted_count": 2, "updated_count": 0}


def test_a_repeated_full_load_delete_changes_nothing(spark, tmp_path, run):
    """F142: 2 is deleted at version 2 by the first Full load; the same Full load again adds no version."""
    run("default", [(1, "Ada"), (2, "Grace")])
    run("default", [(1, "Ada")], load_type="Full")
    expected = [(1, "Ada", 1, "Y", "N"), (2, "Grace", 1, "N", "N"), (2, "Grace", 2, "Y", "Y")]
    assert _table(spark, tmp_path) == expected

    result = run("default", [(1, "Ada")], load_type="Full")

    assert result.status == "success", result.error_details
    assert result.early_exit is True
    assert _table(spark, tmp_path) == expected
    assert _metrics(result) == {"inserted_count": 0, "updated_count": 0}
