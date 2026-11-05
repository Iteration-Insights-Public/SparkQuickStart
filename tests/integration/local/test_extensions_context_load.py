"""End to end: `extensions_context` and `platform_context` through a real load.

A custom `ProcessTransformer` reads `ctx.extensions_ctx` and `ctx.platform_ctx` during a real
`LoadOrchestrator.run()`, and a staging function receives the same extensions through the
`extensions_context` section of its notebook's context. Requires a working Delta install, like
the other tests in this directory.
"""
from __future__ import annotations

import json

import pytest

from spark_quick_start.adapters.local_adapter import LocalAdapter
from spark_quick_start.orchestration import ProcessTransformer, default_pipeline
from spark_quick_start.orchestration.staging_pipeline import create_staging_main_function

_EXTENSIONS = {"acme.test": {"threshold": 3, "tags": ["a", "b"]}, "acme.other": "x"}


@pytest.fixture(autouse=True)
def _require_working_delta(spark, tmp_path_factory):
    """Skip unless this session can round-trip a Delta table; see test_materialize_scratch_integration."""
    pytest.importorskip("delta", reason="delta-spark not installed")
    probe = str(tmp_path_factory.mktemp("delta_probe") / "t")
    try:
        spark.range(1).write.format("delta").mode("overwrite").save(probe)
    except Exception as exc:  # pragma: no cover — environment-dependent
        pytest.skip(f"Delta not usable in this Spark session: {type(exc).__name__}")


def _load_context(root, **extra):
    fields = [("customer_id", "integer"), ("name", "string")]
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
        "scd_context": {"business_key_column": "customer_id"},
        "runtime_context": {"correlation_id": "ext-1", "pipeline_name": "customer", "application_name": "crm"},
        "logging_context": {"logging_level": "INFO", "log_file_name": "customer", "log_file_dir": f"{root}/logs"},
        **extra,
    })


class _ReadsExtensions(ProcessTransformer):
    """Records what a custom step can read from the context during the run."""

    def __init__(self):
        self.seen = {}

    def _run(self, state, ctx):
        self.seen["threshold"] = ctx.extensions_ctx["acme.test"]["threshold"]
        self.seen["all"] = ctx.extensions_ctx.to_dict()
        self.seen["storage"] = ctx.platform_ctx.data_storage_lakehouse_path
        self.seen["engineering"] = ctx.platform_ctx.data_engineering_lakehouse_path
        return state


def test_a_custom_step_and_a_staging_function_both_read_the_extensions(spark, tmp_path):
    from_notebook = {}

    def stage(storage_path, staging_context, global_context, notebook_name, extensions_context):
        from_notebook["extensions"] = extensions_context
        from_notebook["storage_path"] = storage_path
        from_notebook["global_context_keys"] = sorted(global_context)
        return spark.createDataFrame([(1, "Ada")], "customer_id int, name string") \
            .selectExpr("customer_id", "name")

    main = create_staging_main_function(stage, adapter=LocalAdapter(), spark=spark)
    adapter = LocalAdapter(notebook_registry={"NB_Stage_Customer": lambda a: main(a["stage_context_str"]).to_json()})
    step = _ReadsExtensions()

    result = default_pipeline(
        _load_context(tmp_path, extensions_context=_EXTENSIONS), spark, adapter=adapter,
        data_storage_path=f"{tmp_path}/storage", data_eng_path=f"{tmp_path}/engineering",
    ).add(step).run()

    assert result.status == "success", result.error_details
    assert step.seen == {"threshold": 3, "all": _EXTENSIONS,
                         "storage": f"{tmp_path}/storage", "engineering": f"{tmp_path}/engineering"}
    assert from_notebook["extensions"] == _EXTENSIONS
    assert from_notebook["storage_path"] == f"{tmp_path}/storage"
    assert from_notebook["global_context_keys"] == ["source_system", "staging_table_path"]


def test_a_load_without_extensions_gives_a_custom_step_an_empty_section(spark, tmp_path):
    def stage(storage_path, staging_context, global_context, notebook_name):
        return spark.createDataFrame([(1, "Ada")], "customer_id int, name string")

    main = create_staging_main_function(stage, adapter=LocalAdapter(), spark=spark)
    adapter = LocalAdapter(notebook_registry={"NB_Stage_Customer": lambda a: main(a["stage_context_str"]).to_json()})
    seen = {}

    class Probe(ProcessTransformer):
        def _run(self, state, ctx):
            seen["extensions"] = ctx.extensions_ctx.to_dict()
            seen["default"] = ctx.extensions_ctx.get("acme.test", "fallback")
            return state

    result = default_pipeline(_load_context(tmp_path), spark, adapter=adapter).add(Probe()).run()

    assert result.status == "success", result.error_details
    assert seen == {"extensions": {}, "default": "fallback"}
