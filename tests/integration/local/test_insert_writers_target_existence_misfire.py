"""A false "absent" from the target step must not re-insert rows into an append-only or satellite target (F210).

`delta_table_exists` answers False on any error. When the target step's check misfires, the
detector classifies every staged row new; the append-only writer then appended rows already in
the table and a satellite's insert-only MERGE recorded every key again under a new `LoadTS`, both
with status success. The writers now create on that answer through `errorifexists`, so a table
that is in fact there is refused. Against real local Delta targets. Requires a working Delta install.
"""
from __future__ import annotations

import json

import pytest

from spark_quick_start.adapters.local_adapter import LocalAdapter
from spark_quick_start.orchestration import LoadOrchestrator, dv_hub_pipeline, dv_satellite_pipeline
from spark_quick_start.orchestration.staging_pipeline import create_staging_main_function
from spark_quick_start.orchestration.transformers import (
    CandidateGuardTransformer,
    NativeSCDAppendOnlyTransformer,
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


class _BlindAdapter(LocalAdapter):
    """A LocalAdapter whose first existence check for `blind_to` answers False, as a swallowed error does.

    Only the first: the target step's, the more likely transient case, so a later check in the
    writer would find the table.
    """

    blind_to: str | None = None
    blind_calls: int = 0

    def delta_table_exists(self, spark, identifier):
        if identifier == self.blind_to and self.blind_calls > 0:
            self.blind_calls -= 1
            return False
        return super().delta_table_exists(spark, identifier)


def _refused_as_existing(result, target):
    message = result.error_details["error_message"].lower()
    return target in message and "exist" in message


# --- append-only -------------------------------------------------------------------------------

def _append_context(root):
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
        "scd_context": {"business_key_column": "customer_id", "load_type": "Incremental", "scd_type": "scd2"},
        "runtime_context": {"correlation_id": "run", "pipeline_name": "customer", "application_name": "crm"},
        "logging_context": {"logging_level": "INFO", "log_file_name": "customer", "log_file_dir": f"{root}/logs"},
    })


@pytest.fixture
def append_load(spark, tmp_path):
    """append_load(rows, blind=False) -> ExecutionResult; rows are (customer_id, name)."""
    staged = {"rows": []}

    def stage(storage_path, staging_context, global_context, notebook_name):
        return spark.createDataFrame(staged["rows"], "customer_id int, name string")

    main = create_staging_main_function(stage, adapter=LocalAdapter(), spark=spark)
    adapter = _BlindAdapter(notebook_registry={"NB_Stage_Customer": lambda a: main(a["stage_context_str"]).to_json()})

    def _load(rows, blind=False):
        staged["rows"] = rows
        adapter.blind_to = f"{tmp_path}/silver/customer" if blind else None
        adapter.blind_calls = 1 if blind else 0
        return (
            LoadOrchestrator(load_context_json=_append_context(tmp_path), spark=spark, adapter=adapter)
            .add(StagingTransformer())
            .add(SCDTargetTableTransformer())
            .add(SCDChangeDetectorTransformer())
            .add(CandidateGuardTransformer())
            .add(NativeSCDAppendOnlyTransformer())
            .run()
        )

    return _load


def _customers(spark, root):
    return sorted(tuple(r) for r in spark.read.format("delta").load(f"{root}/silver/customer")
                  .select("customer_id", "name", "scdVersion").collect())


def test_append_only_a_false_absent_answer_to_the_target_step_appends_nothing(spark, tmp_path, append_load):
    assert append_load([(1, "Ada"), (2, "Grace")]).status == "success"
    before = _customers(spark, tmp_path)

    result = append_load([(1, "Ada"), (2, "Grace")], blind=True)

    assert _customers(spark, tmp_path) == before
    assert result.status == "error"
    assert _refused_as_existing(result, "silver/customer"), result.error_details


def test_append_only_a_first_load_still_creates_the_target(spark, tmp_path, append_load):
    result = append_load([(1, "Ada"), (2, "Grace")])

    assert result.status == "success", result.error_details
    assert [(c, n) for c, n, _ in _customers(spark, tmp_path)] == [(1, "Ada"), (2, "Grace")]
    assert result.metrics["inserted_count"] == 2


# --- satellite ---------------------------------------------------------------------------------

_KEY = {"name": "HashKey", "expression": {"function": "sha2", "arguments": [
    {"function": "upper", "arguments": [{"function": "trim", "arguments": [{"column": "CUST_ID"}]}]},
    {"literal": 256}]}}


def _vault_context(root, style, table):
    scd_context = {"scd_type": style, "business_key_column": "HashKey", "load_type": "Incremental"}
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


def _satellite_load(spark, root, rows, blind=False):
    """Land *rows* (id, name, city, op) in bronze, then run the hub and the satellite on them."""
    spark.createDataFrame(rows, "CUST_ID string, NAME string, CITY string, OP string") \
        .write.format("delta").mode("overwrite").save(f"{root}/bronze/customers")
    hub = dv_hub_pipeline(_vault_context(root, "dv_hub", "customer_h"), spark, adapter=LocalAdapter()).run()
    assert hub.status == "success", hub.error_details
    adapter = _BlindAdapter()
    adapter.blind_to = f"{root}/vault/customer_crm_s" if blind else None
    adapter.blind_calls = 1 if blind else 0
    return dv_satellite_pipeline(_vault_context(root, "dv_satellite", "customer_crm_s"), spark, adapter=adapter).run()


def _satellite(spark, root):
    return sorted(tuple(r) for r in spark.read.format("delta").load(f"{root}/vault/customer_crm_s")
                  .select("HashKey", "Name", "City", "LoadTS").collect())


def test_a_satellite_a_false_absent_answer_to_the_target_step_records_nothing_again(spark, tmp_path):
    rows = [("1", "Ada", "Oslo", None), ("2", "Grace", "Rome", None)]
    assert _satellite_load(spark, tmp_path, rows).status == "success"
    before = _satellite(spark, tmp_path)

    result = _satellite_load(spark, tmp_path, rows, blind=True)

    assert _satellite(spark, tmp_path) == before
    assert result.status == "error"
    assert _refused_as_existing(result, "vault/customer_crm_s"), result.error_details


def test_a_satellite_first_load_still_creates_the_table(spark, tmp_path):
    result = _satellite_load(spark, tmp_path, [("1", "Ada", "Oslo", None), ("2", "Grace", "Rome", None)])

    assert result.status == "success", result.error_details
    assert result.metrics["inserted_count"] == 2
    assert len(_satellite(spark, tmp_path)) == 2
