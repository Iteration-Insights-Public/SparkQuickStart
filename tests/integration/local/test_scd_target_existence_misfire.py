"""An existence check that wrongly answers "absent" must not empty an SCD target (F209).

`delta_table_exists` answers False on any error. The SCD merge used to create a target it
believed absent by overwriting it with an empty frame, which on a table that does exist
replaces the current version; the MERGE then inserted only the batch. Against a real local
Delta target. Requires a working Delta install.
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


class _BlindAdapter(LocalAdapter):
    """A LocalAdapter whose existence check answers False for the target, as a swallowed error does.

    `blind_calls` is how many of the target's checks answer False: None for every one, 1 for the
    first only (the target step's), the more likely transient case.
    """

    blind_to: str | None = None
    blind_calls: int | None = None

    def delta_table_exists(self, spark, identifier):
        if identifier == self.blind_to and (self.blind_calls is None or self.blind_calls > 0):
            if self.blind_calls is not None:
                self.blind_calls -= 1
            return False
        return super().delta_table_exists(spark, identifier)


def _context(root, scd_type):
    return json.dumps({
        "global_context": {
            "id": "customer",
            "entity_name": "Customer", "source_system": "CRM",
            "target_table_path": f"{root}/silver/customer",
            "staging_table_path": f"{root}/staging/customer",
            "staging_table_schema": {"type": "struct", "fields": [
                {"name": "customer_id", "type": "integer", "nullable": True, "metadata": {}},
                {"name": "name", "type": "string", "nullable": True, "metadata": {}}]},
        },
        "staging_context": {"stages": [{"nb_name": "NB_Stage_Customer"}]},
        "scd_context": {"business_key_column": "customer_id", "load_type": "Incremental",
                        "scd_type": scd_type},
        "runtime_context": {"correlation_id": "run", "pipeline_name": "customer", "application_name": "crm"},
        "logging_context": {"logging_level": "INFO", "log_file_name": "customer", "log_file_dir": f"{root}/logs"},
    })


@pytest.fixture
def load(spark, tmp_path):
    """load(rows, scd_type, blind=False, blind_calls=None) -> ExecutionResult; rows are (customer_id, name)."""
    staged = {"rows": []}

    def stage(storage_path, staging_context, global_context, notebook_name):
        return spark.createDataFrame(staged["rows"], "customer_id int, name string")

    main = create_staging_main_function(stage, adapter=LocalAdapter(), spark=spark)
    adapter = _BlindAdapter(notebook_registry={"NB_Stage_Customer": lambda a: main(a["stage_context_str"]).to_json()})

    def _load(rows, scd_type, blind=False, blind_calls=None):
        staged["rows"] = rows
        adapter.blind_to = f"{tmp_path}/silver/customer" if blind else None
        adapter.blind_calls = blind_calls
        return default_pipeline(_context(tmp_path, scd_type), spark, adapter=adapter).run()

    return _load


def _keys(spark, root):
    return sorted(r["customer_id"] for r in spark.read.format("delta").load(f"{root}/silver/customer").collect())


def _snapshot(spark, root):
    return sorted(tuple(r) for r in spark.read.format("delta").load(f"{root}/silver/customer")
                  .select("customer_id", "name", "scdVersion", "scdCurrentFlag", "scdHash").collect())


def _refused_as_existing(result):
    message = result.error_details["error_message"].lower()
    return "silver/customer" in message and "exist" in message


@pytest.mark.parametrize("scd_type", ["scd1", "scd2"])
def test_a_false_absent_answer_fails_the_load_and_leaves_the_target_whole(spark, tmp_path, load, scd_type):
    assert load([(1, "Ada"), (2, "Grace"), (3, "Linus")], scd_type).status == "success"

    result = load([(1, "Ada L.")], scd_type, blind=True)

    assert _keys(spark, tmp_path) == [1, 2, 3]
    assert result.status == "error"
    assert _refused_as_existing(result), result.error_details


@pytest.mark.parametrize("scd_type", ["scd1", "scd2"])
def test_a_false_absent_answer_to_the_target_step_alone_changes_nothing(spark, tmp_path, load, scd_type):
    # Only the target step's check misfires; a second check in the merge would find the table,
    # and merging candidates classified as a first load into it duplicates or resets rows.
    assert load([(1, "Ada"), (2, "Grace")], scd_type).status == "success"
    assert load([(1, "Ada L."), (2, "Grace")], scd_type).status == "success"
    before = _snapshot(spark, tmp_path)

    result = load([(1, "Ada M."), (2, "Grace")], scd_type, blind=True, blind_calls=1)

    assert _snapshot(spark, tmp_path) == before
    assert result.status == "error"
    assert _refused_as_existing(result), result.error_details


@pytest.mark.parametrize("scd_type", ["scd1", "scd2"])
def test_a_first_load_still_creates_the_target(spark, tmp_path, load, scd_type):
    result = load([(1, "Ada"), (2, "Grace")], scd_type)

    assert result.status == "success", result.error_details
    assert _keys(spark, tmp_path) == [1, 2]
    assert result.metrics["inserted_count"] == 2
