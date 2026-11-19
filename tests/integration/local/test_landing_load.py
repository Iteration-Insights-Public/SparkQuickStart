"""F223 end to end: a landing load from a source step of the user's own, on local Delta.

`scd_type: landing`, `KeyMergeTransformer`, a `source_context` and an `hwm_context`; the latest
state per key, the load time stamped, new columns added, and the refusals before any write.
Requires a working Delta install.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from spark_quick_start.adapters.local_adapter import LocalAdapter
from spark_quick_start.orchestration import KeyMergeTransformer, LoadOrchestrator, PipelineState
from spark_quick_start.orchestration.transformers import SourceTransformer


@pytest.fixture(autouse=True)
def _require_working_delta(spark, tmp_path_factory):
    """Skip unless this session can round-trip a Delta table; see test_dqd_load."""
    pytest.importorskip("delta", reason="delta-spark not installed")
    probe = str(tmp_path_factory.mktemp("delta_probe") / "t")
    try:
        spark.range(1).write.format("delta").mode("overwrite").save(probe)
    except Exception as exc:  # pragma: no cover — environment-dependent
        pytest.skip(f"Delta not usable in this Spark session: {type(exc).__name__}")


_T = [datetime(2026, 1, 1, h) for h in range(24)]
_SCHEMA = "id string, modified_at timestamp, name string"


class _Batch(SourceTransformer):
    """A source of the user's own that yields whatever batch the test set."""

    source_kind = "test"
    batch = None

    def _produce(self, ctx):
        return PipelineState(df=type(self).batch)


def _context(root, load_timestamp, **scd):
    return json.dumps({
        "global_context": {"id": "ent_1", "entity_name": "Customer", "source_system": "BC_Contoso",
                           "target_table_path": f"{root}/raw/bc_contoso_customer",
                           "staging_table_path": f"{root}/unused", "load_timestamp": load_timestamp},
        "scd_context": {"scd_type": "landing", "business_key_column": "id", **scd},
        "runtime_context": {"correlation_id": "c", "pipeline_name": "p", "application_name": "a"},
        "logging_context": {"logging_level": "INFO", "log_file_name": "l", "log_file_dir": f"{root}/logs"},
        "hwm_context": {"hwm_column_name": "modified_at"},
        "source_context": {"kind": "test"},
    })


def _land(spark, root, rows, load_timestamp="2026-11-13T10:00:00Z", schema=_SCHEMA, **scd):
    _Batch.batch = spark.createDataFrame(rows, schema)
    return LoadOrchestrator(_context(root, load_timestamp, **scd), spark, adapter=LocalAdapter()) \
        .add(_Batch()).add(KeyMergeTransformer()).run()


def _table(spark, root):
    return spark.read.format("delta").load(f"{root}/raw/bc_contoso_customer")


def _state(spark, root):
    return {r.id: r for r in _table(spark, root).collect()}


def test_a_first_load_creates_the_table_with_the_load_time(spark, tmp_path):
    result = _land(spark, tmp_path, [("a", _T[1], "A"), ("b", _T[1], "B"), ("c", _T[1], "C")])
    assert result.status == "success", result.error_details
    assert (result.metrics["inserted_count"], result.metrics["updated_count"]) == (3, 0)
    assert [s.stage for s in result.stages] == ["_Batch", "KeyMergeTransformer"]
    table = _table(spark, tmp_path)
    assert table.columns == ["id", "modified_at", "name", "LoadTS"]
    # Spark returns a naive timestamp in the driver's local time; read it back as an instant.
    assert {r.LoadTS.astimezone(UTC) for r in table.collect()} == {datetime(2026, 11, 13, 10, tzinfo=UTC)}


def test_later_batches_insert_update_and_never_regress(spark, tmp_path):
    _land(spark, tmp_path, [("a", _T[1], "A"), ("b", _T[1], "B"), ("c", _T[1], "C")])
    second = [("d", _T[2], "D"), ("a", _T[2], "A2"), ("b", _T[1], "B-same-time")]
    result = _land(spark, tmp_path, second, load_timestamp="2026-11-13T11:00:00Z")
    assert result.status == "success", result.error_details
    assert (result.metrics["inserted_count"], result.metrics["updated_count"]) == (1, 1)
    state = _state(spark, tmp_path)
    assert state["a"].name == "A2" and state["b"].name == "B"
    assert state["b"].LoadTS != state["a"].LoadTS

    replay = _land(spark, tmp_path, second, load_timestamp="2026-11-13T12:00:00Z")
    assert (replay.metrics["inserted_count"], replay.metrics["updated_count"]) == (0, 0)

    older = _land(spark, tmp_path, [("a", _T[0], "A-old")], load_timestamp="2026-11-13T13:00:00Z")
    assert older.status == "success" and _state(spark, tmp_path)["a"].name == "A2"


def test_a_batch_with_a_new_column_adds_it(spark, tmp_path):
    _land(spark, tmp_path, [("a", _T[1], "A")])
    result = _land(spark, tmp_path, [("b", _T[2], "B", "x@y")], schema=_SCHEMA + ", email string")
    assert result.status == "success", result.error_details
    state = _state(spark, tmp_path)
    assert state["a"].email is None and state["b"].email == "x@y"


@pytest.mark.parametrize("ddl", ["name int", "id string, modified_at timestamp, name bigint"])
def test_a_retyped_column_is_refused_and_nothing_is_written(spark, tmp_path, ddl):
    _land(spark, tmp_path, [("a", _T[1], 1)], schema="id string, modified_at timestamp, name int")
    before = _table(spark, tmp_path).collect()
    retyped = "id string, modified_at timestamp, name bigint" if ddl.startswith("id") else _SCHEMA
    rows = [("b", _T[2], 2)] if ddl.startswith("id") else [("b", _T[2], "two")]
    result = _land(spark, tmp_path, rows, schema=retyped)
    assert result.status == "error"
    message = result.error_details["error_message"]
    assert "name" in message and "int" in message
    assert _table(spark, tmp_path).collect() == before


def test_an_empty_first_batch_creates_no_table(spark, tmp_path):
    result = _land(spark, tmp_path, [])
    assert result.status == "success", result.error_details
    assert (result.metrics["inserted_count"], result.metrics["updated_count"]) == (0, 0)
    assert not (tmp_path / "raw" / "bc_contoso_customer").exists()


def test_the_counts_come_from_the_commit(spark, tmp_path):
    from delta.tables import DeltaTable

    _land(spark, tmp_path, [("a", _T[1], "A")])
    result = _land(spark, tmp_path, [("a", _T[2], "A2"), ("b", _T[2], "B")])
    metrics = DeltaTable.forPath(spark, f"{tmp_path}/raw/bc_contoso_customer").history(1).collect()[0].operationMetrics
    assert (result.metrics["inserted_count"], result.metrics["updated_count"]) == \
        (int(metrics["numTargetRowsInserted"]), int(metrics["numTargetRowsUpdated"]))


def test_the_writer_refuses_an_scd_style_before_the_source_runs(spark, tmp_path):
    _Batch.batch = None
    data = json.loads(_context(tmp_path, "2026-11-13T10:00:00Z"))
    data["scd_context"] = {"business_key_column": "id"}
    result = LoadOrchestrator(json.dumps(data), spark, adapter=LocalAdapter()).add(_Batch()).add(KeyMergeTransformer()).run()
    assert result.status == "error" and "landing" in result.error_details["error_message"]


def test_nested_columns_load_again_unchanged(spark, tmp_path):
    """check-adversary 2026-11-13: Delta stores nested fields as nullable; that is not a retype."""
    schema = "id string, modified_at timestamp, s struct<x:int>, arr array<int>"
    first = _land(spark, tmp_path, [("a", _T[1], (1,), [1])], schema=schema)
    again = _land(spark, tmp_path, [("a", _T[2], (2,), [2])], schema=schema)
    assert first.status == "success" and again.status == "success", again.error_details
    assert _state(spark, tmp_path)["a"].s.x == 2


def test_a_batch_carrying_the_load_time_column_in_another_case_is_refused(spark, tmp_path):
    """check-adversary 2026-11-13: the stamp would overwrite a column that differs only by case."""
    result = _land(spark, tmp_path, [("a", _T[1], "A", "mine")], schema=_SCHEMA + ", loadts string")
    assert result.status == "error" and "LoadTS" in result.error_details["error_message"]
