"""KeyMergeTransformer as a step, on MockPlatformAdapter: its refusals, its counts, and the
paths of execute_key_merge, with the Delta history reads stubbed (F223 review)."""
from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from spark_quick_start.adapters import MockPlatformAdapter
from spark_quick_start.orchestration import KeyMergeTransformer, PipelineState, load_pipeline
from spark_quick_start.orchestration.context_manager import LoadContextManager
from spark_quick_start.orchestration.transformers import LoadContext, key_merge

_T = datetime(2026, 1, 1, 10)


def _ctx(scd_type: str = "landing", hwm: bool = True) -> LoadContext:
    data = {
        "global_context": {"id": "e", "entity_name": "Customer", "source_system": "BC",
                           "target_table_path": "/lake/raw/t", "staging_table_path": "/unused",
                           "load_timestamp": "2026-11-13T10:00:00Z"},
        "scd_context": {"business_key_column": "id", **({"scd_type": scd_type} if scd_type else {})},
        "runtime_context": {"correlation_id": "c", "pipeline_name": "p", "application_name": "a"},
        "logging_context": {"logging_level": "INFO", "log_file_name": "l", "log_file_dir": "/tmp/logs"},
        "source_context": {"kind": "test"},
        **({"hwm_context": {"hwm_column_name": "modified_at"}} if hwm else {}),
    }
    m = LoadContextManager(json.dumps(data)).parse("", "")
    return LoadContext(global_ctx=m.global_ctx, staging_ctx=m.staging_ctx, scd_ctx=m.scd_ctx,
                       runtime_ctx=m.runtime_ctx, logging_ctx=m.logging_ctx, hwm_ctx=m.hwm_ctx,
                       source_ctx=m.source_ctx)


def _step(adapter=None) -> KeyMergeTransformer:
    step = KeyMergeTransformer()
    step._spark = object()  # type: ignore[assignment]
    step._adapter = adapter or MockPlatformAdapter()
    return step


def test_the_counts_are_none_until_it_runs():
    step = KeyMergeTransformer()
    assert (step.inserted, step.updated) == (None, None)


def test_it_refuses_another_style_inside_add_if():
    with pytest.raises(ValueError, match="landing"):
        _step()._run(PipelineState(df=None), _ctx(scd_type=""))  # type: ignore[arg-type]


def test_it_refuses_a_load_without_hwm_context():
    with pytest.raises(ValueError, match="hwm_context"):
        _step()._run(PipelineState(df=None), _ctx(hwm=False))  # type: ignore[arg-type]


def test_it_passes_the_context_to_the_merge_and_reports_its_counts(monkeypatch):
    seen = {}

    def fake(spark, adapter, rows, target, **kwargs):
        seen.update(target=target, **kwargs)
        return 3, 1

    monkeypatch.setattr(key_merge, "execute_key_merge", fake)
    step = KeyMergeTransformer(check_keys=False)
    step._spark, step._adapter = object(), MockPlatformAdapter()  # type: ignore[assignment]
    state = PipelineState(df=None)  # type: ignore[arg-type]
    assert step._run(state, _ctx()) is state
    assert (step.inserted, step.updated) == (3, 1)
    assert seen["target"] == "/lake/raw/t"
    assert (seen["key"], seen["ordering"], seen["load_ts_column"], seen["check_nulls"]) == (
        "id", "modified_at", "LoadTS", False)
    assert seen["load_ts"] == datetime(2026, 11, 13, 10, tzinfo=UTC)


class _Log:
    def info(self, *_a, **_k): ...
    def warning(self, *_a, **_k): ...


def _merge(spark, adapter, rows):
    return load_pipeline.execute_key_merge(spark, adapter, rows, "/lake/raw/t", key="id", ordering="modified_at",
                                           load_ts_column="LoadTS", load_ts=datetime(2026, 11, 13, tzinfo=UTC),
                                           logger=_Log())


def test_a_first_load_creates_the_table(spark, monkeypatch):
    monkeypatch.setattr(load_pipeline, "capture_write_statistics", lambda *_a: 2)
    adapter = MockPlatformAdapter()
    rows = spark.createDataFrame([("a", _T), ("b", _T)], "id string, modified_at timestamp")
    assert _merge(spark, adapter, rows) == (2, 0)
    assert adapter.write_delta_create_calls == ["/lake/raw/t"]


def test_an_empty_first_load_creates_nothing(spark):
    adapter = MockPlatformAdapter()
    assert _merge(spark, adapter, spark.createDataFrame([], "id string, modified_at timestamp")) == (0, 0)
    assert adapter.write_delta_create_calls == []


def test_a_new_column_is_added_before_the_merge(spark, monkeypatch):
    monkeypatch.setattr(load_pipeline, "capture_merge_statistics", lambda *_a: (1, 2, 0))
    table = spark.createDataFrame([], "id string, modified_at timestamp, LoadTS timestamp")
    adapter = MockPlatformAdapter(existing_tables={"/lake/raw/t"}, delta_read_result=table)
    rows = spark.createDataFrame([("a", _T, "x@y")], "id string, modified_at timestamp, email string")
    assert _merge(spark, adapter, rows) == (1, 2)
    assert adapter.add_delta_columns_calls == [("/lake/raw/t", ["email"])]


def test_a_retyped_column_is_refused_before_anything_changes(spark):
    table = spark.createDataFrame([], "id string, modified_at timestamp, name int, LoadTS timestamp")
    adapter = MockPlatformAdapter(existing_tables={"/lake/raw/t"}, delta_read_result=table)
    rows = spark.createDataFrame([("a", _T, "x", "y")], "id string, modified_at timestamp, name string, email string")
    with pytest.raises(ValueError, match="name: the table has int, the batch string"):
        _merge(spark, adapter, rows)
    assert adapter.add_delta_columns_calls == [] and adapter.resolve_delta_table_calls == []
