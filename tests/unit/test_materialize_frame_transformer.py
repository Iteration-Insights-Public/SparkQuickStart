"""Tests for MaterializeFrameTransformer."""
from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

import pytest
from _frames import only_partition_probe

# Stub delta so import succeeds without a real Spark session
if "delta" not in sys.modules:
    _d = types.ModuleType("delta")
    _dt = types.ModuleType("delta.tables")
    class _MockDeltaTable:
        @staticmethod
        def isDeltaTable(s, p): return False
        @staticmethod
        def forPath(s, p): return MagicMock()
    _dt.DeltaTable = _MockDeltaTable
    _d.tables = _dt
    sys.modules["delta"] = _d
    sys.modules["delta.tables"] = _dt

from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter
from spark_quick_start.orchestration.models import (
    GlobalContext,
    LoggingContext,
    RuntimeContext,
    SCDContext,
    StagingContext,
)
from spark_quick_start.orchestration.transformers.base import LoadContext, ProcessTransformer
from spark_quick_start.orchestration.transformers.materialize import MaterializeFrameTransformer
from spark_quick_start.orchestration.transformers.state import PipelineState


def _ctx(staging_table_path: str = "/staging/Entity") -> LoadContext:
    return LoadContext(
        global_ctx=GlobalContext({"id": "ent_1", "entity_name": "E", "source_system": "S",
                                   "target_table_path": "/t",
                                   "staging_table_path": staging_table_path}),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext({"business_key_column": "id"}),
        runtime_ctx=RuntimeContext({"correlation_id": "c1", "pipeline_name": "p", "application_name": "a"}),
        logging_ctx=LoggingContext({}),
    )


def test_materialize_frame_transformer_is_process_transformer():
    assert issubclass(MaterializeFrameTransformer, ProcessTransformer)


def test_an_absent_target_frame_passes_through_unchanged():
    adapter = MockPlatformAdapter()
    t = MaterializeFrameTransformer("target")
    t._spark = MagicMock()
    t._adapter = adapter

    frame = MagicMock()
    state = PipelineState(df=frame, target_exists=False)
    result = t(state, _ctx())

    assert "target" not in result.frames
    assert result.target_exists is False
    assert adapter.write_delta_overwrite_calls == []
    assert adapter.read_delta_calls == []
    only_partition_probe(frame)


def test_default_frame_key_derives_scratch_path_from_staging_table_path():
    materialized_df = MagicMock()
    adapter = MockPlatformAdapter(delta_read_result=materialized_df)
    original_df = MagicMock()

    t = MaterializeFrameTransformer()  # default frame_key="target"
    t._spark = MagicMock()
    t._adapter = adapter

    frame = MagicMock()
    state = PipelineState(df=frame, frames={"target": original_df})
    result = t(state, _ctx(staging_table_path="/staging/Entity"))

    expected_path = "/staging/Entity_ent_1_0_materialize_target"
    assert adapter.write_delta_overwrite_calls == [(expected_path, False)]
    assert adapter.drop_delta_calls == [expected_path]
    assert adapter.read_delta_calls == [expected_path]
    assert result.frames["target"] is materialized_df
    assert not original_df.method_calls
    only_partition_probe(frame)


def test_explicit_frame_key_used_in_scratch_path():
    materialized_df = MagicMock()
    adapter = MockPlatformAdapter(delta_read_result=materialized_df)

    t = MaterializeFrameTransformer("candidates")
    t._spark = MagicMock()
    t._adapter = adapter

    frame = MagicMock()
    candidates = MagicMock()
    state = PipelineState(df=frame, frames={"candidates": candidates})
    result = t(state, _ctx(staging_table_path="/staging/Entity"))

    expected_path = "/staging/Entity_ent_1_0_materialize_candidates"
    assert adapter.write_delta_overwrite_calls == [(expected_path, False)]
    assert adapter.read_delta_calls == [expected_path]
    assert result.frames["candidates"] is materialized_df
    assert not candidates.method_calls
    only_partition_probe(frame)


def test_the_scratch_path_cannot_be_passed():
    with pytest.raises(TypeError):
        MaterializeFrameTransformer("target", scratch_path="/custom/scratch/path")  # type: ignore[call-arg]


def test_primary_df_unchanged():
    adapter = MockPlatformAdapter(delta_read_result=MagicMock())
    staged_df = MagicMock()

    t = MaterializeFrameTransformer("target")
    t._spark = MagicMock()
    t._adapter = adapter

    target = MagicMock()
    state = PipelineState(df=staged_df, frames={"target": target})
    result = t(state, _ctx())

    assert result.df is staged_df
    assert not target.method_calls
    only_partition_probe(staged_df)


# ── materialize_strategy ─────────────────────────────────────────────────────

def _strategy_ctx(strategy: str, *, serverless: bool = False) -> LoadContext:
    base = _ctx()
    return LoadContext(
        global_ctx=base.global_ctx, staging_ctx=base.staging_ctx, scd_ctx=base.scd_ctx,
        runtime_ctx=RuntimeContext({
            "correlation_id": "c1", "pipeline_name": "p", "application_name": "a",
            "materialize_strategy": strategy, "serverless": serverless,
        }),
        logging_ctx=base.logging_ctx,
    )


def test_cache_strategy_persists_and_writes_no_scratch_table(spark):
    df = spark.createDataFrame([(1,), (2,)], "k int")
    adapter = MockPlatformAdapter()
    t = MaterializeFrameTransformer()
    t._spark, t._adapter = spark, adapter
    result = t(PipelineState(df=MagicMock(), frames={"target": df}), _strategy_ctx("cache"))
    out = result.frames["target"]
    try:
        assert out.is_cached
        assert any(c is out for c in result.cached)
        assert adapter.write_delta_overwrite_calls == []
        assert adapter.read_delta_calls == []
        assert adapter.drop_delta_calls == []
    finally:
        out.unpersist()


def test_cache_strategy_unpersists_and_reraises_when_the_fill_fails(spark, monkeypatch):
    from pyspark.sql import DataFrame

    df = spark.createDataFrame([(1,)], "k int")
    t = MaterializeFrameTransformer()
    t._spark, t._adapter = spark, MockPlatformAdapter()

    def _boom(self):
        raise RuntimeError("fill failed")

    monkeypatch.setattr(DataFrame, "count", _boom)
    with pytest.raises(RuntimeError, match="fill failed"):
        t(PipelineState(df=MagicMock(), frames={"target": df}), _strategy_ctx("cache"))
    assert not df.is_cached


def test_cache_strategy_on_serverless_falls_back_to_the_delta_round_trip():
    materialized = MagicMock()
    adapter = MockPlatformAdapter(delta_read_result=materialized)
    logger = MagicMock()
    df = MagicMock()
    t = MaterializeFrameTransformer()
    t._spark, t._adapter, t._logger = MagicMock(), adapter, logger
    result = t(PipelineState(df=MagicMock(), frames={"target": df}), _strategy_ctx("cache", serverless=True))
    assert result.frames["target"] is materialized
    assert len(adapter.write_delta_overwrite_calls) == 1
    df.persist.assert_not_called()
    msgs = [c.args[0] for c in logger.info.call_args_list]
    assert sum("materialize_strategy" in m and "serverless" in m for m in msgs) == 1
