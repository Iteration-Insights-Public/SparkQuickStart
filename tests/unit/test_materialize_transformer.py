"""Tests for MaterializeTransformer (the state.df equivalent of MaterializeFrameTransformer)."""
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
from spark_quick_start.orchestration.transformers.materialize import MaterializeTransformer
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


def test_materialize_transformer_is_process_transformer():
    assert issubclass(MaterializeTransformer, ProcessTransformer)


def test_default_scratch_path_derived_from_staging_table_path():
    materialized_df = MagicMock()
    adapter = MockPlatformAdapter(delta_read_result=materialized_df)
    original_df = MagicMock()

    t = MaterializeTransformer()
    t._spark = MagicMock()
    t._adapter = adapter

    state = PipelineState(df=original_df, frames={})
    result = t(state, _ctx(staging_table_path="/staging/Entity"))

    expected_path = "/staging/Entity_ent_1_0_materialize"
    assert adapter.write_delta_overwrite_calls == [(expected_path, False)]
    assert adapter.read_delta_calls == [expected_path]
    assert adapter.drop_delta_calls == [expected_path]
    assert result.df is materialized_df
    only_partition_probe(original_df)


def test_the_scratch_path_cannot_be_passed():
    with pytest.raises(TypeError):
        MaterializeTransformer(scratch_path="/custom/scratch/path")  # type: ignore[call-arg]


def test_two_loads_sharing_a_staging_path_get_different_scratch_tables():
    """`id` is what keeps them apart (F129)."""
    paths = []
    for load_id in ("customer_a", "customer_b"):
        ctx = _ctx(staging_table_path="/staging/shared")
        ctx.global_ctx.id = load_id
        t = MaterializeTransformer()
        t._spark = MagicMock()
        t._adapter = MockPlatformAdapter(delta_read_result=MagicMock())
        frame = MagicMock()
        t(PipelineState(df=frame, frames={}), ctx)
        paths.append(t._adapter.write_delta_overwrite_calls[0][0])
        only_partition_probe(frame)
    assert paths == ["/staging/shared_customer_a_0_materialize", "/staging/shared_customer_b_0_materialize"]


def test_ids_differing_only_in_case_give_the_same_scratch_name():
    """Unity Catalog and Hive names ignore case, so `Cust_A` and `cust_a` must collide on purpose."""
    from spark_quick_start.orchestration.transformers.materialize import scratch_table_name

    names = []
    for load_id in ("Cust_A", "cust_a"):
        ctx = _ctx(staging_table_path="/staging/shared")
        ctx.global_ctx.id = load_id
        names.append(scratch_table_name(ctx, 0, "materialize"))
    assert names == ["/staging/shared_cust_a_0_materialize"] * 2


def test_a_context_without_an_id_is_refused_rather_than_named_without_it():
    ctx = _ctx()
    ctx.global_ctx.id = ""
    t = MaterializeTransformer()
    t._spark = MagicMock()
    t._adapter = MockPlatformAdapter(delta_read_result=MagicMock())
    with pytest.raises(ValueError, match="global_context.id"):
        t._run(PipelineState(df=MagicMock(), frames={}), ctx)


def test_frames_unchanged():
    adapter = MockPlatformAdapter(delta_read_result=MagicMock())
    target_frame = MagicMock()

    t = MaterializeTransformer()
    t._spark = MagicMock()
    t._adapter = adapter

    frame = MagicMock()
    state = PipelineState(df=frame, frames={"target": target_frame})
    result = t(state, _ctx())

    assert result.frames["target"] is target_frame
    assert not target_frame.method_calls
    only_partition_probe(frame)


def test_scratch_table_is_dropped_before_it_is_written():
    """Drop, then write, then read — in that order.

    Overwriting in place leaves every superseded file on disk until VACUUM, so
    the scratch path grows by a full copy per run. Dropping first reclaims the
    data, the log, and the old schema.
    """
    adapter = MockPlatformAdapter(delta_read_result=MagicMock())
    t = MaterializeTransformer()
    t._spark = MagicMock()
    t._adapter = adapter

    frame = MagicMock()
    t(PipelineState(df=frame, frames={}), _ctx(staging_table_path="/staging/E"))

    assert adapter.drop_delta_calls == ["/staging/E_ent_1_0_materialize"]
    assert adapter.write_delta_overwrite_calls == [("/staging/E_ent_1_0_materialize", False)]
    only_partition_probe(frame)


def test_two_instances_in_one_pipeline_use_different_scratch_tables():
    """The bug this guards is silent data loss, not a crash.

    Both instances used to derive one path, so the second wrote to the table
    its own input was reading. Under plain overwrite that survived (the old
    files stayed on disk, unreferenced); combined with dropping it yields an
    EMPTY table with no exception, which CandidateGuardTransformer then reports
    as zero candidates and the pipeline halts as a clean success.
    """
    ctx = _ctx(staging_table_path="/staging/E")

    first, second = MaterializeTransformer(), MaterializeTransformer()
    frames = []
    for i, t in enumerate((first, second), start=3):   # positions in a real pipeline
        t._spark = MagicMock()
        t._adapter = MockPlatformAdapter(delta_read_result=MagicMock())
        t._stage_index = i
        frames.append(MagicMock())
        t(PipelineState(df=frames[-1], frames={}), ctx)

    first_path = first._adapter.write_delta_overwrite_calls[0][0]
    second_path = second._adapter.write_delta_overwrite_calls[0][0]
    assert first_path == "/staging/E_ent_1_3_materialize"
    assert second_path == "/staging/E_ent_1_4_materialize"
    assert first_path != second_path
    for frame in frames:
        only_partition_probe(frame)


def test_orchestrator_injects_distinct_stage_indices():
    """End-to-end: two MaterializeTransformers added to one LoadOrchestrator
    must come out with different scratch paths. The uniqueness comes from the
    runner's injection, so testing the transformers in isolation is not enough.
    """
    from spark_quick_start.orchestration.transformers.base import SourceTransformer

    class _Source(SourceTransformer):
        def _produce(self, ctx):
            df = MagicMock()
            df.columns = ["id", "scdDeletedFlag"]
            return PipelineState(df=df)

    first, second = MaterializeTransformer(), MaterializeTransformer()
    transformers = [_Source(), first, second]
    for i, t in enumerate(transformers):
        t._stage_index = i          # what LoadOrchestrator.run() does

    assert first._stage_index != second._stage_index


def test_raises_clearly_when_used_outside_a_pipeline_run():
    """spark/adapter are injected by LoadOrchestrator.run(); using a Materialize
    transformer standalone must fail with a diagnostic, not an AttributeError on
    None. _TransformerBase declares them Optional (see issue #34), so this guard
    is also what keeps the call type-safe."""
    frame = MagicMock()
    t = MaterializeTransformer()
    # _spark / _adapter deliberately left at their class-level None

    with pytest.raises(RuntimeError, match="before spark/adapter were injected"):
        t(PipelineState(df=frame, frames={}), _ctx())
    only_partition_probe(frame)


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
    t = MaterializeTransformer()
    t._spark, t._adapter = spark, adapter
    result = t(PipelineState(df=df, frames={}), _strategy_ctx("cache"))
    out = result.df
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
    t = MaterializeTransformer()
    t._spark, t._adapter = spark, MockPlatformAdapter()

    def _boom(self):
        raise RuntimeError("fill failed")

    monkeypatch.setattr(DataFrame, "count", _boom)
    with pytest.raises(RuntimeError, match="fill failed"):
        t(PipelineState(df=df, frames={}), _strategy_ctx("cache"))
    assert not df.is_cached


def test_cache_strategy_on_serverless_falls_back_to_the_delta_round_trip():
    materialized = MagicMock()
    adapter = MockPlatformAdapter(delta_read_result=materialized)
    logger = MagicMock()
    df = MagicMock()
    t = MaterializeTransformer()
    t._spark, t._adapter, t._logger = MagicMock(), adapter, logger
    result = t(PipelineState(df=df, frames={}), _strategy_ctx("cache", serverless=True))
    assert result.df is materialized
    assert len(adapter.write_delta_overwrite_calls) == 1
    df.persist.assert_not_called()
    msgs = [c.args[0] for c in logger.info.call_args_list]
    assert sum("materialize_strategy" in m and "serverless" in m for m in msgs) == 1
