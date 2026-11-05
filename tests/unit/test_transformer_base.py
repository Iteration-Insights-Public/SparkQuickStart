# tests/test_transformer_base.py
from __future__ import annotations

import dataclasses
import time
from unittest.mock import MagicMock, patch

import pytest
from _frames import only_partition_probe

from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter
from spark_quick_start.orchestration.models import (
    GlobalContext,
    LoggingContext,
    RuntimeContext,
    SCDContext,
    StagingContext,
)
from spark_quick_start.orchestration.transformers import base as base_module
from spark_quick_start.orchestration.transformers.base import (
    AlwaysRunTransformer,
    LoadContext,
    ProcessTransformer,
    SourceTransformer,
    _TransformerBase,
)
from spark_quick_start.orchestration.transformers.state import PipelineState


def _make_ctx():
    return LoadContext(
        global_ctx=GlobalContext({"id": "ent_1", "entity_name": "E", "source_system": "S",
                                   "target_table_path": "/t", "staging_table_path": "/s"}),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext({"business_key_column": "id"}),
        runtime_ctx=RuntimeContext({"correlation_id": "c1", "pipeline_name": "p1", "application_name": "a1"}),
        logging_ctx=LoggingContext({}),
    )


def test_source_transformer_is_instance_of_base():
    class MySource(SourceTransformer):
        def _produce(self, ctx) -> PipelineState:
            return None  # never called

    assert isinstance(MySource(), _TransformerBase)


def test_process_transformer_is_instance_of_base():
    class MyProcess(ProcessTransformer):
        def _run(self, state, ctx) -> PipelineState:
            return state

    assert isinstance(MyProcess(), _TransformerBase)


def test_source_transformer_lifecycle_order():
    calls = []

    frame = MagicMock()
    frame.columns = ["id", "scdDeletedFlag"]

    class MySource(SourceTransformer):
        def before_run(self, ctx, df=None) -> None:
            super().before_run(ctx, df=df)
            calls.append("before")

        def _produce(self, ctx) -> PipelineState:
            calls.append("produce")
            return PipelineState(df=frame)

        def after_run(self, ctx, df=None, **kwargs) -> None:
            super().after_run(ctx, df=df, **kwargs)
            calls.append("after")

    t = MySource()
    t.produce(_make_ctx())
    assert calls == ["before", "produce", "after"]
    only_partition_probe(frame)


def test_source_transformer_default_hooks_log_transformer_name():
    mock_logger = MagicMock()

    frame = MagicMock()
    frame.columns = ["id", "scdDeletedFlag"]

    class MySource(SourceTransformer):
        def _produce(self, ctx) -> PipelineState:
            return PipelineState(df=frame)

    t = MySource()
    t._logger = mock_logger
    t.produce(_make_ctx())

    calls = mock_logger.info.call_args_list
    assert any(c.kwargs.get("tags", {}).get("event") == "start" and c.kwargs.get("tags", {}).get("transformer") == "MySource" for c in calls)
    assert any(c.kwargs.get("tags", {}).get("event") == "end" and c.kwargs.get("tags", {}).get("transformer") == "MySource" for c in calls)
    only_partition_probe(frame)


def test_source_transformer_hooks_silent_without_logger():
    frame = MagicMock()
    frame.columns = ["id", "scdDeletedFlag"]

    class MySource(SourceTransformer):
        def _produce(self, ctx) -> PipelineState:
            return PipelineState(df=frame)

    t = MySource()
    result = t.produce(_make_ctx())
    assert isinstance(result, PipelineState)
    only_partition_probe(frame)


def test_load_context_is_frozen():
    ctx = _make_ctx()
    with pytest.raises(dataclasses.FrozenInstanceError):
        ctx.global_ctx = MagicMock()  # type: ignore


def test_source_transformer_is_abstract():
    with pytest.raises(TypeError):
        SourceTransformer()  # type: ignore


def test_process_transformer_is_abstract():
    with pytest.raises(TypeError):
        ProcessTransformer()  # type: ignore


def test_concrete_source_transformer_returns_pipeline_state():
    frame = MagicMock()
    frame.columns = ["id", "scdDeletedFlag"]

    class MySource(SourceTransformer):
        def _produce(self, ctx) -> PipelineState:
            return PipelineState(df=frame)

    t = MySource()
    t._spark = MagicMock()
    t._adapter = MockPlatformAdapter()
    result = t.produce(_make_ctx())
    assert isinstance(result, PipelineState)
    only_partition_probe(frame)


def test_source_transformer_raises_when_business_key_missing():
    class MySource(SourceTransformer):
        def _produce(self, ctx) -> PipelineState:
            mock_df = MagicMock()
            mock_df.columns = ["scdDeletedFlag", "name"]  # business key "id" missing
            return PipelineState(df=mock_df)

    t = MySource()
    t._spark = MagicMock()
    with pytest.raises(ValueError, match="id"):
        t.produce(_make_ctx())


def test_source_transformer_accepts_a_source_without_the_deleted_flag():
    """F127: the staged deleted flag is optional; the detector adds "N" when it is missing.
    This test replaced one that required the flag, which is the behaviour F127 removes."""
    frame = MagicMock()
    frame.columns = ["id", "name"]  # no scdDeletedFlag

    class MySource(SourceTransformer):
        def _produce(self, ctx) -> PipelineState:
            return PipelineState(df=frame)

    t = MySource()
    t._spark = MagicMock()
    assert t.produce(_make_ctx()).df is frame
    only_partition_probe(frame)


def test_source_transformer_raises_when_source_system_column_missing_and_required():
    from spark_quick_start.orchestration.models import (
        GlobalContext,
        LoggingContext,
        RuntimeContext,
        SCDContext,
        StagingContext,
    )
    ctx_with_flag = LoadContext(
        global_ctx=GlobalContext({"id": "ent_1", "entity_name": "E", "source_system": "S",
                                   "target_table_path": "/t", "staging_table_path": "/s"}),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext({"business_key_column": "id", "source_system_from_source": True}),
        runtime_ctx=RuntimeContext({"correlation_id": "c", "pipeline_name": "p", "application_name": "a"}),
        logging_ctx=LoggingContext({}),
    )

    class MySource(SourceTransformer):
        def _produce(self, ctx) -> PipelineState:
            mock_df = MagicMock()
            mock_df.columns = ["id", "scdDeletedFlag", "name"]  # scdSourceSystem missing
            return PipelineState(df=mock_df)

    t = MySource()
    t._spark = MagicMock()
    with pytest.raises(ValueError, match="scdSourceSystem"):
        t.produce(ctx_with_flag)


def test_source_transformer_passes_when_source_system_column_present_and_required():
    from spark_quick_start.orchestration.models import (
        GlobalContext,
        LoggingContext,
        RuntimeContext,
        SCDContext,
        StagingContext,
    )
    ctx_with_flag = LoadContext(
        global_ctx=GlobalContext({"id": "ent_1", "entity_name": "E", "source_system": "S",
                                   "target_table_path": "/t", "staging_table_path": "/s"}),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext({"business_key_column": "id", "source_system_from_source": True}),
        runtime_ctx=RuntimeContext({"correlation_id": "c", "pipeline_name": "p", "application_name": "a"}),
        logging_ctx=LoggingContext({}),
    )

    frame = MagicMock()
    frame.columns = ["id", "scdDeletedFlag", "scdSourceSystem", "name"]

    class MySource(SourceTransformer):
        def _produce(self, ctx) -> PipelineState:
            return PipelineState(df=frame)

    t = MySource()
    t._spark = MagicMock()
    result = t.produce(ctx_with_flag)
    assert isinstance(result, PipelineState)
    only_partition_probe(frame)


def test_source_transformer_no_error_when_source_system_missing_but_not_required():
    frame = MagicMock()
    frame.columns = ["id", "scdDeletedFlag", "name"]

    class MySource(SourceTransformer):
        def _produce(self, ctx) -> PipelineState:
            return PipelineState(df=frame)

    t = MySource()
    t._spark = MagicMock()
    result = t.produce(_make_ctx())
    assert isinstance(result, PipelineState)
    only_partition_probe(frame)


def test_concrete_process_transformer_returns_pipeline_state():
    out = MagicMock()
    original = MagicMock()

    class MyProcess(ProcessTransformer):
        def _run(self, state: PipelineState, ctx) -> PipelineState:
            return state.with_df(out)

    t = MyProcess()
    state = PipelineState(df=original)
    result = t(state, _make_ctx())
    assert isinstance(result, PipelineState)
    only_partition_probe(original)
    only_partition_probe(out)


def test_process_transformer_lifecycle_order():
    calls = []

    class MyProcess(ProcessTransformer):
        def before_run(self, state: PipelineState, ctx) -> None:
            super().before_run(state, ctx)
            calls.append("before")

        def _run(self, state: PipelineState, ctx) -> PipelineState:
            calls.append("run")
            return state

        def after_run(self, state: PipelineState, ctx) -> None:
            super().after_run(state, ctx)
            calls.append("after")

    t = MyProcess()
    frame = MagicMock()
    state = PipelineState(df=frame)
    t(state, _make_ctx())
    assert calls == ["before", "run", "after"]
    only_partition_probe(frame)


def test_process_transformer_default_hooks_log_transformer_name():
    mock_logger = MagicMock()

    class MyProcess(ProcessTransformer):
        def _run(self, state: PipelineState, ctx) -> PipelineState:
            return state

    t = MyProcess()
    t._logger = mock_logger
    frame = MagicMock()
    state = PipelineState(df=frame)
    t(state, _make_ctx())

    calls = [str(c) for c in mock_logger.info.call_args_list]
    assert any("MyProcess" in c and "start" in c for c in calls)
    assert any("MyProcess" in c and "end" in c for c in calls)
    only_partition_probe(frame)


def test_process_transformer_hooks_silent_without_logger():
    class MyProcess(ProcessTransformer):
        def _run(self, state: PipelineState, ctx) -> PipelineState:
            return state

    t = MyProcess()  # _logger is None by default
    frame = MagicMock()
    state = PipelineState(df=frame)
    result = t(state, _make_ctx())
    assert result is state
    only_partition_probe(frame)


def test_process_transformer_after_run_receives_result_state():
    seen = {}

    out = MagicMock()

    class MyProcess(ProcessTransformer):
        def _run(self, state: PipelineState, ctx) -> PipelineState:
            return state.with_df(out)

        def after_run(self, state: PipelineState, ctx) -> None:
            seen["state"] = state

    t = MyProcess()
    frame = MagicMock()
    original = PipelineState(df=frame)
    result = t(original, _make_ctx())
    assert seen["state"] is result
    only_partition_probe(frame)
    only_partition_probe(out)


# ── Stage metrics: partitions, row counts, extra_metrics, collector ──────────


def _metric_value(calls, metric_name):
    """Extract metricValue for metric_name from the 'end' event's metrics list."""
    for c in calls:
        if c.kwargs.get("tags", {}).get("event") == "end":
            for m in c.kwargs.get("metrics", []):
                if m["metricName"] == metric_name:
                    return m["metricValue"]
    return None


def test_process_transformer_captures_partitions_in_and_out():
    mock_logger = MagicMock()
    df_in = MagicMock()
    df_in.rdd.getNumPartitions.return_value = 3
    df_out = MagicMock()
    df_out.rdd.getNumPartitions.return_value = 7

    class MyProcess(ProcessTransformer):
        def _run(self, state: PipelineState, ctx) -> PipelineState:
            return state.with_df(df_out)

    t = MyProcess()
    t._logger = mock_logger
    t(PipelineState(df=df_in), _make_ctx())

    calls = mock_logger.info.call_args_list
    assert _metric_value(calls, "partitions_in") == 3
    assert _metric_value(calls, "partitions_out") == 7


def test_source_transformer_captures_partitions_out():
    mock_logger = MagicMock()
    df_out = MagicMock()
    df_out.columns = ["id", "scdDeletedFlag"]
    df_out.rdd.getNumPartitions.return_value = 4

    class MySource(SourceTransformer):
        def _produce(self, ctx) -> PipelineState:
            return PipelineState(df=df_out)

    t = MySource()
    t._logger = mock_logger
    t.produce(_make_ctx())

    calls = mock_logger.info.call_args_list
    assert _metric_value(calls, "partitions_out") == 4
    assert _metric_value(calls, "partitions_in") is None


def test_row_counts_not_captured_by_default():
    mock_logger = MagicMock()
    df = MagicMock()
    df.count.return_value = 99

    class MyProcess(ProcessTransformer):
        def _run(self, state: PipelineState, ctx) -> PipelineState:
            return state

    t = MyProcess()
    t._logger = mock_logger
    t(PipelineState(df=df), _make_ctx())

    df.count.assert_not_called()
    calls = mock_logger.info.call_args_list
    assert _metric_value(calls, "row_count_in") is None
    assert _metric_value(calls, "row_count_out") is None


def test_row_counts_captured_when_enabled():
    ctx = LoadContext(
        global_ctx=GlobalContext({"id": "ent_1", "entity_name": "E", "source_system": "S",
                                   "target_table_path": "/t", "staging_table_path": "/s"}),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext({"business_key_column": "id"}),
        runtime_ctx=RuntimeContext({"correlation_id": "c1", "pipeline_name": "p1",
                                     "application_name": "a1", "capture_row_counts": True}),
        logging_ctx=LoggingContext({}),
    )
    mock_logger = MagicMock()
    df = MagicMock()
    df.count.return_value = 42

    class MyProcess(ProcessTransformer):
        def _run(self, state: PipelineState, ctx) -> PipelineState:
            return state

    t = MyProcess()
    t._logger = mock_logger
    t(PipelineState(df=df), ctx)

    calls = mock_logger.info.call_args_list
    assert _metric_value(calls, "row_count_in") == 42
    assert _metric_value(calls, "row_count_out") == 42


def test_plan_not_captured_by_default():
    from spark_quick_start.orchestration.metrics import StageMetricsCollector

    mock_logger = MagicMock()
    df = MagicMock()
    df._sc._jvm.PythonSQLUtils.explainString.return_value = "== Physical Plan ==\nScan"

    class MyProcess(ProcessTransformer):
        def _run(self, state: PipelineState, ctx) -> PipelineState:
            return state

    t = MyProcess()
    t._logger = mock_logger
    collector = StageMetricsCollector()
    t._metrics_collector = collector
    t(PipelineState(df=df), _make_ctx())

    df._sc._jvm.PythonSQLUtils.explainString.assert_not_called()
    assert collector.stages[0].plan is None
    debug_calls = [c for c in mock_logger.debug.call_args_list
                   if c.kwargs.get("tags", {}).get("event") == "execution_plan"]
    assert debug_calls == []


def test_plan_captured_when_enabled():
    from spark_quick_start.orchestration.metrics import StageMetricsCollector

    ctx = LoadContext(
        global_ctx=GlobalContext({"id": "ent_1", "entity_name": "E", "source_system": "S",
                                   "target_table_path": "/t", "staging_table_path": "/s"}),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext({"business_key_column": "id"}),
        runtime_ctx=RuntimeContext({"correlation_id": "c1", "pipeline_name": "p1",
                                     "application_name": "a1", "capture_plans": True}),
        logging_ctx=LoggingContext({}),
    )
    mock_logger = MagicMock()
    df = MagicMock()
    df._sc._jvm.PythonSQLUtils.explainString.return_value = "== Physical Plan ==\nScan"

    class MyProcess(ProcessTransformer):
        def _run(self, state: PipelineState, ctx) -> PipelineState:
            return state

    t = MyProcess()
    t._logger = mock_logger
    collector = StageMetricsCollector()
    t._metrics_collector = collector
    t(PipelineState(df=df), ctx)

    assert collector.stages[0].plan == "== Physical Plan ==\nScan"
    debug_calls = [c for c in mock_logger.debug.call_args_list
                   if c.kwargs.get("tags", {}).get("event") == "execution_plan"]
    assert len(debug_calls) == 1
    assert "== Physical Plan ==" in debug_calls[0].args[0]
    assert debug_calls[0].kwargs["tags"]["transformer"] == "MyProcess"


def test_plan_capture_falls_back_to_none_on_jvm_error():
    from spark_quick_start.orchestration.metrics import StageMetricsCollector

    ctx = LoadContext(
        global_ctx=GlobalContext({"id": "ent_1", "entity_name": "E", "source_system": "S",
                                   "target_table_path": "/t", "staging_table_path": "/s"}),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext({"business_key_column": "id"}),
        runtime_ctx=RuntimeContext({"correlation_id": "c1", "pipeline_name": "p1",
                                     "application_name": "a1", "capture_plans": True}),
        logging_ctx=LoggingContext({}),
    )
    df = MagicMock()
    df._sc._jvm.PythonSQLUtils.explainString.side_effect = Exception("boom")
    # Simulate the Spark Connect fallback also failing, so this exercises the
    # "both attempts failed" path rather than the Connect fallback trivially
    # succeeding on an unconfigured MagicMock.
    df._explain_string.side_effect = Exception("boom")

    class MyProcess(ProcessTransformer):
        def _run(self, state: PipelineState, ctx) -> PipelineState:
            return state

    t = MyProcess()
    mock_logger = MagicMock()
    t._logger = mock_logger
    collector = StageMetricsCollector()
    t._metrics_collector = collector
    t(PipelineState(df=df), ctx)  # must not raise

    assert collector.stages[0].plan is None
    failed_calls = [c for c in mock_logger.debug.call_args_list
                    if c.kwargs.get("tags", {}).get("event") == "execution_plan_failed"]
    assert len(failed_calls) == 1, "capture_plans=True + JVM failure must be distinguishable from disabled"


def test_plan_capture_failure_not_logged_when_disabled():
    """capture_plans=False must stay silent even though the same JVM call would fail —
    only an explicit opt-in should ever produce an execution_plan_failed log line."""
    mock_logger = MagicMock()
    df = MagicMock()
    df._sc._jvm.PythonSQLUtils.explainString.side_effect = Exception("boom")

    class MyProcess(ProcessTransformer):
        def _run(self, state: PipelineState, ctx) -> PipelineState:
            return state

    t = MyProcess()
    t._logger = mock_logger
    t(PipelineState(df=df), _make_ctx())  # capture_plans defaults to False

    failed_calls = [c for c in mock_logger.debug.call_args_list
                    if c.kwargs.get("tags", {}).get("event") == "execution_plan_failed"]
    assert failed_calls == []


def test_after_run_extra_metrics_appended_to_log():
    mock_logger = MagicMock()

    class MyProcess(ProcessTransformer):
        def _run(self, state: PipelineState, ctx) -> PipelineState:
            return state

        def after_run(self, state: PipelineState, ctx) -> None:
            super().after_run(state, ctx, extra_metrics={"custom_metric": 123})

    t = MyProcess()
    t._logger = mock_logger
    frame = MagicMock()
    t(PipelineState(df=frame), _make_ctx())

    calls = mock_logger.info.call_args_list
    assert _metric_value(calls, "custom_metric") == 123
    only_partition_probe(frame)


def test_metrics_collector_receives_stage_metrics_for_process_transformer():
    from spark_quick_start.orchestration.metrics import StageMetricsCollector

    df = MagicMock()
    df.rdd.getNumPartitions.return_value = 2

    class MyProcess(ProcessTransformer):
        def _run(self, state: PipelineState, ctx) -> PipelineState:
            return state

    t = MyProcess()
    collector = StageMetricsCollector()
    t._metrics_collector = collector
    t(PipelineState(df=df), _make_ctx())

    assert len(collector.stages) == 1
    assert collector.stages[0].stage == "MyProcess"
    assert collector.stages[0].partitions_in == 2
    assert collector.stages[0].partitions_out == 2


def test_metrics_collector_receives_stage_metrics_for_source_transformer():
    from spark_quick_start.orchestration.metrics import StageMetricsCollector

    df = MagicMock()
    df.columns = ["id", "scdDeletedFlag"]
    df.rdd.getNumPartitions.return_value = 5

    class MySource(SourceTransformer):
        def _produce(self, ctx) -> PipelineState:
            return PipelineState(df=df)

    t = MySource()
    collector = StageMetricsCollector()
    t._metrics_collector = collector
    t.produce(_make_ctx())

    assert len(collector.stages) == 1
    assert collector.stages[0].stage == "MySource"
    assert collector.stages[0].partitions_out == 5


# ── _safe_explain_string against a real DataFrame ─────────────────────────────
# The tests above stub df._sc._jvm.PythonSQLUtils.explainString directly on a
# MagicMock, which passes regardless of whether the real attribute chain
# (df._sc._jvm / df._jdf.queryExecution()) is still valid on the installed
# PySpark version. This exercises the actual JVM call.

def _make_ctx_with_runtime(**runtime):
    """_make_ctx() with extra runtime_context keys (capture_partitions, etc.)."""
    base = {"correlation_id": "c1", "pipeline_name": "p1", "application_name": "a1"}
    ctx = _make_ctx()
    return dataclasses.replace(ctx, runtime_ctx=RuntimeContext({**base, **runtime}))


def _slow_partitions_df(seconds: float, partitions: int = 7):
    """A DataFrame mock whose getNumPartitions() is slow.

    Stands in for the real cliff: df.rdd on a plan with an AQE shuffle has to
    materialise the shuffle stages before it can report a final partition
    count, so what reads like a free attribute access runs a Spark job.
    """
    df = MagicMock()
    df.rdd.getNumPartitions.side_effect = lambda: (time.sleep(seconds), partitions)[1]
    return df


def test_duration_ms_includes_the_measurement_cost():
    """Regression: duration_ms used to be read before the measurement block.

    A stage whose partition count changed 16 -> 98 spent 117s inside
    _safe_num_partitions() materialising an AQE shuffle and reported 120s for
    a 238s stage. duration_ms must cover the same span as the start/end log
    timestamps, instrumentation included.
    """
    mock_logger = MagicMock()
    df_out = _slow_partitions_df(0.20)

    class MyProcess(ProcessTransformer):
        def _run(self, state: PipelineState, ctx) -> PipelineState:
            return state.with_df(df_out)

    t = MyProcess()
    t._logger = mock_logger
    t(PipelineState(df=MagicMock()), _make_ctx())

    calls = mock_logger.info.call_args_list
    duration_ms = _metric_value(calls, "duration_ms")
    assert duration_ms >= 200, f"measurement cost excluded from duration_ms: {duration_ms}"


def test_measure_ms_never_exceeds_the_duration_that_contains_it():
    """measure_ms is a part of duration_ms, so it can never be larger — F14.

    Deterministic rather than sleep-based, because the real-world reproduction
    rate is about 7.5% and a gate that fires one run in thirteen is a gate people
    re-run instead of believing.

    The cause is arithmetic, not scheduler noise: ``duration_ms`` is a single
    rounding of the whole span, while ``measure_ms`` used to be the sum of two
    independently rounded sub-spans. Two roundings can each gain half a
    millisecond, so their sum could exceed the single rounding by one.

    ``_TransformerBase`` calls ``time.monotonic()`` exactly four times per
    ``__call__`` — run start, end of before_run's measuring, start of after_run's
    measuring, end. The sequence below is one of the combinations that inverts
    the invariant under the old two-rounding arithmetic: the sub-spans round up
    to 1 + 200 = 201 while the whole span rounds down to 200.
    """
    frame = MagicMock()
    clock = iter([0.0, 0.0006, 0.0006, 0.2001])
    mock_logger = MagicMock()

    class MyProcess(ProcessTransformer):
        def _run(self, state: PipelineState, ctx) -> PipelineState:
            return state

    t = MyProcess()
    t._logger = mock_logger

    with patch.object(base_module.time, "monotonic", lambda: next(clock)):
        t(PipelineState(df=frame), _make_ctx())

    calls = mock_logger.info.call_args_list
    measure_ms = _metric_value(calls, "measure_ms")
    duration_ms = _metric_value(calls, "duration_ms")

    assert duration_ms == 200, f"clock sequence did not produce the intended span: {duration_ms}"
    assert measure_ms <= duration_ms, (
        f"measure_ms ({measure_ms}) exceeds the duration_ms ({duration_ms}) that contains it. "
        "Accumulate the measurement sub-spans as raw seconds and round once, rather than "
        "summing two separately rounded values."
    )
    only_partition_probe(frame)


def test_measure_ms_reports_the_instrumentation_cost():
    """The measurement cost is attributable, not merely folded into duration_ms."""
    mock_logger = MagicMock()
    df_out = _slow_partitions_df(0.20)

    class MyProcess(ProcessTransformer):
        def _run(self, state: PipelineState, ctx) -> PipelineState:
            return state.with_df(df_out)

    t = MyProcess()
    t._logger = mock_logger
    t(PipelineState(df=MagicMock()), _make_ctx())

    calls = mock_logger.info.call_args_list
    measure_ms = _metric_value(calls, "measure_ms")
    duration_ms = _metric_value(calls, "duration_ms")
    assert measure_ms >= 200
    assert measure_ms <= duration_ms


def test_partitions_captured_by_default():
    mock_logger = MagicMock()
    df = MagicMock()
    df.rdd.getNumPartitions.return_value = 5

    class MyProcess(ProcessTransformer):
        def _run(self, state: PipelineState, ctx) -> PipelineState:
            return state

    t = MyProcess()
    t._logger = mock_logger
    t(PipelineState(df=df), _make_ctx())

    assert _metric_value(mock_logger.info.call_args_list, "partitions_out") == 5


def test_capture_partitions_false_skips_the_call_entirely():
    """The escape hatch has to skip df.rdd, not just drop the metric.

    Reading the count is what costs — suppressing the metric afterwards would
    leave the shuffle materialisation in place and save nothing.
    """
    mock_logger = MagicMock()
    df = MagicMock()
    df.rdd.getNumPartitions.return_value = 5

    class MyProcess(ProcessTransformer):
        def _run(self, state: PipelineState, ctx) -> PipelineState:
            return state

    t = MyProcess()
    t._logger = mock_logger
    t(PipelineState(df=df), _make_ctx_with_runtime(capture_partitions=False))

    calls = mock_logger.info.call_args_list
    assert _metric_value(calls, "partitions_in") is None
    assert _metric_value(calls, "partitions_out") is None
    df.rdd.getNumPartitions.assert_not_called()


def test_measure_ms_reaches_the_metrics_collector():
    from spark_quick_start.orchestration.metrics import StageMetricsCollector

    df = MagicMock()
    df.rdd.getNumPartitions.return_value = 2

    class MyProcess(ProcessTransformer):
        def _run(self, state: PipelineState, ctx) -> PipelineState:
            return state

    t = MyProcess()
    collector = StageMetricsCollector()
    t._metrics_collector = collector
    t(PipelineState(df=df), _make_ctx())

    assert collector.stages[0].measure_ms is not None


def test_always_run_transformer_ignores_halted_state():
    """Regression guard for the shared halted-bypass base class used by
    MetricsChartTransformer and PlanReportTransformer."""
    calls = []

    class MyReporter(AlwaysRunTransformer):
        def _run(self, state: PipelineState, ctx) -> PipelineState:
            calls.append("ran")
            return state

    t = MyReporter()
    frame = MagicMock()
    state = PipelineState(df=frame).with_halted("no candidates")

    result = t(state, _make_ctx())

    assert calls == ["ran"]
    assert result.halted is True
    only_partition_probe(frame)


def test_always_run_transformer_is_a_process_transformer():
    assert issubclass(AlwaysRunTransformer, ProcessTransformer)


def test_safe_explain_string_against_real_dataframe(spark):
    from spark_quick_start.orchestration.transformers.base import _safe_explain_string

    df = spark.createDataFrame([(1, "a")], ["id", "name"])

    assert _safe_explain_string(df, enabled=False) is None

    plan = _safe_explain_string(df, enabled=True)
    assert plan is not None
    assert "Physical Plan" in plan


def test_duration_never_exceeds_the_logged_wall_time():
    """duration_ms must not be larger than the start->end log-timestamp gap.

    duration_ms is read at the end of after_run, so it spans before_run as
    well. If before_run logged its start event *after* measuring the input,
    a stage whose input measurement is the expensive part (df.rdd on an
    upstream AQE shuffle) would report a duration exceeding its own logged
    wall time — inverting the exact comparison used to find the bug this
    metric exists for.
    """
    events = []

    logger = MagicMock()

    def _record(msg, **kwargs):
        events.append((time.monotonic(), kwargs.get("tags", {}).get("event"), kwargs))

    logger.info.side_effect = _record

    df_in = _slow_partitions_df(0.20)

    class MyProcess(ProcessTransformer):
        def _run(self, state: PipelineState, ctx) -> PipelineState:
            return state

    t = MyProcess()
    t._logger = logger
    t(PipelineState(df=df_in), _make_ctx())

    start_ts = next(ts for ts, ev, _ in events if ev == "start")
    end_ts, _, end_kwargs = next(e for e in events if e[1] == "end")
    logged_wall_ms = (end_ts - start_ts) * 1000
    duration_ms = next(
        m["metricValue"] for m in end_kwargs["metrics"] if m["metricName"] == "duration_ms"
    )

    assert duration_ms <= logged_wall_ms + 5, (
        f"duration_ms {duration_ms} exceeds logged wall time {logged_wall_ms:.0f}ms"
    )
