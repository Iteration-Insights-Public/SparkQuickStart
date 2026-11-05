"""Tests for MetricsChartTransformer."""
from __future__ import annotations

from unittest.mock import MagicMock

from _frames import only_partition_probe

from spark_quick_start.orchestration.metrics import StageMetrics, StageMetricsCollector
from spark_quick_start.orchestration.models import (
    GlobalContext,
    LoggingContext,
    RuntimeContext,
    SCDContext,
    StagingContext,
)
from spark_quick_start.orchestration.transformers.base import LoadContext
from spark_quick_start.orchestration.transformers.metrics_chart import MetricsChartTransformer
from spark_quick_start.orchestration.transformers.state import PipelineState


def test_render_empty_list_returns_notice():
    output = MetricsChartTransformer._render([])
    assert "No stage metrics captured" in output


def test_render_sorts_by_duration_descending():
    stages = [
        StageMetrics(stage="Fast", duration_ms=10),
        StageMetrics(stage="Slow", duration_ms=1000),
        StageMetrics(stage="Medium", duration_ms=500),
    ]
    output = MetricsChartTransformer._render(stages)
    lines = [l for l in output.splitlines() if "ms" in l]
    order = [l.split()[0] for l in lines]
    assert order == ["Slow", "Medium", "Fast"]


def test_render_longest_bar_reaches_max_width():
    stages = [
        StageMetrics(stage="A", duration_ms=100),
        StageMetrics(stage="B", duration_ms=50),
    ]
    output = MetricsChartTransformer._render(stages)
    lines = [l for l in output.splitlines() if "ms" in l]
    # The slowest stage's bar (between the brackets) must be exactly 50 '#' chars.
    bar = lines[0].split("[", 1)[1].rstrip("]").rstrip()
    assert bar == "#" * 50


def test_render_all_zero_durations_does_not_raise():
    stages = [
        StageMetrics(stage="A", duration_ms=0),
        StageMetrics(stage="B", duration_ms=0),
    ]
    output = MetricsChartTransformer._render(stages)  # must not raise ZeroDivisionError
    lines = [l for l in output.splitlines() if "ms" in l]
    for line in lines:
        bar = line.split("[", 1)[1].rstrip("]").rstrip()
        assert bar == ""


def test_render_includes_stage_name_and_duration():
    stages = [StageMetrics(stage="StagingTransformer", duration_ms=9801)]
    output = MetricsChartTransformer._render(stages)
    assert "StagingTransformer" in output
    assert "9801ms" in output


def _ctx() -> LoadContext:
    return LoadContext(
        global_ctx=GlobalContext({"id": "ent_1", "entity_name": "E", "source_system": "S",
                                   "target_table_path": "/t", "staging_table_path": "/s"}),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext({"business_key_column": "id"}),
        runtime_ctx=RuntimeContext({"correlation_id": "c", "pipeline_name": "p", "application_name": "a"}),
        logging_ctx=LoggingContext({}),
    )


def test_call_runs_even_when_state_halted(capsys):
    """Regression guard for the deliberate halted-bypass — see spec."""
    t = MetricsChartTransformer()
    t._metrics_collector = StageMetricsCollector()
    t._metrics_collector.record(stage="StagingTransformer", duration_ms=100)

    frame = MagicMock()
    state = PipelineState(df=frame).with_halted("no candidates")
    assert state.halted is True

    t(state, _ctx())

    captured = capsys.readouterr()
    assert "StagingTransformer" in captured.out
    only_partition_probe(frame)


def test_call_returns_state_unchanged():
    t = MetricsChartTransformer()
    t._metrics_collector = StageMetricsCollector()

    df = MagicMock()
    target = MagicMock()
    state = PipelineState(df=df, frames={"target": target}).with_halted("reason")
    result = t(state, _ctx())

    assert result.df is df
    assert result.halted is True
    assert result.halt_reason == "reason"
    assert "target" in result.frames
    only_partition_probe(df)
    assert not target.method_calls


def test_run_prints_notice_when_metrics_collector_is_none(capsys):
    t = MetricsChartTransformer()
    t._metrics_collector = None
    frame = MagicMock()
    state = PipelineState(df=frame)

    t(state, _ctx())

    captured = capsys.readouterr()
    assert "No stage metrics captured" in captured.out
    only_partition_probe(frame)


def test_run_prints_all_collected_stages(capsys):
    t = MetricsChartTransformer()
    t._metrics_collector = StageMetricsCollector()
    t._metrics_collector.record(stage="StagingTransformer", duration_ms=9801)
    t._metrics_collector.record(stage="SCDChangeDetectorTransformer", duration_ms=2408)
    frame = MagicMock()
    state = PipelineState(df=frame)

    t(state, _ctx())

    captured = capsys.readouterr()
    assert "StagingTransformer" in captured.out
    assert "SCDChangeDetectorTransformer" in captured.out
    only_partition_probe(frame)
