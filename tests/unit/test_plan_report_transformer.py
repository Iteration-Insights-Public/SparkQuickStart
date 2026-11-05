"""Tests for PlanReportTransformer."""
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
from spark_quick_start.orchestration.transformers.plan_report import PlanReportTransformer
from spark_quick_start.orchestration.transformers.state import PipelineState


def test_render_no_stages_returns_notice():
    output = PlanReportTransformer._render([])
    assert "No plans captured" in output
    assert "capture_plans" in output


def test_render_skips_stages_without_a_plan():
    stages = [
        StageMetrics(stage="StagingTransformer", duration_ms=10, plan=None),
        StageMetrics(stage="SCDChangeDetectorTransformer", duration_ms=20, plan=None),
    ]
    output = PlanReportTransformer._render(stages)
    assert "No plans captured" in output


def test_render_includes_empty_string_plan_not_just_truthy_ones():
    """Regression guard: an empty-but-not-None plan must still render as
    'captured' — a truthy check (`if s.plan`) would wrongly treat "" the
    same as a stage that never captured anything.
    """
    stages = [StageMetrics(stage="StagingTransformer", duration_ms=10, plan="")]
    output = PlanReportTransformer._render(stages)
    assert "No plans captured" not in output
    assert "StagingTransformer" in output


def test_render_includes_stage_name_and_plan_text():
    stages = [
        StageMetrics(stage="StagingTransformer", duration_ms=10, plan="== Physical Plan ==\nScan A"),
        StageMetrics(stage="SCDChangeDetectorTransformer", duration_ms=20, plan=None),
        StageMetrics(stage="NativeSCDMergeTransformer", duration_ms=30, plan="== Physical Plan ==\nMerge"),
    ]
    output = PlanReportTransformer._render(stages)
    assert "StagingTransformer" in output
    assert "Scan A" in output
    assert "NativeSCDMergeTransformer" in output
    assert "Merge" in output
    assert "SCDChangeDetectorTransformer" not in output  # no plan captured for this one


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
    t = PlanReportTransformer()
    t._metrics_collector = StageMetricsCollector()
    t._metrics_collector.record(stage="StagingTransformer", duration_ms=100, plan="== Physical Plan ==\nScan")

    frame = MagicMock()
    state = PipelineState(df=frame).with_halted("no candidates")
    assert state.halted is True

    t(state, _ctx())

    captured = capsys.readouterr()
    assert "StagingTransformer" in captured.out
    only_partition_probe(frame)


def test_call_returns_state_unchanged():
    t = PlanReportTransformer()
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
    t = PlanReportTransformer()
    t._metrics_collector = None
    frame = MagicMock()
    state = PipelineState(df=frame)

    t(state, _ctx())

    captured = capsys.readouterr()
    assert "No plans captured" in captured.out
    only_partition_probe(frame)


def test_run_prints_all_captured_plans(capsys):
    t = PlanReportTransformer()
    t._metrics_collector = StageMetricsCollector()
    t._metrics_collector.record(stage="StagingTransformer", duration_ms=100, plan="== Physical Plan ==\nScan A")
    t._metrics_collector.record(stage="NativeSCDMergeTransformer", duration_ms=200, plan="== Physical Plan ==\nMerge")
    frame = MagicMock()
    state = PipelineState(df=frame)

    t(state, _ctx())

    captured = capsys.readouterr()
    assert "StagingTransformer" in captured.out
    assert "Scan A" in captured.out
    assert "NativeSCDMergeTransformer" in captured.out
    assert "Merge" in captured.out
    only_partition_probe(frame)
