"""
tests/test_stage_metrics.py — Unit tests for orchestration/metrics.py.

No Spark required. All tests are pure Python.
"""
from __future__ import annotations

import pytest

from spark_quick_start.orchestration.metrics import StageMetrics, StageMetricsCollector


class TestStageMetrics:
    def test_only_stage_and_duration_required(self):
        m = StageMetrics(stage="MyStage", duration_ms=10)
        assert m.stage == "MyStage"
        assert m.duration_ms == 10

    def test_optional_fields_default_to_none(self):
        m = StageMetrics(stage="MyStage", duration_ms=10)
        assert m.partitions_in is None
        assert m.partitions_out is None
        assert m.row_count_in is None
        assert m.row_count_out is None
        assert m.inserted is None
        assert m.updated is None
        assert m.deleted is None
        assert m.files_added is None
        assert m.files_removed is None
        assert m.delta_operation is None
        assert m.plan is None


class TestStageMetricsCollector:
    def test_record_appends_and_returns_stage_metrics(self):
        collector = StageMetricsCollector()
        result = collector.record(stage="StageA", duration_ms=5)
        assert isinstance(result, StageMetrics)
        assert collector.stages == [result]

    def test_record_multiple_stages_preserves_order(self):
        collector = StageMetricsCollector()
        collector.record(stage="First", duration_ms=1)
        collector.record(stage="Second", duration_ms=2)
        assert [s.stage for s in collector.stages] == ["First", "Second"]

    def test_record_rejects_unknown_field(self):
        collector = StageMetricsCollector()
        with pytest.raises(TypeError):
            collector.record(stage="StageA", duration_ms=5, not_a_real_field=1)
