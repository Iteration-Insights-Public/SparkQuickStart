"""
metrics.py — Per-stage performance/volume metrics for the orchestration pipeline.

Contains: StageMetrics, StageMetricsCollector.
Does NOT contain: PySpark logic — this module is platform-neutral.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class StageMetrics:
    """Per-transformer performance/volume metrics captured by _TransformerBase.

    Field names intentionally match the existing logged metricName strings
    ("inserted", "updated") so the collector and the structured log payload
    stay in sync without a name-mapping layer.
    """
    stage: str
    duration_ms: int
    measure_ms: int | None = None
    partitions_in: int | None = None
    partitions_out: int | None = None
    row_count_in: int | None = None
    row_count_out: int | None = None
    inserted: int | None = None
    updated: int | None = None
    deleted: int | None = None
    files_added: int | None = None
    files_removed: int | None = None
    delta_operation: str | None = None
    plan: str | None = None


class StageMetricsCollector:
    """Accumulates StageMetrics for a single LoadOrchestrator.run() call.

    Injected into every transformer the same way _spark/_adapter/_logger are
    injected — never threaded through PipelineState/LoadContext.
    """

    def __init__(self) -> None:
        self.stages: list[StageMetrics] = []

    def record(self, **kwargs: Any) -> StageMetrics:
        """Build a StageMetrics from kwargs (must match its field names) and store it."""
        sm = StageMetrics(**kwargs)
        self.stages.append(sm)
        return sm


__all__ = ["StageMetrics", "StageMetricsCollector"]
