"""plan_report.py — PlanReportTransformer: print every captured stage's Spark query plan."""
from __future__ import annotations

from typing import TYPE_CHECKING

from spark_quick_start.orchestration.transformers.base import AlwaysRunTransformer

if TYPE_CHECKING:
    from spark_quick_start.orchestration.metrics import StageMetrics
    from spark_quick_start.orchestration.transformers.base import LoadContext
    from spark_quick_start.orchestration.transformers.state import PipelineState

_HEADER = "=== Pipeline Query Plans ==="
_NONE_CAPTURED = f"{_HEADER}\nNo plans captured — set runtime_ctx.capture_plans=true."


class PlanReportTransformer(AlwaysRunTransformer):
    """Pass-through transformer that prints every captured stage's Spark query plan.

    Reads self._metrics_collector (injected by the pipeline runner the same way
    _spark/_adapter/_logger are) and prints StageMetrics.plan for every stage
    that captured one, in pipeline order. Requires runtime_ctx.capture_plans=True
    — without it, every StageMetrics.plan is None and this prints a
    "no plans captured" notice instead.

    Add as the last .add() step. Runs even when the pipeline halted early — see
    AlwaysRunTransformer — for the same reason as MetricsChartTransformer: plans
    are most useful for diagnosing exactly the early-exit case.
    """

    @staticmethod
    def _render(stages: list[StageMetrics]) -> str:
        captured = [s for s in stages if s.plan is not None]
        if not captured:
            return _NONE_CAPTURED
        sections = [_HEADER]
        for s in captured:
            sections.append(f"\n--- {s.stage} ---\n{s.plan}")
        return "\n".join(sections)

    def _run(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
        stages = self._metrics_collector.stages if self._metrics_collector else []
        print(self._render(stages))
        return state
