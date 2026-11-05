"""metrics_chart.py — MetricsChartTransformer: ASCII bar chart of per-stage duration."""
from __future__ import annotations

from typing import TYPE_CHECKING

from spark_quick_start.orchestration.transformers.base import AlwaysRunTransformer

if TYPE_CHECKING:
    from spark_quick_start.orchestration.metrics import StageMetrics
    from spark_quick_start.orchestration.transformers.base import LoadContext
    from spark_quick_start.orchestration.transformers.state import PipelineState

_BAR_MAX_WIDTH = 50
_HEADER = "=== Pipeline Stage Duration ==="


class MetricsChartTransformer(AlwaysRunTransformer):
    """Pass-through transformer that prints an ASCII bar chart of per-stage duration_ms.

    Add as the last .add() step in a pipeline. Reads self._metrics_collector
    (injected by the pipeline runner the same way _spark/_adapter/_logger are)
    and prints a chart sorted slowest-first. Runs even when the pipeline has
    halted early — see AlwaysRunTransformer — since that is exactly when
    per-stage timing is most useful for diagnosis.
    """

    @staticmethod
    def _render(stages: list[StageMetrics]) -> str:
        if not stages:
            return f"{_HEADER}\nNo stage metrics captured."

        ordered = sorted(stages, key=lambda s: s.duration_ms, reverse=True)
        max_duration = max(s.duration_ms for s in ordered)
        name_width = max(len(s.stage) for s in ordered)

        lines = [_HEADER]
        for s in ordered:
            bar_len = (
                round((s.duration_ms / max_duration) * _BAR_MAX_WIDTH)
                if max_duration > 0
                else 0
            )
            bar = "#" * bar_len
            duration_str = f"{s.duration_ms}ms".rjust(10)
            lines.append(
                f"{s.stage.ljust(name_width)}  {duration_str} [{bar.ljust(_BAR_MAX_WIDTH)}]"
            )
        return "\n".join(lines)

    def _run(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
        stages = self._metrics_collector.stages if self._metrics_collector else []
        print(self._render(stages))
        return state
