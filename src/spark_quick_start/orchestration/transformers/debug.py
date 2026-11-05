"""debug.py — DebugTransformer: pass-through that pretty-prints PipelineState samples."""
from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from spark_quick_start.orchestration.transformers.base import ProcessTransformer

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from spark_quick_start.orchestration.transformers.base import LoadContext
    from spark_quick_start.orchestration.transformers.state import PipelineState


class DebugTransformer(ProcessTransformer):
    """Pass-through transformer that pretty-prints a sample of PipelineState for debugging.

    Insert at any pipeline position to inspect the primary DataFrame and optional
    named frames. Renders output as Markdown tables, readable in Fabric/Databricks
    notebooks and plain terminals alike.

    Never aborts the pipeline: all exceptions inside _emit are caught and demoted
    to warnings. The original state is always returned unchanged.

    When enabled=False nothing is printed and the step runs no count() of its own.
    The stage's instrumentation still runs, as for every step: capture_partitions
    (on by default) can materialise a shuffle, and capture_row_counts runs a
    count() before and after the step.

    Emitting rows writes real data to stdout, and the orchestrator's logger
    captures stdout into the persisted log payload — which normally lands on a
    lakehouse volume whose read permissions do not match the source table's.
    So this transformer warns once per run when it is about to emit rows into
    a persisted log, naming the destination. It warns rather than refuses:
    showing rows is the entire point of the transformer, and suppressing it
    would only push the same data through a hand-written ``print``. Set
    ``logging_context.suppress_stdout_tail`` to keep stdout out of the log
    file, or ``n=0`` to emit no rows.

    Args:
        label: Human-readable name for this checkpoint (printed as a header).
        n: Number of rows to show per frame. 0 = no rows; the row count, and the
            schema when show_schema is True, are still printed, so each frame
            still costs a count().
        frames: Names of state.frames entries to also inspect after state.df.
        show_schema: When True, emit a Markdown schema table before the data rows.
        truncate: Maximum character width per cell value. 0 = unlimited.
        enabled: When False, nothing is printed and the step runs no count() of
            its own; the base instrumentation under the capture flags still runs.

    Example::

        # default_pipeline() already registers its five steps and .add() appends,
        # so a checkpoint between two of them means composing the chain by hand.
        pipeline = (
            LoadOrchestrator(ctx_json, spark, platform="local")
            .add(StagingTransformer())
            .add(DebugTransformer("after staging", n=10))
            .add(SCDTargetTableTransformer())
            .add(SCDChangeDetectorTransformer())
            .add(CandidateGuardTransformer())
            .add(NativeSCDMergeTransformer())
        )
        # A checkpoint at the end needs no rewiring:
        # default_pipeline(ctx_json, spark).add(DebugTransformer("after merge"))
    """

    def __init__(
        self,
        label: str = "",
        *,
        n: int = 20,
        frames: list[str] | None = None,
        show_schema: bool = True,
        truncate: int = 80,
        enabled: bool = True,
    ) -> None:
        self._label = label
        self._n = n
        self._frames = list(frames) if frames else []
        self._show_schema = show_schema
        self._truncate = truncate
        self._enabled = enabled
        self._warned_persisted_log = False

    def before_run(self, state: PipelineState, ctx: LoadContext) -> None:  # type: ignore[override]
        """Reset the per-run warning latch, then log the stage start as usual.

        The latch has to be cleared here rather than only in __init__: on
        Fabric and Databricks the kernel survives between pipeline runs, so a
        module-level pipeline built once and re-run would otherwise warn only
        on the very first run of the session.
        """
        self._warned_persisted_log = False
        super().before_run(state, ctx)

    def _run(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
        if not self._enabled:
            return state
        try:
            self._emit(state, ctx)
        except Exception as exc:
            self._warn(f"DebugTransformer raised during output — pipeline continues. {exc!r}")
        return state

    def _emit(self, state: PipelineState, ctx: LoadContext) -> None:
        self._warn_if_rows_reach_persisted_log(ctx)
        self._output(self._format_header(self._label or "DebugTransformer"))
        self._emit_df("df (primary)", state.df)
        for name in self._frames:
            if name in state.frames:
                self._emit_df(f'frames["{name}"]', state.frames[name])
            else:
                self._output(
                    f'> ⚠️ frame `"{name}"` not found in state.frames. '
                    f"Available: `{list(state.frames)}`"
                )

    def _warn_if_rows_reach_persisted_log(self, ctx: LoadContext) -> None:
        """Warn once per run when sample rows will be written into the log file.

        Only fires when rows are actually emitted (``n > 0``) and the run's
        stdout is being persisted. Schema-only output carries column names, not
        values, so it is not flagged.
        """
        if self._warned_persisted_log or self._n <= 0:
            return
        self._warned_persisted_log = True

        logging_ctx = getattr(ctx, "logging_ctx", None)
        if logging_ctx is None or getattr(logging_ctx, "suppress_stdout_tail", False):
            return

        destination = getattr(logging_ctx, "log_file_dir", None) or "the run log"
        self._warn(
            f"{self._label or 'DebugTransformer'}: emitting up to {self._n} sample "
            f"rows to stdout, which is captured into the persisted log at "
            f"{destination}. Row values will be readable by anyone with access "
            f"to that location. Set logging_context.suppress_stdout_tail=true to "
            f"keep stdout out of the log file, or n=0 to print no rows."
        )

    def _emit_df(self, title: str, df: DataFrame) -> None:
        total = df.count()
        shown = min(self._n, total) if self._n > 0 else 0
        self._output(f"\n### {title}\n")
        self._output(f"**Rows:** {total} total, showing {shown}")

        if self._show_schema:
            self._output(self._schema_table(df))

        if self._n > 0:
            rows = df.limit(self._n).collect()
            self._output(self._data_table(df.columns, rows))

    def _schema_table(self, df: DataFrame) -> str:
        fields = df.schema.fields
        rows = [(f.name, str(f.dataType.simpleString()), str(f.nullable)) for f in fields]
        return "\n**Schema:**\n" + self._md_table(["column", "type", "nullable"], rows)

    def _data_table(self, columns: list[str], rows: list) -> str:
        str_rows = [
            [self._cell(str(row[col])) for col in columns]
            for row in rows
        ]
        return "\n" + self._md_table(columns, str_rows)

    def _cell(self, value: str) -> str:
        # Escape pipes so they don't break Markdown table columns
        escaped = value.replace("|", "\\|").replace("\n", " ")
        if self._truncate and len(escaped) > self._truncate:
            return escaped[: self._truncate] + "…"
        return escaped

    @staticmethod
    def _md_table(headers: list[str], rows: list[list[str]]) -> str:
        col_widths = [max(len(h), *(len(r[i]) for r in rows), 1) for i, h in enumerate(headers)] if rows else [len(h) for h in headers]
        sep = "| " + " | ".join("-" * w for w in col_widths) + " |"
        header_row = "| " + " | ".join(h.ljust(w) for h, w in zip(headers, col_widths)) + " |"
        data_rows = [
            "| " + " | ".join(cell.ljust(w) for cell, w in zip(row, col_widths)) + " |"
            for row in rows
        ]
        return "\n".join([header_row, sep, *data_rows])

    def _output(self, text: str) -> None:
        print(text)

    def _warn(self, message: str) -> None:
        if self._logger is not None:
            self._logger.warning(message)
        else:
            print(f"[DebugTransformer] WARNING: {message}", file=sys.stderr)

    @staticmethod
    def _format_header(label: str) -> str:
        return f"\n## 🔍 DEBUG CHECKPOINT: {label}\n"
