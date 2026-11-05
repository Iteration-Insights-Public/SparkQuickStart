"""
notebook_executor.py — NotebookExecutor and NotebookExecutionResult.

Contains: NotebookExecutor (wraps PlatformAdapter notebook calls),
    NotebookExecutionResult (parses child notebook exit values).
Does NOT contain: staging orchestration logic, platform-specific imports,
    or any import from spark_load_orchestration.

Usage::

    from spark_quick_start.orchestration.notebook_executor import NotebookExecutor
    from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter

    executor = NotebookExecutor(MockPlatformAdapter(), logger)
    json_str = executor.run_single("NB_Stage_Orders", timeout=90, args={})
"""
from __future__ import annotations

import json
from typing import Any

from spark_quick_start.adapters.platform_adapter import PlatformAdapter
from spark_quick_start.orchestration.models import ErrorDetails, ExecutionResult

__all__ = ["NotebookExecutionResult", "NotebookExecutor"]


# ============================================================================
# NOTEBOOK EXECUTION RESULT
# ============================================================================

class NotebookExecutionResult:
    """Parses and wraps the exit value returned by a child staging notebook."""

    def __init__(self, notebook_name: str, result_data: dict[str, Any]) -> None:
        self.notebook_name = notebook_name
        self.raw_result = result_data

        parsed = json.loads(result_data["exitVal"])
        self.has_error: bool = parsed.get("has_error", False)
        self.metrics: dict[str, Any] = parsed.get("metrics") or {}
        self.logs_str: str = parsed.get("logs", "")
        self.error_details: dict[str, Any] = parsed.get("error_details") or ErrorDetails(
            "Unhandled error", "Unhandled error", ""
        ).to_dict()
        try:
            self.logs: Any = json.loads(self.logs_str) if self.logs_str.strip() else {}
        except Exception:
            self.logs = {"raw": self.logs_str}

    def is_successful(self) -> bool:
        return not self.has_error

    def get_metric(self, name: str, default: Any = None) -> Any:
        return self.metrics.get(name, default)

    def format_metrics(self) -> str:
        if not self.metrics:
            return "No metrics available"
        parts = []
        if "source_count" in self.metrics:
            parts.append(f"Source: {self.metrics['source_count']:,}")
        if "target_count" in self.metrics:
            parts.append(f"Staged: {self.metrics['target_count']:,}")
        if "duration_seconds" in self.metrics:
            parts.append(f"Duration: {self.metrics['duration_seconds']:.2f}s")
        if self.metrics.get("hwm_value_before") is not None:
            parts.append(f"HWM Before: {self.metrics['hwm_value_before']}")
        if self.metrics.get("hwm_value_after") is not None:
            parts.append(f"HWM After: {self.metrics['hwm_value_after']}")
        return " | ".join(parts) if parts else "No metrics available"

    def format_error_details(self) -> str:
        if not self.has_error:
            return ""
        return (
            f"Error: {self.error_details.get('error_type', '')}\n"
            f"Message: {self.error_details.get('error_message', '')}\n"
            f"Traceback:\n{self.error_details.get('traceback', '')}"
        )

    def __str__(self) -> str:
        status = "FAILED" if self.has_error else "SUCCESS"
        lines = [f"NotebookExecutionResult({self.notebook_name}): {status}"]
        if self.metrics:
            lines.append(f"  {self.format_metrics()}")
        if self.has_error:
            lines.append(f"  {self.format_error_details()}")
        return "\n".join(lines)


# ============================================================================
# NOTEBOOK EXECUTOR
# ============================================================================

class NotebookExecutor:
    """Runs child staging notebooks via the injected PlatformAdapter.

    Separates the concern of notebook invocation from staging orchestration
    logic. Holds no state between calls; all execution state is returned.

    Args:
        adapter: Platform adapter used for run_notebook and run_notebooks_parallel.
        logger: Logger instance for run-level events.

    Example::

        from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter
        from spark_quick_start.orchestration.notebook_executor import NotebookExecutor
        import logging

        adapter = MockPlatformAdapter()
        executor = NotebookExecutor(adapter, logging.getLogger("test"))
        result_json = executor.run_single("NB_Stage_Orders", timeout=90, args={"ctx": "{}"})
    """

    def __init__(self, adapter: PlatformAdapter, logger: Any) -> None:
        self._adapter = adapter
        self._logger = logger

    def run_single(self, name: str, timeout: int, args: dict[str, Any]) -> str:
        """Run one notebook synchronously.

        Wraps adapter.run_notebook. Catches any exception and returns it
        as a serialised ExecutionResult with status="error" so the caller
        can handle it uniformly without a bare try/except.

        Args:
            name: Notebook name or path.
            timeout: Per-cell timeout in seconds.
            args: Key/value arguments passed to the notebook.

        Returns:
            Raw exit value JSON string (ExecutionResult.to_json() format).
        """
        try:
            return self._adapter.run_notebook(name, timeout, args)
        except Exception as exc:
            import traceback as _tb
            err = ExecutionResult(
                status="error",
                has_error=True,
                error_details=ErrorDetails.from_exception(exc, _tb.format_exc()).to_dict(),
            )
            return err.to_json()

    def run_parallel(self, dag: dict[str, Any]) -> dict[str, Any]:
        """Run multiple notebooks concurrently.

        Delegates entirely to adapter.run_notebooks_parallel.

        Args:
            dag: DAG dict with activities, optional concurrency,
                 and timeoutInSeconds.

        Returns:
            {activity_name: {"exitVal": <json string>}}
        """
        return self._adapter.run_notebooks_parallel(dag)
