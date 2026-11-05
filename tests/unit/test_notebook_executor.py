"""
Tests for NotebookExecutor and NotebookExecutionResult.

No Spark required. All tests use MockPlatformAdapter and a plain logging.Logger.
"""
from __future__ import annotations

import json
import logging
from dataclasses import asdict

from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter
from spark_quick_start.orchestration.models import ExecutionMetrics, ExecutionResult
from spark_quick_start.orchestration.notebook_executor import (
    NotebookExecutionResult,
    NotebookExecutor,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _null_logger(name: str = "test") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.addHandler(logging.NullHandler())
    return logger


def _success_payload(source: int = 10, target: int = 10, duration: float = 1.0) -> dict:
    result = ExecutionResult(
        status="success",
        has_error=False,
        metrics=asdict(
            ExecutionMetrics(source_count=source, target_count=target, duration_seconds=duration)
        ),
    )
    return {"exitVal": result.to_json()}


def _error_payload(error_type: str = "ValueError", message: str = "bad") -> dict:
    result = ExecutionResult(
        status="error",
        has_error=True,
        error_details={
            "error_type": error_type,
            "error_message": message,
            "traceback": "tb...",
        },
    )
    return {"exitVal": result.to_json()}


# ---------------------------------------------------------------------------
# TestNotebookExecutor
# ---------------------------------------------------------------------------

class TestNotebookExecutor:
    def test_run_single_happy_path(self):
        """MockPlatformAdapter with a registered success result; run_single returns JSON with has_error==False."""
        adapter = MockPlatformAdapter(
            notebook_results={
                "NB_Stage_Orders": ExecutionResult(
                    status="success",
                    has_error=False,
                    metrics=asdict(ExecutionMetrics(source_count=5, target_count=5)),
                ).to_json()
            }
        )
        executor = NotebookExecutor(adapter, _null_logger())
        result_json = executor.run_single("NB_Stage_Orders", timeout=90, args={})
        data = json.loads(result_json)
        assert data["has_error"] is False

    def test_run_single_exception_returns_error_result(self):
        """Adapter raises RuntimeError; run_single returns JSON with has_error==True and error_type=='RuntimeError'."""
        adapter = MockPlatformAdapter()
        # Override run_notebook to raise unconditionally
        adapter.run_notebook = lambda name, timeout, args: (_ for _ in ()).throw(RuntimeError("boom"))
        executor = NotebookExecutor(adapter, _null_logger())
        result_json = executor.run_single("NB_Stage_Orders", timeout=90, args={})
        data = json.loads(result_json)
        assert data["has_error"] is True
        assert data["error_details"]["error_type"] == "RuntimeError"

    def test_run_single_error_details_has_the_pinned_shape(self):
        """F114: error_details is ErrorDetails' dict form, with exactly the keys it always had."""
        adapter = MockPlatformAdapter()
        adapter.run_notebook = lambda name, timeout, args: (_ for _ in ()).throw(RuntimeError("boom"))
        data = json.loads(NotebookExecutor(adapter, _null_logger()).run_single("NB", timeout=90, args={}))
        details = data["error_details"]
        assert set(details) == {"error_type", "error_message", "traceback"}
        assert (details["error_type"], details["error_message"]) == ("RuntimeError", "boom")
        assert "RuntimeError: boom" in details["traceback"]

    def test_missing_error_details_default_is_the_pinned_shape(self):
        payload = {"exitVal": json.dumps({"has_error": True})}
        res = NotebookExecutionResult("NB_A", payload)
        assert res.error_details == {
            "error_type": "Unhandled error", "error_message": "Unhandled error", "traceback": ""}

    def test_run_single_records_call(self):
        """After run_single, adapter.run_calls has one entry with the correct notebook name."""
        adapter = MockPlatformAdapter()
        executor = NotebookExecutor(adapter, _null_logger())
        executor.run_single("NB_Stage_Orders", timeout=90, args={"key": "val"})
        assert len(adapter.run_calls) == 1
        assert adapter.run_calls[0][0] == "NB_Stage_Orders"

    def test_run_parallel_delegates_to_adapter(self):
        """run_parallel calls adapter.run_notebooks_parallel; result contains the activity name key."""
        adapter = MockPlatformAdapter()
        executor = NotebookExecutor(adapter, _null_logger())
        dag = {
            "activities": [
                {
                    "name": "Stage_1_NB_Stage_Orders",
                    "path": "NB_Stage_Orders",
                    "timeoutPerCellInSeconds": 90,
                    "args": {},
                }
            ]
        }
        results = executor.run_parallel(dag)
        assert "Stage_1_NB_Stage_Orders" in results


# ---------------------------------------------------------------------------
# TestNotebookExecutionResult
# ---------------------------------------------------------------------------

class TestNotebookExecutionResult:
    def test_success_parsing(self):
        """Payload with has_error=False → is_successful()==True, has_error==False."""
        res = NotebookExecutionResult("NB_A", _success_payload())
        assert res.is_successful() is True
        assert res.has_error is False

    def test_error_parsing(self):
        """Payload with has_error=True → is_successful()==False, 'ValueError' in format_error_details()."""
        res = NotebookExecutionResult("NB_A", _error_payload(error_type="ValueError", message="bad"))
        assert res.is_successful() is False
        assert "ValueError" in res.format_error_details()

    def test_get_metric_present(self):
        """get_metric('source_count')==10."""
        res = NotebookExecutionResult("NB_A", _success_payload(source=10))
        assert res.get_metric("source_count") == 10

    def test_get_metric_absent_uses_default(self):
        """get_metric('missing', default='X')=='X'."""
        res = NotebookExecutionResult("NB_A", _success_payload())
        assert res.get_metric("missing", default="X") == "X"

    def test_format_metrics_happy(self):
        """'Source: 5' and 'Staged: 5' in format_metrics()."""
        res = NotebookExecutionResult("NB_A", _success_payload(source=5, target=5))
        fmt = res.format_metrics()
        assert "Source: 5" in fmt
        assert "Staged: 5" in fmt

    def test_format_metrics_no_data(self):
        """Empty metrics dict → format_metrics()=='No metrics available'."""
        payload = {"exitVal": json.dumps({"has_error": False, "metrics": {}})}
        res = NotebookExecutionResult("NB_A", payload)
        assert res.format_metrics() == "No metrics available"

    def test_format_metrics_hwm_both_present(self):
        """hwm_value_before and hwm_value_after non-None → 'HWM Before:' and 'HWM After:' in output."""
        metrics = asdict(
            ExecutionMetrics(
                source_count=5,
                target_count=5,
                hwm_value_before="2024-01-15T10:00:00",
                hwm_value_after="2024-06-04T12:30:00",
            )
        )
        payload = {
            "exitVal": ExecutionResult(
                status="success", has_error=False, metrics=metrics
            ).to_json()
        }
        res = NotebookExecutionResult("NB_A", payload)
        fmt = res.format_metrics()
        assert "HWM Before:" in fmt
        assert "HWM After:" in fmt

    def test_format_metrics_hwm_none(self):
        """hwm fields set to None → 'HWM' not in output."""
        payload = {
            "exitVal": json.dumps(
                {"has_error": False, "metrics": {"hwm_value_before": None, "hwm_value_after": None}}
            )
        }
        res = NotebookExecutionResult("NB_A", payload)
        assert "HWM" not in res.format_metrics()

    def test_str_success(self):
        """'SUCCESS' in str(result)."""
        res = NotebookExecutionResult("NB_A", _success_payload())
        assert "SUCCESS" in str(res)

    def test_str_failure(self):
        """'FAILED' in str(result)."""
        res = NotebookExecutionResult("NB_A", _error_payload())
        assert "FAILED" in str(res)
