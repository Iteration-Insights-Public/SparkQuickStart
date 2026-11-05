"""
Tests for staging_pipeline.py
==============================

Coverage:
  - execute_staging_phases (sequential, parallel, batched)
  - create_staging_main_function (success, error, correlation_id)
  - initialize_logging (basic fallback covered via create_staging_main_function)

No live Delta or cloud workspace required.  Uses MockPlatformAdapter and
NotebookExecutor to drive all orchestration paths in-process.
"""
from __future__ import annotations

import json
import logging
from dataclasses import asdict
from unittest.mock import MagicMock, patch

import pytest

from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter
from spark_quick_start.orchestration.context_manager import LoadContextManager
from spark_quick_start.orchestration.models import ExecutionMetrics, ExecutionResult
from spark_quick_start.orchestration.notebook_executor import NotebookExecutor
from spark_quick_start.orchestration.staging_pipeline import (
    _resolve_notebook_name,
    create_staging_main_function,
    execute_staging_phases,
)

# ============================================================================
# HELPERS
# ============================================================================

def _null_logger() -> logging.Logger:
    logger = logging.getLogger("test_staging_pipeline_null")
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())
    return logger


def _make_ctx(nb_names=None, parallel=0, max_parallel=None) -> LoadContextManager:
    """Build a minimal parsed LoadContextManager without a live SparkSession."""
    nb_names = nb_names or ["NB_A"]
    stages = [{"nb_name": n} for n in nb_names]
    data = json.dumps({
        "global_context": {
            "id": "ent_1",
            "entity_name": "E",
            "source_system": "S",
            "target_table_path": "/t",
            "staging_table_path": "/s",
        },
        "staging_context": {"stages": stages},
        "scd_context": {"business_key_column": "id"},
        "runtime_context": {
            "correlation_id": "c",
            "pipeline_name": "P",
            "application_name": "A",
            "parallel_execution_enabled": parallel,
            **({"max_parallel_staging": max_parallel} if max_parallel else {}),
        },
        "logging_context": {
            "logging_level": "INFO",
            "log_file_name": "l",
            "log_file_dir": "/tmp",
        },
    })
    ctx = LoadContextManager(data)
    ctx.parse("", "")
    return ctx


# ============================================================================
# TestResolveNotebookName
# ============================================================================

class TestResolveNotebookName:
    def test_returns_adapter_name_when_non_empty(self):
        assert _resolve_notebook_name("MyNotebook", "/tmp/stage__MyNotebook") == "MyNotebook"

    def test_strips_whitespace_from_adapter_name(self):
        assert _resolve_notebook_name("  MyNotebook  ", "/tmp/stage__MyNotebook") == "MyNotebook"

    def test_falls_back_to_path_suffix_when_none(self):
        assert _resolve_notebook_name(None, "/tmp/stage__NB_Stage_Customer") == "NB_Stage_Customer"

    def test_falls_back_to_path_suffix_when_empty_string(self):
        assert _resolve_notebook_name("", "/tmp/stage__NB_Stage_Customer") == "NB_Stage_Customer"

    def test_falls_back_to_path_suffix_when_whitespace(self):
        assert _resolve_notebook_name("   ", "/tmp/stage__NB_Stage_Customer") == "NB_Stage_Customer"

    def test_returns_full_path_when_no_double_underscore(self):
        assert _resolve_notebook_name(None, "/tmp/stage") == "/tmp/stage"

    def test_uses_last_suffix_for_multiple_double_underscores(self):
        assert _resolve_notebook_name(None, "/tmp/base__prefix_NB_Stage") == "prefix_NB_Stage"

    def test_path_prefix_included_in_fallback_value(self):
        assert _resolve_notebook_name(None, "/tmp/stage__customer_NB_Stage") == "customer_NB_Stage"


def _make_error_result(nb_name: str) -> str:
    """Return a serialised ExecutionResult with has_error=True."""
    return ExecutionResult(
        status="error",
        has_error=True,
        error_details={
            "error_type": "RuntimeError",
            "error_message": f"{nb_name} failed",
            "traceback": "",
        },
    ).to_json()


def _make_success_result() -> str:
    return ExecutionResult(
        status="success",
        has_error=False,
        metrics=asdict(ExecutionMetrics(source_count=10, target_count=10)),
    ).to_json()


def _make_stage_context_str() -> str:
    """Minimal stage context JSON accepted by create_staging_main_function."""
    return json.dumps({
        "global_context": {
            "source_system": "SRC",
            "staging_table_path": "/tmp/staging/test",
        },
        "platform_context": {
            "data_storage_lakehouse_path": "/tmp/storage",
            "data_engineering_lakehouse_path": "/tmp/engineering",
        },
        "extensions_context": {"acme.test": {"region": "EU"}},
        "scd_context": {"business_key_column": "id"},
        "staging_context": {"filter_date": "2024-01-01"},
        "logging_context": {
            "logging_level": "INFO",
            "log_file_name": "test_stage",
        },
        "runtime_context": {
            "correlation_id": "corr-test",
            "pipeline_name": "PL_Test",
            "application_name": "EDW",
        },
    })


# ============================================================================
# TestExecuteStagingPhasesSequential
# ============================================================================

class TestExecuteStagingPhasesSequential:
    """Sequential execution mode (parallel_execution_enabled=0)."""

    def test_sequential_single_stage_returns_path(self):
        ctx = _make_ctx(nb_names=["NB_A"])
        adapter = MockPlatformAdapter()
        executor = NotebookExecutor(adapter, _null_logger())

        tables, source_count, target_count = execute_staging_phases(ctx, executor, _null_logger())

        assert len(tables) == 1
        assert tables[0].endswith("__NB_A")
        assert source_count == 10
        assert target_count == 10

    def test_sequential_multi_stage_all_called(self):
        ctx = _make_ctx(nb_names=["NB_A", "NB_B"])
        adapter = MockPlatformAdapter()
        executor = NotebookExecutor(adapter, _null_logger())

        execute_staging_phases(ctx, executor, _null_logger())

        called_names = [call[0] for call in adapter.run_calls]
        assert "NB_A" in called_names
        assert "NB_B" in called_names

    def test_sequential_failure_raises_value_error(self):
        ctx = _make_ctx(nb_names=["NB_A"])
        adapter = MockPlatformAdapter(notebook_results={"NB_A": _make_error_result("NB_A")})
        executor = NotebookExecutor(adapter, _null_logger())

        with pytest.raises(ValueError, match="NB_A"):
            execute_staging_phases(ctx, executor, _null_logger())

    def test_sequential_correct_order(self):
        ctx = _make_ctx(nb_names=["NB_A", "NB_B", "NB_C"])
        adapter = MockPlatformAdapter()
        executor = NotebookExecutor(adapter, _null_logger())

        execute_staging_phases(ctx, executor, _null_logger())

        called_names = [call[0] for call in adapter.run_calls]
        assert called_names == ["NB_A", "NB_B", "NB_C"]


# ============================================================================
# TestExecuteStagingPhasesParallel
# ============================================================================

class TestExecuteStagingPhasesParallel:
    """Parallel execution mode (parallel_execution_enabled=1)."""

    def test_parallel_all_at_once(self):
        """All notebooks dispatched in a single parallel batch."""
        ctx = _make_ctx(nb_names=["NB_A", "NB_B"], parallel=1)
        adapter = MockPlatformAdapter()
        executor = NotebookExecutor(adapter, _null_logger())

        tables, source_count, target_count = execute_staging_phases(ctx, executor, _null_logger())

        called_names = [call[0] for call in adapter.run_calls]
        assert len(tables) == 2
        assert "NB_A" in called_names
        assert "NB_B" in called_names
        assert source_count == 20
        assert target_count == 20

    def test_parallel_batched(self):
        """max_parallel_staging=1 forces two sequential batches of one notebook each."""
        ctx = _make_ctx(nb_names=["NB_A", "NB_B"], parallel=1, max_parallel=1)
        adapter = MockPlatformAdapter()
        executor = NotebookExecutor(adapter, _null_logger())

        tables, source_count, target_count = execute_staging_phases(ctx, executor, _null_logger())

        # Both notebooks must be called; adapter records every run_notebook call
        called_names = [call[0] for call in adapter.run_calls]
        assert len(tables) == 2
        assert "NB_A" in called_names
        assert "NB_B" in called_names
        # Because max_parallel=1 < total=2, execution is batched: 2 batches of 1
        assert len(adapter.run_calls) == 2
        # Counts must accumulate across batches, not just reflect the last one
        assert source_count == 20
        assert target_count == 20


# ============================================================================
# TestCreateStagingMainFunction
# ============================================================================

class TestCreateStagingMainFunction:
    """Factory-produced main() function for staging notebooks."""

    def test_returns_callable(self):
        adapter = MockPlatformAdapter()
        main = create_staging_main_function(lambda *a: None, adapter=adapter)
        assert callable(main)

    def test_success_path(self):
        mock_df = MagicMock()
        mock_df.count.return_value = 2
        mock_df.withColumn.return_value = mock_df

        def create_df(storage_path, staging_context, global_context, nb_name):
            return mock_df

        adapter = MockPlatformAdapter()
        # F.lit requires an active SparkContext — patch it out at the module level.
        mock_lit = MagicMock(return_value=MagicMock())
        with patch(
            "spark_quick_start.orchestration.staging_pipeline.F.lit",
            mock_lit,
        ), patch(
            "spark_quick_start.orchestration.staging_pipeline.write_staging_data",
            return_value=2,
        ), patch(
            "spark_quick_start.orchestration.staging_pipeline.delete_staging_data"
        ):
            main = create_staging_main_function(create_df, adapter=adapter)
            result = main(_make_stage_context_str())

        column_names = [c[0][0] for c in mock_df.withColumn.call_args_list]
        assert "NotebookName" in column_names
        assert "NB_BI_Notebook_Name" not in column_names
        assert result.status == "success"
        assert result.has_error is False
        assert result.metrics["source_count"] == 2

    def test_exception_in_create_df_returns_error(self):
        def create_df(storage_path, staging_context, global_context, nb_name):
            raise RuntimeError("source read failed")

        adapter = MockPlatformAdapter()
        with patch(
            "spark_quick_start.orchestration.staging_pipeline.delete_staging_data"
        ):
            main = create_staging_main_function(create_df, adapter=adapter)
            result = main(_make_stage_context_str())

        assert result.status == "error"
        assert result.has_error is True

    def test_error_details_has_the_pinned_shape(self):
        """F114: error_details is ErrorDetails' dict form, with exactly the keys it always had."""
        def create_df(storage_path, staging_context, global_context, nb_name):
            raise RuntimeError("source read failed")

        with patch("spark_quick_start.orchestration.staging_pipeline.delete_staging_data"):
            main = create_staging_main_function(create_df, adapter=MockPlatformAdapter())
            details = main(_make_stage_context_str()).error_details
        assert set(details) == {"error_type", "error_message", "traceback"}
        assert (details["error_type"], details["error_message"]) == ("RuntimeError", "source read failed")
        assert "RuntimeError: source read failed" in details["traceback"]

    def test_correlation_id_in_result(self):
        mock_df = MagicMock()
        mock_df.count.return_value = 0
        mock_df.withColumn.return_value = mock_df

        def create_df(*args):
            return mock_df

        adapter = MockPlatformAdapter()
        mock_lit = MagicMock(return_value=MagicMock())
        with patch(
            "spark_quick_start.orchestration.staging_pipeline.F.lit",
            mock_lit,
        ), patch(
            "spark_quick_start.orchestration.staging_pipeline.write_staging_data",
            return_value=0,
        ), patch(
            "spark_quick_start.orchestration.staging_pipeline.delete_staging_data"
        ):
            main = create_staging_main_function(create_df, adapter=adapter)
            result = main(_make_stage_context_str())

        assert isinstance(result.correlation_id, str)
        assert len(result.correlation_id) > 0


# ============================================================================
# _process_staging_results — the two failure branches
# ============================================================================

def test_process_staging_results_reports_activity_with_no_result():
    """An activity missing from the results dict is reported, not skipped silently.

    runMultiple can return without an entry for an activity (cancelled, timed
    out, never scheduled). Dropping it would let a staging phase that never ran
    look like a success.
    """
    from spark_quick_start.orchestration.staging_pipeline import _process_staging_results

    logger = MagicMock()
    activities = [{"name": "act1", "path": "NB_Stage_Person"}]

    with pytest.raises(ValueError, match="No result returned"):
        _process_staging_results(results={}, activities=activities, logger=logger)

    logger.error.assert_any_call("No result for: NB_Stage_Person")


def test_process_staging_results_reports_unparseable_result():
    """A result that cannot be parsed is reported with the offending payload.

    NotebookExecutionResult raises when exitVal is absent or not valid JSON —
    the notebook crashed before calling exit(), or exited with something that
    is not an ExecutionResult.
    """
    from spark_quick_start.orchestration.staging_pipeline import _process_staging_results

    logger = MagicMock()
    activities = [{"name": "act1", "path": "NB_Stage_Person"}]
    results = {"act1": {"exitVal": "this is not json"}}

    with pytest.raises(ValueError, match="Could not parse result"):
        _process_staging_results(results=results, activities=activities, logger=logger)

    assert any("Could not parse result" in str(c) for c in logger.error.call_args_list)


def _assert_utc_stamps(metrics: dict) -> None:
    from datetime import UTC, datetime

    for key in ("start_time", "end_time"):
        stamp = datetime.fromisoformat(metrics[key])
        assert stamp.tzinfo is not None, f"{key} is naive: {metrics[key]}"
        assert stamp.utcoffset() == UTC.utcoffset(None), f"{key} is not UTC: {metrics[key]}"


def test_staging_result_timestamps_are_utc_on_success():
    """F115: start_time/end_time are timezone-aware UTC, as LoadOrchestrator.run() stamps them."""
    mock_df = MagicMock()
    mock_df.withColumn.return_value = mock_df

    with patch("spark_quick_start.orchestration.staging_pipeline.F"), patch(
        "spark_quick_start.orchestration.staging_pipeline.write_staging_data", return_value=1
    ), patch("spark_quick_start.orchestration.staging_pipeline.delete_staging_data"):
        main = create_staging_main_function(lambda *a: mock_df, adapter=MockPlatformAdapter())
        result = main(_make_stage_context_str())

    assert result.status == "success"
    _assert_utc_stamps(result.metrics)


def test_staging_result_timestamps_are_utc_on_error():
    """F115: the error path stamps end_time in UTC too."""
    def create_df(*_args):
        raise RuntimeError("source read failed")

    with patch("spark_quick_start.orchestration.staging_pipeline.delete_staging_data"):
        main = create_staging_main_function(create_df, adapter=MockPlatformAdapter())
        result = main(_make_stage_context_str())

    assert result.status == "error"
    _assert_utc_stamps(result.metrics)


# ============================================================================
# platform_context and extensions_context in the staging helper
# ============================================================================


def _run_stage(create_df, stage_context_str):
    frame = MagicMock()
    frame.withColumn.return_value = frame
    create_df.frame = frame
    with patch("spark_quick_start.orchestration.staging_pipeline.F.lit", MagicMock()), patch(
        "spark_quick_start.orchestration.staging_pipeline.write_staging_data", return_value=1
    ), patch("spark_quick_start.orchestration.staging_pipeline.delete_staging_data"):
        return create_staging_main_function(create_df, adapter=MockPlatformAdapter())(stage_context_str)


def test_the_storage_path_comes_from_platform_context():
    seen = {}

    def create_df(storage_path, staging_context, global_context, nb_name):
        seen["storage_path"] = storage_path
        return create_df.frame

    assert _run_stage(create_df, _make_stage_context_str()).status == "success"
    assert seen["storage_path"] == "/tmp/storage"


def test_a_path_left_in_global_context_is_no_longer_read():
    ctx = json.loads(_make_stage_context_str())
    del ctx["platform_context"]
    ctx["global_context"]["data_storage_lakehouse_path"] = "/old/place"
    seen = {}

    def create_df(storage_path, staging_context, global_context, nb_name):
        seen["storage_path"] = storage_path
        return create_df.frame

    assert _run_stage(create_df, json.dumps(ctx)).status == "success"
    assert seen["storage_path"] == ""


def test_a_function_declaring_extensions_context_receives_it():
    seen = {}

    def create_df(storage_path, staging_context, global_context, nb_name, extensions_context):
        seen["extensions"] = extensions_context
        return create_df.frame

    assert _run_stage(create_df, _make_stage_context_str()).status == "success"
    assert seen["extensions"] == {"acme.test": {"region": "EU"}}


def test_a_function_declaring_extensions_context_gets_empty_when_the_notebook_has_none():
    ctx = json.loads(_make_stage_context_str())
    del ctx["extensions_context"]
    seen = {}

    def create_df(storage_path, staging_context, global_context, nb_name, extensions_context=None):
        seen["extensions"] = extensions_context
        return create_df.frame

    assert _run_stage(create_df, json.dumps(ctx)).status == "success"
    assert seen["extensions"] == {}


def test_a_four_parameter_function_is_not_given_extensions():
    def create_df(storage_path, staging_context, global_context, nb_name):
        return create_df.frame

    assert _run_stage(create_df, _make_stage_context_str()).status == "success"
