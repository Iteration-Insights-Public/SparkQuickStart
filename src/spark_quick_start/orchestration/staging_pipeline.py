"""
staging_pipeline.py — Staging phase orchestration.

Contains: execute_staging_phases, create_staging_main_function,
    initialize_logging, and private helpers _build_dag,
    _execute_sequential, _process_staging_results.
Does NOT contain: EDW load pipeline logic, platform-specific imports,
    or any import from spark_load_orchestration.

Usage (load orchestrator)::

    executor = NotebookExecutor(adapter, logger)
    staging_tables, source_count, target_count = execute_staging_phases(ctx_manager, executor, logger)

Usage (staging notebook)::

    main = create_staging_main_function(create_staging_dataframe, adapter=adapter)
    result = main(stage_context_str)
"""
from __future__ import annotations

import inspect
import json
import logging
import sys
import traceback
import uuid
from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

import pyspark.sql.functions as F

from spark_quick_start.adapters.platform_adapter import PlatformAdapter
from spark_quick_start.logger import OrchestratorLogger
from spark_quick_start.orchestration.context_manager import LoadContextManager, parse_context
from spark_quick_start.orchestration.delta_utils import (
    delete_staging_data,
    run_with_delta_retry,
    write_staging_data,
)
from spark_quick_start.orchestration.models import ErrorDetails, ExecutionMetrics, ExecutionResult
from spark_quick_start.orchestration.notebook_executor import (
    NotebookExecutionResult,
    NotebookExecutor,
)

__all__ = [
    "create_staging_main_function",
    "execute_staging_phases",
    "initialize_logging",
]


# ============================================================================
# LOGGING INITIALISATION  (platform-agnostic fallback)
# ============================================================================

def initialize_logging(
    logging_context: dict[str, Any],
    correlation_id: str,
    staging_context: dict[str, Any],
    log_init_fn: Callable | None = None,
) -> Any:
    """Initialise logging for a staging or load notebook.

    If *log_init_fn* is provided (e.g. ``init_logs`` from spark_logging) it is
    called with in-memory mode so logs can be returned to the parent notebook;
    the context's ``log_file_dir`` goes in as ``lakehouse_dir``, the name
    ``init_logs`` gives it (in-memory mode ignores it).
    Otherwise falls back to a standard Python ``logging.Logger``.

    Returns a logger instance.
    """
    if log_init_fn is not None:
        kwargs: dict[str, Any] = dict(
            name_prefix=logging_context.get("log_file_name", "EDW_Stage"),
            level=logging_context.get("logging_level", "INFO"),
            capture_spark=False,
            capture_stdout=True,
            in_memory=True,
            correlation_id=correlation_id,
            context=staging_context,
        )
        if logging_context.get("log_file_dir") is not None:
            kwargs["lakehouse_dir"] = logging_context["log_file_dir"]
        log_init_fn(**kwargs)
        # spark_logging registers the logger globally; return a standard logger with the same name
        import logging as _logging
        return _logging.getLogger(logging_context.get("log_file_name", "EDW_Stage"))

    name = logging_context.get("log_file_name", "EDW_Stage")
    level = logging_context.get("logging_level", "INFO")
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    if not logger.handlers:
        logger.addHandler(logging.StreamHandler(sys.stdout))
    return logger


def _resolve_notebook_name(
    current_nb_name: str | None,
    staging_table_path: str,
) -> str:
    """Return a non-empty notebook identifier for the NotebookName column.

    Resolution order:
      1. adapter.get_current_notebook_name() result if non-empty.
      2. The suffix after the last '__' in staging_table_path.
      3. The full staging_table_path as a last resort.
    """
    if current_nb_name and current_nb_name.strip():
        return current_nb_name.strip()
    if "__" in staging_table_path:
        return staging_table_path.rsplit("__", 1)[-1]
    return staging_table_path


# ============================================================================
# STAGING MAIN FUNCTION FACTORY
# ============================================================================

def _accepts_keyword(fn: Callable[..., Any], name: str) -> bool:
    """Whether a staging function declares a parameter *name* that can be passed by keyword.

    A function taking only ``**kwargs`` is not asked, so it keeps receiving exactly its four
    arguments. A callable whose signature cannot be read is treated as four-parameter.
    """
    try:
        parameter = inspect.signature(fn).parameters.get(name)
    except (TypeError, ValueError):
        return False
    return parameter is not None and parameter.kind is not inspect.Parameter.POSITIONAL_ONLY


def _accepts_hwm_context(fn: Callable[..., Any]) -> bool:
    """Whether a staging function asks for the load's `hwm_context` (F138, decision 9)."""
    return _accepts_keyword(fn, "hwm_context")


def create_staging_main_function(
    create_staging_dataframe_fn: Callable,
    adapter: PlatformAdapter | None = None,
    log_init_fn: Callable | None = None,
    get_logs_fn: Callable | None = None,
    spark: Any | None = None,
) -> Callable[[str], ExecutionResult]:
    """Factory that wraps staging business logic in standardised orchestration.

    Args:
        create_staging_dataframe_fn: Business logic with signature
            ``(storage_path, staging_context, global_context, notebook_name) -> DataFrame``.
            ``storage_path`` is the notebook's ``platform_context.data_storage_lakehouse_path``.
            A function that also declares a parameter named ``hwm_context`` or
            ``extensions_context``, passable by keyword, is given the notebook's section of that
            name (``extensions_context`` is ``{}`` when the notebook has none).
            Must NOT include SCD columns — they are added downstream — except the deleted
            flag column, which a source that marks deleted rows may stage as the delete
            signal (F127).
            Must NOT include ``Constants.EXCLUDE_COLUMNS`` — they are added here.
        adapter: Platform adapter.  Defaults to ``FabricAdapter()``. A build
            without ``fabric_adapter.py`` (the Databricks-filtered release, or a
            source copy stripped the same way) cannot construct that default, so
            pass ``adapter=`` explicitly on any non-Fabric platform.
        spark: Optional SparkSession. When supplied, the staging path is
            confirmed to be a Delta table before the pre-delete removes it —
            see ``delta_utils.delete_staging_data``. Staging notebooks already
            have ``spark`` in scope, so passing it is a one-word change.
        log_init_fn: Optional ``init_logs`` from spark_logging for rich capture.
        get_logs_fn: Optional ``get_logs_json`` from spark_logging.

    Returns:
        ``main(stage_context_str: str) -> ExecutionResult``
    """
    if adapter is None:
        from spark_quick_start.adapters.fabric_adapter import FabricAdapter
        _adapter: PlatformAdapter = FabricAdapter()
    else:
        _adapter = adapter

    def _get_logs() -> str:
        if get_logs_fn:
            try:
                return get_logs_fn()
            except Exception:
                pass
        return ""

    passes_hwm_context = _accepts_hwm_context(create_staging_dataframe_fn)
    passes_extensions_context = _accepts_keyword(create_staging_dataframe_fn, "extensions_context")

    def main(stage_context_str: str) -> ExecutionResult:
        start_time = datetime.now(UTC)
        metrics = ExecutionMetrics(start_time=start_time.isoformat())
        correlation_id = str(uuid.uuid4())
        logger = None

        try:
            (global_context, staging_context, logging_context,
             _runtime_context, correlation_id, current_nb_name) = parse_context(
                stage_context_str, _adapter
            )

            logger = initialize_logging(logging_context, correlation_id, staging_context, log_init_fn)
            logger.info(f"Starting staging: {current_nb_name} | correlation_id={correlation_id}")

            full_context = json.loads(stage_context_str)
            storage_path = full_context.get("platform_context", {}).get("data_storage_lakehouse_path", "")
            staging_table = global_context["staging_table_path"]

            run_with_delta_retry(
                lambda: delete_staging_data(staging_table, _adapter, logger, spark=spark)
            )

            # F138: the load's hwm_context, to a function whose signature asks for it.
            extra: dict[str, Any] = {}
            if passes_hwm_context:
                extra["hwm_context"] = full_context.get("hwm_context")
            if passes_extensions_context:
                extra["extensions_context"] = full_context.get("extensions_context", {})
            staging_df = create_staging_dataframe_fn(
                storage_path, staging_context, global_context, current_nb_name, **extra
            ).withColumn("NotebookName", F.lit(_resolve_notebook_name(current_nb_name, staging_table)))

            # staging is an unfiltered full overwrite of staging_df, so the write's
            # own row count (read for free from Delta commit metadata, see
            # write_staging_data) equals both source_count and target_count —
            # no separate .count() needed.
            target_count = run_with_delta_retry(
                lambda: write_staging_data(staging_df, staging_table, _adapter, logger)
            )
            metrics.source_count = target_count
            metrics.target_count = target_count
            logger.info(f"Source records: {target_count}")
            if target_count == 0:
                logger.warning("No source records found — may be expected for incremental loads")

            end_time = datetime.now(UTC)
            metrics.end_time = end_time.isoformat()
            metrics.duration_seconds = (end_time - start_time).total_seconds()
            logger.info(f"Staging complete: {target_count} rows in {metrics.duration_seconds:.2f}s")

            return ExecutionResult(
                status="success",
                has_error=False,
                metrics=asdict(metrics),
                logs=_get_logs(),
                correlation_id=correlation_id,
            )

        except Exception as exc:
            tb = traceback.format_exc()
            if logger:
                logger.error(f"Staging failed: {type(exc).__name__} — {exc}\n{tb}")
            end_time = datetime.now(UTC)
            metrics.end_time = end_time.isoformat()
            metrics.duration_seconds = (end_time - start_time).total_seconds()
            return ExecutionResult(
                status="error",
                has_error=True,
                error_details=ErrorDetails.from_exception(exc, tb).to_dict(),
                metrics=asdict(metrics),
                logs=_get_logs(),
                correlation_id=correlation_id,
            )

    return main


# ============================================================================
# STAGING PHASE ORCHESTRATION
# ============================================================================

def _build_dag(
    ctx_manager: LoadContextManager,
    timeout_seconds: int,
    logger: Any,
    stage_indices: list[int] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[str]]:
    """Build the DAG dict, activity list, and staging table paths for parallel execution."""
    stages = ctx_manager.staging_ctx.stages
    if stage_indices is None:
        stage_indices = list(range(len(stages)))

    activities: list[dict[str, Any]] = []
    staging_tables: list[str] = []

    for idx in stage_indices:
        nb_name = stages[idx]["nb_name"]
        _path = stages[idx].get("path")
        nb_path = f"{_path}/{nb_name}" if _path else nb_name
        activity_name = f"Stage_{idx + 1}_{nb_name}"
        phase_context = ctx_manager.build_staging_phase_context(idx, nb_name)
        staging_tables.append(phase_context["global_context"]["staging_table_path"])
        activities.append({
            "name": activity_name,
            "path": nb_path,
            "timeoutPerCellInSeconds": timeout_seconds,
            "args": {
                "stage_context_str": json.dumps(phase_context, default=str, ensure_ascii=False)
            },
        })
        logger.info(f"Prepared stage {idx + 1}: {nb_name}")

    dag = {
        "activities": activities,
        "timeoutInSeconds": timeout_seconds * max(len(activities), 1) * 2,
        "concurrency": len(activities),
    }
    return dag, activities, staging_tables


def _execute_sequential(
    ctx_manager: LoadContextManager,
    executor: NotebookExecutor,
    timeout_seconds: int,
    logger: Any,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[str]]:
    """Execute staging notebooks one-by-one. Returns (results, activities, staging_tables)."""
    results: dict[str, Any] = {}
    activities: list[dict[str, Any]] = []
    staging_tables: list[str] = []
    total = len(ctx_manager.staging_ctx.stages)

    for idx, stage in enumerate(ctx_manager.staging_ctx.stages):
        nb_name = stage["nb_name"]
        _path = stage.get("path")
        nb_path = f"{_path}/{nb_name}" if _path else nb_name
        activity_name = f"Stage_{idx + 1}_{nb_name}"
        phase_context = ctx_manager.build_staging_phase_context(idx, nb_name)
        staging_tables.append(phase_context["global_context"]["staging_table_path"])

        activity: dict[str, Any] = {
            "name": activity_name,
            "path": nb_path,
            "timeoutPerCellInSeconds": timeout_seconds,
            "args": {
                "stage_context_str": json.dumps(phase_context, default=str, ensure_ascii=False)
            },
        }
        activities.append(activity)
        logger.info(f"Executing stage {idx + 1}/{total}: {nb_name}")

        exit_val = executor.run_single(nb_path, timeout_seconds, activity["args"])
        results[activity_name] = {"exitVal": exit_val}

    return results, activities, staging_tables


_STAGING_METRIC_KEYS = {"source_count", "target_count", "duration_seconds"}


def _process_staging_results(
    results: dict[str, Any],
    activities: list[dict[str, Any]],
    logger: Any,
) -> tuple[int, int]:
    """Parse notebook results, log metrics, and raise ValueError if any notebook failed.

    Returns:
        (source_count, target_count) summed across all activities in this
        batch — each staging notebook's ``main()`` already computes these
        for free (see ``write_staging_data``); this just stops discarding
        them after logging.
    """
    failed: list[dict[str, Any]] = []
    source_count = 0
    target_count = 0

    for activity in activities:
        act_name = activity["name"]
        nb_name = activity["path"]

        if act_name not in results:
            failed.append({"notebook": nb_name, "error": "No result returned", "logs": ""})
            logger.error(f"No result for: {nb_name}")
            continue

        try:
            res = NotebookExecutionResult(nb_name, results[act_name])
            if res.has_error:
                logger.error(f"Failed: {nb_name}\n{res.format_error_details()}")
                failed.append({"notebook": nb_name, "error": res.format_error_details(), "logs": res.logs_str})
            else:
                source_count += res.get_metric("source_count", 0) or 0
                target_count += res.get_metric("target_count", 0) or 0
                if isinstance(logger, OrchestratorLogger):
                    logger.info(
                        "staging_notebook",
                        tags={"event": "completed", "notebook": nb_name},
                        metrics=[
                            {"metricName": k, "metricValue": v}
                            for k, v in res.metrics.items()
                            if k in _STAGING_METRIC_KEYS and isinstance(v, (int, float))
                        ],
                    )
                else:
                    logger.info(f"Completed: {nb_name} | {res.format_metrics()}")
        except Exception as exc:
            err_msg = f"Could not parse result: {exc}"
            logger.error(f"{nb_name}: {err_msg}")
            failed.append({"notebook": nb_name, "error": err_msg, "logs": str(results[act_name])[:500]})

    logger.info(f"Staging summary — success: {len(activities) - len(failed)}, failed: {len(failed)}")

    if failed:
        summary = f"Staging failed: {len(failed)} notebook(s)\n" + "\n".join(
            f"  {f['notebook']}: {f['error']}" for f in failed
        )
        raise ValueError(summary)

    return source_count, target_count


def execute_staging_phases(
    ctx_manager: LoadContextManager,
    executor: NotebookExecutor,
    logger: Any,
) -> tuple[list[str], int, int]:
    """Execute all staging phases and return the staging Delta table paths + counts.

    Execution mode is driven by ``runtime_context.parallel_execution_enabled`` and
    ``runtime_context.max_parallel_staging``:

    * Sequential (default): one notebook at a time.
    * Parallel (all at once): all notebooks in one DAG call.
    * Batched parallel: groups of *N* notebooks (``max_parallel_staging``).

    Args:
        ctx_manager: Parsed and validated load context manager.
        executor: NotebookExecutor instance wrapping a PlatformAdapter.
        logger: Logger instance for orchestration events.

    Returns:
        (staging_table_paths, source_count, target_count) — one path per
        stage; the two counts are summed across all stages from each
        staging notebook's own metrics (see ``write_staging_data`` — free,
        no extra ``.count()``).

    Raises:
        ValueError: if any staging notebook reports an error.
    """
    parallel = ctx_manager.runtime_ctx.parallel_execution_enabled
    timeout = ctx_manager.global_ctx.notebook_timeout_seconds
    max_parallel = ctx_manager.runtime_ctx.max_parallel_staging
    total = len(ctx_manager.staging_ctx.stages)

    mode = "PARALLEL" if parallel and total > 1 else "SEQUENTIAL"
    logger.info(f"Executing {total} staging phase(s) — mode: {mode}")
    if parallel and max_parallel:
        logger.info(f"Max concurrent notebooks: {max_parallel}")

    all_tables: list[str] = []
    total_source_count = 0
    total_target_count = 0

    if parallel and total > 1:
        if max_parallel and max_parallel < total:
            # Batched parallel
            for batch_start in range(0, total, max_parallel):
                batch_indices = list(range(batch_start, min(batch_start + max_parallel, total)))
                logger.info(f"Batch {batch_start // max_parallel + 1}: stages {[i + 1 for i in batch_indices]}")
                dag, activities, tables = _build_dag(ctx_manager, timeout, logger, batch_indices)
                batch_results = executor.run_parallel(dag)
                batch_source, batch_target = _process_staging_results(batch_results, activities, logger)
                total_source_count += batch_source
                total_target_count += batch_target
                all_tables.extend(tables)
        else:
            # All at once
            dag, activities, tables = _build_dag(ctx_manager, timeout, logger)
            results = executor.run_parallel(dag)
            total_source_count, total_target_count = _process_staging_results(results, activities, logger)
            all_tables = tables
    else:
        results, activities, tables = _execute_sequential(ctx_manager, executor, timeout, logger)
        total_source_count, total_target_count = _process_staging_results(results, activities, logger)
        all_tables = tables

    logger.info("All staging phases completed")
    return all_tables, total_source_count, total_target_count
