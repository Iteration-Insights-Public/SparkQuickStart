"""Databricks platform adapter.

Contains: DatabricksAdapter — implements PlatformAdapter using dbutils.
Does NOT contain: orchestration logic, Spark logic, or any non-Databricks platform code.

dbutils resolution strategy
---------------------------
``import dbutils`` works on classic Databricks clusters (DBR ≤ 14) but raises
``ModuleNotFoundError`` on Databricks Serverless (Spark Connect, DBR 15+) where
dbutils is no longer exposed as a Python module.

``_get_dbutils()`` tries three paths in order so the adapter works on both:

1. ``databricks.sdk.runtime.dbutils`` — recommended on Serverless / DBR 15+.
2. ``pyspark.dbutils.DBUtils(spark)`` — fallback using the active SparkSession.
3. ``import dbutils`` — classic clusters (DBR ≤ 14).

The first path that succeeds is returned; a ``RuntimeError`` is raised only if
all three fail, which indicates an unsupported runtime.
"""
from __future__ import annotations

import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from spark_quick_start.adapters.platform_adapter import PlatformAdapter


def _get_dbutils() -> Any:
    """Resolve the dbutils handle for this Databricks runtime.

    Tries:
    1. ``databricks.sdk.runtime`` — Serverless / DBR 15+.
    2. ``pyspark.dbutils.DBUtils`` via the active SparkSession — Spark Connect fallback.
    3. ``import dbutils`` — classic clusters (DBR ≤ 14).

    Returns:
        dbutils handle.

    Raises:
        RuntimeError: if dbutils cannot be obtained on this runtime.
    """
    # Path 1: Databricks SDK (recommended on Serverless)
    try:
        from databricks.sdk.runtime import dbutils  # type: ignore
        return dbutils
    except ImportError:
        pass

    # Path 2: PySpark DBUtils via active SparkSession (Spark Connect)
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark is not None:
            return DBUtils(spark)
    except Exception:
        pass

    # Path 3: Legacy direct import (classic clusters, DBR ≤ 14)
    try:
        import dbutils as _dbu  # type: ignore
        return _dbu
    except ImportError:
        pass

    raise RuntimeError(
        "DatabricksAdapter: could not obtain dbutils on this runtime. "
        "Ensure you are running on a Databricks cluster with the Databricks SDK "
        "(databricks-sdk) or PySpark DBUtils available."
    )


class DatabricksAdapter(PlatformAdapter):
    """Databricks adapter — delegates to dbutils; parallel execution via ThreadPoolExecutor.

    Compatible with classic Databricks clusters and Databricks Serverless / Spark
    Connect. The library needs Python 3.11, so DBR 15 or later (F191); the
    DBR ≤ 14 path in the module docstring stays as a fallback. See there for the
    dbutils resolution strategy.
    """

    def run_notebook(self, name: str, timeout: int, args: dict[str, Any]) -> str:
        dbu = _get_dbutils()
        return dbu.notebook.run(name, timeout, {k: str(v) for k, v in args.items()})

    def run_notebooks_parallel(self, dag: dict[str, Any]) -> dict[str, Any]:
        activities = dag.get("activities", [])
        concurrency = dag.get("concurrency", len(activities))
        per_nb_timeout = dag.get("timeoutInSeconds", 3600)
        results: dict[str, Any] = {}

        with ThreadPoolExecutor(max_workers=max(1, concurrency)) as executor:
            futures = {
                executor.submit(
                    self.run_notebook,
                    act["path"],
                    act.get("timeoutPerCellInSeconds", per_nb_timeout),
                    act.get("args", {}),
                ): act["name"]
                for act in activities
            }
            for future in as_completed(futures):
                act_name = futures[future]
                try:
                    results[act_name] = {"exitVal": future.result()}
                except Exception as exc:
                    # Function-local: adapters never import orchestration at module scope (layering rule 6).
                    from spark_quick_start.orchestration.models import ErrorDetails, ExecutionResult
                    err = ExecutionResult(
                        status="error",
                        has_error=True,
                        error_details=ErrorDetails.from_exception(exc, traceback.format_exc()).to_dict(),
                    )
                    results[act_name] = {"exitVal": err.to_json()}
        return results

    def delete_path(self, path: str) -> None:
        _get_dbutils().fs.rm(path, recurse=True)

    def get_current_notebook_name(self) -> str | None:
        try:
            dbu = _get_dbutils()
            ctx = dbu.notebook.entry_point.getDbutils().notebook().getContext()
            return ctx.notebookPath().get().split("/")[-1]
        except Exception:
            return None

    def notebook_exit(self, value: str) -> None:
        """Call dbutils.notebook.exit with the given value."""
        _get_dbutils().notebook.exit(value)

    def get_log_persistence_adapter(self) -> Any:
        """Return DatabricksPersistenceAdapter for DBFS/Unity Catalog log file writes."""
        from spark_quick_start.spark_logging import DatabricksPersistenceAdapter
        return DatabricksPersistenceAdapter()
