"""
delta_utils.py — Delta Lake write/delete helpers with retry logic.

Contains: run_with_delta_retry, delete_staging_data, write_staging_data.
Does NOT contain: platform-specific code, SCD logic, merge operations,
    or any import from spark_load_orchestration.
"""
from __future__ import annotations

import random
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame

from spark_quick_start.adapters.platform_adapter import PlatformAdapter
from spark_quick_start.orchestration.load_pipeline import capture_write_statistics


def run_with_delta_retry(op: Callable, max_attempts: int = 6, base_sleep: float = 1.5) -> Any:
    """Execute ``op()`` with exponential-backoff retry on Delta concurrent-append conflicts.

    Non-conflict exceptions are re-raised immediately.  On conflict, waits
    ``base_sleep * 2^(attempt-1) + jitter`` seconds before retrying.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            return op()
        except Exception as exc:
            msg = str(exc)
            is_conflict = "DELTA_CONCURRENT_APPEND" in msg or "ConcurrentAppendException" in msg
            if not is_conflict or attempt == max_attempts:
                raise
            sleep_s = base_sleep * (2 ** (attempt - 1)) + random.random()
            time.sleep(sleep_s)


def delete_staging_data(
    path: str,
    adapter: PlatformAdapter,
    logger: Any,
    spark: Any | None = None,
) -> None:
    """Delete a staging Delta table path or skip for a UC table (best-effort).

    For absolute filesystem paths, delegates to ``adapter.delete_path()``.
    For Unity Catalog three-part names (``catalog.schema.table``), the path
    cannot be deleted via filesystem APIs — the subsequent overwrite write
    in ``write_staging_data`` handles cleanup, so this is a documented no-op.

    *path* comes from ``global_context["staging_table_path"]``, and deletion is
    recursive with no undo. When *spark* is supplied, the target is confirmed to
    be a Delta table first and the deletion is refused otherwise — a mistyped
    path, or one copied from another environment, lands on an unrelated
    directory far more often than on a Delta table. Without *spark* the check
    cannot run and the previous unguarded behaviour applies, logged so the gap
    is visible rather than silent.

    The refusal itself relies on a local filesystem existence check, so on a
    URI-scheme path (``abfss://``, ``dbfs:/``) it cannot distinguish "absent"
    from "present but not Delta" and takes the safe branch: nothing is deleted,
    but no error is raised either. Deletion still only ever happens for a
    confirmed Delta table.

    Args:
        path: Absolute Delta path or Unity Catalog three-part name.
        adapter: Platform adapter used for filesystem deletion (path-based only).
        logger: Logger instance.
        spark: Active SparkSession, used to confirm the target is a Delta table.
            Optional so existing callers keep working; pass it to enable the
            guard.

    Raises:
        ValueError: if *path* exists but is not a Delta table.
    """
    if adapter.is_table_identifier(path):
        logger.info(
            f"Staging table is a Unity Catalog name — skipping pre-delete "
            f"(write_staging_data will overwrite): {path}"
        )
        return

    if spark is not None:
        if not adapter.delta_table_exists(spark, path):
            if Path(path).exists():
                raise ValueError(
                    f"Refusing to delete staging path {path!r}: it exists but is "
                    "not a Delta table. This is usually a mistyped "
                    "staging_table_path or a configuration copied from another "
                    "environment. Nothing has been deleted."
                )
            logger.info(f"Staging table does not exist yet, will be created: {path}")
            return
    else:
        logger.info(
            "delete_staging_data called without a SparkSession — deleting "
            f"{path} without confirming it is a Delta table. Pass spark= to "
            "enable that check."
        )

    try:
        adapter.delete_path(path)
        logger.info(f"Deleted staging table: {path}")
    except Exception:
        logger.info(f"Staging table does not exist yet, will be created: {path}")


def write_staging_data(
    df: DataFrame,
    path: str,
    adapter: PlatformAdapter,
    logger: Any,
) -> int:
    """Write a staging DataFrame to a Delta location in overwrite mode.

    Delegates UC-vs-path routing to ``adapter.write_delta_overwrite()``.

    On Spark Connect (Databricks serverless), ``.save()`` only accepts absolute
    filesystem paths. The adapter's ``write_delta_overwrite`` default impl routes
    UC names to ``.saveAsTable()`` which works correctly on Serverless.

    The row count is read from the write's own Delta commit metadata
    (``operationMetrics['numOutputRows']``, via ``capture_write_statistics`` —
    the same mechanism ``NativeSCDAppendOnlyTransformer``'s raw-append WRITE
    path already uses) rather than a separate ``df.count()`` call. This used
    to cache-then-count after the write — a second full pass over the data
    on top of the write itself, and on Databricks Serverless (no PERSIST
    TABLE support) that meant a full *re-execution* of the DataFrame's
    lineage. Reading it from Delta's own commit metadata instead costs
    nothing beyond the write that was happening anyway, and needs no
    serverless/non-serverless branching.

    Args:
        df: DataFrame to write.
        path: Absolute Delta path or Unity Catalog three-part name.
        adapter: Platform adapter that handles UC/path routing.
        logger: Logger instance.

    Returns:
        Number of rows written.
    """
    spark = df.sparkSession
    pre_version = -1
    if adapter.delta_table_exists(spark, path):
        dt = adapter.resolve_delta_table(spark, path)
        pre_version = int(dt.history(1).selectExpr("max(version)").collect()[0][0] or -1)

    adapter.write_delta_overwrite(df, path)

    row_count = capture_write_statistics(spark, path, pre_version, adapter, logger)
    logger.info(f"Written {row_count} rows to: {path}")
    return row_count
