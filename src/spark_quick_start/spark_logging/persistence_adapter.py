"""Platform-neutral log persistence adapter primitives.

Contains: the LogPersistenceAdapter ABC that every concrete adapter
implements, plus the platform-neutral implementations (NullPersistenceAdapter,
CapturingPersistenceAdapter, LogTableAdapter). The real platform integrations
(FabricPersistenceAdapter, DatabricksPersistenceAdapter,
LocalFilePersistenceAdapter) live in their own sibling modules and import
LogPersistenceAdapter from here directly (not from the package root) to
avoid a circular import with spark_logging/__init__.py.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@dataclass
class LogRow:
    """A single log session row written to a Delta table by ``LogTableAdapter``."""
    run_id: str
    success: bool
    log_timestamp: datetime
    correlation_id: str | None = None
    pipeline_name: str | None = None
    notebook_name: str | None = None
    python_logs: str = ""       # JSON array string
    stdout_tail: str = ""
    stderr_tail: str = ""
    spark_driver_tail: str = ""
    context: str = "{}"         # JSON dict string
    error: str | None = None # JSON dict string or None


# ===========================================================================
# LOG PERSISTENCE ADAPTER
# ===========================================================================

class LogPersistenceAdapter(ABC):
    """Abstract adapter for writing log artifacts to a filesystem.

    Implement for each target platform so ``LogSession`` can persist logs
    without platform-specific code.

    Example — Fabric::

        adapter = FabricPersistenceAdapter()
        sess = init_logs(lakehouse_dir="Files/Logs", adapter=adapter)

    Example — Databricks::

        adapter = DatabricksPersistenceAdapter()
        sess = init_logs(lakehouse_dir="dbfs:/Logs", adapter=adapter)

    Example — Local / testing::

        adapter = LocalFilePersistenceAdapter()
        sess = init_logs(lakehouse_dir="/tmp/logs", adapter=adapter)
    """

    @abstractmethod
    def mkdir(self, path: str) -> None:
        """Create directory (and parents). Best-effort; ignore if already exists."""

    @abstractmethod
    def write(self, path: str, content: str) -> None:
        """Write *content* as UTF-8 text to *path*, overwriting if present."""

    @abstractmethod
    def copy(self, src: str, dst: str) -> None:
        """Copy *src* to *dst* (single file, non-recursive)."""

    def notebook_exit(self, value: str) -> None:
        """Call the platform notebook exit. Default no-op — override if needed."""

    def get_notebook_name(self) -> str | None:
        """Return the current notebook name, or None if not detectable."""
        return None

    def get_spark_session(self) -> Any | None:
        """Return a platform-provided SparkSession, or None to require explicit passing."""
        return None


class NullPersistenceAdapter(LogPersistenceAdapter):
    """No-op adapter for in-memory sessions — no files are written."""

    def mkdir(self, path: str) -> None:
        pass

    def write(self, path: str, content: str) -> None:
        pass

    def copy(self, src: str, dst: str) -> None:
        pass

    def notebook_exit(self, value: str) -> None:
        pass


class CapturingPersistenceAdapter(LogPersistenceAdapter):
    """Test adapter — captures all writes in memory for inspection.

    Example::

        adapter = CapturingPersistenceAdapter()
        sess = init_logs(lakehouse_dir="/logs", adapter=adapter)
        ...
        finalize_logs()
        assert "/logs" in adapter.dirs
        data = json.loads(list(adapter.written.values())[0])
    """

    def __init__(self) -> None:
        self.dirs: list[str] = []
        self.written: dict[str, str] = {}
        self.copies: list[tuple[str, str]] = []
        self.exit_values: list[str] = []
        self.finalized_rows: list[LogRow] = []

    def mkdir(self, path: str) -> None:
        self.dirs.append(path)

    def write(self, path: str, content: str) -> None:
        self.written[path] = content

    def copy(self, src: str, dst: str) -> None:
        self.copies.append((src, dst))

    def notebook_exit(self, value: str) -> None:
        self.exit_values.append(value)

    def finalize_to_table(self, row: LogRow) -> None:
        self.finalized_rows.append(row)


class LogTableAdapter(LogPersistenceAdapter):
    """Delta table adapter — appends one row per session to a Delta table on finalize.

    File-based methods (``mkdir``, ``write``, ``copy``) are no-ops; all log data
    is persisted via ``finalize_to_table()``.

    Example::

        from pyspark.sql import SparkSession
        from spark_quick_start.spark_logging import LogTableAdapter, init_logs

        spark = SparkSession.builder.getOrCreate()
        adapter = LogTableAdapter(spark, "/path/to/logs/delta")
        sess = init_logs(name_prefix="my_pipeline", adapter=adapter)

    Delta schema
    ------------
    run_id, correlation_id, pipeline_name, notebook_name : StringType (nullable)
    success : BooleanType
    log_timestamp : TimestampType
    python_logs, stdout_tail, stderr_tail, spark_driver_tail, context : StringType (JSON)
    error : StringType (nullable JSON)
    """

    def __init__(self, spark: SparkSession, table_path: str) -> None:
        self._spark = spark
        self.table_path = table_path

    def mkdir(self, path: str) -> None:
        pass  # Delta table auto-created on first write

    def write(self, path: str, content: str) -> None:
        pass  # not used; all data written via finalize_to_table

    def copy(self, src: str, dst: str) -> None:
        pass  # not used

    def get_spark_session(self) -> Any:
        return self._spark

    def finalize_to_table(self, row: LogRow) -> None:
        """Append *row* as a single Delta table record."""
        from pyspark.sql.types import (  # lazy import — Delta not available in all envs
            BooleanType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        schema = StructType([
            StructField("run_id", StringType(), nullable=False),
            StructField("correlation_id", StringType(), nullable=True),
            StructField("pipeline_name", StringType(), nullable=True),
            StructField("notebook_name", StringType(), nullable=True),
            StructField("success", BooleanType(), nullable=False),
            StructField("log_timestamp", TimestampType(), nullable=False),
            StructField("python_logs", StringType(), nullable=True),
            StructField("stdout_tail", StringType(), nullable=True),
            StructField("stderr_tail", StringType(), nullable=True),
            StructField("spark_driver_tail", StringType(), nullable=True),
            StructField("context", StringType(), nullable=True),
            StructField("error", StringType(), nullable=True),
        ])
        row_dict = {
            "run_id": row.run_id,
            "correlation_id": row.correlation_id,
            "pipeline_name": row.pipeline_name,
            "notebook_name": row.notebook_name,
            "success": row.success,
            "log_timestamp": row.log_timestamp,
            "python_logs": row.python_logs,
            "stdout_tail": row.stdout_tail,
            "stderr_tail": row.stderr_tail,
            "spark_driver_tail": row.spark_driver_tail,
            "context": row.context,
            "error": row.error,
        }
        df = self._spark.createDataFrame([row_dict], schema=schema)
        df.write.format("delta").mode("append").save(self.table_path)
