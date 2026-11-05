"""
spark_logging/__init__.py — reusable logging for Fabric, Databricks and local notebooks.

Features
- Init → work → finalize (context manager or scattered cells)
- Captures:
  * Python logging records (structured)
  * stdout/stderr tails
  * Spark DRIVER (Log4j) logs (executors not programmatically captured)
- Unless ``in_memory=True``, persists the full combined JSON through the
  ``LogPersistenceAdapter`` you pass (``FabricPersistenceAdapter``,
  ``DatabricksPersistenceAdapter``, ``LocalFilePersistenceAdapter`` or
  ``LogTableAdapter``) under ``lakehouse_dir``; ``in_memory=True`` keeps the
  logs in memory and returns them as JSON instead
- Robust failure capture: records exception type, message, and full traceback;
  auto-finalizes on any unhandled cell error (multiple hooks); idempotent.

Typical use in a payload notebook (scattered cells, persisted; Fabric shown —
pass the persistence adapter for your platform):
    from spark_quick_start.spark_logging import (
        FabricPersistenceAdapter, finalize_logs, get_logger, init_logs,
        install_error_capture,
    )
    sess = init_logs(lakehouse_dir="Files/Logs", name_prefix="orders_ingest",
                     level="INFO", capture_spark=True, capture_stdout=True,
                     adapter=FabricPersistenceAdapter())
    install_error_capture(auto_exit=False)  # finalize + save traceback on any cell failure

    log = get_logger()
    log.info("doing work...")

    # auto_exit=True hands the summary to the adapter's notebook_exit()
    summary = finalize_logs(success=True, auto_exit=True)

Typical use in a payload notebook (in-memory mode for returning logs to parent):
    from spark_quick_start.spark_logging import get_logger, get_logs_json, init_logs
    sess = init_logs(name_prefix="orders_ingest", level="INFO",
                     capture_spark=False, capture_stdout=True, in_memory=True)

    log = get_logger()
    log.info("processing orders...")

    # When done, retrieve logs as JSON to return to parent
    logs_json = get_logs_json()
    # Return logs_json to parent calling routine

Package layout
--------------
This is a package (not a flat module) so platform-specific persistence
adapters can be excluded per platform-filtered release build:
    persistence_adapter.py     — LogPersistenceAdapter (ABC) + platform-neutral
                                  adapters (Null/Capturing/LogTable)
    fabric_persistence.py      — FabricPersistenceAdapter
    databricks_persistence.py  — DatabricksPersistenceAdapter
    local_persistence.py       — LocalFilePersistenceAdapter
Everything else (LogSession, the singleton helpers, error capture) stays
here because they share the ``_CURRENT`` module global — splitting them into
a submodule would break ``global _CURRENT`` aliasing across module
boundaries.
"""

from __future__ import annotations

import contextlib
import io
import json
import logging
import os
import sys
import time
import uuid
import warnings
from dataclasses import dataclass
from datetime import UTC as _UTC  # private: the package's names are its API (F121)
from datetime import datetime
from typing import Any

from spark_quick_start.spark_logging.persistence_adapter import (
    CapturingPersistenceAdapter as CapturingPersistenceAdapter,
)
from spark_quick_start.spark_logging.persistence_adapter import (
    LogPersistenceAdapter,
    LogRow,
    NullPersistenceAdapter,
)
from spark_quick_start.spark_logging.persistence_adapter import (
    LogTableAdapter as LogTableAdapter,
)

# The public surface (F121). A persistence adapter joins it only when its module imports:
# a platform-filtered build deletes the other platforms' files, as in adapters/__init__.py.
__all__ = [
    "CapturingPersistenceAdapter",
    "LogArtifacts",
    "LogPersistenceAdapter",
    "LogRow",
    "LogSession",
    "LogTableAdapter",
    "NullPersistenceAdapter",
    "finalize_logs",
    "get_logger",
    "get_logs",
    "get_logs_json",
    "init_logs",
    "install_error_capture",
    "python_to_spark_level",
]

try:
    from spark_quick_start.spark_logging.fabric_persistence import FabricPersistenceAdapter
except ImportError:
    FabricPersistenceAdapter = None  # type: ignore[assignment,misc]
else:
    __all__.append("FabricPersistenceAdapter")

try:
    from spark_quick_start.spark_logging.databricks_persistence import DatabricksPersistenceAdapter
except ImportError:
    DatabricksPersistenceAdapter = None  # type: ignore[assignment,misc]
else:
    __all__.append("DatabricksPersistenceAdapter")

try:
    from spark_quick_start.spark_logging.local_persistence import LocalFilePersistenceAdapter
except ImportError:
    LocalFilePersistenceAdapter = None  # type: ignore[assignment,misc]
else:
    __all__.append("LocalFilePersistenceAdapter")


def _level_from_any(level: Any) -> int:
    if isinstance(level, int):
        return level
    if isinstance(level, str):
        return getattr(logging, level.upper(), logging.INFO)
    return logging.INFO


def _now_strings():
    """Return the current UTC time and the ``%Y%m%d_T_%H%M%S`` stamp of the run's base name.

    UTC, not the process time zone, so a log's name sorts the same on every cluster.
    """
    now = datetime.now(_UTC)
    return now, now.strftime("%Y%m%d_T_%H%M%S")


@dataclass
class LogArtifacts:
    base_name: str
    run_id: str
    python_json_path: str | None = None
    spark_driver_log_path: str | None = None


# ===========================================================================
# SPARK LOG CAPTURE  (platform-agnostic helpers)
# ===========================================================================

def _setup_spark_log_capture(
    spark: Any,
    base_name: str,
    level_str: str,
) -> tuple[Any | None, str | None]:
    """Attach a Log4j file appender to the Spark driver JVM logger.

    Tries Log4j 1.x first (Fabric / managed Spark), then warns for Log4j 2.x
    (Databricks runtimes).  Returns ``(appender, log_path)`` or ``(None, None)``.
    """
    log_path = f"/tmp/{base_name}_spark_driver.log"
    spark.sparkContext.setLogLevel(level_str)
    jvm = spark._jvm

    # --- Log4j 1.x (Fabric / legacy Spark) ---
    try:
        LogManager = jvm.org.apache.log4j.LogManager
        PatternLayout = jvm.org.apache.log4j.PatternLayout
        FileAppender = jvm.org.apache.log4j.FileAppender
        Level = jvm.org.apache.log4j.Level
        level_map = {
            "FATAL": Level.FATAL, "ERROR": Level.ERROR, "WARN": Level.WARN,
            "INFO": Level.INFO, "DEBUG": Level.DEBUG, "ALL": Level.ALL,
        }
        jvm_level = level_map.get(level_str, Level.INFO)
        layout = PatternLayout("%d{yyyy-MM-dd HH:mm:ss.SSS} %-5p %c - %m%n")
        app = FileAppender(layout, log_path, True)
        app.setThreshold(jvm_level)
        app.setImmediateFlush(True)
        app.activateOptions()
        root = LogManager.getRootLogger()
        root.setLevel(jvm_level)
        root.addAppender(app)
        for cat in ("org.apache.spark", "org.apache.hadoop", "org.sparkproject", "akka"):
            jvm.org.apache.log4j.Logger.getLogger(cat).setLevel(jvm_level)
        return app, log_path
    except Exception:
        pass

    # --- Log4j 2.x (Databricks / newer runtimes) ---
    # Full file-appender setup requires programmatic context reconfiguration.
    # Level-only control is available; file capture is not supported automatically.
    try:
        LogManager2 = jvm.org.apache.logging.log4j.LogManager
        logging.getLogger(__name__).info(
            "Spark driver: Log4j 2.x detected — log level set, file capture unavailable. "
            "Use Databricks cluster log delivery for driver logs."
        )
    except Exception:
        pass

    return None, None


def _stop_spark_log_capture(
    appender: Any | None,
    log_path: str | None,
) -> str | None:
    """Close the file appender and return the captured log text (or None)."""
    if appender is None:
        return None
    try:
        appender.close()
    except Exception:
        pass
    if log_path and os.path.exists(log_path):
        try:
            with open(log_path, encoding="utf-8", errors="replace") as fh:
                return fh.read()
        except Exception:
            pass
    return None


# ===========================================================================
# BUFFER HANDLER
# ===========================================================================

class _ListBufferHandler(logging.Handler):
    """Collect logging records in-memory as JSON-serializable dicts, with context.

    Args:
        static_ctx: Fields merged into every entry (e.g. ``correlation_id``).
        ctx_keys: Per-record keys to pull from ``logging.extra`` when present.
        level: Minimum log level to capture.
        strip_fields: Field names to remove from each entry after all other
            accumulation. Use ``frozenset({"run_id", "correlation_id", "name"})``
            in OrchestratorLogger-managed sessions where these are redundant with
            the payload root. Defaults to ``frozenset()`` (no stripping).
    """

    def __init__(
        self,
        static_ctx: dict[str, Any],
        ctx_keys: list[str],
        level=logging.INFO,
        strip_fields: frozenset = frozenset(),
    ):
        super().__init__(_level_from_any(level))
        self.buffer: list[dict[str, Any]] = []
        self.static_ctx = static_ctx
        self.ctx_keys = set(ctx_keys)
        self.strip_fields = strip_fields
        formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        # UTC like the payload's ``timestamp`` field; the default is the process zone.
        formatter.converter = time.gmtime
        self.setFormatter(formatter)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            asctime = self.formatter.formatTime(record, self.formatter.datefmt)
            asctime = f"{asctime}.{int(record.msecs):03d}Z"  # Z: UTC

            item = {
                "asctime": asctime,
                "levelname": record.levelname,
                "name": record.name,
                "message": record.getMessage(),
            }

            # Merge static context
            if self.static_ctx:
                item.update(self.static_ctx)

            # Pick per-record context keys if present
            if self.ctx_keys:
                for k in self.ctx_keys:
                    if hasattr(record, k):
                        item[k] = getattr(record, k)

            # Explicit tags and metrics — handled before the simple-type catch-all
            # because dicts and lists are not simple types and would be skipped below.
            _tags = getattr(record, "tags", None)
            if isinstance(_tags, dict) and _tags:
                item["tags"] = _tags

            _metrics = getattr(record, "metrics", None)
            if isinstance(_metrics, list) and _metrics:
                item["metrics"] = _metrics

            # Additionally, preserve any custom extras users send (small, safe subset).
            # "tags" and "metrics" are excluded here because they are handled explicitly
            # above and must not fall through as wrong-type simple values.
            for k, v in getattr(record, "__dict__", {}).items():
                if k not in item and k not in ("args", "msg", "message", "exc_info", "exc_text",
                                               "filename", "funcName", "levelno", "lineno",
                                               "module", "msecs", "pathname", "process",
                                               "processName", "relativeCreated", "stack_info",
                                               "thread", "threadName", "created",
                                               "tags", "metrics"):
                    # Only attach JSON-serializable simple types
                    if isinstance(v, (str, int, float, bool)) or v is None:
                        item[k] = v

            # Strip redundant fields last — guaranteed removal even if re-added above.
            for f in self.strip_fields:
                item.pop(f, None)

            self.buffer.append(item)
        except Exception:
            # Never let logging crash user code
            pass


_DEFAULT_MAX_RETURN_CHARS = 250_000
_DEFAULT_ALWAYS_PERSIST = True


def _warn_unread_parameters(
    max_return_chars: int, always_persist: bool, *, stacklevel: int
) -> None:
    """Warn for each deprecated, never-read parameter given a non-default value.

    Compares to the default rather than using a sentinel: an explicit default
    (every existing ``always_persist=True``) asks for what the library already
    does, so there is nothing to migrate. ``stacklevel=3`` makes the warning
    name the line that called ``LogSession(...)`` or ``init_logs(...)``.
    """
    for name, value, default in (
        ("max_return_chars", max_return_chars, _DEFAULT_MAX_RETURN_CHARS),
        ("always_persist", always_persist, _DEFAULT_ALWAYS_PERSIST),
    ):
        if value != default:
            warnings.warn(
                f"{name}= has never had an effect and is deprecated; it will be "
                f"removed. Delete the argument.",
                DeprecationWarning,
                stacklevel=stacklevel,
            )


class LogSession:
    """
    Unified logging session for Fabric, Databricks and local notebooks.

    Keyword-only parameters
    -----------------------
    lakehouse_dir : str
        Destination directory, interpreted by ``adapter`` — e.g. "Files/Logs"
        (relative to the attached Lakehouse) or a full abfss://.../Files/... path
        with FabricPersistenceAdapter, a local directory with
        LocalFilePersistenceAdapter. Ignored if in_memory=True.
    name_prefix : str
        Prefix for artifact names.
    level : str|int
        Python log level (e.g., "INFO", "DEBUG").
    capture_spark : bool
        Capture Spark DRIVER logs via Log4j FileAppender.
    capture_stdout : bool
        Capture stdout/stderr tails.
    auto_exit : bool
        If True, context manager __exit__ passes the summary to the adapter's
        ``notebook_exit()`` and swallows exceptions (activity appears succeeded).
        Default False.
    max_return_chars : int
        Deprecated: never read. Passing a value other than 250000 emits a
        ``DeprecationWarning``; the parameter will be removed.
    always_persist : bool
        Deprecated: never read. Passing a value other than True emits a
        ``DeprecationWarning``; the parameter will be removed. A session that is
        not ``in_memory`` always writes the full combined JSON, whatever this says.
    persist_spark_driver_file : bool
        If True, also copy the captured Spark driver log file next to the JSON
        (``<base_name>_spark_driver.log``) when one was captured. Default False.
    in_memory : bool
        If True, logs are only stored in memory and not persisted to lakehouse.
        Use get_logs() to retrieve the complete log data in JSON format. Default False.
    adapter : Optional[LogPersistenceAdapter]
        Where and how logs are persisted, and the source of notebook_exit(),
        the notebook name and (optionally) the SparkSession. Required when
        in_memory=False (ValueError otherwise); defaults to
        NullPersistenceAdapter when in_memory=True. An adapter with
        ``finalize_to_table`` (LogTableAdapter) also receives one LogRow.
    spark_session : Optional[SparkSession]
        Session used for Spark driver log capture; when None, the adapter's
        get_spark_session() is tried. Only read when capture_spark=True.
    correlation_id : Optional[str]
        Correlation id for this run (also added to each record).
    notebook_name : Optional[str]
        Name of the notebook; auto-detected if not provided.
    context : Optional[dict]
        Arbitrary metadata (e.g., {"pipeline":"x","stage":"y"}). Included in meta & records.
    pipeline_name : Optional[str]
        Recorded in the payload's meta block, on every record, and in the log
        table row.
    include_context_keys_in_records : Optional[List[str]]
        Extra record fields to capture when present in logging.extra({...}).
    strip_fields : frozenset
        Field names removed from every record after all other fields are
        added (e.g. ones redundant with the payload root). Default: none.
    suppress_stdout_tail : bool
        If True, stdout is still captured while the session runs but written
        as "" to the payload and log row. Default False.

    Raises
    ------
    ValueError
        When in_memory=False and no adapter is given.
    """

    def __init__(
        self,
        *,
        lakehouse_dir: str = "Files/Logs",
        name_prefix: str = "notebook",
        level: Any = "INFO",
        capture_spark: bool = True,
        capture_stdout: bool = True,
        auto_exit: bool = False,
        max_return_chars: int = 250_000,
        always_persist: bool = True,
        persist_spark_driver_file: bool = False,
        in_memory: bool = False,
        adapter: LogPersistenceAdapter | None = None,
        spark_session: Any | None = None,
        correlation_id: str | None = None,
        notebook_name: str | None = None,
        context: dict[str, Any] | None = None,
        pipeline_name: str | None = None,
        include_context_keys_in_records: list[str] | None = None,
        strip_fields: frozenset = frozenset(),
        suppress_stdout_tail: bool = False,
    ):
        self.lakehouse_dir = str(lakehouse_dir).rstrip("/")
        self.name_prefix = name_prefix
        self.level = _level_from_any(level)
        self.capture_spark = capture_spark
        self.capture_stdout = capture_stdout
        self.suppress_stdout_tail = suppress_stdout_tail
        self.auto_exit = auto_exit
        _warn_unread_parameters(max_return_chars, always_persist, stacklevel=3)
        self.persist_spark_driver_file = persist_spark_driver_file
        self.in_memory = in_memory

        # Platform adapter — NullPersistenceAdapter for in-memory; explicit required otherwise
        if adapter is not None:
            self._adapter: LogPersistenceAdapter = adapter
        elif in_memory:
            self._adapter = NullPersistenceAdapter()
        else:
            raise ValueError(
                "adapter= must be provided when in_memory=False. "
                "Use FabricPersistenceAdapter(), DatabricksPersistenceAdapter(), "
                "LocalFilePersistenceAdapter(), or LogTableAdapter(spark, table_path)."
            )
        # Explicit SparkSession takes priority; adapter may supply one as fallback
        self._spark_session: Any | None = spark_session

        self.run_id = uuid.uuid4().hex[:12]
        self._started = False
        self._finalized = False
        self._last_error: dict[str, str] | None = None
        self._full_payload: dict[str, Any] | None = None

        # Spark log level
        self.spark_log_level = python_to_spark_level(self.level)

        # Contextual info
        self.correlation_id = correlation_id
        self.pipeline_name = pipeline_name
        self.notebook_name = notebook_name or self._adapter.get_notebook_name()
        self.context = dict(context or {})
        self._static_ctx = {}
        if self.correlation_id:
            self._static_ctx["correlation_id"] = self.correlation_id
        if self.notebook_name:
            self._static_ctx["notebook_name"] = self.notebook_name
        if self.pipeline_name:
            self._static_ctx["pipeline_name"] = self.pipeline_name
        if self.context:
            # flatten shallow keys into records for convenience
            for k, v in self.context.items():
                if isinstance(v, (str, int, float, bool)) or v is None:
                    self._static_ctx.setdefault(k, v)
        self._ctx_keys = list(include_context_keys_in_records or [])
        self._strip_fields = strip_fields

        # IO/state
        self._stdout_buf: io.StringIO | None = None
        self._stderr_buf: io.StringIO | None = None
        self._stdout_cm = None
        self._stderr_cm = None
        self._spark_appender = None
        self._spark_log_path: str | None = None

        # Logging
        self._buf_handler = _ListBufferHandler(
            static_ctx=self._static_ctx,
            ctx_keys=self._ctx_keys,
            level=self.level,
            strip_fields=self._strip_fields,
        )
        self.logger = logging.getLogger(self.name_prefix)
        self.logger.propagate = True
        logging.getLogger().setLevel(self.level)

    # ---- logging interface -----------------------------------------------

    def debug(self, msg: str, **kwargs: Any) -> None:
        """Log at DEBUG level through this session's named logger."""
        self.logger.debug(msg, **kwargs)

    def info(self, msg: str, **kwargs: Any) -> None:
        """Log at INFO level through this session's named logger."""
        self.logger.info(msg, **kwargs)

    def warning(self, msg: str, **kwargs: Any) -> None:
        """Log at WARNING level through this session's named logger."""
        self.logger.warning(msg, **kwargs)

    def error(self, msg: str, **kwargs: Any) -> None:
        """Log at ERROR level through this session's named logger."""
        self.logger.error(msg, **kwargs)

    # ---- lifecycle -------------------------------------------------------

    def start(self) -> LogSession:
        """Start capture; a no-op when already started or already finalized.

        A finalized session is done: starting it again would redirect stdout and
        stderr into buffers that a second ``finalize()`` (which returns the cached
        summary) never restores. It returns ``self`` unchanged, as ``finalize()``
        twice does; ``init_logs`` and ``get_logger`` build a new session instead.
        """
        if self._started or self._finalized:
            return self
        _, ts = _now_strings()
        self._ts = ts
        self.base_name = f"{self.name_prefix}_{ts}_{self.run_id}"

        # Python root -> memory buffer
        root = logging.getLogger()
        root.handlers.clear()
        root.addHandler(self._buf_handler)

        # stdout/stderr capture
        if self.capture_stdout:
            self._stdout_buf = io.StringIO()
            self._stderr_buf = io.StringIO()
            self._stdout_cm = contextlib.redirect_stdout(self._stdout_buf)
            self._stderr_cm = contextlib.redirect_stderr(self._stderr_buf)
            self._stdout_cm.__enter__()
            self._stderr_cm.__enter__()

        # Spark DRIVER logs via Log4j
        if self.capture_spark:
            try:
                spark = self._spark_session or self._adapter.get_spark_session()
                if spark is None:
                    raise RuntimeError(
                        "No SparkSession available for Spark log capture. "
                        "Pass spark_session= to init_logs() or implement "
                        "get_spark_session() on your adapter."
                    )
                self._spark_appender, self._spark_log_path = _setup_spark_log_capture(
                    spark, self.base_name, self.spark_log_level
                )
            except Exception as e:
                logging.getLogger(__name__).warning(
                    f"Spark driver log capture unavailable: {e}"
                )

        self._started = True
        self.logger.info(
            "LogSession started",
            extra={"run_id": self.run_id, **({"event": "start"} if "event" not in self._ctx_keys else {})},
        )
        return self

    def __enter__(self) -> LogSession:
        return self.start()

    def __exit__(self, exc_type, exc, tb):
        success = exc is None
        summary_json = self.finalize(success=success)
        if self.auto_exit:
            self._adapter.notebook_exit(summary_json)
            return True  # swallow
        return False  # propagate

    # ---- error capture -----------------------------------------------------

    def note_error(self, etype: str, message: str, tb_text: str) -> None:
        """Attach last unhandled error details so finalize() includes them."""
        self._last_error = {"type": etype, "message": message, "traceback": tb_text}

    # ---- finalize & persistence -------------------------------------------

    def _persist_full(self, payload_json: str, spark_text: str | None) -> LogArtifacts:
        base_dir = self.lakehouse_dir
        self._adapter.mkdir(base_dir)
        artifacts = LogArtifacts(base_name=self.base_name, run_id=self.run_id)

        py_path = f"{base_dir}/{self.base_name}_python.json"
        self._adapter.write(py_path, payload_json)
        artifacts.python_json_path = py_path

        if (
            self.persist_spark_driver_file
            and spark_text
            and self._spark_log_path
            and os.path.exists(self._spark_log_path)
        ):
            dest = f"{base_dir}/{self.base_name}_spark_driver.log"
            self._adapter.copy(f"file:{self._spark_log_path}", dest)
            artifacts.spark_driver_log_path = dest

        return artifacts

    def finalize(self, success: bool = True) -> str:
        # Idempotent
        if self._finalized:
            return getattr(
                self, "_summary_json", json.dumps({"run_id": self.run_id, "note": "already finalized"})
            )
        if not self._started:
            return json.dumps(
                {"warning": "LogSession not started", "run_id": self.run_id}
            )

        # stop stdout/stderr capture first
        if self.capture_stdout and self._stdout_cm and self._stderr_cm:
            try:
                self._stdout_cm.__exit__(None, None, None)
                self._stderr_cm.__exit__(None, None, None)
            except Exception:
                pass

        # Stop Spark appender and read captured log
        spark_text = _stop_spark_log_capture(self._spark_appender, self._spark_log_path)
        self._spark_appender = None

        # Build combined payload
        stdout_tail = "" if self.suppress_stdout_tail else (
            self._stdout_buf.getvalue()[-50_000:] if self._stdout_buf else ""
        )
        stderr_tail = self._stderr_buf.getvalue()[-50_000:] if self._stderr_buf else ""
        python_logs = self._buf_handler.buffer
        payload = {
            "run_id": self.run_id,
            "base_name": self.base_name,
            "success": success,
            "meta": {
                "correlation_id": self.correlation_id,
                "pipeline_name": self.pipeline_name,
                "notebook_name": self.notebook_name,
                "context": self.context or {},
                "timestamp": datetime.now(_UTC).isoformat(timespec="milliseconds"),
            },
            "python_log_count": len(python_logs),
            "python_logs": python_logs,
            "stdout_tail": stdout_tail,
            "stderr_tail": stderr_tail,
            "spark_driver_tail": (spark_text[-200_000:] if spark_text else ""),
            "error": self._last_error,
        }
        s = json.dumps(payload)

        # Store full payload for in-memory mode
        self._full_payload = payload

        # In-memory mode: return full payload without persisting
        if self.in_memory:
            self._summary_json = s
            self._finalized = True
            self._started = False
            return self._summary_json

        # Delta table persistence (LogTableAdapter or CapturingPersistenceAdapter in tests)
        if hasattr(self._adapter, "finalize_to_table"):
            finalize_ts = datetime.now(_UTC)
            row = LogRow(
                run_id=self.run_id,
                success=success,
                log_timestamp=finalize_ts,
                correlation_id=self.correlation_id,
                pipeline_name=self.pipeline_name,
                notebook_name=self.notebook_name,
                python_logs=json.dumps(python_logs),
                stdout_tail=stdout_tail,
                stderr_tail=stderr_tail,
                spark_driver_tail=(spark_text[-200_000:] if spark_text else ""),
                context=json.dumps(self.context or {}),
                error=json.dumps(self._last_error) if self._last_error else None,
            )
            try:
                self._adapter.finalize_to_table(row)
            except Exception as _fte:
                logging.getLogger(__name__).warning(f"finalize_to_table failed: {_fte}")

        # File-based persistence (LocalFilePersistenceAdapter, FabricPersistenceAdapter, etc.)
        artifacts = self._persist_full(s, spark_text)
        summary = {
            "run_id": self.run_id,
            "base_name": self.base_name,
            "success": success,
            "meta": {
                "correlation_id": self.correlation_id,
                "pipeline_name": self.pipeline_name,
                "notebook_name": self.notebook_name,
                "context": self.context or {},
            },
            "python_log_count": len(python_logs),
            "full_python_logs_path": artifacts.python_json_path,
            "full_spark_driver_log_path": artifacts.spark_driver_log_path,
            "stdout_tail": stdout_tail,
            "stderr_tail": stderr_tail,
        }
        self._summary_json = json.dumps(summary)
        self._finalized = True
        self._started = False
        return self._summary_json

    def get_logs_json(self) -> str:
        """
        Retrieve the complete log data as a JSON string.

        If the session is not yet finalized, this will finalize it first.
        Useful for in-memory mode when you want to pass logs back to a calling routine.

        Returns
        -------
        str
            Complete log data in JSON format including all python_logs, stdout/stderr,
            spark logs, and metadata.
        """
        if not self._finalized:
            self.finalize(success=True)

        if self._full_payload:
            return json.dumps(self._full_payload)
        else:
            # Return summary if full payload not available
            return self._summary_json

    def get_logs(self) -> dict[str, Any]:
        """
        Retrieve the complete log data as a dictionary.

        If the session is not yet finalized, this will finalize it first.
        Useful for in-memory mode when you want to programmatically process logs.

        Returns
        -------
        dict
            Complete log data including all python_logs, stdout/stderr,
            spark logs, and metadata.
        """
        if not self._finalized:
            self.finalize(success=True)

        if self._full_payload:
            return self._full_payload
        else:
            # Return parsed summary if full payload not available
            return json.loads(self._summary_json)


# ----------------- singleton helpers (for scattered cells) -----------------

_CURRENT: LogSession | None = None


def init_logs(
    *,
    lakehouse_dir: str = "Files/Logs",
    name_prefix: str = "notebook",
    level: Any = "INFO",
    capture_spark: bool = False,
    capture_stdout: bool = True,
    auto_exit: bool = False,
    max_return_chars: int = 250_000,
    always_persist: bool = True,
    persist_spark_driver_file: bool = False,
    in_memory: bool = False,
    adapter: LogPersistenceAdapter | None = None,
    spark_session: Any | None = None,
    correlation_id: str | None = None,
    notebook_name: str | None = None,
    context: dict[str, Any] | None = None,
    pipeline_name: str | None = None,
    include_context_keys_in_records: list[str] | None = None,
    suppress_stdout_tail: bool = False,
) -> LogSession:
    """Idempotently start a global LogSession (kwargs only).

    ``max_return_chars`` and ``always_persist`` are deprecated and have never
    had an effect; a non-default value warns here, at the caller, and is not
    forwarded to ``LogSession`` (which would warn a second time). It warns even
    when a session already exists and this call is a no-op, because the caller
    still wrote the parameter.

    Returns:
        The started global ``LogSession`` (the existing one when already started).

    Raises:
        ValueError: When ``in_memory=False`` and no ``adapter`` is given, as
            ``LogSession`` refuses it.
    """
    global _CURRENT
    _warn_unread_parameters(max_return_chars, always_persist, stacklevel=3)
    if _CURRENT is None or not getattr(_CURRENT, "_started", False):
        _CURRENT = LogSession(
            lakehouse_dir=lakehouse_dir,
            name_prefix=name_prefix,
            level=level,
            capture_spark=capture_spark,
            capture_stdout=capture_stdout,
            auto_exit=auto_exit,
            persist_spark_driver_file=persist_spark_driver_file,
            in_memory=in_memory,
            adapter=adapter,
            spark_session=spark_session,
            correlation_id=correlation_id,
            notebook_name=notebook_name,
            context=context,
            pipeline_name=pipeline_name,
            include_context_keys_in_records=include_context_keys_in_records,
            suppress_stdout_tail=suppress_stdout_tail,
        ).start()
    return _CURRENT


def get_logger(name: str | None = None) -> logging.Logger:
    """Get a logger in any cell; ensures the session exists."""
    global _CURRENT
    if _CURRENT is None or not getattr(_CURRENT, "_started", False):
        _CURRENT = LogSession(in_memory=True).start()
    return logging.getLogger(name) if name else _CURRENT.logger


def finalize_logs(*, success: bool = True, auto_exit: bool = False) -> str:
    """
    Finalize the global session; persist artifacts and return summary JSON.
    If auto_exit=True, passes the summary to the session adapter's
    notebook_exit() (a no-op on adapters that do not override it).
    """
    global _CURRENT
    if _CURRENT is None:
        _CURRENT = LogSession(in_memory=True).start()
    summary = _CURRENT.finalize(success=success)
    if auto_exit:
        _CURRENT._adapter.notebook_exit(summary)
    return summary


def get_logs_json() -> str:
    """
    Retrieve the complete log data from the global session as a JSON string.

    If the session is not yet finalized, this will finalize it first.
    Useful for in-memory mode when you want to pass logs back to a calling routine.

    Returns
    -------
    str
        Complete log data in JSON format.
    """
    global _CURRENT
    if _CURRENT is None:
        return json.dumps({"error": "No active log session"})
    return _CURRENT.get_logs_json()


def get_logs() -> dict[str, Any]:
    """
    Retrieve the complete log data from the global session as a dictionary.

    If the session is not yet finalized, this will finalize it first.
    Useful for in-memory mode when you want to programmatically process logs.

    Returns
    -------
    dict
        Complete log data including all python_logs, stdout/stderr, spark logs, and metadata.
    """
    global _CURRENT
    if _CURRENT is None:
        return {"error": "No active log session"}
    return _CURRENT.get_logs()

def python_to_spark_level(level: Any) -> str:
    """
    Translate a Python logging level (name or int) to a Spark/Log4j level string.

    Returns
    -------
    str
        One of: "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "ALL".
    """
    # normalize python level to int
    if isinstance(level, str):
        py_level = getattr(logging, level.upper(), None)
        if py_level is None:
            raise ValueError(f"Unknown Python logging level: {level}")
    elif isinstance(level, int):
        py_level = level
    else:
        raise TypeError("level must be a str or int")

    # map to Spark/Log4j strings
    if py_level >= logging.CRITICAL:
        spark_str = "FATAL"
    elif py_level >= logging.ERROR:
        spark_str = "ERROR"
    elif py_level >= logging.WARNING:
        spark_str = "WARN"
    elif py_level >= logging.INFO:
        spark_str = "INFO"
    elif py_level > logging.NOTSET:
        spark_str = "DEBUG"
    else:
        spark_str = "ALL"  # Python NOTSET

    return spark_str

# ================= Robust error capture & auto-finalize ===================

def _record_and_finalize_from_exc(err, auto_exit: bool):
    """Internal: record error details and finalize once."""
    import traceback as _tb
    _log = logging.getLogger("LogSession")
    global _CURRENT
    if _CURRENT is None or not getattr(_CURRENT, "_started", False):
        return
    try:
        etype = type(err).__name__
        msg = str(err)
        tb_text = "".join(_tb.format_exception(type(err), err, err.__traceback__))
        _CURRENT.note_error(etype, msg, tb_text)
        _log.error("Unhandled error", exc_info=err)
    except Exception:
        pass
    try:
        finalize_logs(success=False, auto_exit=auto_exit)
    except Exception:
        pass


def install_error_capture(auto_exit: bool = False) -> None:
    """
    Catch-all installer: hooks IPython custom exception, Jupyter post_run_cell,
    and sys.excepthook. Finalizes exactly once with full traceback.
    """
    # 1) IPython custom exception hook
    try:
        from IPython import get_ipython  # type: ignore
        ip = get_ipython()
        if ip and not getattr(ip, "_logsession_custom_exc_installed", False):
            def _ip_exc(shell, etype, evalue, tb, tb_offset=None):
                try:
                    _record_and_finalize_from_exc(evalue, auto_exit)
                except Exception:
                    pass
            ip.set_custom_exc((Exception,), _ip_exc)
            ip._logsession_custom_exc_installed = True
    except Exception:
        pass

    # 2) Jupyter post_run_cell
    try:
        from IPython import get_ipython  # type: ignore
        ip = get_ipython()
        if ip and not getattr(ip, "_logsession_post_hook_installed", False):
            def _post_run_cell(result):
                err = getattr(result, "error_in_exec", None) or getattr(result, "error_before_exec", None)
                if err:
                    _record_and_finalize_from_exc(err, auto_exit)
            ip.events.register("post_run_cell", _post_run_cell)
            ip._logsession_post_hook_installed = True
    except Exception:
        pass

    # 3) sys.excepthook (fallback)
    try:
        if not hasattr(sys, "_logsession_exc_installed"):
            _orig = sys.excepthook
            def _hook(etype, evalue, tb):
                try:
                    _record_and_finalize_from_exc(evalue, auto_exit)
                except Exception:
                    pass
                _orig(etype, evalue, tb)
            sys.excepthook = _hook
            sys._logsession_exc_installed = True
    except Exception:
        pass

# ========================================================================

if __name__ == "__main__":
    print("log_session loaded. Use inside Fabric notebooks to persist logs to OneLake.")
