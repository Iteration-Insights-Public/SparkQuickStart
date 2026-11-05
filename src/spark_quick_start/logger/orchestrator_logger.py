"""
orchestrator_logger.py — OrchestratorLogger: injectable logging wrapper.

Contains: OrchestratorLogger — wraps a spark_logging LogSession or falls
    back to a plain logging.Logger. One instance per LoadOrchestrator run.
Does NOT contain: platform-specific imports, Spark logic, or any import
    from spark_load_orchestration.

Usage (with plain logger, e.g. in tests)::

    from spark_quick_start.logger import OrchestratorLogger

    logger = OrchestratorLogger(name="test_pipeline", level="DEBUG")
    logger.info("starting")
    logs_json = logger.finalize(success=True)  # returns ""

Usage (from logging context in production)::

    logger = OrchestratorLogger.from_logging_context(
        logging_context, correlation_id, adapter
    )

Verbose mode::

    # Non-verbose (default): only pipeline start/end, merge stats, errors.
    logger = OrchestratorLogger(name="pipeline", verbose=False)

    # Verbose: every diagnostic message is emitted.
    logger = OrchestratorLogger(name="pipeline", verbose=True)

    # Always-on regardless of verbose setting:
    logger.info("SCD2 merge completed", important=True, metrics=[...])
"""
from __future__ import annotations

import logging
import sys
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from spark_quick_start.adapters.platform_adapter import PlatformAdapter
    from spark_quick_start.orchestration.context_manager import LoggingContext


# ---------------------------------------------------------------------------
# Verbose-filter heuristic
# ---------------------------------------------------------------------------

_ALWAYS_ON_SUBSTRINGS: frozenset = frozenset({
    "started",
    "complete",
    "completed",
    "failed",
    "all staging phases",
    "staging summary",
    "staging complete",
    "merge stats",
    "hwm_before",
    "hwm_after",
    "branch a",
    "branch b",
    "loadorchestrator.run()",
    "mode:",       # "Executing N staging phase(s) — mode: SEQUENTIAL"
    "starting",    # "Starting staging: nb_name"
})

_VERBOSE_ONLY_SUBSTRINGS: frozenset = frozenset({
    "target table loaded",
    "staged data loaded",
    "merge candidates registered",
    "pre-merge version",
    "prepared stage",
    "executing stage ",    # trailing space avoids match on "executing N staging phase"
    "batch ",
    "max concurrent notebooks",
    "no schema validation configured",
    "logsession started",
    "execution_stage",     # Spark runtime JSON blob from log_runtime()
    "to insert:",          # intermediate merge prep counts
    "creating target table",
    "detecting scd",
    "source records:",
})


def _is_verbose_only(
    msg: str,
    *,
    tags: dict[str, str] | None = None,
    metrics: list[dict[str, Any]] | None = None,
) -> bool:
    """Return True if *msg* should be suppressed in non-verbose mode.

    Classification rules (evaluated in order):
    1. Any call with ``metrics`` is always-on — numeric outcomes are never noise.
    2. If the lowercased message contains a substring from ``_ALWAYS_ON_SUBSTRINGS``
       → False (emit regardless of verbose setting).
    3. If the lowercased message contains a substring from ``_VERBOSE_ONLY_SUBSTRINGS``
       → True (suppress in non-verbose mode).
    4. Unknown messages default to always-on (False) — fail-open.

    Args:
        msg: The log message string.
        tags: Optional per-call tag dict (unused in classification currently).
        metrics: Optional list of metric dicts. Any non-empty list → False.

    Returns:
        True if the message is verbose-only (should be filtered when verbose=False).
    """
    if metrics:
        return False
    low = msg.lower()
    if any(s in low for s in _ALWAYS_ON_SUBSTRINGS):
        return False
    if any(s in low for s in _VERBOSE_ONLY_SUBSTRINGS):
        return True
    return False   # safe default: log unknown messages


class _ContextAdapter(logging.LoggerAdapter[logging.Logger]):
    """Stamp one logger instance's run context onto every record it emits.

    A ``LoggerAdapter`` rather than a ``Filter`` or handler because
    ``logging.getLogger(name)`` is process-global: anything attached to it is
    shared by every ``OrchestratorLogger`` of the same name, so one run's
    correlation id would appear in another's records. The adapter lives on the
    instance and leaves the shared logger untouched.

    The context is added as record attributes (``correlation_id``,
    ``notebook_name``, ``pipeline_name``) and as a visible message prefix, so it
    shows under any handler format. Values that are ``None`` are omitted.
    """

    def __init__(self, logger: logging.Logger, context: dict[str, str]) -> None:
        super().__init__(logger, context)
        parts = [f"{label}={context[key]}" for key, label in _PREFIX_ORDER if key in context]
        self._prefix = f"[{' '.join(parts)}] " if parts else ""

    def process(self, msg: Any, kwargs: Any) -> tuple[Any, Any]:
        """Return the prefixed message and kwargs carrying the context as ``extra``."""
        kwargs["extra"] = {**(self.extra or {}), **kwargs.get("extra", {})}
        return f"{self._prefix}{msg}", kwargs


_PREFIX_ORDER: tuple[tuple[str, str], ...] = (
    ("correlation_id", "correlation_id"),
    ("pipeline_name", "pipeline"),
    ("notebook_name", "notebook"),
)


class OrchestratorLogger:
    """Structured logging wrapper for LoadOrchestrator.

    Wraps a LogSession from spark_logging (when available) with a consistent
    interface usable by all orchestration modules. Falls back to a plain
    logging.Logger when no LogSession is available (local/test mode).

    One instance is created per LoadOrchestrator.run() call. It is injected
    into pipeline functions via parameter — it is NOT a global singleton.

    Args:
        name: Logger name / prefix (e.g. pipeline name or entity name).
        level: Python logging level string (``"INFO"``, ``"DEBUG"``, etc.)
            or int.
        correlation_id: Pipeline run identifier. In the plain-logger fallback
            (no ``log_session``) it is stamped on every record as a
            ``correlation_id`` attribute and as a ``[correlation_id=...]``
            message prefix. With a ``log_session`` it is not read here; the
            session carries it.
        notebook_name: Name of the calling notebook. Stamped like
            ``correlation_id`` (attribute ``notebook_name``, prefix
            ``notebook=``) in the plain-logger fallback only.
        pipeline_name: Pipeline name. Stamped like ``correlation_id``
            (attribute ``pipeline_name``, prefix ``pipeline=``) in the
            plain-logger fallback only. Values left as ``None`` are omitted.
        log_session: Pre-constructed LogSession from spark_logging.
            When ``None``, a plain logging.Logger is used as fallback.
        verbose: When ``True``, all messages are emitted. When ``False``
            (default), low-signal diagnostic messages are suppressed and only
            always-on events (pipeline start/end, merge stats, errors) are
            logged. ``debug()`` messages are always suppressed when
            ``verbose=False``.
    """

    def __init__(
        self,
        name: str = "orchestrator",
        level: Any = "INFO",
        *,
        correlation_id: str | None = None,
        notebook_name: str | None = None,
        pipeline_name: str | None = None,
        log_session: Any | None = None,
        verbose: bool = False,
    ) -> None:
        self._name = name
        self._log_session = log_session
        self._correlation_id = correlation_id
        self._notebook_name = notebook_name
        self._pipeline_name = pipeline_name
        self._verbose = verbose

        # Plain-logger fallback — used when no LogSession is injected.
        shared_logger = logging.getLogger(name)
        if isinstance(level, str):
            shared_logger.setLevel(getattr(logging, level.upper(), logging.INFO))
        else:
            shared_logger.setLevel(level)
        if not shared_logger.handlers:
            shared_logger.addHandler(logging.StreamHandler(sys.stdout))
        # Per-instance view of the shared logger above; see _ContextAdapter.
        context = {
            key: value
            for key, value in (
                ("correlation_id", correlation_id),
                ("notebook_name", notebook_name),
                ("pipeline_name", pipeline_name),
            )
            if value is not None
        }
        self._plain_logger = _ContextAdapter(shared_logger, context)

    @classmethod
    def from_logging_context(
        cls,
        logging_context: LoggingContext,
        correlation_id: str,
        adapter: PlatformAdapter,
        pipeline_name: str | None = None,
    ) -> OrchestratorLogger:
        """Factory: build an OrchestratorLogger from a parsed LoggingContext.

        Calls ``adapter.get_log_persistence_adapter()`` to obtain a log
        persistence adapter. When the adapter returns ``None`` (local/test
        mode), falls back to a plain logger.

        If ``spark_logging`` is available and the persistence adapter is
        non-None, constructs a fresh LogSession directly and wraps it — not
        through ``init_logs``, whose module-level session would be reused
        across runs in a long-lived kernel. A ``log_file_dir`` starting with
        ``/`` swaps in ``LocalFilePersistenceAdapter``. Otherwise constructs a
        plain-logger OrchestratorLogger.

        Args:
            logging_context: Parsed LoggingContext with level, name, and
                optional log_file_dir.
            correlation_id: Correlation ID for this run.
            adapter: Platform adapter; its get_log_persistence_adapter()
                is called to resolve log persistence.
            pipeline_name: Pipeline name recorded in the log payload's meta
                block and the log-table row. Optional so existing callers keep
                working; ``LoadOrchestrator`` supplies
                ``runtime_ctx.pipeline_name``.

        Returns:
            Configured OrchestratorLogger.
        """
        name = logging_context.log_file_name or "orchestrator"
        level = logging_context.logging_level or "INFO"
        log_dir = logging_context.log_file_dir or ""
        log_session = None

        persistence = adapter.get_log_persistence_adapter()
        if persistence is not None:
            # Override the platform persistence adapter when log_file_dir is a
            # locally-mounted path.  On Databricks:
            #   /Volumes/...   — Unity Catalog Volumes, mounted as local FS
            #   /dbfs/...      — DBFS FUSE mount, also local FS
            #   /tmp/..., etc. — ephemeral local paths
            # These are all accessible via pathlib without dbutils, so
            # LocalFilePersistenceAdapter is the correct (and Serverless-safe)
            # choice.  URI-scheme paths (dbfs:/, abfss://, Files/...) continue
            # to use the platform adapter returned by the caller.
            # Resolve the notebook name from the *platform* adapter first, because the
            # substitution below replaces get_notebook_name() along with the file
            # operations — and those are two different questions being decided by one
            # condition. LocalFilePersistenceAdapter probes ip.user_ns for
            # __vsc_ipynb_file__ (VS Code) and __session__ (Fabric); a Databricks kernel
            # sets neither, so the recommended Databricks configuration — a POSIX
            # log_file_dir under /Volumes or /dbfs — silently wrote an empty
            # notebook_name into every log row (F27). LogSession prefers an explicit
            # notebook_name over its adapter's own probe, which is what this uses.
            platform_notebook_name = None
            try:
                platform_notebook_name = persistence.get_notebook_name()
            except Exception:  # pragma: no cover — a probe must never break logging setup
                platform_notebook_name = None

            if log_dir.startswith("/"):
                from spark_quick_start.spark_logging import LocalFilePersistenceAdapter
                persistence = LocalFilePersistenceAdapter()

            try:
                # Create a LogSession directly instead of using init_logs().
                # init_logs() manages a module-level _CURRENT singleton — if the
                # Fabric/Databricks kernel stays alive between pipeline runs, it
                # returns the same stale session (with the date from the kernel's
                # first start) rather than creating a fresh one for this run.
                # Constructing LogSession directly guarantees a fresh session
                # with a new timestamp for every LoadOrchestrator.run() call.
                from spark_quick_start.spark_logging import LogSession
                log_session = LogSession(
                    lakehouse_dir=log_dir or "Files/Logs",
                    # Explicit, so a substituted local adapter cannot lose it (F27).
                    notebook_name=platform_notebook_name,
                    name_prefix=name,
                    level=level,
                    adapter=persistence,
                    capture_spark=False,
                    # stdout is still captured (it is what makes a run
                    # diagnosable); suppress_stdout_tail decides whether it is
                    # written to the persisted payload.
                    capture_stdout=True,
                    suppress_stdout_tail=logging_context.suppress_stdout_tail,
                    in_memory=False,
                    correlation_id=correlation_id,
                    # LogSession and the log-table schema have carried a
                    # pipeline_name field all along, but nothing ever supplied
                    # one, so meta.pipeline_name was null in every log produced.
                    pipeline_name=pipeline_name,
                    # Strip fields that are redundant with the payload root / meta block.
                    strip_fields=frozenset({"run_id", "correlation_id", "name"}),
                ).start()
            except Exception:
                log_session = None

        return cls(
            name=name,
            level=level,
            correlation_id=correlation_id,
            pipeline_name=pipeline_name,
            log_session=log_session,
            verbose=logging_context.verbose,
        )

    # ── Logging interface ────────────────────────────────────────────────────

    def debug(
        self,
        msg: str,
        *,
        tags: dict[str, str] | None = None,
        metrics: list[dict[str, Any]] | None = None,
        **kwargs: Any,
    ) -> None:
        """Log at DEBUG level.

        Suppressed entirely when ``verbose=False`` — DEBUG messages are by
        definition low-signal diagnostics. No heuristic call is made.

        Args:
            msg: Log message.
            tags: Optional key/value pairs attached to this entry.
            metrics: Optional list of ``{"metricName": str, "metricValue": number}``
                dicts attached to this entry.
        """
        if not self._verbose:
            return
        extra: dict[str, Any] = {}
        if tags is not None:
            extra["tags"] = tags
        if metrics is not None:
            extra["metrics"] = metrics
        if self._log_session is not None:
            try:
                self._log_session.debug(msg, extra=extra or None, **kwargs)
                return
            except Exception:
                pass
        self._plain_logger.debug(msg)

    def info(
        self,
        msg: str,
        *,
        tags: dict[str, str] | None = None,
        metrics: list[dict[str, Any]] | None = None,
        important: bool = False,
        **kwargs: Any,
    ) -> None:
        """Log at INFO level with optional verbose filtering.

        When ``verbose=False`` and ``important=False``, the message is passed
        through ``_is_verbose_only()``. If the heuristic classifies it as
        verbose-only, the message is silently dropped. The heuristic is
        fail-open: any unexpected exception causes the message to be emitted.

        Use ``important=True`` to unconditionally emit a message regardless of
        the ``verbose`` setting — intended for definitively always-on call sites
        such as pipeline start/end and merge stats with numeric counts.

        Args:
            msg: Log message.
            tags: Optional key/value pairs attached to this entry.
            metrics: Optional list of ``{"metricName": str, "metricValue": number}``
                dicts. Any non-empty list also short-circuits the verbose filter.
            important: When ``True``, bypasses the verbose filter unconditionally.
        """
        if not important:
            try:
                if not self._verbose and _is_verbose_only(msg, tags=tags, metrics=metrics):
                    return
            except Exception:
                pass  # fail-open: heuristic crash → log the message
        extra: dict[str, Any] = {}
        if tags is not None:
            extra["tags"] = tags
        if metrics is not None:
            extra["metrics"] = metrics
        if self._log_session is not None:
            try:
                self._log_session.info(msg, extra=extra or None, **kwargs)
                return
            except Exception:
                pass
        self._plain_logger.info(msg)

    def warning(
        self,
        msg: str,
        *,
        tags: dict[str, str] | None = None,
        metrics: list[dict[str, Any]] | None = None,
        **kwargs: Any,
    ) -> None:
        """Log at WARNING level — always emitted regardless of verbose setting.

        Args:
            msg: Log message.
            tags: Optional key/value pairs attached to this entry.
            metrics: Optional list of metric dicts attached to this entry.
        """
        extra: dict[str, Any] = {}
        if tags is not None:
            extra["tags"] = tags
        if metrics is not None:
            extra["metrics"] = metrics
        if self._log_session is not None:
            try:
                self._log_session.warning(msg, extra=extra or None, **kwargs)
                return
            except Exception:
                pass
        self._plain_logger.warning(msg)

    def error(
        self,
        msg: str,
        *,
        tags: dict[str, str] | None = None,
        metrics: list[dict[str, Any]] | None = None,
        **kwargs: Any,
    ) -> None:
        """Log at ERROR level — always emitted regardless of verbose setting.

        Args:
            msg: Log message.
            tags: Optional key/value pairs attached to this entry.
            metrics: Optional list of metric dicts attached to this entry.
        """
        extra: dict[str, Any] = {}
        if tags is not None:
            extra["tags"] = tags
        if metrics is not None:
            extra["metrics"] = metrics
        if self._log_session is not None:
            try:
                self._log_session.error(msg, extra=extra or None, **kwargs)
                return
            except Exception:
                pass
        self._plain_logger.error(msg)

    def finalize(self, success: bool = True) -> str:
        """Finalize the underlying LogSession and return the log JSON string.

        Wraps the LogSession finalize call in try/except — a log persistence
        failure must never mask a pipeline failure.

        Args:
            success: Whether the pipeline run completed without error.

        Returns:
            JSON string of captured logs, or ``""`` when backed by a plain
            logger (no LogSession active).
        """
        if self._log_session is None:
            return ""
        try:
            # Call finalize() directly on the session we own — bypasses the
            # spark_logging global singleton (finalize_logs / get_logs_json)
            # which would operate on _CURRENT and could return stale data.
            logs_json: str = self._log_session.finalize(success=success)
            return logs_json or ""
        except Exception as exc:
            logging.getLogger(__name__).warning(
                f"OrchestratorLogger.finalize failed — logs may be incomplete: {exc}"
            )
            return ""
