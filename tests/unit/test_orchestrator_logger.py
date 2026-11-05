"""
Tests for OrchestratorLogger.

No Spark, no platform runtime required. All tests run locally.
"""
from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter, PlatformAdapter
from spark_quick_start.logger import OrchestratorLogger
from spark_quick_start.logger.orchestrator_logger import _is_verbose_only
from spark_quick_start.orchestration.models import LoggingContext

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _lc(name: str = "test_log", level: str = "INFO", dir_: str = "/tmp") -> LoggingContext:
    """Build a minimal LoggingContext suitable for all from_logging_context tests."""
    return LoggingContext({"log_file_name": name, "logging_level": level, "log_file_dir": dir_})


# ---------------------------------------------------------------------------
# TestOrchestratorLoggerPlainFallback
# ---------------------------------------------------------------------------

class TestOrchestratorLoggerPlainFallback:
    """OrchestratorLogger constructed without a LogSession falls back to a plain Python logger."""

    def test_init_no_log_session(self):
        """OrchestratorLogger() constructs without error."""
        OrchestratorLogger()  # must not raise

    def test_info_does_not_raise(self):
        """logger.info('msg') completes without exception."""
        logger = OrchestratorLogger(name="test_info")
        logger.info("msg")  # must not raise

    def test_warning_does_not_raise(self):
        """logger.warning('msg') completes without exception."""
        logger = OrchestratorLogger(name="test_warning")
        logger.warning("msg")  # must not raise

    def test_error_does_not_raise(self):
        """logger.error('msg') completes without exception."""
        logger = OrchestratorLogger(name="test_error")
        logger.error("msg")  # must not raise

    def test_debug_does_not_raise(self):
        """logger.debug('msg') completes without exception."""
        logger = OrchestratorLogger(name="test_debug", level="DEBUG")
        logger.debug("msg")  # must not raise

    def test_finalize_returns_empty_string(self):
        """finalize(success=True) returns '' when backed by a plain logger."""
        logger = OrchestratorLogger(name="test_finalize_ok")
        result = logger.finalize(success=True)
        assert result == ""

    def test_finalize_success_false_returns_empty_string(self):
        """finalize(success=False) returns '' when no LogSession is present."""
        logger = OrchestratorLogger(name="test_finalize_fail")
        result = logger.finalize(success=False)
        assert result == ""


class TestPlainLoggerCarriesContext:
    """Without a LogSession, the three context values reach every record (F119)."""

    def test_all_three_on_record_and_in_message(self, caplog):
        """Each given value is a record attribute and shows in the message prefix."""
        logger = OrchestratorLogger(
            name="ctx_all", correlation_id="c-1", notebook_name="nb", pipeline_name="pl",
        )
        with caplog.at_level(logging.INFO, logger="ctx_all"):
            logger.info("hello", important=True)
            logger.warning("careful")
            logger.error("boom")
        assert len(caplog.records) == 3
        for rec in caplog.records:
            assert rec.correlation_id == "c-1"
            assert rec.notebook_name == "nb"
            assert rec.pipeline_name == "pl"
            assert rec.getMessage().startswith(
                "[correlation_id=c-1 pipeline=pl notebook=nb] "
            )
        assert caplog.records[0].getMessage().endswith("hello")

    def test_debug_carries_context_when_verbose(self, caplog):
        """DEBUG records (verbose mode) carry the context too."""
        logger = OrchestratorLogger(
            name="ctx_debug", level="DEBUG", verbose=True, correlation_id="c-d",
        )
        with caplog.at_level(logging.DEBUG, logger="ctx_debug"):
            logger.debug("detail")
        assert caplog.records[0].getMessage() == "[correlation_id=c-d] detail"

    def test_none_values_are_omitted(self, caplog):
        """Only the values that were given appear; no 'None' text, no attribute."""
        logger = OrchestratorLogger(name="ctx_partial", pipeline_name="pl")
        with caplog.at_level(logging.INFO, logger="ctx_partial"):
            logger.info("hello", important=True)
        rec = caplog.records[0]
        assert rec.getMessage() == "[pipeline=pl] hello"
        assert not hasattr(rec, "correlation_id")
        assert not hasattr(rec, "notebook_name")

    def test_no_context_leaves_message_untouched(self, caplog):
        """With nothing given, the message is exactly what the caller passed."""
        logger = OrchestratorLogger(name="ctx_none")
        with caplog.at_level(logging.INFO, logger="ctx_none"):
            logger.info("hello", important=True)
        assert caplog.records[0].getMessage() == "hello"

    def test_same_name_instances_do_not_leak(self, caplog):
        """Two instances sharing a logger name keep their own correlation ids."""
        first = OrchestratorLogger(name="ctx_shared", correlation_id="run-A")
        second = OrchestratorLogger(name="ctx_shared", correlation_id="run-B")
        with caplog.at_level(logging.INFO, logger="ctx_shared"):
            first.info("one", important=True)
            second.info("two", important=True)
            first.info("three", important=True)
        assert [r.correlation_id for r in caplog.records] == ["run-A", "run-B", "run-A"]
        assert [r.getMessage() for r in caplog.records] == [
            "[correlation_id=run-A] one",
            "[correlation_id=run-B] two",
            "[correlation_id=run-A] three",
        ]

    def test_shared_logger_is_not_modified(self):
        """Context is applied per instance, not by adding a filter to the global logger."""
        before = list(logging.getLogger("ctx_untouched").filters)
        OrchestratorLogger(name="ctx_untouched", correlation_id="x")
        assert logging.getLogger("ctx_untouched").filters == before

    def test_log_session_mode_unchanged(self, caplog):
        """With a LogSession, the message passes through unprefixed and the plain logger is silent."""
        mock_session = MagicMock()
        logger = OrchestratorLogger(
            name="ctx_session", log_session=mock_session,
            correlation_id="c-s", notebook_name="nb", pipeline_name="pl",
        )
        with caplog.at_level(logging.INFO, logger="ctx_session"):
            logger.info("hello", important=True)
        mock_session.info.assert_called_once_with("hello", extra=None)
        assert caplog.records == []


# ---------------------------------------------------------------------------
# TestOrchestratorLoggerWithLogSession
# ---------------------------------------------------------------------------

class TestOrchestratorLoggerWithLogSession:
    """OrchestratorLogger delegates to a log_session when one is injected."""

    def _make_with_session(self) -> tuple[OrchestratorLogger, MagicMock]:
        mock_session = MagicMock()
        logger = OrchestratorLogger(name="test_with_session", log_session=mock_session)
        return logger, mock_session

    def test_delegates_info_to_log_session(self):
        """After logger.info('msg'), log_session.info is called."""
        logger, session = self._make_with_session()
        logger.info("test message")
        assert session.info.called

    def test_delegates_error_to_log_session(self):
        """After logger.error('msg'), log_session.error is called."""
        logger, session = self._make_with_session()
        logger.error("something broke")
        assert session.error.called

    def test_finalize_calls_session_finalize(self):
        """finalize() calls log_session.finalize(success=...) directly.

        Uses the LogSession instance directly rather than the spark_logging
        global singleton (finalize_logs / get_logs_json) so each orchestrator
        run gets a fresh session regardless of kernel lifetime.
        """
        mock_session = MagicMock()
        mock_session.finalize.return_value = '{"entries": []}'
        logger = OrchestratorLogger(name="test_finalize_session", log_session=mock_session)

        result = logger.finalize(success=True)

        mock_session.finalize.assert_called_once_with(success=True)
        assert result == '{"entries": []}'

    def test_finalize_log_session_failure_returns_empty_string(self):
        """When log_session.finalize() raises, finalize() returns '' without re-raising."""
        mock_session = MagicMock()
        mock_session.finalize.side_effect = RuntimeError("log failure")
        logger = OrchestratorLogger(name="test_finalize_exc", log_session=mock_session)

        result = logger.finalize(success=True)

        assert result == ""


# ---------------------------------------------------------------------------
# TestOrchestratorLoggerFromLoggingContext
# ---------------------------------------------------------------------------

class TestOrchestratorLoggerFromLoggingContext:
    """OrchestratorLogger.from_logging_context factory."""

    def test_from_logging_context_with_none_persistence(self):
        """MockPlatformAdapter returns None from get_log_persistence_adapter;
        result is OrchestratorLogger with no log_session; finalize returns ''."""
        adapter = MockPlatformAdapter()
        # MockPlatformAdapter.get_log_persistence_adapter() returns None by default (inherited)
        lc = _lc()
        logger = OrchestratorLogger.from_logging_context(lc, correlation_id="corr-1", adapter=adapter)
        assert isinstance(logger, OrchestratorLogger)
        assert logger._log_session is None
        assert logger.finalize(success=True) == ""

    def test_from_logging_context_passes_the_pipeline_name_to_the_plain_logger(self, caplog):
        """F119 review — without a session, the pipeline name reaches the plain logger."""
        lc = _lc(name="ctx_from_lc")
        logger = OrchestratorLogger.from_logging_context(
            lc, correlation_id="c1", adapter=MockPlatformAdapter(), pipeline_name="pl")
        with caplog.at_level(logging.INFO, logger="ctx_from_lc"):
            logger.info("hello", important=True)
        [rec] = [r for r in caplog.records if r.name == "ctx_from_lc"]
        assert rec.pipeline_name == "pl"
        assert "[correlation_id=c1 pipeline=pl] hello" in rec.getMessage()

    def test_from_logging_context_sets_name(self):
        """The logger name comes from logging_context.log_file_name."""
        adapter = MockPlatformAdapter()
        lc = _lc(name="my_pipeline_log")
        logger = OrchestratorLogger.from_logging_context(lc, correlation_id="c1", adapter=adapter)
        assert logger._name == "my_pipeline_log"

    def test_from_logging_context_verbose_propagated(self):
        """verbose=True on LoggingContext is forwarded to the OrchestratorLogger."""
        adapter = MockPlatformAdapter()
        lc = LoggingContext({"log_file_name": "x", "logging_level": "INFO",
                             "log_file_dir": "/tmp", "verbose": True})
        logger = OrchestratorLogger.from_logging_context(lc, correlation_id="c1", adapter=adapter)
        assert logger._verbose is True

    def test_from_logging_context_verbose_default_false(self):
        """verbose defaults to False when not in LoggingContext payload."""
        adapter = MockPlatformAdapter()
        lc = _lc()
        logger = OrchestratorLogger.from_logging_context(lc, correlation_id="c1", adapter=adapter)
        assert logger._verbose is False


# ---------------------------------------------------------------------------
# TestVerboseFilter
# ---------------------------------------------------------------------------

class TestVerboseFilter:
    """OrchestratorLogger respects the verbose flag when deciding what to emit."""

    def _make(self, verbose: bool = False) -> tuple[OrchestratorLogger, MagicMock]:
        mock_session = MagicMock()
        logger = OrchestratorLogger(name="vf_test", log_session=mock_session, verbose=verbose)
        return logger, mock_session

    def test_verbose_false_suppresses_verbose_only_msg(self):
        """Non-verbose mode: 'Pre-merge version' is suppressed."""
        logger, session = self._make(verbose=False)
        logger.info("Pre-merge version: 5")
        session.info.assert_not_called()

    def test_verbose_true_logs_verbose_only_msg(self):
        """Verbose mode: 'Pre-merge version' is emitted."""
        logger, session = self._make(verbose=True)
        logger.info("Pre-merge version: 5")
        session.info.assert_called_once()

    def test_non_verbose_mode_logs_always_on_msg(self):
        """Non-verbose mode: 'SCD2 merge completed' passes through."""
        logger, session = self._make(verbose=False)
        logger.info("SCD2 merge completed")
        session.info.assert_called_once()

    def test_important_true_bypasses_verbose_filter(self):
        """important=True emits even a verbose-only message."""
        logger, session = self._make(verbose=False)
        logger.info("Pre-merge version: 5", important=True)
        session.info.assert_called_once()

    def test_error_bypasses_verbose_filter(self):
        """error() is always emitted regardless of verbose setting."""
        logger, session = self._make(verbose=False)
        logger.error("something broke")
        session.error.assert_called_once()

    def test_warning_bypasses_verbose_filter(self):
        """warning() is always emitted regardless of verbose setting."""
        logger, session = self._make(verbose=False)
        logger.warning("hwm capture failed")
        session.warning.assert_called_once()

    def test_debug_suppressed_in_non_verbose(self):
        """debug() messages are always suppressed when verbose=False."""
        logger, session = self._make(verbose=False)
        logger.debug("internal state dump")
        session.debug.assert_not_called()

    def test_debug_emitted_in_verbose_mode(self):
        """debug() messages are emitted when verbose=True."""
        logger, session = self._make(verbose=True)
        logger.debug("internal state dump")
        session.debug.assert_called_once()

    def test_metrics_bypass_verbose_filter(self):
        """metrics=[...] on info() short-circuits the verbose filter."""
        logger, session = self._make(verbose=False)
        logger.info("Source records: 42", metrics=[{"metricName": "count", "metricValue": 42}])
        session.info.assert_called_once()

    def test_tags_forwarded_to_session(self):
        """tags dict is forwarded via extra= to the log session."""
        logger, session = self._make(verbose=True)
        logger.info("SCD2 merge completed", tags={"entity": "Person"})
        _, call_kwargs = session.info.call_args
        extra = call_kwargs.get("extra", {}) or {}
        assert extra.get("tags") == {"entity": "Person"}

    def test_metrics_forwarded_to_session(self):
        """metrics list is forwarded via extra= to the log session."""
        logger, session = self._make(verbose=True)
        metrics = [{"metricName": "inserted", "metricValue": 10}]
        logger.info("Merge stats", metrics=metrics)
        _, call_kwargs = session.info.call_args
        extra = call_kwargs.get("extra", {}) or {}
        assert extra.get("metrics") == metrics

    def test_no_extra_when_no_tags_or_metrics(self):
        """When tags and metrics are both None, extra=None is passed."""
        logger, session = self._make(verbose=True)
        logger.info("SCD2 merge completed")
        _, call_kwargs = session.info.call_args
        assert call_kwargs.get("extra") is None


# ---------------------------------------------------------------------------
# TestIsVerboseOnlyHeuristic
# ---------------------------------------------------------------------------

class TestIsVerboseOnlyHeuristic:
    """Direct unit tests for the _is_verbose_only() pure function."""

    def test_merge_candidates_is_verbose_only(self):
        assert _is_verbose_only("Merge candidates registered as temp view: v_merge") is True

    def test_merge_completed_is_always_on(self):
        assert _is_verbose_only("SCD2 merge completed") is False

    def test_staging_complete_is_always_on(self):
        assert _is_verbose_only("Staging complete: 42 rows in 3.1s") is False

    def test_all_staging_phases_completed_is_always_on(self):
        assert _is_verbose_only("All staging phases completed") is False

    def test_pre_merge_version_is_verbose_only(self):
        assert _is_verbose_only("Pre-merge version: 5") is True

    def test_spark_runtime_json_is_verbose_only(self):
        assert _is_verbose_only('{"execution_stage": "pre-staging", "app": "spark"}') is True

    def test_source_records_is_verbose_only(self):
        assert _is_verbose_only("Source records: 1024") is True

    def test_executing_stage_is_verbose_only(self):
        assert _is_verbose_only("Executing stage 1/3: NB_Person") is True

    def test_executing_phases_mode_is_always_on(self):
        assert _is_verbose_only("Executing 3 staging phase(s) — mode: SEQUENTIAL") is False

    def test_metrics_present_overrides_verbose_classification(self):
        assert _is_verbose_only("Pre-merge version: 5",
                                metrics=[{"metricName": "ver", "metricValue": 5}]) is False

    def test_unknown_message_defaults_to_always_on(self):
        assert _is_verbose_only("doing something completely unknown") is False

    def test_case_insensitive(self):
        assert _is_verbose_only("PRE-MERGE VERSION: 10") is True
        assert _is_verbose_only("SCD2 MERGE COMPLETED") is False


# ---------------------------------------------------------------------------
# Log payload exposure controls
# ---------------------------------------------------------------------------

class TestStdoutTailSuppression:
    """logging_context.suppress_stdout_tail must reach LogSession.

    Everything printed during a run is captured into the persisted log's
    stdout_tail — including real row values whenever something prints them
    (DebugTransformer does, by design). Logs are written to a lakehouse volume
    whose read permissions do not generally match the source table's. The
    suppression flag existed on LogSession all along but nothing supplied it,
    so there was no way to turn it off from configuration.
    """

    def _capture_log_session_kwargs(self, lc):
        """Build a logger with a stubbed LogSession and return its kwargs."""
        import spark_quick_start.spark_logging as sl

        # spec'd, not MockPlatformAdapter: it has no way to return a persistence adapter
        adapter = MagicMock(spec=PlatformAdapter)
        adapter.get_log_persistence_adapter.return_value = MagicMock()
        captured = {}

        def _fake_log_session(**kwargs):
            captured.update(kwargs)
            session = MagicMock()
            session.start.return_value = session
            return session

        with patch.object(sl, "LogSession", _fake_log_session):
            OrchestratorLogger.from_logging_context(
                lc, correlation_id="c1", adapter=adapter, pipeline_name="my_pipeline"
            )
        return captured

    def test_suppress_stdout_tail_is_forwarded_when_set(self):
        lc = LoggingContext({
            "log_file_name": "n", "logging_level": "INFO", "log_file_dir": "Files/Logs",
            "suppress_stdout_tail": True,
        })
        assert self._capture_log_session_kwargs(lc)["suppress_stdout_tail"] is True

    def test_default_preserves_the_stdout_trail(self):
        """Default must not change behaviour — the trail is what makes a run diagnosable."""
        lc = LoggingContext({
            "log_file_name": "n", "logging_level": "INFO", "log_file_dir": "Files/Logs",
        })
        assert self._capture_log_session_kwargs(lc)["suppress_stdout_tail"] is False

    def test_stdout_is_still_captured_either_way(self):
        """Suppression controls persistence, not capture — console output is unaffected."""
        lc = LoggingContext({
            "log_file_name": "n", "logging_level": "INFO", "log_file_dir": "Files/Logs",
            "suppress_stdout_tail": True,
        })
        assert self._capture_log_session_kwargs(lc)["capture_stdout"] is True

    def test_pipeline_name_reaches_the_log_session(self):
        """meta.pipeline_name was null in every log ever produced: the field is
        plumbed through LogSession and the log-table schema, but the factory
        never supplied one."""
        lc = LoggingContext({
            "log_file_name": "n", "logging_level": "INFO", "log_file_dir": "Files/Logs",
        })
        assert self._capture_log_session_kwargs(lc)["pipeline_name"] == "my_pipeline"


class TestNotebookAttributionSurvivesAPosixLogDir:
    """F27 — one condition was deciding two different questions.

    `from_logging_context` substitutes `LocalFilePersistenceAdapter` whenever
    `log_file_dir` starts with "/". That is right for *writing*: on Databricks
    `/Volumes/...` and `/dbfs/...` are FUSE-mounted and reachable through pathlib with
    no dbutils, which is also the Serverless-safe choice.

    The side effect was that it replaced `get_notebook_name()` too. The local adapter
    probes `ip.user_ns` for `__vsc_ipynb_file__` and `__session__` — a VS Code variable
    and a Fabric one. A Databricks kernel sets neither, and the Databricks persistence
    adapter is no longer in play, so `notebook_name` came out None and the
    `notebook_name` column of every persisted log row was empty — for the *recommended*
    Databricks configuration.

    The fix resolves the name from the platform adapter before the substitution and
    passes it to `LogSession(notebook_name=...)`, a parameter that already exists and is
    preferred over the adapter's own probe.
    """

    def _logging_ctx(self, log_dir):
        from spark_quick_start.orchestration.models import LoggingContext

        return LoggingContext({"log_file_dir": log_dir, "log_file_name": "orch"})

    def _adapter_reporting(self, notebook_name):
        """A platform adapter whose persistence layer knows the notebook name."""
        persistence = MagicMock()
        persistence.get_notebook_name.return_value = notebook_name
        persistence.get_spark_session.return_value = None
        # spec'd, not MockPlatformAdapter: it has no way to return a persistence adapter
        adapter = MagicMock(spec=PlatformAdapter)
        adapter.get_log_persistence_adapter.return_value = persistence
        return adapter

    def test_a_posix_log_dir_keeps_the_platform_notebook_name(self):
        adapter = self._adapter_reporting("NB_Load_Customer")

        with patch("spark_quick_start.spark_logging.LogSession") as mock_session:
            OrchestratorLogger.from_logging_context(
                self._logging_ctx("/Volumes/main/logs"), "corr-1", adapter,
            )

        assert mock_session.call_args is not None, "a LogSession must be constructed"
        assert mock_session.call_args.kwargs.get("notebook_name") == "NB_Load_Customer", (
            "the platform adapter's notebook name must be resolved before the local "
            "adapter is substituted for writing, and passed through explicitly"
        )

    def test_a_uri_scheme_log_dir_is_unaffected(self):
        """The non-substituting path must behave exactly as before."""
        adapter = self._adapter_reporting("NB_Load_Customer")

        with patch("spark_quick_start.spark_logging.LogSession") as mock_session:
            OrchestratorLogger.from_logging_context(
                self._logging_ctx("abfss://logs@acct.dfs.core.windows.net/x"), "corr-1", adapter,
            )

        assert mock_session.call_args.kwargs.get("notebook_name") == "NB_Load_Customer"

    def test_no_notebook_name_available_stays_none(self):
        """An adapter that cannot detect a name must not invent one."""
        adapter = self._adapter_reporting(None)

        with patch("spark_quick_start.spark_logging.LogSession") as mock_session:
            OrchestratorLogger.from_logging_context(
                self._logging_ctx("/tmp/logs"), "corr-1", adapter,
            )

        assert mock_session.call_args.kwargs.get("notebook_name") is None
