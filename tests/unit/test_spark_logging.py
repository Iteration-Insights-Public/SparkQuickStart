"""
Comprehensive tests for spark_logging.py — covering helpers, adapters,
LogSession lifecycle, singleton functions, and error capture hooks.

Run with:
    pytest tests/test_spark_logging.py -v
"""
import json
import logging
import sys
import types

import pytest

# Package is installed via pip install -e . — no sys.path manipulation needed
import spark_quick_start.spark_logging as fl
from spark_quick_start.spark_logging import (
    CapturingPersistenceAdapter,
    # Core classes
    LogArtifacts,
    # Adapters
    LogPersistenceAdapter,
    LogSession,
    NullPersistenceAdapter,
    # Helpers
    _level_from_any,
    # Spark helpers
    _stop_spark_log_capture,
    finalize_logs,
    get_logger,
    get_logs,
    get_logs_json,
    # Singleton API
    init_logs,
    # Error capture
    install_error_capture,
    python_to_spark_level,
)

# ---------------------------------------------------------------------------
# Autouse fixture: reset global singleton before/after every test
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def reset_global_session():
    """Ensure the global _CURRENT session is cleared between tests."""
    fl._CURRENT = None
    yield
    # Also clear any partially-started session
    if fl._CURRENT is not None and getattr(fl._CURRENT, "_started", False):
        try:
            fl._CURRENT.finalize(success=False)
        except Exception:
            pass
    fl._CURRENT = None
    # Restore root logger handlers to avoid cross-test pollution
    logging.getLogger().handlers.clear()


# ===========================================================================
# Helpers
# ===========================================================================

class TestLevelFromAny:
    def test_int_passthrough(self):
        assert _level_from_any(20) == 20

    def test_string_debug(self):
        assert _level_from_any("debug") == logging.DEBUG

    def test_string_info(self):
        assert _level_from_any("INFO") == logging.INFO

    def test_string_warning(self):
        assert _level_from_any("WARNING") == logging.WARNING

    def test_string_error(self):
        assert _level_from_any("ERROR") == logging.ERROR

    def test_string_critical(self):
        assert _level_from_any("CRITICAL") == logging.CRITICAL

    def test_invalid_string_falls_back_to_info(self):
        # _level_from_any is lenient — unknown names fall back to logging.INFO
        assert _level_from_any("NOSUCHLEVEL") == logging.INFO

    def test_none_falls_back_to_info(self):
        # None is not str/int — falls back to logging.INFO
        assert _level_from_any(None) == logging.INFO


class TestPythonToSparkLevel:
    def test_debug(self):
        assert python_to_spark_level("DEBUG") == "DEBUG"

    def test_debug_int(self):
        assert python_to_spark_level(logging.DEBUG) == "DEBUG"

    def test_info(self):
        assert python_to_spark_level("INFO") == "INFO"

    def test_warning_maps_to_warn(self):
        assert python_to_spark_level("WARNING") == "WARN"

    def test_warning_int_maps_to_warn(self):
        assert python_to_spark_level(logging.WARNING) == "WARN"

    def test_error(self):
        assert python_to_spark_level("ERROR") == "ERROR"

    def test_critical_maps_to_fatal(self):
        assert python_to_spark_level("CRITICAL") == "FATAL"

    def test_critical_int_maps_to_fatal(self):
        assert python_to_spark_level(logging.CRITICAL) == "FATAL"

    def test_notset_maps_to_all(self):
        assert python_to_spark_level(logging.NOTSET) == "ALL"

    def test_invalid_string_raises(self):
        with pytest.raises(ValueError):
            python_to_spark_level("NOSUCH")

    def test_invalid_type_raises(self):
        with pytest.raises(TypeError):
            python_to_spark_level([])


# ===========================================================================
# Adapters
# ===========================================================================

class TestNullPersistenceAdapter:
    def test_mkdir_noop(self):
        a = NullPersistenceAdapter()
        a.mkdir("/some/path")  # no exception, no side effect

    def test_write_noop(self):
        a = NullPersistenceAdapter()
        a.write("/some/path", "content")

    def test_copy_noop(self):
        a = NullPersistenceAdapter()
        a.copy("/src", "/dst")

    def test_notebook_exit_noop(self):
        a = NullPersistenceAdapter()
        a.notebook_exit("value")

    def test_is_subclass(self):
        assert issubclass(NullPersistenceAdapter, LogPersistenceAdapter)


class TestCapturingPersistenceAdapter:
    def test_mkdir_recorded(self):
        a = CapturingPersistenceAdapter()
        a.mkdir("/logs/2024")
        assert "/logs/2024" in a.dirs

    def test_write_recorded(self):
        a = CapturingPersistenceAdapter()
        a.write("/logs/out.json", '{"key": "val"}')
        assert "/logs/out.json" in a.written
        assert a.written["/logs/out.json"] == '{"key": "val"}'

    def test_copy_recorded(self):
        a = CapturingPersistenceAdapter()
        a.copy("/tmp/spark.log", "/logs/spark.log")
        assert ("/tmp/spark.log", "/logs/spark.log") in a.copies

    def test_notebook_exit_recorded(self):
        a = CapturingPersistenceAdapter()
        a.notebook_exit("summary-json")
        assert "summary-json" in a.exit_values

    def test_multiple_ops(self):
        a = CapturingPersistenceAdapter()
        a.mkdir("/a")
        a.mkdir("/b")
        a.write("/a/1.json", "x")
        a.write("/a/2.json", "y")
        assert len(a.dirs) == 2
        assert len(a.written) == 2

    def test_initial_state_empty(self):
        a = CapturingPersistenceAdapter()
        assert a.dirs == []
        assert a.written == {}
        assert a.copies == []
        assert a.exit_values == []


# TestLocalFilePersistenceAdapter moved to tests/unit/test_local_persistence_adapter.py

# ===========================================================================
# Spark helpers (no-JVM smoke tests)
# ===========================================================================

class TestSparkHelpers:
    def test_stop_spark_log_capture_none_appender(self):
        result = _stop_spark_log_capture(None, None)
        assert result is None

    def test_stop_spark_log_capture_reads_file(self, tmp_path):
        log_file = tmp_path / "spark.log"
        log_file.write_text("line1\nline2\n", encoding="utf-8")
        result = _stop_spark_log_capture(None, str(log_file))
        # appender is None, so close is skipped, but path still read
        assert result is None  # appender=None → early return

    def test_stop_reads_real_file_when_appender_present(self, tmp_path):
        """If appender.close() doesn't raise and file exists, content is returned."""
        log_file = tmp_path / "spark.log"
        log_file.write_text("captured spark logs", encoding="utf-8")

        class _FakeAppender:
            def close(self):
                pass

        result = _stop_spark_log_capture(_FakeAppender(), str(log_file))
        assert result == "captured spark logs"

    def test_stop_returns_none_missing_file(self):
        class _FakeAppender:
            def close(self):
                pass

        result = _stop_spark_log_capture(_FakeAppender(), "/nonexistent/path.log")
        assert result is None


# ===========================================================================
# _ListBufferHandler
# ===========================================================================

class TestListBufferHandler:
    def _make_handler(self, **kwargs):
        from spark_quick_start.spark_logging import _ListBufferHandler
        return _ListBufferHandler(
            static_ctx=kwargs.get("static_ctx", {}),
            ctx_keys=kwargs.get("ctx_keys", []),
            level=kwargs.get("level", logging.DEBUG),
        )

    def test_emit_basic(self):
        h = self._make_handler()
        logger = logging.getLogger("test_emit")
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)
        logger.info("hello world")
        logger.removeHandler(h)
        assert len(h.buffer) == 1
        assert h.buffer[0]["message"] == "hello world"
        assert h.buffer[0]["levelname"] == "INFO"

    def test_static_ctx_merged(self):
        h = self._make_handler(static_ctx={"correlation_id": "abc123"})
        logger = logging.getLogger("test_static_ctx")
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)
        logger.info("msg")
        logger.removeHandler(h)
        assert h.buffer[0]["correlation_id"] == "abc123"

    def test_ctx_keys_picked_from_extra(self):
        h = self._make_handler(ctx_keys=["run_id"])
        logger = logging.getLogger("test_ctx_keys")
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)
        logger.info("msg", extra={"run_id": "run-42"})
        logger.removeHandler(h)
        assert h.buffer[0]["run_id"] == "run-42"

    def test_level_filter(self):
        h = self._make_handler(level=logging.WARNING)
        logger = logging.getLogger("test_level_filter")
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)
        logger.debug("skip me")
        logger.warning("keep me")
        logger.removeHandler(h)
        assert len(h.buffer) == 1
        assert h.buffer[0]["message"] == "keep me"

    def test_extra_simple_types_included(self):
        h = self._make_handler()
        logger = logging.getLogger("test_extra_types")
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)
        logger.info("msg", extra={"custom_int": 42, "custom_bool": True})
        logger.removeHandler(h)
        assert h.buffer[0].get("custom_int") == 42
        assert h.buffer[0].get("custom_bool") is True

    def test_emit_does_not_raise_on_bad_record(self):
        h = self._make_handler()
        # Create a malformed record that has no getMessage
        record = logging.LogRecord("x", logging.INFO, "", 0, None, None, None)
        record.getMessage = lambda: (_ for _ in ()).throw(RuntimeError("bad"))  # type: ignore
        # emit should swallow the error
        h.emit(record)


# ===========================================================================
# LogSession — lifecycle
# ===========================================================================

class TestLogSessionLifecycle:
    def _make_session(self, **kwargs) -> LogSession:
        defaults = dict(
            in_memory=True,
            capture_spark=False,
            capture_stdout=False,
            adapter=NullPersistenceAdapter(),
        )
        defaults.update(kwargs)
        return LogSession(**defaults)

    def test_start_sets_started(self):
        sess = self._make_session()
        sess.start()
        assert sess._started is True

    def test_start_idempotent(self):
        sess = self._make_session()
        sess.start()
        sess.start()
        assert sess._started is True

    def test_finalize_sets_finalized(self):
        sess = self._make_session()
        sess.start()
        sess.finalize(success=True)
        assert sess._finalized is True

    def test_finalize_returns_json_string(self):
        sess = self._make_session()
        sess.start()
        result = sess.finalize(success=True)
        data = json.loads(result)
        assert "run_id" in data

    def test_finalize_idempotent(self):
        sess = self._make_session()
        sess.start()
        r1 = sess.finalize(success=True)
        r2 = sess.finalize(success=True)
        assert r1 == r2

    def test_finalize_not_started_returns_warning(self):
        sess = self._make_session()
        result = sess.finalize(success=True)
        data = json.loads(result)
        assert "warning" in data

    def test_context_manager(self):
        sess = self._make_session()
        with sess as s:
            assert s is sess
            assert s._started is True
        assert sess._finalized is True

    def test_context_manager_success_true_on_no_exc(self):
        sess = self._make_session()
        with sess:
            pass
        payload = json.loads(sess._summary_json)
        assert payload["success"] is True

    def test_context_manager_success_false_on_exc(self):
        sess = self._make_session()
        with pytest.raises(ValueError), sess:
            raise ValueError("oops")
        payload = json.loads(sess._summary_json)
        assert payload["success"] is False

    def test_run_id_is_unique(self):
        s1 = self._make_session()
        s2 = self._make_session()
        assert s1.run_id != s2.run_id

    def test_adapter_defaults_to_null_when_in_memory(self):
        sess = self._make_session()
        assert isinstance(sess._adapter, NullPersistenceAdapter)

    def test_adapter_can_be_injected(self):
        cap = CapturingPersistenceAdapter()
        sess = self._make_session(adapter=cap)
        assert sess._adapter is cap

    def test_correlation_id_stored(self):
        sess = self._make_session(correlation_id="test-corr-123")
        sess.start()
        sess.finalize()
        payload = json.loads(sess._summary_json)
        assert payload["meta"]["correlation_id"] == "test-corr-123"

    def test_notebook_name_stored(self):
        sess = self._make_session(notebook_name="MyNotebook")
        sess.start()
        sess.finalize()
        payload = json.loads(sess._summary_json)
        assert payload["meta"]["notebook_name"] == "MyNotebook"

    def test_spark_log_level_set(self):
        sess = LogSession(
            in_memory=True, capture_spark=False,
            capture_stdout=False, level="WARNING"
        )
        assert sess.spark_log_level == "WARN"


class TestLogSessionLogging:
    """Test that log records are captured into the buffer."""

    def _make_and_start(self, **kwargs) -> LogSession:
        sess = LogSession(
            in_memory=True,
            capture_spark=False,
            capture_stdout=False,
            **kwargs,
        )
        sess.start()
        return sess

    def test_log_messages_captured(self):
        sess = self._make_and_start()
        sess.logger.info("test message")
        sess.finalize()
        payload = json.loads(sess._summary_json)
        messages = [r["message"] for r in payload["python_logs"]]
        assert any("test message" in m for m in messages)

    def test_log_level_filters_debug_when_info(self):
        sess = self._make_and_start(level="INFO")
        sess.logger.debug("should be filtered")
        sess.logger.info("should appear")
        sess.finalize()
        payload = json.loads(sess._summary_json)
        messages = [r["message"] for r in payload["python_logs"]]
        assert not any("should be filtered" in m for m in messages)
        assert any("should appear" in m for m in messages)

    def test_start_log_message_present(self):
        sess = self._make_and_start()
        sess.finalize()
        payload = json.loads(sess._summary_json)
        messages = [r["message"] for r in payload["python_logs"]]
        assert any("LogSession started" in m for m in messages)

    def test_correlation_id_in_records(self):
        sess = self._make_and_start(correlation_id="corr-99")
        sess.logger.info("tagged message")
        sess.finalize()
        payload = json.loads(sess._summary_json)
        for rec in payload["python_logs"]:
            if "tagged message" in rec["message"]:
                assert rec.get("correlation_id") == "corr-99"
                break

    def test_extra_context_keys_in_records(self):
        sess = self._make_and_start(include_context_keys_in_records=["stage"])
        sess.logger.info("step done", extra={"stage": "transform"})
        sess.finalize()
        payload = json.loads(sess._summary_json)
        for rec in payload["python_logs"]:
            if "step done" in rec["message"]:
                assert rec.get("stage") == "transform"
                break

    def test_note_error_included_in_payload(self):
        sess = self._make_and_start()
        sess.note_error("ValueError", "bad input", "Traceback ...")
        sess.finalize(success=False)
        payload = json.loads(sess._summary_json)
        assert payload["error"] is not None
        assert payload["error"]["type"] == "ValueError"

    def test_success_false_recorded(self):
        sess = self._make_and_start()
        sess.finalize(success=False)
        payload = json.loads(sess._summary_json)
        assert payload["success"] is False

    def test_get_logs_json_before_finalize(self):
        sess = self._make_and_start()
        sess.logger.info("before finalize")
        result = sess.get_logs_json()
        data = json.loads(result)
        assert "run_id" in data

    def test_get_logs_returns_dict(self):
        sess = self._make_and_start()
        sess.finalize()
        data = sess.get_logs()
        assert isinstance(data, dict)
        assert "run_id" in data


class TestLogSessionStdoutCapture:
    def test_stdout_captured(self):
        sess = LogSession(
            in_memory=True,
            capture_spark=False,
            capture_stdout=True,
        )
        with sess:
            print("hello stdout")
        payload = json.loads(sess._summary_json)
        assert "hello stdout" in payload["stdout_tail"]

    def test_stderr_captured(self):
        import sys as _sys
        sess = LogSession(
            in_memory=True,
            capture_spark=False,
            capture_stdout=True,
        )
        with sess:
            print("hello stderr", file=_sys.stderr)
        payload = json.loads(sess._summary_json)
        assert "hello stderr" in payload["stderr_tail"]


class TestLogSessionPersist:
    """Tests for always_persist=True with CapturingPersistenceAdapter."""

    def test_persist_writes_json(self):
        cap = CapturingPersistenceAdapter()
        sess = LogSession(
            lakehouse_dir="/logs/test",
            in_memory=False,
            always_persist=True,
            capture_spark=False,
            capture_stdout=False,
            adapter=cap,
        )
        sess.start()
        sess.logger.info("persisted message")
        sess.finalize()

        # Should have written exactly one file
        assert len(cap.written) == 1
        path, content = next(iter(cap.written.items()))
        assert path.startswith("/logs/test/")
        assert path.endswith("_python.json")
        data = json.loads(content)
        assert "python_logs" in data

    def test_persist_mkdir_called(self):
        cap = CapturingPersistenceAdapter()
        sess = LogSession(
            lakehouse_dir="/logs/mydir",
            in_memory=False,
            always_persist=True,
            capture_spark=False,
            capture_stdout=False,
            adapter=cap,
        )
        sess.start()
        sess.finalize()
        assert "/logs/mydir" in cap.dirs

    def test_persist_summary_contains_path(self):
        cap = CapturingPersistenceAdapter()
        sess = LogSession(
            lakehouse_dir="/logs/test",
            in_memory=False,
            always_persist=True,
            capture_spark=False,
            capture_stdout=False,
            adapter=cap,
        )
        sess.start()
        summary_json = sess.finalize()
        summary = json.loads(summary_json)
        assert "full_python_logs_path" in summary
        assert summary["full_python_logs_path"].startswith("/logs/test/")

    def test_auto_exit_calls_notebook_exit(self):
        cap = CapturingPersistenceAdapter()
        sess = LogSession(
            in_memory=True,
            auto_exit=True,
            capture_spark=False,
            capture_stdout=False,
            adapter=cap,
        )
        sess.start()
        # Manually trigger __exit__ without an exception
        sess.__exit__(None, None, None)
        assert len(cap.exit_values) == 1
        data = json.loads(cap.exit_values[0])
        assert "run_id" in data


# ===========================================================================
# Singleton helpers
# ===========================================================================

class TestLogSessionRestartAfterFinalize:
    """F123: a finalized session does not start again, and never leaves stdout redirected."""

    def _session(self) -> LogSession:
        return LogSession(in_memory=True, capture_spark=False, capture_stdout=True)

    def test_start_after_finalize_leaves_stdout_and_stderr_as_they_were(self):
        original_out, original_err = sys.stdout, sys.stderr
        sess = self._session()
        sess.start()
        first = sess.finalize()
        sess.start()
        second = sess.finalize()

        assert sys.stdout is original_out
        assert sys.stderr is original_err
        assert second == first  # the cached summary, as a second finalize() always returned

    def test_start_after_finalize_is_a_no_op(self):
        sess = self._session()
        sess.start()
        sess.finalize()
        base_name = sess.base_name
        assert sess.start() is sess
        assert sess._started is False
        assert sess.base_name == base_name

    def test_init_logs_still_starts_a_new_session_after_finalize(self):
        first = init_logs(in_memory=True, capture_spark=False, capture_stdout=False)
        first.finalize()
        second = init_logs(in_memory=True, capture_spark=False, capture_stdout=False)
        assert second is not first
        assert second._started is True

    def test_get_logger_still_starts_a_new_session_after_finalize(self):
        first = init_logs(in_memory=True, capture_spark=False, capture_stdout=False)
        first.finalize()
        get_logger()
        assert fl._CURRENT is not first
        assert fl._CURRENT._started is True


class TestSingletonHelpers:
    def test_init_logs_returns_session(self):
        sess = init_logs(
            in_memory=True,
            capture_spark=False,
            capture_stdout=False,
        )
        assert isinstance(sess, LogSession)
        assert sess._started is True

    def test_init_logs_idempotent(self):
        s1 = init_logs(in_memory=True, capture_spark=False, capture_stdout=False)
        s2 = init_logs(in_memory=True, capture_spark=False, capture_stdout=False)
        assert s1 is s2

    def test_get_logger_returns_logger(self):
        init_logs(in_memory=True, capture_spark=False, capture_stdout=False)
        log = get_logger()
        assert isinstance(log, logging.Logger)

    def test_get_logger_creates_session_if_none(self):
        # No session started — get_logger auto-creates one
        log = get_logger()
        assert isinstance(log, logging.Logger)
        assert fl._CURRENT is not None

    def test_finalize_logs_returns_json(self):
        init_logs(in_memory=True, capture_spark=False, capture_stdout=False)
        result = finalize_logs(success=True)
        data = json.loads(result)
        assert "run_id" in data

    def test_finalize_logs_creates_session_if_none(self):
        # Auto-created session without adapter defaults to FabricPersistenceAdapter,
        # which requires notebookutils. Use an in-memory session first.
        init_logs(in_memory=True, capture_spark=False, capture_stdout=False)
        result = finalize_logs(success=True)
        data = json.loads(result)
        assert "run_id" in data

    def test_get_logs_json_returns_json_string(self):
        init_logs(in_memory=True, capture_spark=False, capture_stdout=False)
        get_logger().info("test message")
        finalize_logs()
        result = get_logs_json()
        data = json.loads(result)
        assert "python_logs" in data

    def test_get_logs_json_no_session(self):
        result = get_logs_json()
        data = json.loads(result)
        assert "error" in data

    def test_get_logs_returns_dict(self):
        init_logs(in_memory=True, capture_spark=False, capture_stdout=False)
        finalize_logs()
        data = get_logs()
        assert isinstance(data, dict)
        assert "python_logs" in data

    def test_get_logs_no_session(self):
        result = get_logs()
        assert isinstance(result, dict)
        assert "error" in result

    def test_finalize_logs_auto_exit_calls_adapter(self):
        cap = CapturingPersistenceAdapter()
        init_logs(
            in_memory=True,
            capture_spark=False,
            capture_stdout=False,
            adapter=cap,
        )
        finalize_logs(success=True, auto_exit=True)
        assert len(cap.exit_values) == 1

    def test_init_logs_with_adapter(self):
        cap = CapturingPersistenceAdapter()
        sess = init_logs(
            lakehouse_dir="/logs",
            in_memory=False,
            always_persist=True,
            capture_spark=False,
            capture_stdout=False,
            adapter=cap,
        )
        assert sess._adapter is cap

    def test_init_logs_with_correlation_id(self):
        sess = init_logs(
            in_memory=True,
            capture_spark=False,
            capture_stdout=False,
            correlation_id="singleton-corr",
        )
        assert sess.correlation_id == "singleton-corr"


# ===========================================================================
# Error capture
# ===========================================================================

class TestErrorCapture:
    def test_install_error_capture_does_not_raise(self):
        """install_error_capture should be safe to call in any environment."""
        init_logs(in_memory=True, capture_spark=False, capture_stdout=False)
        try:
            install_error_capture(auto_exit=False)
        except Exception as exc:
            pytest.fail(f"install_error_capture raised: {exc}")

    def test_note_error_records_to_session(self):
        sess = LogSession(
            in_memory=True, capture_spark=False, capture_stdout=False
        )
        sess.start()
        sess.note_error("RuntimeError", "something went wrong", "tb text")
        result = sess.finalize(success=False)
        data = json.loads(result)
        assert data["error"]["type"] == "RuntimeError"
        assert data["error"]["message"] == "something went wrong"

    def test_record_and_finalize_from_exc_no_session(self):
        """_record_and_finalize_from_exc should be safe with no active session."""
        try:
            fl._record_and_finalize_from_exc(ValueError("test"), auto_exit=False)
        except Exception as exc:
            pytest.fail(f"Raised unexpectedly: {exc}")

    def test_record_and_finalize_from_exc_with_session(self):
        init_logs(in_memory=True, capture_spark=False, capture_stdout=False)
        try:
            raise ValueError("triggered error")
        except ValueError as exc:
            fl._record_and_finalize_from_exc(exc, auto_exit=False)
        # Session should be finalized
        assert fl._CURRENT is None or fl._CURRENT._finalized is True


# ===========================================================================
# LogArtifacts
# ===========================================================================

class TestLogArtifacts:
    def test_defaults(self):
        a = LogArtifacts(base_name="test_base", run_id="abc123")
        assert a.base_name == "test_base"
        assert a.run_id == "abc123"
        assert a.python_json_path is None
        assert a.spark_driver_log_path is None

    def test_with_paths(self):
        a = LogArtifacts(
            base_name="b",
            run_id="r",
            python_json_path="/logs/b_python.json",
            spark_driver_log_path="/logs/b_spark.log",
        )
        assert a.python_json_path == "/logs/b_python.json"
        assert a.spark_driver_log_path == "/logs/b_spark.log"


# ===========================================================================
# Adapter ABC contract
# ===========================================================================

# TestAdapterContract's per-platform tests moved to
# tests/unit/fabric/test_fabric_persistence_adapter.py and
# tests/unit/databricks/test_databricks_persistence_adapter.py. The local case
# lives in tests/unit/test_local_persistence_adapter.py::TestAdapterContractLocal
# — not tests/unit/local/, since LocalFilePersistenceAdapter ships on every
# platform (see that file's module docstring).

class TestAdapterContract:
    """Verify that the platform-neutral adapters implement the full interface."""

    def test_null_has_required_methods(self):
        a = NullPersistenceAdapter()
        assert callable(a.mkdir)
        assert callable(a.write)
        assert callable(a.copy)
        assert callable(a.notebook_exit)

    def test_capturing_has_required_methods(self):
        a = CapturingPersistenceAdapter()
        assert callable(a.mkdir)
        assert callable(a.write)
        assert callable(a.copy)
        assert callable(a.notebook_exit)


class TestLocalPersistenceNotebookName:
    """F1 — the `__session__` probe, now live code in the sanctioned file.

    It was one of four branches in `_best_effort_notebook_name()`, a private
    function in `spark_logging/__init__.py` with no production caller, whose
    `import dbutils` broke `platform-boundary` rule 1. Three of its branches were
    already implemented in the per-platform persistence adapters; this one was not,
    so it moved here rather than being dropped with the rest.

    Its two previous tests could not fail. With neither IPython nor dbutils
    installed the function always returned None, so `assert name is None or
    isinstance(name, str)` was `None is None`, and the other asserted that a bare
    `except Exception: pass` does not raise. A boundary violation sat behind them
    unnoticed. These assertions can fail.
    """

    def _adapter_with_ipython(self, monkeypatch, user_ns):
        import spark_quick_start.spark_logging.local_persistence as lp

        class _FakeIPython:
            def __init__(self, ns): self.user_ns = ns

        fake = types.ModuleType("IPython")
        fake.get_ipython = lambda: _FakeIPython(user_ns) if user_ns is not None else None
        monkeypatch.setitem(sys.modules, "IPython", fake)
        return lp.LocalFilePersistenceAdapter()

    def test_session_variable_is_used_when_present(self, monkeypatch):
        adapter = self._adapter_with_ipython(monkeypatch, {"__session__": "MyFabricNotebook"})
        assert adapter.get_notebook_name() == "MyFabricNotebook"

    def test_vscode_file_wins_over_session(self, monkeypatch):
        adapter = self._adapter_with_ipython(
            monkeypatch,
            {"__vsc_ipynb_file__": "/a/b/Explore.ipynb", "__session__": "ignored"},
        )
        assert adapter.get_notebook_name() == "Explore.ipynb"

    def test_returns_none_when_neither_is_set(self, monkeypatch):
        adapter = self._adapter_with_ipython(monkeypatch, {})
        assert adapter.get_notebook_name() is None

    def test_returns_none_when_there_is_no_ipython_session(self, monkeypatch):
        adapter = self._adapter_with_ipython(monkeypatch, None)
        assert adapter.get_notebook_name() is None

    def test_returns_none_when_ipython_is_not_installed(self, monkeypatch):
        """The ImportError arm — the path that runs in every non-notebook environment.

        A None entry in sys.modules makes `from IPython import ...` raise ImportError
        rather than searching the path, which is what an absent IPython does. This was
        the only thing the two deleted tautologies nominally exercised, so without it
        the fix would have left the commonest path less covered than before —
        `spark_logging/*` is in the coverage omit list, so nothing else reports it.
        """
        import spark_quick_start.spark_logging.local_persistence as lp

        monkeypatch.setitem(sys.modules, "IPython", None)
        assert lp.LocalFilePersistenceAdapter().get_notebook_name() is None

    def test_a_non_string_session_is_ignored_rather_than_coerced(self, monkeypatch):
        """The isinstance guard. Fabric's __session__ may not be a name.

        This branch was dead code before F1 moved it into a live method, so `str()`
        had never run. Its result reaches the notebook_name column of every persisted
        log row, so coercing a session object would write "<... object at 0x...>"
        where the dead code wrote nothing. Unprovable without a live Fabric workspace,
        which is why the code guards instead of assuming.
        """
        adapter = self._adapter_with_ipython(monkeypatch, {"__session__": object()})
        assert adapter.get_notebook_name() is None


class TestDeprecatedUnreadParameters:
    """F118: ``max_return_chars`` and ``always_persist`` have never been read.

    They are kept so existing notebooks keep running, and warn when given a
    value other than their default. Passing the default explicitly does not
    warn: that is what every existing caller of ``always_persist=True`` does,
    and the default is the behaviour the library has always had.
    """

    @staticmethod
    def _make(factory, **extra):
        return factory(
            in_memory=True, capture_spark=False, capture_stdout=False, **extra
        )

    @pytest.mark.parametrize(
        ("name", "value"),
        [("max_return_chars", 10), ("always_persist", False)],
    )
    @pytest.mark.parametrize("factory", [LogSession, init_logs])
    def test_a_non_default_value_warns_naming_the_parameter(self, factory, name, value):
        with pytest.warns(DeprecationWarning, match=name) as caught:
            sess = self._make(factory, **{name: value})
        try:
            assert len(caught) == 1  # init_logs must not warn again via LogSession
            assert caught[0].filename == __file__  # points at the caller
            assert "never had an effect" in str(caught[0].message)
            assert "removed" in str(caught[0].message)
        finally:
            sess.finalize()

    @pytest.mark.parametrize("factory", [LogSession, init_logs])
    def test_the_defaults_do_not_warn(self, factory):
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            sess = self._make(factory)
            sess.finalize()
            sess2 = self._make(factory, max_return_chars=250_000, always_persist=True)
            sess2.finalize()

    def test_behaviour_is_unchanged_always_persist_false_still_persists(self):
        cap = CapturingPersistenceAdapter()
        with pytest.warns(DeprecationWarning, match="always_persist"):
            sess = LogSession(
                lakehouse_dir="/logs/x",
                in_memory=False,
                always_persist=False,
                capture_spark=False,
                capture_stdout=False,
                adapter=cap,
            )
        sess.start()
        sess.finalize()
        assert len(cap.written) == 1

    def test_the_unread_values_are_not_stored(self):
        sess = LogSession(in_memory=True, capture_spark=False, capture_stdout=False)
        assert not hasattr(sess, "max_return_chars")
        assert not hasattr(sess, "always_persist")
