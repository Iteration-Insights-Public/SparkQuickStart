"""
tests/test_logging_buffer_handler.py — Direct unit tests for _ListBufferHandler.

No Spark, no platform runtime required. All tests are pure Python.
"""
from __future__ import annotations

import logging

import pytest

from spark_quick_start.spark_logging import _ListBufferHandler

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_record(
    msg: str = "test message",
    level: int = logging.INFO,
    name: str = "test_logger",
    extra: dict | None = None,
) -> logging.LogRecord:
    record = logging.LogRecord(
        name=name,
        level=level,
        pathname="",
        lineno=0,
        msg=msg,
        args=(),
        exc_info=None,
    )
    if extra:
        for k, v in extra.items():
            setattr(record, k, v)
    return record


def _make_handler(**kwargs) -> _ListBufferHandler:
    defaults = {"static_ctx": {}, "ctx_keys": []}
    defaults.update(kwargs)
    return _ListBufferHandler(**defaults)


# ---------------------------------------------------------------------------
# Strip fields
# ---------------------------------------------------------------------------

class TestStripFields:
    def test_strip_fields_removes_name(self):
        handler = _make_handler(
            static_ctx={"correlation_id": "corr-1", "run_id": "abc123"},
            strip_fields=frozenset({"name", "run_id", "correlation_id"}),
        )
        handler.emit(_make_record(extra={"run_id": "abc123"}))
        entry = handler.buffer[0]
        assert "name" not in entry
        assert "run_id" not in entry
        assert "correlation_id" not in entry

    def test_strip_fields_empty_preserves_all_standard_fields(self):
        handler = _make_handler(static_ctx={"correlation_id": "corr-1"}, strip_fields=frozenset())
        handler.emit(_make_record())
        entry = handler.buffer[0]
        assert "name" in entry
        assert "correlation_id" in entry

    def test_strip_nonexistent_field_is_noop(self):
        handler = _make_handler(strip_fields=frozenset({"nonexistent_field"}))
        handler.emit(_make_record())
        # Must not raise; buffer must have an entry
        assert len(handler.buffer) == 1

    def test_strip_applied_after_all_accumulation(self):
        """run_id supplied via extra= is removed by strip_fields."""
        handler = _make_handler(
            static_ctx={},
            strip_fields=frozenset({"run_id"}),
        )
        handler.emit(_make_record(extra={"run_id": "xyz"}))
        entry = handler.buffer[0]
        assert "run_id" not in entry


# ---------------------------------------------------------------------------
# Tags and metrics
# ---------------------------------------------------------------------------

class TestTagsAndMetrics:
    def test_tags_in_extra_appear_in_entry(self):
        handler = _make_handler()
        record = _make_record(extra={"tags": {"entity": "Person", "phase": "merge"}})
        handler.emit(record)
        entry = handler.buffer[0]
        assert entry["tags"] == {"entity": "Person", "phase": "merge"}

    def test_metrics_in_extra_appear_in_entry(self):
        handler = _make_handler()
        metrics = [{"metricName": "inserted", "metricValue": 42}]
        record = _make_record(extra={"metrics": metrics})
        handler.emit(record)
        entry = handler.buffer[0]
        assert entry["metrics"] == metrics

    def test_tags_wrong_type_silently_dropped(self):
        handler = _make_handler()
        record = _make_record(extra={"tags": "not_a_dict"})
        handler.emit(record)
        entry = handler.buffer[0]
        assert "tags" not in entry

    def test_metrics_wrong_type_silently_dropped(self):
        handler = _make_handler()
        record = _make_record(extra={"metrics": {"not": "a_list"}})
        handler.emit(record)
        entry = handler.buffer[0]
        assert "metrics" not in entry

    def test_empty_tags_dict_not_included(self):
        handler = _make_handler()
        record = _make_record(extra={"tags": {}})
        handler.emit(record)
        entry = handler.buffer[0]
        assert "tags" not in entry

    def test_empty_metrics_list_not_included(self):
        handler = _make_handler()
        record = _make_record(extra={"metrics": []})
        handler.emit(record)
        entry = handler.buffer[0]
        assert "metrics" not in entry

    def test_tags_and_metrics_both_absent_by_default(self):
        handler = _make_handler()
        handler.emit(_make_record())
        entry = handler.buffer[0]
        assert "tags" not in entry
        assert "metrics" not in entry

    def test_tags_and_metrics_together(self):
        handler = _make_handler()
        metrics = [{"metricName": "rows", "metricValue": 100}]
        record = _make_record(extra={"tags": {"phase": "load"}, "metrics": metrics})
        handler.emit(record)
        entry = handler.buffer[0]
        assert entry["tags"] == {"phase": "load"}
        assert entry["metrics"] == metrics


# ---------------------------------------------------------------------------
# Resilience
# ---------------------------------------------------------------------------

class TestEmitResilience:
    def test_emit_never_raises(self):
        handler = _make_handler()
        # Corrupt record — should not raise
        bad_record = object()  # type: ignore[arg-type]
        try:
            handler.emit(bad_record)  # type: ignore[arg-type]
        except Exception as exc:
            pytest.fail(f"emit() raised unexpectedly: {exc}")

    def test_standard_fields_always_present(self):
        handler = _make_handler()
        handler.emit(_make_record(msg="hello"))
        entry = handler.buffer[0]
        assert "asctime" in entry
        assert "levelname" in entry
        assert "message" in entry
        assert entry["message"] == "hello"

    def test_static_ctx_merged_into_entry(self):
        handler = _make_handler(static_ctx={"pipeline": "EDW", "run_id": "abc"})
        handler.emit(_make_record())
        entry = handler.buffer[0]
        assert entry["pipeline"] == "EDW"
        assert entry["run_id"] == "abc"
