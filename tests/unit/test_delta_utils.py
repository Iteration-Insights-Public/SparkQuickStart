"""
Tests for run_with_delta_retry, delete_staging_data, and write_staging_data.

No Spark required for retry and delete tests.
write_staging_data tests use a fully mocked DataFrame — delta-spark is NOT required.
"""
from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

from spark_quick_start.adapters.platform_adapter import (
    MockPlatformAdapter,
)
from spark_quick_start.orchestration.delta_utils import (
    delete_staging_data,
    run_with_delta_retry,
    write_staging_data,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _null_logger(name: str = "test_delta") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.addHandler(logging.NullHandler())
    return logger


# ---------------------------------------------------------------------------
# TestRunWithDeltaRetry
# ---------------------------------------------------------------------------

class TestRunWithDeltaRetry:
    def test_succeeds_on_first_attempt(self):
        """op() returns 'ok' immediately; result=='ok', called once."""
        calls = []

        def op():
            calls.append(1)
            return "ok"

        result = run_with_delta_retry(op, max_attempts=3)
        assert result == "ok"
        assert len(calls) == 1

    def test_retries_on_concurrent_append(self):
        """op() raises DELTA_CONCURRENT_APPEND twice then returns 'ok'; result=='ok', called 3 times."""
        calls = []

        def op():
            calls.append(1)
            if len(calls) < 3:
                raise Exception("DELTA_CONCURRENT_APPEND error")
            return "ok"

        with patch("spark_quick_start.orchestration.delta_utils.time.sleep"):
            result = run_with_delta_retry(op, max_attempts=5, base_sleep=0.0)

        assert result == "ok"
        assert len(calls) == 3

    def test_raises_immediately_on_non_conflict(self):
        """op() raises ValueError('other'); re-raised without retry."""
        def op():
            raise ValueError("other")

        with pytest.raises(ValueError, match="other"):
            run_with_delta_retry(op, max_attempts=5)

    def test_raises_after_max_attempts(self):
        """op() always raises DELTA_CONCURRENT_APPEND; raises after max_attempts=3."""
        def op():
            raise Exception("DELTA_CONCURRENT_APPEND")

        with patch("spark_quick_start.orchestration.delta_utils.time.sleep"):
            with pytest.raises(Exception, match="DELTA_CONCURRENT_APPEND"):
                run_with_delta_retry(op, max_attempts=3, base_sleep=0.0)

    def test_max_attempts_respected(self):
        """Call count equals max_attempts when all attempts raise a conflict error."""
        calls = []

        def op():
            calls.append(1)
            raise Exception("DELTA_CONCURRENT_APPEND")

        with patch("spark_quick_start.orchestration.delta_utils.time.sleep"):
            with pytest.raises(Exception):
                run_with_delta_retry(op, max_attempts=3, base_sleep=0.0)

        assert len(calls) == 3


# ---------------------------------------------------------------------------
# TestDeleteStagingData
# ---------------------------------------------------------------------------

class TestDeleteStagingData:
    def test_calls_adapter_delete_path(self):
        """After call, adapter.deleted_paths contains the target path."""
        adapter = MockPlatformAdapter()
        delete_staging_data("/tmp/staging/entity", adapter, _null_logger())
        assert "/tmp/staging/entity" in adapter.deleted_paths

    def test_swallows_adapter_exception(self):
        """Adapter raises FileNotFoundError; delete_staging_data does NOT re-raise."""
        adapter = MockPlatformAdapter(raise_on_delete=True)
        # Must not raise — the function is best-effort
        delete_staging_data("/tmp/nonexistent", adapter, _null_logger())

    def test_logs_on_success(self):
        """No exception raised when logger is a MagicMock."""
        adapter = MockPlatformAdapter()
        mock_logger = MagicMock()
        delete_staging_data("/tmp/ok", adapter, mock_logger)  # should not raise
        mock_logger.info.assert_called()


# ---------------------------------------------------------------------------
# TestWriteStagingData
# ---------------------------------------------------------------------------

class TestWriteStagingData:
    """write_staging_data reads its row count from Delta's own write-commit
    metadata (capture_write_statistics) instead of calling df.count() — no
    cache/unpersist dance needed any more, so these mock capture_write_statistics
    directly rather than df.count()/df.cache()."""

    def test_returns_row_count(self):
        """Row count comes from capture_write_statistics's return value."""
        adapter = MockPlatformAdapter()
        df = MagicMock()
        with patch(
            "spark_quick_start.orchestration.delta_utils.capture_write_statistics",
            return_value=5,
        ):
            result = write_staging_data(df, "/tmp/delta/entity", adapter, _null_logger())
        assert result == 5
        assert not df.method_calls

    def test_write_failure_propagates(self):
        """adapter.write_delta_overwrite raising is not swallowed."""
        adapter = MockPlatformAdapter()
        adapter.write_delta_overwrite = MagicMock(side_effect=RuntimeError("write failed"))
        df = MagicMock()

        with pytest.raises(RuntimeError, match="write failed"):
            write_staging_data(df, "/tmp/delta/entity", adapter, _null_logger())
        assert not df.method_calls

    def test_no_cache_or_count_called(self):
        """Regression: no df.cache()/df.count() — the whole point of reading
        row count from Delta commit metadata is to avoid the extra pass(es)
        over the data those calls used to force."""
        adapter = MockPlatformAdapter()
        df = MagicMock()
        with patch(
            "spark_quick_start.orchestration.delta_utils.capture_write_statistics",
            return_value=5,
        ):
            write_staging_data(df, "/tmp/delta/entity", adapter, _null_logger())

        assert not df.method_calls


# ---------------------------------------------------------------------------
# TestWriteStagingDataWithAdapter
# ---------------------------------------------------------------------------

class TestWriteStagingDataWithAdapter:
    """write_staging_data now requires an adapter for UC/path routing."""

    def test_calls_adapter_write_delta_overwrite(self):
        """adapter.write_delta_overwrite is called with the staging path."""
        adapter = MockPlatformAdapter()
        df = MagicMock()

        with patch(
            "spark_quick_start.orchestration.delta_utils.capture_write_statistics",
            return_value=7,
        ):
            result = write_staging_data(df, "/tmp/delta/entity", adapter, _null_logger())

        assert adapter.write_delta_overwrite_calls == [("/tmp/delta/entity", False)]
        assert result == 7
        assert not df.method_calls

    def test_skips_pre_version_lookup_when_table_does_not_exist(self):
        """First-ever write to a fresh path: delta_table_exists()==False (the
        MockPlatformAdapter default), so resolve_delta_table()/history() must
        not be called — capture_write_statistics gets pre_version=-1 directly."""
        adapter = MockPlatformAdapter()
        df = MagicMock()

        with patch(
            "spark_quick_start.orchestration.delta_utils.capture_write_statistics",
            return_value=3,
        ) as mock_capture:
            write_staging_data(df, "/tmp/delta/entity", adapter, _null_logger())

        assert adapter.resolve_delta_table_calls == []
        mock_capture.assert_called_once()
        assert mock_capture.call_args.args[2] == -1  # pre_version
        assert not df.method_calls

    def test_uc_name_routed_via_adapter(self):
        """UC name is passed directly to adapter.write_delta_overwrite — no path check in delta_utils."""
        adapter = MockPlatformAdapter()
        df = MagicMock()

        with patch(
            "spark_quick_start.orchestration.delta_utils.capture_write_statistics",
            return_value=2,
        ):
            write_staging_data(
                df,
                "catalog.schema.Silver_Orders__NB_Stage",
                adapter,
                _null_logger(),
            )

        assert adapter.write_delta_overwrite_calls == [
            ("catalog.schema.Silver_Orders__NB_Stage", False)
        ]
        assert not df.method_calls


class TestDeleteStagingDataOnlyDeletesDeltaTables:
    """The staging pre-delete is recursive and comes from configuration.

    global_context["staging_table_path"] decides what gets removed. A mistyped
    path, or one copied from another environment, lands on an unrelated
    directory far more often than on a Delta table — so when a SparkSession is
    available to answer the question, confirm before deleting.
    """

    # MockPlatformAdapter throughout: `existing_tables` answers delta_table_exists per
    # path, `deleted_paths` records deletions, `delta_table_exists_calls` records probes,
    # and `is_table_identifier` is the real inherited routing.

    def test_deletes_a_real_delta_table(self, tmp_path):
        target = tmp_path / "staging"
        target.mkdir()
        adapter = MockPlatformAdapter(existing_tables={str(target)})

        delete_staging_data(str(target), adapter, _null_logger(), spark=MagicMock())

        assert adapter.deleted_paths == [str(target)]

    def test_refuses_an_existing_non_delta_directory(self, tmp_path):
        victim = tmp_path / "important_data"
        victim.mkdir()
        (victim / "keepme.txt").write_text("do not delete")
        adapter = MockPlatformAdapter()

        with pytest.raises(ValueError, match="not a Delta table"):
            delete_staging_data(str(victim), adapter, _null_logger(), spark=MagicMock())

        assert adapter.deleted_paths == []
        assert (victim / "keepme.txt").exists()

    def test_no_ops_when_nothing_is_there(self, tmp_path):
        adapter = MockPlatformAdapter()

        delete_staging_data(str(tmp_path / "never_created"), adapter,
                            _null_logger(), spark=MagicMock())

        assert adapter.deleted_paths == []

    def test_without_spark_the_previous_behaviour_is_preserved(self, tmp_path):
        """The guard needs a SparkSession. Absent one, do not silently change
        behaviour — delete as before, and say in the log that the check was
        skipped so the gap is visible."""
        victim = tmp_path / "not_a_delta_table"
        victim.mkdir()
        adapter = MockPlatformAdapter()

        delete_staging_data(str(victim), adapter, _null_logger())

        assert adapter.deleted_paths == [str(victim)]
        assert adapter.delta_table_exists_calls == []

    def test_uc_name_still_skips_entirely(self):
        """UC names were always a documented no-op — write_staging_data
        overwrites them — and the guard must not change that."""
        adapter = MockPlatformAdapter()

        delete_staging_data("catalog.schema.staging_tbl", adapter,
                            _null_logger(), spark=MagicMock())

        assert adapter.deleted_paths == []
        assert adapter.delta_table_exists_calls == []
