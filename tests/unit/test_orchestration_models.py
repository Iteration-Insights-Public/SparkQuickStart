"""
tests/test_orchestration_models.py — Unit tests for orchestration/models.py.

No Spark required. All tests are pure Python.
"""
from __future__ import annotations

import copy
import json
import pickle
from dataclasses import asdict
from datetime import UTC, datetime, timedelta

import pytest

from spark_quick_start.orchestration.models import (
    Constants,
    ErrorDetails,
    ExecutionMetrics,
    ExecutionResult,
    ExtensionsContext,
    GlobalContext,
    LoggingContext,
    PlatformContext,
    RuntimeContext,
    SCDContext,
    StagingContext,
)

_FLAG_KEYS = ("current_flag_value", "inactive_flag_value", "deleted_flag_value",
              "not_deleted_flag_value", "source_deleted_flag_value")

# ============================================================================
# CONSTANTS
# ============================================================================


class TestConstants:
    def test_scd_column_names(self):
        assert Constants.COL_ROW_KEY == "scdRowKey"
        assert Constants.COL_CURRENT_FLAG == "scdCurrentFlag"
        assert Constants.COL_DELETED_FLAG == "scdDeletedFlag"

    def test_flag_values(self):
        assert Constants.FLAG_CURRENT == "Y"
        assert Constants.FLAG_INACTIVE == "N"

    def test_the_unused_default_partition_count_is_gone(self):
        """F106: nothing read DEFAULT_NUM_PARTITIONS; both copies of Constants drop it (CLAUDE.md §9)."""
        from spark_quick_start.orchestration.transformers import _constants

        assert not hasattr(Constants, "DEFAULT_NUM_PARTITIONS")
        assert not hasattr(_constants.Constants, "DEFAULT_NUM_PARTITIONS")

    def test_exclude_columns(self):
        assert "NotebookName" in Constants.EXCLUDE_COLUMNS
        assert "NB_BI_Notebook_Name" not in Constants.EXCLUDE_COLUMNS


# ============================================================================
# ERROR DETAILS
# ============================================================================


class TestErrorDetails:
    def test_defaults(self):
        ed = ErrorDetails(error_type="ValueError", error_message="bad value")
        assert ed.traceback is None
        assert ed.context is None

    def test_full(self):
        ed = ErrorDetails(
            error_type="RuntimeError",
            error_message="something broke",
            traceback="Traceback (most recent call last):\n  ...",
            context={"key": "value"},
        )
        assert ed.error_type == "RuntimeError"
        assert ed.error_message == "something broke"
        assert "Traceback" in ed.traceback
        assert ed.context["key"] == "value"

    def test_to_dict_has_exactly_the_three_keys_pipelines_receive(self):
        """F114: `context` is left out when None, so the JSON shape is what it always was."""
        ed = ErrorDetails(error_type="ValueError", error_message="bad", traceback="tb")
        assert ed.to_dict() == {"error_type": "ValueError", "error_message": "bad", "traceback": "tb"}

    def test_to_dict_keeps_a_none_traceback_and_adds_context_only_when_set(self):
        assert ErrorDetails("E", "m").to_dict() == {"error_type": "E", "error_message": "m", "traceback": None}
        assert ErrorDetails("E", "m", "", {"k": 1}).to_dict()["context"] == {"k": 1}

    def test_from_exception(self):
        ed = ErrorDetails.from_exception(KeyError("k"), "tb text")
        assert ed == ErrorDetails("KeyError", "'k'", "tb text")


# ============================================================================
# EXECUTION METRICS
# ============================================================================


class TestExecutionMetrics:
    def test_defaults(self):
        m = ExecutionMetrics()
        assert m.source_count == 0
        assert m.duration_seconds == 0.0
        assert m.hwm_value_before is None
        assert m.hwm_value_after is None

    def test_hwm_fields_in_asdict(self):
        m = ExecutionMetrics(
            hwm_value_before="2024-01-15T10:00:00",
            hwm_value_after="2024-06-04T12:30:00",
        )
        d = asdict(m)
        assert d["hwm_value_before"] == "2024-01-15T10:00:00"
        assert d["hwm_value_after"] == "2024-06-04T12:30:00"

    def test_hwm_none_serialises_as_null(self):
        m = ExecutionMetrics()
        d = json.loads(json.dumps(asdict(m)))
        assert d["hwm_value_before"] is None
        assert d["hwm_value_after"] is None

    def test_field_assignment(self):
        m = ExecutionMetrics()
        m.source_count = 42
        d = asdict(m)
        assert d["source_count"] == 42


# ============================================================================
# EXECUTION RESULT
# ============================================================================


class TestExecutionResult:
    def test_to_json_and_from_json(self):
        result = ExecutionResult(
            status="success",
            has_error=False,
            correlation_id="corr-abc",
        )
        json_str = result.to_json()
        restored = ExecutionResult.from_json(json_str)
        assert restored.status == "success"
        assert restored.has_error is False
        assert restored.correlation_id == "corr-abc"

    def test_from_json_unknown_fields_ignored(self):
        data = {
            "status": "success",
            "has_error": False,
            "correlation_id": "x",
            "future_unknown_field": "ignored",
        }
        # Must not raise
        r = ExecutionResult.from_json(json.dumps(data))
        assert r.status == "success"

    def test_has_error_true(self):
        result = ExecutionResult(status="error", has_error=True)
        json_str = result.to_json()
        data = json.loads(json_str)
        assert data["status"] == "error"
        assert data["has_error"] is True

    def test_stages_defaults_to_empty_list(self):
        result = ExecutionResult(status="success", has_error=False)
        assert result.stages == []

    def test_stages_round_trips_through_json_as_list_of_dicts(self):
        result = ExecutionResult(
            status="success", has_error=False,
            stages=[{"stage": "StageA", "duration_ms": 5}],
        )
        restored = ExecutionResult.from_json(result.to_json())
        assert restored.stages == [{"stage": "StageA", "duration_ms": 5}]


# ============================================================================
# GLOBAL CONTEXT
# ============================================================================


class TestGlobalContext:
    def _make(self, overrides=None):
        data = {
            "id": "ent_1",
            "entity_name": "Person",
            "source_system": "SRC",
            "target_table_path": "/tmp/target",
            "staging_table_path": "/tmp/stage",
        }
        if overrides:
            data.update(overrides)
        return GlobalContext(data)

    def test_required_fields_present(self):
        ctx = self._make()
        ctx.validate()  # must not raise

    def test_missing_entity_name_raises(self):
        ctx = self._make({"entity_name": ""})
        with pytest.raises(ValueError, match="entity_name"):
            ctx.validate()

    def test_missing_id_raises_naming_the_key(self):
        data = {"entity_name": "Person", "source_system": "SRC", "target_table_path": "/t",
                "staging_table_path": "/s"}
        with pytest.raises(ValueError, match=r"global_context parameters: id\b"):
            GlobalContext(data).validate()

    @pytest.mark.parametrize("bad", ["", "  ", "a-b", "a b", "a/b", "a.b", "é1", "a\n", 7])
    def test_an_id_that_is_not_letters_digits_underscores_raises(self, bad):
        with pytest.raises(ValueError, match="id"):
            self._make({"id": bad}).validate()

    def test_missing_source_system_raises(self):
        ctx = self._make({"source_system": None})
        with pytest.raises(ValueError, match="source_system"):
            ctx.validate()

    def test_an_unknown_key_is_refused_with_the_way_on(self):
        with pytest.raises(ValueError) as exc:
            self._make({"my_param": "my_value"})
        message = str(exc.value)
        assert "my_param" in message
        assert "staging_table_path" in message  # the accepted keys are listed
        assert "extensions_context" in message  # and where user data goes

    def test_every_unknown_key_is_named_at_once(self):
        with pytest.raises(ValueError) as exc:
            self._make({"first_typo": 1, "second_typo": 2})
        assert "first_typo" in str(exc.value) and "second_typo" in str(exc.value)

    def test_a_misspelt_reserved_key_is_refused_not_defaulted(self):
        with pytest.raises(ValueError, match="notebook_timeout_second"):
            self._make({"notebook_timeout_second": 5})

    def test_the_library_paths_are_not_accepted_here(self):
        with pytest.raises(ValueError, match="data_storage_lakehouse_path"):
            self._make({"data_storage_lakehouse_path": "/x"})

    def test_custom_params_are_gone(self):
        ctx = self._make()
        assert not hasattr(ctx, "custom_params")
        assert not hasattr(ctx, "get_custom_param")
        assert not hasattr(ctx, "has_custom_param")

    def test_default_load_timestamp_set(self):
        ctx = self._make()
        assert ctx.load_timestamp
        assert isinstance(ctx.load_timestamp, str)
        assert len(ctx.load_timestamp) > 0

    # ---- load_timestamp (F106): the run's load time T ----

    def test_default_load_timestamp_is_utc_with_microseconds(self):
        ctx = self._make()
        parsed = ctx.load_timestamp_utc
        assert parsed.tzinfo is not None and parsed.utcoffset() == timedelta(0)
        assert len(ctx.load_timestamp.split(".")[-1].split("+")[0]) == 6, ctx.load_timestamp
        ctx.validate()

    def test_the_default_load_timestamp_is_fixed_once_per_context(self):
        ctx = self._make()
        assert ctx.load_timestamp_utc == ctx.load_timestamp_utc

    @pytest.mark.parametrize("raw, expected", [
        ("2026-10-03T12:00:00Z", datetime(2026, 10, 3, 12, 0, tzinfo=UTC)),
        ("2026-10-03T12:00:00+00:00", datetime(2026, 10, 3, 12, 0, tzinfo=UTC)),
        ("2026-10-03T12:00:00.123456Z", datetime(2026, 10, 3, 12, 0, 0, 123456, tzinfo=UTC)),
        # ADF / Fabric utcNow() writes seven fractional digits
        ("2026-10-03T12:00:00.1234567Z", datetime(2026, 10, 3, 12, 0, 0, 123456, tzinfo=UTC)),
        ("2026-10-03 12:00:00.5+00:00", datetime(2026, 10, 3, 12, 0, 0, 500000, tzinfo=UTC)),
        # a non-UTC offset names the same instant
        ("2026-10-03T14:00:00+02:00", datetime(2026, 10, 3, 12, 0, tzinfo=UTC)),
    ])
    def test_load_timestamp_is_read_as_a_utc_instant(self, raw, expected):
        ctx = self._make({"load_timestamp": raw})
        ctx.validate()
        assert ctx.load_timestamp_utc == expected
        assert ctx.load_timestamp_utc.utcoffset() == timedelta(0)

    @pytest.mark.parametrize("raw, fragment", [
        ("2026-10-03T12:00:00", "Z or +00:00"),
        ("2026-10-03 12:00:00.123", "Z or +00:00"),
        ("2026-10-03", "date"),
        ("20261003", "date"),
        ("yesterday", "ISO 8601"),
        ("", "ISO 8601"),
        (1759492800, "string"),
        (None, "string"),
        (True, "string"),
    ])
    def test_a_load_timestamp_that_names_no_instant_is_refused(self, raw, fragment):
        ctx = self._make({"load_timestamp": raw})
        with pytest.raises(ValueError, match="global_context.load_timestamp") as excinfo:
            ctx.validate()
        assert fragment in str(excinfo.value)

    def test_stale_parallel_execution_enabled_raises(self):
        with pytest.raises(ValueError, match="moved to runtime_context"):
            self._make({"parallel_execution_enabled": 1})


# ============================================================================
# STAGING CONTEXT
# ============================================================================


class TestStagingContext:
    def test_valid_stages(self):
        ctx = StagingContext({"stages": [{"nb_name": "NB_Stage_Person"}]})
        ctx.validate()  # must not raise

    def test_empty_stages_no_dqd_raises(self):
        ctx = StagingContext({"stages": []})
        with pytest.raises(ValueError, match="staging_context.stages"):
            ctx.validate(has_dqd=False)

    def test_empty_stages_with_dqd_passes(self):
        ctx = StagingContext({"stages": []})
        ctx.validate(has_dqd=True)  # must not raise

    def test_stage_missing_nb_name_raises(self):
        ctx = StagingContext({"stages": [{"not_nb_name": "oops"}]})
        with pytest.raises(ValueError, match="nb_name"):
            ctx.validate()


# ============================================================================
# SCD CONTEXT
# ============================================================================


class TestSCDContext:
    def test_missing_business_key_raises(self):
        with pytest.raises(ValueError, match="business_key_column"):
            SCDContext({"business_key_column": ""})

    def test_blank_business_key_raises(self):
        with pytest.raises(ValueError, match="business_key_column"):
            SCDContext({"business_key_column": "  "})

    def test_defaults_from_constants(self):
        ctx = SCDContext({"business_key_column": "id"})
        assert ctx.source_system_column == Constants.COL_SOURCE_SYSTEM

    def test_load_type_default(self):
        ctx = SCDContext({"business_key_column": "id"})
        assert ctx.load_type == "Incremental"

    def test_scd_type_default(self):
        ctx = SCDContext({"business_key_column": "id"})
        assert ctx.scd_type == "scd2"

    # ---- default_max_timestamp (F106): the close-out sentinel, an instant in UTC ----

    def test_default_max_timestamp_default_is_read_as_utc(self):
        ctx = SCDContext({"business_key_column": "id"})
        ctx.validate()
        assert ctx.default_max_timestamp_utc == datetime(2999, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)

    @pytest.mark.parametrize("raw, expected", [
        # naive: the shipped default is naive, and is read as UTC
        ("2999-12-31 23:59:59.999999", datetime(2999, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)),
        ("2500-01-01T00:00:00", datetime(2500, 1, 1, tzinfo=UTC)),
        ("2500-01-01T00:00:00Z", datetime(2500, 1, 1, tzinfo=UTC)),
        ("2500-01-01T02:00:00+02:00", datetime(2500, 1, 1, tzinfo=UTC)),
    ])
    def test_default_max_timestamp_naive_is_utc(self, raw, expected):
        ctx = SCDContext({"business_key_column": "id", "default_max_timestamp": raw})
        ctx.validate()
        assert ctx.default_max_timestamp_utc == expected

    @pytest.mark.parametrize("raw", ["2999-12-31", "never", "", 99991231, None])
    def test_an_unreadable_default_max_timestamp_is_refused(self, raw):
        ctx = SCDContext({"business_key_column": "id", "default_max_timestamp": raw})
        if raw is None:  # `null` is "not given" in this class (F156)
            ctx.validate()
            return
        with pytest.raises(ValueError, match="scd_context.default_max_timestamp"):
            ctx.validate()

    # ---- the written flag vocabularies (F106) ----

    def test_flag_value_defaults(self):
        ctx = SCDContext({"business_key_column": "id"})
        assert (ctx.current_flag_value, ctx.inactive_flag_value) == ("Y", "N")
        assert (ctx.deleted_flag_value, ctx.not_deleted_flag_value) == ("Y", "N")
        assert ctx.source_deleted_flag_value == "Y"
        ctx.validate()

    def test_the_source_value_follows_the_resolved_deleted_value_unless_given(self):
        assert SCDContext({"business_key_column": "id", "deleted_flag_value": "D"}).source_deleted_flag_value == "D"
        both = SCDContext({"business_key_column": "id", "deleted_flag_value": "D",
                           "source_deleted_flag_value": "X"})
        assert (both.deleted_flag_value, both.source_deleted_flag_value) == ("D", "X")

    @pytest.mark.parametrize("key", _FLAG_KEYS)
    def test_a_null_flag_value_is_not_given(self, key):
        ctx = SCDContext({"business_key_column": "id", key: None})
        ctx.validate()
        assert getattr(ctx, key) in {"Y", "N"}

    @pytest.mark.parametrize("key", _FLAG_KEYS)
    @pytest.mark.parametrize("value", ["", 1, True, ["Y"]])
    def test_a_flag_value_that_is_not_a_non_empty_string_is_refused(self, key, value):
        ctx = SCDContext({"business_key_column": "id", key: value})
        with pytest.raises(ValueError, match=f"scd_context.{key}") as excinfo:
            ctx.validate()
        assert "non-empty string" in str(excinfo.value)

    @pytest.mark.parametrize("key", _FLAG_KEYS)
    @pytest.mark.parametrize("value", ["a'b", "a\\b", "a`b", "a$b", "a\nb", "a\x7fb", "a\x00b"])
    def test_a_flag_value_that_could_break_the_merge_sql_is_refused(self, key, value):
        ctx = SCDContext({"business_key_column": "id", key: value})
        with pytest.raises(ValueError, match=f"scd_context.{key}") as excinfo:
            ctx.validate()
        assert repr(value) in str(excinfo.value)

    @pytest.mark.parametrize("data", [{"current_flag_value": "1", "inactive_flag_value": "1"},
                                      {"current_flag_value": "N"},          # the inactive default
                                      {"inactive_flag_value": "Y"}])        # the current default
    def test_a_current_flag_that_equals_the_inactive_flag_is_refused(self, data):
        with pytest.raises(ValueError, match="current_flag_value.*inactive_flag_value"):
            SCDContext({"business_key_column": "id", **data}).validate()

    @pytest.mark.parametrize("data", [{"deleted_flag_value": "1", "not_deleted_flag_value": "1"},
                                      {"deleted_flag_value": "N"},
                                      {"not_deleted_flag_value": "Y"}])
    def test_a_deleted_flag_that_equals_the_not_deleted_flag_is_refused(self, data):
        with pytest.raises(ValueError, match="deleted_flag_value.*not_deleted_flag_value"):
            SCDContext({"business_key_column": "id", **data}).validate()

    def test_a_custom_vocabulary_validates(self):
        SCDContext({"business_key_column": "id", "current_flag_value": "1", "inactive_flag_value": "0",
                    "deleted_flag_value": "D", "not_deleted_flag_value": "A",
                    "source_deleted_flag_value": "X"}).validate()

    def test_the_source_value_is_allowed_under_a_data_vault_style(self):
        SCDContext({"business_key_column": "id", "scd_type": "dv_satellite",
                    "source_deleted_flag_value": "D"}).validate()

    # ---- hash_separator applies to every style (F106) ----

    @pytest.mark.parametrize("style", ["scd1", "scd2", "dv_hub", "dv_satellite"])
    @pytest.mark.parametrize("value", ["", "a'b", "a\\b", "a`b", "a$b", "a\nb", "a\x7fb", 7])
    def test_an_unusable_hash_separator_is_refused_under_every_style(self, style, value):
        data = {"business_key_column": "id", "scd_type": style, "hash_separator": value}
        with pytest.raises(ValueError, match="scd_context.hash_separator"):
            SCDContext(data).validate()

    @pytest.mark.parametrize("style, expected", [("scd2", "~"), ("scd1", "~"), ("dv_hub", "||")])
    def test_the_hash_separator_defaults_by_style_and_validates(self, style, expected):
        ctx = SCDContext({"business_key_column": "id", "scd_type": style})
        ctx.validate()
        assert ctx.hash_separator == expected

    @pytest.mark.parametrize("value", ["|", "::", "\u241f"])
    def test_a_usable_hash_separator_validates_under_an_scd_style(self, value):
        SCDContext({"business_key_column": "id", "hash_separator": value}).validate()

    def test_the_business_key_may_be_the_row_key(self):
        """F148 — case 1: the staged key is the target's row key under one name."""
        SCDContext({"business_key_column": "scdRowKey", "row_key_column": "scdRowKey"}).validate()
        SCDContext({"business_key_column": "scdRowKey"}).validate()  # the default row key

    def test_a_business_key_that_is_the_row_key_refuses_row_hashing(self):
        """F148 — the key passes through as staged, so hashing would silently do nothing."""
        ctx = SCDContext({"business_key_column": "scdRowKey", "scd_row_hashing": True})
        with pytest.raises(ValueError, match="scd_row_hashing.*no effect"):
            ctx.validate()

    @pytest.mark.parametrize("hashing", [False, True])
    def test_a_row_key_spelt_as_the_business_key_but_for_case_is_refused(self, hashing):
        """F148 review — under Spark's default case-insensitive resolution it is the key itself,
        rewritten (or hashed) before the target is matched on it."""
        ctx = SCDContext({"business_key_column": "scdRowKey", "row_key_column": "ScdRowKey",
                          "scd_row_hashing": hashing})
        with pytest.raises(ValueError, match="only by case"):
            ctx.validate()

    def test_row_hashing_with_a_separate_row_key_is_valid(self):
        SCDContext({"business_key_column": "id", "scd_row_hashing": True}).validate()

    @pytest.mark.parametrize("field", ["hash_column", "version_column", "current_flag_column",
                                       "deleted_flag_column", "effective_from_column",
                                       "effective_to_column", "correlation_id_column"])
    def test_any_other_meta_column_named_as_the_business_key_is_refused(self, field):
        ctx = SCDContext({"business_key_column": "k", field: "k"})
        with pytest.raises(ValueError, match=f"{field}='k'"):
            ctx.validate()

    def test_a_row_key_named_as_the_source_system_column_is_refused(self):
        ctx = SCDContext({"business_key_column": "id", "row_key_column": "scdSourceSystem"})
        with pytest.raises(ValueError, match="row_key_column='scdSourceSystem'"):
            ctx.validate()


# ============================================================================
# RUNTIME CONTEXT
# ============================================================================


class TestRuntimeContext:
    def test_required_fields_present(self):
        ctx = RuntimeContext({
            "correlation_id": "corr-1",
            "pipeline_name": "PL_Test",
            "application_name": "EDW",
        })
        ctx.validate()  # must not raise

    def test_missing_correlation_id_raises(self):
        ctx = RuntimeContext({"pipeline_name": "PL_Test", "application_name": "EDW"})
        with pytest.raises(ValueError, match="correlation_id"):
            ctx.validate()

    def test_materialize_strategy_defaults_to_delta(self):
        ctx = RuntimeContext({"correlation_id": "c", "pipeline_name": "P", "application_name": "A"})
        assert ctx.materialize_strategy == "delta"
        ctx.validate()

    @pytest.mark.parametrize("value", ["delta", "cache"])
    def test_materialize_strategy_accepts_the_two_values(self, value):
        ctx = RuntimeContext({"correlation_id": "c", "pipeline_name": "P",
                              "application_name": "A", "materialize_strategy": value})
        ctx.validate()
        assert ctx.materialize_strategy == value

    @pytest.mark.parametrize("value", ["memory", "Cache", "", None, True])
    def test_materialize_strategy_refuses_anything_else(self, value):
        ctx = RuntimeContext({"correlation_id": "c", "pipeline_name": "P",
                              "application_name": "A", "materialize_strategy": value})
        with pytest.raises(ValueError, match="materialize_strategy") as excinfo:
            ctx.validate()
        assert "'delta'" in str(excinfo.value) and "'cache'" in str(excinfo.value)

    @pytest.mark.parametrize("key", ["num_partitions", "scd_append_only"])
    @pytest.mark.parametrize("value", [4, None, False])
    def test_removed_key_is_refused_naming_the_field(self, key, value):
        """F198, F106: the key was read and used by nothing, so it is refused, not ignored."""
        data = {"correlation_id": "c", "pipeline_name": "P", "application_name": "A", key: value}
        with pytest.raises(ValueError, match=key) as excinfo:
            RuntimeContext(data)
        assert "runtime_context" in str(excinfo.value)
        assert "remove" in str(excinfo.value).lower()

    @pytest.mark.parametrize("key", ["num_partitions", "scd_append_only"])
    def test_removed_key_is_no_longer_an_attribute(self, key):
        ctx = RuntimeContext({"correlation_id": "c", "pipeline_name": "P", "application_name": "A"})
        assert not hasattr(ctx, key)

    def test_parallel_execution_disabled_by_default(self):
        ctx = RuntimeContext({"correlation_id": "c", "pipeline_name": "P", "application_name": "A"})
        assert ctx.parallel_execution_enabled is False

    def test_parallel_execution_enabled_from_int_1(self):
        ctx = RuntimeContext({"correlation_id": "c", "pipeline_name": "P", "application_name": "A", "parallel_execution_enabled": 1})
        assert ctx.parallel_execution_enabled is True

    def test_parallel_execution_int_0_is_false(self):
        ctx = RuntimeContext({"correlation_id": "c", "pipeline_name": "P", "application_name": "A", "parallel_execution_enabled": 0})
        assert ctx.parallel_execution_enabled is False

    def test_capture_row_counts_default_false(self):
        ctx = RuntimeContext({"correlation_id": "c", "pipeline_name": "P", "application_name": "A"})
        assert ctx.capture_row_counts is False

    def test_capture_row_counts_true_from_payload(self):
        ctx = RuntimeContext({
            "correlation_id": "c", "pipeline_name": "P", "application_name": "A",
            "capture_row_counts": True,
        })
        assert ctx.capture_row_counts is True

    def test_capture_plans_default_false(self):
        ctx = RuntimeContext({"correlation_id": "c", "pipeline_name": "P", "application_name": "A"})
        assert ctx.capture_plans is False

    def test_capture_plans_true_from_payload(self):
        ctx = RuntimeContext({
            "correlation_id": "c", "pipeline_name": "P", "application_name": "A",
            "capture_plans": True,
        })
        assert ctx.capture_plans is True


# ============================================================================
# LOGGING CONTEXT
# ============================================================================


class TestLoggingContext:
    def _make(self, overrides=None) -> LoggingContext:
        data = {"logging_level": "INFO", "log_file_name": "test_log", "log_file_dir": "/tmp/logs"}
        if overrides:
            data.update(overrides)
        return LoggingContext(data)

    def test_required_fields_present(self):
        ctx = self._make()
        ctx.validate()  # must not raise

    def test_missing_log_file_name_raises(self):
        ctx = LoggingContext({"logging_level": "INFO", "log_file_dir": "/tmp/logs"})
        with pytest.raises(ValueError, match="log_file_name"):
            ctx.validate()

    def test_verbose_default_false(self):
        ctx = self._make()
        assert ctx.verbose is False

    def test_verbose_true_from_payload(self):
        ctx = self._make({"verbose": True})
        assert ctx.verbose is True

    def test_verbose_false_explicit(self):
        ctx = self._make({"verbose": False})
        assert ctx.verbose is False

    def test_validate_passes_without_verbose(self):
        ctx = LoggingContext({"logging_level": "INFO", "log_file_name": "x", "log_file_dir": "/tmp"})
        ctx.validate()  # must not raise — verbose is optional


# ============================================================================
# STAGING CONTEXT PATH VALIDATION
# ============================================================================


class TestStagingContextPathValidation:
    def _make_ctx(self, stages):
        return StagingContext({"stages": stages})

    def test_path_absent_is_valid(self):
        ctx = self._make_ctx([{"nb_name": "NB_Stage"}])
        ctx.validate()  # should not raise

    def test_path_present_and_non_blank_is_valid(self):
        ctx = self._make_ctx([{"nb_name": "NB_Stage", "path": "customer_"}])
        ctx.validate()  # should not raise

    def test_path_present_but_blank_raises(self):
        ctx = self._make_ctx([{"nb_name": "NB_Stage", "path": ""}])
        with pytest.raises(ValueError, match="'path' is present but blank"):
            ctx.validate()

    def test_path_present_whitespace_only_raises(self):
        ctx = self._make_ctx([{"nb_name": "NB_Stage", "path": "   "}])
        with pytest.raises(ValueError, match="'path' is present but blank"):
            ctx.validate()


# ============================================================================
# ExtensionsContext / PlatformContext
# ============================================================================


class TestExtensionsContext:
    def test_it_reads_like_a_mapping(self):
        ext = ExtensionsContext({"acme.quality": {"max_null_ratio": 0.02}, "flag": True})
        assert ext["acme.quality"] == {"max_null_ratio": 0.02}
        assert ext.get("flag") is True
        assert ext.get("absent") is None
        assert ext.get("absent", 7) == 7
        assert "flag" in ext and "absent" not in ext
        assert sorted(ext) == ["acme.quality", "flag"] and len(ext) == 2

    def test_a_missing_key_raises_key_error(self):
        with pytest.raises(KeyError):
            ExtensionsContext({})["absent"]

    def test_it_is_read_only(self):
        ext = ExtensionsContext({"k": 1})
        with pytest.raises(TypeError):
            ext["k"] = 2
        with pytest.raises(TypeError):
            del ext["k"]
        assert ext["k"] == 1

    def test_a_change_to_the_source_object_does_not_reach_it(self):
        source = {"k": {"inner": 1}}
        ext = ExtensionsContext(source)
        source["k"]["inner"] = 2
        source["new"] = 1
        assert ext.to_dict() == {"k": {"inner": 1}}

    def test_to_dict_returns_a_copy(self):
        ext = ExtensionsContext({"k": {"inner": 1}})
        ext.to_dict()["k"]["inner"] = 99
        assert ext["k"] == {"inner": 1}

    def test_it_is_empty_by_default(self):
        assert ExtensionsContext().to_dict() == {}

    @pytest.mark.parametrize("bad", [[1], "text", 3, None, True])
    def test_a_value_that_is_not_an_object_is_refused(self, bad):
        with pytest.raises(ValueError, match="extensions_context must be a JSON object"):
            ExtensionsContext.from_raw(bad)


class TestPlatformContext:
    def test_it_holds_the_two_paths_and_is_frozen(self):
        platform = PlatformContext(data_storage_lakehouse_path="/s", data_engineering_lakehouse_path="/e")
        assert platform.to_dict() == {"data_storage_lakehouse_path": "/s",
                                      "data_engineering_lakehouse_path": "/e"}
        with pytest.raises(Exception):  # FrozenInstanceError
            platform.data_storage_lakehouse_path = "/other"

    def test_it_is_empty_strings_by_default(self):
        assert PlatformContext().to_dict() == {"data_storage_lakehouse_path": "",
                                               "data_engineering_lakehouse_path": ""}


class TestContextsCopyAndCompare:
    def _load_context(self):
        from spark_quick_start.orchestration.transformers.base import LoadContext

        global_ctx = GlobalContext({"id": "e1", "entity_name": "E", "source_system": "S",
                                    "target_table_path": "/t", "staging_table_path": "/s"})
        return LoadContext(
            global_ctx=global_ctx, staging_ctx=StagingContext({}), scd_ctx=SCDContext({"business_key_column": "k"}),
            runtime_ctx=RuntimeContext({}), logging_ctx=LoggingContext({}),
            extensions_ctx=ExtensionsContext({"acme.q": {"n": [1, 2]}}),
            platform_ctx=PlatformContext("/s", "/e"),
        )

    def test_a_load_context_deep_copies(self):
        ctx = self._load_context()
        clone = copy.deepcopy(ctx)
        assert clone.extensions_ctx == ctx.extensions_ctx and clone.extensions_ctx is not ctx.extensions_ctx
        assert clone.platform_ctx == ctx.platform_ctx
        with pytest.raises(TypeError):
            clone.extensions_ctx["x"] = 1

    def test_the_contexts_survive_a_pickle_round_trip(self):
        ext = ExtensionsContext({"acme.q": {"n": [1, 2]}})
        back = pickle.loads(pickle.dumps(ext))
        assert back == ext and back.to_dict() == ext.to_dict()
        platform = PlatformContext("/s", "/e")
        assert pickle.loads(pickle.dumps(platform)) == platform

    def test_equality_follows_the_data(self):
        assert ExtensionsContext({"a": 1}) == ExtensionsContext({"a": 1})
        assert ExtensionsContext({"a": 1}) != ExtensionsContext({"a": 2})
        assert ExtensionsContext({}) != {}

    def test_repr_shows_the_data(self):
        assert repr(ExtensionsContext({"a": 1})) == "ExtensionsContext({'a': 1})"
