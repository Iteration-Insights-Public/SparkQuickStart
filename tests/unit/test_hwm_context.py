"""`hwm_context` (F138): the load's watermark, shared by staging-notebook and DQD loads.

The section has two keys for now, `hwm_column_name` and `hwm_value` (owner, 2026-09-29). A
staging function receives it only when its signature declares `hwm_context`, and
`StagingTransformer` reports `max(hwm_column_name)` over the staged rows as an `HWMProvider`.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from spark_quick_start.adapters import MockPlatformAdapter
from spark_quick_start.orchestration.context_manager import LoadContextManager
from spark_quick_start.orchestration.models import (
    GlobalContext,
    HWMContext,
    LoggingContext,
    RuntimeContext,
    SCDContext,
    StagingContext,
)
from spark_quick_start.orchestration.staging_pipeline import create_staging_main_function
from spark_quick_start.orchestration.transformers.base import LoadContext
from spark_quick_start.orchestration.transformers.protocols import HWMProvider
from spark_quick_start.orchestration.transformers.staging import StagingTransformer

# --- the context class -----------------------------------------------------------------------


class TestHWMContext:
    @pytest.mark.parametrize("value", [None, "2026-09-01 00:00:00", 42, 1.5])
    def test_a_column_and_an_optional_value_are_valid(self, value):
        data = {"hwm_column_name": "modified_at"} | ({} if value is None else {"hwm_value": value})
        ctx = HWMContext(data)
        ctx.validate()
        assert (ctx.hwm_column_name, ctx.hwm_value) == ("modified_at", value)

    @pytest.mark.parametrize("data, message", [
        ({}, "hwm_column_name"),
        ({"hwm_column_name": ""}, "hwm_column_name"),
        ({"hwm_column_name": "  "}, "hwm_column_name"),
        ({"hwm_column_name": 3}, "hwm_column_name"),
        ({"hwm_column_name": "m", "hwm_value": True}, "hwm_value"),
        ({"hwm_column_name": "m", "hwm_value": {"a": 1}}, "hwm_value"),
        ({"hwm_column_name": "m", "hwm_column": "m"}, "unknown key 'hwm_column'"),
        ({"hwmColumnName": "m"}, "unknown key 'hwmColumnName'"),
    ])
    def test_a_malformed_section_is_refused(self, data, message):
        with pytest.raises(ValueError, match=message):
            HWMContext(data).validate()

    def test_it_hands_a_notebook_both_keys(self):
        assert HWMContext({"hwm_column_name": "m"}).to_dict() == {"hwm_column_name": "m", "hwm_value": None}


# --- the context manager ---------------------------------------------------------------------


def _load_context(**extra) -> str:
    return json.dumps({
        "global_context": {"id": "ent_1", "entity_name": "E", "source_system": "S", "target_table_path": "/t",
                           "staging_table_path": "/s",
                           "staging_table_schema": {"type": "struct", "fields": []}},
        "staging_context": {"stages": [{"nb_name": "NB_A"}]},
        "scd_context": {"business_key_column": "id"},
        "runtime_context": {"correlation_id": "c", "pipeline_name": "p", "application_name": "a"},
        "logging_context": {"logging_level": "INFO", "log_file_name": "f", "log_file_dir": "/l"},
        **extra,
    })


def _manager(**extra) -> LoadContextManager:
    manager = LoadContextManager(_load_context(**extra)).parse("storage", "engineering")
    manager.validate_all()
    return manager


class TestLoadContextManager:
    def test_without_the_section_there_is_no_watermark(self):
        manager = _manager()
        assert manager.hwm_ctx is None
        assert "hwm_context" not in manager.build_staging_phase_context(0, "NB_A")

    def test_the_section_is_parsed_and_handed_to_each_notebook(self):
        manager = _manager(hwm_context={"hwm_column_name": "modified_at", "hwm_value": "2026-09-01"})
        assert manager.hwm_ctx.hwm_column_name == "modified_at"
        assert manager.build_staging_phase_context(0, "NB_A")["hwm_context"] == {
            "hwm_column_name": "modified_at", "hwm_value": "2026-09-01"}

    def test_a_section_that_is_not_an_object_is_refused(self):
        with pytest.raises(ValueError, match="hwm_context must be a JSON object"):
            LoadContextManager(_load_context(hwm_context=["modified_at"])).parse("s", "e")

    def test_validation_checks_the_section(self):
        manager = LoadContextManager(_load_context(hwm_context={"hwm_value": "x"})).parse("s", "e")
        with pytest.raises(ValueError, match="hwm_column_name"):
            manager.validate_all()


# --- the staging function --------------------------------------------------------------------


def _stage_context(hwm_context=None) -> str:
    ctx = {
        "global_context": {"source_system": "S", "staging_table_path": "/tmp/s__NB"},
        "staging_context": {"region": "EU"},
        "logging_context": {"logging_level": "INFO", "log_file_name": "f"},
        "runtime_context": {"correlation_id": "c", "pipeline_name": "p", "application_name": "a"},
    }
    if hwm_context is not None:
        ctx["hwm_context"] = hwm_context
    return json.dumps(ctx)


def _run_main(fn, stage_context):
    frame = MagicMock()
    frame.withColumn.return_value = frame
    fn.frame = frame
    with patch("spark_quick_start.orchestration.staging_pipeline.F.lit", MagicMock()), \
         patch("spark_quick_start.orchestration.staging_pipeline.write_staging_data", return_value=1), \
         patch("spark_quick_start.orchestration.staging_pipeline.delete_staging_data"):
        return create_staging_main_function(fn, adapter=MockPlatformAdapter())(stage_context)


_HWM = {"hwm_column_name": "modified_at", "hwm_value": "2026-09-01"}


class TestStagingFunction:

    def test_a_function_declaring_hwm_context_receives_it(self):
        seen = {}

        def stage(storage_path, staging_context, global_context, notebook_name, hwm_context):
            seen["hwm"] = hwm_context
            return stage.frame

        assert _run_main(stage, _stage_context(_HWM)).status == "success"
        assert seen["hwm"] == _HWM

    def test_it_receives_none_when_the_load_has_no_watermark(self):
        seen = {}

        def stage(storage_path, staging_context, global_context, notebook_name, hwm_context=None):
            seen["hwm"] = hwm_context
            return stage.frame

        assert _run_main(stage, _stage_context()).status == "success"
        assert seen == {"hwm": None}

    def test_a_four_parameter_function_is_called_as_before(self):
        def stage(storage_path, staging_context, global_context, notebook_name):
            return stage.frame

        assert _run_main(stage, _stage_context(_HWM)).status == "success"

    @pytest.mark.parametrize("hwm", [_HWM, None])
    def test_a_function_taking_only_keywords_is_not_asked(self, hwm):
        seen = {}

        def stage(*args, **kwargs):
            seen.update(kwargs)
            return stage.frame

        assert _run_main(stage, _stage_context(hwm)).status == "success"
        assert seen == {}

    def test_a_positional_only_hwm_context_is_not_passed_by_keyword(self):
        from spark_quick_start.orchestration.staging_pipeline import _accepts_hwm_context

        def positional(a, b, c, d, hwm_context=None, /):
            return None

        def keyword(a, b, c, d, *, hwm_context=None):
            return None

        assert not _accepts_hwm_context(positional)
        assert _accepts_hwm_context(keyword)


# --- StagingTransformer as a watermark provider ----------------------------------------------

_MODULE = "spark_quick_start.orchestration.transformers.staging"


def _ctx(hwm=None) -> LoadContext:
    return LoadContext(
        global_ctx=GlobalContext({"id": "ent_1", "entity_name": "E", "source_system": "S", "target_table_path": "/t",
                                  "staging_table_path": "/s"}),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext({"business_key_column": "id"}),
        runtime_ctx=RuntimeContext({"correlation_id": "c", "pipeline_name": "p", "application_name": "a"}),
        logging_ctx=LoggingContext({}),
        hwm_ctx=None if hwm is None else HWMContext(hwm),
    )


def _produce(staged, hwm=None) -> StagingTransformer:
    transformer = StagingTransformer()
    transformer._ctx_manager = MagicMock()
    transformer._ctx_manager.global_ctx.staging_table_schema = {"type": "struct", "fields": []}
    with patch(f"{_MODULE}.NotebookExecutor"), \
         patch(f"{_MODULE}.validate_and_merge_staging_tables"), \
         patch(f"{_MODULE}.execute_staging_phases", return_value=(["/s__A"], 1, 1)), \
         patch(f"{_MODULE}.load_staged_dataframe", return_value=staged):
        transformer._produce(_ctx(hwm))
    return transformer


def test_the_staging_step_always_declares_the_provider_attributes():
    transformer = StagingTransformer()
    assert isinstance(transformer, HWMProvider)
    assert (transformer.hwm_before, transformer.hwm_after, transformer.hwm_column) == (None, None, None)


def test_without_a_watermark_the_staging_step_reports_none(spark):
    transformer = _produce(spark.createDataFrame([(1,)], "id int"))
    assert (transformer.hwm_before, transformer.hwm_after, transformer.hwm_column) == (None, None, None)


def test_the_staging_step_reports_the_latest_staged_value(spark):
    staged = spark.createDataFrame([(1, "2026-09-02 10:00:00"), (2, "2026-09-03 08:30:00")],
                                   "id int, modified_at string").selectExpr("id", "timestamp(modified_at) AS modified_at")
    transformer = _produce(staged, {"hwm_column_name": "modified_at", "hwm_value": "2026-09-01 00:00:00"})
    assert transformer.hwm_column == "modified_at"
    assert transformer.hwm_before == "2026-09-01 00:00:00"
    assert transformer.hwm_after == "2026-09-03 08:30:00"


def test_with_nothing_staged_the_watermark_stays(spark):
    transformer = _produce(spark.createDataFrame([], "id int, modified_at timestamp"),
                           {"hwm_column_name": "modified_at", "hwm_value": "2026-09-01 00:00:00"})
    assert transformer.hwm_after == transformer.hwm_before == "2026-09-01 00:00:00"


def test_a_numeric_watermark_is_reported_as_text(spark):
    transformer = _produce(spark.createDataFrame([(1, 7), (2, 9)], "id int, seq int"),
                           {"hwm_column_name": "seq", "hwm_value": 5})
    assert (transformer.hwm_before, transformer.hwm_after) == ("5", "9")


def test_the_watermark_column_is_matched_as_the_session_matches(spark):
    staged = spark.createDataFrame([(1, 7)], "id int, Seq int")
    assert _produce(staged, {"hwm_column_name": "seq"}).hwm_after == "7"
    transformer = StagingTransformer()
    transformer._spark = spark
    spark.conf.set("spark.sql.caseSensitive", "true")
    try:
        with pytest.raises(ValueError, match="'seq' is not a column"):
            transformer._staged_column(staged, "seq")
    finally:
        spark.conf.set("spark.sql.caseSensitive", "false")


def test_a_watermark_column_two_staged_columns_share_but_for_case_is_refused():
    frame = MagicMock()
    frame.columns = ["Seq", "SEQ"]
    with pytest.raises(ValueError, match="'seq' is more than one column"):
        StagingTransformer()._staged_column(frame, "seq")


def test_a_watermark_column_the_staged_rows_lack_is_named(spark):
    with pytest.raises(ValueError, match="hwm_column_name 'modified_at'.*staged"):
        _produce(spark.createDataFrame([(1,)], "id int"), {"hwm_column_name": "modified_at"})
