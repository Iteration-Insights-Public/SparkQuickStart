from __future__ import annotations

import dataclasses
import sys
import types
from unittest.mock import MagicMock, patch

import pytest
from _frames import only_partition_probe

# Stub delta so import succeeds without a real Spark session
if "delta" not in sys.modules:
    _d = types.ModuleType("delta")
    _dt = types.ModuleType("delta.tables")
    class _MockDeltaTable:
        @staticmethod
        def isDeltaTable(s, p): return False
        @staticmethod
        def forPath(s, p): return MagicMock()
    _dt.DeltaTable = _MockDeltaTable
    _d.tables = _dt
    sys.modules["delta"] = _d
    sys.modules["delta.tables"] = _dt

from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter
from spark_quick_start.orchestration.models import (
    GlobalContext,
    LoggingContext,
    RuntimeContext,
    SCDContext,
    StagingContext,
)
from spark_quick_start.orchestration.transformers.base import LoadContext, ProcessTransformer
from spark_quick_start.orchestration.transformers.state import PipelineState
from spark_quick_start.orchestration.transformers.target import (
    SCDIncrementalTargetTableTransformer,
    SCDTargetTableTransformer,
)


def _ctx(**scd_overrides):
    scd_data = {"business_key_column": "id"}
    scd_data.update(scd_overrides)
    return LoadContext(
        global_ctx=GlobalContext({"id": "ent_1", "entity_name": "E", "source_system": "S",
                                   "target_table_path": "/t", "staging_table_path": "/s"}),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext(scd_data),
        runtime_ctx=RuntimeContext({"correlation_id": "c1", "pipeline_name": "p", "application_name": "a"}),
        logging_ctx=LoggingContext({}),
    )


def _state():
    return PipelineState(df=MagicMock())


def test_target_table_transformer_is_process_transformer():
    assert issubclass(SCDTargetTableTransformer, ProcessTransformer)


def test_target_table_transformer_stashes_target_df():
    spark = MagicMock()
    staged_df = MagicMock()
    target_df = MagicMock()

    with patch(
        "spark_quick_start.orchestration.transformers.target.load_target_dataframe",
        return_value=(MagicMock(), target_df),
    ):
        t = SCDTargetTableTransformer()
        t._spark = spark
        t._adapter = MockPlatformAdapter()
        state = PipelineState(df=staged_df)
        result = t(state, _ctx())

    assert result.frames["target"] is target_df
    assert result.target_exists is True
    assert not target_df.method_calls


def test_target_table_transformer_preserves_primary_df():
    staged_df = MagicMock()

    with patch(
        "spark_quick_start.orchestration.transformers.target.load_target_dataframe",
        return_value=(MagicMock(), MagicMock()),
    ):
        t = SCDTargetTableTransformer()
        t._spark = MagicMock()
        state = PipelineState(df=staged_df)
        result = t(state, _ctx())

    assert result.df is staged_df
    only_partition_probe(staged_df)


def test_target_table_transformer_calls_load_target_dataframe():
    with patch(
        "spark_quick_start.orchestration.transformers.target.load_target_dataframe",
        return_value=(MagicMock(), MagicMock()),
    ) as mock_load:
        t = SCDTargetTableTransformer()
        t._spark = MagicMock()
        t(PipelineState(df=MagicMock()), _ctx())

    mock_load.assert_called_once()


def test_target_table_transformer_states_the_target_absent_on_first_load():
    """When the target table doesn't exist load_target_dataframe returns (None, None).
    The state then has no "target" key and says so with target_exists=False (F28)."""
    with patch(
        "spark_quick_start.orchestration.transformers.target.load_target_dataframe",
        return_value=(None, None),
    ):
        t = SCDTargetTableTransformer()
        t._spark = MagicMock()
        result = t(PipelineState(df=MagicMock()), _ctx())

    assert "target" not in result.frames
    assert result.target_exists is False


# ── SCDIncrementalTargetTableTransformer ───────────────────────────────────────

def test_incremental_target_table_transformer_is_process_transformer():
    assert issubclass(SCDIncrementalTargetTableTransformer, ProcessTransformer)


def test_incremental_target_raises_on_full_load():
    # capture_partitions off: the base hooks would otherwise probe the frame, and the claim here
    # is that the step refuses before anything reads it.
    runtime = RuntimeContext({"correlation_id": "c1", "pipeline_name": "p", "application_name": "a",
                              "capture_partitions": False})
    ctx = dataclasses.replace(_ctx(load_type="Full"), runtime_ctx=runtime)
    frame = MagicMock()
    with patch(
        "spark_quick_start.orchestration.transformers.target.load_target_dataframe",
    ) as mock_load:
        t = SCDIncrementalTargetTableTransformer()
        t._spark = MagicMock()
        with pytest.raises(ValueError, match="Full"):
            t(PipelineState(df=frame), ctx)

    mock_load.assert_not_called()
    assert not frame.method_calls


def test_incremental_target_states_the_target_absent_on_first_load():
    with patch(
        "spark_quick_start.orchestration.transformers.target.load_target_dataframe",
        return_value=(None, None),
    ):
        t = SCDIncrementalTargetTableTransformer()
        t._spark = MagicMock()
        result = t(PipelineState(df=MagicMock()), _ctx(load_type="Incremental"))

    assert "target" not in result.frames
    assert result.target_exists is False


def test_incremental_target_preserves_primary_df(spark):
    staged_df = spark.createDataFrame([{"id": "A"}])
    target_df = spark.createDataFrame([{"id": "A", "scdSourceSystem": "S"}])

    with patch(
        "spark_quick_start.orchestration.transformers.target.load_target_dataframe",
        return_value=(MagicMock(), target_df),
    ):
        t = SCDIncrementalTargetTableTransformer()
        t._spark = spark
        result = t(PipelineState(df=staged_df), _ctx(load_type="Incremental"))

    assert result.df is staged_df


def test_incremental_target_filters_to_incoming_keys(spark):
    """Target rows whose (id, scdSourceSystem) is absent from staged data are dropped."""
    target_df = spark.createDataFrame([
        {"id": "A", "scdSourceSystem": "S", "value": 1},
        {"id": "B", "scdSourceSystem": "S", "value": 2},
        {"id": "C", "scdSourceSystem": "S", "value": 3},
    ])
    staged_df = spark.createDataFrame([{"id": "A"}, {"id": "C"}])

    with patch(
        "spark_quick_start.orchestration.transformers.target.load_target_dataframe",
        return_value=(MagicMock(), target_df),
    ):
        t = SCDIncrementalTargetTableTransformer()
        t._spark = spark
        result = t(PipelineState(df=staged_df), _ctx(load_type="Incremental"))

    kept_ids = {r["id"] for r in result.frames["target"].collect()}
    assert kept_ids == {"A", "C"}
    assert result.target_exists is True


def test_incremental_target_filtering_everything_out_is_still_a_present_frame(spark):
    """An existing table whose rows match no incoming key is a frame (empty), not an absent target."""
    target_df = spark.createDataFrame([{"id": "A", "scdSourceSystem": "S"}])
    staged_df = spark.createDataFrame([{"id": "Z"}])

    with patch(
        "spark_quick_start.orchestration.transformers.target.load_target_dataframe",
        return_value=(MagicMock(), target_df),
    ):
        t = SCDIncrementalTargetTableTransformer()
        t._spark = spark
        result = t(PipelineState(df=staged_df), _ctx(load_type="Incremental"))

    assert result.target_exists is True
    assert result.frames["target"].isEmpty()


def test_incremental_target_left_semi_no_row_inflation(spark):
    """Duplicate keys on the staged side must not multiply matching target rows."""
    target_df = spark.createDataFrame([{"id": "A", "scdSourceSystem": "S", "value": 1}])
    staged_df = spark.createDataFrame([{"id": "A"}, {"id": "A"}, {"id": "A"}])

    with patch(
        "spark_quick_start.orchestration.transformers.target.load_target_dataframe",
        return_value=(MagicMock(), target_df),
    ):
        t = SCDIncrementalTargetTableTransformer()
        t._spark = spark
        result = t(PipelineState(df=staged_df), _ctx(load_type="Incremental"))

    assert result.frames["target"].count() == 1


def test_incremental_target_stamps_source_system_when_not_from_source(spark):
    """staged data has no scdSourceSystem column (source_system_from_source=False,
    the default) — the join key must be reconstructed from global_ctx.source_system,
    mirroring SCDChangeDetectorTransformer's own stamping, not read off state.df."""
    target_df = spark.createDataFrame([
        {"id": "A", "scdSourceSystem": "S", "value": 1},
        {"id": "A", "scdSourceSystem": "OTHER", "value": 99},
    ])
    staged_df = spark.createDataFrame([{"id": "A"}])  # no scdSourceSystem column at all
    assert "scdSourceSystem" not in staged_df.columns

    with patch(
        "spark_quick_start.orchestration.transformers.target.load_target_dataframe",
        return_value=(MagicMock(), target_df),
    ):
        t = SCDIncrementalTargetTableTransformer()
        t._spark = spark
        result = t(PipelineState(df=staged_df), _ctx(load_type="Incremental"))

    rows = result.frames["target"].collect()
    assert len(rows) == 1
    assert rows[0]["scdSourceSystem"] == "S"  # matches global_ctx.source_system, not "OTHER"


def test_incremental_target_uses_existing_source_system_when_from_source(spark):
    target_df = spark.createDataFrame([
        {"id": "A", "scdSourceSystem": "SYS1", "value": 1},
        {"id": "A", "scdSourceSystem": "SYS2", "value": 2},
    ])
    staged_df = spark.createDataFrame([{"id": "A", "scdSourceSystem": "SYS2"}])

    with patch(
        "spark_quick_start.orchestration.transformers.target.load_target_dataframe",
        return_value=(MagicMock(), target_df),
    ):
        t = SCDIncrementalTargetTableTransformer()
        t._spark = spark
        result = t(
            PipelineState(df=staged_df),
            _ctx(load_type="Incremental", source_system_from_source=True),
        )

    rows = result.frames["target"].collect()
    assert len(rows) == 1
    assert rows[0]["value"] == 2


def test_incremental_target_broadcast_false_still_correct(spark):
    target_df = spark.createDataFrame([{"id": "A", "scdSourceSystem": "S", "value": 1}])
    staged_df = spark.createDataFrame([{"id": "A"}])

    with patch(
        "spark_quick_start.orchestration.transformers.target.load_target_dataframe",
        return_value=(MagicMock(), target_df),
    ):
        t = SCDIncrementalTargetTableTransformer(broadcast=False)
        t._spark = spark
        result = t(PipelineState(df=staged_df), _ctx(load_type="Incremental"))

    assert result.frames["target"].count() == 1
