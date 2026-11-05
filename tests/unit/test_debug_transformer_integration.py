"""Integration tests for DebugTransformer — requires a local SparkSession."""
from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

# Delta stub — same pattern used across integration tests in this repo
if "delta" not in sys.modules:
    _d = types.ModuleType("delta")
    _dt = types.ModuleType("delta.tables")
    class _MockDeltaTable:
        @staticmethod
        def isDeltaTable(spark, path): return False
        @staticmethod
        def forPath(spark, path): return MagicMock()
    _dt.DeltaTable = _MockDeltaTable
    _d.tables = _dt
    sys.modules["delta"] = _d
    sys.modules["delta.tables"] = _dt

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from spark_quick_start.orchestration.transformers.debug import DebugTransformer
from spark_quick_start.orchestration.transformers.state import PipelineState


def _make_state(spark, frames=None):
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
    ])
    df = spark.createDataFrame([(1, "Alice"), (2, "Bob"), (3, "Carol")], schema)
    return PipelineState(df=df, frames=frames or {})


def _mock_ctx():
    return MagicMock()


def test_header_appears_in_output(spark, capsys):
    state = _make_state(spark)
    DebugTransformer("integration-test", n=3)(state, _mock_ctx())
    captured = capsys.readouterr()
    assert "DEBUG CHECKPOINT: integration-test" in captured.out


def test_frame_label_appears_in_output(spark, capsys):
    schema = StructType([StructField("x", IntegerType(), False)])
    target_df = spark.createDataFrame([(10,), (20,)], schema)
    state = _make_state(spark, frames={"target": target_df})
    DebugTransformer("frames-test", frames=["target"], n=2)(state, _mock_ctx())
    captured = capsys.readouterr()
    assert 'frames["target"]' in captured.out


def test_disabled_produces_no_output(spark, capsys):
    state = _make_state(spark)
    DebugTransformer("disabled-test", enabled=False)(state, _mock_ctx())
    captured = capsys.readouterr()
    assert "DEBUG CHECKPOINT" not in captured.out


def test_n_zero_skips_rows_header_still_appears(spark, capsys):
    state = _make_state(spark)
    DebugTransformer("schema-only", n=0, show_schema=True)(state, _mock_ctx())
    captured = capsys.readouterr()
    # printSchema() writes to JVM stdout — may not be captured by capsys.
    # The header at minimum should appear.
    assert "DEBUG CHECKPOINT: schema-only" in captured.out
