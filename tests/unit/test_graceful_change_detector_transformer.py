"""Tests for _graceful_hash_expr and SCDGracefulChangeDetectorTransformer."""
from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from conftest import is_serverless_session
from spark_quick_start.orchestration.models import (
    GlobalContext,
    LoggingContext,
    RuntimeContext,
    SCDContext,
    StagingContext,
)
from spark_quick_start.orchestration.transformers.base import LoadContext
from spark_quick_start.orchestration.transformers.change_detector import (
    SCDGracefulChangeDetectorTransformer,
    _graceful_hash_expr,
)
from spark_quick_start.orchestration.transformers.state import PipelineState


def _eval(spark, expr, **col_values):
    """Evaluate a Column expression on a single row built from keyword args.

    All values are treated as nullable strings so Spark can infer types even
    when some values are None.
    """
    schema = StructType([
        StructField(k, StringType(), nullable=True) for k in col_values
    ])
    df = spark.createDataFrame([tuple(col_values.values())], schema=schema)
    return df.select(expr.alias("h")).collect()[0]["h"]


def _xxhash(spark, *string_literals):
    """Compute xxhash64(concat_ws('~', *literals)) for comparison."""
    return spark.range(1).select(
        F.xxhash64(F.concat_ws("~", *[F.lit(v) for v in string_literals])).alias("h")
    ).collect()[0]["h"]


# ── Expression unit tests (_graceful_hash_expr) ──────────────────────────────

def test_all_nonnull_matches_flat_concat(spark):
    expr = _graceful_hash_expr(["A", "B", "C"], "<NULL>", "~")
    assert _eval(spark, expr, A="1", B="2", C="3") == _xxhash(spark, "1", "2", "3")


def test_last_column_null_drops_trailing(spark):
    expr = _graceful_hash_expr(["A", "B", "C"], "<NULL>", "~")
    # C is null — result equals hash over A, B only
    assert _eval(spark, expr, A="1", B="2", C=None) == _xxhash(spark, "1", "2")


def test_all_columns_null_returns_empty_hash(spark):
    expr = _graceful_hash_expr(["A", "B", "C"], "<NULL>", "~")
    expected = spark.range(1).select(F.xxhash64(F.lit("")).alias("h")).collect()[0]["h"]
    assert _eval(spark, expr, A=None, B=None, C=None) == expected


def test_interior_null_trailing_nonnull_preserved(spark):
    expr = _graceful_hash_expr(["A", "B", "C"], "<NULL>", "~")
    # B is null but C is not → B's sentinel is included because C follows
    assert _eval(spark, expr, A="1", B=None, C="3") == _xxhash(spark, "1", "<NULL>", "3")


def test_schema_evolution_new_null_column_unchanged(spark):
    old_expr = _graceful_hash_expr(["A", "B"], "<NULL>", "~")
    new_expr = _graceful_hash_expr(["A", "B", "C"], "<NULL>", "~")
    old_hash = _eval(spark, old_expr, A="1", B="2")
    new_hash = _eval(spark, new_expr, A="1", B="2", C=None)
    assert old_hash == new_hash


def test_two_trailing_nulls_equals_hash_up_to_last_nonnull(spark):
    expr = _graceful_hash_expr(["A", "B", "C", "D"], "<NULL>", "~")
    # C=null, D=null → hash("A~B"); C="3", D=null → hash("A~B~3") — different
    hash_two_trailing = _eval(spark, expr, A="1", B="2", C=None, D=None)
    hash_one_trailing = _eval(spark, expr, A="1", B="2", C="3", D=None)
    assert hash_two_trailing != hash_one_trailing
    assert hash_two_trailing == _xxhash(spark, "1", "2")
    assert hash_one_trailing == _xxhash(spark, "1", "2", "3")


def test_single_column_nonnull(spark):
    expr = _graceful_hash_expr(["A"], "<NULL>", "~")
    assert _eval(spark, expr, A="hello") == _xxhash(spark, "hello")


def test_single_column_null_returns_empty_hash(spark):
    expr = _graceful_hash_expr(["A"], "<NULL>", "~")
    expected = spark.range(1).select(F.xxhash64(F.lit("")).alias("h")).collect()[0]["h"]
    assert _eval(spark, expr, A=None) == expected


# ── Shared fixtures for integration tests ────────────────────────────────────

_STAGING_SCHEMA_ABC = {
    "type": "struct",
    "fields": [
        {"name": "A", "type": "string", "nullable": True, "metadata": {}},
        {"name": "B", "type": "string", "nullable": True, "metadata": {}},
        {"name": "C", "type": "string", "nullable": True, "metadata": {}},
    ],
}

_SRC_SCHEMA = StructType([
    StructField("id", StringType(), False),
    StructField("A", StringType(), True),
    StructField("B", StringType(), True),
    StructField("C", StringType(), True),
])

_TGT_SCHEMA = StructType([
    StructField("id", StringType(), False),
    StructField("A", StringType(), True),
    StructField("B", StringType(), True),
    StructField("C", StringType(), True),
    StructField("scdHash", LongType(), True),
    StructField("scdVersion", LongType(), True),
    StructField("scdCurrentFlag", StringType(), True),
    StructField("scdDeletedFlag", StringType(), True),
    StructField("scdEffectiveFrom", TimestampType(), True),
    StructField("scdEffectiveTo", TimestampType(), True),
    StructField("scdSourceSystem", StringType(), True),
    StructField("scdCorrelationId", StringType(), True),
    StructField("scdRowKey", StringType(), True),
])

_T0 = datetime(2024, 1, 1)
_MAX_TS = datetime(2999, 12, 31, 23, 59, 59)


def _ctx_graceful(staging_schema=_STAGING_SCHEMA_ABC):
    return LoadContext(
        global_ctx=GlobalContext({
            "id": "ent_1",
            "entity_name": "Ent",
            "source_system": "SYS",
            "target_table_path": "/tmp/t",
            "staging_table_path": "/tmp/s",
            "staging_table_schema": staging_schema,
        }),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext({
            "business_key_column": "id",
            "scd_type": "scd2",
            "load_type": "Incremental",
        }),
        runtime_ctx=RuntimeContext({
            "correlation_id": "c1", "pipeline_name": "p", "application_name": "a",
            "serverless": is_serverless_session(),
        }),
        logging_ctx=LoggingContext({}),
    )


def _run(spark, rows, tgt_df=None, ctx=None):
    if ctx is None:
        ctx = _ctx_graceful()
    t = SCDGracefulChangeDetectorTransformer()
    t._spark = spark
    t._logger = None
    t._adapter = None
    src_df = spark.createDataFrame(rows, _SRC_SCHEMA)
    frames = {"target": tgt_df} if tgt_df is not None else {}
    state = PipelineState(df=src_df, frames=frames)
    return t(state, ctx)


def _tgt_row(spark, id_val, A, B, C, hash_val):
    return spark.createDataFrame(
        [(id_val, A, B, C, hash_val, 1, "Y", "N", _T0, _MAX_TS, "SYS", "c0", f"rk_{id_val}")],
        _TGT_SCHEMA,
    )


# ── Integration tests ────────────────────────────────────────────────────────

def test_schema_evolution_no_spurious_change(spark):
    """Adding column C=null to an existing row must NOT produce a changed candidate."""
    # Existing Silver hash was computed with schema [A, B], C did not exist yet.
    # Graceful hash over [A, B, C] with C=null == graceful hash over [A, B].
    old_hash = _xxhash(spark, "v1", "v2")  # what was stored when C didn't exist
    tgt = _tgt_row(spark, "r1", "v1", "v2", None, old_hash)
    result = _run(spark, [("r1", "v1", "v2", None)], tgt_df=tgt)
    assert result.df.count() == 0, "Row must be unchanged — no candidate expected"


def test_genuine_change_detected(spark):
    """A real data change (A value differs) must still be detected as changed."""
    old_hash = _xxhash(spark, "old_v1", "v2")
    tgt = _tgt_row(spark, "r1", "old_v1", "v2", None, old_hash)
    result = _run(spark, [("r1", "new_v1", "v2", None)], tgt_df=tgt)
    rows = result.df.collect()
    assert len(rows) == 1
    assert rows[0]["_change_type"] == "changed"


def test_missing_staging_schema_raises(spark):
    """SCDGracefulChangeDetectorTransformer must raise ValueError when staging_table_schema absent."""
    ctx = LoadContext(
        global_ctx=GlobalContext({
            "id": "ent_1",
            "entity_name": "Ent", "source_system": "SYS",
            "target_table_path": "/tmp/t", "staging_table_path": "/tmp/s",
        }),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext({
            "business_key_column": "id", "scd_type": "scd2", "load_type": "Incremental",
        }),
        runtime_ctx=RuntimeContext({
            "correlation_id": "c", "pipeline_name": "p", "application_name": "a",
            "serverless": is_serverless_session(),
        }),
        logging_ctx=LoggingContext({}),
    )
    t = SCDGracefulChangeDetectorTransformer()
    t._spark = spark
    t._logger = None
    t._adapter = None
    src_df = spark.createDataFrame([("r1", "v1", "v2", None)], _SRC_SCHEMA)
    state = PipelineState(df=src_df)
    with pytest.raises(ValueError, match="staging_table_schema"):
        t(state, ctx)


def test_extra_source_column_not_in_schema_ignored(spark):
    """A column in the source DataFrame but not in staging_table_schema is excluded from hash."""
    src_with_extra = StructType([
        StructField("id", StringType(), False),
        StructField("A", StringType(), True),
        StructField("B", StringType(), True),
        StructField("C", StringType(), True),
        StructField("EXTRA", StringType(), True),
    ])
    # Hash must equal graceful hash over [A, B] (C=null trailing is dropped)
    expected_hash = _xxhash(spark, "v1", "v2")
    t = SCDGracefulChangeDetectorTransformer()
    t._spark = spark
    t._logger = None
    t._adapter = None
    src_df = spark.createDataFrame([("r1", "v1", "v2", None, "ignored")], src_with_extra)
    state = PipelineState(df=src_df)
    result = t(state, _ctx_graceful())
    row = result.df.filter(F.col("id") == "r1").collect()[0]
    assert row["scdHash"] == expected_hash


def test_schema_order_differs_from_alphabetical(spark):
    """Schema declaration order [C, A, B] must be used, not alphabetical [A, B, C]."""
    # Schema declares columns in order C, A, B
    schema_cab = {
        "type": "struct",
        "fields": [
            {"name": "C", "type": "string", "nullable": True, "metadata": {}},
            {"name": "A", "type": "string", "nullable": True, "metadata": {}},
            {"name": "B", "type": "string", "nullable": True, "metadata": {}},
        ],
    }
    ctx = _ctx_graceful(staging_schema=schema_cab)
    # Graceful hash with trailing B=null → concat over [C, A] only
    expected = _xxhash(spark, "c_val", "a_val")
    t = SCDGracefulChangeDetectorTransformer()
    t._spark = spark
    t._logger = None
    t._adapter = None
    src_df = spark.createDataFrame([("r1", "a_val", None, "c_val")], _SRC_SCHEMA)
    state = PipelineState(df=src_df)
    result = t(state, ctx)
    row = result.df.filter(F.col("id") == "r1").collect()[0]
    assert row["scdHash"] == expected


# ── hash_separator: the context's, unless the constructor gives one (F106) ─────

def _graceful_hash(spark, step, ctx):
    step._spark = spark
    src = spark.createDataFrame([("1", "x", "y", None)], _SRC_SCHEMA)
    return step(PipelineState(df=src, target_exists=False), ctx).df.first()["scdHash"]


def _graceful_ctx_with(**scd):
    ctx = _ctx_graceful()
    return LoadContext(**{**ctx.__dict__, "scd_ctx": SCDContext({
        "business_key_column": "id", "scd_type": "scd2", "load_type": "Incremental", **scd})})


def _xxhash_sep(spark, separator, *literals):
    return spark.range(1).select(
        F.xxhash64(F.concat_ws(separator, *[F.lit(v) for v in literals])).alias("h")).first()["h"]


def test_the_graceful_variant_hashes_with_the_contexts_separator(spark):
    step = SCDGracefulChangeDetectorTransformer()
    assert _graceful_hash(spark, step, _graceful_ctx_with(hash_separator="|")) == _xxhash_sep(spark, "|", "x", "y")
    assert _graceful_hash(spark, step, _graceful_ctx_with()) == _xxhash_sep(spark, "~", "x", "y")


def test_the_graceful_variant_takes_a_constructor_separator_and_refuses_a_conflicting_one(spark):
    assert _graceful_hash(spark, SCDGracefulChangeDetectorTransformer(hash_separator="|"),
                          _graceful_ctx_with()) == _xxhash_sep(spark, "|", "x", "y")
    with pytest.raises(ValueError, match="hash_separator"):
        _graceful_hash(spark, SCDGracefulChangeDetectorTransformer(hash_separator="|"),
                       _graceful_ctx_with(hash_separator="#"))
