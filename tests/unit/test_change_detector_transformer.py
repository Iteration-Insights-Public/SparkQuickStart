"""Tests for SCDChangeDetectorTransformer — run before implementation (TDD)."""
from __future__ import annotations

import inspect
from datetime import datetime

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

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
    SCDChangeDetectorTransformer,
    SCDGracefulChangeDetectorTransformer,
)
from spark_quick_start.orchestration.transformers.state import PipelineState


def _ctx(scd_type="scd2", load_type="Incremental", source_system_from_source=False):
    return LoadContext(
        global_ctx=GlobalContext({
            "id": "ent_1",
            "entity_name": "Customer", "source_system": "CRM",
            "target_table_path": "/tmp/t", "staging_table_path": "/tmp/s",
        }),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext({
            "business_key_column": "id",
            "scd_type": scd_type,
            "load_type": load_type,
            "source_system_from_source": source_system_from_source,
        }),
        runtime_ctx=RuntimeContext({
            "correlation_id": "corr-1", "pipeline_name": "p", "application_name": "a",
            "serverless": is_serverless_session(),
        }),
        logging_ctx=LoggingContext({}),
    )


_SRC_SCHEMA = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
])

_TGT_SCHEMA = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
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


def _empty_target(spark):
    return spark.createDataFrame([], _TGT_SCHEMA)


def _target_row(spark, id_val, name, email, hash_val, version):
    return spark.createDataFrame(
        [(id_val, name, email, hash_val, version, "Y", "N", _T0, _MAX_TS, "CRM", "corr-0", "rk")],
        _TGT_SCHEMA,
    )


# ── Test 1: First load — target empty ────────────────────────────────────────

def test_first_load_empty_target_all_rows_new(spark):
    src = spark.createDataFrame([("1", "Alice", "a@x.com"), ("2", "Bob", "b@x.com")], _SRC_SCHEMA)
    state = PipelineState(df=src, frames={"target": _empty_target(spark)})
    result = SCDChangeDetectorTransformer()(state, _ctx())
    candidates = result.df

    assert candidates.count() == 2
    rows = {r["id"]: r for r in candidates.collect()}
    assert all(r["scdVersion"] == 1 for r in rows.values())
    assert all(r["_prev_scdVersion"] is None for r in rows.values())
    assert all(r["_change_type"] == "new" for r in rows.values())


# ── duplicate candidates: classified, not validated (F29) ────────────────────

def test_duplicate_source_keys_pass_through_unvalidated(spark):
    """F29 — the detector classifies and does not validate, so duplicates pass through.

    Two source rows sharing one business key both land as version=1 with the same
    source system. This used to raise here, behind a `validate_no_dups` flag. It no
    longer does: whether a duplicate candidate is acceptable depends on which pipeline
    is running, and the detector does not know that.

    The catching moved to the guard steps (`CandidateGuardTransformer`, which
    `default_pipeline` includes, and `SCDCandidateVersionGuardTransformer`). That it still
    gets caught when the detector and a guard are wired together is asserted in
    tests/integration, since it takes both units real.
    """
    src = spark.createDataFrame(
        [("1", "Alice", "a@x.com"), ("1", "Alice2", "a2@x.com")], _SRC_SCHEMA,
    )
    state = PipelineState(df=src, frames={"target": _empty_target(spark)})

    result = SCDChangeDetectorTransformer()(state, _ctx())

    assert result.df.count() == 2, "both duplicate candidates must survive classification"


# ── Test 2: First load — target None ─────────────────────────────────────────

def test_first_load_none_target_treated_as_first_load(spark):
    src = spark.createDataFrame([("1", "Alice", "a@x.com")], _SRC_SCHEMA)
    state = PipelineState(df=src, target_exists=False)
    result = SCDChangeDetectorTransformer()(state, _ctx())
    assert result.df.count() == 1
    assert result.df.first()["_change_type"] == "new"


# ── Test 3: SCD2 changed row ──────────────────────────────────────────────────

def test_scd2_changed_row_emits_one_candidate_with_incremented_version(spark):
    src = spark.createDataFrame([("1", "Alice Updated", "a@x.com")], _SRC_SCHEMA)
    tgt = _target_row(spark, "1", "Alice", "a@x.com", hash_val=999, version=3)
    state = PipelineState(df=src, frames={"target": tgt})

    result = SCDChangeDetectorTransformer()(state, _ctx())
    candidates = result.df

    assert candidates.count() == 1
    row = candidates.first()
    assert row["_change_type"] == "changed"
    assert row["scdVersion"] == 4
    assert row["_prev_scdVersion"] == 3


# ── Test 4: SCD2 unchanged row → 0 candidates ────────────────────────────────

def test_scd2_unchanged_row_produces_no_candidate(spark):
    src = spark.createDataFrame([("1", "Alice", "a@x.com")], _SRC_SCHEMA)
    t = SCDChangeDetectorTransformer()

    first = t(PipelineState(df=src, frames={"target": _empty_target(spark)}), _ctx())
    stored_hash = first.df.first()["scdHash"]

    tgt = _target_row(spark, "1", "Alice", "a@x.com", hash_val=stored_hash, version=1)
    result = t(PipelineState(df=src, frames={"target": tgt}), _ctx())
    assert result.df.count() == 0


# ── Test 5: Full-load delete ──────────────────────────────────────────────────

def test_full_load_empty_source_raises_value_error(spark):
    """Full load with empty source aborts with ValueError to prevent mass deletion."""
    src = spark.createDataFrame([], _SRC_SCHEMA)
    tgt = _target_row(spark, "1", "Alice", "a@x.com", hash_val=123, version=2)
    state = PipelineState(df=src, frames={"target": tgt})

    with pytest.raises(ValueError, match="Full load aborted"):
        SCDChangeDetectorTransformer()(state, _ctx(load_type="Full"))


# ── Test 6: Incremental — absent key produces no candidate ───────────────────

def test_incremental_absent_key_not_deleted(spark):
    src = spark.createDataFrame([], _SRC_SCHEMA)
    tgt = _target_row(spark, "1", "Alice", "a@x.com", hash_val=123, version=2)
    state = PipelineState(df=src, frames={"target": tgt})

    result = SCDChangeDetectorTransformer()(state, _ctx(load_type="Incremental"))
    assert result.df.count() == 0


# ── Test 7: SCD1 changed → version stays 1 ───────────────────────────────────

def test_scd1_changed_row_version_is_1(spark):
    src = spark.createDataFrame([("1", "Alice Updated", "a@x.com")], _SRC_SCHEMA)
    tgt = _target_row(spark, "1", "Alice", "a@x.com", hash_val=999, version=1)
    state = PipelineState(df=src, frames={"target": tgt})

    result = SCDChangeDetectorTransformer()(state, _ctx(scd_type="scd1"))
    assert result.df.count() == 1
    row = result.df.first()
    assert row["scdVersion"] == 1
    assert row["_prev_scdVersion"] == 1
    assert row["_change_type"] == "changed"


# ── Test 8: exclude_from_hash ─────────────────────────────────────────────────

def test_exclude_from_hash_column_change_suppressed(spark):
    t = SCDChangeDetectorTransformer(exclude_from_hash=["email"])
    src_base = spark.createDataFrame([("1", "Alice", "old@x.com")], _SRC_SCHEMA)
    stored_hash = t(
        PipelineState(df=src_base, frames={"target": _empty_target(spark)}), _ctx()
    ).df.first()["scdHash"]

    tgt = _target_row(spark, "1", "Alice", "old@x.com", hash_val=stored_hash, version=1)
    src_changed = spark.createDataFrame([("1", "Alice", "new@x.com")], _SRC_SCHEMA)
    result = t(PipelineState(df=src_changed, frames={"target": tgt}), _ctx())
    assert result.df.count() == 0, "Email-only change must be suppressed when email excluded from hash"


# ── Test 8b: exclude_from_hash via scd_context ────────────────────────────────

def test_exclude_from_hash_via_scd_context(spark):
    """scd_context.exclude_from_hash must suppress changes just like the constructor kwarg."""
    ctx = LoadContext(
        global_ctx=GlobalContext({
            "id": "ent_1",
            "entity_name": "Customer", "source_system": "CRM",
            "target_table_path": "/tmp/t", "staging_table_path": "/tmp/s",
        }),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext({
            "business_key_column": "id",
            "scd_type": "scd2",
            "load_type": "Incremental",
            "exclude_from_hash": ["email"],
        }),
        runtime_ctx=RuntimeContext({
            "correlation_id": "corr-1", "pipeline_name": "p", "application_name": "a",
            "serverless": is_serverless_session(),
        }),
        logging_ctx=LoggingContext({}),
    )
    t = SCDChangeDetectorTransformer()
    src_base = spark.createDataFrame([("1", "Alice", "old@x.com")], _SRC_SCHEMA)
    stored_hash = t(
        PipelineState(df=src_base, frames={"target": _empty_target(spark)}), ctx
    ).df.first()["scdHash"]

    tgt = _target_row(spark, "1", "Alice", "old@x.com", hash_val=stored_hash, version=1)
    src_changed = spark.createDataFrame([("1", "Alice", "new@x.com")], _SRC_SCHEMA)
    result = t(PipelineState(df=src_changed, frames={"target": tgt}), ctx)
    assert result.df.count() == 0, "Email-only change must be suppressed when excluded via scd_context"


# ── Test 9: NULL vs "None" produce different hashes ─────────────────────────

def test_null_and_string_none_produce_different_hashes(spark):
    null_src   = spark.createDataFrame([("1", None,   "x@x.com")], _SRC_SCHEMA)
    string_src = spark.createDataFrame([("1", "None", "x@x.com")], _SRC_SCHEMA)
    t = SCDChangeDetectorTransformer()
    empty = _empty_target(spark)

    h_null   = t(PipelineState(df=null_src,   frames={"target": empty}), _ctx()).df.first()["scdHash"]
    h_string = t(PipelineState(df=string_src, frames={"target": empty}), _ctx()).df.first()["scdHash"]
    assert h_null != h_string


# ── Test 10: source_system_injected_from_global_ctx ───────────────────────────

def test_source_system_injected_from_global_ctx(spark):
    src = spark.createDataFrame([("1", "Alice", "a@x.com")], _SRC_SCHEMA)
    result = SCDChangeDetectorTransformer()(
        PipelineState(df=src, frames={"target": _empty_target(spark)}), _ctx()
    )
    assert result.df.first()["scdSourceSystem"] == "CRM"


_SRC_WITH_SS = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("scdSourceSystem", StringType(), True),
])

def test_source_system_preserved_from_source_column(spark):
    src = spark.createDataFrame([("1", "Alice", "a@x.com", "ERP")], _SRC_WITH_SS)
    result = SCDChangeDetectorTransformer()(
        PipelineState(df=src, frames={"target": _empty_target(spark)}),
        _ctx(source_system_from_source=True),
    )
    assert result.df.first()["scdSourceSystem"] == "ERP"


# ── Test 11: Uniqueness on (bk, scdVersion, scdSourceSystem) ─────────────────

@pytest.mark.parametrize("scenario,scd_type,load_type,src_data,make_tgt", [
    (
        "first_load", "scd2", "Full",
        [("1", "Alice", "a@x.com"), ("2", "Bob", "b@x.com")],
        lambda spark: None,
    ),
    (
        "scd2_change", "scd2", "Incremental",
        [("1", "Alice Updated", "a@x.com")],
        lambda spark: _target_row(spark, "1", "Alice", "a@x.com", 999, 3),
    ),
    (
        "scd1_change", "scd1", "Incremental",
        [("1", "Alice Updated", "a@x.com")],
        lambda spark: _target_row(spark, "1", "Alice", "a@x.com", 999, 1),
    ),
])
def test_candidates_unique_on_bk_version_ss(spark, scenario, scd_type, load_type, src_data, make_tgt):
    src = spark.createDataFrame(src_data, _SRC_SCHEMA)
    tgt = make_tgt(spark)
    if tgt is None:
        tgt = _empty_target(spark)
    state = PipelineState(df=src, frames={"target": tgt})
    result = SCDChangeDetectorTransformer()(state, _ctx(scd_type=scd_type, load_type=load_type))
    candidates = result.df
    total = candidates.count()
    distinct = candidates.select("id", "scdVersion", "scdSourceSystem").distinct().count()
    assert total == distinct, f"Duplicate candidates in scenario '{scenario}'"


# ── Test 12: Guard — inbound scdHash raises ValueError ───────────────────────

def test_raises_when_staged_df_contains_scd_hash(spark):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("scdHash", LongType(), True),
    ])
    src = spark.createDataFrame([("1", 12345)], schema)
    state = PipelineState(df=src, frames={"target": _empty_target(spark)})
    with pytest.raises(ValueError, match="scdHash"):
        SCDChangeDetectorTransformer()(state, _ctx())


# ── Test 13: scdRowKey generation ────────────────────────────────────────────

def test_new_row_has_not_deleted_flag(spark):
    src = spark.createDataFrame([("1", "Alice", "a@x.com")], _SRC_SCHEMA)
    result = SCDChangeDetectorTransformer()(
        PipelineState(df=src, frames={"target": _empty_target(spark)}), _ctx()
    )
    assert result.df.first()["scdDeletedFlag"] == "N"


def test_changed_row_has_not_deleted_flag(spark):
    src = spark.createDataFrame([("1", "Alice Updated", "a@x.com")], _SRC_SCHEMA)
    tgt = _target_row(spark, "1", "Alice", "a@x.com", hash_val=999, version=1)
    result = SCDChangeDetectorTransformer()(
        PipelineState(df=src, frames={"target": tgt}), _ctx()
    )
    assert result.df.first()["scdDeletedFlag"] == "N"


def test_row_key_is_deterministic(spark):
    src = spark.createDataFrame([("1", "Alice", "a@x.com")], _SRC_SCHEMA)
    t = SCDChangeDetectorTransformer()
    r1 = t(PipelineState(df=src, frames={"target": _empty_target(spark)}), _ctx()).df.first()["scdRowKey"]
    r2 = t(PipelineState(df=src, frames={"target": _empty_target(spark)}), _ctx()).df.first()["scdRowKey"]
    assert r1 == r2
    assert r1 is not None


def test_row_key_default_is_raw_bk(spark):
    """hash_row_key defaults to False: scdRowKey == str(business_key)."""
    src = spark.createDataFrame([("BK-42", "Alice", "a@x.com")], _SRC_SCHEMA)
    row = SCDChangeDetectorTransformer()(
        PipelineState(df=src, frames={"target": _empty_target(spark)}), _ctx()
    ).df.first()
    assert row["scdRowKey"] == "BK-42"


def test_row_key_sha256_when_hash_row_key_true(spark):
    """hash_row_key=True: scdRowKey is 64-char hex SHA-256 of the BK string."""
    src = spark.createDataFrame([("BK-42", "Alice", "a@x.com")], _SRC_SCHEMA)
    ctx = LoadContext(
        global_ctx=GlobalContext({
            "id": "ent_1",
            "entity_name": "Customer", "source_system": "CRM",
            "target_table_path": "/tmp/t", "staging_table_path": "/tmp/s",
        }),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext({"business_key_column": "id", "scd_type": "scd2", "scd_row_hashing": True}),
        runtime_ctx=RuntimeContext({
            "correlation_id": "c", "pipeline_name": "p", "application_name": "a",
            "serverless": is_serverless_session(),
        }),
        logging_ctx=LoggingContext({}),
    )
    row = SCDChangeDetectorTransformer()(
        PipelineState(df=src, frames={"target": _empty_target(spark)}), ctx
    ).df.first()
    assert len(row["scdRowKey"]) == 64
    assert row["scdRowKey"] != "BK-42"


def _case_1_ctx():
    """F148 — the business key is the row key: `scdRowKey` under both names."""
    return LoadContext(
        global_ctx=GlobalContext({
            "id": "ent_1",
            "entity_name": "Customer", "source_system": "CRM",
            "target_table_path": "/tmp/t", "staging_table_path": "/tmp/s",
        }),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext({"business_key_column": "scdRowKey", "row_key_column": "scdRowKey"}),
        runtime_ctx=RuntimeContext({
            "correlation_id": "c", "pipeline_name": "p", "application_name": "a",
            "serverless": is_serverless_session(),
        }),
        logging_ctx=LoggingContext({}),
    )


def test_a_business_key_that_is_the_row_key_passes_through_unchanged(spark):
    """F148 — case 1: the key keeps its value and its type, and no second key column appears.

    A cast to string would change an integer key's type in the target; the key is also what
    the target is matched on, so it must reach the join as it was staged."""
    src = spark.createDataFrame([(42, "Alice")], "scdRowKey int, name string")
    result = SCDChangeDetectorTransformer()(
        PipelineState(df=src, target_exists=False), _case_1_ctx()).df
    assert dict(result.dtypes)["scdRowKey"] == "int"
    assert result.columns.count("scdRowKey") == 1
    assert result.first()["scdRowKey"] == 42


def test_a_business_key_that_is_the_row_key_matches_the_target_on_it(spark):
    """F148 — case 1 against a populated target: a changed row is found by its key, not new."""
    tgt_schema = StructType([f if f.name != "id" else StructField("scdRowKey", StringType(), False)
                             for f in _TGT_SCHEMA.fields if f.name != "scdRowKey"])
    tgt = spark.createDataFrame([("AW~A~1", "Alice", "a@x.com", 1, 1, "Y", "N", _T0, _MAX_TS, "CRM", "c0")],
                                tgt_schema)
    src = spark.createDataFrame([("AW~A~1", "Alicia", "a@x.com")], "scdRowKey string, name string, email string")
    rows = SCDChangeDetectorTransformer()(
        PipelineState(df=src, frames={"target": tgt}), _case_1_ctx()).df.collect()
    assert [(r["scdRowKey"], r["_change_type"], r["scdVersion"]) for r in rows] == [("AW~A~1", "changed", 2)]


# ── Test 14: Audit columns must not appear in deleted candidates ──────────────

def test_deleted_candidates_do_not_contain_audit_columns(spark):
    """Audit columns from Silver target must not appear in candidates output."""
    # Source has key "2" only; target has key "1" → key "1" absent from source → deleted candidate.
    src = spark.createDataFrame([("2", "Bob", "b@x.com")], _SRC_SCHEMA)
    tgt = _target_row(spark, "1", "Alice", "a@x.com", hash_val=123, version=2)
    state = PipelineState(df=src, frames={"target": tgt})

    result = SCDChangeDetectorTransformer()(state, _ctx(load_type="Full"))
    cols = set(result.df.columns)
    audit_cols = {"scdEffectiveFrom", "scdEffectiveTo", "scdCurrentFlag", "scdCorrelationId"}
    assert cols.isdisjoint(audit_cols), f"Audit columns leaked into candidates: {cols & audit_cols}"


# ── Test 15: Custom row_key_column from ColumnNamingPolicy ──────────────────────

def test_custom_row_key_column_name(spark):
    """Custom row_key_column from policy appears in candidates output."""
    from spark_quick_start.orchestration.column_policy import ColumnNamingPolicy
    from spark_quick_start.orchestration.models import SCDContext

    policy = ColumnNamingPolicy(
        row_key_column="natKey",
        hash_column="natHash",
        version_column="natVersion",
        effective_from_column="scdEffectiveFrom",
        effective_to_column="scdEffectiveTo",
        current_flag_column="scdCurrentFlag",
        deleted_flag_column="scdDeletedFlag",
        source_system_column="scdSourceSystem",
        correlation_id_column="scdCorrelationId",
    )
    scd_ctx = SCDContext({"business_key_column": "id"}, policy=policy)
    ctx = LoadContext(
        global_ctx=GlobalContext({
            "id": "ent_1",
            "entity_name": "Customer", "source_system": "CRM",
            "target_table_path": "/tmp/t", "staging_table_path": "/tmp/s",
        }),
        staging_ctx=StagingContext({}),
        scd_ctx=scd_ctx,
        runtime_ctx=RuntimeContext({
            "correlation_id": "corr-1", "pipeline_name": "p", "application_name": "a",
            "serverless": is_serverless_session(),
        }),
        logging_ctx=LoggingContext({}),
    )

    src_df = spark.createDataFrame([{"id": "A", "val": 1}], "id STRING, val INT")
    state = PipelineState(df=src_df, frames={"target": _empty_target(spark)})
    t = SCDChangeDetectorTransformer()
    result = t(state, ctx)

    assert "natKey" in result.df.columns
    assert "natHash" in result.df.columns
    assert "natVersion" in result.df.columns
    assert "scdRowKey" not in result.df.columns
    assert "scdHash" not in result.df.columns


# ── Test 16: Guard — required source columns must be present ─────────────────

def test_missing_business_key_column_raises(spark):
    """Clear error when the business key column is absent from the staged DataFrame."""
    ctx = _ctx()  # default: business_key_column="id"
    src_df = spark.createDataFrame([{"not_id": "A", "val": 1}])  # "id" is absent
    state = PipelineState(df=src_df, frames={})
    t = SCDChangeDetectorTransformer()
    with pytest.raises(ValueError, match="required source column.*missing.*id"):
        t(state, ctx)


def test_missing_source_deleted_flag_column_raises(spark):
    """Clear error when source_deleted_flag_column is configured but absent."""
    scd_ctx = SCDContext({
        "business_key_column": "id",
        "source_deleted_flag_column": "is_deleted",
    })
    ctx = LoadContext(
        global_ctx=GlobalContext({
            "id": "ent_1",
            "entity_name": "Customer", "source_system": "CRM",
            "target_table_path": "/tmp/t", "staging_table_path": "/tmp/s",
        }),
        staging_ctx=StagingContext({}),
        scd_ctx=scd_ctx,
        runtime_ctx=RuntimeContext({
            "correlation_id": "corr-1", "pipeline_name": "p", "application_name": "a",
            "serverless": is_serverless_session(),
        }),
        logging_ctx=LoggingContext({}),
    )
    src_df = spark.createDataFrame([{"id": "A", "val": 1}])  # "is_deleted" absent
    state = PipelineState(df=src_df, frames={})
    t = SCDChangeDetectorTransformer()
    with pytest.raises(ValueError, match="required source column.*missing.*is_deleted"):
        t(state, ctx)


def test_missing_both_required_columns_listed_in_one_error(spark):
    """Both missing columns appear in the single raised ValueError."""
    scd_ctx = SCDContext({
        "business_key_column": "id",
        "source_deleted_flag_column": "is_deleted",
    })
    ctx = LoadContext(
        global_ctx=GlobalContext({
            "id": "ent_1",
            "entity_name": "Customer", "source_system": "CRM",
            "target_table_path": "/tmp/t", "staging_table_path": "/tmp/s",
        }),
        staging_ctx=StagingContext({}),
        scd_ctx=scd_ctx,
        runtime_ctx=RuntimeContext({
            "correlation_id": "corr-1", "pipeline_name": "p", "application_name": "a",
            "serverless": is_serverless_session(),
        }),
        logging_ctx=LoggingContext({}),
    )
    src_df = spark.createDataFrame([{"val": 1}])  # neither "id" nor "is_deleted"
    state = PipelineState(df=src_df, frames={})
    t = SCDChangeDetectorTransformer()
    with pytest.raises(ValueError) as exc:
        t(state, ctx)
    msg = str(exc.value)
    assert "id" in msg
    assert "is_deleted" in msg


def test_source_deleted_flag_column_none_does_not_raise_when_column_absent(spark):
    """When source_deleted_flag_column is None (default), no error is raised even if
    a 'source_deleted_flag_column' value is absent from the DataFrame."""
    ctx = _ctx()  # source_deleted_flag_column defaults to None
    src_df = spark.createDataFrame([{"id": "A", "val": 1}])
    state = PipelineState(df=src_df, frames={})
    t = SCDChangeDetectorTransformer()
    t._spark = spark
    # Should not raise — None means "not configured"
    result = t(state, ctx)
    assert result is not None


# ── Helper for scd_ctx-parameterised tests ────────────────────────────────────

def _make_ctx(scd_ctx: SCDContext) -> LoadContext:
    return LoadContext(
        global_ctx=GlobalContext({
            "id": "ent_1",
            "entity_name": "Customer", "source_system": "SYS",
            "target_table_path": "/tmp/t", "staging_table_path": "/tmp/s",
        }),
        staging_ctx=StagingContext({}),
        scd_ctx=scd_ctx,
        runtime_ctx=RuntimeContext({
            "correlation_id": "corr-1", "pipeline_name": "p", "application_name": "a",
            "serverless": is_serverless_session(),
        }),
        logging_ctx=LoggingContext({}),
    )


# ── Test 17: Source-deleted row classified as deleted on incremental load ──────

def test_source_deleted_row_classified_as_deleted_incremental(spark):
    """Source row with deleted_flag_value is classified as 'deleted' on Incremental load."""
    scd_ctx = SCDContext({
        "business_key_column": "id",
        "source_deleted_flag_column": "is_deleted",
        "load_type": "Incremental",
        "scd_type": "scd2",
    })
    ctx = _make_ctx(scd_ctx=scd_ctx)

    tgt_df = spark.createDataFrame([{
        "id": "A", "val": 1,
        "scdHash": 12345, "scdVersion": 1, "scdRowKey": "rk_A",
        "scdSourceSystem": "SYS", "scdDeletedFlag": "N",
    }])
    src_df = spark.createDataFrame([{"id": "A", "val": 1, "is_deleted": "Y"}])
    state = PipelineState(df=src_df, frames={"target": tgt_df})

    t = SCDChangeDetectorTransformer()
    t._spark = spark
    result = t(state, ctx)

    rows = result.df.collect()
    assert len(rows) == 1
    assert rows[0]["_change_type"] == "deleted"
    assert rows[0]["scdDeletedFlag"] == "Y"
    assert rows[0]["_prev_scdVersion"] == 1  # must equal tgt scdVersion for merger to find the close-out row


# ── Test 18: Source-deleted row with no target is skipped ─────────────────────

def test_source_deleted_row_with_no_target_is_skipped(spark):
    """Source row marked deleted that has no matching target row is silently skipped."""
    scd_ctx = SCDContext({
        "business_key_column": "id",
        "source_deleted_flag_column": "is_deleted",
        "load_type": "Incremental",
    })
    ctx = _make_ctx(scd_ctx=scd_ctx)

    tgt_df = spark.createDataFrame([{
        "id": "B", "val": 2,
        "scdHash": 99999, "scdVersion": 1, "scdRowKey": "rk_B",
        "scdSourceSystem": "SYS", "scdDeletedFlag": "N",
    }])
    src_df = spark.createDataFrame([{"id": "A", "val": 1, "is_deleted": "Y"}])
    state = PipelineState(df=src_df, frames={"target": tgt_df})

    t = SCDChangeDetectorTransformer()
    t._spark = spark
    result = t(state, ctx)

    assert result.df.count() == 0


# ── Test 19: Source-deleted row excluded from new on first load ───────────────

def test_source_deleted_row_excluded_from_new_on_first_load(spark):
    """Source rows with deleted flag are not classified as 'new' on first load."""
    scd_ctx = SCDContext({
        "business_key_column": "id",
        "source_deleted_flag_column": "is_deleted",
    })
    ctx = _make_ctx(scd_ctx=scd_ctx)

    src_df = spark.createDataFrame([
        {"id": "A", "val": 1, "is_deleted": "N"},
        {"id": "B", "val": 2, "is_deleted": "Y"},
    ])
    state = PipelineState(df=src_df, frames={})

    t = SCDChangeDetectorTransformer()
    t._spark = spark
    result = t(state, ctx)

    rows = result.df.collect()
    assert len(rows) == 1
    assert rows[0]["id"] == "A"
    assert rows[0]["_change_type"] == "new"


# ── Test 20: Source-deleted flag excluded from hash ───────────────────────────

def test_source_deleted_flag_excluded_from_hash(spark):
    """is_deleted column value does not affect scdHash — only business columns are hashed."""
    scd_ctx = SCDContext({
        "business_key_column": "id",
        "source_deleted_flag_column": "is_deleted",
        "load_type": "Incremental",
    })
    ctx = _make_ctx(scd_ctx=scd_ctx)

    # Target row for "A" so both source variants go through the changed path
    tgt_df = spark.createDataFrame([{
        "id": "A", "val": 0,
        "scdHash": 99999, "scdVersion": 1, "scdRowKey": "rk_A",
        "scdSourceSystem": "SYS", "scdDeletedFlag": "N",
    }])

    # Two source rows: identical business values ("val": 1), differ only in is_deleted
    src_df1 = spark.createDataFrame([{"id": "A", "val": 1, "is_deleted": "N"}])
    src_df2 = spark.createDataFrame([{"id": "A", "val": 1, "is_deleted": "X"}])

    t = SCDChangeDetectorTransformer()
    t._spark = spark

    hash1 = t(PipelineState(df=src_df1, frames={"target": tgt_df}), ctx).df.collect()[0]["scdHash"]
    hash2 = t(PipelineState(df=src_df2, frames={"target": tgt_df}), ctx).df.collect()[0]["scdHash"]

    # Hashes must be equal — is_deleted is excluded from hash computation
    assert hash1 == hash2


# ── Test 21: Non-deleted source rows still classified normally ────────────────

def test_non_deleted_source_rows_still_classified_normally(spark):
    """Rows with is_deleted='N' are classified as changed as before."""
    scd_ctx = SCDContext({
        "business_key_column": "id",
        "source_deleted_flag_column": "is_deleted",
        "load_type": "Incremental",
        "scd_type": "scd2",
    })
    ctx = _make_ctx(scd_ctx=scd_ctx)

    tgt_df = spark.createDataFrame([{
        "id": "A", "val": 1,
        "scdHash": 99999, "scdVersion": 1, "scdRowKey": "rk_A",
        "scdSourceSystem": "SYS", "scdDeletedFlag": "N",
    }])
    src_df = spark.createDataFrame([{"id": "A", "val": 99, "is_deleted": "N"}])
    state = PipelineState(df=src_df, frames={"target": tgt_df})

    t = SCDChangeDetectorTransformer()
    t._spark = spark
    result = t(state, ctx)

    rows = result.df.collect()
    assert len(rows) == 1
    assert rows[0]["_change_type"] == "changed"


# ── Test 22: Source-deleted row classified as deleted on full load ──────────────

def test_source_deleted_row_classified_as_deleted_full_load(spark):
    """Source-deleted row on Full load is classified as 'deleted' (separate from key-absence deletes)."""
    scd_ctx = SCDContext({
        "business_key_column": "id",
        "source_deleted_flag_column": "is_deleted",
        "load_type": "Full",
        "scd_type": "scd2",
    })
    ctx = _make_ctx(scd_ctx=scd_ctx)

    tgt_df = spark.createDataFrame([
        {"id": "A", "val": 1, "scdHash": 111111, "scdVersion": 2, "scdRowKey": "rk_A",
         "scdSourceSystem": "SYS", "scdDeletedFlag": "N"},
        {"id": "B", "val": 2, "scdHash": 222222, "scdVersion": 1, "scdRowKey": "rk_B",
         "scdSourceSystem": "SYS", "scdDeletedFlag": "N"},
    ])
    # Source sends A as deleted, B is absent entirely (key-absence delete)
    src_df = spark.createDataFrame([{"id": "A", "val": 1, "is_deleted": "Y"}])
    state = PipelineState(df=src_df, frames={"target": tgt_df})

    t = SCDChangeDetectorTransformer()
    t._spark = spark
    result = t(state, ctx)

    rows = {r["id"]: r for r in result.df.collect()}
    # A: source-deleted → deleted
    assert rows["A"]["_change_type"] == "deleted"
    assert rows["A"]["scdDeletedFlag"] == "Y"
    assert rows["A"]["_prev_scdVersion"] == 2
    # B: key-absence delete → deleted
    assert rows["B"]["_change_type"] == "deleted"
    assert rows["B"]["scdDeletedFlag"] == "Y"


# ── Test 23: Output schema excludes source_deleted_flag_column ────────────────

def test_source_deleted_flag_column_not_in_output_schema(spark):
    """The control column (is_deleted) must not appear in the candidates output schema."""
    from spark_quick_start.orchestration.models import SCDContext
    scd_ctx = SCDContext({
        "business_key_column": "id",
        "source_deleted_flag_column": "is_deleted",
        "load_type": "Incremental",
    })
    ctx = _make_ctx(scd_ctx=scd_ctx)
    src_df = spark.createDataFrame([{"id": "A", "val": 1, "is_deleted": "N"}])
    state = PipelineState(df=src_df, frames={})
    t = SCDChangeDetectorTransformer()
    t._spark = spark
    result = t(state, ctx)
    assert "is_deleted" not in result.df.columns


# ── Test 24: SCD1 + source-deleted produces deleted with target version ───────

def test_source_deleted_row_scd1_produces_deleted_candidate(spark):
    """SCD1 + source_deleted_flag_column: deleted row emits one candidate with scdVersion from tgt."""
    from spark_quick_start.orchestration.models import SCDContext
    scd_ctx = SCDContext({
        "business_key_column": "id",
        "source_deleted_flag_column": "is_deleted",
        "load_type": "Incremental",
        "scd_type": "scd1",
    })
    ctx = _make_ctx(scd_ctx=scd_ctx)

    tgt_df = spark.createDataFrame([{
        "id": "A", "val": 1,
        "scdHash": "oldhash", "scdVersion": 3, "scdRowKey": "rk_A",
        "scdSourceSystem": "SYS", "scdDeletedFlag": "N",
    }])
    src_df = spark.createDataFrame([{"id": "A", "val": 1, "is_deleted": "Y"}])
    state = PipelineState(df=src_df, frames={"target": tgt_df})

    t = SCDChangeDetectorTransformer()
    t._spark = spark
    result = t(state, ctx)

    rows = result.df.collect()
    assert len(rows) == 1
    assert rows[0]["_change_type"] == "deleted"
    assert rows[0]["scdVersion"] == 3   # preserves tgt version for SCD1
    assert rows[0]["_prev_scdVersion"] == 3
    assert rows[0]["scdDeletedFlag"] == "Y"


# ── Test 25a: Multiple source-deleted keys in one batch ──────────────────────

def test_multiple_source_deleted_rows_all_classified_as_deleted(spark):
    """All rows flagged as deleted in a single batch are emitted as 'deleted' candidates."""
    scd_ctx = SCDContext({
        "business_key_column": "id",
        "source_deleted_flag_column": "is_deleted",
        "load_type": "Incremental",
        "scd_type": "scd2",
    })
    ctx = _make_ctx(scd_ctx=scd_ctx)

    tgt_df = spark.createDataFrame([
        {"id": "A", "val": 1, "scdHash": 111, "scdVersion": 1, "scdRowKey": "rk_A",
         "scdSourceSystem": "SYS", "scdDeletedFlag": "N"},
        {"id": "B", "val": 2, "scdHash": 222, "scdVersion": 3, "scdRowKey": "rk_B",
         "scdSourceSystem": "SYS", "scdDeletedFlag": "N"},
        {"id": "C", "val": 3, "scdHash": 333, "scdVersion": 2, "scdRowKey": "rk_C",
         "scdSourceSystem": "SYS", "scdDeletedFlag": "N"},
    ])
    src_df = spark.createDataFrame([
        {"id": "A", "val": 1, "is_deleted": "Y"},
        {"id": "B", "val": 2, "is_deleted": "Y"},
        {"id": "C", "val": 3, "is_deleted": "Y"},
    ])
    state = PipelineState(df=src_df, frames={"target": tgt_df})

    t = SCDChangeDetectorTransformer()
    t._spark = spark
    result = t(state, ctx)

    rows = {r["id"]: r for r in result.df.collect()}
    assert set(rows.keys()) == {"A", "B", "C"}
    for key in ("A", "B", "C"):
        assert rows[key]["_change_type"] == "deleted"
        assert rows[key]["scdDeletedFlag"] == "Y"
    # _prev_scdVersion must mirror each key's target version so the merger finds the right row to close out
    assert rows["A"]["_prev_scdVersion"] == 1
    assert rows["B"]["_prev_scdVersion"] == 3
    assert rows["C"]["_prev_scdVersion"] == 2


# ── Test 25b: Mixed batch — new, changed, and deleted in same source ───────────

def test_mixed_batch_new_changed_deleted_classified_correctly(spark):
    """A single source batch can contain new, changed, and source-deleted rows simultaneously."""
    scd_ctx = SCDContext({
        "business_key_column": "id",
        "source_deleted_flag_column": "is_deleted",
        "load_type": "Incremental",
        "scd_type": "scd2",
    })
    ctx = _make_ctx(scd_ctx=scd_ctx)

    # Target: A exists (will be changed), B exists (will be deleted). C does not exist (will be new).
    tgt_df = spark.createDataFrame([
        {"id": "A", "val": 10, "scdHash": 0, "scdVersion": 1, "scdRowKey": "rk_A",
         "scdSourceSystem": "SYS", "scdDeletedFlag": "N"},
        {"id": "B", "val": 20, "scdHash": 0, "scdVersion": 2, "scdRowKey": "rk_B",
         "scdSourceSystem": "SYS", "scdDeletedFlag": "N"},
    ])
    src_df = spark.createDataFrame([
        {"id": "A", "val": 99, "is_deleted": "N"},   # changed — different val
        {"id": "B", "val": 20, "is_deleted": "Y"},   # source-deleted — exists in target
        {"id": "C", "val": 30, "is_deleted": "N"},   # new — no target row
    ])
    state = PipelineState(df=src_df, frames={"target": tgt_df})

    t = SCDChangeDetectorTransformer()
    t._spark = spark
    result = t(state, ctx)

    rows = {r["id"]: r for r in result.df.collect()}
    assert set(rows.keys()) == {"A", "B", "C"}, f"Unexpected keys: {set(rows.keys())}"
    assert rows["A"]["_change_type"] == "changed"
    assert rows["B"]["_change_type"] == "deleted"
    assert rows["B"]["scdDeletedFlag"] == "Y"
    assert rows["B"]["_prev_scdVersion"] == 2
    assert rows["C"]["_change_type"] == "new"


# ── Test 25c: Custom deleted_flag_value (not 'Y') ─────────────────────────────

def test_custom_deleted_flag_value_respected(spark):
    """source_deleted_flag_value in SCDContext is what the source column is compared with, not a
    hardcoded 'Y' (F106: deleted_flag_value is the value the library writes)."""
    scd_ctx = SCDContext({
        "business_key_column": "id",
        "source_deleted_flag_column": "status",
        "source_deleted_flag_value": "DELETED",
        "load_type": "Incremental",
        "scd_type": "scd2",
    })
    ctx = _make_ctx(scd_ctx=scd_ctx)

    tgt_df = spark.createDataFrame([{
        "id": "A", "val": 1, "scdHash": 999, "scdVersion": 1, "scdRowKey": "rk_A",
        "scdSourceSystem": "SYS", "scdDeletedFlag": "N",
    }])
    src_df = spark.createDataFrame([{"id": "A", "val": 1, "status": "DELETED"}])
    state = PipelineState(df=src_df, frames={"target": tgt_df})

    t = SCDChangeDetectorTransformer()
    t._spark = spark
    result = t(state, ctx)

    rows = result.df.collect()
    assert len(rows) == 1
    assert rows[0]["_change_type"] == "deleted"
    assert rows[0]["scdDeletedFlag"] == "Y"


# ── Test 25d: Resurrection — soft-deleted target reappears in source ──────────

def test_resurrected_record_detected_as_changed(spark):
    """A record previously soft-deleted (scdDeletedFlag='Y' in target) that reappears
    in source with the same business data must be classified as 'changed'.

    Without this, the record stays stuck with the deletion marker as its current
    version — the hash comparison alone cannot detect the resurrection because
    scdDeletedFlag is excluded from the hash to avoid spurious version bumps.
    """
    scd_ctx = SCDContext({
        "business_key_column": "id",
        "load_type": "Incremental",
        "scd_type": "scd2",
    })
    ctx = _make_ctx(scd_ctx=scd_ctx)

    # Target: 'A' is the current version but was previously soft-deleted
    tgt_df = spark.createDataFrame([{
        "id": "A", "val": 1,
        "scdHash": 0,           # placeholder — will be overwritten in test setup
        "scdVersion": 2, "scdRowKey": "rk_A",
        "scdSourceSystem": "SYS", "scdDeletedFlag": "Y",   # ← deletion marker
    }])

    # Compute the real hash so target and source match on business columns
    t = SCDChangeDetectorTransformer()
    t._spark = spark
    # Use a first-load run to get the canonical hash for id='A', val=1
    hash_state = t(
        PipelineState(df=spark.createDataFrame([{"id": "A", "val": 1}]), frames={}),
        ctx,
    )
    canonical_hash = hash_state.df.collect()[0]["scdHash"]

    # Build the real target with the correct hash
    tgt_df = spark.createDataFrame([{
        "id": "A", "val": 1,
        "scdHash": canonical_hash,
        "scdVersion": 2, "scdRowKey": "rk_A",
        "scdSourceSystem": "SYS", "scdDeletedFlag": "Y",
    }])
    # Source: same business data (same hash) — no deleted flag
    src_df = spark.createDataFrame([{"id": "A", "val": 1}])
    state = PipelineState(df=src_df, frames={"target": tgt_df})

    result = t(state, ctx)

    rows = result.df.collect()
    assert len(rows) == 1, f"Expected 1 candidate (resurrection), got {len(rows)}"
    assert rows[0]["_change_type"] == "changed", (
        f"Expected 'changed' for resurrected record, got '{rows[0]['_change_type']}'"
    )
    assert rows[0]["scdDeletedFlag"] == "N"
    assert rows[0]["scdVersion"] == 3   # tgt version (2) + 1


# ── Test 25: Empty source on Full load raises ValueError ──────────────────────

def test_full_load_empty_source_raises(spark):
    """Full load with an empty source DataFrame must raise ValueError to prevent mass deletion."""
    scd_ctx = SCDContext({"business_key_column": "id", "load_type": "Full"})
    ctx = _make_ctx(scd_ctx=scd_ctx)

    tgt_df = spark.createDataFrame([{
        "id": "A", "val": 1,
        "scdHash": "h1", "scdVersion": 1, "scdRowKey": "rk_A",
        "scdSourceSystem": "SYS", "scdDeletedFlag": "N",
    }])
    src_schema = spark.createDataFrame([{"id": "x", "val": 0}]).schema
    src_df = spark.createDataFrame([], src_schema)

    state = PipelineState(df=src_df, frames={"target": tgt_df})
    t = SCDChangeDetectorTransformer()
    t._spark = spark

    with pytest.raises(ValueError, match="Full load aborted"):
        t(state, ctx)


def test_target_frame_key_is_no_longer_a_constructor_parameter() -> None:
    """F2 — the parameter was removed, and the removal is the fix, not a side effect.

    It accepted any string while `SCDTargetTableTransformer` and
    `NativeSCDMergeTransformer` read the literal `"target"`, so setting it left the
    detector reading an absent frame. `change_detector.py` treats an absent target as
    a first load (`first_load = tgt is None or tgt.isEmpty()`), so every source row
    was classified `new` at version 1 and the log said "First load — all source rows
    classified as new". Against a populated target that meant either two rows flagged
    current for one business key, or — when the target's current row was itself at
    version 1 — the MERGE's SCD2 update_set advancing `scdHash` past business columns
    it does not carry, which makes the next run classify the row unchanged and loses
    the change permanently.

    A `TypeError` is the intended outcome and an improvement on every reading: a
    caller passing this argument today is silently corrupting a target, and the
    frames key is not configurable on any other side, so `data-contract`'s
    "configurable on all sides or on none" resolves to none.

    Asserted on the signature rather than by constructing. A bare
    `pytest.raises(TypeError)` has a false-pass path: `ProcessTransformer` is an ABC,
    so if `_run` were ever renamed the constructor would raise
    "Can't instantiate abstract class" and the test would go green while reporting
    that the parameter is gone.
    """
    assert "target_frame_key" not in inspect.signature(SCDChangeDetectorTransformer).parameters
    assert "target_frame_key" not in inspect.signature(SCDGracefulChangeDetectorTransformer).parameters
    with pytest.raises(TypeError, match="target_frame_key"):
        SCDChangeDetectorTransformer(target_frame_key="somewhere_else")  # type: ignore[call-arg]


def test_a_target_with_two_current_rows_fans_out_to_distinct_versions(spark):
    """F29 — the premise the version guard rests on, produced by the detector rather than assumed.

    `SCDCandidateVersionGuardTransformer` groups on (business_key, source_system, version)
    because a fan-out's candidates carry *distinct* derived versions while a dirty source's
    collide. Every test of that guard hand-builds the versions, so if the detector's
    arithmetic ever changed — `_tgt_scdVersion + 1`, one candidate per matched target row —
    they would all still pass while the guard's premise was gone. That is the failure mode
    that halted F29's first attempt, one layer further out.

    The shape is a target holding two rows for one key, both flagged current:
    `load_target_dataframe` filters on that flag and returns both, and the detector's left
    join fans out to one candidate per row.

    scd2 only. Under scd1 the detector emits `F.lit(1)` for new and changed alike, so there is
    no fan-out and the guard degenerates to grouping on the key pair.
    """
    scd_ctx = SCDContext({
        "business_key_column": "id",
        "load_type": "Incremental",
        "scd_type": "scd2",
    })
    ctx = _make_ctx(scd_ctx=scd_ctx)

    # scdSourceSystem must match what the detector injects, or the join finds nothing and
    # both rows come back classified `new` — which is how a first draft of this test
    # "disproved" the fan-out. _target_row hardcodes "CRM"; _make_ctx injects "SYS".
    two_current = (
        _target_row(spark, "A", "Alice", "a@x.com", 111, 1)
        .unionByName(_target_row(spark, "A", "Alicia", "a@x.com", 222, 2))
        .withColumn("scdSourceSystem", F.lit("SYS"))
    )
    assert two_current.count() == 2, "the fixture is a target's two current rows"

    src = spark.createDataFrame([("A", "Alysia", "a@x.com")], _SRC_SCHEMA)

    t = SCDChangeDetectorTransformer()
    t._spark = spark
    result = t(PipelineState(df=src, frames={"target": two_current}), ctx)

    rows = result.df.collect()
    versions = sorted(r["scdVersion"] for r in rows)
    assert len(rows) == 2, f"the join must fan out to one candidate per current row, got {len(rows)}"
    assert versions == [2, 3], (
        "each candidate is its matched target row's version + 1, so they differ — which is "
        f"exactly what lets the version-aware guard accept this shape. Got {versions}"
    )
    assert len({r["id"] for r in rows}) == 1, "both candidates are for one business key"


# ── F127: the staged deleted flag is optional, and the delete signal when present ──────────

_F127_SRC = StructType([
    StructField("id", StringType(), False),
    StructField("val", LongType(), True),
    StructField("scdDeletedFlag", StringType(), True),
])


def _f127_ctx(**scd):
    return _make_ctx(scd_ctx=SCDContext({"business_key_column": "id", **scd}))


def _f127_run(spark, src, ctx, target=None):
    t = SCDChangeDetectorTransformer()
    t._spark = spark
    return t(PipelineState(df=src, frames={} if target is None else {"target": target}), ctx).df


def _f127_rows(df):
    assert df.columns.count("scdDeletedFlag") == 1, df.columns
    return sorted((r["id"], r["_change_type"], r["scdDeletedFlag"]) for r in df.collect())


def test_a_source_without_the_deleted_flag_loads_as_not_deleted(spark):
    src = spark.createDataFrame([("A", 1)], "id string, val long")
    assert _f127_rows(_f127_run(spark, src, _f127_ctx())) == [("A", "new", "N")]


def test_a_staged_flag_is_the_delete_signal_on_a_first_load(spark):
    src = spark.createDataFrame([("A", 1, "N"), ("B", 2, "Y"), ("C", 3, None)], _F127_SRC)
    assert _f127_rows(_f127_run(spark, src, _f127_ctx())) == [("A", "new", "N"), ("C", "new", "N")]


def test_a_staged_flag_is_the_delete_signal_on_a_later_load(spark):
    target = spark.createDataFrame([
        {"id": "A", "val": 1, "scdHash": 1, "scdVersion": 1, "scdRowKey": "A", "scdSourceSystem": "CRM",
         "scdDeletedFlag": "N"},
        {"id": "B", "val": 2, "scdHash": 2, "scdVersion": 1, "scdRowKey": "B", "scdSourceSystem": "CRM",
         "scdDeletedFlag": "N"},
    ])
    src = spark.createDataFrame([("A", 1, "Y"), ("B", 20, None)], _F127_SRC)
    ctx = _make_ctx(scd_ctx=SCDContext({"business_key_column": "id"}))
    ctx.global_ctx.source_system = "CRM"
    assert _f127_rows(_f127_run(spark, src, ctx, target)) == [("A", "deleted", "Y"), ("B", "changed", "N")]


def test_an_explicit_other_column_wins_and_the_staged_flag_is_dropped(spark):
    src = spark.createDataFrame([("A", 1, "Y", "N"), ("B", 2, "N", "Y")],
                                "id string, val long, scdDeletedFlag string, is_deleted string")
    out = _f127_run(spark, src, _f127_ctx(source_deleted_flag_column="is_deleted"))
    assert "is_deleted" not in out.columns
    assert _f127_rows(out) == [("A", "new", "N")]


def test_a_null_explicit_flag_means_not_deleted(spark):
    """Closes F108: a row whose explicit flag is NULL used to be dropped without a message."""
    src = spark.createDataFrame([("A", 1, None), ("B", 2, "Y")], "id string, val long, is_deleted string")
    out = _f127_run(spark, src, _f127_ctx(source_deleted_flag_column="is_deleted"))
    assert _f127_rows(out) == [("A", "new", "N")]


def test_the_staged_flag_is_left_out_of_the_hash(spark):
    first = spark.createDataFrame([("A", 1, "N")], _F127_SRC)
    second = spark.createDataFrame([("A", 1, None)], _F127_SRC)
    ctx = _f127_ctx()
    hashes = [_f127_run(spark, frame, ctx).collect()[0]["scdHash"] for frame in (first, second)]
    assert hashes[0] == hashes[1]


def test_a_staged_flag_is_found_whatever_its_case(spark):
    src = spark.createDataFrame([("A", 1, "N"), ("B", 2, "Y")], "id string, val long, ScdDeletedFlag string")
    out = _f127_run(spark, src, _f127_ctx())
    assert "ScdDeletedFlag" not in out.columns
    assert _f127_rows(out) == [("A", "new", "N")]


@pytest.mark.parametrize("detector", [SCDChangeDetectorTransformer, SCDGracefulChangeDetectorTransformer])
def test_a_staged_flag_does_not_change_the_hash(spark, detector):
    """A staged flag, of any value, leaves scdHash as it is without the column, for both
    detectors: tables written before F127 keep their hashes (F127 review)."""
    ctx = _f127_ctx()
    ctx.global_ctx.staging_table_schema = {"type": "struct", "fields": [
        {"name": "id", "type": "string", "nullable": False, "metadata": {}},
        {"name": "val", "type": "long", "nullable": True, "metadata": {}},
        {"name": "scdDeletedFlag", "type": "string", "nullable": True, "metadata": {}},
    ]}
    bare = spark.createDataFrame([("A", 1)], "id string, val long")
    hashes = []
    for frame in (bare, spark.createDataFrame([("A", 1, "N")], _F127_SRC),
                  spark.createDataFrame([("A", 1, None)], _F127_SRC)):
        t = detector()
        t._spark = spark
        hashes.append(t(PipelineState(df=frame, frames={}), ctx).df.collect()[0]["scdHash"])
    assert len(set(hashes)) == 1, hashes



# ── F142: a delete is not repeated while the target's current row is already deleted ─────────

_F142_TGT = (
    "id string, val long, scdHash long, scdVersion long, scdRowKey string, "
    "scdSourceSystem string, scdDeletedFlag string"
)


def _f142_run(spark, src, target_flag, load_type="Incremental", src_schema="id string, val long, is_deleted string"):
    flag = {"source_deleted_flag_column": "is_deleted"} if "is_deleted" in src_schema else {}
    ctx = _make_ctx(SCDContext({
        "business_key_column": "id", "load_type": load_type, "scd_type": "scd2", **flag,
    }))
    tgt = spark.createDataFrame([("A", 1, 1, 3, "rk_A", "SYS", target_flag)], _F142_TGT)
    t = SCDChangeDetectorTransformer()
    t._spark = spark
    return t(PipelineState(df=spark.createDataFrame(src, src_schema), frames={"target": tgt}), ctx).df


def test_a_source_delete_of_an_already_deleted_target_row_is_skipped(spark):
    assert _f142_run(spark, [("A", 1, "Y")], "Y").count() == 0


def test_a_full_load_absent_key_whose_target_row_is_already_deleted_is_skipped(spark):
    out = _f142_run(spark, [("Z", 9, "N")], "Y", load_type="Full")
    assert sorted((r["id"], r["_change_type"]) for r in out.collect()) == [("Z", "new")]


def test_a_source_delete_against_a_null_target_flag_is_still_deleted(spark):
    rows = _f142_run(spark, [("A", 1, "Y")], None).collect()
    assert [(r["_change_type"], r["scdDeletedFlag"], r["_prev_scdVersion"]) for r in rows] == [("deleted", "Y", 3)]


def test_a_deleted_target_row_with_a_not_deleted_source_is_changed_to_the_next_version(spark):
    rows = _f142_run(spark, [("A", 1, "N")], "Y").collect()
    assert [(r["_change_type"], r["scdVersion"], r["scdDeletedFlag"]) for r in rows] == [("changed", 4, "N")]
    rows = _f142_run(spark, [("A", 1)], "Y", src_schema="id string, val long").collect()
    assert [(r["_change_type"], r["scdVersion"]) for r in rows] == [("changed", 4)]


def test_a_full_load_absence_against_a_null_target_flag_is_still_deleted(spark):
    out = _f142_run(spark, [("Z", 9, "N")], None, load_type="Full")
    assert sorted((r["id"], r["_change_type"]) for r in out.collect()) == [("A", "deleted"), ("Z", "new")]


def test_the_target_is_compared_to_the_written_flag_not_the_source_signal(spark):
    """With source_deleted_flag_value="D" the target still holds "Y", what the writer stamps."""
    def run(target_flag):
        ctx = _make_ctx(SCDContext({
            "business_key_column": "id", "source_deleted_flag_column": "is_deleted",
            "source_deleted_flag_value": "D", "scd_type": "scd2",
        }))
        tgt = spark.createDataFrame([("A", 1, 1, 3, "rk_A", "SYS", target_flag)], _F142_TGT)
        src = spark.createDataFrame([("A", 1, "D")], "id string, val long, is_deleted string")
        t = SCDChangeDetectorTransformer()
        t._spark = spark
        return t(PipelineState(df=src, frames={"target": tgt}), ctx).df.collect()

    assert run("Y") == []
    assert [r["_change_type"] for r in run("N")] == ["deleted"]


# ── debug_hash_string_column is declared on the state so the writer can drop it (F111) ──

def test_a_debug_hash_string_column_is_added_and_declared_on_the_state(spark):
    src = spark.createDataFrame([("1", "Alice", "a@x.com")], _SRC_SCHEMA)
    state = PipelineState(df=src, target_exists=False)

    result = SCDChangeDetectorTransformer(debug_hash_string_column="dbg")(state, _ctx())

    assert "dbg" in result.df.columns
    assert result.debug_columns == frozenset({"dbg"})


def test_no_debug_column_is_declared_when_none_is_asked_for(spark):
    src = spark.createDataFrame([("1", "Alice", "a@x.com")], _SRC_SCHEMA)

    result = SCDChangeDetectorTransformer()(PipelineState(df=src, target_exists=False), _ctx())

    assert result.debug_columns == frozenset()


# ── The written deleted-flag vocabulary and the source's own (F106) ───────────
# `deleted_flag_value` / `not_deleted_flag_value` are what the detector writes and compares the
# target's flag with; `source_deleted_flag_value` is what the delete signal is compared with.

def _vocab_ctx(load_type="Incremental", **scd):
    return _make_ctx(SCDContext({
        "business_key_column": "id", "scd_type": "scd2", "load_type": load_type,
        "deleted_flag_value": "D", "not_deleted_flag_value": "A", **scd,
    }))


def _vocab_run(spark, src, tgt_flag, ctx, src_schema="id string, val long, is_deleted string", variant=None):
    tgt = spark.createDataFrame([("A", 1, 1, 3, "rk_A", "SYS", tgt_flag)], _F142_TGT)
    t = (variant or SCDChangeDetectorTransformer)()
    t._spark = spark
    state = PipelineState(df=spark.createDataFrame(src, src_schema), frames={"target": tgt})
    return t(state, ctx).df


@pytest.mark.parametrize("variant", [SCDChangeDetectorTransformer, SCDGracefulChangeDetectorTransformer])
def test_a_first_load_and_a_new_key_are_written_with_the_not_deleted_value(spark, variant):
    ctx = _vocab_ctx(source_deleted_flag_column="is_deleted", source_deleted_flag_value="X")
    ctx = LoadContext(**{**ctx.__dict__, "global_ctx": GlobalContext({
        "id": "ent_1", "entity_name": "Customer", "source_system": "SYS",
        "target_table_path": "/tmp/t", "staging_table_path": "/tmp/s",
        "staging_table_schema": {"type": "struct", "fields": [
            {"name": n, "type": t, "nullable": True, "metadata": {}}
            for n, t in (("id", "string"), ("val", "long"), ("is_deleted", "string"))]}})})
    first = variant()
    first._spark = spark
    out = first(PipelineState(df=spark.createDataFrame([("A", 1, None)], "id string, val long, is_deleted string"),
                              target_exists=False), ctx).df
    assert [(r["_change_type"], r["scdDeletedFlag"]) for r in out.collect()] == [("new", "A")]


def test_a_changed_row_is_written_with_the_not_deleted_value(spark):
    out = _vocab_run(spark, [("A", 2, None)], "A", _vocab_ctx(source_deleted_flag_column="is_deleted",
                                                               source_deleted_flag_value="X"))
    assert [(r["_change_type"], r["scdDeletedFlag"]) for r in out.collect()] == [("changed", "A")]


def test_a_source_delete_is_written_with_the_deleted_value_when_the_source_says_its_own(spark):
    ctx = _vocab_ctx(source_deleted_flag_column="is_deleted", source_deleted_flag_value="X")
    out = _vocab_run(spark, [("A", 1, "X")], "A", ctx)
    assert [(r["_change_type"], r["scdDeletedFlag"]) for r in out.collect()] == [("deleted", "D")]
    assert _vocab_run(spark, [("A", 1, "D")], "A", ctx).collect()[0]["_change_type"] != "deleted", \
        "the source's value is source_deleted_flag_value, not the written one"


def test_the_source_value_defaults_to_the_written_deleted_value(spark):
    ctx = _vocab_ctx(source_deleted_flag_column="is_deleted")
    out = _vocab_run(spark, [("A", 1, "D")], "A", ctx)
    assert [(r["_change_type"], r["scdDeletedFlag"]) for r in out.collect()] == [("deleted", "D")]


def test_a_staged_flag_column_is_compared_with_the_source_value(spark):
    """No source_deleted_flag_column: a staged scdDeletedFlag is the signal, read as the source says it."""
    ctx = _vocab_ctx(source_deleted_flag_value="X")
    out = _vocab_run(spark, [("A", 1, "X")], "A", ctx, src_schema="id string, val long, scdDeletedFlag string")
    assert [(r["_change_type"], r["scdDeletedFlag"]) for r in out.collect()] == [("deleted", "D")]


def test_an_already_deleted_target_row_is_recognised_by_the_written_deleted_value(spark):
    ctx = _vocab_ctx(source_deleted_flag_column="is_deleted")
    assert _vocab_run(spark, [("A", 1, "D")], "D", ctx).count() == 0, "F142 against the written value"
    # "Y" is only the default: here it is not the deleted value, so the target row is live
    assert [r["_change_type"] for r in _vocab_run(spark, [("A", 1, "D")], "Y", ctx).collect()] == ["deleted"]


def test_a_deleted_target_row_with_a_live_source_row_is_changed_back_to_the_not_deleted_value(spark):
    ctx = _vocab_ctx(source_deleted_flag_column="is_deleted")
    out = _vocab_run(spark, [("A", 1, "N")], "D", ctx)
    assert [(r["_change_type"], r["scdVersion"], r["scdDeletedFlag"]) for r in out.collect()] == [("changed", 4, "A")]


def test_a_full_load_absence_is_written_with_the_deleted_value_and_skips_the_already_deleted(spark):
    ctx = _vocab_ctx(load_type="Full", source_deleted_flag_column="is_deleted")
    out = _vocab_run(spark, [("Z", 9, "N")], "A", ctx)
    assert sorted((r["id"], r["_change_type"], r["scdDeletedFlag"]) for r in out.collect()) == [
        ("A", "deleted", "D"), ("Z", "new", "A")]
    out = _vocab_run(spark, [("Z", 9, "N")], "D", ctx)
    assert sorted((r["id"], r["_change_type"]) for r in out.collect()) == [("Z", "new")]


# ── hash_separator: the context's, unless the constructor gives one (F106) ─────

# xxhash64(concat_ws(sep, "a@x.com", "Alice")): the hash columns of the row below, in alphabetical order.
_HASH_TILDE = -7225343315537871054
_HASH_PIPE = 9127371432000752885


def _separator_ctx(**scd):
    return _make_ctx(SCDContext({"business_key_column": "id", "scd_type": "scd2", **scd}))


def _first_load_hash(spark, step, ctx):
    src = spark.createDataFrame([("1", "Alice", "a@x.com")], _SRC_SCHEMA)
    step._spark = spark
    return step(PipelineState(df=src, target_exists=False), ctx).df.first()["scdHash"]


def test_the_default_hash_is_unchanged_by_the_separator_becoming_a_context_key(spark):
    """Pinned: a context that does not give hash_separator hashes with "~" as it always did."""
    assert _first_load_hash(spark, SCDChangeDetectorTransformer(), _separator_ctx()) == _HASH_TILDE


def test_the_contexts_hash_separator_reaches_the_hash(spark):
    assert _first_load_hash(spark, SCDChangeDetectorTransformer(), _separator_ctx(hash_separator="|")) == _HASH_PIPE


def test_the_contexts_hash_separator_reaches_the_debug_string(spark):
    src = spark.createDataFrame([("1", "Alice", "a@x.com")], _SRC_SCHEMA)
    step = SCDChangeDetectorTransformer(debug_hash_string_column="dbg")
    step._spark = spark
    out = step(PipelineState(df=src, target_exists=False), _separator_ctx(hash_separator="|")).df.first()
    assert out["dbg"] == "a@x.com|Alice"


def test_a_constructor_separator_is_used_when_the_context_gives_none(spark):
    assert _first_load_hash(spark, SCDChangeDetectorTransformer(hash_separator="|"), _separator_ctx()) == _HASH_PIPE


def test_a_constructor_separator_that_matches_the_given_one_is_accepted(spark):
    ctx = _separator_ctx(hash_separator="|")
    assert _first_load_hash(spark, SCDChangeDetectorTransformer(hash_separator="|"), ctx) == _HASH_PIPE


def test_a_constructor_separator_that_differs_from_the_given_one_is_refused(spark):
    step = SCDChangeDetectorTransformer(hash_separator="|")
    step._spark = spark
    src = spark.createDataFrame([("1", "Alice", "a@x.com")], _SRC_SCHEMA)
    with pytest.raises(ValueError, match=r"hash_separator.*'\|'.*'#'"):
        step(PipelineState(df=src, target_exists=False), _separator_ctx(hash_separator="#"))


def test_one_step_reused_under_two_contexts_hashes_each_with_its_own_separator(spark):
    """The separator is resolved per run and not stored on the step."""
    step = SCDChangeDetectorTransformer()
    assert _first_load_hash(spark, step, _separator_ctx(hash_separator="|")) == _HASH_PIPE
    assert _first_load_hash(spark, step, _separator_ctx()) == _HASH_TILDE


def test_the_separator_is_a_keyword_only_option_defaulting_to_none():
    parameter = inspect.signature(SCDChangeDetectorTransformer).parameters["hash_separator"]
    assert parameter.default is None and parameter.kind is inspect.Parameter.KEYWORD_ONLY
