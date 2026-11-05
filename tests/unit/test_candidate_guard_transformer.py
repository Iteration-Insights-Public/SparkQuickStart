"""Tests for CandidateGuardTransformer."""
from __future__ import annotations

import pytest

from conftest import is_serverless_session
from spark_quick_start.orchestration.models import (
    GlobalContext,
    LoggingContext,
    RuntimeContext,
    SCDContext,
    StagingContext,
)
from spark_quick_start.orchestration.transformers.base import LoadContext
from spark_quick_start.orchestration.transformers.candidate_guard import CandidateGuardTransformer
from spark_quick_start.orchestration.transformers.candidate_version_guard import (
    SCDCandidateVersionGuardTransformer,
)
from spark_quick_start.orchestration.transformers.state import PipelineState


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
            "correlation_id": "c", "pipeline_name": "p", "application_name": "a",
            "serverless": is_serverless_session(),
        }),
        logging_ctx=LoggingContext({}),
    )


def _ctx():
    return _make_ctx(SCDContext({"business_key_column": "id", "scd_type": "scd2"}))


# ── unique keys pass through ──────────────────────────────────────────────────

def test_unique_keys_returns_state_unchanged(spark):
    df = spark.createDataFrame([
        {"id": "A", "scdSourceSystem": "SYS", "_change_type": "new"},
        {"id": "B", "scdSourceSystem": "SYS", "_change_type": "changed"},
        {"id": "C", "scdSourceSystem": "SYS", "_change_type": "deleted"},
    ])
    state = PipelineState(df=df, frames={})
    t = CandidateGuardTransformer()
    result = t(state, _ctx())
    assert result is state


def test_empty_candidates_halts_pipeline(spark):
    df = spark.createDataFrame([], "id string, scdSourceSystem string, _change_type string")
    state = PipelineState(df=df, frames={})
    result = CandidateGuardTransformer()(state, _ctx())
    assert result.halted is True
    assert result.halt_reason != ""


def test_single_row_passes(spark):
    df = spark.createDataFrame([{"id": "A", "scdSourceSystem": "SYS", "_change_type": "new"}])
    state = PipelineState(df=df, frames={})
    result = CandidateGuardTransformer()(state, _ctx())
    assert result is state


# ── duplicate keys raise ValueError ──────────────────────────────────────────

def test_duplicate_key_raises(spark):
    df = spark.createDataFrame([
        {"id": "A", "scdSourceSystem": "SYS", "_change_type": "changed"},
        {"id": "A", "scdSourceSystem": "SYS", "_change_type": "deleted"},
    ])
    state = PipelineState(df=df, frames={})
    with pytest.raises(ValueError, match="more than one candidate"):
        CandidateGuardTransformer()(state, _ctx())


def test_error_message_contains_offending_key(spark):
    df = spark.createDataFrame([
        {"id": "DUPE_KEY", "scdSourceSystem": "SYS", "_change_type": "changed"},
        {"id": "DUPE_KEY", "scdSourceSystem": "SYS", "_change_type": "deleted"},
    ])
    state = PipelineState(df=df, frames={})
    with pytest.raises(ValueError, match="DUPE_KEY"):
        CandidateGuardTransformer()(state, _ctx())


def test_error_message_contains_both_change_types(spark):
    df = spark.createDataFrame([
        {"id": "A", "scdSourceSystem": "SYS", "_change_type": "changed"},
        {"id": "A", "scdSourceSystem": "SYS", "_change_type": "deleted"},
    ])
    state = PipelineState(df=df, frames={})
    with pytest.raises(ValueError) as exc_info:
        CandidateGuardTransformer()(state, _ctx())
    msg = str(exc_info.value)
    assert "changed" in msg
    assert "deleted" in msg


def test_only_duplicate_key_flagged_not_unique_ones(spark):
    df = spark.createDataFrame([
        {"id": "A", "scdSourceSystem": "SYS", "_change_type": "changed"},
        {"id": "A", "scdSourceSystem": "SYS", "_change_type": "deleted"},
        {"id": "B", "scdSourceSystem": "SYS", "_change_type": "new"},   # unique — should not appear in error
    ])
    state = PipelineState(df=df, frames={})
    with pytest.raises(ValueError) as exc_info:
        CandidateGuardTransformer()(state, _ctx())
    msg = str(exc_info.value)
    assert "'A'" in msg
    assert "'B'" not in msg


def test_all_rows_duplicates_no_unique_keys(spark):
    """Every row is part of a duplicate group — no unique keys at all. Exercises
    the new single-pass sum-based counting (candidate_count/dup_count both
    derived from the same grouped aggregation) at the edge where there's no
    "normal" group to anchor against."""
    df = spark.createDataFrame([
        {"id": "A", "scdSourceSystem": "SYS", "_change_type": "changed"},
        {"id": "A", "scdSourceSystem": "SYS", "_change_type": "deleted"},
        {"id": "B", "scdSourceSystem": "SYS", "_change_type": "new"},
        {"id": "B", "scdSourceSystem": "SYS", "_change_type": "changed"},
    ])
    state = PipelineState(df=df, frames={})
    with pytest.raises(ValueError) as exc_info:
        CandidateGuardTransformer()(state, _ctx())
    msg = str(exc_info.value)
    assert "2 business key(s)" in msg
    assert "'A'" in msg
    assert "'B'" in msg


def test_multiple_duplicate_keys_all_reported(spark):
    df = spark.createDataFrame([
        {"id": "A", "scdSourceSystem": "SYS", "_change_type": "changed"},
        {"id": "A", "scdSourceSystem": "SYS", "_change_type": "deleted"},
        {"id": "B", "scdSourceSystem": "SYS", "_change_type": "new"},
        {"id": "B", "scdSourceSystem": "SYS", "_change_type": "changed"},
    ])
    state = PipelineState(df=df, frames={})
    with pytest.raises(ValueError) as exc_info:
        CandidateGuardTransformer()(state, _ctx())
    msg = str(exc_info.value)
    assert "'A'" in msg
    assert "'B'" in msg


# ── same key, different source systems — not a duplicate ─────────────────────

def test_same_key_different_source_system_is_not_a_duplicate(spark):
    df = spark.createDataFrame([
        {"id": "A", "scdSourceSystem": "SYS1", "_change_type": "new"},
        {"id": "A", "scdSourceSystem": "SYS2", "_change_type": "new"},
    ])
    state = PipelineState(df=df, frames={})
    result = CandidateGuardTransformer()(state, _ctx())
    assert result is state


def test_two_candidates_for_one_key_raise(spark):
    """F29 — the guard's contract on the key pair, proved without Delta.

    Two candidates for one (business_key, source_system), whatever their versions, are
    exactly what this transformer rejects; `SCDCandidateVersionGuardTransformer` is the
    variant that tells distinct versions from colliding ones.
    """
    fanned_out = spark.createDataFrame([
        {"id": "A", "scdSourceSystem": "SYS", "_change_type": "changed"},
        {"id": "A", "scdSourceSystem": "SYS", "_change_type": "changed"},
    ])

    with pytest.raises(ValueError, match="more than"):
        CandidateGuardTransformer()(PipelineState(df=fanned_out, frames={}), _ctx())


class TestVersionAwareVariant:
    """F29 — the version-aware guard, which must tell two duplicate shapes apart.

    `CandidateGuardTransformer` groups on (business_key, source_system) and is right for
    a merge load. `SCDCandidateVersionGuardTransformer` adds the version, which is the one
    difference between the two shapes that matter:

    - a target holding two current rows for one key fans out to candidates with *distinct*
      derived versions, because the detector computes `_tgt_scdVersion + 1` per matched
      target row — so it must pass;
    - a dirty source yields duplicates that *collide* on version, because on a first load
      both land at 1 — so it must raise.

    The removed `validate_no_dups` check used exactly this key, which is why removing it
    from the detector took dirty-source protection away rather than fan-out protection. It never had fan-out protection to take.
    """

    def test_a_fanout_with_distinct_versions_passes(self, spark):
        fanned_out = spark.createDataFrame([
            {"id": "A", "scdSourceSystem": "SYS", "scdVersion": 2, "_change_type": "changed"},
            {"id": "A", "scdSourceSystem": "SYS", "scdVersion": 3, "_change_type": "changed"},
        ])
        state = PipelineState(df=fanned_out, frames={})
        assert SCDCandidateVersionGuardTransformer()(state, _ctx()) is state

    def test_a_dirty_source_raises(self, spark):
        dirty = spark.createDataFrame([
            {"id": "A", "scdSourceSystem": "SYS", "scdVersion": 1, "_change_type": "new"},
            {"id": "A", "scdSourceSystem": "SYS", "scdVersion": 1, "_change_type": "new"},
        ])
        with pytest.raises(ValueError, match="Duplicate"):
            SCDCandidateVersionGuardTransformer()(PipelineState(df=dirty, frames={}), _ctx())

    def test_the_stronger_guard_rejects_what_this_one_accepts(self, spark):
        """The two are genuinely different, asserted rather than assumed."""
        fanned_out = spark.createDataFrame([
            {"id": "A", "scdSourceSystem": "SYS", "scdVersion": 2, "_change_type": "changed"},
            {"id": "A", "scdSourceSystem": "SYS", "scdVersion": 3, "_change_type": "changed"},
        ])
        state = PipelineState(df=fanned_out, frames={})
        assert SCDCandidateVersionGuardTransformer()(state, _ctx()) is state
        with pytest.raises(ValueError, match="more than"):
            CandidateGuardTransformer()(state, _ctx())


# ── the guard's cache is released on every path ───────────────────────────────

def _record_cached(monkeypatch):
    """Wrap DataFrame.cache so the frames it returns can be inspected afterwards."""
    from pyspark.sql import DataFrame

    cached: list = []
    real_cache = DataFrame.cache

    def _cache(self):
        out = real_cache(self)
        cached.append(out)
        return out

    monkeypatch.setattr(DataFrame, "cache", _cache)
    return cached


def test_cache_released_when_the_action_raises(spark, monkeypatch):
    from pyspark.sql import DataFrame

    df = spark.createDataFrame(
        [{"id": "A", "scdSourceSystem": "SYS", "_change_type": "new"}]
    )
    cached = _record_cached(monkeypatch)

    def _boom(self):
        raise RuntimeError("action failed")

    monkeypatch.setattr(DataFrame, "collect", _boom)
    ctx = _ctx()
    ctx = LoadContext(
        global_ctx=ctx.global_ctx, staging_ctx=ctx.staging_ctx, scd_ctx=ctx.scd_ctx,
        runtime_ctx=RuntimeContext({
            "correlation_id": "c", "pipeline_name": "p", "application_name": "a",
            "serverless": False,
        }),
        logging_ctx=ctx.logging_ctx,
    )
    with pytest.raises(RuntimeError, match="action failed"):
        CandidateGuardTransformer()(PipelineState(df=df, frames={}), ctx)
    assert cached, "the guard was expected to cache off serverless"
    assert all(not c.is_cached for c in cached)
