"""Tests for NativeSCDMergeTransformer and NativeSCDAppendOnlyTransformer."""
from __future__ import annotations

import sys
import types
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

# Delta stub — load_pipeline.py imports DeltaTable at module level
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

from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter
from spark_quick_start.orchestration.models import (
    GlobalContext,
    LoggingContext,
    RuntimeContext,
    SCDContext,
    StagingContext,
)
from spark_quick_start.orchestration.transformers.base import LoadContext, ProcessTransformer
from spark_quick_start.orchestration.transformers.native_scd import (
    NativeSCDAppendOnlyTransformer,
    NativeSCDMergeTransformer,
)
from spark_quick_start.orchestration.transformers.state import PipelineState


def _ctx(scd_type="scd2", load_type="Incremental", load_timestamp=None):
    return LoadContext(
        global_ctx=GlobalContext({
            "id": "ent_1",
            "entity_name": "Customer", "source_system": "CRM",
            "target_table_path": "/tmp/t", "staging_table_path": "/tmp/s",
            **({"load_timestamp": load_timestamp} if load_timestamp else {}),
        }),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext({"business_key_column": "id", "scd_type": scd_type, "load_type": load_type}),
        runtime_ctx=RuntimeContext({"correlation_id": "corr-1", "pipeline_name": "p", "application_name": "a"}),
        logging_ctx=LoggingContext({}),
    )


# Candidates schema — output of SCDChangeDetectorTransformer
_CAND_SCHEMA = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("scdHash", LongType(), True),
    StructField("scdVersion", LongType(), True),
    StructField("_prev_scdVersion", LongType(), True),
    StructField("scdRowKey", StringType(), True),
    StructField("scdDeletedFlag", StringType(), True),
    StructField("scdSourceSystem", StringType(), True),
    StructField("_change_type", StringType(), True),
])

_TGT_SCHEMA = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("scdHash", LongType(), True),
    StructField("scdVersion", LongType(), True),
    StructField("scdRowKey", StringType(), True),
    StructField("scdDeletedFlag", StringType(), True),
    StructField("scdSourceSystem", StringType(), True),
    StructField("scdCurrentFlag", StringType(), True),
    StructField("scdCorrelationId", StringType(), True),
    StructField("scdEffectiveFrom", TimestampType(), True),
    StructField("scdEffectiveTo", TimestampType(), True),
])

_T0 = datetime(2024, 1, 1)
_MAX_TS = datetime(2999, 12, 31, 23, 59, 59)


def _new_candidate(spark, id_val="1", name="Alice", version=1):
    return spark.createDataFrame(
        [(id_val, name, 12345, version, None, "rk", "N", "CRM", "new")],
        _CAND_SCHEMA,
    )


def _changed_candidate(spark, id_val="1", name="Alice Updated", new_version=2, prev_version=1):
    return spark.createDataFrame(
        [(id_val, name, 99999, new_version, prev_version, "rk", "N", "CRM", "changed")],
        _CAND_SCHEMA,
    )


def _deleted_candidate(spark, id_val="1", version=5):
    return spark.createDataFrame(
        [(id_val, "Alice", 12345, version, version, "rk", "Y", "CRM", "deleted")],
        _CAND_SCHEMA,
    )


def _tgt_row(spark, id_val="1", name="Alice", version=1):
    return spark.createDataFrame(
        [(id_val, name, 12345, version, "rk", "N", "CRM", "Y", "c0", _T0, _MAX_TS)],
        _TGT_SCHEMA,
    )


# ── Sanity ────────────────────────────────────────────────────────────────────

def test_native_scd_merge_is_process_transformer():
    assert issubclass(NativeSCDMergeTransformer, ProcessTransformer)


def test_native_scd_append_only_is_process_transformer():
    assert issubclass(NativeSCDAppendOnlyTransformer, ProcessTransformer)


# ── NativeSCDMergeTransformer ─────────────────────────────────────────────────

def _run_merge(spark, state, ctx):
    """Helper: run NativeSCDMergeTransformer with mocked helpers, capture staged view."""
    captured = {}

    def fake_merge(spark_s, scd_ctx, target_table, view_name, adapter, logger, *, target_exists=None):
        captured["df"] = spark_s.table(view_name)
        return 0

    adapter = MockPlatformAdapter(existing_tables={"/tmp/t"})

    t = NativeSCDMergeTransformer()
    t._spark = spark
    t._adapter = adapter
    t._logger = MagicMock()

    with patch("spark_quick_start.orchestration.transformers.native_scd.execute_scd_merge",
               side_effect=fake_merge), \
         patch("spark_quick_start.orchestration.transformers.native_scd.capture_merge_statistics",
               return_value=(1, 0, 0)), \
         patch("spark_quick_start.orchestration.transformers.native_scd.get_high_watermark",
               return_value="2024-01-01"):
        t(state, ctx)

    return captured.get("df"), t


def test_merge_new_row_has_audit_fields_stamped(spark):
    state = PipelineState(df=_new_candidate(spark), frames={"target": spark.createDataFrame([], _TGT_SCHEMA)})
    df, _ = _run_merge(spark, state, _ctx())
    row = df.first()
    assert row["scdEffectiveFrom"] is not None
    assert row["scdEffectiveTo"] is not None
    assert row["scdCurrentFlag"] == "Y"
    assert row["scdCorrelationId"] == "corr-1"


def test_merge_internal_cols_dropped_before_view(spark):
    state = PipelineState(df=_new_candidate(spark), frames={"target": spark.createDataFrame([], _TGT_SCHEMA)})
    df, _ = _run_merge(spark, state, _ctx())
    assert "_change_type" not in df.columns
    assert "_prev_scdVersion" not in df.columns


def test_merge_scd2_changed_produces_two_rows(spark):
    """Changed row yields new-version row + close-out row from target."""
    candidates = _changed_candidate(spark, new_version=2, prev_version=1)
    tgt = _tgt_row(spark, version=1)
    state = PipelineState(df=candidates, frames={"target": tgt})
    df, _ = _run_merge(spark, state, _ctx())

    assert df.count() == 2
    new_row   = df.filter(F.col("scdVersion") == 2).first()
    closeout  = df.filter(F.col("scdVersion") == 1).first()
    assert new_row["scdCurrentFlag"] == "Y"
    assert closeout["scdCurrentFlag"] == "N"
    assert closeout["scdEffectiveTo"] is not None


def test_merge_closeout_effective_to_before_new_effective_from(spark):
    """Close-out effective_to must be strictly earlier than new row effective_from."""
    candidates = _changed_candidate(spark, new_version=2, prev_version=1)
    tgt = _tgt_row(spark, version=1)
    state = PipelineState(df=candidates, frames={"target": tgt})
    df, _ = _run_merge(spark, state, _ctx())

    new_row  = df.filter(F.col("scdVersion") == 2).first()
    closeout = df.filter(F.col("scdVersion") == 1).first()
    assert closeout["scdEffectiveTo"] < new_row["scdEffectiveFrom"]


def test_merge_effective_from_is_consistent_within_call(spark):
    """All rows in a single __call__ must share the same scdEffectiveFrom value."""
    candidates = _changed_candidate(spark, new_version=2, prev_version=1)
    tgt = _tgt_row(spark, version=1)
    state = PipelineState(df=candidates, frames={"target": tgt})
    df, _ = _run_merge(spark, state, _ctx())

    from_values = {r["scdEffectiveFrom"] for r in df.collect() if r["scdCurrentFlag"] == "Y"}
    assert len(from_values) == 1, "All new-version rows must share the same effective_from"


def test_merge_sets_statistics_attributes(spark):
    state = PipelineState(df=_new_candidate(spark), frames={"target": spark.createDataFrame([], _TGT_SCHEMA)})
    _, t = _run_merge(spark, state, _ctx())
    assert hasattr(t, "inserted")
    assert hasattr(t, "updated")


def test_merge_logs_inserted_updated_and_duration(spark):
    """Refactored after_run must preserve the inserted/updated logging contract."""
    state = PipelineState(df=_new_candidate(spark), frames={"target": spark.createDataFrame([], _TGT_SCHEMA)})
    _, t = _run_merge(spark, state, _ctx())

    end_calls = [c for c in t._logger.info.call_args_list
                 if c.kwargs.get("tags", {}).get("event") == "end"]
    assert len(end_calls) == 1
    metric_names = {m["metricName"] for m in end_calls[0].kwargs["metrics"]}
    assert {"duration_ms", "inserted", "updated"} <= metric_names


def test_merge_populates_metrics_collector(spark):
    from spark_quick_start.orchestration.metrics import StageMetricsCollector

    state = PipelineState(df=_new_candidate(spark), frames={"target": spark.createDataFrame([], _TGT_SCHEMA)})
    ctx = _ctx()
    adapter = MockPlatformAdapter(existing_tables={"/tmp/t"})

    t = NativeSCDMergeTransformer()
    t._spark = spark
    t._adapter = adapter
    t._logger = MagicMock()
    collector = StageMetricsCollector()
    t._metrics_collector = collector

    with patch("spark_quick_start.orchestration.transformers.native_scd.execute_scd_merge",
               return_value=0), \
         patch("spark_quick_start.orchestration.transformers.native_scd.capture_merge_statistics",
               return_value=(1, 2, 0)), \
         patch("spark_quick_start.orchestration.transformers.native_scd.get_high_watermark",
               return_value="2024-01-01"):
        t(state, ctx)

    assert len(collector.stages) == 1
    sm = collector.stages[0]
    assert sm.stage == "NativeSCDMergeTransformer"
    assert sm.inserted == 1
    assert sm.updated == 2


# ── Delete close-out preserves original scdEffectiveFrom ─────────────────────

def test_merge_full_load_deleted_produces_closeout_and_new_version(spark):
    """A deleted candidate must produce 2 rows: close-out (N) of old version and new soft-delete version (Y)."""
    deleted = _deleted_candidate(spark, id_val="1", version=2)
    tgt = _tgt_row(spark, id_val="1", version=2)
    state = PipelineState(df=deleted, frames={"target": tgt})

    ctx = _ctx(scd_type="scd2", load_type="Full")
    df, _ = _run_merge(spark, state, ctx)

    assert df.count() == 2, f"Expected 2 rows (close-out + soft-delete insert), got {df.count()}"

    closed = df.filter(df["scdCurrentFlag"] == "N").first()
    assert closed is not None, "No close-out row (scdCurrentFlag=N)"
    assert closed["scdEffectiveFrom"] == _T0, f"Close-out must preserve original effective_from {_T0}"
    assert closed["scdVersion"] == 2

    new_ver = df.filter(df["scdCurrentFlag"] == "Y").first()
    assert new_ver is not None, "No soft-delete insert row (scdCurrentFlag=Y)"
    assert new_ver["scdVersion"] == 3, f"New deleted version must be old_version+1, got {new_ver['scdVersion']}"
    assert new_ver["scdDeletedFlag"] == "Y"


def test_merge_incremental_deleted_produces_closeout_and_new_version(spark):
    """Incremental deleted candidate must behave identically to Full-load deleted:
    close-out the existing current row (scdCurrentFlag=N) + insert new soft-delete version (Y).

    Previously Group C was gated on load_type='Full', leaving incremental deletes
    without a close-out — the old version would stay scdCurrentFlag='Y' forever.
    """
    deleted = _deleted_candidate(spark, id_val="1", version=2)
    tgt = _tgt_row(spark, id_val="1", version=2)
    state = PipelineState(df=deleted, frames={"target": tgt})

    ctx = _ctx(scd_type="scd2", load_type="Incremental")   # ← key difference from existing test
    df, _ = _run_merge(spark, state, ctx)

    assert df.count() == 2, f"Expected 2 rows (close-out + soft-delete insert), got {df.count()}"

    closed = df.filter(df["scdCurrentFlag"] == "N").first()
    assert closed is not None, "No close-out row (scdCurrentFlag=N)"
    assert closed["scdEffectiveFrom"] == _T0, "Close-out must preserve original scdEffectiveFrom"
    assert closed["scdVersion"] == 2

    new_ver = df.filter(df["scdCurrentFlag"] == "Y").first()
    assert new_ver is not None, "No soft-delete insert row (scdCurrentFlag=Y)"
    assert new_ver["scdVersion"] == 3, f"New deleted version must be prev+1, got {new_ver['scdVersion']}"
    assert new_ver["scdDeletedFlag"] == "Y"


# ── NativeSCDAppendOnlyTransformer ────────────────────────────────────────────

def _run_append_only(spark, state, ctx):
    """Helper: run NativeSCDAppendOnlyTransformer with mocked helpers, capture staged view."""
    captured = {}

    def fake_append_only(spark_s, target_table, view_name, adapter, logger, *, target_exists=None):
        captured["df"] = spark_s.table(view_name)
        return 0

    adapter = MockPlatformAdapter(existing_tables={"/tmp/t"})

    t = NativeSCDAppendOnlyTransformer()
    t._spark = spark
    t._adapter = adapter
    t._logger = MagicMock()

    with patch("spark_quick_start.orchestration.transformers.native_scd.execute_append_only",
               side_effect=fake_append_only), \
         patch("spark_quick_start.orchestration.transformers.native_scd.capture_write_statistics",
               return_value=2), \
         patch("spark_quick_start.orchestration.transformers.native_scd.get_high_watermark",
               return_value="2024-01-01"):
        t(state, ctx)

    return captured.get("df"), t


def test_append_only_new_candidates_are_stamped_and_appended(spark):
    state = PipelineState(df=_new_candidate(spark), frames={"target": spark.createDataFrame([], _TGT_SCHEMA)})
    df, _ = _run_append_only(spark, state, _ctx())
    row = df.first()
    assert row["scdEffectiveFrom"] is not None
    assert row["scdEffectiveTo"] is not None
    assert row["scdCurrentFlag"] == "Y"
    assert "_change_type" not in df.columns
    assert "_prev_scdVersion" not in df.columns


def test_append_only_excludes_deleted_candidates(spark):
    all_candidates = (
        _new_candidate(spark, id_val="1")
        .unionByName(_deleted_candidate(spark, id_val="3"))
    )
    state = PipelineState(df=all_candidates, frames={"target": spark.createDataFrame([], _TGT_SCHEMA)})
    df, _ = _run_append_only(spark, state, _ctx())
    assert df.count() == 1
    assert df.first()["id"] == "1"


def test_append_only_changed_candidates_raise_value_error(spark):
    """A 'changed' candidate for an append-only entity is a data-quality anomaly, not a valid update.

    The refusal names the offending business key, and nothing is written.
    """
    adapter = MockPlatformAdapter(existing_tables={"/tmp/t"})

    t = NativeSCDAppendOnlyTransformer()
    t._spark = spark
    t._adapter = adapter
    t._logger = MagicMock()

    state = PipelineState(df=_changed_candidate(spark, id_val="bk-1"), target_exists=False)

    with patch("spark_quick_start.orchestration.transformers.native_scd.execute_append_only") as mock_exec:
        with pytest.raises(ValueError, match=r"'changed'.*bk-1"):
            t(state, _ctx())
        mock_exec.assert_not_called()


def test_append_only_sets_statistics_attributes(spark):
    state = PipelineState(df=_new_candidate(spark), frames={"target": spark.createDataFrame([], _TGT_SCHEMA)})
    _, t = _run_append_only(spark, state, _ctx())
    assert t.inserted == 2


def test_append_only_captures_hwm(spark):
    state = PipelineState(df=_new_candidate(spark), frames={"target": spark.createDataFrame([], _TGT_SCHEMA)})
    _, t = _run_append_only(spark, state, _ctx())
    assert t.hwm_before == "2024-01-01"
    assert t.hwm_after == "2024-01-01"
    assert t.hwm_column == "scdEffectiveFrom"


def test_append_only_populates_metrics_collector(spark):
    from spark_quick_start.orchestration.metrics import StageMetricsCollector

    state = PipelineState(df=_new_candidate(spark), frames={"target": spark.createDataFrame([], _TGT_SCHEMA)})
    ctx = _ctx()
    adapter = MockPlatformAdapter(existing_tables={"/tmp/t"})

    t = NativeSCDAppendOnlyTransformer()
    t._spark = spark
    t._adapter = adapter
    t._logger = MagicMock()
    collector = StageMetricsCollector()
    t._metrics_collector = collector

    with patch("spark_quick_start.orchestration.transformers.native_scd.execute_append_only",
               return_value=0), \
         patch("spark_quick_start.orchestration.transformers.native_scd.capture_write_statistics",
               return_value=5), \
         patch("spark_quick_start.orchestration.transformers.native_scd.get_high_watermark",
               return_value="2024-01-01"):
        t(state, ctx)

    assert len(collector.stages) == 1
    sm = collector.stages[0]
    assert sm.stage == "NativeSCDAppendOnlyTransformer"
    assert sm.inserted == 5


def test_hwm_lookup_uses_configured_effective_from_column_not_hardcoded(spark):
    """get_high_watermark() must be called with the SCDContext's actual
    effective_from_column — not silently default to the literal 'scdEffectiveFrom'
    when a ColumnNamingPolicy renames it."""
    ctx = LoadContext(
        global_ctx=GlobalContext({
            "id": "ent_1",
            "entity_name": "Customer", "source_system": "CRM",
            "target_table_path": "/tmp/t", "staging_table_path": "/tmp/s",
        }),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext({
            "business_key_column": "id", "scd_type": "scd2", "load_type": "Incremental",
            "effective_from_column": "ValidFrom",
        }),
        runtime_ctx=RuntimeContext({"correlation_id": "corr-1", "pipeline_name": "p", "application_name": "a"}),
        logging_ctx=LoggingContext({}),
    )
    state = PipelineState(df=_new_candidate(spark), frames={"target": spark.createDataFrame([], _TGT_SCHEMA)})
    adapter = MockPlatformAdapter(existing_tables={"/tmp/t"})

    t = NativeSCDMergeTransformer()
    t._spark = spark
    t._adapter = adapter
    t._logger = MagicMock()

    with patch("spark_quick_start.orchestration.transformers.native_scd.execute_scd_merge",
               return_value=0), \
         patch("spark_quick_start.orchestration.transformers.native_scd.capture_merge_statistics",
               return_value=(1, 0, 0)), \
         patch("spark_quick_start.orchestration.transformers.native_scd.get_high_watermark",
               return_value="2024-01-01") as mock_hwm:
        t(state, ctx)

    for call in mock_hwm.call_args_list:
        assert call.kwargs.get("hwm_column") == "ValidFrom", (
            f"expected hwm_column='ValidFrom', got call: {call}"
        )
    assert mock_hwm.call_count == 2  # hwm_before + hwm_after


# =====================================================================

# HWM READ FAILURE — the swallow and what it costs
# =====================================================================


def test_merge_hwm_read_failure_is_swallowed_and_leaves_hwm_after_none(spark):
    """A failed post-merge HWM read leaves hwm_after None on a merge that succeeded.

    native_scd.py wraps both get_high_watermark() calls in `except Exception:
    pass`, so a transient failure reading the watermark cannot fail the load —
    deliberate, since the merge itself has already committed.

    F117: the orchestrator no longer takes `hwm_after` as proof the writer ran (it reads
    the counts, None until `_run` sets them), so the swallow costs only the watermark. It
    is logged as a warning rather than passed over. See
    test_load_orchestrator.py::test_ran_writer_with_failed_hwm_read_is_still_harvested.
    """
    state = PipelineState(df=_new_candidate(spark), frames={"target": spark.createDataFrame([], _TGT_SCHEMA)})

    adapter = MockPlatformAdapter(existing_tables={"/tmp/t"})

    t = NativeSCDMergeTransformer()
    t._spark = spark
    t._adapter = adapter
    t._logger = MagicMock()

    with patch("spark_quick_start.orchestration.transformers.native_scd.execute_scd_merge",
               return_value=0), \
         patch("spark_quick_start.orchestration.transformers.native_scd.capture_merge_statistics",
               return_value=(7, 3, 0)), \
         patch("spark_quick_start.orchestration.transformers.native_scd.get_high_watermark",
               side_effect=RuntimeError("transient metastore failure")):
        t(state, _ctx())

    # The merge itself succeeded and its statistics were captured.
    assert (t.inserted, t.updated) == (7, 3)
    assert t.hwm_before is None
    assert t.hwm_after is None
    warnings = [c.args[0] for c in t._logger.warning.call_args_list]
    assert any("transient metastore failure" in w and "watermark" in w for w in warnings), warnings


@pytest.mark.parametrize("cls", [NativeSCDMergeTransformer, NativeSCDAppendOnlyTransformer])
def test_a_writer_that_has_not_run_reports_no_counts(cls):
    """None until `_run` sets them: the orchestrator tells the writer that ran from one that did not."""
    t = cls()
    assert (t.inserted, t.updated) == (None, None)


class TestTargetFramePresenceIsAsserted:
    """F24 — the merger asserts an invariant it already depends on.

    `state.frames.get()` collapses "key absent" and "key present, value None" into
    the same None, and None is the *legitimate* first-load value
    (`target.py:97`, guarded by the Delta table not existing). So the close-out
    guard cannot be strengthened to "not None" — that is the first-load path.

    What makes a `deleted` candidate with no target a contradiction rather than a
    state to handle: both sites that write `_change_type = "deleted"` sit inside
    the `else` of `first_load = tgt is None or tgt.isEmpty()`
    (`change_detector.py:227`), so `deleted` proves the frame was non-None and
    non-empty at classification time.

    Before this guard, Group D ran anyway and inserted a correctly-numbered
    `prev + 1` row flagged current while Group B closed nothing out — two rows
    current for one business key. Not a NULL version, as the backlog entry
    claimed: `_prev_scdVersion` for a deleted row comes from the target's own
    version column (`change_detector.py:310`), so it is non-NULL by construction.

    Reachable only from a custom ProcessTransformer. Every shipped state method
    copies `frames`, only SourceTransformers hand-build a PipelineState and the
    orchestrator pins those to position 0, and neither factory pipeline has any
    step between the detector and the writer. So this is a guard against a shape
    that cannot be demonstrated in production, not a live bug being fixed — which
    is exactly why it needs a test rather than a field report.

    Both cases, because one assertion cannot tell them apart and that is the
    entire finding.
    """

    def test_changed_candidate_with_no_target_frame_raises(self, spark):
        """The case F24's own Verification line names, and the common one by volume.

        A first version of this guard covered `deleted` only. That would have fired
        only when a batch happened to contain a delete, so the same broken pipeline
        would pass in dev and corrupt in prod depending on data content.
        """
        changed = _changed_candidate(spark, id_val="1", new_version=2, prev_version=1)
        state = PipelineState(df=changed, frames={})

        with pytest.raises(ValueError, match="no target frame"):
            _run_merge(spark, state, _ctx(scd_type="scd2", load_type="Incremental"))

    @pytest.mark.parametrize("target_exists", [False, None], ids=["stated_absent", "never_stated"])
    def test_deleted_candidate_raises_whether_the_target_is_stated_absent_or_never_stated(self, spark, target_exists):
        """`deleted`, and the second collapse the guard covers: no key and no word from a target step.

        `target_exists=False` is what a target step says for a table that does not exist;
        `None` is a state no target step touched. Neither can precede a `deleted` row.
        """
        deleted = _deleted_candidate(spark, id_val="1", version=2)
        state = PipelineState(df=deleted, target_exists=target_exists)

        with pytest.raises(ValueError, match="no target frame"):
            _run_merge(spark, state, _ctx(scd_type="scd2", load_type="Full"))

    def test_the_message_says_when_the_target_was_stated_absent(self, spark):
        deleted = _deleted_candidate(spark, id_val="1", version=2)
        state = PipelineState(df=deleted, target_exists=False)

        with pytest.raises(ValueError, match="no target frame.*stated the target does not exist"):
            _run_merge(spark, state, _ctx(scd_type="scd2", load_type="Full"))

    def test_the_message_says_when_no_target_step_ran(self, spark):
        deleted = _deleted_candidate(spark, id_val="1", version=2)
        state = PipelineState(df=deleted)

        with pytest.raises(ValueError, match="no target frame.*no target step"):
            _run_merge(spark, state, _ctx(scd_type="scd2", load_type="Full"))

    def test_a_state_cannot_be_built_with_a_none_target_frame(self):
        """The third collapse, made unrepresentable: key present, value None."""
        with pytest.raises(TypeError, match="with_target"):
            PipelineState(df=object(), frames={"target": None})

    def test_a_genuine_first_load_still_appends(self, spark):
        """The legitimate None. All candidates are `new`, so nothing is contradicted."""
        new = _new_candidate(spark, id_val="1", version=1)
        state = PipelineState(df=new, target_exists=False)

        df, _ = _run_merge(spark, state, _ctx(scd_type="scd2", load_type="Incremental"))

        assert df.count() == 1
        assert df.first()["scdCurrentFlag"] == "Y"
        assert df.first()["scdVersion"] == 1


# ── SCD1 soft delete (F110) ───────────────────────────────────────────────────

def test_scd1_deleted_candidate_is_staged_with_audit_stamps(spark):
    """Under SCD1 a deleted candidate reaches the write: flagged deleted, stamped like an update."""
    state = PipelineState(df=_deleted_candidate(spark, id_val="1", version=1), frames={"target": _tgt_row(spark)})
    df, _ = _run_merge(spark, state, _ctx(scd_type="scd1"))

    rows = df.collect()
    assert len(rows) == 1, "the deleted candidate must be staged, no close-out or new version row"
    row = rows[0]
    assert row["scdDeletedFlag"] == "Y"
    assert row["scdVersion"] == 1
    assert row["scdEffectiveFrom"] is not None
    assert row["scdCorrelationId"] == "corr-1"
    assert "_change_type" not in df.columns


def test_scd1_new_and_changed_candidates_are_staged_not_deleted(spark):
    cands = _new_candidate(spark, id_val="1").unionByName(_changed_candidate(spark, id_val="2", new_version=1))
    state = PipelineState(df=cands, frames={"target": _tgt_row(spark, id_val="2")})
    df, _ = _run_merge(spark, state, _ctx(scd_type="scd1"))

    assert {r["id"]: r["scdDeletedFlag"] for r in df.collect()} == {"1": "N", "2": "N"}


def test_scd1_merge_soft_deletes_a_matched_deleted_row_without_touching_business_columns(spark):
    """The MERGE has a first matched clause for deleted candidates setting only the flag and the stamps."""
    # ``spark`` is not used directly: the merge builds ``F.col`` expressions, which need an
    # active SparkContext, so the test is an engine test and must carry the ``spark`` marker.
    from spark_quick_start.orchestration.load_pipeline import execute_scd_merge

    spark_m = MagicMock()
    spark_m.table.return_value.columns = ["id", "name", "scdHash", "scdVersion", "scdRowKey", "scdDeletedFlag",
                                          "scdSourceSystem", "scdCurrentFlag", "scdCorrelationId",
                                          "scdEffectiveFrom", "scdEffectiveTo"]
    adapter = MockPlatformAdapter(existing_tables={"/tmp/t"})
    merge = MagicMock()
    builder = merge.alias.return_value.merge.return_value
    builder.whenMatchedUpdate.return_value = builder
    builder.whenNotMatchedInsertAll.return_value = builder
    adapter.resolve_delta_table = MagicMock(return_value=merge)

    execute_scd_merge(spark_m, _ctx(scd_type="scd1").scd_ctx, "/tmp/t", "v", adapter, MagicMock())

    clauses = [c.kwargs for c in builder.whenMatchedUpdate.call_args_list]
    assert len(clauses) == 2
    soft = clauses[0]
    assert soft["condition"] == "src.`scdDeletedFlag` <=> 'Y'"
    assert set(soft["set"]) == {"scdDeletedFlag", "scdEffectiveFrom", "scdCorrelationId"}
    assert "name" in clauses[1]["set"] and "condition" not in clauses[1]


# ── a declared debug column never reaches the write (F111) ────────────────────

def test_merge_drops_declared_debug_columns_before_the_view(spark):
    cands = _new_candidate(spark).withColumn("dbg", F.lit("a~b"))
    state = PipelineState(df=cands, frames={"target": spark.createDataFrame([], _TGT_SCHEMA)}).with_debug_columns("dbg")

    df, _ = _run_merge(spark, state, _ctx())

    assert "dbg" not in df.columns
    assert "name" in df.columns


def test_append_only_drops_declared_debug_columns_before_the_view(spark):
    cands = _new_candidate(spark).withColumn("dbg", F.lit("a~b"))
    state = PipelineState(df=cands, target_exists=False).with_debug_columns("dbg")

    df, _ = _run_append_only(spark, state, _ctx())

    assert "dbg" not in df.columns



@pytest.mark.parametrize("present", [False, True])
def test_merge_passes_the_target_steps_answer_to_the_merge(spark, present):
    """F209: the merge creates or merges on the target step's answer, not on a second check."""
    seen = {}

    def fake_merge(spark_s, scd_ctx, target_table, view_name, adapter, logger, *, target_exists=None):
        seen["target_exists"] = target_exists
        return 0

    state = PipelineState(df=_new_candidate(spark)).with_target(
        spark.createDataFrame([], _TGT_SCHEMA) if present else None)
    t = NativeSCDMergeTransformer()
    t._spark, t._adapter, t._logger = spark, MockPlatformAdapter(existing_tables={"/tmp/t"}), MagicMock()
    with patch("spark_quick_start.orchestration.transformers.native_scd.execute_scd_merge",
               side_effect=fake_merge), \
         patch("spark_quick_start.orchestration.transformers.native_scd.capture_merge_statistics",
               return_value=(1, 0, 0)), \
         patch("spark_quick_start.orchestration.transformers.native_scd.get_high_watermark",
               return_value="2024-01-01"):
        t(state, _ctx())
    assert seen["target_exists"] is present


@pytest.mark.parametrize("present", [False, True])
def test_append_only_passes_the_target_steps_answer_to_the_write(spark, present):
    """F210: the append creates or appends on the target step's answer, not on a second check."""
    seen = {}

    def fake_append(spark_s, target, view_name, adapter, logger, *, target_exists=None):
        seen["target_exists"] = target_exists
        return 0

    state = PipelineState(df=_new_candidate(spark)).with_target(
        spark.createDataFrame([], _TGT_SCHEMA) if present else None)
    t = _wired(NativeSCDAppendOnlyTransformer, spark)
    with patch(f"{_NATIVE}.execute_append_only", side_effect=fake_append), \
         patch(f"{_NATIVE}.capture_write_statistics", return_value=1), \
         patch(f"{_NATIVE}.get_high_watermark", return_value="x"):
        t(state, _ctx())
    assert seen["target_exists"] is present


def test_append_only_passes_none_when_no_target_step_ran(spark):
    t = _wired(NativeSCDAppendOnlyTransformer, spark)
    with patch(f"{_NATIVE}.execute_append_only", return_value=0) as write, \
         patch(f"{_NATIVE}.capture_write_statistics", return_value=1), \
         patch(f"{_NATIVE}.get_high_watermark", return_value="x"):
        t(PipelineState(df=_new_candidate(spark)), _ctx())
    assert write.call_args.kwargs["target_exists"] is None


# ── The run's load time T (F106) ─────────────────────────────────────────────

_T = "2026-10-03T12:00:00.123456+00:00"
_T_DT = datetime(2026, 10, 3, 12, 0, 0, 123456, tzinfo=UTC)
_NATIVE = "spark_quick_start.orchestration.transformers.native_scd"


def _micros(moment):
    return (moment - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)


def _instants(df, column):
    """The column's values as epoch microseconds, free of the session's time zone."""
    return sorted(r[0] for r in df.select(F.unix_micros(F.col(column))).collect())


def _wired(cls, spark, *, existing=True):
    t = cls()
    t._spark, t._adapter, t._logger = spark, MockPlatformAdapter(existing_tables={"/tmp/t"} if existing else set()), MagicMock()
    return t


def test_merge_stamps_effective_from_with_the_load_time_and_closes_out_one_microsecond_before(spark):
    state = PipelineState(df=_changed_candidate(spark, new_version=2, prev_version=1),
                          frames={"target": _tgt_row(spark, version=1)})
    df, _ = _run_merge(spark, state, _ctx(load_timestamp=_T))

    new_row, closeout = df.filter(F.col("scdVersion") == 2), df.filter(F.col("scdVersion") == 1)
    assert _instants(new_row, "scdEffectiveFrom") == [_micros(_T_DT)]
    assert _instants(closeout, "scdEffectiveTo") == [_micros(_T_DT) - 1]


def test_merge_stamps_scd1_soft_deletes_with_the_load_time(spark):
    state = PipelineState(df=_deleted_candidate(spark), frames={"target": _tgt_row(spark, version=5)})
    df, _ = _run_merge(spark, state, _ctx(scd_type="scd1", load_timestamp=_T))
    assert _instants(df, "scdEffectiveFrom") == [_micros(_T_DT)]


def test_append_only_stamps_effective_from_with_the_load_time(spark):
    captured = {}

    def fake_append(spark_s, target, view_name, adapter, logger, *, target_exists=None):
        captured["df"] = spark_s.table(view_name)
        return 0

    t = _wired(NativeSCDAppendOnlyTransformer, spark)
    with patch(f"{_NATIVE}.execute_append_only", side_effect=fake_append), \
         patch(f"{_NATIVE}.capture_write_statistics", return_value=1), \
         patch(f"{_NATIVE}.get_high_watermark", return_value="x"):
        t(PipelineState(df=_new_candidate(spark), frames={}), _ctx(load_timestamp=_T))
    assert _instants(captured["df"], "scdEffectiveFrom") == [_micros(_T_DT)]


@pytest.mark.parametrize("cls", [NativeSCDMergeTransformer, NativeSCDAppendOnlyTransformer])
def test_a_load_time_equal_to_the_targets_newest_effective_from_is_not_refused(spark, cls):
    """The library does not check that load times increase (F106; the owner's decision, 2026-10-04):
    a rerun at the same T writes. The target's newest effective-from is the user's to respect."""
    t = _wired(cls, spark)
    writer = "execute_scd_merge" if cls is NativeSCDMergeTransformer else "execute_append_only"
    with patch(f"{_NATIVE}.{writer}", return_value=0) as write, \
         patch(f"{_NATIVE}.capture_merge_statistics", return_value=(1, 0, 0)), \
         patch(f"{_NATIVE}.capture_write_statistics", return_value=1), \
         patch(f"{_NATIVE}.get_high_watermark", return_value="2026-10-03 12:00:00.123456"):
        t(PipelineState(df=_new_candidate(spark), frames={"target": spark.createDataFrame([], _TGT_SCHEMA)}),
          _ctx(load_timestamp=_T))
    write.assert_called_once()
    assert t.hwm_before == "2026-10-03 12:00:00.123456"
