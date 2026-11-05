"""
Tests for LoadOrchestrator from orchestration/orchestrator.py
=============================================================

Coverage:
  - __init__: adapter resolution from platform string; explicit adapter injection
  - _resolve_adapter: unknown-platform error path (concrete per-platform
    resolution tests live in tests/unit/fabric/, tests/unit/databricks/ and
    tests/integration/local/)
  - run(): parse failure, empty-stages-no-source guard, success path (mocked pipeline),
    HWM propagation into metrics

No live Delta tables or cloud workspace is required.  Most run() tests replace
`_run_pipeline` on purpose: they are about what run() assembles into
`ExecutionResult.metrics` from transformers' already-set attributes, not about what
transformers do.  `test_run_populates_stages_with_real_pipeline` and
`test_ran_writer_with_failed_hwm_read_is_still_harvested` run the real pipeline with
stub transformers; log_runtime, LoadContextManager and OrchestratorLogger are patched
in all of them.
"""
from __future__ import annotations

import json
import sys
import types
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Stub the 'delta' package before any library code is imported.
# delta-spark is not installed in the local dev/CI environment; all tests
# that exercise run() already patch the relevant helpers so the stubs below
# are only needed to satisfy the `from delta.tables import DeltaTable` lazy
# import inside orchestrator.run().
# ---------------------------------------------------------------------------
if "delta" not in sys.modules:
    _delta_stub = types.ModuleType("delta")
    _delta_tables_stub = types.ModuleType("delta.tables")

    class _MockDeltaTable:
        @staticmethod
        def isDeltaTable(spark, path):
            return False

        @staticmethod
        def forPath(spark, path):
            return MagicMock()

    _delta_tables_stub.DeltaTable = _MockDeltaTable
    _delta_stub.tables = _delta_tables_stub
    sys.modules["delta"] = _delta_stub
    sys.modules["delta.tables"] = _delta_tables_stub

from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter
from spark_quick_start.orchestration import LoadOrchestrator
from spark_quick_start.orchestration.models import ExecutionResult

# ============================================================================
# HELPERS
# ============================================================================

def _ctx_json(
    entity: str = "Person",
    nb_names=None,
) -> str:
    """Return a minimal valid load context JSON string."""
    stages = [{"nb_name": n} for n in (nb_names or ["NB_Stage_Person"])]
    d = {
        "global_context": {
            "id": "ent_1",
            "entity_name": entity,
            "source_system": "SRC",
            "target_table_path": "/tmp/target",
            "staging_table_path": "/tmp/stage",
            "staging_table_schema": {"type": "struct", "fields": []},
        },
        "staging_context": {"stages": stages},
        "scd_context": {"business_key_column": "id"},
        "runtime_context": {
            "correlation_id": "corr-xyz",
            "pipeline_name": "PL_Person",
            "application_name": "EDW",
        },
        "logging_context": {
            "logging_level": "INFO",
            "log_file_name": "test_log",
            "log_file_dir": "/tmp/logs",
        },
    }
    return json.dumps(d)


def _empty_stages_json() -> str:
    """Context JSON with no staging phases and no dqd_context."""
    return json.dumps({
        "global_context": {
            "id": "ent_1",
            "entity_name": "E",
            "source_system": "S",
            "target_table_path": "/t",
            "staging_table_path": "/s",
        },
        "staging_context": {"stages": []},
        "scd_context": {"business_key_column": "id"},
        "runtime_context": {
            "correlation_id": "c",
            "pipeline_name": "P",
            "application_name": "A",
        },
        "logging_context": {
            "logging_level": "INFO",
            "log_file_name": "l",
            "log_file_dir": "/tmp",
        },
    })


# Patch targets for the run() success path
_LOAD_PIPELINE = "spark_quick_start.orchestration.orchestrator"


# ============================================================================
# TestLoadOrchestratorInit
# ============================================================================

class TestLoadOrchestratorInit:
    """Adapter resolution and __init__ guard rails."""

    def test_unknown_platform_raises_value_error(self):
        spark = MagicMock()
        with pytest.raises(ValueError, match="unknown"):
            LoadOrchestrator(
                load_context_json=_ctx_json(),
                spark=spark,
                platform="unknown_platform",
            )

    def test_explicit_adapter_bypasses_platform_string(self):
        """When adapter is injected, the platform string is ignored — no ValueError."""
        spark = MagicMock()
        adapter = MockPlatformAdapter()
        # "bad_platform" would raise if _resolve_adapter were called
        orch = LoadOrchestrator(
            load_context_json=_ctx_json(),
            spark=spark,
            adapter=adapter,
            platform="bad_platform",
        )
        assert orch._adapter is adapter


# ============================================================================
# TestLoadOrchestratorRun
# ============================================================================

class TestLoadOrchestratorRun:
    """run() method — what run() assembles into ExecutionResult.

    `_run_pipeline` is replaced on purpose in most tests here: they are about what `run()`
    assembles into `ExecutionResult.metrics` from transformers' already-set attributes,
    not about what transformers do. Pipeline behaviour is covered unpatched by
    `test_run_populates_stages_with_real_pipeline` and tests/integration/local/.
    """

    def _make_orch(self, ctx_json: str, spark=None) -> LoadOrchestrator:
        if spark is None:
            spark = MagicMock()
        return LoadOrchestrator(
            load_context_json=ctx_json,
            spark=spark,
            adapter=MockPlatformAdapter(),
        )

    def _run_with_mocked_pipeline(self, orch, staging_df=None, hwm_before=None, hwm_after=None):
        """Run orch.run() with `_run_pipeline` replaced on purpose (no Spark, no staging).

        The unit under test is run()'s assembly of ExecutionResult.metrics from the
        transformers' already-set attributes (the SCD writer is pre-wired with
        inserted=5, updated=2), not what transformers do. Pipeline behaviour is covered
        unpatched by `test_run_populates_stages_with_real_pipeline` and
        tests/integration/local/.
        """
        from spark_quick_start.orchestration.transformers.base import SourceTransformer
        from spark_quick_start.orchestration.transformers.native_scd import (
            NativeSCDMergeTransformer,
        )
        from spark_quick_start.orchestration.transformers.state import PipelineState

        mock_state = PipelineState(df=MagicMock())
        mock_source = MagicMock(spec=SourceTransformer)

        # Pre-wire SCD transformer with canned metrics
        scd_tx = NativeSCDMergeTransformer()
        scd_tx.inserted = 5
        scd_tx.updated = 2
        scd_tx.hwm_before = hwm_before
        scd_tx.hwm_after = hwm_after
        scd_tx.hwm_column = "scdEffectiveFrom"

        orch._transformers = [mock_source, scd_tx]

        with patch(f"{_LOAD_PIPELINE}.LoadContextManager") as MockCtxMgr, \
             patch("spark_quick_start.orchestration.orchestrator.OrchestratorLogger") as MockLogger, \
             patch("spark_quick_start.orchestration.load_pipeline.log_runtime"), \
             patch.object(LoadOrchestrator, '_run_pipeline', return_value=mock_state):

            mock_ctx = MagicMock()
            mock_ctx.runtime_ctx.correlation_id = "corr-xyz"
            mock_ctx.global_ctx.entity_name = "Person"
            mock_ctx.global_ctx.target_table_path = "/tmp/target"
            mock_ctx.global_ctx.staging_table_path = "/tmp/stage"
            mock_ctx.global_ctx.source_system = "SRC"
            mock_ctx.global_ctx.staging_table_schema = {}
            mock_ctx.scd_ctx.scd_type = "scd2"
            mock_ctx.scd_ctx.business_key_column = "id"
            mock_ctx.scd_ctx.load_type = "Incremental"
            mock_ctx.staging_ctx.stages = [{"nb_name": "NB_Stage_Person"}]
            mock_ctx.hwm_ctx = None
            MockCtxMgr.return_value = mock_ctx

            mock_logger_instance = MagicMock()
            mock_logger_instance.finalize.return_value = ""
            MockLogger.from_logging_context.return_value = mock_logger_instance
            MockLogger.return_value = mock_logger_instance

            return orch.run()

    # ── Individual test cases ───────────────────────────────────────────────

    def test_run_returns_execution_result(self):
        orch = self._make_orch(_ctx_json())
        # hwm_after set: the harvest treats `hwm_after is not None` as proof the writer ran
        result = self._run_with_mocked_pipeline(orch, hwm_after="2024-02")
        assert isinstance(result, ExecutionResult)
        assert result.metrics["inserted_count"] == 5  # pre-wired by the helper; a broken harvest fails here
        assert result.status == "success"
        assert result.has_error is False

    def test_run_parse_failure_returns_error_result(self):
        """Malformed JSON causes run() to return an error ExecutionResult, not raise."""
        spark = MagicMock()
        orch = LoadOrchestrator(
            load_context_json="{not valid json",
            spark=spark,
            adapter=MockPlatformAdapter(),
        )
        result = orch.run()
        assert result.status == "error"
        assert result.has_error is True

    def test_run_error_details_has_the_pinned_shape(self):
        """F114: error_details is ErrorDetails' dict form, with exactly the keys it always had."""
        orch = LoadOrchestrator(
            load_context_json="{not valid json", spark=MagicMock(), adapter=MockPlatformAdapter())
        details = orch.run().error_details
        assert set(details) == {"error_type", "error_message", "traceback"}
        assert details["error_type"] == "JSONDecodeError"
        assert details["error_message"].startswith("Expecting property name")
        assert "JSONDecodeError" in details["traceback"]

    def test_run_empty_stages_no_source_returns_error(self):
        """
        No staging phases and no dqd_context: validate_all() raises.
        run() must return an error result whose error_message references the missing stages.
        """
        spark = MagicMock()
        orch = LoadOrchestrator(
            load_context_json=_empty_stages_json(),
            spark=spark,
            adapter=MockPlatformAdapter(),
        )
        result = orch.run()
        assert result.has_error is True
        # Either validate_all raises ("staging_context.stages") or
        # _execute_data_acquisition raises ("nothing to execute") — both are valid.
        error_msg = result.error_details.get("error_message", "")
        assert "staging" in error_msg.lower() or "nothing to execute" in error_msg

    def test_run_hwm_captured_in_metrics(self):
        """HWM before and after the merge are forwarded into metrics."""
        orch = self._make_orch(_ctx_json())
        result = self._run_with_mocked_pipeline(
            orch,
            hwm_before="2024-01-01",
            hwm_after="2024-06-01",
        )
        assert result.metrics["hwm_value_before"] == "2024-01-01"
        assert result.metrics["hwm_value_after"] == "2024-06-01"

    def test_run_source_and_target_count_captured_in_metrics(self):
        """StagingTransformer's .source_count/.target_count (already populated
        for free from Delta write metadata — see delta_utils.write_staging_data)
        must flow into the final ExecutionResult.metrics."""
        from spark_quick_start.orchestration.transformers.native_scd import (
            NativeSCDMergeTransformer,
        )
        from spark_quick_start.orchestration.transformers.staging import StagingTransformer

        source = StagingTransformer()
        source.source_count = 226886
        source.target_count = 226886
        scd_tx = NativeSCDMergeTransformer()

        orch = self._make_orch(_ctx_json())
        result = self._run_with_mocked_pipeline_transformers(orch, [source, scd_tx])

        assert result.metrics["source_count"] == 226886
        assert result.metrics["target_count"] == 226886

    def test_run_source_count_none_collapses_to_zero(self):
        """DQDTransformer without hwm_context leaves source_count=None (not
        computed) — the orchestrator must collapse that to 0 in metrics
        rather than propagate None (ExecutionMetrics.source_count is typed
        int, not Optional[int])."""
        from spark_quick_start.orchestration.transformers.dqd import DQDTransformer
        from spark_quick_start.orchestration.transformers.native_scd import (
            NativeSCDMergeTransformer,
        )

        source = DQDTransformer()
        source.source_count = None
        source.target_count = None
        scd_tx = NativeSCDMergeTransformer()

        orch = self._make_orch(_ctx_json())
        result = self._run_with_mocked_pipeline_transformers(orch, [source, scd_tx])

        assert result.metrics["source_count"] == 0
        assert result.metrics["target_count"] == 0

    def _run_with_mocked_pipeline_transformers(self, orch, transformers):
        """Like _run_with_mocked_pipeline, but with a caller-supplied
        transformer list instead of the hardcoded mock_source + scd_tx pair."""
        from spark_quick_start.orchestration.transformers.state import PipelineState

        mock_state = PipelineState(df=MagicMock())
        orch._transformers = transformers

        with patch(f"{_LOAD_PIPELINE}.LoadContextManager") as MockCtxMgr, \
             patch("spark_quick_start.orchestration.orchestrator.OrchestratorLogger") as MockLogger, \
             patch("spark_quick_start.orchestration.load_pipeline.log_runtime"), \
             patch.object(LoadOrchestrator, '_run_pipeline', return_value=mock_state):

            mock_ctx = MagicMock()
            mock_ctx.runtime_ctx.correlation_id = "corr-xyz"
            mock_ctx.global_ctx.entity_name = "Person"
            mock_ctx.global_ctx.target_table_path = "/tmp/target"
            mock_ctx.global_ctx.staging_table_path = "/tmp/stage"
            mock_ctx.global_ctx.source_system = "SRC"
            mock_ctx.global_ctx.staging_table_schema = {}
            mock_ctx.scd_ctx.scd_type = "scd2"
            mock_ctx.scd_ctx.business_key_column = "id"
            mock_ctx.scd_ctx.load_type = "Incremental"
            mock_ctx.staging_ctx.stages = [{"nb_name": "NB_Stage_Person"}]
            mock_ctx.hwm_ctx = None
            MockCtxMgr.return_value = mock_ctx

            mock_logger_instance = MagicMock()
            mock_logger_instance.finalize.return_value = ""
            MockLogger.from_logging_context.return_value = mock_logger_instance
            MockLogger.return_value = mock_logger_instance

            return orch.run()

    def test_run_hwm_from_a_source_wrapped_in_conditional_source_transformer_is_not_ignored(self):
        """Regression: a DQDTransformer (or any HWMProvider source) picked via
        .set_source()/ConditionalSourceTransformer must still win the HWM scan —
        it must not silently fall back to the target table's scdEffectiveFrom
        just because it's wrapped."""
        from spark_quick_start.orchestration.transformers.base import SourceTransformer
        from spark_quick_start.orchestration.transformers.conditional_source import (
            ConditionalSourceTransformer,
        )
        from spark_quick_start.orchestration.transformers.native_scd import (
            NativeSCDMergeTransformer,
        )
        from spark_quick_start.orchestration.transformers.state import PipelineState

        mock_state = PipelineState(df=MagicMock())

        # The wrapped source that "already ran" and captured a real source watermark.
        wrapped_source = MagicMock(spec=SourceTransformer)
        wrapped_source.hwm_before = "2024-01-01"
        wrapped_source.hwm_after = "2024-09-01"
        wrapped_source.hwm_column = "last_seen_at"

        wrapper = ConditionalSourceTransformer(
            predicate=True, source_if_true=wrapped_source, source_if_false=MagicMock(spec=SourceTransformer),
        )
        wrapper.selected_source = wrapped_source  # what _produce() would have set

        # SCD transformer registered after the source — must NOT win, since the
        # source (position 0) takes priority when it has a real hwm_after.
        scd_tx = NativeSCDMergeTransformer()
        scd_tx.inserted = 0
        scd_tx.updated = 0
        scd_tx.hwm_before = "2024-01-01"
        scd_tx.hwm_after = "2024-01-01"  # would be the wrong (target-table) value
        scd_tx.hwm_column = "scdEffectiveFrom"

        orch = self._make_orch(_ctx_json())
        orch._transformers = [wrapper, scd_tx]

        with patch(f"{_LOAD_PIPELINE}.LoadContextManager") as MockCtxMgr, \
             patch("spark_quick_start.orchestration.orchestrator.OrchestratorLogger") as MockLogger, \
             patch("spark_quick_start.orchestration.load_pipeline.log_runtime"), \
             patch.object(LoadOrchestrator, '_run_pipeline', return_value=mock_state):

            mock_ctx = MagicMock()
            mock_ctx.runtime_ctx.correlation_id = "corr-xyz"
            mock_ctx.global_ctx.entity_name = "Person"
            mock_ctx.global_ctx.target_table_path = "/tmp/target"
            mock_ctx.global_ctx.staging_table_path = "/tmp/stage"
            mock_ctx.global_ctx.source_system = "SRC"
            mock_ctx.global_ctx.staging_table_schema = {}
            mock_ctx.scd_ctx.scd_type = "scd2"
            mock_ctx.scd_ctx.business_key_column = "id"
            mock_ctx.scd_ctx.load_type = "Incremental"
            mock_ctx.staging_ctx.stages = [{"nb_name": "NB_Stage_Person"}]
            mock_ctx.hwm_ctx = None
            MockCtxMgr.return_value = mock_ctx

            mock_logger_instance = MagicMock()
            mock_logger_instance.finalize.return_value = ""
            MockLogger.from_logging_context.return_value = mock_logger_instance
            MockLogger.return_value = mock_logger_instance

            result = orch.run()

        assert result.metrics["hwm_value_after"] == "2024-09-01", (
            "expected the wrapped source's real hwm_after, not a fallback"
        )

    def test_run_inserted_updated_from_conditional_scd_transformer_that_actually_ran(self):
        """Regression: when two SCD transformers are registered via mutually
        exclusive .add_if() branches (e.g. NativeSCDAppendOnlyTransformer for
        append_only and NativeSCDMergeTransformer for not append_only — the
        exact pattern reference_design/local/NB_BI_LoadEntity_Gen3.ipynb uses),
        inserted/updated must come from whichever one's predicate actually
        evaluated True and ran — not from the first SCD-typed transformer in
        registration order, which may never have run at all and still be
        sitting at its __init__ defaults of inserted=0/updated=0.

        `_run_pipeline` is replaced on purpose and no transformer's `_run` executes: the
        attributes a run would leave behind are preset, because the unit under test is
        run()'s harvest over them."""
        from spark_quick_start.orchestration.transformers.conditional import (
            ConditionalTransformer,
        )
        from spark_quick_start.orchestration.transformers.native_scd import (
            NativeSCDAppendOnlyTransformer,
            NativeSCDMergeTransformer,
        )
        from spark_quick_start.orchestration.transformers.state import PipelineState

        mock_state = PipelineState(df=MagicMock())

        # Registered first (append_only=False here, so its predicate is False —
        # it never runs and stays at its __init__ defaults).
        never_ran = NativeSCDAppendOnlyTransformer()
        never_ran_wrapper = ConditionalTransformer(predicate=False, transformer=never_ran)

        # Registered second — its predicate is True, so it's the one that
        # actually ran and captured the real merge statistics.
        actually_ran = NativeSCDMergeTransformer()
        actually_ran.inserted = 226886
        actually_ran.updated = 0
        actually_ran.hwm_before = None
        actually_ran.hwm_after = "2026-08-27 13:58:36.071802"
        actually_ran.hwm_column = "scdEffectiveFrom"
        actually_ran_wrapper = ConditionalTransformer(predicate=True, transformer=actually_ran)

        orch = self._make_orch(_ctx_json())
        orch._transformers = [never_ran_wrapper, actually_ran_wrapper]

        with patch(f"{_LOAD_PIPELINE}.LoadContextManager") as MockCtxMgr, \
             patch("spark_quick_start.orchestration.orchestrator.OrchestratorLogger") as MockLogger, \
             patch("spark_quick_start.orchestration.load_pipeline.log_runtime"), \
             patch.object(LoadOrchestrator, '_run_pipeline', return_value=mock_state):

            mock_ctx = MagicMock()
            mock_ctx.runtime_ctx.correlation_id = "corr-xyz"
            mock_ctx.global_ctx.entity_name = "Person"
            mock_ctx.global_ctx.target_table_path = "/tmp/target"
            mock_ctx.global_ctx.staging_table_path = "/tmp/stage"
            mock_ctx.global_ctx.source_system = "SRC"
            mock_ctx.global_ctx.staging_table_schema = {}
            mock_ctx.scd_ctx.scd_type = "scd2"
            mock_ctx.scd_ctx.business_key_column = "id"
            mock_ctx.scd_ctx.load_type = "Incremental"
            mock_ctx.staging_ctx.stages = [{"nb_name": "NB_Stage_Person"}]
            mock_ctx.hwm_ctx = None
            MockCtxMgr.return_value = mock_ctx

            mock_logger_instance = MagicMock()
            mock_logger_instance.finalize.return_value = ""
            MockLogger.from_logging_context.return_value = mock_logger_instance
            MockLogger.return_value = mock_logger_instance

            result = orch.run()

        assert result.metrics["inserted_count"] == 226886, (
            "expected the actually-run transformer's real count, not the "
            "never-run transformer's __init__ default of 0"
        )
        assert result.metrics["updated_count"] == 0

    def test_run_harvests_scd_metrics_from_any_protocol_implementer(self):
        """A transformer outside the shipped NativeSCD* family must be harvested.

        The orchestrator used to discover SCD transformers with a hardcoded
        isinstance() against those three concrete classes, so any new or custom
        SCD writer required editing orchestrator.py to be seen at all. It now
        matches on the SCDMetricsProvider protocol, like the HWMProvider scan
        beside it — no registration, no central edit.

        `_run_pipeline` is replaced on purpose and the writer's `_run` never executes:
        its attributes are preset, because the unit under test is run()'s harvest.
        """
        from spark_quick_start.orchestration.transformers.base import ProcessTransformer
        from spark_quick_start.orchestration.transformers.state import PipelineState

        class CustomSCDWriter(ProcessTransformer):
            """Not a NativeSCD* subclass — satisfies the protocol structurally."""

            def __init__(self) -> None:
                self.inserted: int = 4242
                self.updated: int = 17
                self.hwm_before: str | None = None
                self.hwm_after: str | None = "2026-01-01"
                self.hwm_column: str | None = "scdEffectiveFrom"

            def _run(self, state, ctx):
                return state

        mock_state = PipelineState(df=MagicMock())
        orch = self._make_orch(_ctx_json())
        orch._transformers = [CustomSCDWriter()]

        with patch(f"{_LOAD_PIPELINE}.LoadContextManager") as MockCtxMgr, \
             patch("spark_quick_start.orchestration.orchestrator.OrchestratorLogger") as MockLogger, \
             patch("spark_quick_start.orchestration.load_pipeline.log_runtime"), \
             patch.object(LoadOrchestrator, '_run_pipeline', return_value=mock_state):

            mock_ctx = MagicMock()
            mock_ctx.runtime_ctx.correlation_id = "corr-custom"
            mock_ctx.global_ctx.entity_name = "Person"
            mock_ctx.global_ctx.target_table_path = "/tmp/target"
            mock_ctx.global_ctx.staging_table_path = "/tmp/stage"
            mock_ctx.global_ctx.staging_table_schema = {}
            mock_ctx.scd_ctx.business_key_column = "id"
            mock_ctx.staging_ctx.stages = [{"nb_name": "NB_Stage_Person"}]
            mock_ctx.hwm_ctx = None
            MockCtxMgr.return_value = mock_ctx

            mock_logger_instance = MagicMock()
            mock_logger_instance.finalize.return_value = ""
            MockLogger.from_logging_context.return_value = mock_logger_instance
            MockLogger.return_value = mock_logger_instance

            result = orch.run()

        assert result.metrics["inserted_count"] == 4242
        assert result.metrics["updated_count"] == 17

    def test_scd_writers_declare_counts_as_none_until_they_run(self):
        """Both writers declare `inserted` and `updated`, None until `_run` sets them (F117).

        The orchestrator previously read this with getattr(tx, "updated", 0),
        which cannot distinguish "attribute absent" from "genuinely zero"; None says
        "has not run" and is what its harvest skips.
        """
        from spark_quick_start.orchestration.transformers.native_scd import (
            NativeSCDAppendOnlyTransformer,
            NativeSCDMergeTransformer,
        )
        from spark_quick_start.orchestration.transformers.protocols import SCDMetricsProvider

        for cls in (
            NativeSCDMergeTransformer,
            NativeSCDAppendOnlyTransformer,
        ):
            tx = cls()
            assert isinstance(tx, SCDMetricsProvider), f"{cls.__name__} must satisfy the protocol"
            assert tx.inserted is None and tx.updated is None, f"{cls.__name__} must declare None counts"

    def test_non_scd_transformers_do_not_satisfy_the_protocol(self):
        """Guard against the protocol being so loose it matches unrelated steps."""
        from spark_quick_start.orchestration.transformers.candidate_guard import (
            CandidateGuardTransformer,
        )
        from spark_quick_start.orchestration.transformers.debug import DebugTransformer
        from spark_quick_start.orchestration.transformers.protocols import SCDMetricsProvider

        assert not isinstance(CandidateGuardTransformer(), SCDMetricsProvider)
        assert not isinstance(DebugTransformer("x"), SCDMetricsProvider)

    def test_run_populates_stages_with_real_pipeline(self):
        """A real _run_pipeline call must thread a StageMetricsCollector into every
        transformer and merge the collected StageMetrics into ExecutionResult.stages."""
        from spark_quick_start.orchestration.transformers.base import (
            ProcessTransformer,
            SourceTransformer,
        )
        from spark_quick_start.orchestration.transformers.state import PipelineState

        class FakeSource(SourceTransformer):
            def _produce(self, ctx):
                df = MagicMock()
                df.columns = ["id", "scdDeletedFlag"]
                df.rdd.getNumPartitions.return_value = 2
                return PipelineState(df=df)

        class FakeProcess(ProcessTransformer):
            def _run(self, state, ctx):
                return state

        orch = self._make_orch(_ctx_json())
        orch._transformers = [FakeSource(), FakeProcess()]

        with patch(f"{_LOAD_PIPELINE}.LoadContextManager") as MockCtxMgr, \
             patch("spark_quick_start.orchestration.orchestrator.OrchestratorLogger") as MockLogger, \
             patch("spark_quick_start.orchestration.load_pipeline.log_runtime"):

            mock_ctx = MagicMock()
            mock_ctx.runtime_ctx.correlation_id = "corr-xyz"
            mock_ctx.runtime_ctx.capture_row_counts = False
            mock_ctx.global_ctx.entity_name = "Person"
            mock_ctx.global_ctx.target_table_path = "/tmp/target"
            mock_ctx.global_ctx.staging_table_path = "/tmp/stage"
            mock_ctx.global_ctx.source_system = "SRC"
            mock_ctx.global_ctx.staging_table_schema = {}
            mock_ctx.scd_ctx.scd_type = "scd2"
            mock_ctx.scd_ctx.business_key_column = "id"
            mock_ctx.scd_ctx.deleted_flag_column = "scdDeletedFlag"
            mock_ctx.scd_ctx.source_system_from_source = False
            mock_ctx.scd_ctx.load_type = "Incremental"
            mock_ctx.staging_ctx.stages = [{"nb_name": "NB_Stage_Person"}]
            mock_ctx.hwm_ctx = None
            MockCtxMgr.return_value = mock_ctx

            mock_logger_instance = MagicMock()
            mock_logger_instance.finalize.return_value = ""
            MockLogger.from_logging_context.return_value = mock_logger_instance
            MockLogger.return_value = mock_logger_instance

            result = orch.run()

        assert result.status == "success"
        assert len(result.stages) == 2
        assert {s.stage for s in result.stages} == {"FakeSource", "FakeProcess"}

    def _run_with_swallowed_hwm_writer(self):
        """Run the real `_run_pipeline` with a writer that set 7/3 and left hwm_after None.

        Mirrors what NativeSCDMergeTransformer does when get_high_watermark raises:
        the merge commits, inserted/updated are captured from Delta commit metadata,
        and hwm_after stays None because the read was swallowed. Only
        LoadContextManager, OrchestratorLogger and log_runtime are patched, so the
        writer's `_run` really executes. Returns `(result, writer)`.
        """
        from spark_quick_start.orchestration.transformers.base import (
            ProcessTransformer,
            SourceTransformer,
        )
        from spark_quick_start.orchestration.transformers.state import PipelineState

        class StubSource(SourceTransformer):
            def _produce(self, ctx):
                df = MagicMock()
                df.columns = ["id", "scdDeletedFlag"]
                df.rdd.getNumPartitions.return_value = 2
                return PipelineState(df=df)

        class WriterWithSwallowedHWM(ProcessTransformer):
            def __init__(self) -> None:
                self.inserted: int | None = None
                self.updated: int | None = None
                self.hwm_before: str | None = None
                self.hwm_after: str | None = None
                self.hwm_column: str | None = None

            def _run(self, state, ctx):
                # The merge succeeded and its statistics were read...
                self.inserted, self.updated = 7, 3
                # ...but get_high_watermark raised and native_scd swallowed it,
                # so hwm_after is never assigned.
                return state

        orch = self._make_orch(_ctx_json())
        writer = WriterWithSwallowedHWM()
        orch._transformers = [StubSource(), writer]

        with patch(f"{_LOAD_PIPELINE}.LoadContextManager") as MockCtxMgr, \
             patch("spark_quick_start.orchestration.orchestrator.OrchestratorLogger") as MockLogger, \
             patch("spark_quick_start.orchestration.load_pipeline.log_runtime"):

            mock_ctx = MagicMock()
            mock_ctx.runtime_ctx.correlation_id = "corr-swallowed-hwm"
            mock_ctx.runtime_ctx.capture_row_counts = False
            mock_ctx.global_ctx.entity_name = "Person"
            mock_ctx.global_ctx.target_table_path = "/tmp/target"
            mock_ctx.global_ctx.staging_table_path = "/tmp/stage"
            mock_ctx.global_ctx.source_system = "SRC"
            mock_ctx.global_ctx.staging_table_schema = {}
            mock_ctx.scd_ctx.scd_type = "scd2"
            mock_ctx.scd_ctx.business_key_column = "id"
            mock_ctx.scd_ctx.deleted_flag_column = "scdDeletedFlag"
            mock_ctx.scd_ctx.source_system_from_source = False
            mock_ctx.scd_ctx.load_type = "Incremental"
            mock_ctx.staging_ctx.stages = [{"nb_name": "NB_Stage_Person"}]
            mock_ctx.hwm_ctx = None
            MockCtxMgr.return_value = mock_ctx

            mock_logger_instance = MagicMock()
            mock_logger_instance.finalize.return_value = ""
            MockLogger.from_logging_context.return_value = mock_logger_instance
            MockLogger.return_value = mock_logger_instance

            result = orch.run()

        return result, writer

    def test_swallowed_hwm_harness_runs_the_writer_and_succeeds(self):
        """Control for the strict xfail below: the harness itself is sound.

        An error result also reports inserted_count 0, so the xfail alone cannot tell
        "the harvest defect is still there" from "the harness broke". This sibling is
        not xfailed: the run succeeds and the writer really ran and holds 7/3. With it
        green, the xfail can only be absorbing the harvest defect.
        """
        result, writer = self._run_with_swallowed_hwm_writer()

        assert result.status == "success"
        assert (writer.inserted, writer.updated) == (7, 3)

    def test_ran_writer_with_failed_hwm_read_is_still_harvested(self):
        """A writer that ran must report its counts even if its HWM read failed.

        F117: the harvest takes the provider whose counts are not None, so the proof that a
        writer ran no longer rides on the watermark read. See
        test_native_scd_transformer.py::test_merge_hwm_read_failure_is_swallowed_and_leaves_hwm_after_none
        for the transformer-level half of this, and
        test_swallowed_hwm_harness_runs_the_writer_and_succeeds for the proof that
        the harness works.
        """
        result, _ = self._run_with_swallowed_hwm_writer()

        assert result.metrics["inserted_count"] == 7, (
            "a successful merge reported inserted_count=0 because its watermark "
            "read failed — the counts and the run-proof are independent facts"
        )
        assert result.metrics["updated_count"] == 3

    def test_run_pipeline_injects_ctx_manager_into_transformers_that_declare_it(self):
        """StagingTransformer's _ctx_manager is wired by the pipeline runner, not __init__.

        It is the only injected service that is conditional — set on transformers
        that declare the attribute and skipped for the rest — and it is what
        StagingTransformer calls build_staging_phase_context() on. Nothing
        exercised this path, so a source needing the manager could have been
        handed None by any refactor of the injection loop.
        """
        import types as _types

        from spark_quick_start.orchestration.metrics import StageMetricsCollector
        from spark_quick_start.orchestration.transformers.base import SourceTransformer
        from spark_quick_start.orchestration.transformers.state import PipelineState

        class _NeedsCtxManager(SourceTransformer):
            _ctx_manager = None

            def _produce(self, ctx):
                return PipelineState(df=_types.SimpleNamespace(columns=["id", "scdDeletedFlag"]))

        class _NoCtxManager(SourceTransformer):
            def _produce(self, ctx):  # pragma: no cover — never first in this test
                raise AssertionError

        source = _NeedsCtxManager()
        orch = self._make_orch(_ctx_json())
        orch.add(source)

        ctx = MagicMock()
        ctx.scd_ctx.business_key_column = "id"
        ctx.scd_ctx.deleted_flag_column = "scdDeletedFlag"
        ctx.scd_ctx.source_system_from_source = False
        ctx.runtime_ctx.capture_partitions = False
        ctx.runtime_ctx.capture_row_counts = False
        ctx.runtime_ctx.capture_plans = False

        orch._run_pipeline(ctx, MagicMock(), StageMetricsCollector())

        assert source._ctx_manager is ctx, "the context manager never reached the source"
        # The unconditional services land too.
        assert source._adapter is orch._adapter

    def test_set_source_registers_a_conditional_source_and_chains(self):
        """.set_source() is the public API for a runtime source choice.

        Every existing test built ConditionalSourceTransformer directly, so the
        orchestrator method that is actually documented in the README had no
        coverage at all.
        """
        from spark_quick_start.orchestration.transformers.base import SourceTransformer
        from spark_quick_start.orchestration.transformers.conditional_source import (
            ConditionalSourceTransformer,
        )

        staging = MagicMock(spec=SourceTransformer)
        dqd = MagicMock(spec=SourceTransformer)
        orch = self._make_orch(_ctx_json())

        returned = orch.set_source(lambda ctx: True, staging, dqd, label="source-choice")

        assert returned is orch, ".set_source() must chain"
        wrapper = orch._transformers[0]
        assert isinstance(wrapper, ConditionalSourceTransformer)
        assert wrapper._source_if_true is staging
        assert wrapper._source_if_false is dqd
        assert wrapper._name == "source-choice"


# ============================================================================
# F156 — an SCD step under a Data Vault style is refused at composition
# ============================================================================

def test_an_scd_step_in_a_data_vault_load_is_refused_before_the_source_runs():
    """The refusal comes before staging: the source's produce() must never be called."""
    from spark_quick_start.orchestration.transformers import (
        ConditionalTransformer,
        SCDChangeDetectorTransformer,
        SourceTransformer,
    )

    class _RecordingSource(SourceTransformer):
        produced = False

        def _produce(self, ctx):
            type(self).produced = True
            raise AssertionError("the source ran")

    raw = json.loads(_ctx_json())
    raw["scd_context"] = {"business_key_column": "HashKey", "scd_type": "dv_satellite"}

    def _run(*steps):
        orch = LoadOrchestrator(json.dumps(raw), MagicMock(), adapter=MockPlatformAdapter())
        for step in steps:
            orch.add(step)
        # log_runtime serialises the session's settings, which a MagicMock session cannot give.
        with patch("spark_quick_start.orchestration.load_pipeline.log_runtime"):
            return orch.run()

    result = _run(_RecordingSource(), SCDChangeDetectorTransformer())

    assert result.status == "error"
    assert "SCDChangeDetectorTransformer" in result.error_details["error_message"]
    assert "Data Vault" in result.error_details["error_message"]
    assert not _RecordingSource.produced

    # Inside .add_if() the step may never run, so composition leaves it to its own check.
    _RecordingSource.produced = False
    skipped = _run(_RecordingSource(), ConditionalTransformer(False, SCDChangeDetectorTransformer()))
    assert _RecordingSource.produced, "a step in a disabled branch must not block the load"
    assert "the source ran" in skipped.error_details["error_message"]
