"""
Tests for WI-06: transformer pipeline wired into LoadOrchestrator.run().

These tests verify the branching logic in run() without a live Spark session. The two
harvest tests replace `_run_pipeline` on purpose: run()'s harvest over preset attributes is
the unit under test. Unpatched pipeline behaviour is covered by
`test_run_populates_stages_with_real_pipeline` and tests/integration/local/.
"""
from __future__ import annotations

import contextlib
from unittest.mock import MagicMock, patch

import pytest

from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter
from spark_quick_start.orchestration.metrics import StageMetricsCollector
from spark_quick_start.orchestration.orchestrator import LoadOrchestrator
from spark_quick_start.orchestration.transformers.base import (
    ProcessTransformer,
    SourceTransformer,
)
from spark_quick_start.orchestration.transformers.conditional import ConditionalTransformer
from spark_quick_start.orchestration.transformers.native_scd import (
    NativeSCDAppendOnlyTransformer,
    NativeSCDMergeTransformer,
)
from spark_quick_start.orchestration.transformers.state import PipelineState


def _make_orchestrator() -> LoadOrchestrator:
    """Build a bare LoadOrchestrator bypassing __init__."""
    orch = LoadOrchestrator.__new__(LoadOrchestrator)
    orch._spark = MagicMock()
    orch._adapter = MockPlatformAdapter()
    orch._load_context_json = "{}"
    orch._data_storage_path = ""
    orch._data_eng_path = ""
    orch._injected_logger = MagicMock()
    orch._injected_logger.finalize.return_value = "{}"
    orch._transformers = []
    return orch


def _make_ctx() -> MagicMock:
    ctx = MagicMock()
    ctx.runtime_ctx.correlation_id = "corr-1"
    ctx.global_ctx.entity_name = "TestEntity"
    ctx.logging_ctx = MagicMock()
    ctx.global_ctx.staging_ctx = MagicMock()
    ctx.global_ctx.scd_ctx = MagicMock()
    ctx.global_ctx.runtime_ctx = MagicMock()
    ctx.global_ctx.logging_ctx = MagicMock()
    ctx.staging_ctx = MagicMock()
    ctx.scd_ctx = MagicMock()
    return ctx


def test_run_returns_error_when_no_transformers():
    """run() with no transformers should return an error ExecutionResult."""
    orch = _make_orchestrator()
    orch._transformers = []

    fake_ctx = _make_ctx()

    with (
        patch('spark_quick_start.orchestration.orchestrator.LoadContextManager') as mock_cm,
        patch('spark_quick_start.orchestration.orchestrator.OrchestratorLogger'),
        patch('spark_quick_start.orchestration.load_pipeline.log_runtime'),
    ):
        mock_cm.return_value = fake_ctx
        fake_ctx.parse = MagicMock()
        fake_ctx.validate_all = MagicMock()

        result = orch.run()

    assert result.status == "error"
    assert "transformers" in result.error_details.get("error_message", "").lower()


def test_run_harvests_scd_metrics():
    """run() harvests SCD metrics from preset attributes; `_run_pipeline` is replaced on
    purpose (the harvest in run() is the unit under test, not the pipeline)."""
    orch = _make_orchestrator()

    scd_tx = NativeSCDMergeTransformer()
    scd_tx.inserted = 5
    scd_tx.updated = 3
    scd_tx.hwm_before = "2024-01"
    scd_tx.hwm_after = "2024-02"
    scd_tx.hwm_column = "scdEffectiveFrom"
    orch._transformers = [MagicMock(spec=SourceTransformer), scd_tx]

    fake_ctx = _make_ctx()
    fake_state = PipelineState(df=MagicMock())

    with (
        patch.object(LoadOrchestrator, '_run_pipeline', return_value=fake_state),
        patch('spark_quick_start.orchestration.orchestrator.LoadContextManager') as mock_cm,
        patch('spark_quick_start.orchestration.orchestrator.OrchestratorLogger'),
        patch('spark_quick_start.orchestration.load_pipeline.log_runtime'),
    ):
        mock_cm.return_value = fake_ctx
        fake_ctx.parse = MagicMock()
        fake_ctx.validate_all = MagicMock()

        result = orch.run()

    assert result.metrics['inserted_count'] == 5
    assert result.metrics['updated_count'] == 3
    assert result.metrics['hwm_value_before'] == "2024-01"
    assert result.metrics['hwm_value_after'] == "2024-02"


def test_run_harvests_append_only_metrics():
    """NativeSCDAppendOnlyTransformer.inserted must be harvested into metrics too —
    it has no 'updated' concept, so updated_count stays 0.

    `_run_pipeline` is replaced on purpose: run() harvests, not `_run_pipeline`."""
    orch = _make_orchestrator()

    append_only_tx = NativeSCDAppendOnlyTransformer()
    append_only_tx.inserted = 7
    append_only_tx.updated = 0  # what its _run sets; `_run_pipeline` is replaced here
    append_only_tx.hwm_before = "2024-01"
    append_only_tx.hwm_after = "2024-02"
    append_only_tx.hwm_column = "scdEffectiveFrom"
    orch._transformers = [MagicMock(spec=SourceTransformer), append_only_tx]

    fake_ctx = _make_ctx()
    fake_state = PipelineState(df=MagicMock())

    with (
        patch.object(LoadOrchestrator, '_run_pipeline', return_value=fake_state),
        patch('spark_quick_start.orchestration.orchestrator.LoadContextManager') as mock_cm,
        patch('spark_quick_start.orchestration.orchestrator.OrchestratorLogger'),
        patch('spark_quick_start.orchestration.load_pipeline.log_runtime'),
    ):
        mock_cm.return_value = fake_ctx
        fake_ctx.parse = MagicMock()
        fake_ctx.validate_all = MagicMock()

        result = orch.run()

    assert result.metrics['inserted_count'] == 7
    assert result.metrics['updated_count'] == 0
    assert result.metrics['hwm_value_before'] == "2024-01"
    assert result.metrics['hwm_value_after'] == "2024-02"


# ── _run_pipeline unit tests ─────────────────────────────────────────────────

def _make_load_ctx() -> MagicMock:
    """Build a minimal LoadContextManager mock for _run_pipeline."""
    ctx = MagicMock()
    ctx.global_ctx = MagicMock()
    ctx.staging_ctx = MagicMock()
    ctx.scd_ctx = MagicMock()
    ctx.runtime_ctx = MagicMock()
    ctx.logging_ctx = MagicMock()
    return ctx


def test_run_pipeline_returns_pipeline_state():
    """_run_pipeline should return a PipelineState, not a raw DataFrame."""
    orch = _make_orchestrator()

    fake_df = MagicMock()
    expected_state = PipelineState(df=fake_df)

    source = MagicMock(spec=SourceTransformer)
    source.produce.return_value = expected_state
    orch._transformers = [source]

    ctx = _make_load_ctx()
    logger = MagicMock()

    result = orch._run_pipeline(ctx, logger, StageMetricsCollector())

    assert isinstance(result, PipelineState)
    assert result is expected_state
    assert not fake_df.method_calls


def test_run_pipeline_plain_callable_shim():
    """A plain (df, ctx) -> df callable should be accepted and shimmed via with_df()."""
    orch = _make_orchestrator()

    fake_df = MagicMock()
    transformed_df = MagicMock()
    initial_state = PipelineState(df=fake_df)

    source = MagicMock(spec=SourceTransformer)
    source.produce.return_value = initial_state

    shim_callable = MagicMock(return_value=transformed_df)

    orch._transformers = [source, shim_callable]

    ctx = _make_load_ctx()
    logger = MagicMock()

    result = orch._run_pipeline(ctx, logger, StageMetricsCollector())

    shim_callable.assert_called_once()
    call_args = shim_callable.call_args
    assert call_args[0][0] is fake_df  # first arg is the raw df
    assert isinstance(result, PipelineState)
    assert result.df is transformed_df
    assert not fake_df.method_calls
    assert not transformed_df.method_calls


def test_run_pipeline_process_transformer_called_with_state():
    """A ProcessTransformer should receive PipelineState, not a raw DataFrame."""
    orch = _make_orchestrator()

    fake_df = MagicMock()
    initial_state = PipelineState(df=fake_df)
    output_state = PipelineState(df=MagicMock())

    source = MagicMock(spec=SourceTransformer)
    source.produce.return_value = initial_state

    process = MagicMock(spec=ProcessTransformer)
    process.return_value = output_state

    orch._transformers = [source, process]

    ctx = _make_load_ctx()
    logger = MagicMock()

    result = orch._run_pipeline(ctx, logger, StageMetricsCollector())

    process.assert_called_once()
    call_args = process.call_args
    assert isinstance(call_args[0][0], PipelineState)
    assert result is output_state
    assert not fake_df.method_calls


def test_run_pipeline_raises_when_no_transformers():
    orch = _make_orchestrator()
    orch._transformers = []
    ctx = _make_load_ctx()
    with pytest.raises(ValueError, match="empty transformer list"):
        orch._run_pipeline(ctx, MagicMock(), StageMetricsCollector())


def test_run_pipeline_raises_when_first_is_not_source():
    orch = _make_orchestrator()
    orch._transformers = [MagicMock(spec=ProcessTransformer)]
    ctx = _make_load_ctx()
    with pytest.raises(ValueError, match="SourceTransformer"):
        orch._run_pipeline(ctx, MagicMock(), StageMetricsCollector())


def test_run_pipeline_raises_when_source_at_non_zero_position():
    orch = _make_orchestrator()
    orch._transformers = [MagicMock(spec=SourceTransformer), MagicMock(spec=SourceTransformer)]
    ctx = _make_load_ctx()
    with pytest.raises(ValueError):
        orch._run_pipeline(ctx, MagicMock(), StageMetricsCollector())


def test_run_pipeline_raises_on_exploding_transformer():
    """A transformer that raises halts the pipeline with the original exception."""
    class _ExplodingProcess(ProcessTransformer):
        def _run(self, state, ctx):
            raise RuntimeError("kaboom")

    import json

    from spark_quick_start.orchestration.context_manager import LoadContextManager
    ctx = LoadContextManager(json.dumps({
        "global_context": {
            "id": "ent_1",
            "entity_name": "E", "source_system": "S",
            "target_table_path": "/t", "staging_table_path": "/s",
        },
        "staging_context": {},
        "scd_context": {"business_key_column": "id", "effective_from_column": "ef", "scd_type": "scd1"},
        "runtime_context": {},
        "logging_context": {},
    }))
    ctx.parse("", "")

    source = MagicMock(spec=SourceTransformer)
    source.produce.return_value = PipelineState(df=MagicMock())

    orch = _make_orchestrator()
    orch._transformers = [source, _ExplodingProcess()]

    from spark_quick_start.logger import OrchestratorLogger
    with pytest.raises(RuntimeError, match="kaboom"):
        orch._run_pipeline(ctx, OrchestratorLogger(), StageMetricsCollector())


def test_the_quality_step_halts_the_chain_on_zero_candidates(spark):
    """F29 — default_pipeline gained a step that can halt, and that changes the outcome.

    `CandidateGuardTransformer` halts when there are no candidates. F29 composed it into
    `default_pipeline`, so a no-change load now reaches a terminal state it never reached
    before: `ProcessTransformer.__call__` short-circuits every later step, the merger
    never runs and never sets `hwm_after`, and the halt surfaces on `ExecutionResult` as
    `early_exit` / `early_exit_reason` with the merge stage absent from `stages`.

    Asserted at `_run_pipeline`, which is where the halt is produced and propagated; the
    state-to-ExecutionResult mapping is already covered in test_load_orchestrator.py. The
    point here is that the guard at this position halts and the writer after it does not
    run — the consequence the review of F29's first attempt found unstated in the diff,
    the comments and the tests.

    Read as a warning, not just a test: that JSON goes back to pipelines outside this
    repo, and a run that used to report a successful no-op merge now reports an early
    exit.
    """
    from spark_quick_start.orchestration.transformers.candidate_guard import (
        CandidateGuardTransformer,
    )

    empty = spark.createDataFrame(
        [], "id string, scdSourceSystem string, scdDeletedFlag string, _change_type string",
    )

    class _EmptySource(SourceTransformer):
        """A real source, not a mock — the point is to exercise the real chain."""

        def _produce(self, ctx):
            return PipelineState(df=empty, frames={})

    class _NeverRuns(NativeSCDMergeTransformer):
        ran = False

        def _run(self, state, ctx):  # pragma: no cover — asserted not to be reached
            type(self).ran = True
            return state

    ctx = _make_ctx()
    # Real column names, because the source template validates required SCD columns and
    # the guard groups by two of them — a MagicMock name fails both.
    ctx.scd_ctx.business_key_column = "id"
    ctx.scd_ctx.source_system_column = "scdSourceSystem"
    ctx.scd_ctx.deleted_flag_column = "scdDeletedFlag"
    ctx.scd_ctx.source_system_from_source = False
    ctx.runtime_ctx.serverless = True

    orch = _make_orchestrator()
    writer = _NeverRuns()
    orch._transformers = [_EmptySource(), CandidateGuardTransformer(), writer]

    collector = StageMetricsCollector()
    state = orch._run_pipeline(ctx, MagicMock(), collector)

    assert state.halted is True
    assert "no candidates" in (state.halt_reason or "")
    assert _NeverRuns.ran is False, "the writer must not run after the guard halts"

    # The stage's absence, not hwm_after. A first version asserted
    # `writer.hwm_after is None`, which cannot fail — _NeverRuns never assigns it, so it is
    # None whether or not the writer ran. This is the consequence that is both real and
    # cheap to check: the short-circuit returns before before_run/after_run, so nothing is
    # recorded for the writer at all.
    recorded = [getattr(s, "stage", getattr(s, "name", "")) for s in collector.stages]
    assert not any("NativeSCD" in str(n) for n in recorded), (
        f"a halted run must record no writer stage; got {recorded}"
    )


# ── F234 / F232: what run() reports when the writer did not write ─────────────

class _Writer(ProcessTransformer):
    """A writer of our own shape: counts and watermark None until `_run` sets them (F117)."""

    def __init__(self):
        self.inserted = None
        self.updated = None
        self.hwm_before = None
        self.hwm_after = None
        self.hwm_column = "scdEffectiveFrom"

    def _run(self, state, ctx):
        self.inserted, self.updated = 5, 3
        self.hwm_before, self.hwm_after = "2024-01", "2024-02"
        return state


def _run_once(orch, fake_state=None):
    """Drive run(); `_run_pipeline` is real unless a state is given to replace it with."""
    fake_ctx = _make_ctx()
    fake_ctx.hwm_ctx = None
    patches = [
        patch('spark_quick_start.orchestration.orchestrator.LoadContextManager',
              return_value=fake_ctx),
        patch('spark_quick_start.orchestration.orchestrator.OrchestratorLogger'),
        patch('spark_quick_start.orchestration.load_pipeline.log_runtime'),
    ]
    if fake_state is not None:
        patches.append(patch.object(LoadOrchestrator, '_run_pipeline', return_value=fake_state))
    with contextlib.ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        return orch.run()


def test_a_second_run_does_not_report_the_first_runs_writer_counts():
    """F234: the same orchestrator, run twice; the writer runs the first time only.

    The provider attributes live on the step instances, which a second `run()` reuses, so
    without a reset the second run harvests the first run's counts and watermark from a writer
    it skipped, and `inserted is not None` (F117's proof that a writer ran) lies.
    """
    source = MagicMock(spec=SourceTransformer)
    source.produce.return_value = PipelineState(df=MagicMock())
    write = {"now": True}

    orch = _make_orchestrator()
    orch._adapter = MagicMock()
    orch._adapter.delta_table_exists.return_value = False
    orch._transformers = [source, ConditionalTransformer(lambda ctx: write["now"], _Writer())]

    first = _run_once(orch)
    assert first.status == "success", first.error_details
    assert (first.metrics["inserted_count"], first.metrics["updated_count"]) == (5, 3)

    write["now"] = False
    second = _run_once(orch)
    assert second.status == "success", second.error_details
    assert (second.metrics["inserted_count"], second.metrics["updated_count"]) == (0, 0)
    assert second.metrics["hwm_value_after"] != "2024-02"


class _WatermarkSource(SourceTransformer):
    """A source that reports a watermark, as a source owning `hwm_context`'s column does."""

    def __init__(self):
        self.hwm_column = "modified_at"
        self.hwm_before = "B"  # the watermark the run was given
        self.hwm_after = "T"   # the latest of the rows it read

    def _produce(self, ctx):  # pragma: no cover — `_run_pipeline` is replaced in these tests
        raise AssertionError("not reached")


def _ran(writer):
    writer.inserted, writer.updated = 2, 0
    return writer


@pytest.mark.parametrize(("halted", "writer", "expected"), [
    pytest.param(True, _Writer(), "B", id="halt-refuses-the-batch"),
    pytest.param(True, _ran(_Writer()), "B", id="halt-after-a-write"),
    pytest.param(False, _Writer(), "B", id="registered-writer-skipped"),
    pytest.param(False, _ran(_Writer()), "T", id="writer-ran"),
    pytest.param(False, None, "T", id="no-writer-the-library-can-see"),
])
def test_the_watermark_moves_only_when_the_batch_reached_the_target(halted, writer, expected):
    """F232: a caller stores `hwm_value_after` and reads from it next run, so it may move past
    the batch only when a writer wrote it. A halt, which any step may impose, holds it like an
    error, and so does a registered writer that did not run (a false `.add_if()`). A writer the
    library cannot see, one that is no `SCDMetricsProvider`, keeps today's behaviour."""
    orch = _make_orchestrator()
    orch._transformers = [_WatermarkSource()] + ([] if writer is None else [writer])
    state = PipelineState(df=MagicMock())
    if halted:
        state = state.with_halted("only 2 rows staged, expected at least 3")

    result = _run_once(orch, fake_state=state)

    assert result.status == "success", result.error_details
    assert result.early_exit is halted
    assert result.metrics["hwm_value_before"] == "B"
    assert result.metrics["hwm_value_after"] == expected
