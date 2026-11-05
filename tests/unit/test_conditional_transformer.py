"""Tests for ConditionalTransformer and LoadOrchestrator.add_if."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from _frames import only_partition_probe

from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter
from spark_quick_start.orchestration.models import (
    GlobalContext,
    LoggingContext,
    RuntimeContext,
    SCDContext,
    StagingContext,
)
from spark_quick_start.orchestration.transformers.base import LoadContext, ProcessTransformer
from spark_quick_start.orchestration.transformers.conditional import ConditionalTransformer
from spark_quick_start.orchestration.transformers.state import PipelineState


def _ctx(load_type: str = "Full") -> LoadContext:
    return LoadContext(
        global_ctx=GlobalContext({
            "id": "ent_1",
            "entity_name": "Customer", "source_system": "SYS",
            "target_table_path": "/tmp/t", "staging_table_path": "/tmp/s",
        }),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext({"business_key_column": "id", "scd_type": "scd2", "load_type": load_type}),
        runtime_ctx=RuntimeContext({"correlation_id": "c", "pipeline_name": "p", "application_name": "a"}),
        logging_ctx=LoggingContext({}),
    )


def _state() -> PipelineState:
    return PipelineState(df=MagicMock(), frames={})


def _inner() -> ProcessTransformer:
    inner = MagicMock(spec=ProcessTransformer)
    inner._spark = None
    inner._adapter = None
    inner._logger = None
    inner._metrics_collector = None
    # ConditionalTransformer delegates the full inner lifecycle via inner(...),
    # so the mock is invoked directly. Aliasing _run to the mock itself keeps
    # inner.return_value / inner.assert_called_once_with(...) working whichever
    # entry point is used, in every test below.
    inner._run = inner
    return inner


# ── predicate=True ────────────────────────────────────────────────────────────

def test_predicate_true_calls_inner():
    inner = _inner()
    state = _state()
    new_state = _state()
    inner.return_value = new_state
    ctx = _ctx()

    ct = ConditionalTransformer(predicate=lambda _: True, transformer=inner)
    ct._spark = ct._adapter = ct._logger = None
    result = ct(state, ctx)

    inner.assert_called_once_with(state, ctx)
    assert result is new_state


def test_bool_true_predicate_calls_inner():
    inner = _inner()
    state = _state()
    inner.return_value = _state()

    ct = ConditionalTransformer(predicate=True, transformer=inner)
    ct._spark = ct._adapter = ct._logger = None
    ct(state, _ctx())

    inner.assert_called_once()


# ── predicate=False ───────────────────────────────────────────────────────────

def test_predicate_false_returns_passthrough():
    inner = _inner()
    state = _state()

    ct = ConditionalTransformer(predicate=lambda _: False, transformer=inner)
    ct._spark = ct._adapter = ct._logger = None
    result = ct(state, _ctx())

    inner.assert_not_called()
    assert result is state


def test_bool_false_predicate_skips_inner():
    inner = _inner()
    state = _state()

    ct = ConditionalTransformer(predicate=False, transformer=inner)
    ct._spark = ct._adapter = ct._logger = None
    ct(state, _ctx())

    inner.assert_not_called()


def test_predicate_false_emits_skip_log():
    inner = _inner()
    logger = MagicMock()

    ct = ConditionalTransformer(predicate=False, transformer=inner, label="my-step")
    ct._spark = ct._adapter = None
    ct._logger = logger
    ct(_state(), _ctx())

    logger.debug.assert_called_once()
    assert "my-step" in logger.debug.call_args[0][0]
    assert "False" in logger.debug.call_args[0][0]


def test_predicate_false_no_error_when_logger_none():
    inner = _inner()

    ct = ConditionalTransformer(predicate=False, transformer=inner)
    ct._spark = ct._adapter = ct._logger = None
    ct(_state(), _ctx())  # must not raise


# ── LoadContext-based predicate ───────────────────────────────────────────────

def test_predicate_receives_load_context():
    received = []
    inner = _inner()
    inner.return_value = _state()

    def pred(ctx: LoadContext) -> bool:
        received.append(ctx)
        return True

    ct = ConditionalTransformer(predicate=pred, transformer=inner)
    ct._spark = ct._adapter = ct._logger = None
    ctx = _ctx(load_type="Incremental")
    ct(_state(), ctx)

    assert len(received) == 1
    assert received[0] is ctx


def test_predicate_skips_on_full_load_type():
    inner = _inner()
    state = _state()

    ct = ConditionalTransformer(
        predicate=lambda ctx: ctx.scd_ctx.load_type == "Incremental",
        transformer=inner,
    )
    ct._spark = ct._adapter = ct._logger = None
    result = ct(state, _ctx(load_type="Full"))

    inner.assert_not_called()
    assert result is state


def test_predicate_runs_on_incremental_load_type():
    inner = _inner()
    inner.return_value = _state()

    ct = ConditionalTransformer(
        predicate=lambda ctx: ctx.scd_ctx.load_type == "Incremental",
        transformer=inner,
    )
    ct._spark = ct._adapter = ct._logger = None
    ct(_state(), _ctx(load_type="Incremental"))

    inner.assert_called_once()


# ── services forwarded ────────────────────────────────────────────────────────

def test_services_forwarded_to_inner_when_predicate_true():
    inner = _inner()
    inner.return_value = _state()
    spark = MagicMock()
    adapter = MockPlatformAdapter()
    logger = MagicMock()

    ct = ConditionalTransformer(predicate=True, transformer=inner)
    ct._spark = spark
    ct._adapter = adapter
    ct._logger = logger
    ct(_state(), _ctx())

    assert inner._spark is spark
    assert inner._adapter is adapter
    assert inner._logger is logger


def test_services_forwarded_to_inner_even_when_predicate_false():
    inner = _inner()
    spark = MagicMock()

    ct = ConditionalTransformer(predicate=False, transformer=inner)
    ct._spark = spark
    ct._adapter = ct._logger = None
    ct(_state(), _ctx())

    assert inner._spark is spark


def test_metrics_collector_forwarded_to_inner_when_predicate_true():
    from spark_quick_start.orchestration.metrics import StageMetricsCollector

    inner = _inner()
    inner.return_value = _state()
    collector = StageMetricsCollector()

    ct = ConditionalTransformer(predicate=True, transformer=inner)
    ct._spark = ct._adapter = ct._logger = None
    ct._metrics_collector = collector
    ct(_state(), _ctx())

    assert inner._metrics_collector is collector


# ── error cases ───────────────────────────────────────────────────────────────

def test_predicate_raises_wraps_as_runtime_error():
    inner = _inner()

    def bad_pred(ctx):
        raise ValueError("boom")

    ct = ConditionalTransformer(predicate=bad_pred, transformer=inner)
    ct._spark = ct._adapter = ct._logger = None

    with pytest.raises(RuntimeError, match="boom"):
        ct(_state(), _ctx())


def test_predicate_raises_includes_inner_name_in_message():
    class MyTransformer(ProcessTransformer):
        def _run(self, state, ctx): ...

    ct = ConditionalTransformer(predicate=lambda _: (_ for _ in ()).throw(ValueError("oops")), transformer=MyTransformer())
    ct._spark = ct._adapter = ct._logger = None

    with pytest.raises(RuntimeError) as exc_info:
        ct(_state(), _ctx())
    assert "MyTransformer" in str(exc_info.value)


def test_wrong_predicate_type_raises_type_error():
    with pytest.raises(TypeError, match="predicate must be bool or Callable"):
        ConditionalTransformer(predicate="yes", transformer=_inner())


# ── label and __repr__ ────────────────────────────────────────────────────────

def test_repr_with_label():
    ct = ConditionalTransformer(predicate=True, transformer=_inner(), label="my-label")
    assert repr(ct) == "ConditionalTransformer(my-label)"


def test_repr_without_label_uses_inner_class_name():
    class FancyTransformer(ProcessTransformer):
        def _run(self, state, ctx): ...

    ct = ConditionalTransformer(predicate=True, transformer=FancyTransformer())
    assert repr(ct) == "ConditionalTransformer(FancyTransformer)"


def test_label_used_in_skip_log_not_class_name():
    class FancyTransformer(ProcessTransformer):
        def _run(self, state, ctx): ...

    logger = MagicMock()
    ct = ConditionalTransformer(predicate=False, transformer=FancyTransformer(), label="my-step")
    ct._spark = ct._adapter = None
    ct._logger = logger
    ct(_state(), _ctx())

    msg = logger.debug.call_args[0][0]
    assert "my-step" in msg
    assert "FancyTransformer" not in msg


# ── add_if on LoadOrchestrator ────────────────────────────────────────────────

def test_add_if_produces_conditional_transformer():
    from spark_quick_start.orchestration.orchestrator import LoadOrchestrator
    orch = LoadOrchestrator.__new__(LoadOrchestrator)
    orch._transformers = []

    inner = _inner()
    orch.add_if(True, inner, label="test-step")

    assert len(orch._transformers) == 1
    ct = orch._transformers[0]
    assert isinstance(ct, ConditionalTransformer)
    assert ct._label == "test-step"
    assert ct._transformer is inner


def test_add_if_without_label():
    from spark_quick_start.orchestration.orchestrator import LoadOrchestrator
    orch = LoadOrchestrator.__new__(LoadOrchestrator)
    orch._transformers = []

    inner = _inner()
    orch.add_if(False, inner)

    ct = orch._transformers[0]
    assert isinstance(ct, ConditionalTransformer)
    assert ct._label is None


# ── _unwrap for SCD metric harvesting ────────────────────────────────────────

def test_unwrap_finds_wrapped_scd_transformer():
    from spark_quick_start.orchestration.transformers.conditional import ConditionalTransformer
    from spark_quick_start.orchestration.transformers.native_scd import NativeSCDMergeTransformer

    scd = NativeSCDMergeTransformer()
    wrapper = ConditionalTransformer(predicate=True, transformer=scd)

    def _unwrap(t):
        return t._transformer if isinstance(t, ConditionalTransformer) else t

    assert _unwrap(wrapper) is scd
    assert _unwrap(scd) is scd


def test_unwrap_non_conditional_is_identity():
    from spark_quick_start.orchestration.transformers.conditional import ConditionalTransformer
    from spark_quick_start.orchestration.transformers.native_scd import NativeSCDMergeTransformer

    scd = NativeSCDMergeTransformer()

    def _unwrap(t):
        return t._transformer if isinstance(t, ConditionalTransformer) else t

    assert _unwrap(scd) is scd


# ── Regression: no double before_run/after_run on the inner transformer ──────

def test_predicate_true_does_not_double_run_or_double_record_metrics():
    """The inner transformer's _run() must execute exactly once, and exactly
    one StageMetrics entry must be recorded — not one for ConditionalTransformer
    plus a second one from the inner transformer's own before_run/after_run.
    """
    from spark_quick_start.orchestration.metrics import StageMetricsCollector

    run_calls = []
    frame, out = MagicMock(), MagicMock()

    class RealInner(ProcessTransformer):
        def _run(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
            run_calls.append(1)
            return state.with_df(out)

    inner = RealInner()
    ct = ConditionalTransformer(predicate=True, transformer=inner)
    ct._spark = ct._adapter = ct._logger = None
    collector = StageMetricsCollector()
    ct._metrics_collector = collector

    ct(PipelineState(df=frame, frames={}), _ctx())

    assert run_calls == [1], "inner._run() must be called exactly once"
    assert len(collector.stages) == 1, "exactly one StageMetrics entry, not two"
    only_partition_probe(frame)
    only_partition_probe(out)


def test_predicate_false_uses_real_logger_debug_method():
    """Regression guard: the skip-log call must use a method that actually
    exists on OrchestratorLogger — a MagicMock would silently accept `.log(...)`
    even though the real class only has debug/info/warning/error."""
    from spark_quick_start.logger import OrchestratorLogger

    inner = _inner()
    ct = ConditionalTransformer(predicate=False, transformer=inner)
    ct._spark = ct._adapter = None
    ct._logger = OrchestratorLogger()  # real object, not a mock

    ct(_state(), _ctx())  # must not raise AttributeError


# ── Regression: the inner transformer's own lifecycle hooks must still fire ───

class _SCDLikeInner(ProcessTransformer):
    """Stands in for NativeSCDMergeTransformer: attaches extra metrics in after_run.

    NativeSCDMergeTransformer / NativeSCDAppendOnlyTransformer
    both override after_run purely to add
    inserted/updated to their StageMetrics entry, which is the contribution a
    wrapper must not swallow.
    """

    inserted = 7
    updated = 3

    def _run(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
        return state

    def after_run(self, state: PipelineState, ctx: LoadContext) -> None:  # type: ignore[override]
        super().after_run(
            state, ctx, extra_metrics={"inserted": self.inserted, "updated": self.updated}
        )


def _collector_for(transformer):
    from spark_quick_start.orchestration.metrics import StageMetricsCollector

    collector = StageMetricsCollector()
    transformer._spark = transformer._adapter = transformer._logger = None
    transformer._metrics_collector = collector
    return collector


def test_wrapped_inner_after_run_override_still_contributes_extra_metrics():
    """A wrapped transformer must record the same StageMetrics as an unwrapped one.

    Regression: the wrapper used to call inner._run() directly, so an inner
    after_run override never fired and .add_if(pred, NativeSCDMergeTransformer())
    silently lost inserted/updated from ExecutionResult.stages.
    """
    bare = _SCDLikeInner()
    bare_stages = _collector_for(bare)
    bare_frame = MagicMock()
    bare(PipelineState(df=bare_frame, frames={}), _ctx())

    wrapped = ConditionalTransformer(predicate=True, transformer=_SCDLikeInner())
    wrapped_stages = _collector_for(wrapped)
    wrapped_frame = MagicMock()
    wrapped(PipelineState(df=wrapped_frame, frames={}), _ctx())

    assert len(wrapped_stages.stages) == 1, "still exactly one entry, not two"
    got = wrapped_stages.stages[0]
    expected = bare_stages.stages[0]
    assert got.stage == expected.stage == "_SCDLikeInner"
    assert (got.inserted, got.updated) == (7, 3)
    assert (got.inserted, got.updated) == (expected.inserted, expected.updated)
    only_partition_probe(bare_frame)
    only_partition_probe(wrapped_frame)


def test_skipped_conditional_records_no_stage_metrics():
    """A skipped step emits the DEBUG skip log but no StageMetrics entry.

    The wrapper's own hooks previously left a zero-duration entry carrying the
    inner's name, which read as though the step had run.
    """
    ct = ConditionalTransformer(predicate=False, transformer=_SCDLikeInner())
    stages = _collector_for(ct)

    ct(_state(), _ctx())

    assert stages.stages == []


# ── Regression: wrapping must not downgrade AlwaysRunTransformer semantics ────

def test_always_run_inner_still_runs_on_halted_pipeline():
    """AlwaysRunTransformer exists to run on a halted pipeline; .add_if() must
    not turn one into a skip-on-halt step."""
    from spark_quick_start.orchestration.transformers.base import AlwaysRunTransformer

    calls = []

    class _Reporter(AlwaysRunTransformer):
        def _run(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
            calls.append(1)
            return state

    ct = ConditionalTransformer(predicate=True, transformer=_Reporter())
    ct._spark = ct._adapter = ct._logger = ct._metrics_collector = None

    frame = MagicMock()
    ct(PipelineState(df=frame, frames={}).with_halted("early exit"), _ctx())

    assert calls == [1]
    only_partition_probe(frame)


def test_plain_inner_still_skipped_on_halted_pipeline():
    """The halted short-circuit is preserved for ordinary ProcessTransformers."""
    calls = []

    class _Plain(ProcessTransformer):
        def _run(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
            calls.append(1)
            return state

    ct = ConditionalTransformer(predicate=True, transformer=_Plain())
    ct._spark = ct._adapter = ct._logger = ct._metrics_collector = None

    halted = _state().with_halted("early exit")
    result = ct(halted, _ctx())

    assert calls == []
    assert result is halted


def test_predicate_not_evaluated_on_halted_pipeline():
    """A halted pipeline must not run the predicate — it may touch state the
    early exit means is unavailable."""
    def _boom(_ctx_):
        raise AssertionError("predicate must not be evaluated on a halted pipeline")

    ct = ConditionalTransformer(predicate=_boom, transformer=_SCDLikeInner())
    ct._spark = ct._adapter = ct._logger = ct._metrics_collector = None

    ct(_state().with_halted("early exit"), _ctx())  # must not raise
