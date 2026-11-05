"""Tests for ConditionalSourceTransformer service forwarding."""
from __future__ import annotations

from unittest.mock import MagicMock

from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter
from spark_quick_start.orchestration.metrics import StageMetricsCollector
from spark_quick_start.orchestration.models import (
    GlobalContext,
    LoggingContext,
    RuntimeContext,
    SCDContext,
    StagingContext,
)
from spark_quick_start.orchestration.transformers.base import LoadContext, SourceTransformer
from spark_quick_start.orchestration.transformers.conditional_source import (
    ConditionalSourceTransformer,
)


def _ctx() -> LoadContext:
    return LoadContext(
        global_ctx=GlobalContext({
            "id": "ent_1",
            "entity_name": "Customer", "source_system": "SYS",
            "target_table_path": "/tmp/t", "staging_table_path": "/tmp/s",
        }),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext({"business_key_column": "id"}),
        runtime_ctx=RuntimeContext({"correlation_id": "c", "pipeline_name": "p", "application_name": "a"}),
        logging_ctx=LoggingContext({}),
    )


def _source() -> SourceTransformer:
    source = MagicMock(spec=SourceTransformer)
    source._spark = None
    source._adapter = None
    source._logger = None
    return source


def test_services_forwarded_to_selected_source_when_predicate_true():
    true_source = _source()
    false_source = _source()
    spark = MagicMock()
    adapter = MockPlatformAdapter()
    logger = MagicMock()

    cst = ConditionalSourceTransformer(predicate=True, source_if_true=true_source, source_if_false=false_source)
    cst._spark = spark
    cst._adapter = adapter
    cst._logger = logger
    cst._produce(_ctx())

    assert true_source._spark is spark
    assert true_source._adapter is adapter
    assert true_source._logger is logger


def test_metrics_collector_forwarded_to_selected_source():
    true_source = _source()
    false_source = _source()
    collector = StageMetricsCollector()

    cst = ConditionalSourceTransformer(predicate=True, source_if_true=true_source, source_if_false=false_source)
    cst._spark = cst._adapter = cst._logger = None
    cst._metrics_collector = collector
    cst._produce(_ctx())

    assert true_source._metrics_collector is collector


# ── Regression: the selected source's own lifecycle hooks must fire ───────────

class _HookedSource(SourceTransformer):
    """A source that contributes extra metrics from its own after_run override."""

    def __init__(self, inserted: int) -> None:
        self.inserted = inserted

    def _produce(self, ctx: LoadContext):
        from spark_quick_start.orchestration.transformers.state import PipelineState

        df = MagicMock()
        # _validate_source_columns needs the required SCD columns present
        df.columns = ["id", "scdDeletedFlag", "payload"]
        return PipelineState(df=df)

    def after_run(self, ctx: LoadContext, df=None, *, extra_metrics=None) -> None:
        super().after_run(ctx, df=df, extra_metrics={"inserted": self.inserted})


def _wire(transformer, collector):
    transformer._spark = transformer._adapter = transformer._logger = None
    transformer._metrics_collector = collector
    return transformer


def test_selected_source_after_run_override_contributes_extra_metrics():
    """produce() must delegate the whole lifecycle to the selected source.

    Regression: the wrapper used to call source._produce() directly, so the
    selected source's before_run/after_run never fired — its StageMetrics entry
    was recorded under the wrapper instead, losing any extra metrics the source
    attaches and any hook override it defines.
    """
    bare_collector = StageMetricsCollector()
    bare = _wire(_HookedSource(inserted=42), bare_collector)
    bare.produce(_ctx())

    wrapped_collector = StageMetricsCollector()
    wrapped = _wire(
        ConditionalSourceTransformer(True, _HookedSource(inserted=42), _source()),
        wrapped_collector,
    )
    wrapped.produce(_ctx())

    assert len(wrapped_collector.stages) == 1, "exactly one entry, not two"
    got = wrapped_collector.stages[0]
    assert got.stage == bare_collector.stages[0].stage == "_HookedSource"
    assert got.inserted == 42


def test_validation_error_names_the_real_source_not_the_wrapper():
    """A missing required SCD column must point at the transformer that produced
    the DataFrame, not at ConditionalSourceTransformer."""
    import pytest

    from spark_quick_start.orchestration.transformers.state import PipelineState

    class _BadSource(SourceTransformer):
        def _produce(self, ctx: LoadContext):
            df = MagicMock()
            df.columns = ["name"]  # missing the business key; the deleted flag is optional (F127)
            return PipelineState(df=df)

    cst = _wire(ConditionalSourceTransformer(True, _BadSource(), _source()), None)

    with pytest.raises(ValueError) as exc_info:
        cst.produce(_ctx())
    assert "_BadSource.produce()" in str(exc_info.value)
    assert "'id'" in str(exc_info.value)


def test_selected_source_is_recorded_for_the_orchestrator_scan():
    """selected_source must still be set — LoadOrchestrator._unwrap relies on it
    to find HWM/source-metrics values on the inner transformer."""
    true_source = _HookedSource(inserted=1)
    cst = _wire(ConditionalSourceTransformer(True, true_source, _source()), StageMetricsCollector())

    cst.produce(_ctx())

    assert cst.selected_source is true_source


# ============================================================================
# _ctx_manager propagation and predicate validation
# ============================================================================

def test_ctx_manager_is_forwarded_to_the_selected_source():
    """StagingTransformer needs _ctx_manager; picking it via .set_source() must still wire it.

    The orchestrator injects _ctx_manager into transformers that declare the
    attribute, but a source chosen at runtime is reached through this wrapper,
    not the orchestrator's loop. Without the forward, a StagingTransformer
    selected this way would call build_staging_phase_context() on None.
    """
    from spark_quick_start.orchestration.transformers.conditional_source import (
        ConditionalSourceTransformer,
    )

    class _NeedsCtxManager(SourceTransformer):
        _ctx_manager = None

        def _produce(self, ctx):
            raise AssertionError("not reached — _select_source is what we exercise")

    inner = _NeedsCtxManager()
    wrapper = ConditionalSourceTransformer(True, inner, MagicMock(spec=SourceTransformer))
    sentinel = object()
    wrapper._ctx_manager = sentinel

    selected = wrapper._select_source(MagicMock())

    assert selected is inner
    assert inner._ctx_manager is sentinel


def test_non_callable_predicate_is_rejected_at_construction():
    """A bad predicate fails where it is written, not mid-run."""
    import pytest

    from spark_quick_start.orchestration.transformers.conditional_source import (
        ConditionalSourceTransformer,
    )

    with pytest.raises(TypeError, match="predicate must be bool or"):
        ConditionalSourceTransformer(
            "not-a-predicate", MagicMock(spec=SourceTransformer), MagicMock(spec=SourceTransformer)
        )


def test_predicate_that_raises_is_wrapped_with_the_source_names():
    """A raising predicate reports which selection failed, not a bare traceback."""
    import pytest

    from spark_quick_start.orchestration.transformers.conditional_source import (
        ConditionalSourceTransformer,
    )

    def _boom(ctx):
        raise KeyError("staging_ctx")

    wrapper = ConditionalSourceTransformer(
        _boom, MagicMock(spec=SourceTransformer), MagicMock(spec=SourceTransformer)
    )

    with pytest.raises(RuntimeError, match="predicate raised while selecting source"):
        wrapper._select_source(MagicMock())
