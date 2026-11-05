"""
Smoke tests for pipeline factory: default_pipeline().
"""
from __future__ import annotations

import inspect
from unittest.mock import MagicMock, patch

from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter
from spark_quick_start.orchestration import default_pipeline
from spark_quick_start.orchestration.orchestrator import LoadOrchestrator
from spark_quick_start.orchestration.transformers import (
    CandidateGuardTransformer,
    NativeSCDMergeTransformer,
    SCDChangeDetectorTransformer,
    SCDTargetTableTransformer,
    StagingTransformer,
)


def _mock_spark():
    return MagicMock()


def _mock_adapter():
    return MockPlatformAdapter()


def test_default_pipeline_returns_orchestrator():
    with patch('spark_quick_start.orchestration.orchestrator.LoadOrchestrator._resolve_adapter'):
        orch = default_pipeline("{}", _mock_spark(), adapter=_mock_adapter())
    assert isinstance(orch, LoadOrchestrator)


def test_default_pipeline_registers_5_step_merge_chain_with_the_quality_step():
    """F29 — the quality step sits between detection and the merge.

    SCDChangeDetectorTransformer no longer validates. It classifies, and nothing else;
    a duplicate candidate is a data-quality question, not a change-detection one.
    CandidateGuardTransformer owns it, grouped on (business_key, source_system),
    which is the shape a corrupted target actually produces.
    """
    with patch('spark_quick_start.orchestration.orchestrator.LoadOrchestrator._resolve_adapter'):
        orch = default_pipeline("{}", _mock_spark(), adapter=_mock_adapter())
    assert len(orch._transformers) == 5
    assert isinstance(orch._transformers[0], StagingTransformer)
    assert isinstance(orch._transformers[1], SCDTargetTableTransformer)
    assert isinstance(orch._transformers[2], SCDChangeDetectorTransformer)
    assert isinstance(orch._transformers[3], CandidateGuardTransformer)
    assert isinstance(orch._transformers[4], NativeSCDMergeTransformer)


def test_change_detector_no_longer_takes_validate_no_dups():
    """F29 — validation left the detector, so its constructor lost the flag.

    Breaking by design, the same shape as F2's target_frame_key removal: a caller
    passing it was configuring a check that is no longer the detector's to run, and a
    TypeError says so at construction rather than silently doing nothing.
    """
    assert "validate_no_dups" not in inspect.signature(SCDChangeDetectorTransformer).parameters


def test_default_pipeline_supports_fluent_add():
    """Additional transformer can be chained after the factory call."""
    custom = MagicMock()
    with patch('spark_quick_start.orchestration.orchestrator.LoadOrchestrator._resolve_adapter'):
        orch = default_pipeline("{}", _mock_spark(), adapter=_mock_adapter()).add(custom)
    # 5 factory steps after F29 added the quality step, so the custom one is index 5.
    assert len(orch._transformers) == 6
    assert orch._transformers[5] is custom


def test_default_pipeline_import_from_orchestration_package():
    """Verify the factory is importable from the top-level package."""
    from spark_quick_start.orchestration import default_pipeline as dp
    assert callable(dp)


def test_new_transformers_importable_from_orchestration_package():
    """Core transformers are top-level exports from orchestration package."""
    from spark_quick_start.orchestration import (
        NativeSCDMergeTransformer,
        PipelineState,
        SCDChangeDetectorTransformer,
        SCDTargetTableTransformer,
    )
    assert SCDTargetTableTransformer is not None
    assert SCDChangeDetectorTransformer is not None
    assert NativeSCDMergeTransformer is not None
    assert PipelineState is not None


def test_dv_hub_pipeline_composes_the_hub_steps_and_no_candidate_guard():
    """F159: the source picked by dqd_context, the staged-key guard, the hub target and writer.

    No CandidateGuardTransformer: it groups on the source system column a hub's staged rows do
    not carry, and the staged-key guard already refuses a key staged twice."""
    from spark_quick_start.orchestration import dv_hub_pipeline
    from spark_quick_start.orchestration.transformers import (
        ConditionalSourceTransformer,
        DQDTransformer,
        DVHubTargetTransformer,
        DVHubWriterTransformer,
        StagedKeyGuardTransformer,
    )

    with patch('spark_quick_start.orchestration.orchestrator.LoadOrchestrator._resolve_adapter'):
        orch = dv_hub_pipeline("{}", _mock_spark(), adapter=_mock_adapter())
    steps = orch._transformers
    assert [type(s) for s in steps[1:]] == [StagedKeyGuardTransformer, DVHubTargetTransformer, DVHubWriterTransformer]
    source = steps[0]
    assert isinstance(source, ConditionalSourceTransformer)
    assert isinstance(source._source_if_true, DQDTransformer)
    assert isinstance(source._source_if_false, StagingTransformer)
    with_dqd, without = MagicMock(dqd_ctx=object()), MagicMock(dqd_ctx=None)
    assert source._predicate(with_dqd) is True and source._predicate(without) is False
    assert not any(isinstance(s, CandidateGuardTransformer) for s in steps)



def test_dv_satellite_pipeline_picks_the_target_step_by_load_type():
    """F162: the source, the staged-key guard, one of the two target steps by load_type, the
    detector and the writer; no CandidateGuardTransformer, as in the hub."""
    from spark_quick_start.orchestration import dv_satellite_pipeline
    from spark_quick_start.orchestration.transformers import (
        ConditionalSourceTransformer,
        ConditionalTransformer,
        DVSatelliteChangeDetectorTransformer,
        DVSatelliteIncrementalTargetTransformer,
        DVSatelliteTargetTransformer,
        DVSatelliteWriterTransformer,
        StagedKeyGuardTransformer,
    )

    with patch('spark_quick_start.orchestration.orchestrator.LoadOrchestrator._resolve_adapter'):
        orch = dv_satellite_pipeline("{}", _mock_spark(), adapter=_mock_adapter())
    steps = orch._transformers
    assert isinstance(steps[0], ConditionalSourceTransformer)
    assert type(steps[1]) is StagedKeyGuardTransformer
    full_step, incremental_step = steps[2], steps[3]
    assert isinstance(full_step, ConditionalTransformer) and isinstance(incremental_step, ConditionalTransformer)
    assert type(full_step._transformer) is DVSatelliteTargetTransformer
    assert type(incremental_step._transformer) is DVSatelliteIncrementalTargetTransformer
    for load_type, runs_full in (("Full", True), ("Incremental", False)):
        ctx = MagicMock()
        ctx.scd_ctx.load_type = load_type
        assert full_step._predicate(ctx) is runs_full and incremental_step._predicate(ctx) is not runs_full
    assert [type(s) for s in steps[4:]] == [DVSatelliteChangeDetectorTransformer, DVSatelliteWriterTransformer]
    assert not any(isinstance(s, CandidateGuardTransformer) for s in steps)


def test_dv_link_pipeline_composes_the_link_steps_and_no_candidate_guard():
    """F168: as the hub's, with the link's target and writer."""
    from spark_quick_start.orchestration import dv_link_pipeline
    from spark_quick_start.orchestration.transformers import (
        ConditionalSourceTransformer,
        DVLinkTargetTransformer,
        DVLinkWriterTransformer,
        StagedKeyGuardTransformer,
    )

    with patch('spark_quick_start.orchestration.orchestrator.LoadOrchestrator._resolve_adapter'):
        orch = dv_link_pipeline("{}", _mock_spark(), adapter=_mock_adapter())
    steps = orch._transformers
    assert isinstance(steps[0], ConditionalSourceTransformer)
    assert [type(s) for s in steps[1:]] == [StagedKeyGuardTransformer, DVLinkTargetTransformer, DVLinkWriterTransformer]
    assert not any(isinstance(s, CandidateGuardTransformer) for s in steps)
