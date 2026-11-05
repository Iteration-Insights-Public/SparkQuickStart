"""
pipeline_factory.py — Pre-built LoadOrchestrator pipeline configurations.

Provides default_pipeline() and the Data Vault pipelines as convenience factories
that mirror the legacy hardcoded paths using the new transformer architecture.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from spark_quick_start.adapters.platform_adapter import PlatformAdapter
    from spark_quick_start.logger import OrchestratorLogger
    from spark_quick_start.orchestration.orchestrator import LoadOrchestrator
    from spark_quick_start.orchestration.transformers.base import LoadContext


def default_pipeline(
    load_context_json: str,
    spark: SparkSession,
    *,
    adapter: PlatformAdapter | None = None,
    platform: str = "fabric",
    data_storage_path: str = "",
    data_eng_path: str = "",
    logger: OrchestratorLogger | None = None,
) -> LoadOrchestrator:
    """Return a LoadOrchestrator pre-configured with the standard merge pipeline.

    Registers the 5-step pipeline:
    StagingTransformer → SCDTargetTableTransformer → SCDChangeDetectorTransformer → CandidateGuardTransformer → NativeSCDMergeTransformer

    This is equivalent to the legacy Branch A + SCD merge path. All keyword
    arguments are forwarded to LoadOrchestrator.__init__ unchanged.

    Args:
        load_context_json: JSON string with all context categories.
        spark: Active SparkSession.
        adapter: Platform adapter (takes priority over platform string).
        platform: "fabric", "databricks", or "local" when adapter is None.
        data_storage_path: Resolved {DataStorageLakehouse} placeholder value.
        data_eng_path: Resolved {DataEngineeringLakehouse} placeholder value.
        logger: Pre-constructed OrchestratorLogger for testing.

    Returns:
        LoadOrchestrator with 5-step merge pipeline registered.
    """
    from spark_quick_start.orchestration.orchestrator import LoadOrchestrator
    from spark_quick_start.orchestration.transformers import (
        CandidateGuardTransformer,
        NativeSCDMergeTransformer,
        SCDChangeDetectorTransformer,
        SCDTargetTableTransformer,
        StagingTransformer,
    )

    return (
        LoadOrchestrator(
            load_context_json=load_context_json,
            spark=spark,
            adapter=adapter,
            platform=platform,
            data_storage_path=data_storage_path,
            data_eng_path=data_eng_path,
            logger=logger,
        )
        .add(StagingTransformer())
        .add(SCDTargetTableTransformer())
        .add(SCDChangeDetectorTransformer())
        # The quality step. One candidate per (business_key, source_system) is a
        # property of this pipeline, not of the detector, so it is asserted here by
        # composition. An append-only load built by hand composes the version-aware
        # variant instead (F29).
        #
        # Behaviour change this introduces, stated because two things outside this repo
        # consume it. CandidateGuardTransformer halts on zero candidates, so a no-change
        # load now returns early_exit=True with no merge stage in result.stages, where it
        # previously reported a successful no-op merge. And execute_scd_merge is the only
        # path that *creates* the target table when absent, so a first load whose source
        # yields zero rows no longer bootstraps an empty Silver table — the halt comes
        # first. Remove this step if a caller depends on either.
        .add(CandidateGuardTransformer())
        .add(NativeSCDMergeTransformer())
    )


def dv_hub_pipeline(
    load_context_json: str,
    spark: SparkSession,
    *,
    adapter: PlatformAdapter | None = None,
    platform: str = "fabric",
    data_storage_path: str = "",
    data_eng_path: str = "",
    logger: OrchestratorLogger | None = None,
) -> LoadOrchestrator:
    """Return a LoadOrchestrator that loads a Data Vault hub (F159).

    The source is the load context's `dqd_context` when it has one, its staging notebooks
    otherwise; then `StagedKeyGuardTransformer`, which refuses a key staged twice, NULL or
    empty before anything is written; `DVHubTargetTransformer`; `DVHubWriterTransformer`.
    The load context's `scd_context.scd_type` must be `dv_hub`. Keyword arguments go to
    LoadOrchestrator unchanged.
    """
    from spark_quick_start.orchestration.orchestrator import LoadOrchestrator
    from spark_quick_start.orchestration.transformers import (
        DQDTransformer,
        DVHubTargetTransformer,
        DVHubWriterTransformer,
        StagedKeyGuardTransformer,
        StagingTransformer,
    )

    return (
        LoadOrchestrator(
            load_context_json=load_context_json,
            spark=spark,
            adapter=adapter,
            platform=platform,
            data_storage_path=data_storage_path,
            data_eng_path=data_eng_path,
            logger=logger,
        )
        .set_source(lambda ctx: ctx.dqd_ctx is not None, DQDTransformer(), StagingTransformer())
        .add(StagedKeyGuardTransformer())
        .add(DVHubTargetTransformer())
        .add(DVHubWriterTransformer())
    )


def dv_link_pipeline(
    load_context_json: str,
    spark: SparkSession,
    *,
    adapter: PlatformAdapter | None = None,
    platform: str = "fabric",
    data_storage_path: str = "",
    data_eng_path: str = "",
    logger: OrchestratorLogger | None = None,
) -> LoadOrchestrator:
    """Return a LoadOrchestrator that loads a Data Vault link (F168).

    As `dv_hub_pipeline`, with the link's steps: the source picked by `dqd_context`;
    `StagedKeyGuardTransformer`, which also refuses a NULL or empty hub key; then
    `DVLinkTargetTransformer` and `DVLinkWriterTransformer`. The load context's
    `scd_context.scd_type` must be `dv_link`, with `link_hub_key_columns`. Keyword arguments go
    to LoadOrchestrator unchanged.
    """
    from spark_quick_start.orchestration.orchestrator import LoadOrchestrator
    from spark_quick_start.orchestration.transformers import (
        DQDTransformer,
        DVLinkTargetTransformer,
        DVLinkWriterTransformer,
        StagedKeyGuardTransformer,
        StagingTransformer,
    )

    return (
        LoadOrchestrator(
            load_context_json=load_context_json,
            spark=spark,
            adapter=adapter,
            platform=platform,
            data_storage_path=data_storage_path,
            data_eng_path=data_eng_path,
            logger=logger,
        )
        .set_source(lambda ctx: ctx.dqd_ctx is not None, DQDTransformer(), StagingTransformer())
        .add(StagedKeyGuardTransformer())
        .add(DVLinkTargetTransformer())
        .add(DVLinkWriterTransformer())
    )


def dv_satellite_pipeline(
    load_context_json: str,
    spark: SparkSession,
    *,
    adapter: PlatformAdapter | None = None,
    platform: str = "fabric",
    data_storage_path: str = "",
    data_eng_path: str = "",
    logger: OrchestratorLogger | None = None,
) -> LoadOrchestrator:
    """Return a LoadOrchestrator that loads a Data Vault satellite (F162).

    The source is the load context's `dqd_context` when it has one, its staging notebooks
    otherwise; then `StagedKeyGuardTransformer`; the satellite's latest rows, all of them for a
    `Full` load (`DVSatelliteTargetTransformer`) and the batch's keys otherwise
    (`DVSatelliteIncrementalTargetTransformer`); `DVSatelliteChangeDetectorTransformer`;
    `DVSatelliteWriterTransformer`. No `CandidateGuardTransformer`: it is SCD-only, and with
    one staged row and one latest row per key the detector cannot emit a key twice. The load
    context's `scd_context.scd_type` must be `dv_satellite`. Keyword arguments go to
    LoadOrchestrator unchanged.
    """
    from spark_quick_start.orchestration.orchestrator import LoadOrchestrator
    from spark_quick_start.orchestration.transformers import (
        DQDTransformer,
        DVSatelliteChangeDetectorTransformer,
        DVSatelliteIncrementalTargetTransformer,
        DVSatelliteTargetTransformer,
        DVSatelliteWriterTransformer,
        StagedKeyGuardTransformer,
        StagingTransformer,
    )

    def full(ctx: LoadContext) -> bool:
        return ctx.scd_ctx.load_type == "Full"

    return (
        LoadOrchestrator(
            load_context_json=load_context_json,
            spark=spark,
            adapter=adapter,
            platform=platform,
            data_storage_path=data_storage_path,
            data_eng_path=data_eng_path,
            logger=logger,
        )
        .set_source(lambda ctx: ctx.dqd_ctx is not None, DQDTransformer(), StagingTransformer())
        .add(StagedKeyGuardTransformer())
        .add_if(full, DVSatelliteTargetTransformer())
        .add_if(lambda ctx: not full(ctx), DVSatelliteIncrementalTargetTransformer())
        .add(DVSatelliteChangeDetectorTransformer())
        .add(DVSatelliteWriterTransformer())
    )

