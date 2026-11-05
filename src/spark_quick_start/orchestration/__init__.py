"""
orchestration — Platform-neutral orchestration layer for spark_quick_start.

Import from here::

    from spark_quick_start.orchestration import LoadOrchestrator
    from spark_quick_start.orchestration import ExecutionResult, ExecutionMetrics
    from spark_quick_start.orchestration import LoadContextManager
    from spark_quick_start.orchestration import (
        LoadContext, SourceTransformer, ProcessTransformer,
        StagingTransformer, DQDTransformer,
        SCDTargetTableTransformer, SCDChangeDetectorTransformer,
        NativeSCDMergeTransformer,
        PipelineState,
    )
"""
from spark_quick_start.orchestration.column_policy import ColumnNamingPolicy
from spark_quick_start.orchestration.context_manager import LoadContextManager
from spark_quick_start.orchestration.models import (
    Constants,
    ErrorDetails,
    ExecutionMetrics,
    ExecutionResult,
    ExtensionsContext,
    GlobalContext,
    LoggingContext,
    PlatformContext,
    RuntimeContext,
    SCDContext,
    StagingContext,
)
from spark_quick_start.orchestration.orchestrator import LoadOrchestrator
from spark_quick_start.orchestration.pipeline_factory import (
    default_pipeline,
    dv_hub_pipeline,
    dv_link_pipeline,
    dv_satellite_pipeline,
)
from spark_quick_start.orchestration.transformers import (
    CandidateGuardTransformer,
    ConditionalTransformer,
    DebugTransformer,
    DQDTransformer,
    DVHubTargetTransformer,
    DVHubWriterTransformer,
    DVLinkTargetTransformer,
    DVLinkWriterTransformer,
    DVSatelliteChangeDetectorTransformer,
    DVSatelliteIncrementalTargetTransformer,
    DVSatelliteTargetTransformer,
    DVSatelliteWriterTransformer,
    LoadContext,
    NativeSCDMergeTransformer,
    ProcessTransformer,
    SCDCandidateVersionGuardTransformer,
    SCDChangeDetectorTransformer,
    SCDGracefulChangeDetectorTransformer,
    SCDTargetTableTransformer,
    SourceTransformer,
    StagedKeyGuardTransformer,
    StagingTransformer,
)
from spark_quick_start.orchestration.transformers._aliases import SCD_RENAMES, deprecated_aliases
from spark_quick_start.orchestration.transformers.state import PipelineState

__all__ = [
    "CandidateGuardTransformer",
    "ColumnNamingPolicy",
    "ConditionalTransformer",
    "Constants",
    "DQDTransformer",
    "DVHubTargetTransformer",
    "DVHubWriterTransformer",
    "DVLinkTargetTransformer",
    "DVLinkWriterTransformer",
    "DVSatelliteChangeDetectorTransformer",
    "DVSatelliteIncrementalTargetTransformer",
    "DVSatelliteTargetTransformer",
    "DVSatelliteWriterTransformer",
    "DebugTransformer",
    "ErrorDetails",
    "ExecutionMetrics",
    "ExecutionResult",
    "ExtensionsContext",
    "GlobalContext",
    "LoadContext",
    "LoadContextManager",
    "LoadOrchestrator",
    "LoggingContext",
    "NativeSCDMergeTransformer",
    "PipelineState",
    "PlatformContext",
    "ProcessTransformer",
    "RuntimeContext",
    "SCDCandidateVersionGuardTransformer",
    "SCDChangeDetectorTransformer",
    "SCDContext",
    "SCDGracefulChangeDetectorTransformer",
    "SCDTargetTableTransformer",
    "SourceTransformer",
    "StagedKeyGuardTransformer",
    "StagingContext",
    "StagingTransformer",
    "default_pipeline",
    "dv_hub_pipeline",
    "dv_link_pipeline",
    "dv_satellite_pipeline",
]


# F155: the pre-rename names still resolve, with a DeprecationWarning (see _aliases.py).
__getattr__ = deprecated_aliases(__name__, globals(), SCD_RENAMES)
