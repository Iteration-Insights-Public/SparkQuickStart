from spark_quick_start.orchestration.transformers._aliases import SCD_RENAMES, deprecated_aliases
from spark_quick_start.orchestration.transformers.base import (
    AlwaysRunTransformer,
    LoadContext,
    ProcessTransformer,
    SourceTransformer,
)
from spark_quick_start.orchestration.transformers.candidate_guard import CandidateGuardTransformer
from spark_quick_start.orchestration.transformers.candidate_version_guard import (
    SCDCandidateVersionGuardTransformer,
)
from spark_quick_start.orchestration.transformers.change_detector import (
    SCDChangeDetectorTransformer,
    SCDGracefulChangeDetectorTransformer,
)
from spark_quick_start.orchestration.transformers.conditional import ConditionalTransformer
from spark_quick_start.orchestration.transformers.conditional_source import (
    ConditionalSourceTransformer,
)
from spark_quick_start.orchestration.transformers.debug import DebugTransformer
from spark_quick_start.orchestration.transformers.dqd import DQDTransformer
from spark_quick_start.orchestration.transformers.dv_hub import (
    DVHubTargetTransformer,
    DVHubWriterTransformer,
)
from spark_quick_start.orchestration.transformers.dv_link import (
    DVLinkTargetTransformer,
    DVLinkWriterTransformer,
)
from spark_quick_start.orchestration.transformers.dv_satellite import (
    DVSatelliteChangeDetectorTransformer,
    DVSatelliteIncrementalTargetTransformer,
    DVSatelliteTargetTransformer,
    DVSatelliteWriterTransformer,
)
from spark_quick_start.orchestration.transformers.materialize import (
    MaterializeFrameTransformer,
    MaterializeTransformer,
)
from spark_quick_start.orchestration.transformers.metrics_chart import MetricsChartTransformer
from spark_quick_start.orchestration.transformers.native_scd import (
    NativeSCDAppendOnlyTransformer,
    NativeSCDMergeTransformer,
)
from spark_quick_start.orchestration.transformers.plan_report import PlanReportTransformer
from spark_quick_start.orchestration.transformers.protocols import (
    HWMProvider,
    SCDMetricsProvider,
    SourceMetricsProvider,
)
from spark_quick_start.orchestration.transformers.staged_key_guard import StagedKeyGuardTransformer
from spark_quick_start.orchestration.transformers.staging import StagingTransformer
from spark_quick_start.orchestration.transformers.target import (
    SCDIncrementalTargetTableTransformer,
    SCDTargetTableTransformer,
)

__all__ = [
    "AlwaysRunTransformer",
    "CandidateGuardTransformer",
    "ConditionalSourceTransformer",
    "ConditionalTransformer",
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
    "HWMProvider",
    "LoadContext",
    "MaterializeFrameTransformer",
    "MaterializeTransformer",
    "MetricsChartTransformer",
    "NativeSCDAppendOnlyTransformer",
    "NativeSCDMergeTransformer",
    "PlanReportTransformer",
    "ProcessTransformer",
    "SCDCandidateVersionGuardTransformer",
    "SCDChangeDetectorTransformer",
    "SCDGracefulChangeDetectorTransformer",
    "SCDIncrementalTargetTableTransformer",
    "SCDMetricsProvider",
    "SCDTargetTableTransformer",
    "SourceMetricsProvider",
    "SourceTransformer",
    "StagedKeyGuardTransformer",
    "StagingTransformer",
]


# F155: the pre-rename names still resolve, with a DeprecationWarning (see _aliases.py).
__getattr__ = deprecated_aliases(__name__, globals(), SCD_RENAMES)
