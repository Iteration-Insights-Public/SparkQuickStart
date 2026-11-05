"""F156 — every SCD step refuses a Data Vault style before it touches anything.

A wrongly composed pipeline must fail on its first SCD step, naming what to use instead, not
write SCD columns into a satellite. No Spark: the refusal comes before any DataFrame is read.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from spark_quick_start.orchestration.models import (
    GlobalContext,
    LoggingContext,
    RuntimeContext,
    SCDContext,
    StagingContext,
)
from spark_quick_start.orchestration.transformers import (
    NativeSCDAppendOnlyTransformer,
    NativeSCDMergeTransformer,
    SCDCandidateVersionGuardTransformer,
    SCDChangeDetectorTransformer,
    SCDGracefulChangeDetectorTransformer,
    SCDIncrementalTargetTableTransformer,
    SCDTargetTableTransformer,
)
from spark_quick_start.orchestration.transformers.base import LoadContext
from spark_quick_start.orchestration.transformers.state import PipelineState

_STEPS = [SCDTargetTableTransformer, SCDIncrementalTargetTableTransformer, SCDChangeDetectorTransformer,
          SCDGracefulChangeDetectorTransformer, SCDCandidateVersionGuardTransformer,
          NativeSCDMergeTransformer, NativeSCDAppendOnlyTransformer]


def _ctx(style: str) -> LoadContext:
    return LoadContext(
        global_ctx=GlobalContext({"id": "ent_1", "entity_name": "E", "source_system": "S",
                                  "target_table_path": "/t", "staging_table_path": "/s"}),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext({"business_key_column": "HashKey", "scd_type": style}),
        # The template's before_run measures partitions before any step's work; turned off here,
        # so the assertion is about the step's own reads (capture_partitions, spark-delta-cost 4).
        runtime_ctx=RuntimeContext({"correlation_id": "c", "pipeline_name": "p", "application_name": "a",
                                    "capture_partitions": False}),
        logging_ctx=LoggingContext({}),
    )


@pytest.mark.parametrize("step", _STEPS, ids=lambda s: s.__name__)
@pytest.mark.parametrize("style", ["dv_hub", "dv_satellite"])
def test_an_scd_step_refuses_a_data_vault_style_before_reading_anything(step, style) -> None:
    frame = MagicMock()
    with pytest.raises(ValueError, match=rf"{step.__name__}.*{style}.*SCD"):
        step()(PipelineState(df=frame, frames={"target": frame}), _ctx(style))
    assert not frame.method_calls, f"{step.__name__} touched the DataFrame before refusing"
