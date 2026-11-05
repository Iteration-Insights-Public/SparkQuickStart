"""F162 — DVSatelliteWriterTransformer's branches, with the Delta work patched out.

The write itself needs a Delta table and is proven in
tests/integration/local/test_dv_satellite_load.py.
"""
from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from conftest import is_serverless_session
from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter
from spark_quick_start.orchestration.models import (
    GlobalContext,
    LoggingContext,
    RuntimeContext,
    SCDContext,
    StagingContext,
)
from spark_quick_start.orchestration.transformers import DVSatelliteWriterTransformer
from spark_quick_start.orchestration.transformers._constants import CHANGE_TYPE
from spark_quick_start.orchestration.transformers.base import LoadContext
from spark_quick_start.orchestration.transformers.state import PipelineState

_T0 = datetime(2026, 10, 1, 8, 0, tzinfo=UTC)
_MOD = "spark_quick_start.orchestration.transformers.dv_satellite"


def _ctx(style="dv_satellite") -> LoadContext:
    return LoadContext(
        global_ctx=GlobalContext({"id": "ent_1", "entity_name": "Customer", "source_system": "CRM",
                                  "target_table_path": "/vault/customer_crm_s", "staging_table_path": "/s",
                                  "load_timestamp": _T0.isoformat()}),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext({"business_key_column": "HashKey", "scd_type": style}),
        runtime_ctx=RuntimeContext({"correlation_id": "run-1", "pipeline_name": "p", "application_name": "a",
                                    "capture_partitions": False, "serverless": is_serverless_session()}),
        logging_ctx=LoggingContext({}),
    )


def _candidates(spark):
    return spark.createDataFrame([("h1", "Ada", "d1", "N", "CRM", "new")],
                                 f"HashKey string, Name string, HashDiff string, IsDeleted string, "
                                 f"RecordSource string, {CHANGE_TYPE} string")


@pytest.mark.parametrize("created,needs_seal", [(True, True), (False, False), (False, True)])
def test_the_writer_counts_from_the_right_commit_and_seals_only_a_first_write(spark, created, needs_seal):
    writer = DVSatelliteWriterTransformer()
    writer._adapter, writer._spark = MockPlatformAdapter(), spark
    with patch(f"{_MOD}.check_hash_seal", return_value=needs_seal), \
         patch(f"{_MOD}.write_hash_seal") as seal, \
         patch(f"{_MOD}.execute_dv_insert_only", return_value=(-1 if created else 4, created)) as write, \
         patch(f"{_MOD}.capture_write_statistics", return_value=7) as written, \
         patch(f"{_MOD}.capture_merge_statistics", return_value=(3, 0, 0)) as merged:
        writer(PipelineState(df=_candidates(spark), frames={}), _ctx())
    assert writer.inserted == (7 if created else 3) and writer.updated == 0
    assert (written.called, merged.called) == (created, not created)
    assert seal.called is needs_seal
    rows, keys = write.call_args.args[2], write.call_args.args[3]
    assert keys == ["HashKey", "LoadTS"], "insert-only on the key and the run's load timestamp"
    assert write.call_args.kwargs.get("retry") is False, (
        "a retry re-runs the detector's rows on its old snapshot, inserting a change a concurrent "
        "run already wrote; a conflict must fail the run so a rerun re-detects (F162's review)")
    assert CHANGE_TYPE not in rows.columns, "the routing column never reaches the satellite"
    assert {"RecordSource", "LoadTS", "LoadEndTS", "CorrelationId", "HashDiff", "IsDeleted"} <= set(rows.columns)
    assert (writer.hwm_column, writer.hwm_before) == ("LoadTS", None)
    assert writer.hwm_after == _T0.strftime("%Y-%m-%d %H:%M:%S.%f")


@pytest.mark.parametrize("answer", [False, True, None])
def test_the_writer_passes_the_target_steps_answer_to_the_insert(spark, answer):
    """F210: the satellite is created or merged into on the target step's answer, not on a second check."""
    state = PipelineState(df=_candidates(spark), frames={}, target_exists=None)
    if answer is not None:
        state = state.with_target(spark.createDataFrame([], "HashKey string") if answer else None)
    writer = DVSatelliteWriterTransformer()
    writer._adapter, writer._spark = MockPlatformAdapter(), spark
    with patch(f"{_MOD}.check_hash_seal", return_value=False), \
         patch(f"{_MOD}.execute_dv_insert_only", return_value=(4, False)) as write, \
         patch(f"{_MOD}.capture_merge_statistics", return_value=(3, 0, 0)):
        writer(state, _ctx())
    assert write.call_args.kwargs["target_exists"] is answer


def test_a_failed_seal_check_writes_nothing(spark):
    writer = DVSatelliteWriterTransformer()
    writer._adapter, writer._spark = MockPlatformAdapter(), spark
    with patch(f"{_MOD}.check_hash_seal", side_effect=ValueError("sealed otherwise")), \
         patch(f"{_MOD}.execute_dv_insert_only") as write:
        with pytest.raises(ValueError, match="sealed otherwise"):
            writer(PipelineState(df=_candidates(spark), frames={}), _ctx())
    write.assert_not_called()


@pytest.mark.parametrize("style", ["dv_hub", "scd2"])
def test_the_writer_refuses_another_style_before_reading(style):
    frame = MagicMock()
    adapter = MockPlatformAdapter()
    writer = DVSatelliteWriterTransformer()
    writer._adapter, writer._spark = adapter, MagicMock()
    with pytest.raises(ValueError, match=rf"DVSatelliteWriterTransformer.*{style}.*'dv_satellite'"):
        writer(PipelineState(df=frame, frames={}), _ctx(style))
    assert adapter.delta_table_exists_calls == []
    assert not frame.method_calls
