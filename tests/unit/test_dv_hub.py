"""F159 — the Data Vault hub steps, one at a time: the target read, the style refusals, the stamp.

The write itself needs a Delta table and is proven in tests/integration/local/test_dv_hub_load.py.
"""
from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

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
from spark_quick_start.orchestration.transformers import (
    DVHubTargetTransformer,
    DVHubWriterTransformer,
)
from spark_quick_start.orchestration.transformers.base import LoadContext
from spark_quick_start.orchestration.transformers.dv_hub import stamp_data_vault_columns
from spark_quick_start.orchestration.transformers.state import PipelineState

_T0 = datetime(2026, 10, 1, 8, 0, tzinfo=UTC)


def _ctx(style="dv_hub", **scd) -> LoadContext:
    return LoadContext(
        global_ctx=GlobalContext({"id": "ent_1", "entity_name": "Customer", "source_system": "CRM",
                                  "target_table_path": "/vault/customer_h", "staging_table_path": "/s",
                                  "load_timestamp": _T0.isoformat()}),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext({"business_key_column": "HashKey", "scd_type": style, **scd}),
        runtime_ctx=RuntimeContext({"correlation_id": "run-1", "pipeline_name": "p", "application_name": "a",
                                    "pipeline_run_id": "adf-7", "capture_partitions": False,
                                    "serverless": is_serverless_session()}),
        logging_ctx=LoggingContext({}),
    )


def _wire(step, adapter, spark):
    step._adapter, step._spark = adapter, spark
    return step


def test_a_missing_hub_leaves_no_target_frame(spark):
    staged = spark.createDataFrame([("h1", "1")], "HashKey string, CustomerNo string")
    step = _wire(DVHubTargetTransformer(), MockPlatformAdapter(), spark)
    result = step(PipelineState(df=staged, frames={}), _ctx())
    assert "target" not in result.frames
    assert result.target_exists is False


def test_the_target_frame_holds_only_the_batch_keys_the_hub_has(spark):
    """A hub never deletes, so its keys outside the batch are never read into the frame."""
    hub = spark.createDataFrame([("h1",), ("h2",), ("h9",)], "HashKey string")
    staged = spark.createDataFrame([("h1", "1"), ("h3", "3")], "HashKey string, CustomerNo string")
    adapter = MockPlatformAdapter(existing_tables={"/vault/customer_h"}, delta_read_result=hub)
    result = _wire(DVHubTargetTransformer(), adapter, spark)(PipelineState(df=staged, frames={}), _ctx(load_type="Full"))
    assert sorted(r.HashKey for r in result.frames["target"].collect()) == ["h1"]
    assert result.target_exists is True


@pytest.mark.parametrize("step", [DVHubTargetTransformer, DVHubWriterTransformer], ids=lambda s: s.__name__)
@pytest.mark.parametrize("style", ["dv_satellite", "scd2"])
def test_a_hub_step_refuses_another_style_before_reading_anything(step, style):
    frame = MagicMock()
    adapter = MockPlatformAdapter()
    with pytest.raises(ValueError, match=rf"{step.__name__}.*{style}.*'dv_hub'"):
        _wire(step(), adapter, MagicMock())(PipelineState(df=frame, frames={}), _ctx(style))
    assert not frame.method_calls and adapter.delta_table_exists_calls == []


def test_the_stamp_adds_every_load_column(spark):
    rows = spark.createDataFrame([("h1", "1")], "HashKey string, CustomerNo string")
    from pyspark.sql import functions as F
    stamped = stamp_data_vault_columns(rows, _ctx(pipeline_run_id_column="PipelineRunId"), _T0)
    out = stamped.select("RecordSource", "CorrelationId", "PipelineRunId",
                         F.col("LoadTS").cast("double").alias("load"),
                         F.col("LoadEndTS").cast("double").alias("load_end")).first()
    assert (out.RecordSource, out.CorrelationId, out.PipelineRunId) == ("CRM", "run-1", "adf-7")
    # Epoch seconds, so the session time zone cannot shift what is compared.
    assert out.load == _T0.timestamp()
    assert out.load_end >= out.load


def test_the_stamp_keeps_the_rows_own_source_under_source_system_from_source(spark):
    rows = spark.createDataFrame([("h1", "ERP")], "HashKey string, RecordSource string")
    out = stamp_data_vault_columns(rows, _ctx(source_system_from_source=True), _T0).first()
    assert out.RecordSource == "ERP"


def test_no_pipeline_run_id_column_unless_named(spark):
    rows = spark.createDataFrame([("h1",)], "HashKey string")
    assert "PipelineRunId" not in stamp_data_vault_columns(rows, _ctx(), _T0).columns


# ── the writer's branches, with the Delta work patched out ───────────────────

_MOD = "spark_quick_start.orchestration.transformers.dv_hub"


@pytest.mark.parametrize("created,needs_seal", [(True, True), (False, False), (False, True)])
def test_the_writer_counts_from_the_right_commit_and_seals_only_a_first_write(spark, created, needs_seal):
    from unittest.mock import patch
    staged = spark.createDataFrame([("h1", "1")], "HashKey string, CustomerNo string")
    writer = _wire(DVHubWriterTransformer(), MockPlatformAdapter(), spark)
    with patch(f"{_MOD}.check_hash_seal", return_value=needs_seal), \
         patch(f"{_MOD}.write_hash_seal") as seal, \
         patch(f"{_MOD}.execute_dv_insert_only", return_value=(-1 if created else 4, created)) as write, \
         patch(f"{_MOD}.capture_write_statistics", return_value=7) as written, \
         patch(f"{_MOD}.capture_merge_statistics", return_value=(3, 0, 0)) as merged:
        writer(PipelineState(df=staged, target_exists=False), _ctx())
    assert writer.inserted == (7 if created else 3) and writer.updated == 0
    assert (written.called, merged.called) == (created, not created)
    assert seal.called is needs_seal
    rows = write.call_args.args[2]
    assert {"RecordSource", "LoadTS", "LoadEndTS", "CorrelationId"} <= set(rows.columns)
    assert (writer.hwm_column, writer.hwm_before) == ("LoadTS", None)
    assert writer.hwm_after == _T0.strftime("%Y-%m-%d %H:%M:%S.%f")


def test_the_writer_passes_on_only_the_keys_the_hub_lacks(spark):
    from unittest.mock import patch
    staged = spark.createDataFrame([("h1", "1"), ("h2", "2")], "HashKey string, CustomerNo string")
    existing = spark.createDataFrame([("h1",)], "HashKey string")
    writer = _wire(DVHubWriterTransformer(), MockPlatformAdapter(), spark)
    with patch(f"{_MOD}.check_hash_seal", return_value=False), patch(f"{_MOD}.write_hash_seal"), \
         patch(f"{_MOD}.execute_dv_insert_only", return_value=(4, False)) as write, \
         patch(f"{_MOD}.capture_merge_statistics", return_value=(1, 0, 0)):
        writer(PipelineState(df=staged, frames={"target": existing}), _ctx())
    assert [r.HashKey for r in write.call_args.args[2].collect()] == ["h2"]


def test_a_failed_check_writes_nothing(spark):
    from unittest.mock import patch
    staged = spark.createDataFrame([("h1", "1")], "HashKey string, CustomerNo string")
    writer = _wire(DVHubWriterTransformer(), MockPlatformAdapter(), spark)
    with patch(f"{_MOD}.check_hash_seal", side_effect=ValueError("sealed otherwise")), \
         patch(f"{_MOD}.execute_dv_insert_only") as write:
        with pytest.raises(ValueError, match="sealed otherwise"):
            writer(PipelineState(df=staged, target_exists=False), _ctx())
    write.assert_not_called()



def test_the_load_context_carries_no_run_start():
    """F106: the run's time is global_context.load_timestamp; LoadContext.run_started_at is gone."""
    from dataclasses import fields

    assert "run_started_at" not in {f.name for f in fields(LoadContext)}
