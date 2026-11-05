"""F168 — the Data Vault link steps, one at a time, and their factory.

The link shares the hub's logic (`keys_in_target`, `insert_new_keys` in dv_hub.py); these tests
pin the link's own surface: its style, its refusals and its provider attributes. The write is
proven in tests/integration/local/test_dv_link_load.py.
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
from spark_quick_start.orchestration.transformers import (
    DVHubWriterTransformer,
    DVLinkTargetTransformer,
    DVLinkWriterTransformer,
)
from spark_quick_start.orchestration.transformers.base import LoadContext
from spark_quick_start.orchestration.transformers.state import PipelineState

_T0 = datetime(2026, 10, 1, 8, 0, tzinfo=UTC)
_HUB = "spark_quick_start.orchestration.transformers.dv_hub"


def _ctx(style="dv_link") -> LoadContext:
    scd = {"business_key_column": "LinkHashKey", "scd_type": style}
    if style == "dv_link":
        scd["link_hub_key_columns"] = ["OrderHashKey", "ProductHashKey"]
    return LoadContext(
        global_ctx=GlobalContext({"id": "ent_1", "entity_name": "OrderLine", "source_system": "CRM",
                                  "target_table_path": "/vault/order_product_l", "staging_table_path": "/s",
                                  "load_timestamp": _T0.isoformat()}),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext(scd),
        runtime_ctx=RuntimeContext({"correlation_id": "run-1", "pipeline_name": "p", "application_name": "a",
                                    "capture_partitions": False, "serverless": is_serverless_session()}),
        logging_ctx=LoggingContext({}),
    )


def _wire(step, adapter, spark):
    step._adapter, step._spark = adapter, spark
    return step


def test_the_target_frame_holds_only_the_batch_keys_the_link_has(spark):
    link = spark.createDataFrame([("l1",), ("l9",)], "LinkHashKey string")
    staged = spark.createDataFrame([("l1", "o1", "p1"), ("l2", "o1", "p2")],
                                   "LinkHashKey string, OrderHashKey string, ProductHashKey string")
    adapter = MockPlatformAdapter(existing_tables={"/vault/order_product_l"}, delta_read_result=link)
    out = _wire(DVLinkTargetTransformer(), adapter, spark)(PipelineState(df=staged, frames={}), _ctx())
    assert [r.LinkHashKey for r in out.frames["target"].collect()] == ["l1"]
    assert out.target_exists is True


def test_a_missing_link_leaves_no_target_frame_and_says_so(spark):
    staged = spark.createDataFrame([("l1", "o1", "p1")], "LinkHashKey string, OrderHashKey string, ProductHashKey string")
    out = _wire(DVLinkTargetTransformer(), MockPlatformAdapter(), spark)(PipelineState(df=staged, frames={}), _ctx())
    assert "target" not in out.frames
    assert out.target_exists is False


def test_the_writer_reports_the_shared_insert_and_its_load_ts(spark):
    staged = spark.createDataFrame([("l1", "o1", "p1")], "LinkHashKey string, OrderHashKey string, ProductHashKey string")
    writer = _wire(DVLinkWriterTransformer(), MockPlatformAdapter(), spark)
    with patch(f"{_HUB}.check_hash_seal", return_value=False), \
         patch(f"{_HUB}.execute_dv_insert_only", return_value=(4, False)) as write, \
         patch(f"{_HUB}.capture_merge_statistics", return_value=(1, 0, 0)):
        writer(PipelineState(df=staged, target_exists=False), _ctx())
    assert (writer.inserted, writer.updated) == (1, 0)
    assert write.call_args.args[3] == ["LinkHashKey"], "insert-only on the link key alone"
    assert write.call_args.kwargs.get("retry", True) is True, "the MERGE re-checks the live table"
    assert (writer.hwm_column, writer.hwm_before) == ("LoadTS", None)
    assert writer.hwm_after == _T0.strftime("%Y-%m-%d %H:%M:%S.%f")


@pytest.mark.parametrize("step", [DVLinkTargetTransformer, DVLinkWriterTransformer], ids=lambda s: s.__name__)
@pytest.mark.parametrize("style", ["dv_hub", "dv_satellite", "scd2"])
def test_a_link_step_refuses_another_style_before_reading(step, style):
    frame = MagicMock()
    adapter = MockPlatformAdapter()
    with pytest.raises(ValueError, match=rf"{step.__name__}.*{style}.*'dv_link'"):
        _wire(step(), adapter, MagicMock())(PipelineState(df=frame, frames={}), _ctx(style))
    assert adapter.delta_table_exists_calls == []
    assert not frame.method_calls


def test_a_hub_step_refuses_a_link_load():
    frame = MagicMock()
    with pytest.raises(ValueError, match="DVHubWriterTransformer.*dv_link.*'dv_hub'"):
        _wire(DVHubWriterTransformer(), MockPlatformAdapter(), MagicMock())(
            PipelineState(df=frame, frames={}), _ctx())
    assert not frame.method_calls


def test_the_link_steps_are_not_hub_steps():
    """Siblings over shared functions (the link design, option b), not subclasses."""
    assert not issubclass(DVLinkWriterTransformer, DVHubWriterTransformer)
    assert DVLinkTargetTransformer.data_vault_style == DVLinkWriterTransformer.data_vault_style == "dv_link"
