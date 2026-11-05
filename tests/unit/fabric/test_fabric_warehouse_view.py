"""FabricAdapter.read_warehouse_view (F140): a Fabric Warehouse or SQL-endpoint view.

The Fabric call is the one the removed DQF engine made for its "view" kind: options workspaceId
and lakehouseId on the reader, then synapsesql. What the connector does with them is not provable
here; this proves the call is made as DQF made it, through the adapter.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from spark_quick_start.adapters.fabric_adapter import FabricAdapter


def test_the_view_is_read_with_the_connector_and_its_ids():
    spark = MagicMock()
    reader = spark.read
    reader.option.return_value = reader
    frame = FabricAdapter().read_warehouse_view(spark, "wh.dbo.v_customer", workspace_id="ws", lakehouse_id="wh")
    assert reader.option.call_args_list[0].args == ("workspaceId", "ws")
    assert reader.option.call_args_list[1].args == ("lakehouseId", "wh")
    reader.synapsesql.assert_called_once_with("wh.dbo.v_customer")
    assert frame is reader.synapsesql.return_value


def test_without_ids_no_option_is_set():
    spark = MagicMock()
    FabricAdapter().read_warehouse_view(spark, "wh.dbo.v")
    spark.read.option.assert_not_called()
    spark.read.synapsesql.assert_called_once_with("wh.dbo.v")


def test_a_reader_without_the_connector_is_refused_with_what_is_missing():
    class _Reader:
        def option(self, *_):
            return self

    spark = MagicMock()
    spark.read = _Reader()
    with pytest.raises(RuntimeError, match="no synapsesql.*Warehouse connector"):
        FabricAdapter().read_warehouse_view(spark, "wh.dbo.v")

