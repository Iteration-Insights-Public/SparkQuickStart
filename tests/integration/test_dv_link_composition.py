"""F167 — a `dv_link` load context is valid, but no hub or satellite step takes it.

Until the link steps land (F168), a `dv_link` context given to a hub or satellite factory is
refused when the pipeline is assembled, before the source step reads anything.
"""
from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter
from spark_quick_start.orchestration import dv_hub_pipeline, dv_satellite_pipeline
from spark_quick_start.orchestration.transformers import DQDTransformer

_LINK = json.dumps({
    "global_context": {"id": "ent_1", "entity_name": "OrderProduct", "source_system": "CRM",
                       "target_table_path": "/vault/order_product_l", "staging_table_path": "/s"},
    "scd_context": {"scd_type": "dv_link", "business_key_column": "LinkHashKey",
                    "link_hub_key_columns": ["OrderHashKey", "ProductHashKey"]},
    "runtime_context": {"correlation_id": "c", "pipeline_name": "p", "application_name": "a"},
    "logging_context": {"logging_level": "INFO", "log_file_name": "l", "log_file_dir": "/tmp/logs"},
    "dqd_context": {"source_kind": "delta", "source_identifier": "/bronze/order_lines",
                    "selection": [{"name": "LinkHashKey", "expression": {"column": "L"}}]},
})


@pytest.mark.parametrize("factory,style", [(dv_hub_pipeline, "dv_hub"), (dv_satellite_pipeline, "dv_satellite")],
                         ids=["hub", "satellite"])
def test_a_link_context_is_refused_by_the_other_factories_before_the_source_runs(spark, factory, style):
    with patch.object(DQDTransformer, "_produce", side_effect=AssertionError("the source ran")) as produce:
        result = factory(_LINK, spark, adapter=MockPlatformAdapter()).run()
    assert result.status == "error"
    assert f"for '{style}' loads only" in result.error_details["error_message"]
    assert "dv_link" in result.error_details["error_message"]
    produce.assert_not_called()
