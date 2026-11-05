"""LoadOrchestrator._resolve_adapter("databricks").

No such test existed before the platform-scoped test split — added here for
symmetry with tests/unit/fabric/test_fabric_adapter_resolution.py.
"""
from __future__ import annotations


def test_resolve_databricks_adapter():
    from spark_quick_start.adapters.databricks_adapter import DatabricksAdapter
    from spark_quick_start.orchestration import LoadOrchestrator

    adapter = LoadOrchestrator._resolve_adapter("databricks")
    assert isinstance(adapter, DatabricksAdapter)
