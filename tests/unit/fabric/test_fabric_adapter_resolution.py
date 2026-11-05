"""LoadOrchestrator._resolve_adapter("fabric") — relocated from
tests/test_load_orchestrator.py during the platform-scoped test split.
"""
from __future__ import annotations


def test_resolve_fabric_adapter():
    from spark_quick_start.adapters.fabric_adapter import FabricAdapter
    from spark_quick_start.orchestration import LoadOrchestrator

    adapter = LoadOrchestrator._resolve_adapter("fabric")
    assert isinstance(adapter, FabricAdapter)
