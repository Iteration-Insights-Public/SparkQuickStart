"""
Tests for LoadOrchestrator transformer pipeline builder (.add() fluent API).
============================================================================

Coverage:
  - .add() method: returns self for chaining, accumulates transformers
  - Plain callables accepted in .add()

_run_pipeline runtime behaviour is covered in test_orchestrator_pipeline_integration.py.
"""
from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Stub the 'delta' package before any library code is imported.
# ---------------------------------------------------------------------------
if "delta" not in sys.modules:
    _delta_stub = types.ModuleType("delta")
    _delta_tables_stub = types.ModuleType("delta.tables")

    class _MockDeltaTable:
        @staticmethod
        def isDeltaTable(spark, path):
            return False

        @staticmethod
        def forPath(spark, path):
            return MagicMock()

    _delta_tables_stub.DeltaTable = _MockDeltaTable
    _delta_stub.tables = _delta_tables_stub
    sys.modules["delta"] = _delta_stub
    sys.modules["delta.tables"] = _delta_tables_stub

from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter
from spark_quick_start.orchestration import LoadOrchestrator
from spark_quick_start.orchestration.transformers.base import (
    ProcessTransformer,
    SourceTransformer,
)


def _make_orchestrator():
    """Factory for a minimal LoadOrchestrator with valid context JSON."""
    import json
    ctx = {
        "global_context": {
            "id": "ent_1",
            "entity_name": "test_entity",
            "source_system": "test_source",
            "target_table_path": "/tmp/test_target",
            "staging_table_path": "/tmp/test_staging",
        },
        "scd_context": {
            "business_key_column": "id",
            "effective_from_column": "effective_from",
            "scd_type": "scd1",
        },
        "staging_context": {},
        "runtime_context": {},
        "logging_context": {},
    }
    return LoadOrchestrator(
        load_context_json=json.dumps(ctx),
        spark=MagicMock(),
        adapter=MockPlatformAdapter(),
    )


class _DummySource(SourceTransformer):
    def _produce(self, ctx):
        from spark_quick_start.orchestration.transformers.state import PipelineState
        mock_df = MagicMock()
        mock_df.columns = ["id", "scdDeletedFlag"]
        return PipelineState(df=mock_df)


class _DummyProcess(ProcessTransformer):
    def _run(self, state, ctx):
        return state


# ============================================================================
# TESTS
# ============================================================================


def test_add_returns_self():
    orch = _make_orchestrator()
    result = orch.add(_DummySource())
    assert result is orch


def test_add_accumulates_transformers():
    orch = _make_orchestrator()
    orch.add(_DummySource()).add(_DummyProcess())
    assert len(orch._transformers) == 2


def test_plain_callable_accepted_in_pipeline():
    orch = _make_orchestrator()

    def my_transformer(df, ctx):
        return df

    orch.add(_DummySource()).add(my_transformer)
    assert len(orch._transformers) == 2
    assert callable(orch._transformers[1])
