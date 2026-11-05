"""Tests for StagingTransformer's .source_count/.target_count capture.

execute_staging_phases/validate_and_merge_staging_tables/load_staged_dataframe/
NotebookExecutor are mocked at their import points inside staging.py — this
tests StagingTransformer's own wiring, not the underlying staging pipeline
(covered by test_staging_pipeline.py).
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter
from spark_quick_start.orchestration.models import (
    GlobalContext,
    LoggingContext,
    RuntimeContext,
    SCDContext,
    StagingContext,
)
from spark_quick_start.orchestration.transformers.base import LoadContext, SourceTransformer
from spark_quick_start.orchestration.transformers.staging import StagingTransformer

_MODULE = "spark_quick_start.orchestration.transformers.staging"


def _ctx() -> LoadContext:
    return LoadContext(
        global_ctx=GlobalContext({
            "id": "ent_1",
            "entity_name": "E", "source_system": "S",
            "target_table_path": "/t", "staging_table_path": "/s",
        }),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext({"business_key_column": "id"}),
        runtime_ctx=RuntimeContext({"correlation_id": "abc", "pipeline_name": "p", "application_name": "a"}),
        logging_ctx=LoggingContext({}),
    )


def test_is_source_transformer():
    assert issubclass(StagingTransformer, SourceTransformer)


def test_source_count_and_target_count_default_to_none():
    t = StagingTransformer()
    assert t.source_count is None
    assert t.target_count is None


def test_produce_captures_counts_from_execute_staging_phases():
    """The (paths, source_count, target_count) tuple execute_staging_phases
    returns must end up on self.source_count/self.target_count."""
    ctx_manager = MagicMock()
    ctx_manager.global_ctx.staging_table_schema = {"type": "struct", "fields": []}
    ctx_manager.global_ctx.staging_table_path = "/s"

    mock_df = MagicMock()

    with patch(f"{_MODULE}.NotebookExecutor"), \
         patch(f"{_MODULE}.validate_and_merge_staging_tables"), \
         patch(f"{_MODULE}.execute_staging_phases", return_value=(["/s__A", "/s__B"], 226886, 226886)), \
         patch(f"{_MODULE}.load_staged_dataframe", return_value=mock_df):
        t = StagingTransformer()
        t._ctx_manager = ctx_manager
        t._spark = MagicMock()
        t._adapter = MockPlatformAdapter()
        t._logger = MagicMock()
        state = t._produce(_ctx())

    assert t.source_count == 226886
    assert t.target_count == 226886
    assert state.df is mock_df
    assert not mock_df.method_calls


def test_produce_merges_staging_tables_when_schema_is_configured():
    """With a staging_table_schema, the per-notebook tables are validated and merged.

    The merge is what turns N per-notebook staging tables into the single table
    load_staged_dataframe then reads. A context without the schema is refused at
    validation (F107), so the step has no skip branch.
    """
    schema = {"type": "struct", "fields": []}
    ctx_manager = MagicMock()
    ctx_manager.global_ctx.staging_table_schema = schema
    ctx_manager.global_ctx.staging_table_path = "/s"

    spark, adapter, logger = MagicMock(), MockPlatformAdapter(), MagicMock()

    with patch(f"{_MODULE}.NotebookExecutor"), \
         patch(f"{_MODULE}.execute_staging_phases", return_value=(["/s__A", "/s__B"], 10, 10)), \
         patch(f"{_MODULE}.validate_and_merge_staging_tables") as mock_merge, \
         patch(f"{_MODULE}.load_staged_dataframe", return_value=MagicMock()):
        t = StagingTransformer()
        t._ctx_manager = ctx_manager
        t._spark = spark
        t._adapter = adapter
        t._logger = logger
        t._produce(_ctx())

    mock_merge.assert_called_once()
    args = mock_merge.call_args[0]
    assert args[0] is spark
    assert args[1] == ["/s__A", "/s__B"]
    assert args[2] is schema
    assert args[3] == "/s"
