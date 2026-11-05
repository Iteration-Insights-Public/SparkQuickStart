"""LoadOrchestrator reports a writer's counts whether or not its watermark read worked (F117).

The real `NativeSCD*` writers run inside the real `_run_pipeline`; the Delta helpers they call
are patched, as in the unit suite, because the question is the harvest, not the MERGE.
"""
from __future__ import annotations

import json
from unittest.mock import patch

from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter
from spark_quick_start.orchestration import LoadOrchestrator
from spark_quick_start.orchestration.transformers.base import SourceTransformer
from spark_quick_start.orchestration.transformers.conditional import ConditionalTransformer
from spark_quick_start.orchestration.transformers.native_scd import (
    NativeSCDAppendOnlyTransformer,
    NativeSCDMergeTransformer,
)
from spark_quick_start.orchestration.transformers.state import PipelineState

_NATIVE = "spark_quick_start.orchestration.transformers.native_scd"


def _context_json() -> str:
    return json.dumps({
        "global_context": {
            "id": "customer", "entity_name": "Customer", "source_system": "CRM",
            "target_table_path": "/tmp/t", "staging_table_path": "/tmp/s",
            "staging_table_schema": {"type": "struct", "fields": []},
        },
        "staging_context": {"stages": [{"nb_name": "NB"}]},
        "scd_context": {"business_key_column": "id", "scd_type": "scd2"},
        "runtime_context": {"correlation_id": "c", "pipeline_name": "p", "application_name": "a"},
        "logging_context": {"logging_level": "INFO", "log_file_name": "x", "log_file_dir": "/tmp/logs"},
    })


def _candidates(spark):
    return spark.createDataFrame(
        [("1", "Alice", 1, 1, None, "rk", "N", "CRM", "new")],
        "id string, name string, scdHash long, scdVersion long, _prev_scdVersion long, "
        "scdRowKey string, scdDeletedFlag string, scdSourceSystem string, _change_type string",
    )


def _run(spark, writers, hwm_side_effect):
    class Source(SourceTransformer):
        def _produce(self, ctx):
            return PipelineState(df=_candidates(spark))

    orch = LoadOrchestrator(
        load_context_json=_context_json(), spark=spark,
        adapter=MockPlatformAdapter(existing_tables={"/tmp/t"}),
    )
    orch._transformers = [Source(), *writers]
    with patch(f"{_NATIVE}.execute_scd_merge", return_value=0), \
         patch(f"{_NATIVE}.capture_merge_statistics", return_value=(7, 3, 0)), \
         patch(f"{_NATIVE}.get_high_watermark", side_effect=hwm_side_effect), \
         patch("spark_quick_start.orchestration.load_pipeline.log_runtime"):
        return orch.run()


def test_a_failed_watermark_read_still_reports_the_merge_counts(spark):
    writer = NativeSCDMergeTransformer()

    result = _run(spark, [writer], RuntimeError("metastore down"))

    assert result.status == "success", result.error_details
    assert (result.metrics["inserted_count"], result.metrics["updated_count"]) == (7, 3)
    assert writer.hwm_after is None


def test_the_branch_that_ran_is_harvested_when_its_watermark_read_fails(spark):
    never_ran = ConditionalTransformer(predicate=False, transformer=NativeSCDAppendOnlyTransformer())
    ran = ConditionalTransformer(predicate=True, transformer=NativeSCDMergeTransformer())

    result = _run(spark, [never_ran, ran], RuntimeError("metastore down"))

    assert (result.metrics["inserted_count"], result.metrics["updated_count"]) == (7, 3)


def test_counts_are_reported_when_the_watermark_works(spark):
    result = _run(spark, [NativeSCDMergeTransformer()], lambda *a, **k: "2024-01-01")

    assert (result.metrics["inserted_count"], result.metrics["updated_count"]) == (7, 3)
