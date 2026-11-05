"""F134 and F154 end to end: the staged-key guard refuses bad keys before anything is written.

Each case is the entry's own reproduction, run through a real orchestrator, a DQD source, the
SCD steps and a local Delta target. Requires a working Delta install.
"""
from __future__ import annotations

import json

import pytest

from spark_quick_start.adapters.local_adapter import LocalAdapter
from spark_quick_start.orchestration import (
    CandidateGuardTransformer,
    DQDTransformer,
    LoadOrchestrator,
    NativeSCDMergeTransformer,
    SCDChangeDetectorTransformer,
    SCDTargetTableTransformer,
)
from spark_quick_start.orchestration.transformers import StagedKeyGuardTransformer


@pytest.fixture(autouse=True)
def _require_working_delta(spark, tmp_path_factory):
    """Skip unless this session can round-trip a Delta table; see test_dqd_load."""
    pytest.importorskip("delta", reason="delta-spark not installed")
    probe = str(tmp_path_factory.mktemp("delta_probe") / "t")
    try:
        spark.range(1).write.format("delta").mode("overwrite").save(probe)
    except Exception as exc:  # pragma: no cover — environment-dependent
        pytest.skip(f"Delta not usable in this Spark session: {type(exc).__name__}")


def _context(root):
    return json.dumps({
        "global_context": {"id": "ent_1", "entity_name": "Customer", "source_system": "CRM",
                           "target_table_path": f"{root}/silver/customer", "staging_table_path": f"{root}/unused"},
        "scd_context": {"business_key_column": "id"},
        "runtime_context": {"correlation_id": "guard-1", "pipeline_name": "customer", "application_name": "crm"},
        "logging_context": {"logging_level": "INFO", "log_file_name": "customer", "log_file_dir": f"{root}/logs"},
        "dqd_context": {"source_kind": "delta", "source_identifier": f"{root}/bronze/customers",
                        "selection": [{"name": "id"}, {"name": "name"}]},
    })


def _load(spark, root, rows):
    spark.createDataFrame(rows, "id string, name string").write.format("delta").mode("overwrite") \
        .save(f"{root}/bronze/customers")
    return (LoadOrchestrator(_context(root), spark, adapter=LocalAdapter())
            .add(DQDTransformer()).add(StagedKeyGuardTransformer()).add(SCDTargetTableTransformer())
            .add(SCDChangeDetectorTransformer()).add(CandidateGuardTransformer())
            .add(NativeSCDMergeTransformer())).run()


def _current(spark, root):
    target = spark.read.format("delta").load(f"{root}/silver/customer")
    return sorted((r.id, r.name, r.scdVersion) for r in target.filter("scdCurrentFlag = 'Y'").collect())


def test_f134_a_duplicate_whose_one_copy_equals_the_target_is_refused_and_nothing_is_written(spark, tmp_path):
    first = _load(spark, tmp_path, [("1", "Ada")])
    assert first.status == "success", first.error_details

    second = _load(spark, tmp_path, [("1", "Ada"), ("1", "Ada L.")])

    assert second.status == "error"
    assert "staged more than once" in second.error_details["error_message"]
    assert _current(spark, tmp_path) == [("1", "Ada", 1)]


def test_f154_a_null_business_key_is_refused_before_the_first_write(spark, tmp_path):
    result = _load(spark, tmp_path, [("1", "a"), (None, "b")])

    assert result.status == "error"
    assert "NULL key" in result.error_details["error_message"]
    assert not (tmp_path / "silver" / "customer").exists()
