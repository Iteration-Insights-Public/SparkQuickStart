"""A real load under ``runtime_context.materialize_strategy``: "cache" against "delta".

Proves the three claims that matter, end to end on a local Delta target: the cache mode
produces the same target rows as the Delta round trip, writes no scratch table, and leaves
nothing cached once ``run()`` has returned; and on serverless "cache" falls back to the scratch
table. Skipped unless this session can round-trip a Delta table.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from pyspark.sql import DataFrame

from spark_quick_start.adapters.local_adapter import LocalAdapter

_VIEW = "strategy_view"


@pytest.fixture(autouse=True)
def _require_working_delta(spark, tmp_path_factory):
    pytest.importorskip("delta", reason="delta-spark not installed")
    probe = str(tmp_path_factory.mktemp("delta_probe") / "t")
    try:
        spark.range(1).write.format("delta").mode("overwrite").save(probe)
    except Exception as exc:  # pragma: no cover — environment-dependent
        pytest.skip(f"Delta not usable in this Spark session: {type(exc).__name__}")


def _context(root: Path, **runtime: object) -> str:
    return json.dumps({
        "global_context": {"id": "cust_1", "entity_name": "Customer", "source_system": "CRM",
                           "target_table_path": f"{root}/silver/customer",
                           "staging_table_path": f"{root}/staging"},
        "scd_context": {"business_key_column": "id"},
        "runtime_context": {"correlation_id": "c1", "pipeline_name": "p", "application_name": "a",
                            **runtime},
        "logging_context": {"logging_level": "INFO", "log_file_name": "n", "log_file_dir": f"{root}/logs"},
        "dqd_context": {"source_kind": "view", "source_identifier": _VIEW,
                        "selection": [{"name": "id"}, {"name": "val"}]},
    })


def _run(spark, root: Path, val: str, **runtime: object):
    from spark_quick_start.orchestration import (
        CandidateGuardTransformer,
        DQDTransformer,
        LoadOrchestrator,
        NativeSCDMergeTransformer,
        SCDChangeDetectorTransformer,
        SCDTargetTableTransformer,
    )
    from spark_quick_start.orchestration.transformers import (
        MaterializeFrameTransformer,
        MaterializeTransformer,
    )

    spark.createDataFrame([(1, val), (2, "b")], "id int, val string").createOrReplaceTempView(_VIEW)
    try:
        return (
            LoadOrchestrator(_context(root, **runtime), spark, adapter=LocalAdapter())
            .add(DQDTransformer())
            .add(SCDTargetTableTransformer())
            .add(MaterializeFrameTransformer())
            .add(SCDChangeDetectorTransformer())
            .add(MaterializeTransformer())
            .add(CandidateGuardTransformer())
            .add(NativeSCDMergeTransformer())
            .run()
        )
    finally:
        spark.catalog.dropTempView(_VIEW)


def _business_rows(spark, root: Path) -> list[tuple]:
    rows = spark.read.format("delta").load(f"{root}/silver/customer")
    return sorted(tuple(r) for r in rows.select("id", "val", "scdVersion").collect())


def _scratch(root: Path) -> list[str]:
    return sorted(p.name for p in root.iterdir() if p.name.startswith("staging_"))


@pytest.fixture
def persisted(monkeypatch):
    """Every frame passed to DataFrame.persist during the test."""
    seen: list[DataFrame] = []
    real = DataFrame.persist

    def _persist(self, *args, **kwargs):
        seen.append(self)
        return real(self, *args, **kwargs)

    monkeypatch.setattr(DataFrame, "persist", _persist)
    return seen


def test_cache_strategy_matches_delta_writes_no_scratch_and_releases_every_frame(
    spark, tmp_path: Path, persisted,
):
    delta_root, cache_root = tmp_path / "delta", tmp_path / "cache"
    delta_root.mkdir()
    cache_root.mkdir()
    for root, extra in ((delta_root, {}), (cache_root, {"materialize_strategy": "cache"})):
        for val in ("a", "a-changed"):  # second run: a changed row, so the target frame is read
            result = _run(spark, root, val, **extra)
            assert result.status == "success", result.error_details

    assert _business_rows(spark, cache_root) == _business_rows(spark, delta_root)
    assert _scratch(delta_root) != [], "the delta run is expected to write its scratch tables"
    assert _scratch(cache_root) == [], "cache mode must write no scratch table"
    assert persisted, "cache mode was expected to persist at least one frame"
    assert all(not f.is_cached for f in persisted), "every cached frame is released after run()"


def test_cache_strategy_on_serverless_falls_back_to_the_scratch_table(
    spark, tmp_path: Path, persisted,
):
    result = _run(spark, tmp_path, "a", materialize_strategy="cache", serverless=True)
    assert result.status == "success", result.error_details
    assert persisted == []
    assert _scratch(tmp_path) == ["staging_cust_1_4_materialize"]



def test_cache_mode_is_not_a_snapshot_of_the_target_after_the_write(spark, tmp_path: Path):
    """Measured 2026-10-05, local Delta: after the MERGE a cached frame is recomputed.

    The cached candidates have lineage over the target table, and the write invalidates and
    recomputes cached plans that read it. So every count taken before the write agrees between
    the modes, but the writer's ``row_count_out`` (a read of ``state.df`` after the MERGE) is the
    candidates re-evaluated against the merged table: 0 in cache mode, 1 in delta mode, where the
    scratch table is a snapshot. Pins the behaviour so the documentation's claim stays true.
    """
    counts = {}
    for mode, extra in (("delta", {}), ("cache", {"materialize_strategy": "cache"})):
        root = tmp_path / mode
        root.mkdir()
        for val in ("a", "a-changed"):
            result = _run(spark, root, val, capture_row_counts=True, **extra)
            assert result.status == "success", result.error_details
        counts[mode] = {s.stage: (s.row_count_in, s.row_count_out) for s in result.stages}

    writer = "NativeSCDMergeTransformer"
    before_write = [k for k in counts["delta"] if k != writer]
    assert {k: counts["cache"][k] for k in before_write} == {k: counts["delta"][k] for k in before_write}
    assert counts["delta"][writer] == (1, 1)
    assert counts["cache"][writer] == (1, 0)
