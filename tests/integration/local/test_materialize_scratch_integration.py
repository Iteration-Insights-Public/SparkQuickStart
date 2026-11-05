"""End-to-end proof that chained materialize steps don't destroy their own input.

Requires a real delta-spark install (``pip install spark_quick_start[local]``);
skipped otherwise, the same way TestLocalAdapterIntegration is.

The unit tests assert that two MaterializeTransformers derive different scratch
paths. These assert the thing that actually matters: that chaining them through
real Delta preserves the data. The failure mode being guarded is silent — a
drop-then-write against a table the input still reads returns an EMPTY result
with no exception — so a test that only checks "no error" would pass against
the bug.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import functions as F

from spark_quick_start.adapters.local_adapter import LocalAdapter


@pytest.fixture(autouse=True)
def _require_working_delta(spark, tmp_path_factory):
    """Skip unless this session can actually round-trip a Delta table.

    Deliberately a capability probe rather than an import check. Two separate
    things can be missing: the delta-spark Python package, and the Scala JARs
    it needs on the Spark classpath. Only the pair together makes a Delta write
    work, and no import check sees either — which is why this probe writes a table.

    Corrected 2026-09-26 (F30): this used to say conftest's heuristic reported a real
    install as *usable* with no JARs. It did the opposite — it read every real
    delta-spark 3.2.0 as a stub, because that package exposes no `__version__`, so the
    Delta configuration was never applied and this probe skipped on every run. conftest
    now supplies the JAR coordinate as well as the extension, and falls back to a
    session without Delta if that cannot be satisfied, so this probe is what decides
    whether Delta actually works here.
    """
    pytest.importorskip("delta", reason="delta-spark not installed")
    probe = str(tmp_path_factory.mktemp("delta_probe") / "t")
    try:
        spark.range(1).write.format("delta").mode("overwrite").save(probe)
    except Exception as exc:  # pragma: no cover — environment-dependent
        pytest.skip(f"Delta not usable in this Spark session: {type(exc).__name__}")


def _materialize_via_adapter(spark, adapter, df, path):
    from spark_quick_start.orchestration.transformers.materialize import _materialize
    return _materialize(spark, adapter, df, path)


class TestChainedMaterializeAgainstRealDelta:
    def test_two_materializes_on_distinct_paths_preserve_every_row(self, spark, tmp_path: Path):
        """The fixed arrangement: stage 3 writes scratch_3, stage 4 writes scratch_4."""
        adapter = LocalAdapter(base_path=str(tmp_path))
        source = spark.range(0, 500).withColumn("v", F.col("id") * 2)

        first = _materialize_via_adapter(
            spark, adapter, source, str(tmp_path / "E_materialize_scratch_3")
        )
        transformed = first.withColumn("v2", F.col("v") + 1)
        second = _materialize_via_adapter(
            spark, adapter, transformed, str(tmp_path / "E_materialize_scratch_4")
        )

        assert second.count() == 500
        assert second.agg(F.sum("v2")).collect()[0][0] == sum(i * 2 + 1 for i in range(500))

    def test_reusing_one_path_for_both_would_empty_the_table(self, spark, tmp_path: Path):
        """Documents *why* the stage index is in the path.

        This is the pre-fix arrangement, reproduced deliberately: materialize a
        frame, derive from it, then materialize back onto the same path. Delta
        raises nothing — it just yields zero rows. Pinned here so that if a
        future change ever collapses the two paths again, this test says
        exactly what the consequence is.
        """
        adapter = LocalAdapter(base_path=str(tmp_path))
        shared = str(tmp_path / "E_materialize_scratch")
        source = spark.range(0, 500).withColumn("v", F.col("id") * 2)

        first = _materialize_via_adapter(spark, adapter, source, shared)
        transformed = first.withColumn("v2", F.col("v") + 1)
        second = _materialize_via_adapter(spark, adapter, transformed, shared)

        assert second.count() == 0, (
            "expected the known silent-empty-write; if this now returns 500 the "
            "hazard is gone and the stage-index path scheme can be revisited"
        )

    def test_scratch_table_keeps_no_history_across_runs(self, spark, tmp_path: Path):
        """Dropping, not overwriting, is what stops the scratch path growing.

        After three materializes the Delta log must show a fresh table each
        time (version 0), not an accumulating version history.
        """
        from delta.tables import DeltaTable

        adapter = LocalAdapter(base_path=str(tmp_path))
        path = str(tmp_path / "E_materialize_scratch_0")

        for _ in range(3):
            _materialize_via_adapter(
                spark, adapter, spark.range(0, 100).withColumn("v", F.col("id")), path
            )

        versions = DeltaTable.forPath(spark, path).history().count()
        assert versions == 1, f"expected a freshly created table, got {versions} versions"


# ---------------------------------------------------------------------------
# F129: scratch names carry global_context.id, so no two steps and no two loads collide.
# ---------------------------------------------------------------------------

_VIEW = "scratch_name_view"


def _load_context(root: Path, *, load_id: str, target: str, eager: bool = False) -> str:
    import json

    context = {
        "global_context": {"id": load_id, "entity_name": "Customer", "source_system": "CRM",
                           "target_table_path": f"{root}/silver/{target}",
                           "staging_table_path": f"{root}/shared_staging"},
        "scd_context": {"business_key_column": "id"},
        "runtime_context": {"correlation_id": "c1", "pipeline_name": "p", "application_name": "a"},
        "logging_context": {"logging_level": "INFO", "log_file_name": "n", "log_file_dir": f"{root}/logs"},
        "dqd_context": {"source_kind": "view", "source_identifier": _VIEW,
                        "selection": [{"name": "id"}, {"name": "val"}]},
    }
    if eager:
        context["dqd_context"]["eager_materialize_source"] = True
    return json.dumps(context)


class TestScratchNamesCarryTheLoadId:
    def test_two_materialize_steps_in_one_pipeline_keep_a_changed_row(self, spark, tmp_path: Path):
        """F129's backlog scenario: a frame materialize and a primary materialize in one pipeline.

        Both used to be given one path and the second dropped the table the target frame still
        read, so a changed row was lost and reported as written. Their names now differ.
        """
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

        def run(val: str):
            spark.createDataFrame([(1, val), (2, "b")], "id int, val string").createOrReplaceTempView(_VIEW)
            return (
                LoadOrchestrator(_load_context(tmp_path, load_id="cust_1", target="customer"), spark,
                                 adapter=LocalAdapter())
                .add(DQDTransformer())
                .add(SCDTargetTableTransformer())
                .add(MaterializeFrameTransformer())
                .add(SCDChangeDetectorTransformer())
                .add(MaterializeTransformer())
                .add(CandidateGuardTransformer())
                .add(NativeSCDMergeTransformer())
                .run()
            )

        assert run("a").status == "success"
        second = run("a-changed")
        assert second.status == "success", second.error_details
        spark.catalog.dropTempView(_VIEW)

        rows = spark.read.format("delta").load(f"{tmp_path}/silver/customer")
        assert rows.filter("id = 1").count() == 2, "the changed row must be versioned, not lost"
        assert rows.filter("id = 1 AND val = 'a-changed'").count() == 1
        scratch = sorted(p.name for p in tmp_path.iterdir() if p.name.startswith("shared_staging"))
        assert scratch == ["shared_staging_cust_1_2_materialize_target", "shared_staging_cust_1_4_materialize"]

    def test_two_loads_sharing_a_staging_path_write_different_dqd_scratch_tables(self, spark, tmp_path: Path):
        from spark_quick_start.orchestration import (
            DQDTransformer,
            LoadOrchestrator,
            NativeSCDMergeTransformer,
            SCDChangeDetectorTransformer,
            SCDTargetTableTransformer,
        )

        spark.createDataFrame([(1, "a")], "id int, val string").createOrReplaceTempView(_VIEW)
        for load_id in ("load_a", "load_b"):
            result = (
                LoadOrchestrator(_load_context(tmp_path, load_id=load_id, target=load_id, eager=True), spark,
                                 adapter=LocalAdapter())
                .add(DQDTransformer())
                .add(SCDTargetTableTransformer())
                .add(SCDChangeDetectorTransformer())
                .add(NativeSCDMergeTransformer())
                .run()
            )
            assert result.status == "success", result.error_details
        spark.catalog.dropTempView(_VIEW)

        scratch = sorted(p.name for p in tmp_path.iterdir() if p.name.startswith("shared_staging"))
        assert scratch == ["shared_staging_load_a_0_dqd", "shared_staging_load_b_0_dqd"]
