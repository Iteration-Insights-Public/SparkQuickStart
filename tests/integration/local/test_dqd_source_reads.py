"""How many times a DQD load reads its source (F143).

The source is a temp view whose key (and so its watermark column) comes from a Python UDF that adds 1 to a Spark
accumulator for every row it evaluates, so every physical scan of the source adds its row count.
The UDF makes the business key, which every query over the source needs, so no query can prune
it, and it is declared non-deterministic so the optimizer evaluates it once per scan instead of
copying it into a pushed-down filter (measured: left deterministic, a watermark filter doubled the
count). The source kind is `view` (`PlatformAdapter.read_table` is `spark.table`). The loads are
first loads, or later ones over the existing target, as each case says.

Delta's `spark.databricks.delta.merge.materializeSource` is pinned to "none". A Python UDF is not
deterministic to Delta, and under its default ("auto") the merge then materializes the source
itself and reads it once, so the counts would understate what the merge does with a deterministic
source: measured over the same cases, "auto" gave one fewer scan on a first load with hwm_context and two fewer on a later load. "none"
is the merge's own plan, which is what a source Delta does not materialize sees; whether Delta's
"auto" materializes the guide's own Delta source is not proven here. The counter cannot be made
deterministic: left deterministic, the optimizer copies it into a pushed-down filter.
"""
from __future__ import annotations

import json

import pytest
from pyspark.sql import functions as F

from spark_quick_start.adapters.local_adapter import LocalAdapter
from spark_quick_start.orchestration import (
    CandidateGuardTransformer,
    DQDTransformer,
    LoadOrchestrator,
    NativeSCDMergeTransformer,
    SCDChangeDetectorTransformer,
    SCDTargetTableTransformer,
)
from spark_quick_start.orchestration.transformers.materialize import MaterializeTransformer

_ROWS = 50
_VIEW = "dqd_source_reads_view"


@pytest.fixture(autouse=True)
def _require_working_delta(spark, tmp_path_factory):
    pytest.importorskip("delta", reason="delta-spark not installed")
    probe = str(tmp_path_factory.mktemp("delta_probe") / "t")
    try:
        spark.range(1).write.format("delta").mode("overwrite").save(probe)
    except Exception as exc:  # pragma: no cover — environment-dependent
        pytest.skip(f"Delta not usable in this Spark session: {type(exc).__name__}")


def _context(root, *, with_hwm, eager=False, hwm_value="0"):
    context = {
        "global_context": {"id": "ent_1", "entity_name": "Reads", "source_system": "T",
                           "target_table_path": f"{root}/silver/reads", "staging_table_path": f"{root}/scratch"},
        "scd_context": {"business_key_column": "id"},
        "runtime_context": {"correlation_id": "reads-1", "pipeline_name": "reads", "application_name": "t"},
        "logging_context": {"logging_level": "INFO", "log_file_name": "reads", "log_file_dir": f"{root}/logs"},
        "dqd_context": {"source_kind": "view", "source_identifier": _VIEW,
                        "selection": [{"name": "id"}, {"name": "seq"}, {"name": "val"}]},
    }
    if with_hwm:
        context["hwm_context"] = {"hwm_column_name": "seq", "hwm_value": hwm_value}
    if eager:
        context["dqd_context"]["eager_materialize_source"] = True
    return json.dumps(context)


def _publish_source(spark, value):
    """Register the counting view, every row's `val` set to `value`; return the accumulator."""
    accumulator = spark.sparkContext.accumulator(0)

    def count_row(key):
        accumulator.add(1)
        return key

    counted = F.udf(count_row, "long").asNondeterministic()
    spark.range(1, _ROWS + 1).select(counted("id").alias("id")).withColumn("seq", F.col("id")) \
        .withColumn("val", F.lit(value)).createOrReplaceTempView(_VIEW)
    return accumulator


def _pipeline(spark, root, *, with_hwm, guard, materialize, eager=False, hwm_value="0"):
    pipeline = LoadOrchestrator(_context(root, with_hwm=with_hwm, eager=eager, hwm_value=hwm_value), spark, adapter=LocalAdapter()).add(DQDTransformer())
    if materialize:
        pipeline = pipeline.add(MaterializeTransformer())
    pipeline = pipeline.add(SCDTargetTableTransformer()).add(SCDChangeDetectorTransformer())
    if guard:
        pipeline = pipeline.add(CandidateGuardTransformer())
    return pipeline.add(NativeSCDMergeTransformer())


def _source_reads(spark, root, *, with_hwm, guard=True, materialize=False, later=False, eager=False):
    """Run a load of the view and return (result, physical scans of the source's rows).

    A later load runs a first one without counting, then changes every row's `val` and runs the
    shape under test over the existing target: every key is changed, so the merge closes and
    inserts versions.
    """
    if later:
        _publish_source(spark, 1)
        first = _pipeline(spark, root, with_hwm=False, guard=guard, materialize=materialize, eager=eager).run()
        assert first.status == "success", first.error_details
    accumulator = _publish_source(spark, 2 if later else 1)
    result = _pipeline(spark, root, with_hwm=with_hwm, guard=guard, materialize=materialize, eager=eager).run()
    spark.catalog.dropTempView(_VIEW)
    return result, accumulator.value


# (with_hwm, guard, materialize) -> scans of the source, on a first load and on a later one
# (every key changed, the target present, SCD2). Measured with the setting pinned below. Not
# explained by the DQD alone: the merge's own passes are included (its `needs_closeout.isEmpty()`
# probe and the MERGE's join), and hwm_context's first-load rows carry one pass more in the merge
# step than the aggregation accounts for, which was not traced. `capture_partitions` was measured
# to change nothing. MaterializeTransformer after the DQD leaves one read of the source for its
# write, plus the aggregation under hwm_context; every later action reads the scratch table.
_SCANS = {
    (False, False, False): (1, 4),
    (True, False, False): (3, 5),
    (False, True, False): (2, 5),
    (True, True, False): (4, 6),
    (False, True, True): (1, 1),
    (True, True, True): (2, 2),
}
_IDS = {
    (False, False, False): "write-only", (True, False, False): "hwm+write", (False, True, False): "guard+write",
    (True, True, False): "hwm+guard+write", (False, True, True): "materialize-no-hwm",
    (True, True, True): "materialize-with-hwm",
}


@pytest.fixture
def pinned_merge_source(spark):
    """Delta's `materializeSource` set to "none", restored after (see the module docstring)."""
    key = "spark.databricks.delta.merge.materializeSource"
    previous = spark.conf.get(key, None)
    spark.conf.set(key, "none")
    yield
    if previous is None:
        spark.conf.unset(key)
    else:
        spark.conf.set(key, previous)


@pytest.mark.parametrize("later", [False, True], ids=["first-load", "later-load"])
@pytest.mark.parametrize("shape", list(_SCANS), ids=list(_IDS.values()))
def test_the_scans_of_a_dqd_source(spark, tmp_path, pinned_merge_source, shape, later):
    with_hwm, guard, materialize = shape
    result, reads = _source_reads(spark, tmp_path, with_hwm=with_hwm, guard=guard, materialize=materialize,
                                  later=later)
    assert result.status == "success", result.error_details
    assert reads == _SCANS[shape][int(later)] * _ROWS


# `dqd_context.eager_materialize_source` (F201): the filtered, watermarked source is written once to
# a scratch table and the aggregation and every later action read that, so the source is read once
# whatever the pipeline after the DQD does.
@pytest.mark.parametrize("later", [False, True], ids=["first-load", "later-load"])
@pytest.mark.parametrize("guard", [False, True], ids=["no-guard", "guard"])
@pytest.mark.parametrize("with_hwm", [False, True], ids=["no-hwm", "hwm"])
def test_the_eager_flag_reads_the_source_once(spark, tmp_path, pinned_merge_source, with_hwm, guard, later):
    result, reads = _source_reads(spark, tmp_path, with_hwm=with_hwm, guard=guard, later=later, eager=True)
    assert result.status == "success", result.error_details
    assert reads == _ROWS


@pytest.mark.parametrize("later", [False, True], ids=["first-load", "later-load"])
def test_the_eager_flag_changes_neither_the_watermark_nor_the_count(spark, tmp_path_factory, pinned_merge_source,
                                                                     later):
    plain, _ = _source_reads(spark, tmp_path_factory.mktemp("plain"), with_hwm=True, later=later)
    eager, _ = _source_reads(spark, tmp_path_factory.mktemp("eager"), with_hwm=True, later=later, eager=True)
    assert plain.status == eager.status == "success", (plain.error_details, eager.error_details)
    assert eager.metrics["hwm_value_after"] == plain.metrics["hwm_value_after"] == str(_ROWS)
    assert eager.metrics["source_count"] == plain.metrics["source_count"] == _ROWS
    assert eager.metrics["inserted_count"] == plain.metrics["inserted_count"]


def test_a_malformed_watermark_still_fails_before_a_write_with_the_eager_flag(spark, tmp_path):
    accumulator = _publish_source(spark, 1)
    try:
        result = _pipeline(spark, tmp_path, with_hwm=True, guard=True, materialize=False, eager=True,
                           hwm_value="not-a-number").run()
    finally:
        spark.catalog.dropTempView(_VIEW)
    assert result.status == "error"
    assert "cannot be read as the type of hwm_column_name" in result.error_details["error_message"]
    assert not (tmp_path / "silver").exists()
    # The failure comes from the aggregation over the scratch table, after the scratch write. The
    # source was not scanned at all (measured: 0): `seq > NULL`, from the value that does not cast,
    # is folded to an empty filter, so the write held no rows; the scratch table exists, empty.
    assert accumulator.value == 0
    assert (tmp_path / "scratch_ent_1_0_dqd").exists()


def test_a_source_column_delta_refuses_by_name_loads_with_the_eager_flag(spark, tmp_path, pinned_merge_source):
    """The scratch table holds the raw source columns, so a name Delta refuses must not reach it."""
    spark.range(1, 4).select("id", F.lit("x").alias("Order Date"), F.lit(1).alias("a,b")) \
        .createOrReplaceTempView(_VIEW)
    context = json.loads(_context(tmp_path, with_hwm=False, eager=True))
    context["dqd_context"]["selection"] = [
        {"name": "id"}, {"name": "order_date", "expression": {"column": "Order Date"}},
        {"name": "ab", "expression": {"column": "a,b"}}]
    try:
        result = LoadOrchestrator(json.dumps(context), spark, adapter=LocalAdapter()).add(DQDTransformer()) \
            .add(SCDTargetTableTransformer()).add(SCDChangeDetectorTransformer()) \
            .add(NativeSCDMergeTransformer()).run()
    finally:
        spark.catalog.dropTempView(_VIEW)
    assert result.status == "success", result.error_details
    assert result.metrics["inserted_count"] == 3
    loaded = spark.read.format("delta").load(f"{tmp_path}/silver/reads")
    assert {"order_date", "ab"} <= set(loaded.columns)
    assert {r.order_date for r in loaded.collect()} == {"x"}
