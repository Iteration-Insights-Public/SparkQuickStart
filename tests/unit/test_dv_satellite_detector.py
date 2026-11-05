"""F161 — DVSatelliteChangeDetectorTransformer, one test per row of the design's semantics table.

The detector classifies staged rows against each key's latest satellite row (F160's target
frame) on the pair (hash diff, deleted flag), compared null-safely. A delete, signalled by the
source or found absent under `Full`, carries the latest row's attributes and hash diff forward,
so a key that reappears unchanged is seen only by its deleted flag flipping.
"""
from __future__ import annotations

import hashlib
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from conftest import is_serverless_session
from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter
from spark_quick_start.orchestration.models import (
    GlobalContext,
    LoggingContext,
    RuntimeContext,
    SCDContext,
    StagingContext,
)
from spark_quick_start.orchestration.transformers import DVSatelliteChangeDetectorTransformer
from spark_quick_start.orchestration.transformers._constants import CHANGE_TYPE
from spark_quick_start.orchestration.transformers.base import LoadContext
from spark_quick_start.orchestration.transformers.state import PipelineState

_STAGED = "HashKey string, City string, Name string, Op string"
_LATEST = ("HashKey string, City string, Name string, HashDiff string, IsDeleted string, "
           "LoadTS timestamp, RecordSource string, CorrelationId string")
_T = datetime(2026, 9, 1)


def _diff(*values: str | None) -> str:
    """The diff `hash_hex` computes: the values joined by `||`, NULL as `<NULL>`, sha256 hex."""
    text = "||".join("<NULL>" if v is None else v for v in values)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _ctx(style="dv_satellite", load_type="Incremental", **scd) -> LoadContext:
    return LoadContext(
        global_ctx=GlobalContext({"id": "ent_1", "entity_name": "Customer", "source_system": "CRM",
                                  "target_table_path": "/vault/customer_crm_s", "staging_table_path": "/s"}),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext({"business_key_column": "HashKey", "scd_type": style, "load_type": load_type,
                            "source_deleted_flag_column": "Op", **scd}),
        runtime_ctx=RuntimeContext({"correlation_id": "run-2", "pipeline_name": "p", "application_name": "a",
                                    "capture_partitions": False, "serverless": is_serverless_session()}),
        logging_ctx=LoggingContext({}),
    )


def _latest(spark, *rows):
    """Latest satellite rows: (key, city, name, deleted), the diff computed as the writer would."""
    return spark.createDataFrame(
        [(k, c, n, _diff(c, n), d, _T, "CRM", "run-1") for k, c, n, d in rows], _LATEST)


def _detect(spark, staged_rows, target, load_type="Incremental", **scd):
    staged = spark.createDataFrame(staged_rows, _STAGED)
    step = DVSatelliteChangeDetectorTransformer()
    step._adapter, step._spark = MockPlatformAdapter(), spark
    # `target=None` is the missing satellite: a target step states it absent (F28).
    state = PipelineState(df=staged).with_target(target)
    out = step(state, _ctx(load_type=load_type, **scd)).df
    return {r.HashKey: r for r in out.collect()}, out


# ── the semantics table ──────────────────────────────────────────────────────

def test_a_key_absent_from_the_satellite_is_new(spark):
    rows, _ = _detect(spark, [("h1", "Oslo", "Ada", None)], _latest(spark, ("h9", "Rome", "Old", "N")))
    assert (rows["h1"][CHANGE_TYPE], rows["h1"].IsDeleted, rows["h1"].HashDiff) == ("new", "N", _diff("Oslo", "Ada"))


def test_a_changed_diff_is_changed(spark):
    rows, _ = _detect(spark, [("h1", "Oslo", "Ada L.", None)], _latest(spark, ("h1", "Oslo", "Ada", "N")))
    assert (rows["h1"][CHANGE_TYPE], rows["h1"].IsDeleted, rows["h1"].Name) == ("changed", "N", "Ada L.")


def test_an_equal_diff_adds_nothing(spark):
    rows, _ = _detect(spark, [("h1", "Oslo", "Ada", None)], _latest(spark, ("h1", "Oslo", "Ada", "N")))
    assert rows == {}


def test_a_source_delete_of_a_live_key_is_deleted_carrying_the_latest_attributes(spark):
    """The staged delete row is blank, as a CDC delete often is; the satellite keeps what it knew."""
    rows, _ = _detect(spark, [("h1", None, None, "Y")], _latest(spark, ("h1", "Oslo", "Ada", "N")))
    r = rows["h1"]
    assert (r[CHANGE_TYPE], r.IsDeleted, r.City, r.Name, r.HashDiff) == ("deleted", "Y", "Oslo", "Ada", _diff("Oslo", "Ada"))


def test_a_source_delete_of_a_deleted_key_adds_nothing(spark):
    """The SCD detector does the same since F142; it once deleted again on every load."""
    rows, _ = _detect(spark, [("h1", None, None, "Y")], _latest(spark, ("h1", "Oslo", "Ada", "Y")))
    assert rows == {}


def test_a_deleted_key_that_reappears_unchanged_is_changed(spark):
    rows, _ = _detect(spark, [("h1", "Oslo", "Ada", None)], _latest(spark, ("h1", "Oslo", "Ada", "Y")))
    assert (rows["h1"][CHANGE_TYPE], rows["h1"].IsDeleted) == ("changed", "N")


def test_a_live_key_absent_from_a_full_load_is_deleted_from_its_latest_row(spark):
    target = _latest(spark, ("h1", "Oslo", "Ada", "N"), ("h2", "Rome", "Grace", "N"), ("h3", "Kyiv", "Lin", "Y"))
    rows, _ = _detect(spark, [("h1", "Oslo", "Ada", None)], target, load_type="Full")
    assert sorted(rows) == ["h2"], "h3 is already deleted, h1 is unchanged"
    r = rows["h2"]
    assert (r[CHANGE_TYPE], r.IsDeleted, r.City, r.Name, r.HashDiff, r.RecordSource) == \
        ("deleted", "Y", "Rome", "Grace", _diff("Rome", "Grace"), "CRM")


def test_an_absent_key_is_not_deleted_by_an_incremental_load(spark):
    rows, _ = _detect(spark, [("h1", "Oslo", "Ada", None)], _latest(spark, ("h2", "Rome", "Grace", "N")))
    assert sorted(rows) == ["h1"], "h2 is absent from an Incremental batch, which says nothing about it"


# ── around the table ─────────────────────────────────────────────────────────

def test_a_source_delete_of_a_key_the_satellite_lacks_is_dropped(spark):
    rows, _ = _detect(spark, [("h1", None, None, "Y")], _latest(spark, ("h9", "Rome", "Old", "N")))
    assert rows == {}


def test_with_no_satellite_every_live_row_is_new_and_deletes_are_dropped(spark):
    rows, _ = _detect(spark, [("h1", "Oslo", "Ada", None), ("h2", None, None, "Y")], None)
    assert sorted(rows) == ["h1"] and rows["h1"][CHANGE_TYPE] == "new"


def test_an_empty_satellite_is_a_first_load_without_an_action_on_it(spark):
    """An empty frame joins to nothing; the detector never asks the target whether it is empty."""
    empty = _latest(spark).limit(0)
    rows, _ = _detect(spark, [("h1", "Oslo", "Ada", None)], empty)
    assert rows["h1"][CHANGE_TYPE] == "new"


def test_a_full_load_of_an_empty_source_over_a_populated_satellite_is_refused(spark):
    with pytest.raises(ValueError, match="Full load .*0 rows"):
        _detect(spark, [], _latest(spark, ("h1", "Oslo", "Ada", "N")), load_type="Full")


def test_the_output_carries_no_control_or_load_columns(spark):
    _, out = _detect(spark, [("h1", "Oslo", "Ada", None)], _latest(spark, ("h2", "Rome", "Grace", "N")),
                     load_type="Full")
    assert set(out.columns) == {"HashKey", "City", "Name", "HashDiff", "IsDeleted", "RecordSource", CHANGE_TYPE}


def test_the_diff_leaves_out_the_excluded_columns(spark):
    rows, _ = _detect(spark, [("h1", "Oslo", "Ada", None)], None, exclude_from_hash=["City"])
    assert rows["h1"].HashDiff == _diff("Ada")


def test_md5_is_used_when_named(spark):
    rows, _ = _detect(spark, [("h1", "Oslo", "Ada", None)], None, hash_algorithm="md5")
    assert rows["h1"].HashDiff == hashlib.md5(b"Oslo||Ada").hexdigest()


@pytest.mark.parametrize("column", ["HashDiff", "LoadTS", "LoadEndTS"])
def test_a_staged_column_the_load_writes_is_refused(spark, column):
    staged = spark.createDataFrame([("h1", "x")], f"HashKey string, {column} string")
    step = DVSatelliteChangeDetectorTransformer()
    step._adapter, step._spark = MockPlatformAdapter(), spark
    with pytest.raises(ValueError, match=column):
        step(PipelineState(df=staged, target_exists=False), _ctx(source_deleted_flag_column=None))


@pytest.mark.parametrize("style", ["dv_hub", "scd2"])
def test_another_style_is_refused_before_reading(style):
    frame = MagicMock()
    step = DVSatelliteChangeDetectorTransformer()
    step._adapter, step._spark = MockPlatformAdapter(), MagicMock()
    with pytest.raises(ValueError, match=rf"DVSatelliteChangeDetectorTransformer.*{style}.*'dv_satellite'"):
        step(PipelineState(df=frame, frames={}), _ctx(style))
    assert not frame.method_calls


# ── review findings, F161 ────────────────────────────────────────────────────

def test_a_full_load_of_an_empty_source_over_an_all_deleted_satellite_adds_nothing(spark):
    """Only live keys would be deleted; a satellite of deleted keys has none to lose."""
    rows, _ = _detect(spark, [], _latest(spark, ("h1", "Oslo", "Ada", "Y")), load_type="Full")
    assert rows == {}


def test_a_full_batch_deletes_a_signalled_key_and_an_absent_key_once_each(spark):
    target = _latest(spark, ("h1", "Oslo", "Ada", "N"), ("h2", "Rome", "Grace", "N"), ("h3", "Kyiv", "Lin", "N"))
    _, out = _detect(spark, [("h1", None, None, "Y"), ("h3", "Kyiv", "Lin", None)], target, load_type="Full")
    assert sorted((r.HashKey, r[CHANGE_TYPE]) for r in out.collect()) == [("h1", "deleted"), ("h2", "deleted")]


def test_under_source_system_from_source_a_delete_records_who_saw_it(spark):
    """A signalled delete takes the staged source; an absent key keeps its latest row's."""
    staged = spark.createDataFrame([("h1", None, None, "Y", "ERP"), ("h3", "Kyiv", "Lin", None, "ERP")],
                                   f"{_STAGED}, RecordSource string")
    target = _latest(spark, ("h1", "Oslo", "Ada", "N"), ("h2", "Rome", "Grace", "N"))
    step = DVSatelliteChangeDetectorTransformer()
    step._adapter, step._spark = MockPlatformAdapter(), spark
    ctx = _ctx(load_type="Full", source_system_from_source=True)
    rows = {r.HashKey: r for r in step(PipelineState(df=staged, frames={"target": target}), ctx).df.collect()}
    assert {k: (r[CHANGE_TYPE], r.RecordSource) for k, r in rows.items()} == \
        {"h1": ("deleted", "ERP"), "h2": ("deleted", "CRM"), "h3": ("new", "ERP")}


def test_a_staged_column_the_satellite_lacks_is_null_on_a_carried_delete(spark):
    staged = spark.createDataFrame([("h1", None, None, "Y", None)], f"{_STAGED}, Phone string")
    step = DVSatelliteChangeDetectorTransformer()
    step._adapter, step._spark = MockPlatformAdapter(), spark
    out = step(PipelineState(df=staged, frames={"target": _latest(spark, ("h1", "Oslo", "Ada", "N"))}), _ctx()).df
    r = out.first()
    assert (r[CHANGE_TYPE], r.City, r.Phone) == ("deleted", "Oslo", None)


def test_a_column_spelled_in_another_case_in_the_satellite_is_still_carried(spark):
    """Spark and Delta match names case-insensitively; so does the carry-forward."""
    target = _latest(spark, ("h1", "Oslo", "Ada", "N")).withColumnRenamed("City", "city")
    rows, _ = _detect(spark, [("h1", None, None, "Y")], target)
    assert rows["h1"].City == "Oslo"


def test_a_column_typed_differently_in_the_satellite_is_refused_naming_it(spark):
    """Left to the union, the runtime's coercion would decide; a Data Vault table never evolves implicitly."""
    from pyspark.sql import functions as F
    target = _latest(spark, ("h1", "Oslo", "Ada", "N")).withColumn("Name", F.lit(1))
    with pytest.raises(ValueError, match="Name"):
        _detect(spark, [("h1", "Oslo", "Ada", None)], target)


# ── The written deleted-flag vocabulary and the source's own (F106) ───────────

_VOCAB = {"deleted_flag_value": "D", "not_deleted_flag_value": "A", "source_deleted_flag_value": "X"}


def test_a_new_row_is_written_with_the_not_deleted_value(spark):
    rows, _ = _detect(spark, [("h1", "Oslo", "Ada", None)], None, **_VOCAB)
    assert (rows["h1"][CHANGE_TYPE], rows["h1"].IsDeleted) == ("new", "A")


def test_a_changed_row_is_written_with_the_not_deleted_value(spark):
    rows, _ = _detect(spark, [("h1", "Oslo", "Ada L.", None)], _latest(spark, ("h1", "Oslo", "Ada", "A")), **_VOCAB)
    assert (rows["h1"][CHANGE_TYPE], rows["h1"].IsDeleted) == ("changed", "A")


def test_an_equal_diff_against_a_not_deleted_target_adds_nothing(spark):
    rows, _ = _detect(spark, [("h1", "Oslo", "Ada", None)], _latest(spark, ("h1", "Oslo", "Ada", "A")), **_VOCAB)
    assert rows == {}


def test_a_source_delete_is_read_as_the_source_says_and_written_as_the_deleted_value(spark):
    rows, _ = _detect(spark, [("h1", None, None, "X")], _latest(spark, ("h1", "Oslo", "Ada", "A")), **_VOCAB)
    assert (rows["h1"][CHANGE_TYPE], rows["h1"].IsDeleted, rows["h1"].City) == ("deleted", "D", "Oslo")
    other, _ = _detect(spark, [("h1", None, None, "D")], _latest(spark, ("h1", "Oslo", "Ada", "A")), **_VOCAB)
    assert other.get("h1") is None or other["h1"][CHANGE_TYPE] != "deleted", "D is what is written, not what is read"


def test_a_source_value_defaults_to_the_deleted_value(spark):
    rows, _ = _detect(spark, [("h1", None, None, "D")], _latest(spark, ("h1", "Oslo", "Ada", "A")),
                      deleted_flag_value="D", not_deleted_flag_value="A")
    assert (rows["h1"][CHANGE_TYPE], rows["h1"].IsDeleted) == ("deleted", "D")


def test_a_key_the_satellite_holds_deleted_is_recognised_by_the_deleted_value(spark):
    already, _ = _detect(spark, [("h1", None, None, "X")], _latest(spark, ("h1", "Oslo", "Ada", "D")), **_VOCAB)
    assert already == {}
    back, _ = _detect(spark, [("h1", "Oslo", "Ada", None)], _latest(spark, ("h1", "Oslo", "Ada", "D")), **_VOCAB)
    assert (back["h1"][CHANGE_TYPE], back["h1"].IsDeleted) == ("changed", "A")


def test_a_full_load_deletes_an_absent_live_key_with_the_deleted_value(spark):
    target = _latest(spark, ("h1", "Oslo", "Ada", "A"), ("h2", "Rome", "Grace", "A"), ("h3", "Kyiv", "Lin", "D"))
    rows, _ = _detect(spark, [("h1", "Oslo", "Ada", None)], target, load_type="Full", **_VOCAB)
    assert sorted(rows) == ["h2"]
    assert (rows["h2"][CHANGE_TYPE], rows["h2"].IsDeleted) == ("deleted", "D")
