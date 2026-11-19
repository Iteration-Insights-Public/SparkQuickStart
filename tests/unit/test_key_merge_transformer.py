"""The landing writer's batch rules: one row per key, the latest winning, and its refusals (F223).

The dedup and the guards run on a local Spark session; nothing is written.
"""
from __future__ import annotations

from datetime import datetime

import pytest

from spark_quick_start.orchestration.load_pipeline import check_landing_batch, latest_per_key

_SCHEMA = "id string, modified_at timestamp, name string"
_T1, _T2 = datetime(2026, 1, 1, 10), datetime(2026, 1, 1, 11)


def _rows(spark, rows, schema=_SCHEMA):
    return spark.createDataFrame(rows, schema)


def test_the_latest_row_of_a_key_wins(spark):
    out = latest_per_key(_rows(spark, [("a", _T1, "old"), ("a", _T2, "new"), ("b", _T1, "b")]), "id", "modified_at")
    assert sorted((r.id, r.name) for r in out.collect()) == [("a", "new"), ("b", "b")]


def test_two_identical_rows_keep_one(spark):
    out = latest_per_key(_rows(spark, [("a", _T1, "x"), ("a", _T1, "x")]), "id", "modified_at")
    assert [(r.id, r.name) for r in out.collect()] == [("a", "x")]


def test_a_tie_between_different_rows_is_settled_the_same_way_every_time(spark):
    rows = [("a", _T1, name) for name in ("p", "q", "r", "s")]
    winners = {latest_per_key(_rows(spark, order).repartition(3), "id", "modified_at").collect()[0].name
               for order in (rows, rows[::-1], rows[1:] + rows[:1])}
    assert len(winners) == 1


def test_the_dedup_keeps_every_column(spark):
    out = latest_per_key(_rows(spark, [("a", _T1, "x")]), "id", "modified_at")
    assert out.columns == ["id", "modified_at", "name"]


def test_a_null_key_is_refused_with_its_count(spark):
    with pytest.raises(ValueError, match="2 row.*NULL.*id"):
        check_landing_batch(_rows(spark, [(None, _T1, "x"), (None, _T2, "y"), ("a", _T1, "z")]), "id", "modified_at",
                            check_nulls=True)


def test_a_null_ordering_value_is_refused_with_its_count(spark):
    with pytest.raises(ValueError, match="1 row.*NULL.*modified_at"):
        check_landing_batch(_rows(spark, [("a", None, "x")]), "id", "modified_at", check_nulls=True)


def test_the_null_guard_can_be_left_out(spark):
    check_landing_batch(_rows(spark, [(None, None, "x")]), "id", "modified_at", check_nulls=False)


@pytest.mark.parametrize("missing", ["id", "modified_at"])
def test_a_missing_column_is_refused_by_name(spark, missing):
    rows = _rows(spark, [("a", _T1, "x")]).drop(missing)
    with pytest.raises(ValueError, match=missing):
        check_landing_batch(rows, "id", "modified_at", check_nulls=False)


def test_a_string_ordering_column_is_refused(spark):
    rows = _rows(spark, [("a", "2026-01-01T10:00:00Z", "x")], "id string, modified_at string, name string")
    with pytest.raises(ValueError, match="modified_at.*string"):
        check_landing_batch(rows, "id", "modified_at", check_nulls=False)


@pytest.mark.parametrize("ddl", ["modified_at bigint", "modified_at date", "modified_at timestamp_ntz",
                                 "modified_at decimal(20,0)"])
def test_a_number_or_a_date_orders(spark, ddl):
    rows = spark.createDataFrame([], f"id string, {ddl}")
    check_landing_batch(rows, "id", "modified_at", check_nulls=False)


# The check-adversary run of 2026-11-13 found each case below broken; each is pinned here.

def test_a_tie_with_nulls_in_different_columns_is_settled_the_same_way(spark):
    schema = "id string, modified_at timestamp, a string, b string"
    rows = [("k", _T1, None, "x"), ("k", _T1, "x", None)]
    winners = {tuple(latest_per_key(spark.createDataFrame(order, schema), "id", "modified_at").collect()[0])
               for order in (rows, rows[::-1])}
    assert len(winners) == 1


def test_a_map_column_takes_part_in_the_tie_break(spark):
    rows = spark.createDataFrame([("k", _T1, {"a": 1}), ("k", _T1, {"a": 2})], "id string, modified_at timestamp, m map<string,int>")
    assert latest_per_key(rows, "id", "modified_at").count() == 1


@pytest.mark.parametrize("name", ["a.b", "a`b"])
def test_a_column_name_with_a_dot_or_backtick_is_one_name(spark, name):
    rows = spark.createDataFrame([("k", _T1, "x")], ["id", "modified_at", name])
    out = latest_per_key(rows, "id", "modified_at")
    assert out.columns == ["id", "modified_at", name]
    check_landing_batch(rows.withColumnRenamed("id", name + "_key"), name + "_key", "modified_at", check_nulls=True)


def test_a_column_named_like_the_rank_helper_is_kept(spark):
    rows = spark.createDataFrame([("k", _T1, "keep")], "id string, modified_at timestamp, _spark_quick_start_rank string")
    out = latest_per_key(rows, "id", "modified_at").collect()
    assert [tuple(r) for r in out] == [("k", _T1, "keep")]


def test_a_nan_ordering_value_is_refused(spark):
    rows = spark.createDataFrame([("k", float("nan"))], "id string, modified_at double")
    with pytest.raises(ValueError, match="NaN.*modified_at"):
        check_landing_batch(rows, "id", "modified_at", check_nulls=True)


def test_the_key_is_found_ignoring_case(spark):
    check_landing_batch(_rows(spark, [("a", _T1, "x")]).withColumnRenamed("id", "ID"), "id", "modified_at",
                        check_nulls=True)


def test_two_columns_differing_only_by_case_are_refused(spark):
    rows = spark.createDataFrame([("a", _T1, "x", "y")], ["id", "modified_at", "name", "NAME"])
    with pytest.raises(ValueError, match="differ only by case"):
        check_landing_batch(rows, "id", "modified_at", check_nulls=False)
