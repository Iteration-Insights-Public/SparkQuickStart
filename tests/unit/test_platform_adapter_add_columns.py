"""PlatformAdapter.add_delta_columns: the DDL it builds, and the names it refuses (F223 review)."""
from __future__ import annotations

import pytest
from pyspark.sql.types import ArrayType, IntegerType, MapType, StringType, StructField, StructType

from spark_quick_start.adapters import MockPlatformAdapter
from spark_quick_start.adapters.platform_adapter import UnsupportedTableIdentifier, _add_columns_sql


class _Spark:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def sql(self, text: str) -> None:
        self.statements.append(text)


def _nested() -> StructField:
    inner = StructType([StructField("a b", IntegerType()), StructField("c-d", ArrayType(StringType()))])
    return StructField("weird`name", StructType([StructField("x,y>", MapType(StringType(), inner))]))


def test_every_name_is_quoted_at_every_depth():
    sql = _add_columns_sql("/lake/raw/t", [StructField("email", StringType()), _nested()])
    assert sql == ("ALTER TABLE delta.`/lake/raw/t` ADD COLUMNS (`email` string, `weird``name` "
                   "struct<`x,y>`: map<string, struct<`a b`: int, `c-d`: array<string>>>>)")


def test_the_default_runs_one_statement_and_none_for_no_fields():
    from spark_quick_start.adapters.local_adapter import LocalAdapter

    spark = _Spark()
    LocalAdapter().add_delta_columns(spark, "cat.sch.t", [])
    LocalAdapter().add_delta_columns(spark, "cat.sch.t", [StructField("n", IntegerType())])
    assert spark.statements == ["ALTER TABLE `cat`.`sch`.`t` ADD COLUMNS (`n` int)"]


@pytest.mark.parametrize("field", [
    StructField("a${x}", StringType()),
    StructField("a\nb", StringType()),
    StructField("s", StructType([StructField("in$ide", StringType())])),
])
def test_a_dollar_or_control_character_at_any_depth_is_refused(field):
    with pytest.raises(UnsupportedTableIdentifier):
        _add_columns_sql("/lake/raw/t", [field])


def test_the_mock_refuses_what_the_default_refuses_and_records_the_rest():
    adapter = MockPlatformAdapter()
    with pytest.raises(UnsupportedTableIdentifier):
        adapter.add_delta_columns(None, "/lake/$raw/t", [StructField("n", IntegerType())])  # type: ignore[arg-type]
    adapter.add_delta_columns(None, "/lake/raw/t", [StructField("n", IntegerType())])  # type: ignore[arg-type]
    assert adapter.add_delta_columns_calls == [("/lake/raw/t", ["n"])]
