"""
test_validation.py — Unit tests for spark_quick_start.orchestration.validation.

Coverage:
  - has_data
  - assert_no_dups
  - schema_to_primitive_map
  - compare_schemas_primitive_ignore_order
"""
from __future__ import annotations

import pytest
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from spark_quick_start.orchestration.validation import (
    compare_schemas_primitive_ignore_order,
    schema_to_primitive_map,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


# Schema reused across assert_no_dups tests.


# ---------------------------------------------------------------------------
# TestHasData
# ---------------------------------------------------------------------------

class TestSchemaToPrimitiveMap:
    def test_string_field(self):
        schema = StructType([StructField("name", StringType())])
        result = schema_to_primitive_map(schema)
        assert result == {"name": "string"}

    def test_integer_field(self):
        schema = StructType([StructField("age", IntegerType())])
        result = schema_to_primitive_map(schema)
        assert result == {"age": "int"}

    def test_case_insensitive_default(self):
        schema = StructType([StructField("Name", StringType())])
        result = schema_to_primitive_map(schema, case_insensitive=True)
        assert "name" in result
        assert "Name" not in result

    def test_case_sensitive_flag(self):
        schema = StructType([StructField("Name", StringType())])
        result = schema_to_primitive_map(schema, case_insensitive=False)
        assert "Name" in result
        assert "name" not in result

    def test_duplicate_column_after_normalisation_raises(self):
        # "Name" and "name" collide when lowercased
        schema = StructType([
            StructField("Name", StringType()),
            StructField("name", IntegerType()),
        ])
        with pytest.raises(ValueError, match="Duplicate column"):
            schema_to_primitive_map(schema, case_insensitive=True)

    def test_complex_type_raises(self):
        nested = StructType([StructField("x", IntegerType())])
        schema = StructType([StructField("nested", nested)])
        with pytest.raises(ValueError):
            schema_to_primitive_map(schema)


# ---------------------------------------------------------------------------
# TestCompareSchemasIgnoreOrder
# ---------------------------------------------------------------------------

class TestCompareSchemasIgnoreOrder:
    def _make_schema(self, *fields):
        return StructType(list(fields))

    def test_identical_schemas_ok(self):
        schema = self._make_schema(
            StructField("id", IntegerType()),
            StructField("name", StringType()),
        )
        ok, report = compare_schemas_primitive_ignore_order(schema, schema)
        assert ok is True
        assert report["missing"] == []
        assert report["extra"] == []
        assert report["type_mismatches"] == []

    def test_different_order_ok(self):
        actual = self._make_schema(
            StructField("name", StringType()),
            StructField("id", IntegerType()),
        )
        expected = self._make_schema(
            StructField("id", IntegerType()),
            StructField("name", StringType()),
        )
        ok, report = compare_schemas_primitive_ignore_order(actual, expected)
        assert ok is True

    def test_missing_column_detected(self):
        actual = self._make_schema(StructField("id", IntegerType()))
        expected = self._make_schema(
            StructField("id", IntegerType()),
            StructField("name", StringType()),
        )
        ok, report = compare_schemas_primitive_ignore_order(actual, expected)
        assert ok is False
        assert "name" in report["missing"]

    def test_extra_column_detected(self):
        actual = self._make_schema(
            StructField("id", IntegerType()),
            StructField("extra_col", StringType()),
        )
        expected = self._make_schema(StructField("id", IntegerType()))
        ok, report = compare_schemas_primitive_ignore_order(actual, expected)
        assert ok is False
        assert "extra_col" in report["extra"]

    def test_type_mismatch_detected(self):
        actual = self._make_schema(StructField("id", IntegerType()))
        expected = self._make_schema(StructField("id", StringType()))
        ok, report = compare_schemas_primitive_ignore_order(actual, expected)
        assert ok is False
        assert len(report["type_mismatches"]) == 1
        mismatch = report["type_mismatches"][0]
        assert mismatch["column"] == "id"
        assert mismatch["actual"] == "int"
        assert mismatch["expected"] == "string"

    def test_case_insensitive_match(self):
        actual = self._make_schema(StructField("Name", StringType()))
        expected = self._make_schema(StructField("name", StringType()))
        ok, report = compare_schemas_primitive_ignore_order(actual, expected, case_insensitive=True)
        assert ok is True


def test_assert_no_dups_releases_cache_when_the_action_raises(spark, monkeypatch):
    from pyspark.sql import DataFrame

    from spark_quick_start.orchestration.transformers._validation import assert_no_dups

    df = spark.createDataFrame([("A",), ("A",)], "k string")
    cached: list = []
    real_cache = DataFrame.cache

    def _cache(self):
        out = real_cache(self)
        cached.append(out)
        return out

    def _boom(self):
        raise RuntimeError("action failed")

    monkeypatch.setattr(DataFrame, "cache", _cache)
    monkeypatch.setattr(DataFrame, "count", _boom)
    with pytest.raises(RuntimeError, match="action failed"):
        assert_no_dups(df, "src", ["k"], serverless=False)
    assert cached
    assert all(not c.is_cached for c in cached)
