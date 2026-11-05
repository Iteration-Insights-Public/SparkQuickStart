"""F157 — PlatformAdapter reads and writes a Delta table's properties by identifier.

The Data Vault writers seal their hash settings on the table (F158) through these two methods.
The defaults are SQL only, with no DeltaTable, so they hold where delta-spark's Python API is
absent (Spark Connect, serverless). Both interpolate into SQL, so the identifier, the keys and
the values are checked as strictly as `drop_delta`'s identifier is (spark-delta-cost 5).
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter, PlatformAdapter


class _Adapter(PlatformAdapter):
    """The defaults under test; every abstract method is irrelevant here."""

    def run_notebook(self, name, timeout, args):  # pragma: no cover
        raise NotImplementedError

    def get_current_notebook_name(self):  # pragma: no cover
        return None

    def delete_path(self, path):  # pragma: no cover
        raise NotImplementedError

    def run_notebooks_parallel(self, dag):  # pragma: no cover
        raise NotImplementedError


def _spark(properties=None):
    spark = MagicMock()
    spark.sql.return_value.first.return_value = {"properties": properties or {}}
    return spark


# ── reading ──────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("identifier,target", [
    ("/lake/silver/customer_s", "delta.`/lake/silver/customer_s`"),
    ("abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Tables/s", "delta.`abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Tables/s`"),
    ("cat.vault.customer_s", "`cat`.`vault`.`customer_s`"),
])
def test_get_table_properties_describes_the_table_by_name_or_path(identifier, target):
    spark = _spark({"spark_quick_start.hash_algorithm": "sha256", "delta.minReaderVersion": "1"})
    got = _Adapter().get_table_properties(spark, identifier)
    spark.sql.assert_called_once_with(f"DESCRIBE DETAIL {target}")
    assert got == {"spark_quick_start.hash_algorithm": "sha256", "delta.minReaderVersion": "1"}


def test_get_table_properties_of_a_table_without_any_is_empty():
    spark = _spark(None)
    assert _Adapter().get_table_properties(spark, "/lake/t") == {}


# ── writing ──────────────────────────────────────────────────────────────────

def test_set_table_properties_alters_the_table_once_with_every_key():
    spark = MagicMock()
    _Adapter().set_table_properties(spark, "/lake/t", {"spark_quick_start.hash_algorithm": "sha256",
                                                       "spark_quick_start.hash_separator": "||"})
    spark.sql.assert_called_once_with(
        "ALTER TABLE delta.`/lake/t` SET TBLPROPERTIES ("
        "'spark_quick_start.hash_algorithm' = 'sha256', 'spark_quick_start.hash_separator' = '||')"
    )


def test_set_table_properties_with_nothing_to_set_issues_no_sql():
    spark = MagicMock()
    _Adapter().set_table_properties(spark, "/lake/t", {})
    spark.sql.assert_not_called()


# ── refusals: everything that reaches the SQL text ───────────────────────────

@pytest.mark.parametrize("key", ["hash_algorithm", "delta.appendOnly", "spark_quick_start.Hash",
                                 "spark_quick_start.a'b", "spark_quick_start.", "spark_quick_start.a b"])
def test_a_key_outside_the_library_namespace_is_refused(key):
    spark = MagicMock()
    with pytest.raises(ValueError, match="property key"):
        _Adapter().set_table_properties(spark, "/lake/t", {key: "x"})
    spark.sql.assert_not_called()


@pytest.mark.parametrize("value", ["a'b", "a\\b", "a\nb", "a\x00b", "a`b",
                                   # the adversary's: Spark substitutes ${…} inside literals
                                   "${env:HOME}", "${x.q}", "$x"])
def test_a_value_that_could_leave_its_literal_is_refused(value):
    spark = MagicMock()
    with pytest.raises(ValueError, match="property value"):
        _Adapter().set_table_properties(spark, "/lake/t", {"spark_quick_start.x": value})
    spark.sql.assert_not_called()


@pytest.mark.parametrize("identifier", ["/lake/a`b", "/lake/a\nb", "/lake/a\x00b", "cat.sch.t; DROP TABLE x",
                                        "cat.sch.t.extra",
                                        # the adversary's: a path rewritten into another table
                                        "/lake/${x.dir}", "/lake/${x.bt}"])
@pytest.mark.parametrize("method", ["get", "set"])
def test_an_identifier_that_could_leave_its_quotes_is_refused(identifier, method):
    spark = _spark()
    with pytest.raises(ValueError, match="(?i)refusing|unsafe"):
        if method == "get":
            _Adapter().get_table_properties(spark, identifier)
        else:
            _Adapter().set_table_properties(spark, identifier, {"spark_quick_start.x": "y"})
    spark.sql.assert_not_called()


# ── the recording mock ───────────────────────────────────────────────────────

def test_the_mock_returns_configured_properties_and_records_both_calls():
    adapter = MockPlatformAdapter(table_properties={"/lake/t": {"spark_quick_start.hash_algorithm": "md5"}},
                                  existing_tables={"/lake/other"})
    assert adapter.get_table_properties(MagicMock(), "/lake/t") == {"spark_quick_start.hash_algorithm": "md5"}
    assert adapter.get_table_properties(MagicMock(), "/lake/other") == {}
    adapter.set_table_properties(MagicMock(), "/lake/other", {"spark_quick_start.hash_algorithm": "sha256"})
    assert adapter.get_table_properties(MagicMock(), "/lake/other") == {"spark_quick_start.hash_algorithm": "sha256"}
    assert adapter.get_table_properties_calls == ["/lake/t", "/lake/other", "/lake/other"]
    assert adapter.set_table_properties_calls == [("/lake/other", {"spark_quick_start.hash_algorithm": "sha256"})]


def test_the_mock_refuses_what_the_default_refuses():
    with pytest.raises(ValueError, match="property key"):
        MockPlatformAdapter().set_table_properties(MagicMock(), "/lake/t", {"bad": "x"})
    for identifier in ("cat.sch.t.extra", "Tables/x", "/lake/${x}"):
        with pytest.raises(ValueError):
            MockPlatformAdapter(existing_tables={identifier}).get_table_properties(MagicMock(), identifier)
        with pytest.raises(ValueError):
            MockPlatformAdapter().set_table_properties(MagicMock(), identifier, {"spark_quick_start.x": "y"})


def test_the_mock_raises_for_a_missing_table_as_describe_detail_does():
    """F158 must tell a missing table from one present without a seal; the mock cannot blur them."""
    adapter = MockPlatformAdapter(existing_tables={"/lake/empty"})
    assert adapter.get_table_properties(MagicMock(), "/lake/empty") == {}
    with pytest.raises(RuntimeError, match="no Delta table"):
        adapter.get_table_properties(MagicMock(), "/lake/missing")


# ── a real Delta round trip ──────────────────────────────────────────────────

def test_a_local_delta_table_keeps_what_was_set(spark, tmp_path):
    pytest.importorskip("delta", reason="delta-spark not installed")
    path = str(tmp_path / "s")
    try:
        spark.range(1).write.format("delta").save(path)
    except Exception as exc:  # pragma: no cover — environment-dependent
        pytest.skip(f"Delta not usable in this Spark session: {type(exc).__name__}")
    real = _Adapter()
    assert not {k for k in real.get_table_properties(spark, path) if k.startswith("spark_quick_start.")}
    real.set_table_properties(spark, path, {"spark_quick_start.hash_algorithm": "sha256",
                                            "spark_quick_start.hash_separator": "||"})
    real.set_table_properties(spark, path, {"spark_quick_start.hash_algorithm": "md5"})
    got = real.get_table_properties(spark, path)
    assert got["spark_quick_start.hash_algorithm"] == "md5"
    assert got["spark_quick_start.hash_separator"] == "||"


# ── check-adversary findings, F157 ───────────────────────────────────────────

@pytest.mark.parametrize("identifier,target", [
    ("C:\\data\\t", "delta.`C:\\data\\t`"),
    ("\\\\srv\\share\\t", "delta.`\\\\srv\\share\\t`"),
    ("/lake/it's here", "delta.`/lake/it's here`"),
])
def test_windows_unc_and_quoted_paths_stay_valid(identifier, target):
    """A backslash and a single quote are literal inside a backtick-quoted name."""
    spark = _spark()
    _Adapter().get_table_properties(spark, identifier)
    spark.sql.assert_called_once_with(f"DESCRIBE DETAIL {target}")


class _Sneaky(str):
    """A str whose formatting and splitting differ from what a regex sees."""

    def __format__(self, spec):
        return "x'), ('delta.appendOnly' = 'true"

    def split(self, *a, **k):
        return ["a`b"]


@pytest.mark.parametrize("where", ["identifier", "key", "value"])
def test_a_str_subclass_is_refused_wherever_it_would_be_interpolated(where):
    spark = MagicMock()
    identifier, key, value = "/lake/t", "spark_quick_start.x", "sha256"
    if where == "identifier":
        identifier = _Sneaky("tbl")
    elif where == "key":
        key = _Sneaky("spark_quick_start.x")
    else:
        value = _Sneaky("sha256")
    with pytest.raises(ValueError):
        _Adapter().set_table_properties(spark, identifier, {key: value})
    spark.sql.assert_not_called()


@pytest.mark.parametrize("key", [1, None])
def test_a_non_string_key_is_refused_as_a_value_error(key):
    with pytest.raises(ValueError, match="property key"):
        _Adapter().set_table_properties(MagicMock(), "/lake/t", {key: "x"})

