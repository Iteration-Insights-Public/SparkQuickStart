"""F159 — PlatformAdapter.write_delta_create creates a table and refuses to write over one.

The Data Vault writers create their tables through it. `delta_table_exists` swallows errors and
answers False, so a create that appended would, after a missed check, write every key a hub
already has a second time. `errorifexists` refuses instead for a Delta table. A path holding
other files but no Delta log is not refused by Delta's own check; `path_has_content` refuses
it (F164).
"""
from __future__ import annotations

import pytest

from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter, PlatformAdapter


class _Adapter(PlatformAdapter):
    def run_notebook(self, name, timeout, args):  # pragma: no cover
        raise NotImplementedError

    def run_notebooks_parallel(self, dag):  # pragma: no cover
        raise NotImplementedError

    def get_current_notebook_name(self):  # pragma: no cover
        return None

    def delete_path(self, path):  # pragma: no cover
        raise NotImplementedError


@pytest.fixture
def _delta(spark, tmp_path):
    pytest.importorskip("delta", reason="delta-spark not installed")
    try:
        spark.range(1).write.format("delta").save(str(tmp_path / "probe"))
    except Exception as exc:  # pragma: no cover — environment-dependent
        pytest.skip(f"Delta not usable in this Spark session: {type(exc).__name__}")


def test_a_missing_path_is_created_with_its_rows(spark, tmp_path, _delta):
    path = str(tmp_path / "hub")
    _Adapter().write_delta_create(spark.createDataFrame([("h1",)], "HashKey string"), path)
    assert [r.HashKey for r in spark.read.format("delta").load(path).collect()] == ["h1"]


def test_an_existing_delta_table_is_refused_and_keeps_its_rows(spark, tmp_path, _delta):
    path = str(tmp_path / "hub")
    spark.createDataFrame([("h1",)], "HashKey string").write.format("delta").save(path)
    with pytest.raises(Exception, match="(?i)already exist"):
        _Adapter().write_delta_create(spark.createDataFrame([("h1",)], "HashKey string"), path)
    assert spark.read.format("delta").load(path).count() == 1


def test_a_path_holding_other_files_is_refused(spark, tmp_path, _delta):
    path = tmp_path / "not_delta"
    path.mkdir()
    (path / "notes.txt").write_text("someone else's data")
    with pytest.raises(Exception, match="(?i)already exist"):
        _Adapter().write_delta_create(spark.createDataFrame([("h1",)], "HashKey string"), str(path))
    assert not (path / "_delta_log").exists()


def test_the_mock_refuses_a_table_it_knows_and_records_the_rest():
    adapter = MockPlatformAdapter(existing_tables={"/vault/h"})
    with pytest.raises(RuntimeError, match="already exists"):
        adapter.write_delta_create(object(), "/vault/h")
    adapter.write_delta_create(object(), "/vault/new")
    assert adapter.write_delta_create_calls == ["/vault/new"]
    assert adapter.delta_table_exists(None, "/vault/new")


def test_the_rows_of_a_create_by_catalog_name_are_counted(spark, _delta):
    """F159's review: Delta records a create by name as CREATE TABLE AS SELECT, not WRITE, and the
    write statistics read only WRITE, so a first load by name reported 0 rows."""
    from unittest.mock import MagicMock

    from spark_quick_start.orchestration.load_pipeline import capture_write_statistics
    name = "f159_create_by_name"
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    try:
        _Adapter().write_delta_create(spark.createDataFrame([("h1",), ("h2",)], "HashKey string"), name)
        assert capture_write_statistics(spark, name, -1, _Adapter(), MagicMock()) == 2
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")



class _Frame:
    """A frame that records a write: nothing may reach it when the create is refused."""

    def __init__(self):
        from unittest.mock import MagicMock
        self.sparkSession = MagicMock()
        self.write = MagicMock()


def test_a_path_the_adapter_reports_as_holding_files_is_refused_before_any_write():
    class Holding(_Adapter):
        def path_has_content(self, spark, path):
            return not path.endswith("/_delta_log")  # files, and no Delta log

    frame = _Frame()
    with pytest.raises(ValueError, match="already exists.*not a Delta table"):
        Holding().write_delta_create(frame, "abfss://c@a.dfs.core.windows.net/Tables/hub")
    frame.write.format.assert_not_called()


def test_a_delta_table_is_left_to_errorifexists_for_its_own_refusal():
    class DeltaThere(_Adapter):
        def path_has_content(self, spark, path):
            return True  # the path, and its _delta_log

        def delta_table_exists(self, spark, identifier):  # pragma: no cover
            raise AssertionError("the guard must not depend on DeltaTable")

    frame = _Frame()
    DeltaThere().write_delta_create(frame, "/vault/hub")
    frame.write.format.return_value.mode.assert_called_once_with("errorifexists")


def test_a_catalog_name_is_not_checked_as_a_path():
    class Boom(_Adapter):
        def path_has_content(self, spark, path):  # pragma: no cover
            raise AssertionError("a UC name is not a filesystem path")

    frame = _Frame()
    Boom().write_delta_create(frame, "catalog.schema.hub")
    frame.write.format.return_value.mode.return_value.saveAsTable.assert_called_once_with("catalog.schema.hub")
