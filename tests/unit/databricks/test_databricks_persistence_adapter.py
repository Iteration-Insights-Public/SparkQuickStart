"""DatabricksPersistenceAdapter contract check (spark_logging), relocated
from tests/test_spark_logging.py's TestAdapterContract during the
platform-scoped test split.
"""
from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

import pytest

from spark_quick_start.spark_logging import DatabricksPersistenceAdapter


def test_databricks_has_required_methods():
    a = DatabricksPersistenceAdapter()
    assert callable(a.mkdir)
    assert callable(a.write)
    assert callable(a.copy)
    assert callable(a.notebook_exit)


# ---------------------------------------------------------------------------
# F120 — dbutils is resolved the way DatabricksAdapter resolves it
# ---------------------------------------------------------------------------


@pytest.fixture
def _sdk_dbutils(monkeypatch):
    """`databricks.sdk.runtime.dbutils` faked, and no importable `dbutils` module.

    That is a Databricks serverless runtime as far as the lookup can tell: dbutils is a notebook
    global and an SDK attribute, not a module. Whether a real runtime looks like this is not
    provable here (F120).
    """
    dbutils = MagicMock(name="dbutils")
    runtime = types.ModuleType("databricks.sdk.runtime")
    runtime.dbutils = dbutils  # type: ignore[attr-defined]
    for name in ("databricks", "databricks.sdk"):
        monkeypatch.setitem(sys.modules, name, types.ModuleType(name))
    monkeypatch.setitem(sys.modules, "databricks.sdk.runtime", runtime)
    monkeypatch.setitem(sys.modules, "dbutils", None)  # `import dbutils` raises ImportError
    return dbutils


def test_mkdir_and_write_go_through_the_sdk_dbutils(_sdk_dbutils):
    a = DatabricksPersistenceAdapter()
    a.mkdir("/Volumes/c/s/v/logs")
    a.write("/Volumes/c/s/v/logs/a.json", "{}")
    _sdk_dbutils.fs.mkdirs.assert_called_once_with("/Volumes/c/s/v/logs")
    _sdk_dbutils.fs.put.assert_called_once_with("/Volumes/c/s/v/logs/a.json", "{}", overwrite=True)


def test_copy_and_exit_go_through_the_sdk_dbutils(_sdk_dbutils):
    a = DatabricksPersistenceAdapter()
    a.copy("/a", "/b")
    a.notebook_exit("done")
    _sdk_dbutils.fs.cp.assert_called_once_with("/a", "/b", recurse=False)
    _sdk_dbutils.notebook.exit.assert_called_once_with("done")


def test_the_notebook_name_is_read_through_the_sdk_dbutils(_sdk_dbutils):
    ctx = _sdk_dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    ctx.notebookPath().get.return_value = "/Users/me/NB_Load"
    assert DatabricksPersistenceAdapter().get_notebook_name() == "NB_Load"
