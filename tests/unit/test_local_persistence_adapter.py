"""Tests for LocalFilePersistenceAdapter (spark_logging).

LocalFilePersistenceAdapter is platform-neutral, not local-only: real
Fabric/Databricks deployments use it via orchestrator_logger.py whenever
the configured log_file_dir is a locally-mounted path (/Volumes/...,
/dbfs/..., /tmp/..., etc. — accessible via pathlib without dbutils, and
the correct Serverless-safe choice per the comment in
OrchestratorLogger.from_logging_context()). It therefore ships on every
platform-filtered release branch, unlike LocalAdapter and
local_notebookutils, which are genuinely local-dev/CI-only. Lives at
tests/unit/ root (not tests/unit/local/) for that reason.
"""
from __future__ import annotations

import json
import os

from spark_quick_start.spark_logging import LocalFilePersistenceAdapter, LogPersistenceAdapter


class TestLocalFilePersistenceAdapter:
    def test_mkdir(self, tmp_path):
        a = LocalFilePersistenceAdapter()
        target = str(tmp_path / "sub" / "dir")
        a.mkdir(target)
        assert os.path.isdir(target)

    def test_mkdir_idempotent(self, tmp_path):
        a = LocalFilePersistenceAdapter()
        target = str(tmp_path / "sub")
        a.mkdir(target)
        a.mkdir(target)  # should not raise

    def test_write(self, tmp_path):
        a = LocalFilePersistenceAdapter()
        path = str(tmp_path / "out.json")
        a.write(path, '{"hello": "world"}')
        assert json.loads(open(path, encoding="utf-8").read()) == {"hello": "world"}

    def test_write_overwrite(self, tmp_path):
        a = LocalFilePersistenceAdapter()
        path = str(tmp_path / "out.txt")
        a.write(path, "first")
        a.write(path, "second")
        assert open(path, encoding="utf-8").read() == "second"

    def test_copy(self, tmp_path):
        a = LocalFilePersistenceAdapter()
        src = tmp_path / "source.txt"
        dst = str(tmp_path / "dest.txt")
        src.write_text("hello", encoding="utf-8")
        a.copy(str(src), dst)
        assert open(dst, encoding="utf-8").read() == "hello"

    def test_notebook_exit_noop(self):
        a = LocalFilePersistenceAdapter()
        a.notebook_exit("value")  # should not raise or do anything

    def test_is_subclass(self):
        assert issubclass(LocalFilePersistenceAdapter, LogPersistenceAdapter)


class TestAdapterContractLocal:
    """LogPersistenceAdapter contract check, relocated from
    tests/test_spark_logging.py's TestAdapterContract."""

    def test_local_has_required_methods(self):
        a = LocalFilePersistenceAdapter()
        assert callable(a.mkdir)
        assert callable(a.write)
        assert callable(a.copy)
        assert callable(a.notebook_exit)
