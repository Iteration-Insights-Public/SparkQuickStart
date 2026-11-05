"""Azure Databricks log persistence adapter.

Contains: DatabricksPersistenceAdapter — delegates to dbutils, resolved through
    ``adapters.databricks_adapter._get_dbutils`` (lazy import), the lookup DatabricksAdapter uses.
Does NOT contain: orchestration logic or any non-Databricks platform code.
"""
from __future__ import annotations

from typing import Any

from spark_quick_start.spark_logging.persistence_adapter import LogPersistenceAdapter


def _dbutils() -> Any:
    """The dbutils handle, resolved as ``DatabricksAdapter`` resolves it (F120).

    dbutils is usually a notebook global or ``databricks.sdk.runtime.dbutils``, not an importable
    module, so a bare ``import dbutils`` fails on serverless and the failure surfaced only as an
    empty ``ExecutionResult.logs``. One lookup, shared rather than copied: the import is lazy,
    inside the function, and both files are databricks-only in a platform-filtered build, so a
    fabric-only build never reaches it.
    """
    from spark_quick_start.adapters.databricks_adapter import _get_dbutils
    return _get_dbutils()


class DatabricksPersistenceAdapter(LogPersistenceAdapter):
    """Databricks adapter — delegates to ``dbutils.fs`` and ``dbutils.notebook`` (lazy import)."""

    def _fs(self):
        return _dbutils().fs

    def _nb(self):
        return _dbutils().notebook

    def mkdir(self, path: str) -> None:
        self._fs().mkdirs(path)

    def write(self, path: str, content: str) -> None:
        self._fs().put(path, content, overwrite=True)

    def copy(self, src: str, dst: str) -> None:
        self._fs().cp(src, dst, recurse=False)

    def notebook_exit(self, value: str) -> None:
        self._nb().exit(value)

    def get_notebook_name(self) -> str | None:
        try:
            ctx = _dbutils().notebook.entry_point.getDbutils().notebook().getContext()
            nb_path = ctx.notebookPath().get()
            return nb_path.split("/")[-1] if nb_path else None
        except Exception:
            return None
