"""Microsoft Fabric log persistence adapter.

Contains: FabricPersistenceAdapter — delegates to notebookutils.mssparkutils
(lazy import). Does NOT contain: orchestration logic or any non-Fabric
platform code.
"""
from __future__ import annotations

from spark_quick_start.spark_logging.persistence_adapter import LogPersistenceAdapter


class FabricPersistenceAdapter(LogPersistenceAdapter):
    """Microsoft Fabric adapter — delegates to ``notebookutils.mssparkutils.fs`` (lazy import)."""

    def _fs(self):
        from notebookutils import mssparkutils  # type: ignore
        return mssparkutils.fs

    def _nb(self):
        from notebookutils import mssparkutils  # type: ignore
        return mssparkutils.notebook

    def mkdir(self, path: str) -> None:
        self._fs().mkdirs(path)

    def write(self, path: str, content: str) -> None:
        self._fs().put(path, content, overwrite=True)

    def copy(self, src: str, dst: str) -> None:
        self._fs().cp(src, dst, True)

    def notebook_exit(self, value: str) -> None:
        self._nb().exit(value)

    def get_notebook_name(self) -> str | None:
        try:
            from notebookutils import mssparkutils  # type: ignore
            ctx = mssparkutils.runtime.context
            return ctx.get("currentNotebookName")
        except Exception:
            return None
