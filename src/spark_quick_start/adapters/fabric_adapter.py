"""Microsoft Fabric platform adapter.

Contains: FabricAdapter — implements PlatformAdapter using notebookutils (lazy import).
Does NOT contain: orchestration logic, Spark logic, or any non-Fabric platform code.
"""
from __future__ import annotations

from typing import Any

from spark_quick_start.adapters.platform_adapter import PlatformAdapter


class FabricAdapter(PlatformAdapter):
    """Microsoft Fabric adapter — delegates to notebookutils (lazy import)."""

    def run_notebook(self, name: str, timeout: int, args: dict[str, Any]) -> str:
        import notebookutils  # type: ignore
        return notebookutils.notebook.run(name, timeout, args)

    def run_notebooks_parallel(self, dag: dict[str, Any]) -> dict[str, Any]:
        import notebookutils  # type: ignore
        return notebookutils.notebook.runMultiple(dag, {"displayDAGViaGraphviz": False})

    def delete_path(self, path: str) -> None:
        import notebookutils  # type: ignore
        notebookutils.fs.rm(path, recurse=True)

    def get_current_notebook_name(self) -> str | None:
        try:
            import notebookutils  # type: ignore
            return notebookutils.runtime.context.get("currentNotebookName")
        except Exception:
            return None

    def notebook_exit(self, value: str) -> None:
        """Call notebookutils.notebook.exit with the given value."""
        import notebookutils  # type: ignore
        notebookutils.notebook.exit(value)

    def read_warehouse_view(
        self,
        spark: Any,
        identifier: str,
        *,
        workspace_id: str | None = None,
        lakehouse_id: str | None = None,
    ) -> Any:
        """Read a Fabric Warehouse or SQL-endpoint view with Fabric's Warehouse connector (F140).

        The same call the removed DQF engine made for its ``view`` kind, behind the adapter: the
        ``workspaceId`` and ``lakehouseId`` reader options when given, then ``synapsesql``.
        ``lakehouse_id`` is the old DQF spec's ``defaultLakehouseId``, a lakehouse SQL endpoint; whether a
        Warehouse item needs another option is not known here (F140). Not provable outside Fabric.

        Raises:
            RuntimeError: when the reader has no ``synapsesql``: the Warehouse connector is not
                loaded in this session.
        """
        try:
            # Registers DataFrameReader.synapsesql where the runtime has not already; absent
            # outside Fabric, so guarded (rule: platform-boundary §2).
            import com.microsoft.spark.fabric  # type: ignore  # noqa: F401
        except ImportError:
            pass
        reader = spark.read
        if not hasattr(reader, "synapsesql"):
            raise RuntimeError(
                "FabricAdapter.read_warehouse_view: the Spark reader has no synapsesql; the Fabric "
                "Warehouse connector is not loaded in this session (it comes with the Fabric Spark "
                "runtime, as com.microsoft.spark.fabric)"
            )
        if workspace_id:
            reader = reader.option("workspaceId", workspace_id)
        if lakehouse_id:
            reader = reader.option("lakehouseId", lakehouse_id)
        return reader.synapsesql(identifier)

    def get_log_persistence_adapter(self) -> Any:
        """Return a ``FabricPersistenceAdapter`` for OneLake log file writes.

        The adapter persists log JSON to the path supplied in
        ``logging_context.log_file_dir``, writing through
        ``notebookutils.mssparkutils.fs``. This method itself calls no SDK.
        """
        from spark_quick_start.spark_logging import FabricPersistenceAdapter
        return FabricPersistenceAdapter()
