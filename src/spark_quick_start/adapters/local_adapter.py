"""Local filesystem platform adapter for development and CI integration testing.

Implements ``PlatformAdapter`` using stdlib ``shutil`` for path deletion, an
optional in-process callable registry for notebook simulation, and
``ThreadPoolExecutor`` for parallel dispatch.

Delta I/O is handled by PySpark + ``delta-spark``; the adapter itself
implements the four abstract methods that surround that I/O and overrides
``notebook_exit`` and ``get_log_persistence_adapter`` with local versions.
Install the optional extra to get ``delta-spark``::

    pip install spark_quick_start[local]

The caller is responsible for constructing and configuring the ``SparkSession``
with the Delta extension before running any orchestration code.

Note: ``timeout`` passed to ``run_notebook`` is not enforced locally.
Rely on your test-runner or CI job-level timeout for long-running callables.

Example::

    from spark_quick_start.adapters import LocalAdapter
    from spark_quick_start.orchestration.models import ExecutionResult

    def my_notebook(args):
        return ExecutionResult(status="success", has_error=False).to_json()

    adapter = LocalAdapter(
        base_path="/tmp/delta_test",
        notebook_registry={"NB_Stage_Customer": my_notebook},
        current_notebook_name="OrchestratorTest",
    )
"""
from __future__ import annotations

import shutil
import traceback
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from spark_quick_start.adapters.platform_adapter import PlatformAdapter
from spark_quick_start.local_notebookutils._notebook import NotebookStub


class LocalAdapter(PlatformAdapter):
    """Local-filesystem platform adapter for development and CI testing.

    Uses ``shutil.rmtree`` for deletion, an optional callable registry for
    notebook simulation, and ``ThreadPoolExecutor`` for parallel dispatch.
    No Fabric or Databricks runtime utilities are required.

    Args:
        base_path: Root directory prepended to relative paths passed to
            ``delete_path``. Absolute paths are used unchanged.
            Exposed as a public attribute for callers that construct staging
            table paths. Defaults to ``""`` (no prefix applied).
        notebook_registry: Maps notebook name → callable accepting
            ``(args: Dict[str, Any]) -> str`` and returning an
            ``ExecutionResult.to_json()`` string.
            Pass ``{}`` to enable registry mode with no notebooks registered yet.
            If ``None`` (default), ``run_notebook`` raises ``NotImplementedError``.
        current_notebook_name: Value returned by ``get_current_notebook_name``.
            Defaults to ``None``.

    Example::

        from spark_quick_start.adapters import LocalAdapter
        from spark_quick_start.orchestration.models import ExecutionResult

        def stage_customer(args):
            return ExecutionResult(status="success", has_error=False).to_json()

        adapter = LocalAdapter(
            base_path="/tmp/delta_test",
            notebook_registry={"NB_Stage_Customer": stage_customer},
            current_notebook_name="IntegrationTest",
        )
        executor = NotebookExecutor(adapter, logger)
        paths, source_count, target_count = execute_staging_phases(
            ctx_manager, executor, logger
        )
    """

    def __init__(
        self,
        base_path: str = "",
        notebook_registry: dict[str, Callable[[dict[str, Any]], str]] | None = None,
        current_notebook_name: str | None = None,
        notebooks_dir: str | None = None,
    ) -> None:
        self.base_path: str = base_path
        self._registry: dict[str, Callable[[dict[str, Any]], str]] = (
            notebook_registry if notebook_registry is not None else {}
        )
        self._registry_enabled: bool = notebook_registry is not None or notebooks_dir is not None
        self._current_notebook_name: str | None = current_notebook_name
        nb_dir = Path(notebooks_dir) if notebooks_dir is not None else None
        self._notebook_stub = NotebookStub(self._registry, notebooks_dir=nb_dir)

    def run_notebook(self, name: str, timeout: int, args: dict[str, Any]) -> str:
        """Run a registered callable as a notebook substitute.

        Args:
            name: Notebook name matched against the registry.
            timeout: Accepted for interface compatibility; not enforced locally.
            args: Argument dict passed through to the callable.

        Returns:
            JSON exit-value string (``ExecutionResult.to_json()`` format).

        Raises:
            NotImplementedError: if no ``notebook_registry`` was provided at construction.
            KeyError: if ``name`` is not in the registry.
        """
        if not self._registry_enabled:
            raise NotImplementedError(
                f"LocalAdapter.run_notebook: neither notebook_registry nor notebooks_dir "
                f"was provided. Pass notebooks_dir to execute .ipynb files, or register "
                f"a callable for '{name}'."
            )
        return self._notebook_stub.run(name, timeout, args)

    def run_notebooks_parallel(self, dag: dict[str, Any]) -> dict[str, Any]:
        """Run registered callables concurrently via ``ThreadPoolExecutor``.

        Follows the same DAG shape and result structure as ``DatabricksAdapter``.
        One failing callable does not abort others.

        Args:
            dag: Dict with keys ``activities`` (list), optional ``concurrency``
                 (int), and optional ``timeoutInSeconds`` (int).

        Returns:
            Dict mapping activity name → ``{"exitVal": <json string>}``.
        """
        activities = dag.get("activities", [])
        concurrency = dag.get("concurrency", len(activities))
        per_nb_timeout = dag.get("timeoutInSeconds", 3600)
        results: dict[str, Any] = {}

        with ThreadPoolExecutor(max_workers=max(1, concurrency)) as executor:
            futures = {
                executor.submit(
                    self.run_notebook,
                    act["path"],
                    act.get("timeoutPerCellInSeconds", per_nb_timeout),
                    act.get("args", {}),
                ): act["name"]
                for act in activities
            }
            for future in as_completed(futures):
                act_name = futures[future]
                try:
                    results[act_name] = {"exitVal": future.result()}
                except Exception as exc:
                    # Function-local: adapters never import orchestration at module scope (layering rule 6).
                    from spark_quick_start.orchestration.models import ErrorDetails, ExecutionResult
                    err = ExecutionResult(
                        status="error",
                        has_error=True,
                        error_details=ErrorDetails.from_exception(exc, traceback.format_exc()).to_dict(),
                    )
                    results[act_name] = {"exitVal": err.to_json()}
        return results

    def delete_path(self, path: str) -> None:
        """Delete a local filesystem path using ``shutil.rmtree``.

        Relative paths are resolved against ``base_path``. Absolute paths are
        used unchanged. Silently succeeds if the resolved path does not exist.

        Args:
            path: Absolute or relative filesystem path to delete.

        Raises:
            NotADirectoryError: if the path exists but is a plain file.
            PermissionError: if the OS denies deletion.
        """
        p = Path(path)
        if not p.is_absolute() and self.base_path:
            p = Path(self.base_path) / p
        if p.exists():
            shutil.rmtree(p)

    def get_current_notebook_name(self) -> str | None:
        """Return the constructor-injected notebook name, or ``None``."""
        return self._current_notebook_name

    def notebook_exit(self, value: str) -> None:
        """Log the exit value locally via NotebookStub (no runtime to signal)."""
        self._notebook_stub.exit(value)

    def get_log_persistence_adapter(self) -> Any:
        """Return LocalFilePersistenceAdapter for local filesystem log persistence.

        ``OrchestratorLogger.from_logging_context`` constructs a ``LogSession``
        directly (not through ``init_logs``) with this adapter, which writes the
        log JSON file under ``logging_context.log_file_dir`` (``Files/Logs``
        when it is unset).
        """
        from spark_quick_start.spark_logging import LocalFilePersistenceAdapter
        return LocalFilePersistenceAdapter()
