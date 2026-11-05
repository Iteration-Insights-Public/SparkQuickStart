"""NotebookStub — local implementation of notebookutils.notebook.*"""
from __future__ import annotations

import logging
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

_log = logging.getLogger("local_notebookutils")


class NotebookExitSignal(Exception):
    """Raised by notebook.exit() during child-notebook execution to capture the exit value."""


class NotebookStub:
    """Local stub for the notebookutils.notebook namespace.

    Supports two execution modes, tried in order:

    1. **File execution** (``notebooks_dir`` set): finds ``{name}.ipynb`` under
       ``notebooks_dir`` and executes it in-process using ``nbformat`` + ``exec``.
    2. **Registry execution** (fallback): calls the callable registered for ``name``.

    Both modes share the same ``registry`` dict with ``LocalAdapter`` — a single
    registration point for pure-Python stubs when file execution is not needed.
    """

    def __init__(
        self,
        registry: dict[str, Callable[[dict[str, Any]], str]],
        notebooks_dir: Path | None = None,
    ) -> None:
        self._registry = registry
        self._notebooks_dir = notebooks_dir

    def run(
        self,
        name: str,
        timeout: int = 300,
        args: dict[str, Any] | None = None,
    ) -> str:
        """Run a notebook by name.

        Resolves to a ``.ipynb`` file in ``notebooks_dir`` if configured,
        otherwise falls back to the callable registry.
        """
        resolved_args = args or {}

        if self._notebooks_dir is not None:
            nb_path = self._resolve_notebook_path(name)
            if nb_path is not None:
                return self._run_ipynb(nb_path, resolved_args)

        if name not in self._registry:
            raise KeyError(
                f"local_notebookutils: no callable registered for notebook '{name}' "
                f"and no .ipynb file found"
                + (f" under '{self._notebooks_dir}'" if self._notebooks_dir else "")
                + f". Available in registry: {sorted(self._registry)}"
            )
        return self._registry[name](resolved_args)

    def runMultiple(
        self,
        notebooks: list[dict[str, Any]],
        *,
        max_parallel: int = 5,
    ) -> list[dict[str, Any]]:
        """Run multiple notebooks sequentially (locally).

        ``max_parallel`` is accepted to match the Fabric API signature but
        is ignored — local execution is always sequential.

        Returns a list of result dicts with keys ``path``, ``exitValue``,
        ``exception`` (mirrors the Fabric result shape).
        """
        results = []
        for nb in notebooks:
            name = nb["path"]
            params = nb.get("parameters", {})
            nb_timeout = nb.get("timeout", 300)
            try:
                exit_value = self.run(name, nb_timeout, params)
                results.append({"path": name, "exitValue": exit_value, "exception": None})
            except Exception as exc:
                results.append({"path": name, "exitValue": None, "exception": str(exc)})
        return results

    def exit(self, value: str) -> None:
        """Log the notebook exit value locally.

        On Fabric this halts kernel execution. Locally execution continues —
        a known divergence. During child-notebook execution via ``run()``,
        this method is temporarily patched to raise ``NotebookExitSignal``
        so the exit value can be captured and returned.
        """
        _log.info("notebook.exit: %s", value)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_notebook_path(self, name: str) -> Path | None:
        """Find the .ipynb file for ``name``.

        Resolution order:
          1. ``{name}.ipynb`` as an absolute path (covers path/nb_name combos
             where path is a local directory).
          2. ``{notebooks_dir}/{stem}.ipynb`` where stem is Path(name).stem —
             covers Databricks/Fabric workspace paths whose stem matches a local
             file (e.g. ``/Workspace/Repos/proj/NB_Stage`` → ``NB_Stage.ipynb``).
        """
        p = Path(name)
        if p.is_absolute():
            direct = p.with_suffix(".ipynb")
            if direct.exists():
                return direct
        stem = p.stem if p.is_absolute() else name
        candidate = self._notebooks_dir / f"{stem}.ipynb"  # type: ignore[operator]
        return candidate if candidate.exists() else None

    def _run_ipynb(self, path: Path, args: dict[str, Any]) -> str:
        """Execute a .ipynb file in-process and return the notebook exit value.

        Replicates papermill's parameter injection: after the cell tagged
        "parameters" (or the first code cell if no tag is present), ``args``
        are overlaid onto the namespace so they override notebook-level defaults
        without being overwritten by later cells.

        If the notebook calls ``notebookutils.notebook.exit(value)`` the value
        is captured and returned. Otherwise the namespace is checked for a
        ``result`` variable with a ``to_json()`` method (common Fabric pattern
        where the last cell stores the result without explicitly exiting).
        Falls back to a default success ``ExecutionResult`` JSON if neither is
        present.
        """
        try:
            import nbformat
        except ImportError as exc:
            raise ImportError(
                "local_notebookutils: executing .ipynb files requires 'nbformat'. "
                "Install it with: pip install nbformat"
            ) from exc

        _log.info("local_notebookutils: executing notebook file '%s'", path)
        nb = nbformat.read(path, as_version=4)

        code_cells = [
            c for c in nb.cells
            if c.cell_type == "code" and c.source.strip()
        ]

        # Find the parameters cell — prefer explicit "parameters" tag (papermill
        # convention), fall back to the first code cell as a heuristic.
        params_idx: int | None = None
        for i, cell in enumerate(code_cells):
            tags = cell.get("metadata", {}).get("tags", [])
            if "parameters" in tags:
                params_idx = i
                break
        if params_idx is None and code_cells:
            params_idx = 0  # treat first code cell as parameters cell

        # Patch sys.modules["notebookutils"].notebook.exit to raise so we can
        # capture the value. Restored unconditionally in the finally block.
        nu_module = sys.modules.get("notebookutils")
        original_exit = None
        if nu_module is not None and hasattr(nu_module, "notebook"):
            original_exit = nu_module.notebook.exit
            nu_module.notebook.exit = lambda v: (_ for _ in ()).throw(NotebookExitSignal(v))

        namespace: dict[str, Any] = {}
        try:
            for i, cell in enumerate(code_cells):
                try:
                    exec(compile(cell.source, str(path), "exec"), namespace)
                except NotebookExitSignal as sig:
                    return str(sig.args[0])

                # After the parameters cell, overlay args so they win over defaults.
                if i == params_idx:
                    namespace.update(args)
        finally:
            if original_exit is not None and nu_module is not None:
                nu_module.notebook.exit = original_exit

        # Notebook completed without exit() — check if result is in namespace.
        nb_result = namespace.get("result")
        if nb_result is not None and hasattr(nb_result, "to_json"):
            return nb_result.to_json()

        from spark_quick_start.orchestration.models import ExecutionResult
        return ExecutionResult(status="success", has_error=False).to_json()
