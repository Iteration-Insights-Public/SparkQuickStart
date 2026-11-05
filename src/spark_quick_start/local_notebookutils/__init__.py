"""local_notebookutils — local stub for notebookutils notebook execution.

Injects a ``notebookutils``-compatible object into ``sys.modules`` so that
notebook cells calling ``notebookutils.notebook.run()`` or
``notebookutils.notebook.runMultiple()`` work locally without a Fabric runtime.

Only the notebook execution surface is stubbed (``run``, ``runMultiple``,
``exit``). Other namespaces (``fs``, ``lakehouse``, ``secrets``, ``runtime``)
are not provided.

Two execution modes are supported:

**File execution** (recommended): pass ``notebooks_dir`` pointing to the
directory that contains your ``.ipynb`` files. ``notebook.run("NB_Name")``
will find and execute ``NB_Name.ipynb`` in-process using ``nbformat``.
Requires ``pip install nbformat``.

**Registry execution** (fallback / unit tests): pass a ``notebook_registry``
dict mapping notebook name → Python callable. Used when ``notebooks_dir`` is
not set, or when a notebook is not found on disk.

Usage with file execution::

    from spark_quick_start.local_notebookutils import activate

    activate(notebooks_dir="/path/to/notebooks")

    # notebook cells now work — NB_Stage_Customer.ipynb is found and executed:
    import notebookutils
    result = notebookutils.notebook.run("NB_Stage_Customer", 300, {"env": "dev"})

Usage with registry (unit tests)::

    from spark_quick_start.local_notebookutils import activate

    registry = {"NB_Stage_Customer": lambda args: '{"status": "success"}'}
    activate(notebook_registry=registry)

Sharing the registry with LocalAdapter::

    from spark_quick_start.adapters import LocalAdapter

    registry = {}
    activate(notebook_registry=registry)
    adapter = LocalAdapter(base_path="/tmp/delta", notebook_registry=registry)
    # registrations added to `registry` after this point are visible to both

Known divergence: ``notebookutils.notebook.exit()`` logs locally and returns;
on Fabric it halts kernel execution. During child notebook file execution,
``exit()`` is temporarily patched to stop execution and return the exit value.
"""
from __future__ import annotations

import sys
import types
from collections.abc import Callable
from pathlib import Path
from typing import Any

from spark_quick_start.local_notebookutils._notebook import NotebookExitSignal, NotebookStub

__all__ = ["NotebookExitSignal", "activate", "deactivate"]


def activate(
    notebook_registry: dict[str, Callable[[dict[str, Any]], str]] | None = None,
    *,
    notebooks_dir: str | Path | None = None,
) -> None:
    """Inject the notebookutils stub into sys.modules.

    Args:
        notebook_registry: Mapping of notebook name → callable. Used as a
            fallback when ``notebooks_dir`` is not set or a file is not found.
            The same dict can be shared with ``LocalAdapter``.
        notebooks_dir: Directory containing ``.ipynb`` notebook files.
            When set, ``notebook.run(name)`` executes ``{name}.ipynb`` from
            this directory in-process. Requires ``nbformat`` to be installed.

    Example::

        activate(notebooks_dir="/workspace/notebooks")
        # or for unit tests:
        activate(notebook_registry={"NB_A": lambda args: '{"status": "success"}'})
    """
    registry = notebook_registry if notebook_registry is not None else {}
    nb_dir = Path(notebooks_dir) if notebooks_dir is not None else None

    stub = NotebookStub(registry, notebooks_dir=nb_dir)
    module = types.SimpleNamespace(notebook=stub)
    sys.modules["notebookutils"] = module  # type: ignore[assignment]


def deactivate() -> None:
    """Remove the notebookutils stub from sys.modules. No-op if not present."""
    sys.modules.pop("notebookutils", None)
