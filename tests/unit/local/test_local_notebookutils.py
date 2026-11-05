"""Unit tests for local_notebookutils stub."""
from __future__ import annotations

import pytest

from spark_quick_start.local_notebookutils import (
    activate,
    deactivate,
)
from spark_quick_start.local_notebookutils._notebook import NotebookStub


@pytest.fixture(autouse=True)
def clean_notebookutils():
    yield
    deactivate()


# ---------------------------------------------------------------------------
# NotebookStub.run
# ---------------------------------------------------------------------------

def test_run_invokes_callable():
    called_with = []
    registry = {"NB_A": lambda args: (called_with.append(args), "ok")[1]}
    stub = NotebookStub(registry)
    result = stub.run("NB_A", 60, {"x": 1})
    assert result == "ok"
    assert called_with == [{"x": 1}]


def test_run_defaults_args_to_empty_dict():
    registry = {"NB_A": lambda args: str(args)}
    stub = NotebookStub(registry)
    assert stub.run("NB_A") == "{}"


def test_run_raises_key_error_for_missing_notebook():
    stub = NotebookStub({"NB_A": lambda a: "ok"})
    with pytest.raises(KeyError, match="NB_B"):
        stub.run("NB_B")


def test_run_error_message_lists_available_notebooks():
    stub = NotebookStub({"NB_A": lambda a: "ok", "NB_B": lambda a: "ok"})
    with pytest.raises(KeyError, match="NB_A"):
        stub.run("NB_X")


# ---------------------------------------------------------------------------
# NotebookStub.runMultiple
# ---------------------------------------------------------------------------

def test_run_multiple_invokes_all_notebooks():
    order = []
    registry = {
        "NB_A": lambda args: (order.append("A"), "a")[1],
        "NB_B": lambda args: (order.append("B"), "b")[1],
    }
    stub = NotebookStub(registry)
    results = stub.runMultiple([
        {"path": "NB_A", "parameters": {}},
        {"path": "NB_B", "parameters": {}},
    ])
    assert order == ["A", "B"]
    assert [r["exitValue"] for r in results] == ["a", "b"]


def test_run_multiple_captures_exception_without_aborting():
    registry = {
        "NB_FAIL": lambda args: (_ for _ in ()).throw(RuntimeError("boom")),
        "NB_OK":   lambda args: "ok",
    }
    stub = NotebookStub(registry)
    results = stub.runMultiple([
        {"path": "NB_FAIL"},
        {"path": "NB_OK"},
    ])
    assert results[0]["exception"] == "boom"
    assert results[0]["exitValue"] is None
    assert results[1]["exitValue"] == "ok"
    assert results[1]["exception"] is None


def test_run_multiple_accepts_max_parallel_without_error():
    registry = {"NB_A": lambda args: "ok"}
    stub = NotebookStub(registry)
    results = stub.runMultiple([{"path": "NB_A"}], max_parallel=10)
    assert results[0]["exitValue"] == "ok"


# ---------------------------------------------------------------------------
# NotebookStub.exit
# ---------------------------------------------------------------------------

def test_exit_does_not_raise_by_default(caplog):
    import logging
    stub = NotebookStub({})
    with caplog.at_level(logging.INFO, logger="local_notebookutils"):
        stub.exit("done")
    assert "done" in caplog.text


# ---------------------------------------------------------------------------
# activate / deactivate
# ---------------------------------------------------------------------------

def test_activate_injects_into_sys_modules():
    activate(notebook_registry={"NB_A": lambda a: "ok"})
    import notebookutils
    assert notebookutils.notebook.run("NB_A") == "ok"


def test_deactivate_removes_from_sys_modules():
    import sys
    activate(notebook_registry={})
    deactivate()
    assert "notebookutils" not in sys.modules


def test_activate_is_idempotent():
    activate(notebook_registry={"NB_A": lambda a: "first"})
    activate(notebook_registry={"NB_A": lambda a: "second"})
    import notebookutils
    assert notebookutils.notebook.run("NB_A") == "second"


def test_exit_does_not_raise_at_top_level(caplog):
    """notebook.exit() at the top level (not inside run()) logs and returns."""
    import logging
    activate(notebook_registry={})
    import notebookutils
    with caplog.at_level(logging.INFO, logger="local_notebookutils"):
        notebookutils.notebook.exit("done")  # must not raise
    assert "done" in caplog.text


# ---------------------------------------------------------------------------
# .ipynb file execution (requires nbformat)
# ---------------------------------------------------------------------------

nbformat = pytest.importorskip("nbformat", reason="nbformat not installed; skipping .ipynb execution tests")


def test_run_ipynb_file_captures_exit_value(tmp_path):
    """run() executes a .ipynb file and returns the notebook.exit() value."""

    nb = nbformat.v4.new_notebook()
    nb.cells = [
        nbformat.v4.new_code_cell("import notebookutils"),
        nbformat.v4.new_code_cell("notebookutils.notebook.exit('result_ok')"),
        nbformat.v4.new_code_cell("raise RuntimeError('this line must not run')"),
    ]
    nb_path = tmp_path / "NB_Test.ipynb"
    nbformat.write(nb, nb_path)

    activate(notebooks_dir=tmp_path)
    import notebookutils
    result = notebookutils.notebook.run("NB_Test")
    assert result == "result_ok"


def test_run_ipynb_returns_success_json_when_no_exit(tmp_path):
    """Notebook that completes without exit() returns a success ExecutionResult JSON."""
    import json

    nb = nbformat.v4.new_notebook()
    nb.cells = [nbformat.v4.new_code_cell("x = 1 + 1")]
    nbformat.write(nb, tmp_path / "NB_NoExit.ipynb")

    activate(notebooks_dir=tmp_path)
    import notebookutils
    result = notebookutils.notebook.run("NB_NoExit")
    parsed = json.loads(result)
    assert parsed["has_error"] is False
    assert parsed["status"] == "success"


def test_run_ipynb_returns_namespace_result_when_no_exit(tmp_path):
    """Notebook that stores result variable without exit() returns result.to_json()."""
    import json


    nb = nbformat.v4.new_notebook()
    nb.cells = [
        nbformat.v4.new_code_cell("from spark_quick_start.orchestration.models import ExecutionResult"),
        nbformat.v4.new_code_cell("result = ExecutionResult(status='success', has_error=False)"),
    ]
    nbformat.write(nb, tmp_path / "NB_Result.ipynb")

    activate(notebooks_dir=tmp_path)
    import notebookutils
    raw = notebookutils.notebook.run("NB_Result")
    parsed = json.loads(raw)
    assert parsed["status"] == "success"
    assert parsed["has_error"] is False


def test_run_ipynb_args_override_parameter_cell_defaults(tmp_path):
    """Args passed to run() override the notebook's default parameter values."""
    nb = nbformat.v4.new_notebook()
    nb.cells = [
        nbformat.v4.new_code_cell("stage_context_str = '{}'"),   # default (parameters cell)
        nbformat.v4.new_code_cell(
            "import notebookutils\n"
            "notebookutils.notebook.exit(stage_context_str)"
        ),
    ]
    nbformat.write(nb, tmp_path / "NB_Params.ipynb")

    activate(notebooks_dir=tmp_path)
    import notebookutils
    result = notebookutils.notebook.run("NB_Params", args={"stage_context_str": '{"injected": true}'})
    assert result == '{"injected": true}'


def test_run_ipynb_restores_exit_after_execution(tmp_path):
    """exit() at top level still logs (not raises) after a child notebook finishes."""

    nb = nbformat.v4.new_notebook()
    nb.cells = [nbformat.v4.new_code_cell("import notebookutils; notebookutils.notebook.exit('x')")]
    nbformat.write(nb, tmp_path / "NB_Child.ipynb")

    activate(notebooks_dir=tmp_path)
    import notebookutils
    notebookutils.notebook.run("NB_Child")
    # After child returns, top-level exit() must NOT raise
    notebookutils.notebook.exit("top_level_exit")


def test_shared_registry_late_registration():
    """Callable added to registry after activate() is visible immediately."""
    registry: dict = {}
    activate(notebook_registry=registry)
    registry["NB_LATE"] = lambda args: "late_result"

    import notebookutils
    assert notebookutils.notebook.run("NB_LATE") == "late_result"
