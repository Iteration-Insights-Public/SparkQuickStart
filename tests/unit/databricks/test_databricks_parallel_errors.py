"""DatabricksAdapter.run_notebooks_parallel — the error result a failed notebook becomes (F114)."""
from __future__ import annotations

import json


def test_failed_notebook_error_details_has_the_pinned_shape():
    from spark_quick_start.adapters.databricks_adapter import DatabricksAdapter

    adapter = DatabricksAdapter()

    def boom(name, timeout, args):
        raise RuntimeError("Deliberate failure")

    adapter.run_notebook = boom  # type: ignore[method-assign]
    results = adapter.run_notebooks_parallel({"activities": [{"name": "act", "path": "NB"}]})
    details = json.loads(results["act"]["exitVal"])["error_details"]
    assert set(details) == {"error_type", "error_message", "traceback"}
    assert (details["error_type"], details["error_message"]) == ("RuntimeError", "Deliberate failure")
    assert "RuntimeError: Deliberate failure" in details["traceback"]
