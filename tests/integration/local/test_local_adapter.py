"""Unit tests for LocalAdapter.

Integration tests (Section 8.7 of the design doc) are gated with
pytest.importorskip("delta") and require ``pip install spark_quick_start[local]``.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from spark_quick_start.adapters import LocalAdapter
from spark_quick_start.adapters.local_adapter import LocalAdapter as LocalAdapterDirect
from spark_quick_start.orchestration.models import ExecutionResult

# ---------------------------------------------------------------------------
# 8.9 — adapters/__init__.py re-export sanity
# ---------------------------------------------------------------------------

class TestAdaptersReExport:
    def test_local_adapter_importable_from_adapters(self):
        assert LocalAdapter is not None

    def test_local_adapter_direct_and_re_export_are_same_class(self):
        assert LocalAdapter is LocalAdapterDirect


# ---------------------------------------------------------------------------
# 8.6 — base_path attribute
# ---------------------------------------------------------------------------

class TestBasePathAttribute:
    def test_base_path_stored_as_attribute(self):
        adapter = LocalAdapter(base_path="/tmp/x")
        assert adapter.base_path == "/tmp/x"

    def test_base_path_defaults_to_empty_string(self):
        adapter = LocalAdapter()
        assert adapter.base_path == ""


# ---------------------------------------------------------------------------
# 8.2 — get_current_notebook_name
# ---------------------------------------------------------------------------

class TestGetCurrentNotebookName:
    def test_returns_injected_value(self):
        adapter = LocalAdapter(current_notebook_name="MyTest")
        assert adapter.get_current_notebook_name() == "MyTest"

    def test_returns_none_by_default(self):
        adapter = LocalAdapter()
        assert adapter.get_current_notebook_name() is None


# ---------------------------------------------------------------------------
# 8.1 — delete_path
# ---------------------------------------------------------------------------

class TestDeletePath:
    def test_removes_existing_directory(self, tmp_path: Path):
        target = tmp_path / "staging"
        target.mkdir()
        (target / "file.txt").write_text("data")

        adapter = LocalAdapter()
        adapter.delete_path(str(target))

        assert not target.exists()

    def test_nonexistent_path_is_silent(self):
        adapter = LocalAdapter()
        adapter.delete_path("/tmp/__nonexistent_delta_xyz_99999__")

    def test_permission_error_propagates(self, tmp_path: Path):
        target = tmp_path / "staging"
        target.mkdir()
        adapter = LocalAdapter()

        with patch("shutil.rmtree", side_effect=PermissionError("denied")):
            with pytest.raises(PermissionError):
                adapter.delete_path(str(target))

    def test_plain_file_raises(self, tmp_path: Path):
        plain_file = tmp_path / "not_a_dir.parquet"
        plain_file.write_text("bytes")
        adapter = LocalAdapter()

        with pytest.raises(NotADirectoryError):
            adapter.delete_path(str(plain_file))

    def test_relative_path_resolved_against_base_path(self, tmp_path: Path):
        staging = tmp_path / "staging" / "customer"
        staging.mkdir(parents=True)

        adapter = LocalAdapter(base_path=str(tmp_path))
        adapter.delete_path("staging/customer")

        assert not staging.exists()

    def test_absolute_path_ignores_base_path(self, tmp_path: Path):
        target = tmp_path / "absolute_target"
        target.mkdir()

        other_base = "/tmp/__should_not_matter__"
        adapter = LocalAdapter(base_path=other_base)
        adapter.delete_path(str(target))

        assert not target.exists()

    def test_relative_path_without_base_path_is_unchanged(self, tmp_path: Path):
        # When base_path is empty, relative paths are resolved from CWD by Path.
        # We just assert no exception for a nonexistent relative path (most common case).
        adapter = LocalAdapter(base_path="")
        adapter.delete_path("__nonexistent_relative__")  # should not raise


# ---------------------------------------------------------------------------
# 8.3 — run_notebook without registry
# ---------------------------------------------------------------------------

class TestRunNotebookNoRegistry:
    def test_raises_not_implemented(self):
        adapter = LocalAdapter()
        with pytest.raises(NotImplementedError):
            adapter.run_notebook("NB_Stage_Foo", 90, {})

    def test_error_message_names_notebook(self):
        adapter = LocalAdapter()
        with pytest.raises(NotImplementedError, match="NB_Stage_Foo"):
            adapter.run_notebook("NB_Stage_Foo", 90, {})


# ---------------------------------------------------------------------------
# 8.4 — run_notebook with registry
# ---------------------------------------------------------------------------

class TestRunNotebookWithRegistry:
    def _success_result(self) -> str:
        return ExecutionResult(status="success", has_error=False).to_json()

    def test_dispatches_to_callable(self):
        received: list = []

        def nb_a(args):
            received.append(args)
            return self._success_result()

        adapter = LocalAdapter(notebook_registry={"NB_A": nb_a})
        result = adapter.run_notebook("NB_A", 90, {"k": "v"})

        assert result == self._success_result()
        assert received == [{"k": "v"}]

    def test_unknown_name_raises_key_error(self):
        adapter = LocalAdapter(notebook_registry={"NB_A": lambda a: self._success_result()})
        with pytest.raises(KeyError):
            adapter.run_notebook("NB_B", 90, {})

    def test_empty_registry_raises_key_error_not_not_implemented(self):
        # notebook_registry={} enables registry mode even with no entries registered
        adapter = LocalAdapter(notebook_registry={})
        with pytest.raises(KeyError):
            adapter.run_notebook("NB_A", 90, {})

    def test_timeout_is_not_enforced(self):
        # Callable that runs normally; timeout=1 should not truncate it.
        def nb(args):
            return self._success_result()

        adapter = LocalAdapter(notebook_registry={"NB": nb})
        result = adapter.run_notebook("NB", 1, {})
        assert result == self._success_result()


# ---------------------------------------------------------------------------
# 8.5 — run_notebooks_parallel
# ---------------------------------------------------------------------------

class TestRunNotebooksParallel:
    def _make_dag(self, activities: list, concurrency: int | None = None) -> dict:
        dag: dict = {"activities": activities}
        if concurrency is not None:
            dag["concurrency"] = concurrency
        return dag

    def _activity(self, name: str, path: str, args: dict | None = None) -> dict:
        return {"name": name, "path": path, "args": args or {}}

    def test_all_succeed(self):
        def nb(args):
            return ExecutionResult(status="success", has_error=False).to_json()

        adapter = LocalAdapter(notebook_registry={"NB_A": nb, "NB_B": nb})
        dag = self._make_dag([
            self._activity("act_a", "NB_A"),
            self._activity("act_b", "NB_B"),
        ])
        results = adapter.run_notebooks_parallel(dag)

        assert set(results.keys()) == {"act_a", "act_b"}
        for act_name, res in results.items():
            payload = json.loads(res["exitVal"])
            assert payload["has_error"] is False

    def test_one_fails_other_succeeds(self):
        def nb_ok(args):
            return ExecutionResult(status="success", has_error=False).to_json()

        def nb_fail(args):
            raise RuntimeError("Deliberate failure")

        adapter = LocalAdapter(notebook_registry={"NB_OK": nb_ok, "NB_FAIL": nb_fail})
        dag = self._make_dag([
            self._activity("act_ok", "NB_OK"),
            self._activity("act_fail", "NB_FAIL"),
        ])
        results = adapter.run_notebooks_parallel(dag)

        ok_payload = json.loads(results["act_ok"]["exitVal"])
        fail_payload = json.loads(results["act_fail"]["exitVal"])

        assert ok_payload["has_error"] is False
        assert fail_payload["has_error"] is True
        assert fail_payload["error_details"]["error_type"] == "RuntimeError"

    def test_failed_notebook_error_details_has_the_pinned_shape(self):
        """F114: error_details is ErrorDetails' dict form, with exactly the keys it always had."""
        def nb_fail(args):
            raise RuntimeError("Deliberate failure")

        results = LocalAdapter(notebook_registry={"NB": nb_fail}).run_notebooks_parallel(
            self._make_dag([self._activity("act", "NB")]))
        details = json.loads(results["act"]["exitVal"])["error_details"]
        assert set(details) == {"error_type", "error_message", "traceback"}
        assert (details["error_type"], details["error_message"]) == ("RuntimeError", "Deliberate failure")
        assert "RuntimeError: Deliberate failure" in details["traceback"]

    def test_respects_concurrency_param(self):
        def nb(args):
            return ExecutionResult(status="success", has_error=False).to_json()

        adapter = LocalAdapter(notebook_registry={"NB": nb})
        dag = self._make_dag(
            [self._activity("act_a", "NB"), self._activity("act_b", "NB")],
            concurrency=1,
        )
        results = adapter.run_notebooks_parallel(dag)

        # Both complete; sequential order is not asserted because threading is not deterministic.
        assert len(results) == 2

    def test_no_registry_propagates_not_implemented_into_result(self):
        adapter = LocalAdapter()
        dag = self._make_dag([self._activity("act_a", "NB_A")])
        results = adapter.run_notebooks_parallel(dag)

        payload = json.loads(results["act_a"]["exitVal"])
        assert payload["has_error"] is True
        assert payload["error_details"]["error_type"] == "NotImplementedError"

    def test_empty_dag_returns_empty_results(self):
        adapter = LocalAdapter(notebook_registry={})
        results = adapter.run_notebooks_parallel({"activities": []})
        assert results == {}

    def test_args_passed_to_callable(self):
        received: list = []

        def nb(args):
            received.append(args)
            return ExecutionResult(status="success", has_error=False).to_json()

        adapter = LocalAdapter(notebook_registry={"NB": nb})
        dag = self._make_dag([self._activity("act", "NB", args={"foo": "bar"})])
        adapter.run_notebooks_parallel(dag)

        assert received == [{"foo": "bar"}]


# ---------------------------------------------------------------------------
# 8.7 — Integration tests (require delta-spark)
# ---------------------------------------------------------------------------

class TestLocalAdapterIntegration:
    """Integration tests that require ``pip install spark_quick_start[local]``.

    Skipped automatically if delta-spark is not installed.
    """

    @pytest.fixture(autouse=True)
    def _require_delta(self, spark, tmp_path: Path):
        """Probe the capability, not the shape of sys.modules.

        Two shape-based versions of this check have now been wrong. The first asserted
        that "a real installation always carries a __version__ attribute" and skipped
        every genuine delta-spark 3.2.0, whose `delta/__init__.py` exports only
        DeltaTable and configure_spark_with_delta_pip; its skip message reported a real
        install as a stub. The replacement keyed on configure_spark_with_delta_pip
        instead, which is true of a real package but still reads `sys.modules` — and
        `adapters/platform_adapter.py` imports delta at module scope, so whether the real
        package or a test module's `types.ModuleType` stub is there depends on collection
        order. `pytest tests/unit/test_native_scd_transformer.py tests/integration/local/`
        would have skipped on a machine with delta-spark installed, silently, which is the
        same class of defect F30 is.

        A write settles it regardless of order, and it is also the only check that covers
        the package-present-but-JARs-absent case — the one this test used to fail on with
        ClassNotFoundException rather than skip (F30).
        """
        pytest.importorskip("delta", reason="delta-spark not installed; skipping integration test")
        probe = str(tmp_path / "_delta_probe")
        try:
            spark.range(1).write.format("delta").mode("overwrite").save(probe)
        except Exception as exc:  # pragma: no cover — environment-dependent
            pytest.skip(f"Delta not usable in this Spark session: {type(exc).__name__}")

    def test_delete_path_removes_delta_table_directory(self, spark, tmp_path: Path):
        # Uses the shared session fixture (tests/conftest.py) rather than building
        # its own. The previous inline builder asked for spark.sql.extensions,
        # which is a *static* config: if any other module had already created the
        # session, Delta support was silently dropped and this test ran against a
        # session that could not write Delta at all. It also called spark.stop()
        # on the way out, tearing the JVM session down for every test after it.
        delta_path = str(tmp_path / "delta_table")
        spark.range(5).write.format("delta").mode("overwrite").save(delta_path)
        assert Path(delta_path).exists()

        adapter = LocalAdapter()
        adapter.delete_path(delta_path)

        assert not Path(delta_path).exists()


# ---------------------------------------------------------------------------
# LoadOrchestrator._resolve_adapter("local") — relocated from
# tests/test_load_orchestrator.py during the platform-scoped test split.
# ---------------------------------------------------------------------------

class TestResolveLocalAdapter:
    def test_resolve_local_adapter(self):
        from spark_quick_start.orchestration import LoadOrchestrator

        adapter = LoadOrchestrator._resolve_adapter("local")
        assert isinstance(adapter, LocalAdapter)
