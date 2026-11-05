"""
Tests for _is_uc_name and the four default adapter Delta methods.

No Spark or Delta runtime required — DataFrame/SparkSession are mocked.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from spark_quick_start.adapters.platform_adapter import (
    MockPlatformAdapter,
    _is_uc_name,
)

# ---------------------------------------------------------------------------
# _is_uc_name
# ---------------------------------------------------------------------------

class TestIsUcName:
    def test_absolute_unix_path(self):
        assert _is_uc_name("/mnt/delta/entity") is False

    def test_abfss_uri(self):
        assert _is_uc_name("abfss://container@account.dfs.core.windows.net/path") is False

    def test_dbfs_uri(self):
        assert _is_uc_name("dbfs:/user/hive/warehouse/entity") is False

    def test_s3_uri(self):
        assert _is_uc_name("s3://bucket/prefix/entity") is False

    @pytest.mark.parametrize("scheme", [
        "hdfs", "abfs", "abfss", "wasb", "wasbs", "s3", "s3a", "s3n", "adl", "gs", "dbfs", "file",
        "foo+bar", "x-1.y",
    ])
    def test_any_uri_scheme_is_a_path(self, scheme):
        assert _is_uc_name(f"{scheme}://host/path/entity") is False

    @pytest.mark.parametrize("identifier", ["table", "schema.table", "catalog.schema.table"])
    def test_one_to_three_part_names_are_still_names(self, identifier):
        assert _is_uc_name(identifier) is True

    def test_windows_drive_path(self):
        assert _is_uc_name("C:\\Users\\data\\entity") is False

    def test_unc_path(self):
        assert _is_uc_name("\\\\server\\share\\entity") is False

    def test_three_part_uc_name(self):
        assert _is_uc_name("catalog.schema.table") is True

    def test_three_part_uc_name_with_underscores(self):
        assert _is_uc_name("my_catalog.my_schema.my_table__NB_Stage") is True

    def test_three_part_uc_name_with_numbers(self):
        assert _is_uc_name("dbx_test_ca_01_7405617526458037.stage.Silver_Orders") is True


# ---------------------------------------------------------------------------
# MockPlatformAdapter.read_delta
# ---------------------------------------------------------------------------

class TestMockAdapterReadDelta:
    def test_returns_configured_dataframe(self):
        mock_df = MagicMock()
        adapter = MockPlatformAdapter(delta_read_result=mock_df)
        spark = MagicMock()
        result = adapter.read_delta(spark, "catalog.schema.table")
        assert result is mock_df
        assert not mock_df.method_calls

    def test_records_call(self):
        adapter = MockPlatformAdapter()
        spark = MagicMock()
        adapter.read_delta(spark, "/tmp/delta/entity")
        assert adapter.read_delta_calls == ["/tmp/delta/entity"]


# ---------------------------------------------------------------------------
# MockPlatformAdapter.write_delta_overwrite
# ---------------------------------------------------------------------------

class TestMockAdapterWriteDeltaOverwrite:
    def test_records_call(self):
        adapter = MockPlatformAdapter()
        df = MagicMock()
        adapter.write_delta_overwrite(df, "catalog.schema.table")
        assert adapter.write_delta_overwrite_calls == [("catalog.schema.table", False)]
        assert not df.method_calls

    def test_records_overwrite_schema_flag(self):
        adapter = MockPlatformAdapter()
        df = MagicMock()
        adapter.write_delta_overwrite(df, "/tmp/staging", overwrite_schema=True)
        assert adapter.write_delta_overwrite_calls == [("/tmp/staging", True)]
        assert not df.method_calls


# ---------------------------------------------------------------------------
# MockPlatformAdapter.delta_table_exists
# ---------------------------------------------------------------------------

class TestMockAdapterDeltaTableExists:
    def test_returns_false_by_default(self):
        adapter = MockPlatformAdapter()
        spark = MagicMock()
        assert adapter.delta_table_exists(spark, "catalog.schema.table") is False

    def test_returns_true_when_configured(self):
        adapter = MockPlatformAdapter(existing_tables={"catalog.schema.table"})
        spark = MagicMock()
        assert adapter.delta_table_exists(spark, "catalog.schema.table") is True

    def test_records_call(self):
        adapter = MockPlatformAdapter()
        spark = MagicMock()
        adapter.delta_table_exists(spark, "/tmp/delta/entity")
        assert adapter.delta_table_exists_calls == ["/tmp/delta/entity"]


# ---------------------------------------------------------------------------
# Default PlatformAdapter.read_delta routing
# ---------------------------------------------------------------------------

class TestDefaultReadDeltaRouting:
    def test_uc_name_uses_spark_table(self):
        """UC name → spark.table(identifier), not spark.read.format().load()"""
        from spark_quick_start.adapters.platform_adapter import PlatformAdapter

        class ConcreteAdapter(PlatformAdapter):
            def run_notebook(self, name, timeout, args): ...
            def run_notebooks_parallel(self, dag): ...
            def delete_path(self, path): ...
            def get_current_notebook_name(self): ...

        adapter = ConcreteAdapter()
        spark = MagicMock()
        expected_df = MagicMock()
        spark.table.return_value = expected_df

        result = adapter.read_delta(spark, "catalog.schema.table")

        spark.table.assert_called_once_with("catalog.schema.table")
        spark.read.format.assert_not_called()
        assert result is expected_df
        assert not expected_df.method_calls

    def test_path_uses_spark_read_load(self):
        """Filesystem path → spark.read.format('delta').load(path)"""
        from spark_quick_start.adapters.platform_adapter import PlatformAdapter

        class ConcreteAdapter(PlatformAdapter):
            def run_notebook(self, name, timeout, args): ...
            def run_notebooks_parallel(self, dag): ...
            def delete_path(self, path): ...
            def get_current_notebook_name(self): ...

        adapter = ConcreteAdapter()
        spark = MagicMock()
        expected_df = MagicMock()
        spark.read.format.return_value.load.return_value = expected_df

        result = adapter.read_delta(spark, "/tmp/delta/entity")

        spark.read.format.assert_called_once_with("delta")
        spark.read.format.return_value.load.assert_called_once_with("/tmp/delta/entity")
        assert result is expected_df
        assert not expected_df.method_calls


# ---------------------------------------------------------------------------
# Default PlatformAdapter.delta_table_exists routing
# ---------------------------------------------------------------------------

class TestDefaultDeltaTableExistsRouting:
    def test_uc_name_uses_catalog_table_exists(self):
        from spark_quick_start.adapters.platform_adapter import PlatformAdapter

        class ConcreteAdapter(PlatformAdapter):
            def run_notebook(self, name, timeout, args): ...
            def run_notebooks_parallel(self, dag): ...
            def delete_path(self, path): ...
            def get_current_notebook_name(self): ...

        adapter = ConcreteAdapter()
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True

        result = adapter.delta_table_exists(spark, "catalog.schema.table")

        spark.catalog.tableExists.assert_called_once_with("catalog.schema.table")
        assert result is True

    def test_path_uses_delta_is_delta_table(self):
        from spark_quick_start.adapters.platform_adapter import PlatformAdapter

        class ConcreteAdapter(PlatformAdapter):
            def run_notebook(self, name, timeout, args): ...
            def run_notebooks_parallel(self, dag): ...
            def delete_path(self, path): ...
            def get_current_notebook_name(self): ...

        adapter = ConcreteAdapter()
        spark = MagicMock()

        with patch("spark_quick_start.adapters.platform_adapter.DeltaTable") as mock_dt:
            mock_dt.isDeltaTable.return_value = True
            result = adapter.delta_table_exists(spark, "/tmp/delta/entity")

        mock_dt.isDeltaTable.assert_called_once_with(spark, "/tmp/delta/entity")
        assert result is True

    def test_exception_returns_false(self):
        from spark_quick_start.adapters.platform_adapter import PlatformAdapter

        class ConcreteAdapter(PlatformAdapter):
            def run_notebook(self, name, timeout, args): ...
            def run_notebooks_parallel(self, dag): ...
            def delete_path(self, path): ...
            def get_current_notebook_name(self): ...

        adapter = ConcreteAdapter()
        spark = MagicMock()
        spark.catalog.tableExists.side_effect = Exception("catalog error")

        result = adapter.delta_table_exists(spark, "catalog.schema.table")
        assert result is False

    def test_a_path_without_delta_spark_raises_the_install_hint(self):
        """A path is checked with ``DeltaTable``; without delta-spark the answer is not "absent".

        Answering False would send the load down the first-load branch, which then fails at
        the create with an "already exists" error that names neither cause.
        """
        from spark_quick_start.adapters.platform_adapter import PlatformAdapter

        class ConcreteAdapter(PlatformAdapter):
            def run_notebook(self, name, timeout, args): ...
            def run_notebooks_parallel(self, dag): ...
            def delete_path(self, path): ...
            def get_current_notebook_name(self): ...

        with patch("spark_quick_start.adapters.platform_adapter.DeltaTable", None), \
                pytest.raises(RuntimeError, match="delta-spark is not installed"):
            ConcreteAdapter().delta_table_exists(MagicMock(), "/tmp/delta/entity")

    def test_a_name_without_delta_spark_still_uses_the_catalog(self):
        from spark_quick_start.adapters.platform_adapter import PlatformAdapter

        class ConcreteAdapter(PlatformAdapter):
            def run_notebook(self, name, timeout, args): ...
            def run_notebooks_parallel(self, dag): ...
            def delete_path(self, path): ...
            def get_current_notebook_name(self): ...

        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        with patch("spark_quick_start.adapters.platform_adapter.DeltaTable", None):
            assert ConcreteAdapter().delta_table_exists(spark, "catalog.schema.table") is True


# ---------------------------------------------------------------------------
# Default PlatformAdapter.write_delta_overwrite routing
# ---------------------------------------------------------------------------

class TestDefaultWriteDeltaOverwriteRouting:
    def _make_adapter(self):
        from spark_quick_start.adapters.platform_adapter import PlatformAdapter

        class ConcreteAdapter(PlatformAdapter):
            def run_notebook(self, name, timeout, args): ...
            def run_notebooks_parallel(self, dag): ...
            def delete_path(self, path): ...
            def get_current_notebook_name(self): ...

        return ConcreteAdapter()

    def test_uc_name_uses_save_as_table(self):
        """UC name → writer.saveAsTable(identifier)"""
        adapter = self._make_adapter()
        df = MagicMock()
        writer = MagicMock()
        df.write.format.return_value.mode.return_value = writer
        writer.option.return_value = writer

        adapter.write_delta_overwrite(df, "catalog.schema.table")

        writer.saveAsTable.assert_called_once_with("catalog.schema.table")
        writer.save.assert_not_called()

    def test_path_uses_save(self):
        """Filesystem path → writer.save(identifier)"""
        adapter = self._make_adapter()
        df = MagicMock()
        writer = MagicMock()
        df.write.format.return_value.mode.return_value = writer
        writer.option.return_value = writer

        adapter.write_delta_overwrite(df, "/tmp/delta/entity")

        writer.save.assert_called_once_with("/tmp/delta/entity")
        writer.saveAsTable.assert_not_called()

    def test_overwrite_schema_option_applied(self):
        """overwrite_schema=True → writer.option('overwriteSchema', 'true') called"""
        adapter = self._make_adapter()
        df = MagicMock()
        writer = MagicMock()
        df.write.format.return_value.mode.return_value = writer
        writer.option.return_value = writer

        adapter.write_delta_overwrite(df, "/tmp/delta/entity", overwrite_schema=True)

        writer.option.assert_called_once_with("overwriteSchema", "true")


# ---------------------------------------------------------------------------
# MockPlatformAdapter.write_delta_append
# ---------------------------------------------------------------------------

class TestMockAdapterWriteDeltaAppend:
    def test_records_call(self):
        adapter = MockPlatformAdapter()
        df = MagicMock()
        adapter.write_delta_append(df, "catalog.schema.table")
        assert adapter.write_delta_append_calls == ["catalog.schema.table"]
        assert not df.method_calls


# ---------------------------------------------------------------------------
# Default PlatformAdapter.write_delta_append routing
# ---------------------------------------------------------------------------

class TestDefaultWriteDeltaAppendRouting:
    def _make_adapter(self):
        from spark_quick_start.adapters.platform_adapter import PlatformAdapter

        class ConcreteAdapter(PlatformAdapter):
            def run_notebook(self, name, timeout, args): ...
            def run_notebooks_parallel(self, dag): ...
            def delete_path(self, path): ...
            def get_current_notebook_name(self): ...

        return ConcreteAdapter()

    def test_uc_name_uses_save_as_table(self):
        """UC name → writer.saveAsTable(identifier)"""
        adapter = self._make_adapter()
        df = MagicMock()
        writer = MagicMock()
        df.write.format.return_value.mode.return_value = writer

        adapter.write_delta_append(df, "catalog.schema.table")

        writer.saveAsTable.assert_called_once_with("catalog.schema.table")
        writer.save.assert_not_called()

    def test_path_uses_save(self):
        """Filesystem path → writer.save(identifier)"""
        adapter = self._make_adapter()
        df = MagicMock()
        writer = MagicMock()
        df.write.format.return_value.mode.return_value = writer

        adapter.write_delta_append(df, "/tmp/delta/entity")

        writer.save.assert_called_once_with("/tmp/delta/entity")
        writer.saveAsTable.assert_not_called()

    def test_mode_append_used_not_overwrite(self):
        """Must call .mode('append'), never .mode('overwrite')."""
        adapter = self._make_adapter()
        df = MagicMock()

        adapter.write_delta_append(df, "/tmp/delta/entity")

        df.write.format.return_value.mode.assert_called_once_with("append")


# ---------------------------------------------------------------------------
# Default PlatformAdapter.resolve_delta_table routing
# ---------------------------------------------------------------------------

class TestDefaultResolveDeltaTableRouting:
    def _make_adapter(self):
        from spark_quick_start.adapters.platform_adapter import PlatformAdapter

        class ConcreteAdapter(PlatformAdapter):
            def run_notebook(self, name, timeout, args): ...
            def run_notebooks_parallel(self, dag): ...
            def delete_path(self, path): ...
            def get_current_notebook_name(self): ...

        return ConcreteAdapter()

    def test_uc_name_uses_for_name(self):
        """UC name → DeltaTable.forName(spark, identifier)"""
        adapter = self._make_adapter()
        spark = MagicMock()

        with patch("spark_quick_start.adapters.platform_adapter.DeltaTable") as mock_dt:
            adapter.resolve_delta_table(spark, "catalog.schema.table")

        mock_dt.forName.assert_called_once_with(spark, "catalog.schema.table")
        mock_dt.forPath.assert_not_called()

    def test_path_uses_for_path(self):
        """Filesystem path → DeltaTable.forPath(spark, identifier)"""
        adapter = self._make_adapter()
        spark = MagicMock()

        with patch("spark_quick_start.adapters.platform_adapter.DeltaTable") as mock_dt:
            adapter.resolve_delta_table(spark, "/tmp/delta/entity")

        mock_dt.forPath.assert_called_once_with(spark, "/tmp/delta/entity")
        mock_dt.forName.assert_not_called()

    def test_raises_when_delta_not_installed(self):
        """When DeltaTable is None (delta-spark not installed), raises RuntimeError."""
        adapter = self._make_adapter()
        spark = MagicMock()

        with patch("spark_quick_start.adapters.platform_adapter.DeltaTable", None):
            with pytest.raises(RuntimeError, match="delta-spark is not installed"):
                adapter.resolve_delta_table(spark, "/tmp/delta/entity")


# ---------------------------------------------------------------------------
# drop_delta — UC vs path routing
# ---------------------------------------------------------------------------
class TestDefaultDropDeltaRouting:
    """drop_delta must route like the other Delta helpers.

    delete_path alone is not a substitute: every adapter implements it as a
    filesystem call (dbutils.fs.rm / shutil.rmtree), so on a UC name it would
    be read as a relative path — deleting the wrong thing at worst, and
    leaving the metastore entry behind either way.
    """

    @staticmethod
    def _make_adapter(recorder: list[str]):
        from spark_quick_start.adapters.platform_adapter import PlatformAdapter

        class ConcreteAdapter(PlatformAdapter):
            def run_notebook(self, name, timeout, args): ...
            def run_notebooks_parallel(self, dag): ...
            def delete_path(self, path): recorder.append(path)
            def get_current_notebook_name(self): ...

            def delta_table_exists(self, spark, identifier):
                # These tests are about *routing*, so the existence guard is
                # stubbed to "yes" rather than left to the default. The default
                # calls DeltaTable.isDeltaTable(), whose answer depends on
                # whether delta-spark is installed: truthy against a MagicMock
                # session where it is, False where it is not (CI installs
                # .[dev,spark,runtime], which has no delta). Relying on it made
                # this test pass locally and fail in CI.
                return True

        return ConcreteAdapter()

    def test_uc_name_uses_drop_table_not_delete_path(self):
        deleted: list[str] = []
        adapter = self._make_adapter(deleted)
        spark = MagicMock()

        adapter.drop_delta(spark, "catalog.schema.table_materialize_scratch_3")

        spark.sql.assert_called_once_with(
            "DROP TABLE IF EXISTS `catalog`.`schema`.`table_materialize_scratch_3`"
        )
        assert deleted == []

    def test_path_uses_delete_path_not_drop_table(self):
        deleted: list[str] = []
        adapter = self._make_adapter(deleted)
        spark = MagicMock()

        adapter.drop_delta(spark, "/staging/Entity_materialize_scratch_3")

        assert deleted == ["/staging/Entity_materialize_scratch_3"]
        spark.sql.assert_not_called()

    def test_never_raises_when_the_table_is_already_absent(self):
        """Dropping something already gone is the intended end state."""
        from spark_quick_start.adapters.platform_adapter import PlatformAdapter

        class RaisingAdapter(PlatformAdapter):
            def run_notebook(self, name, timeout, args): ...
            def run_notebooks_parallel(self, dag): ...
            def delete_path(self, path): raise FileNotFoundError(path)
            def get_current_notebook_name(self): ...

        RaisingAdapter().drop_delta(MagicMock(), "/staging/gone")  # must not raise

    def test_never_raises_when_drop_table_fails(self):
        adapter = self._make_adapter([])
        spark = MagicMock()
        spark.sql.side_effect = RuntimeError("no such catalog")

        adapter.drop_delta(spark, "catalog.schema.missing")  # must not raise


class TestDropDeltaIdentifierSafety:
    """drop_delta is the only place this codebase interpolates into SQL.

    DROP TABLE takes an identifier, which cannot be a bind parameter, and the
    identifier comes from caller configuration. Spark's sql() already rejects
    statement chaining ("a; DROP TABLE b" is a ParseException), so these pin
    the part that is genuinely up to us: nothing but a plain 1-3 part name is
    ever interpolated, and it is quoted.
    """

    @staticmethod
    def _make_adapter():
        from spark_quick_start.adapters.platform_adapter import PlatformAdapter

        class ConcreteAdapter(PlatformAdapter):
            def run_notebook(self, name, timeout, args): ...
            def run_notebooks_parallel(self, dag): ...
            def delete_path(self, path): ...
            def get_current_notebook_name(self): ...

        return ConcreteAdapter()

    @pytest.mark.parametrize("identifier", [
        "catalog.schema.tbl_materialize_scratch_0",
        "schema.tbl",
        "tbl",
    ])
    def test_valid_names_are_backtick_quoted(self, identifier):
        adapter = self._make_adapter()
        spark = MagicMock()

        adapter.drop_delta(spark, identifier)

        expected = ".".join(f"`{p}`" for p in identifier.split("."))
        spark.sql.assert_called_once_with(f"DROP TABLE IF EXISTS {expected}")

    @pytest.mark.parametrize("identifier", [
        "tbl`; DROP TABLE other; --",
        "a.b.c.d",                      # more than three parts
        "tbl WHERE 1=1",
        "tbl--comment",
        "tbl/*x*/",
        "",
    ])
    def test_unsafe_names_raise_instead_of_being_interpolated(self, identifier):
        adapter = self._make_adapter()
        spark = MagicMock()

        with pytest.raises(ValueError, match="unsafe table identifier"):
            adapter.drop_delta(spark, identifier)

        spark.sql.assert_not_called()


class TestDropDeltaOnlyDeletesDeltaTables:
    """drop_delta must refuse a target that exists but is not a Delta table.

    The identifier comes from configuration — staging_table_path and
    global_context.id, which build the scratch table names — and drop_delta issues a
    recursive delete with no undo. The realistic failure is not an attacker
    (editing the config already implies pipeline access) but a mistyped path or
    a config copied from another environment, and such a path lands on an
    unrelated directory far more often than on a Delta table.
    """

    @staticmethod
    def _make_adapter(deleted: list[str], *, is_delta: bool):
        from spark_quick_start.adapters.platform_adapter import PlatformAdapter

        class ConcreteAdapter(PlatformAdapter):
            def run_notebook(self, name, timeout, args): ...
            def run_notebooks_parallel(self, dag): ...
            def delete_path(self, path): deleted.append(path)
            def get_current_notebook_name(self): ...
            def delta_table_exists(self, spark, identifier): return is_delta

        return ConcreteAdapter()

    def test_deletes_a_real_delta_table(self, tmp_path):
        deleted: list[str] = []
        adapter = self._make_adapter(deleted, is_delta=True)
        target = tmp_path / "scratch"
        (target / "_delta_log").mkdir(parents=True)
        (target / "_delta_log" / "00000000000000000000.json").write_text("{}")

        adapter.drop_delta(MagicMock(), str(target))

        assert deleted == [str(target)]

    def test_a_delta_directory_is_dropped_though_isdeltatable_raises(self, tmp_path):
        """F164: DeltaTable.isDeltaTable may fail where the DeltaTable API is unavailable (F206); a visible path must not depend on it."""
        from unittest.mock import patch

        deleted: list[str] = []
        adapter = self._make_adapter(deleted, is_delta=False)
        target = tmp_path / "scratch"
        (target / "_delta_log").mkdir(parents=True)
        (target / "_delta_log" / "00000000000000000000.json").write_text("{}")
        (target / "part-0.parquet").write_text("data")

        with patch("spark_quick_start.adapters.platform_adapter.DeltaTable") as dt:
            dt.isDeltaTable.side_effect = RuntimeError("Spark Connect: no JVM")
            adapter.drop_delta(MagicMock(), str(target))

        assert deleted == [str(target)]

    def test_files_without_a_delta_log_are_refused_even_beside_other_files(self, tmp_path):
        deleted: list[str] = []
        adapter = self._make_adapter(deleted, is_delta=True)  # delta_table_exists says yes; ignored
        target = tmp_path / "scratch"
        target.mkdir()
        (target / "part-0.parquet").write_text("data")

        with pytest.raises(ValueError, match="not a Delta table"):
            adapter.drop_delta(MagicMock(), str(target))
        assert deleted == []

    def test_an_error_checking_the_path_propagates_and_nothing_is_deleted(self):
        from spark_quick_start.adapters.platform_adapter import PlatformAdapter

        deleted: list[str] = []

        class Unreadable(PlatformAdapter):
            def run_notebook(self, name, timeout, args): ...
            def run_notebooks_parallel(self, dag): ...
            def delete_path(self, path): deleted.append(path)
            def get_current_notebook_name(self): ...
            def path_has_content(self, spark, path): raise PermissionError("403 forbidden")

        with pytest.raises(PermissionError):
            Unreadable().drop_delta(MagicMock(), "abfss://c@a.dfs.core.windows.net/x")
        assert deleted == []

    def test_no_ops_when_nothing_is_there(self, tmp_path):
        """Absent is the intended end state, not an error."""
        deleted: list[str] = []
        adapter = self._make_adapter(deleted, is_delta=False)

        adapter.drop_delta(MagicMock(), str(tmp_path / "never_created"))

        assert deleted == []

    def test_refuses_an_existing_non_delta_directory(self, tmp_path):
        """The mistyped-path case: something is there, but we do not own it."""
        deleted: list[str] = []
        adapter = self._make_adapter(deleted, is_delta=False)
        victim = tmp_path / "important_data"
        victim.mkdir()
        (victim / "keepme.txt").write_text("do not delete")

        with pytest.raises(ValueError, match="not a Delta table"):
            adapter.drop_delta(MagicMock(), str(victim))

        assert deleted == []
        assert (victim / "keepme.txt").exists(), "refusal must not delete anything"

    def test_uc_name_that_is_not_a_delta_table_is_a_no_op(self):
        """catalog.schema.table has already been answered by tableExists();
        a path-based existence check does not apply to a catalog name."""
        deleted: list[str] = []
        adapter = self._make_adapter(deleted, is_delta=False)
        spark = MagicMock()

        adapter.drop_delta(spark, "catalog.schema.missing")

        spark.sql.assert_not_called()
        assert deleted == []


class TestDefaultPathHasContent:
    """F164: the portable default is the local filesystem; entries, not mere existence."""

    @staticmethod
    def _adapter():
        from spark_quick_start.adapters.platform_adapter import PlatformAdapter

        class Local(PlatformAdapter):
            def run_notebook(self, name, timeout, args): ...
            def run_notebooks_parallel(self, dag): ...
            def delete_path(self, path): ...
            def get_current_notebook_name(self): ...

        return Local()

    def test_a_directory_with_a_file_has_content(self, tmp_path):
        (tmp_path / "a.txt").write_text("x")
        assert self._adapter().path_has_content(MagicMock(), str(tmp_path)) is True

    def test_a_file_has_content(self, tmp_path):
        f = tmp_path / "a.txt"
        f.write_text("x")
        assert self._adapter().path_has_content(MagicMock(), str(f)) is True

    def test_an_empty_directory_has_none(self, tmp_path):
        assert self._adapter().path_has_content(MagicMock(), str(tmp_path)) is False

    def test_a_missing_path_has_none(self, tmp_path):
        assert self._adapter().path_has_content(MagicMock(), str(tmp_path / "gone")) is False

    def test_a_catalog_name_has_none(self):
        assert self._adapter().path_has_content(MagicMock(), "catalog.schema.table") is False

    @pytest.mark.parametrize("path", [
        "abfss://c@a.dfs.core.windows.net/Tables/x",
        "dbfs:/mnt/staging/x",
        "s3://bucket/key",
    ])
    def test_a_uri_path_answers_false_because_the_default_cannot_see_it(self, path):
        assert self._adapter().path_has_content(MagicMock(), path) is False

    def test_the_mock_records_and_answers_from_the_paths_it_was_given(self):
        from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter
        mock = MockPlatformAdapter(non_empty_paths={"/full"})
        assert mock.path_has_content(None, "/full") is True
        assert mock.path_has_content(None, "/empty") is False
        assert mock.path_has_content_calls == ["/full", "/empty"]


class TestDropDeltaAsksTheAdapterAboutThePath:
    """F164: drop_delta refuses a path a subclass's own `path_has_content` says holds files (the default sees local paths only)."""

    @staticmethod
    def _adapter(deleted, *, has_content):
        from spark_quick_start.adapters.platform_adapter import PlatformAdapter

        class Remote(PlatformAdapter):
            def run_notebook(self, name, timeout, args): ...
            def run_notebooks_parallel(self, dag): ...
            def delete_path(self, path): deleted.append(path)
            def get_current_notebook_name(self): ...
            def delta_table_exists(self, spark, identifier): return False
            def path_has_content(self, spark, path):
                return has_content and not path.endswith("/_delta_log")

        return Remote()

    def test_a_uri_path_holding_files_is_refused(self):
        deleted: list[str] = []
        with pytest.raises(ValueError, match="not a Delta table"):
            self._adapter(deleted, has_content=True).drop_delta(
                MagicMock(), "abfss://c@a.dfs.core.windows.net/Tables/Staging")
        assert deleted == []

    def test_a_uri_path_with_a_delta_log_is_dropped(self):
        deleted: list[str] = []
        from spark_quick_start.adapters.platform_adapter import PlatformAdapter

        class Delta(PlatformAdapter):
            def run_notebook(self, name, timeout, args): ...
            def run_notebooks_parallel(self, dag): ...
            def delete_path(self, path): deleted.append(path)
            def get_current_notebook_name(self): ...
            def path_has_content(self, spark, path): return True  # the path and its _delta_log

        Delta().drop_delta(MagicMock(), "abfss://c@a.dfs.core.windows.net/Tables/Staging")
        assert deleted == ["abfss://c@a.dfs.core.windows.net/Tables/Staging"]

    def test_a_uri_path_holding_nothing_is_a_no_op(self):
        deleted: list[str] = []
        self._adapter(deleted, has_content=False).drop_delta(MagicMock(), "dbfs:/mnt/staging/x")
        assert deleted == []


class TestDropDeltaOnAPathTheDefaultCannotSee:
    """F164 simplified: a path the default cannot see (a URI, a /mnt path) is decided as before
    F164, by delta_table_exists; the listing guard covers visible paths only."""

    @staticmethod
    def _adapter(deleted, *, exists):
        from spark_quick_start.adapters.platform_adapter import PlatformAdapter

        class Uri(PlatformAdapter):
            def run_notebook(self, name, timeout, args): ...
            def run_notebooks_parallel(self, dag): ...
            def delete_path(self, path): deleted.append(path)
            def get_current_notebook_name(self): ...

            def delta_table_exists(self, spark, identifier):
                self.asked.append(identifier)
                return exists

        a = Uri()
        a.asked = []
        return a

    _URI = "abfss://c@a.dfs.core.windows.net/Tables/Staging_1_2_materialize"

    def test_a_delta_table_at_a_uri_path_is_dropped_through_delta_table_exists(self):
        deleted: list[str] = []
        adapter = self._adapter(deleted, exists=True)
        adapter.drop_delta(MagicMock(), self._URI)
        assert adapter.asked == [self._URI]
        assert deleted == [self._URI]

    @pytest.mark.parametrize(("exists", "dropped"), [(True, True), (False, False)])
    def test_a_mnt_path_is_decided_by_delta_table_exists(self, exists, dropped):
        path = "/mnt/staging/x_1_2_materialize_target"
        deleted: list[str] = []
        adapter = self._adapter(deleted, exists=exists)
        adapter.drop_delta(MagicMock(), path)
        assert adapter.asked == [path]
        assert deleted == ([path] if dropped else [])

    def test_nothing_at_a_uri_path_is_a_no_op_not_a_refusal(self):
        deleted: list[str] = []
        adapter = self._adapter(deleted, exists=False)
        adapter.drop_delta(MagicMock(), self._URI)
        assert adapter.asked == [self._URI]
        assert deleted == []


def test_a_refusal_tells_the_owner_how_a_crashed_first_write_is_cleared(tmp_path):
    from spark_quick_start.adapters.platform_adapter import PlatformAdapter

    class A(PlatformAdapter):
        def run_notebook(self, name, timeout, args): ...
        def run_notebooks_parallel(self, dag): ...
        def delete_path(self, path): ...
        def get_current_notebook_name(self): ...

    target = tmp_path / "half"
    (target / "_delta_log").mkdir(parents=True)
    (target / "part-0.parquet").write_text("x")
    with pytest.raises(ValueError, match="crashed first write.*by hand"):
        A().drop_delta(MagicMock(), str(target))
