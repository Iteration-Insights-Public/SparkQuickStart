# Platform adapters

A platform adapter is where the orchestration layer touches a runtime: running notebooks, deleting
paths, naming the current notebook, signalling notebook exit, choosing where logs persist, and
reading and writing Delta tables by name or path. `PlatformAdapter` is the interface, defined in
`src/spark_quick_start/adapters/platform_adapter.py`; each platform implements it in its own
module under `src/spark_quick_start/adapters/`. Everything is exported by
`spark_quick_start.adapters`. `LoadOrchestrator` builds the platform's adapter from its
`platform` argument when no adapter is passed.

## `spark_quick_start.adapters`

A platform-filtered build deletes the other platform's adapter module; its name is then `None`
and is left out of `__all__`.

| Name | Kind | Meaning |
|---|---|---|
| `PlatformAdapter` | abstract base class | The interface. Four methods are abstract; the rest have portable defaults. |
| `FabricAdapter` | adapter | Microsoft Fabric, through `notebookutils`, imported inside each method. Takes no parameters. |
| `DatabricksAdapter` | adapter | Azure Databricks, through `dbutils`. Takes no parameters. |
| `LocalAdapter` | adapter | Local development and CI: notebooks are callables or `.ipynb` files, deletes are local. |
| `MockPlatformAdapter` | test double | Records each notebook run, delete and Delta call, and returns configured results; needs no runtime and no Spark. `notebook_exit`, `get_current_notebook_name`, `get_log_persistence_adapter` and `is_table_identifier` record nothing. |

## `PlatformAdapter`

*Overridden by* names the shipped adapters that replace the default; an abstract method has no
default and every adapter implements it.

| Method | Signature | Returns | Meaning |
|---|---|---|---|
| `run_notebook` | `(name: str, timeout: int, args: dict[str, Any])` | `str` | Abstract. Runs one notebook and returns its exit value, an `ExecutionResult` JSON string. `DatabricksAdapter` passes every argument as a string. `LocalAdapter` runs a matching `.ipynb` when `notebooks_dir` is set and one exists (see `notebooks_dir` below), else the registered callable; running a notebook file raises `ImportError` when `nbformat` is not installed; it raises `NotImplementedError` when it has neither a registry nor a directory, and `KeyError` for an unknown name. |
| `run_notebooks_parallel` | `(dag: dict[str, Any])` | `dict[str, Any]` | Abstract. Runs the DAG's `activities` and returns `{activity name: {"exitVal": ...}}`. Fabric hands the DAG to `notebookutils.notebook.runMultiple`. Databricks and Local run it on a thread pool of `concurrency` workers, default one per activity, with a per-notebook timeout of `timeoutPerCellInSeconds`, else `timeoutInSeconds`, else `3600`; a failing notebook's `exitVal` is an error `ExecutionResult`, and the others still run. |
| `delete_path` | `(path: str)` | `None` | Abstract. Deletes a path recursively: `notebookutils.fs.rm`, `dbutils.fs.rm`, or `shutil.rmtree` on Local, which resolves a relative path against `base_path` and does nothing when the path is absent. |
| `get_current_notebook_name` | `()` | `str \| None` | Abstract. The running notebook's name, or `None` when it cannot be read. Databricks returns the last segment of the notebook path; Local returns `current_notebook_name`. |
| `notebook_exit` | `(value: str)` | `None` | Signals notebook completion with `value`. The default does nothing. Overridden by Fabric (`notebookutils.notebook.exit`), Databricks (`dbutils.notebook.exit`) and Local, which logs the value and keeps running. |
| `get_log_persistence_adapter` | `()` | `Any \| None` | The log persistence backend for this platform. The default returns `None`. Overridden by Fabric (`FabricPersistenceAdapter`), Databricks (`DatabricksPersistenceAdapter`) and Local (`LocalFilePersistenceAdapter`). |
| `is_table_identifier` | `(identifier: str)` | `bool` | False for an absolute path, a Windows drive or UNC path, or a URI (any `scheme://` prefix, such as `abfss://`, `hdfs://` or `s3a://`, and `dbfs:/` or `file:`); true for anything else. A relative path such as `Files/staging/x` counts as a table name, and the number of name parts is not checked. Every Delta method below routes on this. Not overridden. |
| `read_delta` | `(spark: SparkSession, identifier: str)` | `DataFrame` | `spark.table()` for a table name, `spark.read.format("delta").load()` for a path. |
| `read_parquet` | `(spark: SparkSession, identifier: str)` | `DataFrame` | `spark.read.parquet()` at a path. Portable; no adapter overrides it. Used by `DQDTransformer`. |
| `read_table` | `(spark: SparkSession, identifier: str)` | `DataFrame` | `spark.table()` for a catalog table or view. Portable; no adapter overrides it. Used by `DQDTransformer`. |
| `read_warehouse_view` | `(spark: SparkSession, identifier: str, *, workspace_id: str \| None = None, lakehouse_id: str \| None = None)` | `DataFrame` | A Fabric Warehouse or SQL-endpoint view. `FabricAdapter` reads it with Fabric's Warehouse connector, `synapsesql`, with the ids as the `workspaceId` and `lakehouseId` options, and raises `RuntimeError` when the reader has no `synapsesql` (the connector is not loaded in the session); every other adapter raises `NotImplementedError`. Used by `DQDTransformer`. |
| `write_delta_overwrite` | `(df: DataFrame, identifier: str, overwrite_schema: bool = False)` | `None` | Overwrites the Delta table, with `saveAsTable()` for a name and `save()` for a path. `overwrite_schema=True` adds the `overwriteSchema` option. |
| `write_delta_append` | `(df: DataFrame, identifier: str)` | `None` | Appends to the Delta table, creating it when absent, with `saveAsTable()` or `save()`. |
| `delta_table_exists` | `(spark: SparkSession, identifier: str)` | `bool` | `spark.catalog.tableExists()` for a name, `DeltaTable.isDeltaTable()` for a path. Returns `False` on any error rather than raising, except one: a path checked with delta-spark not installed raises `RuntimeError` with the install hint, since `False` there would send the load down its first-load branch. |
| `drop_delta` | `(spark: SparkSession, identifier: str)` | `None` | Drops a Delta table and its data: `DROP TABLE IF EXISTS` for a name, `delete_path` for a path. Does nothing when nothing is there. A path the driver's local filesystem shows with entries (a local path, or a Databricks `/Volumes/...` path, which is FUSE-visible) is a Delta table when its `_delta_log` holds entries (asked through `path_has_content`, not `DeltaTable`), and a directory of other files is refused. Anything else (a URI, a Databricks `/mnt/...` path, an empty or absent directory) is decided by `delta_table_exists`, as before F164, and is dropped only when that answers True; `delta_table_exists` answers False on any error, and the drop then does nothing; without delta-spark installed, such a path raises its `RuntimeError` instead. Raises `ValueError` when a visible path holds files but is not a Delta table, and when an existing table's name is not a plain one-to-three-part name. Errors from checking a visible path propagate, so an unverified path is never deleted; only errors from the drop itself are swallowed. |
| `path_has_content` | `(spark: SparkSession, path: str)` | `bool` | True when the filesystem path is a directory holding at least one entry, or an existing file; an empty directory and a catalog name are False. Reads the driver's local filesystem only (a Databricks `/Volumes/...` path is visible; a `/mnt/...` path or a URI is not): such a path or a catalog name answers False, because the method cannot see it. No platform overrides it; a listing through `notebookutils.fs` or `dbutils.fs` could fail closed on a live runtime, and F129's scratch naming makes the check enough locally. Used by `drop_delta` and `write_delta_create`. |
| `write_delta_create` | `(df: DataFrame, identifier: str)` | `None` | Creates a Delta table from `df` with `mode("errorifexists")`, by catalog name or path, and raises Spark's error when a Delta table is already there, so a missed existence check cannot become an append over existing rows. For a path the driver's local filesystem shows with entries, first asks `path_has_content` and raises `ValueError`, writing nothing, when the path holds files and has no `_delta_log` entries; anything else is left to `errorifexists`. The Data Vault writers and the SCD merge (F209) create their tables with it. No platform overrides it. |
| `get_table_properties` | `(spark: SparkSession, identifier: str)` | `dict[str, str]` | Every property of the Delta table, the library's (`spark_quick_start.*`) and Delta's alike, by catalog name or path, through `DESCRIBE DETAIL`; no `DeltaTable`, so it works on Spark Connect and serverless. One Spark action on metadata. Raises `UnsupportedTableIdentifier`, a `ValueError` subclass in `adapters/platform_adapter.py` that `set_table_properties` raises too, for an identifier that is not a plain `str`, a name that is not 1 to 3 plain parts (a relative `Tables/x` counts as a name and is refused), or a path with a backtick, `$` or control character; a backslash or single quote in a path is kept, since both are literal inside the quoted name. A missing or non-Delta target raises Spark's own error: check `delta_table_exists` first. No platform overrides it. |
| `set_table_properties` | `(spark: SparkSession, identifier: str, properties: Mapping[str, str])` | `None` | Sets properties through `ALTER TABLE … SET TBLPROPERTIES`, a Delta commit of its own that conflicts with any write running on the table at the same time; nothing to set issues no SQL. The identifier is refused as `get_table_properties` refuses it. Keys must be `spark_quick_start.<lower_snake_case>`, values plain strings with no quote, backslash, backtick, `$` or control character, because both go into the statement and Spark substitutes `${…}` even inside quotes; anything else raises `ValueError` and nothing is written. No platform overrides it. |
| `resolve_delta_table` | `(spark: SparkSession, identifier: str)` | `DeltaTable` | `DeltaTable.forName()` or `DeltaTable.forPath()`. Raises `RuntimeError` when `delta-spark` is not installed. |

`MockPlatformAdapter` overrides every method except `notebook_exit`, `get_log_persistence_adapter`
and `is_table_identifier`; see its section below.

## `LocalAdapter`

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `base_path` | `str` | `""` | Prefix for a relative path passed to `delete_path`. Also readable as the attribute `base_path`. |
| `notebook_registry` | `dict[str, Callable[[dict[str, Any]], str]] \| None` | `None` | Notebook name to a callable that takes the arguments and returns an `ExecutionResult` JSON string. `{}` enables notebook runs with nothing registered yet. |
| `current_notebook_name` | `str \| None` | `None` | What `get_current_notebook_name` returns. |
| `notebooks_dir` | `str \| None` | `None` | A directory of `.ipynb` files, tried before the registry. An absolute name tries `<name>.ipynb`, then its last segment under this directory; any other name is looked up whole under this directory, subdirectories included. Running a file needs `nbformat`, and raises `ImportError` without it. |

With neither `notebook_registry` nor `notebooks_dir`, `run_notebook` raises `NotImplementedError`.

## `MockPlatformAdapter`

*Parameter* rows are constructor arguments; *recorded* rows are lists on the instance, each
appended to by one method, for a test to assert on.

| Member | Kind | Type | Default | Meaning |
|---|---|---|---|---|
| `notebook_results` | parameter | `dict[str, str] \| None` | `None` | Notebook name to the exit value `run_notebook` returns. Any other name gets a success result with `source_count` and `target_count` of 10. |
| `current_nb_name` | parameter | `str \| None` | `"TestNotebook"` | What `get_current_notebook_name` returns. |
| `raise_on_delete` | parameter | `bool` | `False` | When true, `delete_path` raises `FileNotFoundError` and records nothing. |
| `delta_read_result` | parameter | `Any \| None` | `None` | What `read_delta`, `read_parquet`, `read_table` and `read_warehouse_view` return. When `None`, a new `MagicMock`. |
| `existing_tables` | parameter | `set \| None` | `None` | The identifiers `delta_table_exists` reports as present. |
| `non_empty_paths` | parameter | `set \| None` | `None` | The paths `path_has_content` reports as holding files. |
| `table_properties` | parameter | `dict[str, dict[str, str]] \| None` | `None` | The properties `get_table_properties` returns, per identifier; `set_table_properties` updates them, so a seal written in a test is read back. An identifier in neither this nor `existing_tables` raises `RuntimeError` on read, as a missing table does for the default. |
| `run_calls` | recorded | `list[tuple[str, int, dict]]` | `[]` | `(name, timeout, args)` per notebook run, including each activity of `run_notebooks_parallel`, whose default timeout is `90`. |
| `deleted_paths` | recorded | `list[str]` | `[]` | Each path passed to `delete_path`. |
| `read_delta_calls` | recorded | `list[str]` | `[]` | Each identifier passed to `read_delta`. |
| `read_parquet_calls` | recorded | `list[str]` | `[]` | Each identifier passed to `read_parquet`. |
| `read_table_calls` | recorded | `list[str]` | `[]` | Each identifier passed to `read_table`. |
| `read_warehouse_view_calls` | recorded | `list[tuple[str, str \| None, str \| None]]` | `[]` | `(identifier, workspace_id, lakehouse_id)` per `read_warehouse_view` call. |
| `write_delta_overwrite_calls` | recorded | `list[tuple[str, bool]]` | `[]` | `(identifier, overwrite_schema)` per `write_delta_overwrite`. Nothing is written. |
| `write_delta_append_calls` | recorded | `list[str]` | `[]` | Each identifier passed to `write_delta_append`. Nothing is written. |
| `delta_table_exists_calls` | recorded | `list[str]` | `[]` | Each identifier passed to `delta_table_exists`. |
| `resolve_delta_table_calls` | recorded | `list[str]` | `[]` | Each identifier passed to `resolve_delta_table`, which returns a new `MagicMock`. |
| `path_has_content_calls` | recorded | `list[str]` | `[]` | Each path passed to `path_has_content`. |
| `drop_delta_calls` | recorded | `list[str]` | `[]` | Each identifier passed to `drop_delta`, which never refuses and never deletes. |
| `write_delta_create_calls` | recorded | `list[str]` | `[]` | Each identifier passed to `write_delta_create`, which raises `RuntimeError` for one in `existing_tables` or `table_properties` and otherwise adds it to `existing_tables`. |
| `get_table_properties_calls` | recorded | `list[str]` | `[]` | Each identifier passed to `get_table_properties`. |
| `set_table_properties_calls` | recorded | `list[tuple[str, dict[str, str]]]` | `[]` | Each identifier and properties passed to `set_table_properties`, which refuses what the default refuses. |

## See also

- [Test without a cluster](../guides/test-without-a-cluster.md): using `LocalAdapter` and
  `MockPlatformAdapter` in tests.
- [Transformer reference](transformers.md): the steps that call the adapter.
