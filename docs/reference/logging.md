# Logging

Two layers. `OrchestratorLogger`, in `src/spark_quick_start/logger/orchestrator_logger.py`, is
the logger `LoadOrchestrator` gives every step; it wraps a `LogSession` when there is one to
persist to and a plain Python logger otherwise. `spark_quick_start.spark_logging`, in
`src/spark_quick_start/spark_logging/`, is the session itself: it buffers log records and
stdout and stderr, then writes one JSON document per run through a persistence adapter. A load
configures it through the `logging_context` section of the
[load context](load-context.md#loggingcontext); a staging notebook can call it directly.

## `OrchestratorLogger`

`LoadOrchestrator.run()` builds one with `from_logging_context` unless one was passed to the
`LoadOrchestrator` constructor as `logger=`. The logging methods' `tags`, `metrics` and other
keyword arguments reach the record only through a session; without one, or when the session
call fails, they are dropped, and the constructor's `correlation_id`, `pipeline_name` and
`notebook_name` are stamped on the record and prefixed to the message.

| Member | Kind | Type | Default | Meaning |
|---|---|---|---|---|
| `name` | parameter | `str` | `"orchestrator"` | The name of the plain Python logger used when there is no session. |
| `level` | parameter | `str \| int` | `"INFO"` | The plain logger's level, as a name or a number. An unknown name means `INFO`. |
| `correlation_id` | parameter | `str \| None` | `None` | Keyword-only. With no `log_session`, stamped on every plain-logger record as a `correlation_id` attribute and a `[correlation_id=...]` message prefix; with one, not read here, the session carries it. |
| `notebook_name` | parameter | `str \| None` | `None` | Keyword-only. With no `log_session`, stamped on every plain-logger record as a `notebook_name` attribute and a `notebook=...` prefix entry; with one, not read here. |
| `pipeline_name` | parameter | `str \| None` | `None` | Keyword-only. With no `log_session`, stamped on every plain-logger record as a `pipeline_name` attribute and a `pipeline=...` prefix entry; with one, not read here, the session carries it. `from_logging_context` passes it on. Parameters left as `None` are omitted. |
| `log_session` | parameter | `LogSession \| None` | `None` | Keyword-only. The started session to write through. When `None`, messages go to the plain logger, which prints to stdout. |
| `verbose` | parameter | `bool` | `False` | Keyword-only. When false, `debug` messages are dropped, and so is an `info` message that matches a fixed list of low-signal phrases and none of a fixed list of always-on ones, unless it carries non-empty `metrics` or `important=True`. |
| `from_logging_context` | classmethod, `(logging_context, correlation_id, adapter, pipeline_name=None) -> OrchestratorLogger` | — | — | Builds the logger for a load. It asks `adapter.get_log_persistence_adapter()` for a destination; when there is one, it starts a `LogSession` that writes to `log_file_dir` (default `Files/Logs`) named after `log_file_name`, capturing stdout and stderr but not the Spark driver log. A `log_file_dir` starting with `/` switches the destination to `LocalFilePersistenceAdapter`; that prefix is the whole test, so a URI or a relative path such as `Files/Logs` does not. When the platform gives no destination, or the session cannot start, it logs to the plain logger only. |
| `debug` | method, `(msg, *, tags=None, metrics=None, **kwargs) -> None` | — | — | Logs at `DEBUG`, only when `verbose`. `tags` is a dict and `metrics` a list of `{"metricName": ..., "metricValue": ...}`, both attached to the record. |
| `info` | method, `(msg, *, tags=None, metrics=None, important=False, **kwargs) -> None` | — | — | Logs at `INFO`, subject to the `verbose` filter above. |
| `warning` | method, `(msg, *, tags=None, metrics=None, **kwargs) -> None` | — | — | Logs at `WARNING`, always. |
| `error` | method, `(msg, *, tags=None, metrics=None, **kwargs) -> None` | — | — | Logs at `ERROR`, always. |
| `finalize` | method, `(success=True) -> str` | — | — | Finalizes the session and returns its JSON; `""` with no session, or when finalizing fails, which is logged as a warning and never raised. |

## `spark_quick_start.spark_logging`

The package's `__all__` lists these classes and functions. A platform-filtered build removes the
other platform's persistence adapter: it is left out of `__all__`, and its name, imported
explicitly, is `None`.

| Name | Kind | Meaning |
|---|---|---|
| `LogSession` | class | One logging session: buffers records and output, then writes the run's JSON. |
| `init_logs` | function | Starts the package's one shared session, or returns it when it is already running. |
| `get_logger` | function | A logger that writes into the shared session. |
| `finalize_logs` | function | Finalizes the shared session. |
| `get_logs_json` | function | The shared session's full log as JSON. |
| `get_logs` | function | The shared session's full log as a dict. |
| `install_error_capture` | function | Finalizes the shared session with the traceback when a notebook cell fails. |
| `python_to_spark_level` | function | Translates a Python logging level to a Spark log level name. |
| `LogArtifacts` | dataclass | Where a session wrote its files. |
| `LogRow` | dataclass | One session as a row of a log table. |
| `LogPersistenceAdapter` | abstract base class | Where a session writes: the interface below. |
| `FabricPersistenceAdapter` | adapter | Writes through `mssparkutils.fs` on Fabric. |
| `DatabricksPersistenceAdapter` | adapter | Writes through `dbutils.fs`, resolving `dbutils` as `DatabricksAdapter` does: the SDK, then `pyspark.dbutils`, then a module. |
| `LocalFilePersistenceAdapter` | adapter | Writes to the local filesystem. |
| `LogTableAdapter` | adapter | Appends one row per session to a Delta table instead of writing files. |
| `NullPersistenceAdapter` | adapter | Writes nothing; used by in-memory sessions. |
| `CapturingPersistenceAdapter` | test double | Records every call instead of writing. |

## `LogSession`

Every constructor parameter is keyword-only. The session writes into Python's root logger:
`start()` replaces every handler on it with the session's buffer, and the constructor sets the
root level to `level`.

| Member | Kind | Type | Default | Meaning |
|---|---|---|---|---|
| `lakehouse_dir` | parameter | `str` | `"Files/Logs"` | The directory the JSON file is written to. |
| `name_prefix` | parameter | `str` | `"notebook"` | Starts the file name, `<name_prefix>_<YYYYMMDD_T_HHMMSS>_<run_id>_python.json` (the stamp is UTC), and names the session's logger. |
| `level` | parameter | `str \| int` | `"INFO"` | The lowest level captured. An unknown name means `INFO`. |
| `capture_spark` | parameter | `bool` | `True` | Sets the Spark log level, then tries to attach a Log4j 1.x file appender to the driver; when the Log4j 1.x classes are not there, no file is captured. Needs `spark_session` or an adapter that supplies one, and a session with a `sparkContext`; otherwise a warning, not an error. |
| `capture_stdout` | parameter | `bool` | `True` | Captures stdout and stderr between `start()` and `finalize()`; the last 50,000 characters of each are kept. |
| `auto_exit` | parameter | `bool` | `False` | Used as a context manager only: on exit, passes the summary to the adapter's `notebook_exit` and swallows any exception. |
| `max_return_chars` | parameter | `int` | `250000` | Deprecated: never read; passing a non-default value warns. |
| `always_persist` | parameter | `bool` | `True` | Deprecated: never read; passing a non-default value warns. A session that is not in memory always writes. |
| `persist_spark_driver_file` | parameter | `bool` | `False` | Also copies the captured Spark driver log next to the JSON file. |
| `in_memory` | parameter | `bool` | `False` | Writes nothing; `finalize()` returns the full log. When false, `adapter` is required and its absence raises `ValueError`. |
| `adapter` | parameter | `LogPersistenceAdapter \| None` | `None` | Where the session writes. `NullPersistenceAdapter` when `in_memory` and none is given. |
| `spark_session` | parameter | `SparkSession \| None` | `None` | The session for Spark log capture; else the adapter's `get_spark_session()`. |
| `correlation_id` | parameter | `str \| None` | `None` | Added to the log's `meta` and to every record. |
| `notebook_name` | parameter | `str \| None` | `None` | Added to `meta` and every record; when `None`, the adapter's `get_notebook_name()`. |
| `context` | parameter | `dict[str, Any] \| None` | `None` | Added to `meta`; its plain values are also added to every record. |
| `pipeline_name` | parameter | `str \| None` | `None` | Added to `meta` and every record. |
| `include_context_keys_in_records` | parameter | `list[str] \| None` | `None` | Keys of a record's `extra` copied into its entry even when the value is not a plain one; plain values are copied anyway. |
| `strip_fields` | parameter | `frozenset` | `frozenset()` | Field names removed from every record entry. `OrchestratorLogger` strips `run_id`, `correlation_id` and `name`. Not accepted by `init_logs`. |
| `suppress_stdout_tail` | parameter | `bool` | `False` | Leaves `stdout_tail` empty in the log. |
| `start` | method, `() -> LogSession` | — | — | Begins capture and returns the session; does nothing while the session is running, and nothing once it is finalized: call `init_logs` or build a new `LogSession` to log again. |
| `debug` | method, `(msg, **kwargs) -> None` | — | — | Logs at `DEBUG` through the session's logger. |
| `info` | method, `(msg, **kwargs) -> None` | — | — | Logs at `INFO`. |
| `warning` | method, `(msg, **kwargs) -> None` | — | — | Logs at `WARNING`. |
| `error` | method, `(msg, **kwargs) -> None` | — | — | Logs at `ERROR`. |
| `note_error` | method, `(etype, message, tb_text) -> None` | — | — | Records an error for `finalize()` to put in the log's `error`. |
| `finalize` | method, `(success=True) -> str` | — | — | Stops capture and builds the log: `run_id`, `base_name`, `success`, `meta`, the records (each with an `asctime` in UTC, `YYYY-MM-DD HH:MM:SS.mmmZ`), the output tails, the Spark driver tail and `error`. In memory, returns it. Otherwise writes it as a file, adds a table row when the adapter has `finalize_to_table`, and returns a summary with the file paths instead of the records. Returns the same value if called again, and a warning JSON if the session never started. |
| `get_logs_json` | method, `() -> str` | — | — | The full log as JSON, finalizing first if needed. |
| `get_logs` | method, `() -> dict[str, Any]` | — | — | The full log as a dict, finalizing first if needed. |

## `init_logs`

Starts the shared session, `LogSession(...).start()`, unless one is already running, in which
case it returns that one and ignores its arguments. Its parameters are `LogSession`'s, all
keyword-only and with the same meaning, except that `strip_fields` is not accepted and
`capture_spark` defaults to `False`.

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `lakehouse_dir` | `str` | `"Files/Logs"` | As for `LogSession`. |
| `name_prefix` | `str` | `"notebook"` | As for `LogSession`. |
| `level` | `str \| int` | `"INFO"` | As for `LogSession`. |
| `capture_spark` | `bool` | `False` | As for `LogSession`, with the opposite default. |
| `capture_stdout` | `bool` | `True` | As for `LogSession`. |
| `auto_exit` | `bool` | `False` | As for `LogSession`. |
| `max_return_chars` | `int` | `250000` | As for `LogSession`: deprecated, never read; passing a non-default value warns. |
| `always_persist` | `bool` | `True` | As for `LogSession`: deprecated, never read; passing a non-default value warns. |
| `persist_spark_driver_file` | `bool` | `False` | As for `LogSession`. |
| `in_memory` | `bool` | `False` | As for `LogSession`. |
| `adapter` | `LogPersistenceAdapter \| None` | `None` | As for `LogSession`: required unless `in_memory`. |
| `spark_session` | `SparkSession \| None` | `None` | As for `LogSession`. |
| `correlation_id` | `str \| None` | `None` | As for `LogSession`. |
| `notebook_name` | `str \| None` | `None` | As for `LogSession`. |
| `context` | `dict[str, Any] \| None` | `None` | As for `LogSession`. |
| `pipeline_name` | `str \| None` | `None` | As for `LogSession`. |
| `include_context_keys_in_records` | `list[str] \| None` | `None` | As for `LogSession`. |
| `suppress_stdout_tail` | `bool` | `False` | As for `LogSession`. |

## `get_logger`

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `name` | `str \| None` | `None` | The logger to return; when `None`, the session's own. Starts an in-memory shared session when none is running. |

## `finalize_logs`

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `success` | `bool` | `True` | Keyword-only. Recorded as the log's `success`. Starts and finalizes an in-memory session when there is none. |
| `auto_exit` | `bool` | `False` | Keyword-only. When true, passes the result to the adapter's `notebook_exit`. |

`get_logs_json()` and `get_logs()` take no parameters; with no shared session they return
`{"error": "No active log session"}`, as JSON or as a dict.

## `install_error_capture`

Hooks IPython's exception handler, its post-cell event and `sys.excepthook`, each once per
process. On an unhandled error it records the exception on the shared session and finalizes it
with `success` false, once.

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `auto_exit` | `bool` | `False` | Passed to `finalize_logs`. |

## `python_to_spark_level`

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `level` | `str \| int` | required | Returns `FATAL`, `ERROR`, `WARN`, `INFO`, `DEBUG`, or `ALL` for `NOTSET`. An unknown name raises `ValueError`; any other type raises `TypeError`. |

## `LogArtifacts`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `base_name` | `str` | required | `<name_prefix>_<timestamp>_<run_id>`, the stem of every file. The timestamp is UTC. |
| `run_id` | `str` | required | The session's id: 12 hexadecimal characters. |
| `python_json_path` | `str \| None` | `None` | Where the JSON log was written. |
| `spark_driver_log_path` | `str \| None` | `None` | Where the Spark driver log was copied, when it was. |

## `LogRow`

What `finalize()` hands to an adapter with `finalize_to_table`; `LogTableAdapter` writes it with
the same column names.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `run_id` | `str` | required | The session's id. |
| `success` | `bool` | required | As passed to `finalize()`. |
| `log_timestamp` | `datetime` | required | When the session finalized, in UTC. |
| `correlation_id` | `str \| None` | `None` | The session's correlation id. |
| `pipeline_name` | `str \| None` | `None` | The session's pipeline name. |
| `notebook_name` | `str \| None` | `None` | The session's notebook name. |
| `python_logs` | `str` | `""` | The records, as a JSON array. |
| `stdout_tail` | `str` | `""` | The captured stdout tail. |
| `stderr_tail` | `str` | `""` | The captured stderr tail. |
| `spark_driver_tail` | `str` | `""` | The last 200,000 characters of the Spark driver log. Its lines carry the driver JVM's time zone, with no marker, because Log4j's `PatternLayout` takes no zone; unlike the records' `asctime`, they are not UTC. |
| `context` | `str` | `"{}"` | The session's `context`, as JSON. |
| `error` | `str \| None` | `None` | The noted error, as JSON. |

## `LogPersistenceAdapter`

| Method | Signature | Returns | Meaning |
|---|---|---|---|
| `mkdir` | `(path: str)` | `None` | Abstract. Creates the directory and its parents. |
| `write` | `(path: str, content: str)` | `None` | Abstract. Writes the text, replacing any file there. |
| `copy` | `(src: str, dst: str)` | `None` | Abstract. Copies one file. |
| `notebook_exit` | `(value: str)` | `None` | Ends the notebook with `value`. The default does nothing; Fabric and Databricks call the platform's exit. |
| `get_notebook_name` | `()` | `str \| None` | The running notebook's name. The default returns `None`; Fabric, Databricks and Local read it from the runtime and return `None` when they cannot. |
| `get_spark_session` | `()` | `SparkSession \| None` | A session for Spark log capture. The default returns `None`; `LogTableAdapter` returns its own. |

## `LogTableAdapter`

Its `mkdir`, `write` and `copy` do nothing.

| Member | Kind | Type | Default | Meaning |
|---|---|---|---|---|
| `spark` | parameter | `SparkSession` | required | The session used to write. |
| `table_path` | parameter | `str` | required | The Delta table's path, created on the first write. Also readable as the attribute `table_path`. |
| `finalize_to_table` | method, `(row: LogRow) -> None` | — | — | Appends the row to the table. |

## `CapturingPersistenceAdapter`

| Member | Kind | Type | Default | Meaning |
|---|---|---|---|---|
| `dirs` | recorded | `list[str]` | `[]` | Each path passed to `mkdir`. |
| `written` | recorded | `dict[str, str]` | `{}` | Each path passed to `write`, to its content. |
| `copies` | recorded | `list[tuple[str, str]]` | `[]` | `(src, dst)` per `copy`. |
| `exit_values` | recorded | `list[str]` | `[]` | Each value passed to `notebook_exit`. |
| `finalized_rows` | recorded | `list[LogRow]` | `[]` | Each row passed to `finalize_to_table`. |
| `finalize_to_table` | method, `(row: LogRow) -> None` | — | — | Records the row. Its presence makes a session hand it one. |

## See also

- [Load-context reference](load-context.md#loggingcontext): the settings a load passes to the logger.
- [Execution-result reference](execution-result.md): `ExecutionResult.logs` carries the JSON `finalize()` returns.
