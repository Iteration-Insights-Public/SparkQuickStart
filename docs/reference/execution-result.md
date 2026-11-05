# Execution result

`LoadOrchestrator.run()` returns an `ExecutionResult`. It is a dataclass defined in
`src/spark_quick_start/orchestration/models.py` and exported by
`spark_quick_start.orchestration`, as are `ExecutionMetrics`, the shape of its `metrics`, and
`ErrorDetails`. `StageMetrics`, the shape of each entry in `stages`, is defined in
`src/spark_quick_start/orchestration/metrics.py`. `run()` does not raise: an exception inside
the load, including a pipeline with no transformers, becomes a result with `status` `"error"`.
The cells below describe the result `run()` returns; results built elsewhere are under
[Other results](#other-results).

## `ExecutionResult`

| Member | Type | Default | Meaning |
|---|---|---|---|
| `status` | `str` | required | `"success"` or `"error"`. The notebook display also knows `"warning"`, which the library never sets. |
| `has_error` | `bool` | required | True exactly when `status` is `"error"`. |
| `error_details` | `dict[str, Any] \| None` | `None` | On an error, a dict with `error_type` (the exception's class name), `error_message` and `traceback`. `None` on success. |
| `metrics` | `dict[str, Any] \| None` | `None` | The run's `ExecutionMetrics` as a dict, on success and on error. On an error only `start_time`, `end_time` and `duration_seconds` are filled; the rest keep their defaults. |
| `logs` | `str` | `""` | The JSON string `OrchestratorLogger.finalize()` returns; `""` when finalizing failed. |
| `correlation_id` | `str` | `""` | `runtime_context.correlation_id`, which the load context must set. A UUID is generated only for an error result raised before validation completes, when no id is known yet; no row is written then. |
| `early_exit` | `bool` | `False` | True when a step halted the pipeline, as `CandidateGuardTransformer` does when there are no candidates. Always false on an error. |
| `early_exit_reason` | `str` | `""` | The halting step's reason. |
| `stages` | `list[Any]` | `[]` | One `StageMetrics` per step that completed, in pipeline order, on success and on error. The step that raised has no entry, so on an error the last entry is the step before it. A skipped conditional step has none, nor does a plain callable step; a wrapped step records under its own class name. After `from_json`, each entry is a plain dict. |
| `to_json` | method, `() -> str` | — | The result as indented JSON, every field included, `stages` as dicts. |
| `from_json` | classmethod, `(json_str: str) -> ExecutionResult` | — | Builds a result from `to_json`'s output. Keys the class does not have are ignored; a missing required key raises `TypeError`. |

`ExecutionResult` defines `_repr_markdown_`, so a notebook front end that honours it (IPython-based
ones do; it was not observed on Fabric or Databricks here) renders a result left as the cell's last
expression as a Markdown summary: the status, the early-exit reason, any error, the metrics, the log's `run_id`,
`base_name` and `success`, and its stdout and stderr tails.

## `ExecutionMetrics`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `source_count` | `int` | `0` | Rows the source step acquired. For `StagingTransformer`, the sum of the `source_count` each staging notebook reports in its own result; no Spark action here. For `DQDTransformer`, the rows it passed on when the load has an `hwm_context`, counted in the same Spark action as the watermark; `0` otherwise, like an empty result. |
| `target_count` | `int` | `0` | Rows the source step handed on: the sum of the staging notebooks' reported `target_count`, or the same count as `source_count` for a DQD. |
| `inserted_count` | `int` | `0` | Rows the writer inserted, from the write's commit metadata. `0` when no writer ran, as on an early exit. For a Data Vault hub or link, the new keys; for a satellite, the new rows, whether `new`, `changed` or `deleted`. |
| `updated_count` | `int` | `0` | Rows the writer updated; under SCD1, soft-deleted rows included. `0` for the append-only writer and the Data Vault writers, which never update. |
| `duration_seconds` | `float` | `0.0` | Wall time of `run()`, in seconds. |
| `start_time` | `str` | `""` | When `run()` started, ISO 8601 in UTC. |
| `end_time` | `str` | `""` | When `run()` finished, ISO 8601 in UTC. |
| `hwm_value_before` | `str \| None` | `None` | The high watermark before the run. With a load-context `hwm_context`, it is the step that reports for `hwm_column_name`, even when it found none (F138): for a `StagingTransformer` or `DQDTransformer` source, the context's `hwm_value`. Otherwise it is the first `HWMProvider` in pipeline order whose `hwm_after` is set: the writer's maximum of the effective-from column, read before its write. With no provider, the target's current watermark, the same value as after. `None` when not captured, as on a first load; `""` when the table existed and was empty. |
| `hwm_value_after` | `str \| None` | `None` | The high watermark after the run: the source step's latest `hwm_column_name` among the rows it passed on, or the writer's maximum read after its write, with the same fallback and sentinels. A Data Vault writer, with no `hwm_context`, reports its run's `LoadTS` here and nothing as `hwm_value_before`: a hub or satellite has no watermark of its own, and nothing is read to find one. |

## `StageMetrics`

Recorded by every step's `after_run`. Capture flags are `runtime_context` fields.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `stage` | `str` | required | The step's class name. Five changed with F155's `SCD` prefix, for example `ChangeDetectorTransformer` became `SCDChangeDetectorTransformer`; see the [transformer reference](transformers.md). |
| `duration_ms` | `int` | required | Wall time from the start of `before_run` to the end of the step's instrumentation in `after_run`, in milliseconds, instrumentation included. |
| `measure_ms` | `int \| None` | `None` | The part of `duration_ms` spent on the instrumentation below. Reading a partition count can run a shuffle, so this can be large. |
| `partitions_in` | `int \| None` | `None` | Partitions of `state.df` on entry, when `capture_partitions` is on (the default). `None` for a source step. |
| `partitions_out` | `int \| None` | `None` | Partitions of `state.df` on exit, when `capture_partitions` is on. |
| `row_count_in` | `int \| None` | `None` | Rows of `state.df` on entry, when `capture_row_counts` is on. A `count()`: a Spark action. |
| `row_count_out` | `int \| None` | `None` | Rows of `state.df` on exit, when `capture_row_counts` is on. A `count()`. |
| `inserted` | `int \| None` | `None` | Set by the `NativeSCD*` writers. |
| `updated` | `int \| None` | `None` | Set by `NativeSCDMergeTransformer` only. |
| `deleted` | `int \| None` | `None` | Not set by any shipped step; a custom step can pass it through `after_run(extra_metrics=...)`. |
| `files_added` | `int \| None` | `None` | Not set by any shipped step. |
| `files_removed` | `int \| None` | `None` | Not set by any shipped step. |
| `delta_operation` | `str \| None` | `None` | Not set by any shipped step. |
| `plan` | `str \| None` | `None` | The query plan of `state.df` on exit, when `capture_plans` is on. No Spark action. Each captured plan is also logged at `DEBUG` with the tags `event=execution_plan` and `transformer=<name>`, written only when `logging_context.verbose` is true; a failed capture logs `event=execution_plan_failed` instead. |

## `ErrorDetails`

Every error the library reports is built from this class, and `ExecutionResult.error_details`
holds its `to_dict()`: a plain dict with `error_type`, `error_message` and `traceback`, and
`context` only when it is set. The library never sets `context`, so the JSON a pipeline receives
has the first three keys only.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `error_type` | `str` | required | The exception's class name. |
| `error_message` | `str` | required | The exception's message. |
| `traceback` | `str \| None` | `None` | The formatted traceback. |
| `context` | `dict[str, Any] \| None` | `None` | Free-form context. |
| `from_exception` | classmethod, `(exc: BaseException, traceback_text: str \| None = None) -> ErrorDetails` | — | Describes `exc`: its class name and `str()`, with the traceback text the caller passes. |
| `to_dict` | method, `() -> dict[str, Any]` | — | The `error_details` dict: the first three fields, plus `context` only when it is not `None`. |

## Other results

The same class carries results the library builds outside `run()`, and their fields differ:

- A staging notebook built with the staging helper returns its own result through
  `to_json()`. It has no `stages`; its `logs` come from the notebook's log function; and its
  `start_time` and `end_time` are ISO 8601 in UTC, as `run()` stamps them.
- When a notebook cannot be run at all, the executor and the Databricks and Local
  `run_notebooks_parallel` build an error result with `error_details` only: `metrics` is `None`
  and `correlation_id` is `""`.

## See also

- [Transformer reference](transformers.md): the steps that set these values.
- [Load-context reference](load-context.md#runtimecontext): the capture flags and `correlation_id`.
