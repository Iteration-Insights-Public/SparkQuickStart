# Transformers

A load is a pipeline of transformers: one source transformer, which produces the first
`PipelineState`, then any number of process transformers, each taking a state and returning
one. Everything a pipeline is built from is exported by
`spark_quick_start.orchestration.transformers`, defined in the modules of
`src/spark_quick_start/orchestration/transformers/`. This page lists every export, then the
constructor parameters of each class that takes any, then the attributes of each protocol.
Services such as the Spark session, the platform adapter and the logger are injected when the
pipeline runs, never passed to a constructor. The load-context fields the transformers read
are on the [load-context reference](load-context.md).

Five SCD-specific steps were renamed with an `SCD` prefix (backlog F155), so the Data Vault steps
can be named by their style alike. The old names still import, each raising a
`DeprecationWarning` that names the new one, and are removed in 0.5.0. A step's stage name in
`ExecutionResult.stages` is its class name, so it changed with it.

| Old name | New name |
|---|---|
| `TargetTableTransformer` | `SCDTargetTableTransformer` |
| `IncrementalTargetTableTransformer` | `SCDIncrementalTargetTableTransformer` |
| `ChangeDetectorTransformer` | `SCDChangeDetectorTransformer` |
| `GracefulChangeDetectorTransformer` | `SCDGracefulChangeDetectorTransformer` |
| `CandidateVersionGuardTransformer` | `SCDCandidateVersionGuardTransformer` |

A `None` target written through a generic setter is deprecated too (backlog F28). It still
works, raising a `DeprecationWarning` that points at the caller; the state it returns is
`with_target(None)`. See the [`PipelineState` reference](pipeline-state.md).

| Old form | New form |
|---|---|
| `state.with_frame("target", None)` | `state.with_target(None)` |
| `state.with_updates(target=None)` | `state.with_target(None)` |

## `spark_quick_start.orchestration.transformers`

Kind *source* runs at position 0 and produces the first state; *process* runs after it and
passes through untouched once the pipeline has halted; *always runs* is a process transformer
that runs on a halted pipeline too. A class with no container below takes no constructor
parameters.

| Name | Kind | Meaning |
|---|---|---|
| `StagingTransformer` | source, `HWMProvider` | Runs the staging notebooks and reads the staged table they produced as `state.df`. Reports `source_count` and `target_count` from the notebooks' own write metrics, with no extra Spark action. It reports a watermark only with a load-context [`hwm_context`](load-context.md#hwmcontext), and leaves its three `HWMProvider` attributes `None` without one: the latest `hwm_column_name` among the staged rows, one Spark action, and `ValueError` when they lack that column. |
| `DQDTransformer` | source, `HWMProvider` | Reads one source through the adapter and applies a [DQD](dqd-definition.md)'s filters, transformations and selection, row by row. With an `hwm_context`, filters by it and reports the latest `hwm_column_name` and, as `source_count`, the rows it passed on, both from one Spark action (over the scratch table when `dqd_context.eager_materialize_source` is true, in which case the step writes a Delta table at `<staging_table_path>_<id>_0_dqd` first, see [Tables the library creates](load-context.md#tables-the-library-creates)); without it, `source_count` is not computed and the result shows 0. Raises `DQDDefinitionError` naming the entry for a malformed expression or a missing column, before the source is read for a malformed one. |
| `ConditionalSourceTransformer` | source | Runs one of two source transformers, chosen by a predicate when the pipeline runs. Built by `.set_source()`. |
| `SCDTargetTableTransformer` | process | Reads the target table's current-flag rows into `state.frames["target"]`; when the table does not exist yet it leaves the key absent and sets `state.target_exists` to `False` (`True` otherwise). `state.df` passes through. |
| `SCDIncrementalTargetTableTransformer` | process | As `SCDTargetTableTransformer`, filtered to the business keys and source systems present in `state.df`. Raises `ValueError` when `load_type` is `Full`, where delete detection needs the whole target. |
| `SCDChangeDetectorTransformer` | process | Classifies each staged row as new, changed or deleted against `state.frames["target"]` and drops unchanged rows. Adds the hash, version and row-key columns and the internal routing columns. Hashes columns in alphabetical order. A source delete, or a `Full` load's absence, of a key whose current target row is already deleted is dropped; a NULL target flag counts as not deleted. Raises `ValueError` when `state.df` already has the hash or version column, when `scd_type` is not `scd1` or `scd2`, when the business-key or source deleted-flag column is missing, and when a `Full` load has an empty source and a non-empty target. |
| `SCDGracefulChangeDetectorTransformer` | process | As `SCDChangeDetectorTransformer`, but hashes columns in `staging_table_schema` order and drops trailing NULL columns from the hash, so a new nullable column does not version every row. Raises `ValueError` when `staging_table_schema` is not set. |
| `DVHubTargetTransformer` | process | Data Vault hub loads only (`scd_type` `dv_hub`). Reads the hub's keys that the staged batch carries into `state.frames["target"]`, a semi-join whatever the `load_type`, since a hub never deletes; when the hub does not exist, the key is absent and `state.target_exists` is `False`. Lazy: no Spark action of its own. |
| `DVHubWriterTransformer` | process, `HWMProvider` | Data Vault hub loads only. Inserts the staged rows whose key the hub lacks, every staged column kept, stamped with `RecordSource`, `LoadTS` (the run's load time, `global_context.load_timestamp`, any value: a backfilled one included, as the key is first seen then), `LoadEndTS` (the write's start; with a backfilled load time it minus `LoadTS` is not a duration), the correlation id and, when named, the pipeline run id; never updates. Creates the hub, without clustering (see [Load a Data Vault entity](../guides/load-a-data-vault-entity.md)), with `PlatformAdapter.write_delta_create` when absent, which refuses a table already there, otherwise an insert-only MERGE on the key, retried on a concurrent append. Seals the hash settings at the first write and refuses a load whose settings differ (`ValueError`). Reports `inserted` from the commit, `updated` 0, and this run's `LoadTS` as `hwm_after` with no `hwm_before`. |
| `DVLinkTargetTransformer` | process | Data Vault link loads only (`scd_type` `dv_link`). Reads the link's keys that the staged batch carries into `state.frames["target"]`, by a semi-join whatever the `load_type`; a link never deletes, so it is never read whole. When the link does not exist, the key is absent and `state.target_exists` is `False`. No Spark action of its own. |
| `DVLinkWriterTransformer` | process, `HWMProvider` | Data Vault link loads only. Inserts the staged relationships whose link key the link lacks, every staged column kept, stamped as a hub row is, `LoadTS` included; never updates. Does not check that the link's hub keys exist in their hubs: it relies on run order (load every hub a link references before the link), and an orphan key is loaded as is. Creates the link, without clustering, with `PlatformAdapter.write_delta_create` when absent, otherwise an insert-only MERGE on the link key alone, retried on a concurrent append. Seals the hash settings at the first write and refuses a load whose settings differ (`ValueError`). Reports `inserted` from the commit, `updated` 0, and its run's `LoadTS` as `hwm_after`. |
| `DVSatelliteTargetTransformer` | process | Data Vault satellite loads only (`scd_type` `dv_satellite`). Reads every key's latest row, by load timestamp and with every column, into `state.frames["target"]`: the step for a `Full` load, which needs the keys absent from the source. One aggregation over the whole satellite, growing with its history; no Spark action of its own. When the satellite does not exist, the key is absent and `state.target_exists` is `False`. |
| `DVSatelliteIncrementalTargetTransformer` | process | As `DVSatelliteTargetTransformer`, for the keys the staged batch carries only, after a semi-join, so its cost follows the batch. Raises `ValueError` on a `Full` load. |
| `DVSatelliteChangeDetectorTransformer` | process | Data Vault satellite loads only. Classifies the staged rows against each key's latest row on the pair (hash diff, deleted flag), null-safely: `new`, `changed` (a deleted key that reappears included), or `deleted` (a source delete of a live key, or under `Full` a live key absent from the source), carrying the latest row's attributes and hash diff forward. Writes nothing for an equal pair, a delete of a deleted key or of a key the satellite lacks. Computes the hash diff with `hash_algorithm` over the staged descriptive columns, alphabetically. Raises `ValueError` when the staged rows carry the hash diff or load timestamp columns, when a column is typed differently in the staged rows and the satellite, and on a `Full` load of an empty source over live keys. Its Spark actions: under `Full` only, one `isEmpty` on the source, and one on the satellite's live rows when the source is empty. |
| `DVSatelliteWriterTransformer` | process, `HWMProvider` | Data Vault satellite loads only. Drops `_change_type`, stamps `RecordSource`, `LoadTS`, `LoadEndTS`, the correlation id and, when named, the pipeline run id, and inserts the detector's rows: an insert-only MERGE on the key and `LoadTS`, not retried, so a concurrent load's commit fails this one and its rerun classifies again; or `PlatformAdapter.write_delta_create`, which creates it without clustering, when the target step stated the satellite absent (`state.target_exists` is False; with no target step, when `delta_table_exists` says so), which refuses a table already there, so a false "absent" fails the load instead of recording every key again. Seals the hash settings at the first write and raises `ValueError` before writing when the satellite was sealed with others, or when the rows' columns differ from the satellite's. `LoadTS` is `global_context.load_timestamp`; the step does not check that it increases (see the `load_timestamp` row of the load context). Reports `inserted` from the commit, `updated` 0, and its run's `LoadTS` as `hwm_after`. |
| `StagedKeyGuardTransformer` | process | Placed right after the source. Raises `ValueError` when a staged key, the business key and, under `source_system_from_source`, the source system, appears twice, has a NULL part or is an empty string; under a Data Vault style the business key alone, `source_system_from_source` or not; under `dv_link` also a NULL or empty value in a `link_hub_key_columns` column, which may repeat; one message lists every problem with counts and samples. Never halts, so an empty source still reaches the detector. One Spark action over the staged rows. Not composed by `default_pipeline`: add it to refuse bad keys before they are classified (backlog F134, F154). |
| `CandidateGuardTransformer` | process | Raises `ValueError` when a business key and source system have more than one candidate row. Halts the pipeline when there are no candidates. One Spark action: a grouped aggregation over every candidate by key, which also collects each key's change types for the error message (a `collect_list`, which can spill past 128 keys a task), cached off serverless and not on it. Optional: composed by `default_pipeline`, and a hand-built pipeline that leaves it out loses the duplicate-key check and the halt on zero candidates. |
| `SCDCandidateVersionGuardTransformer` | process | Raises `ValueError` when a business key, source system and version have more than one candidate row, and when the version column is missing. One Spark action. Not composed by `default_pipeline`: an append-only load built by hand composes it; see [Choose a load style](../guides/choose-a-load-style.md). |
| `NativeSCDMergeTransformer` | process, `HWMProvider` | Stamps the audit columns and writes the candidates with one Delta `MERGE`. Every row's effective-from is `global_context.load_timestamp`, and an SCD2 close-out's effective-to is that time less one microsecond. The step does not check that the load time increases (see the `load_timestamp` row of the load context). SCD1 inserts new rows and updates changed ones in place: every business column plus the current flag, effective-from, hash, deleted flag, version and correlation id columns, found by the context's meta-column names, so a business column starting with `scd` is updated and the row key, effective-to and source system columns are not. A deleted candidate is a soft delete under SCD1: the matched row's deleted flag is set to `deleted_flag_value` (`"Y"`) and its effective-from and correlation id columns are stamped, and the row keeps its business columns, hash and version; no row is added or removed, and the write counts in `updated`. A key that returns is updated and its flag set back to `not_deleted_flag_value` (`"N"`). SCD2 closes out the current version of each changed or deleted key and inserts a new version, with the deleted flag set for a deleted key. Under SCD2, raises `ValueError` when candidates are classified changed or deleted and `state.frames["target"]` is absent. Seals the target's flag values, `default_max_timestamp`, hash separator and session time zone as table properties (see the load context): one metadata read before the write to an existing target, a `ValueError` and nothing written when a sealed value differs from this load's, a seal after a successful write of a target that was created or had none (a WARNING for the latter). A seal that cannot be written is a WARNING and the load stays `success`; a target the table-property helpers refuse is neither checked nor sealed. Sets `inserted` and `updated`. Before the `MERGE` it takes at most one Spark action on the candidates: under SCD2, one `isEmpty()` that answers whether there is close-out work, and none under SCD1; it also takes one `delta_table_exists`, reads the target's max effective-from before the write when it exists, and after it, and the write's commit metadata for the counts. A `MaterializeTransformer` placed before it pays the lineage once when that lineage is expensive. |
| `NativeSCDAppendOnlyTransformer` | process, `HWMProvider` | Stamps the audit columns, effective-from being `global_context.load_timestamp`, and appends new rows with a plain Delta append, no `MERGE`; when the target step stated the target absent (`state.target_exists` is False) it creates through `write_delta_create` instead, which refuses a table that is in fact there. Deleted candidates are dropped: only `new` rows are written. Raises `ValueError` when any candidate is classified changed; its own Spark action is one `isEmpty()` on the changed candidates, plus a `limit(5).collect()` of sample keys for that error. Seals the target as `NativeSCDMergeTransformer` does: a refusal before the append when a sealed value differs, a seal after the write. A seal that cannot be written, or an unsupported target, is a WARNING. Sets `inserted`, and `updated` to 0. |
| `ConditionalTransformer` | process | Runs a wrapped process transformer only when a predicate is true; otherwise passes the state through and records no stage. Built by `.add_if()`. |
| `MaterializeTransformer` | process | Writes `state.df` to a scratch Delta table and reads it back, so later steps read a short lineage; with `runtime_context.materialize_strategy` `"cache"` (not on serverless, which falls back) it instead persists `state.df` in memory with one action and writes no table. The scratch table, `{staging_table_path}_{id}_{stage}_materialize` (`{id}` is `global_context.id`, lower-cased, `{stage}` the step's position in the pipeline), cannot be chosen and is dropped and rewritten on every run; see [Tables the library creates](load-context.md#tables-the-library-creates). |
| `MaterializeFrameTransformer` | process | As `MaterializeTransformer`, for one named entry of `state.frames`, including the `materialize_strategy` behaviour. Passes through when that entry is absent. |
| `DebugTransformer` | process | Prints a Markdown sample of `state.df` and of named frames to stdout; never raises and never changes the state. Each frame printed costs a `count()`. |
| `MetricsChartTransformer` | always runs | Prints an ASCII bar chart of each stage's `duration_ms`, slowest first. Add it last. |
| `PlanReportTransformer` | always runs | Prints each stage's captured query plan; prints a notice instead when `capture_plans` is off. Add it last. |
| `SourceTransformer` | base class | Subclass it for a source: implement `_produce(ctx)`. Raises `ValueError` when `state.df` lacks the business-key column, or the source-system column when `source_system_from_source` is set. The deleted-flag column is optional: `SCDChangeDetectorTransformer` reads it as the delete signal when present and writes it itself. |
| `ProcessTransformer` | base class | Subclass it for a step: implement `_run(state, ctx)`. |
| `AlwaysRunTransformer` | base class | Subclass it for a reporting step that must run on a halted pipeline. It must not change the state. |
| `HWMProvider` | protocol | Declares the high watermark a transformer reports. |
| `SCDMetricsProvider` | protocol | Declares the insert and update counts a writer reports. |
| `SourceMetricsProvider` | protocol | Declares the row counts a source reports. |
| `LoadContext` | frozen dataclass | The parsed load context every transformer receives as `ctx`. |

## `SCDChangeDetectorTransformer`

Every parameter is keyword-only.

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `exclude_from_hash` | `Sequence[str] \| None` | `None` | Columns left out of the change hash, in addition to the business key, source system, correlation id, deleted flag, row key, hash and version columns, `scd_context.exclude_from_hash` and the source deleted-flag column. |
| `hash_null_token` | `str` | `"<NULL>"` | Stands in for a NULL value in the string that is hashed. |
| `hash_separator` | `str \| None` | `None` | Joins the column values in the string that is hashed. `None` reads `scd_context.hash_separator`, `"~"` unless the context gives one, when the step runs. A value that differs from one the context gives raises `ValueError` at run time. Changing the separator of a table that holds rows re-versions every key. |
| `debug_hash_string_column` | `str \| None` | `None` | When set, adds a column of this name holding the string that was hashed, for troubleshooting, and declares it on the state's `debug_columns`. The `NativeSCD*` writers drop it before the write, so it never reaches the target. A target written before that holds the column already: later loads, the `MERGE` and the append alike, leave it `NULL` for new rows and keep it in the schema (observed on local Delta; unproven on Fabric and Databricks). Leave it, or drop it with `ALTER TABLE ... DROP COLUMN`, which needs Delta column mapping. |

## `SCDGracefulChangeDetectorTransformer`

Every parameter is keyword-only and inherited from `SCDChangeDetectorTransformer`.

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `exclude_from_hash` | `Sequence[str] \| None` | `None` | As for `SCDChangeDetectorTransformer`. |
| `hash_null_token` | `str` | `"<NULL>"` | Stands in for a NULL value that has a non-NULL column after it in schema order. Trailing NULLs are dropped instead. |
| `hash_separator` | `str \| None` | `None` | As for `SCDChangeDetectorTransformer`. |
| `debug_hash_string_column` | `str \| None` | `None` | As for `SCDChangeDetectorTransformer`; the string shown has its trailing NULLs dropped. |

## `StagedKeyGuardTransformer`

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `sample_n` | `int` | `5` | How many offending keys of each kind the error message shows. Keyword-only. |

## `CandidateGuardTransformer`

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `sample_n` | `int` | `5` | Keyword-only. How many duplicate keys the `ValueError` message names. |

## `SCDCandidateVersionGuardTransformer`

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `sample_n` | `int` | `5` | Keyword-only. How many duplicate keys the `ValueError` message names. |

## `SCDIncrementalTargetTableTransformer`

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `broadcast` | `bool` | `True` | Keyword-only. When true, the incoming keys are broadcast in the filtering join. When false, no hint is given and Spark chooses the join strategy. |

## `DQDTransformer`

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `dqd_definition` | `dict[str, Any] \| None` | `None` | A definition in `dqd_context`'s shape, used instead of the load context's. The load context must still have a `dqd_context` or staging stages to pass validation; when neither this nor `dqd_context` is set, `ValueError`. |

## `ConditionalSourceTransformer`

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `predicate` | `bool \| Callable[[LoadContext], bool]` | required | Chooses the source when the pipeline runs. Anything else raises `TypeError` at construction; a callable that raises is re-raised as `RuntimeError`. |
| `source_if_true` | `SourceTransformer` | required | The source run when the predicate is true. |
| `source_if_false` | `SourceTransformer` | required | The source run when the predicate is false. |
| `label` | `str \| None` | `None` | The name used in log messages. When `None`, the two sources' class names. |

## `ConditionalTransformer`

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `predicate` | `bool \| Callable[[LoadContext], bool]` | required | Decides, once per run, whether the wrapped step runs. Anything else raises `TypeError` at construction; a callable that raises is re-raised as `RuntimeError`. |
| `transformer` | `ProcessTransformer` | required | The step to run when the predicate is true. Its stage metrics are recorded under its own name. |
| `label` | `str \| None` | `None` | The name used in log messages. When `None`, the wrapped class's name. |

## `MaterializeFrameTransformer`

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `frame_key` | `str` | `"target"` | The `state.frames` entry to materialize. |

The scratch table is `{staging_table_path}_{id}_{stage}_materialize_{frame_key}`, where `{id}` is `global_context.id`, lower-cased, and `{stage}` the step's position in the pipeline; it cannot be chosen. See [Tables the library creates](load-context.md#tables-the-library-creates) for its lifecycle and cleanup.

## `DebugTransformer`

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `label` | `str` | `""` | The heading printed above the sample. |
| `n` | `int` | `20` | Keyword-only. Rows shown per frame. `0` shows no rows; the row count, and the schema when `show_schema` is true, are still printed. |
| `frames` | `list[str] \| None` | `None` | Keyword-only. Entries of `state.frames` to print after `state.df`. A missing entry prints a notice, not an error. |
| `show_schema` | `bool` | `True` | Keyword-only. When true, prints a schema table before the rows. |
| `truncate` | `int` | `80` | Keyword-only. Maximum characters per cell. `0` is unlimited. |
| `enabled` | `bool` | `True` | Keyword-only. When false, nothing is printed and the step runs no `count()` of its own. The stage's instrumentation still runs, as for every step: `capture_partitions`, on by default, can materialise a shuffle, and `capture_row_counts` runs a `count()` before and after the step. |

Rows printed to stdout are captured into the persisted log unless
`logging_context.suppress_stdout_tail` is set; the step warns once per run when that happens.

## `LoadContext`

Built by the orchestrator from the load-context JSON; a transformer reads it and never changes
it.

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `global_ctx` | `GlobalContext` | required | The `global_context` section: see [`GlobalContext`](load-context.md#globalcontext). |
| `staging_ctx` | `StagingContext` | required | The `staging_context` section: see [`StagingContext`](load-context.md#stagingcontext). |
| `scd_ctx` | `SCDContext` | required | The `scd_context` section: see [`SCDContext`](load-context.md#scdcontext). |
| `runtime_ctx` | `RuntimeContext` | required | The `runtime_context` section: see [`RuntimeContext`](load-context.md#runtimecontext). |
| `logging_ctx` | `LoggingContext` | required | The `logging_context` section: see [`LoggingContext`](load-context.md#loggingcontext). |
| `dqd_ctx` | `DQDContext` or `None` | `None` | The `dqd_context` section when the load has one: see [`DQDContext`](load-context.md#dqdcontext). |
| `hwm_ctx` | `HWMContext` or `None` | `None` | The `hwm_context` section when the load has one: see [`HWMContext`](load-context.md#hwmcontext). |
| `extensions_ctx` | `ExtensionsContext` | an empty one | The `extensions_context` section, never `None`: see [`ExtensionsContext`](load-context.md#extensionscontext). |
| `platform_ctx` | `PlatformContext` | an empty one | The two lakehouse paths the library resolved, never `None`: see [`PlatformContext`](load-context.md#platformcontext). |

## `HWMProvider`

A transformer that declares these attributes reports the run's high watermark. With an
`hwm_context`, the first provider in pipeline order whose `hwm_column` equals
`hwm_context.hwm_column_name` owns the watermark, even when its `hwm_after` is `None` (a first run
that staged nothing, F138). Otherwise, or when no provider reports for that column, the
orchestrator uses the first provider in pipeline order whose `hwm_after` is not `None`, so a
source registered before the writer wins. Set them inside `_run()` or `_produce()`.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `hwm_before` | `str \| None` | required | The watermark before this run. |
| `hwm_after` | `str \| None` | required | The watermark after this run. It only selects the watermark source; the proof that a writer ran is its `SCDMetricsProvider` counts (F117). |
| `hwm_column` | `str \| None` | required | The column the watermark is read from. The `NativeSCD*` writers use the effective-from column; the Data Vault writers use `load_ts_column`; `StagingTransformer` and `DQDTransformer` use `hwm_context.hwm_column_name`. |

## `SCDMetricsProvider`

A writer that declares these attributes reports its counts. Declare both as `None` in `__init__`
and set them inside `_run`: the orchestrator uses the first provider whose `inserted` and
`updated` are not `None`, so a writer that never ran, such as the other branch of an `.add_if()`
pair, is skipped, and a writer whose watermark read failed is still counted. A writer need not
implement `HWMProvider`. Every shipped writer, including the Data Vault ones, follows this.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `inserted` | `int \| None` | required | Rows the write inserted; `None` until the writer has run. |
| `updated` | `int \| None` | required | Rows the write updated; `None` until the writer has run, then `0` for a writer that never updates. |

## `SourceMetricsProvider`

A source that declares these attributes reports the rows it produced. The orchestrator uses the
first provider it finds, since a pipeline has one active source (a source chosen with
`.set_source()` is unwrapped first). `StagingTransformer` and `DQDTransformer` implement it;
`DQDTransformer`'s counts are `None` when the load has no `hwm_context`. Set both inside `_produce()`.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `source_count` | `int \| None` | required | Rows the source produced; `None` means not computed, not zero. The orchestrator writes `None` into `ExecutionMetrics.source_count` as `0`. |
| `target_count` | `int \| None` | required | The count the result reports for the target; a source with no separate target step mirrors `source_count`. Populated together with `source_count`. |

## See also

- [`PipelineState` reference](pipeline-state.md): the state every transformer receives and returns.
- [Load-context reference](load-context.md): the fields the transformers read.
- [Orchestration user guide](../guides/orchestration-user-guide.md): building and running a pipeline.
