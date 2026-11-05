# Orchestrator

`LoadOrchestrator` runs one entity load: it parses the load context, runs the registered
transformers in order, and returns an [`ExecutionResult`](execution-result.md). It is defined in
`src/spark_quick_start/orchestration/orchestrator.py`; `default_pipeline`, `dv_hub_pipeline`,
`dv_link_pipeline` and `dv_satellite_pipeline`, which return one with the standard steps registered, are in
`src/spark_quick_start/orchestration/pipeline_factory.py`. All five are exported by
`spark_quick_start.orchestration`. The steps themselves are on the
[transformer reference](transformers.md), the load context on the
[load-context reference](load-context.md).

## `LoadOrchestrator`

Every constructor parameter after `spark` is keyword-only.

| Member | Kind | Type | Default | Meaning |
|---|---|---|---|---|
| `load_context_json` | parameter | `str` | required | The load context as a JSON string. It is parsed and validated inside `run()`, so a malformed context becomes an error result, not an exception. |
| `spark` | parameter | `SparkSession` | required | The session every step uses. |
| `adapter` | parameter | `PlatformAdapter \| None` | `None` | The [platform adapter](platform-adapters.md). When given, `platform` is ignored. |
| `platform` | parameter | `str` | `"fabric"` | Chooses the adapter when `adapter` is `None`; the values are under `LoadOrchestrator.platform` below. Resolved in the constructor, so an unknown value raises `ValueError` there, not in `run()`. |
| `data_storage_path` | parameter | `str` | `""` | Replaces every `{DataStorageLakehouse}` in the raw JSON text before it is parsed. |
| `data_eng_path` | parameter | `str` | `""` | Replaces every `{DataEngineeringLakehouse}` in the raw JSON text before it is parsed. |
| `logger` | parameter | `OrchestratorLogger \| None` | `None` | The logger to use. When `None`, `run()` builds one from `logging_context`, the run's correlation id, the adapter and `runtime_context.pipeline_name`. |
| `add` | method, `(transformer) -> LoadOrchestrator` | — | — | Appends a step and returns the orchestrator for chaining. The first step must be a source transformer; each later one a process transformer, or a plain callable taking `(DataFrame, LoadContext)` and returning a `DataFrame`, which replaces `state.df`. Nothing is checked here. When `run()` starts it checks that the first step is a source and no later one is; a step that is neither a process transformer nor a callable is found only when the pipeline reaches it, after the steps before it have run, writes included. |
| `add_if` | method, `(predicate, transformer, label=None) -> LoadOrchestrator` | — | — | Appends `ConditionalTransformer(predicate, transformer, label=label)`: the step runs only when `predicate`, a `bool` or a callable taking the `LoadContext`, is true. Raises `TypeError` here when `predicate` is neither a `bool` nor a callable. |
| `set_source` | method, `(predicate, source_if_true, source_if_false, label=None) -> LoadOrchestrator` | — | — | Appends `ConditionalSourceTransformer(...)`, which chooses between two sources when the pipeline runs. It must be the first step, as any source must. Raises `TypeError` here when `predicate` is neither a `bool` nor a callable. |
| `run` | method, `() -> ExecutionResult` | — | — | Parses and validates the load context, runs the steps, collects the metrics and watermark, finalizes the logs and returns the result. Does not raise: a missing step, a source anywhere but first, a step that is neither a transformer nor a callable, and any exception a step raises all become a result with `status` `"error"`. |

## `LoadOrchestrator.platform`

The values the `platform` argument accepts, compared without regard to case. Each builds its
adapter with no arguments.

| Value | Adapter | Meaning |
|---|---|---|
| `fabric` | `FabricAdapter` | Microsoft Fabric. The default. |
| `databricks` | `DatabricksAdapter` | Azure Databricks. |
| `local` | `LocalAdapter` | Local development. Built with no notebook registry or directory, so it cannot run a staging notebook; pass a configured `LocalAdapter` as `adapter` instead when the pipeline has one. |

Any other value raises `ValueError` naming the accepted ones. So does a value whose adapter
module a platform-filtered build has removed, with a message saying so.

## `default_pipeline`

Returns a `LoadOrchestrator` built from these arguments, which it passes on unchanged, with five
steps registered: `StagingTransformer`, `SCDTargetTableTransformer`, `SCDChangeDetectorTransformer`,
`CandidateGuardTransformer`, `NativeSCDMergeTransformer`. A load with no changes therefore ends
with `early_exit` true and no merge stage.

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `load_context_json` | `str` | required | As for `LoadOrchestrator`. |
| `spark` | `SparkSession` | required | As for `LoadOrchestrator`. |
| `adapter` | `PlatformAdapter \| None` | `None` | As for `LoadOrchestrator`. Keyword-only, as are the rest. |
| `platform` | `str` | `"fabric"` | As for `LoadOrchestrator`. |
| `data_storage_path` | `str` | `""` | As for `LoadOrchestrator`. |
| `data_eng_path` | `str` | `""` | As for `LoadOrchestrator`. |
| `logger` | `OrchestratorLogger \| None` | `None` | As for `LoadOrchestrator`. |

## `dv_hub_pipeline`

A Data Vault hub load (`scd_context.scd_type` `dv_hub`): the source is `DQDTransformer` when the
load context has a `dqd_context`, `StagingTransformer` otherwise, then
`StagedKeyGuardTransformer`, `DVHubTargetTransformer` and `DVHubWriterTransformer`. No candidate
guard: the staged-key guard already refuses a key staged twice, and a hub has no change
classification.

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `load_context_json` | `str` | required | As for `LoadOrchestrator`; its `scd_context.scd_type` must be `dv_hub`. |
| `spark` | `SparkSession` | required | As for `LoadOrchestrator`. |
| `adapter` | `PlatformAdapter \| None` | `None` | As for `LoadOrchestrator`. Keyword-only, as are the rest. |
| `platform` | `str` | `"fabric"` | As for `LoadOrchestrator`. |
| `data_storage_path` | `str` | `""` | As for `LoadOrchestrator`. |
| `data_eng_path` | `str` | `""` | As for `LoadOrchestrator`. |
| `logger` | `OrchestratorLogger \| None` | `None` | As for `LoadOrchestrator`. |

## `dv_link_pipeline`

A Data Vault link load (`scd_type` `dv_link`, with `link_hub_key_columns`): the source as for
`dv_hub_pipeline`, then `StagedKeyGuardTransformer`, which also refuses a NULL or empty hub key,
`DVLinkTargetTransformer` and `DVLinkWriterTransformer`. No candidate guard, as in the hub.

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `load_context_json` | `str` | required | As for `LoadOrchestrator`; its `scd_context.scd_type` must be `dv_link`. |
| `spark` | `SparkSession` | required | As for `LoadOrchestrator`. |
| `adapter` | `PlatformAdapter \| None` | `None` | As for `LoadOrchestrator`. Keyword-only, as are the rest. |
| `platform` | `str` | `"fabric"` | As for `LoadOrchestrator`. |
| `data_storage_path` | `str` | `""` | As for `LoadOrchestrator`. |
| `data_eng_path` | `str` | `""` | As for `LoadOrchestrator`. |
| `logger` | `OrchestratorLogger \| None` | `None` | As for `LoadOrchestrator`. |

## `dv_satellite_pipeline`

A Data Vault satellite load (`scd_context.scd_type` `dv_satellite`): the source as for
`dv_hub_pipeline`, then `StagedKeyGuardTransformer`; `DVSatelliteTargetTransformer` for a `Full`
load and `DVSatelliteIncrementalTargetTransformer` otherwise, each inside `.add_if()`;
`DVSatelliteChangeDetectorTransformer`; `DVSatelliteWriterTransformer`. No candidate guard: with
one staged row and one latest satellite row per key, the detector cannot emit a key twice.

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `load_context_json` | `str` | required | As for `LoadOrchestrator`; its `scd_context.scd_type` must be `dv_satellite`. |
| `spark` | `SparkSession` | required | As for `LoadOrchestrator`. |
| `adapter` | `PlatformAdapter \| None` | `None` | As for `LoadOrchestrator`. Keyword-only, as are the rest. |
| `platform` | `str` | `"fabric"` | As for `LoadOrchestrator`. |
| `data_storage_path` | `str` | `""` | As for `LoadOrchestrator`. |
| `data_eng_path` | `str` | `""` | As for `LoadOrchestrator`. |
| `logger` | `OrchestratorLogger \| None` | `None` | As for `LoadOrchestrator`. |

## See also

- [Orchestration user guide](../guides/orchestration-user-guide.md): building and running a pipeline.
