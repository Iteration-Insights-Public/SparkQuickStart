# Orchestration User Guide

`LoadOrchestrator` drives a composable, metadata-driven pipeline for loading data into Delta Lake Silver tables on Microsoft Fabric and Azure Databricks. This guide covers the pipeline model, all built-in transformers, the `ExecutionResult` contract, and the `HWMProvider` protocol for high-watermark ownership.

---

## Contents

1. [Quick start](#1-quick-start)
2. [Transformer pipeline model](#2-transformer-pipeline-model)
3. [Built-in transformers](#3-built-in-transformers)
4. [Custom transformers](#4-custom-transformers)
5. [Early exit](#5-early-exit)
6. [ExecutionResult](#6-executionresult)
7. [HWMProvider — high-watermark ownership](#7-hwmprovider--high-watermark-ownership)
8. [Logging](#8-logging)
9. [Load context JSON reference](#9-load-context-json-reference)

---

## 1. Quick start

```python
from spark_quick_start.orchestration import LoadOrchestrator
from spark_quick_start.orchestration.transformers import (
    StagingTransformer,
    SCDTargetTableTransformer,
    SCDChangeDetectorTransformer,
    CandidateGuardTransformer,
    NativeSCDMergeTransformer,
)

result = (
    LoadOrchestrator(
        load_context_json=load_context,   # JSON string parameter from pipeline
        spark=spark,
        platform="fabric",                # or "databricks" / "local"
        data_storage_path=notebookutils.lakehouse.get("DataStorage").abfsPath,
        data_eng_path=notebookutils.lakehouse.get("DataEngineering").abfsPath,
    )
    .add(StagingTransformer())
    .add(SCDTargetTableTransformer())
    .add(SCDChangeDetectorTransformer())
    .add(CandidateGuardTransformer())
    .add(NativeSCDMergeTransformer())
    .run()
)

notebookutils.notebook.exit(result.to_json())
```

`result` is always an `ExecutionResult` — `.run()` never raises.

---

## 2. Transformer pipeline model

```
SourceTransformer.produce(ctx)       → PipelineState
    → ProcessTransformer.__call__(state, ctx)  → PipelineState
    → ProcessTransformer.__call__(state, ctx)  → PipelineState
    → ...
```

- The **first** transformer must be a `SourceTransformer` (produces an initial `PipelineState` from staging notebooks or a DQD).
- All subsequent transformers are `ProcessTransformer` instances that receive and return a `PipelineState`.
- `PipelineState` is **immutable** — each transformer returns a new state via `state.with_df(...)`, `state.with_frame(...)`, `state.with_target(...)`, `state.with_updates(...)`, `state.with_debug_columns(...)`, `state.with_hash_separator(...)`, `state.with_cached(...)` or `state.with_halted(reason)`.
- Plain callables `(DataFrame, LoadContext) → DataFrame` are also accepted and shimmed automatically.

### PipelineState

| Attribute | Type | Description |
|---|---|---|
| `df` | `DataFrame` | The working DataFrame (source / candidate data) |
| `frames` | `dict[str, DataFrame]` | Named side frames (e.g. `"target"` loaded by `SCDTargetTableTransformer`); never holds `None` |
| `halted` | `bool` | True when a transformer short-circuited the pipeline |
| `halt_reason` | `str` | Human-readable reason for the halt |
| `target_exists` | `bool \| None` | `None` until a target step runs; then `True` (the `"target"` frame is present, possibly empty) or `False` (the table does not exist; the key is absent). Set by `with_target()` |
| `debug_columns` | `frozenset[str]` | Troubleshooting columns on `df` that the `NativeSCD*` writers drop before the write. Set by `with_debug_columns()` |
| `hash_separator` | `str \| None` | The separator the SCD change detector hashed with, sealed on the target by the `NativeSCD*` writers. Set by `with_hash_separator()` |
| `cached` | `tuple[DataFrame, ...]` | Frames a step persisted in memory, released when the run ends. Set by `with_cached()` |

Every field and setter is on the [pipeline-state reference](../reference/pipeline-state.md).

### Conditional steps

`.add_if()` and `.set_source()`, with worked examples, are in the guide
[Build a pipeline](build-a-pipeline.md).

### Lifecycle hooks

The hooks are `before_run` and `after_run`, on source and process steps alike; extend them and
call `super()`. See the [pipeline model](../architecture/pipeline-model.md) and the
[transformer-contract rule](../../.claude/rules/transformer-contract.md).

### Per-stage metrics

Reading each step's metrics, a timing chart and query plans is the guide [Inspect a run](inspect-a-run.md).

---

## 3. Built-in transformers

Every shipped transformer, its constructor parameters and what it raises are on the
[transformer reference](../reference/transformers.md).

---

## 4. Custom transformers

See the guide [Write a custom transformer](write-a-custom-transformer.md).

---

## 5. Early exit

See the guide [Handle early exit and errors](handle-early-exit-and-errors.md).

---

## 6. ExecutionResult

`LoadOrchestrator.run()` returns an `ExecutionResult` and does not raise. Its fields, its
metrics, its per-stage metrics and how it renders in a notebook are on the
[execution-result reference](../reference/execution-result.md).

---

## 7. HWMProvider — high-watermark ownership

How the run's watermark is chosen, and what each shipped step reports, is on
[metrics and watermarks](../architecture/metrics-and-watermarks.md). Running incremental loads
from staging notebooks with a source watermark is the guide
[Run incremental loads](run-incremental-loads.md).

### Implementing a custom HWMProvider

See the guide [Write a custom transformer](write-a-custom-transformer.md), step 4.

---

## 8. Logging

Every logging class, function and parameter is on the
[logging reference](../reference/logging.md).

`LoadOrchestrator` uses `OrchestratorLogger` internally. Log output goes to stdout (not stderr) so it does not pollute `stderr_tail` in the `ExecutionResult.logs` payload on successful runs.

Logging from a staging notebook, and returning its log to the load, is in the guide
[Write a staging notebook](write-a-staging-notebook.md), step 2.

---

## 9. Load context JSON reference

Every section and field of the load-context JSON, with its type, default and meaning, is in the [load-context reference](../reference/load-context.md).
