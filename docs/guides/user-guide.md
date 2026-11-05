# User Guide

## Table of Contents

1. [Installation](#installation)
2. [Load Context JSON](#load-context-json)
3. [Running a Pipeline](#running-a-pipeline)
4. [Pipeline Orchestration Architecture](#pipeline-orchestration-architecture)
5. [SCD Context Reference](#scd-context-reference)
6. [Transformer Reference](#transformer-reference)
7. [Conditional Steps](#conditional-steps)
8. [Platform Adapters](#platform-adapters)
9. [Declarative Query Definition (DQD)](#declarative-query-definition-dqd)
10. [Structured Logging](#structured-logging)
11. [Testing Without a Cluster](#testing-without-a-cluster)

---

## Installation

Installing the library, as a wheel or as a copy of `src/` on `sys.path`, is in the
[README's installation section](../../README.md#installation).

**Locally (development):** see the guide [Set up for development](set-up-for-development.md).

---

## Load Context JSON

Every pipeline run is driven by a single JSON string that carries all configuration. The string is typically passed as a notebook parameter (`load_context_json`) from a master orchestration pipeline.

Every section and field, with its type, default and meaning, is in the [load-context reference](../reference/load-context.md).

### Keeping row data out of the log file

Setting `suppress_stdout_tail` for sensitive data, and writing a whole load context, is the guide
[Write a load context](write-a-load-context.md).

There is deliberately no column-level redaction option. It would require
enumerating every sensitive column correctly, and a miss would read as safe.
`suppress_stdout_tail` is binary and means exactly what it says. (This reasoning has no
architecture page yet; it stays here until one exists.)

---

## Running a Pipeline

How to run a load, from the staging notebook to the result, on Fabric, Databricks or locally, is
the guide [Run your first load](run-your-first-load.md). Every field of the result is on the
[execution-result reference](../reference/execution-result.md).

---

## Pipeline Orchestration Architecture

### Overview

`LoadOrchestrator` runs a single entity load as a chain of **transformers** applied to an immutable state object. You build the chain with `.add()` / `.add_if()` / `.set_source()`, then call `.run()` once:

```
LoadOrchestrator
  .add(source: SourceTransformer)     # exactly one, always first
  .add(step: ProcessTransformer)      # zero or more, in order
  .add(step: ProcessTransformer)
  ...
  .run() -> ExecutionResult
```

`default_pipeline()` (`spark_quick_start.orchestration`) is a pre-wired 5-step chain for the common case — start from it and add/remove/`.add_if()` steps as needed:

```
default_pipeline: StagingTransformer -> SCDTargetTableTransformer -> SCDChangeDetectorTransformer -> CandidateGuardTransformer -> NativeSCDMergeTransformer
```

An append-only load is built by hand; see [Choose a load style](choose-a-load-style.md).

### `PipelineState` — what flows between transformers

Everything a transformer needs to know about "the data so far" lives in one dataclass:

```python
@dataclass
class PipelineState:
    df: DataFrame                        # the primary DataFrame
    frames: dict[str, DataFrame] = {}    # named secondary frames, e.g. "target"; never holds None
    halted: bool = False                 # short-circuits the rest of the chain when True
    halt_reason: str = ""
    target_exists: bool | None = None    # None: no target step ran; True: "target" present; False: table absent
    debug_columns: frozenset[str] = frozenset()  # troubleshooting columns the NativeSCD* writers drop
    hash_separator: str | None = None    # the separator the change detector hashed with
    cached: tuple[DataFrame, ...] = ()   # frames a step persisted, released when the run ends
```

Every field and setter is on the [pipeline-state reference](../reference/pipeline-state.md).

`PipelineState` is immutable by convention — transformers never mutate `state.df` in place. They return a **new** state via `with_df()`, `with_frame()`, `with_target()` (the target step's setter: a frame, or `None` for a table that does not exist), `with_halted()`, `with_debug_columns()`, `with_hash_separator()`, `with_cached()` (declares a frame the step persisted, released when the run ends), or `with_updates()` (replaces `df` and/or adds named frames in one call). Every one of them carries every field. This makes each step's contract explicit: given this state, produce that state, with no hidden mutation of shared objects.

### `LoadContext` — read-only configuration

`LoadContext` is a frozen dataclass bundling every parsed context section (`global_ctx`, `staging_ctx`, `scd_ctx`, `runtime_ctx`, `logging_ctx`, and `hwm_ctx`, `dqd_ctx`, `extensions_ctx`, `platform_ctx`). `LoadOrchestrator` builds it once, after `LoadContextManager` parses and validates the load-context JSON, and passes it unchanged to every transformer. Transformers read configuration from it; they never write to it.

### The two transformer kinds

Every step in the chain is one of two ABCs from `spark_quick_start.orchestration.transformers.base`:

| Base class | Signature | Role |
|---|---|---|
| `SourceTransformer` | `produce(ctx) -> PipelineState` | Produces the *first* `PipelineState`. Exactly one per pipeline, always position 0. |
| `ProcessTransformer` | `__call__(state, ctx) -> PipelineState` | Takes a `PipelineState`, returns a (possibly new) `PipelineState`. Zero or more, any position after 0. |

Both follow a **template-method** lifecycle — subclasses implement only the underscore-prefixed hook, the base class wraps it with logging and metrics capture:

```
SourceTransformer.produce():   before_run(ctx) -> _produce(ctx) -> validate required SCD columns -> after_run(ctx)
ProcessTransformer.__call__(): before_run(state, ctx) -> _run(state, ctx) -> after_run(state, ctx)
```

You implement `_produce()` or `_run()`; you don't normally override `produce()` / `__call__()` directly. `before_run` / `after_run` can also be overridden (call `super()` to keep the base logging/metrics).

A plain callable `(DataFrame, LoadContext) -> DataFrame` is also accepted anywhere after position 0 — `LoadOrchestrator` shims it into `state.with_df(fn(state.df, ctx))` automatically, so a one-off transform doesn't need its own class.

### Service injection — not constructor arguments

`spark`, `adapter`, `logger`, and the stage-metrics collector are **not** passed to transformer constructors. `LoadOrchestrator` injects them as attributes (`_spark`, `_adapter`, `_logger`, `_metrics_collector`, and the step's position as `_stage_index`) on every registered transformer right before executing the chain. This is why you construct transformers with only their own config — `SCDChangeDetectorTransformer(exclude_from_hash=[...])`, no `spark=`/`adapter=` kwarg — and why the same transformer instance can be reused across `LoadOrchestrator` runs in tests without re-wiring.

### Halting the pipeline early

See the guide [Handle early exit and errors](handle-early-exit-and-errors.md), step 4.

### Structural protocols — opt-in capabilities without registration

How a step reports a watermark or counts, and how the orchestrator picks one, is on
[metrics and watermarks](../architecture/metrics-and-watermarks.md); writing such a step is the
guide [Write a custom transformer](write-a-custom-transformer.md).

### Per-stage metrics

See the guide [Inspect a run](inspect-a-run.md), step 3.

### Capturing every stage's Spark query plan

See the guide [Inspect a run](inspect-a-run.md), step 1.

### Composing conditionally

`.add_if()` and `.set_source()`, with worked examples, are in the guide [Build a pipeline](build-a-pipeline.md).

### Breaking DataFrame lineage with `MaterializeFrameTransformer`

See the guide [Tune performance](tune-performance.md), step 3.

### End-to-end example

```python
from spark_quick_start.orchestration import (
    LoadOrchestrator, StagingTransformer, SCDTargetTableTransformer,
    SCDChangeDetectorTransformer, CandidateGuardTransformer,
    NativeSCDMergeTransformer,
)
from spark_quick_start.orchestration.transformers import (
    MaterializeFrameTransformer, MetricsChartTransformer,
)

result = (
    LoadOrchestrator(load_context_json=load_context, spark=spark, platform="fabric")
    .add(StagingTransformer())                  # SourceTransformer -> first PipelineState
    .add(SCDTargetTableTransformer())               # ProcessTransformer -> frames["target"]
    .add(MaterializeFrameTransformer())           # break lineage before repeated reads
    .add(SCDChangeDetectorTransformer())             # classify new / changed / deleted
    .add(CandidateGuardTransformer())             # halts early if zero candidates
    .add(NativeSCDMergeTransformer())             # SCD2 MERGE; passes through if halted
    .add(MetricsChartTransformer())               # prints stage timings; runs even if halted
    .run()
)
```

---

## SCD Context Reference

Every `scd_context` field, with its type, default and meaning, is in the [load-context reference](../reference/load-context.md#scdcontext).

### Column naming policy

All column name defaults come from `ColumnNamingPolicy.default()`. A load run through `LoadOrchestrator` or the pipeline factories cannot take a policy of your own: there, set each name with its own `scd_context` key (see the [`SCDContext` reference](../reference/load-context.md#scdcontext)). A policy applies only to code that builds `SCDContext` itself:

```python
from dataclasses import replace

from spark_quick_start.orchestration import ColumnNamingPolicy, SCDContext

policy = replace(
    ColumnNamingPolicy.default(),
    row_key_column="row_key",
    hash_column="hash",
    version_column="version",
    current_flag_column="is_current",
    deleted_flag_column="is_deleted",
    effective_from_column="valid_from",
    effective_to_column="valid_to",
)
ctx = SCDContext({"business_key_column": "id"}, policy=policy)
```

---

## Transformer Reference

### Built-in transformers

Every shipped transformer, its constructor parameters and what it raises are on the
[transformer reference](../reference/transformers.md).

### `SCDChangeDetectorTransformer` options

```python
SCDChangeDetectorTransformer(
    exclude_from_hash=["audit_ts", "load_date"],  # extra columns to exclude from hash
    hash_null_token="<NULL>",                      # sentinel for NULL before hashing
    hash_separator="~",                            # separator in concat_ws; None reads scd_context.hash_separator
)
```

### `SCDGracefulChangeDetectorTransformer`

When to use it is in the guide [Choose a load style](choose-a-load-style.md), step 3.

### `DebugTransformer`

Its options are on the [transformer reference](../reference/transformers.md); using it is the
guide [Inspect a run](inspect-a-run.md).

### `ConditionalTransformer` / `.add_if()`

See the guide [Build a pipeline](build-a-pipeline.md).

## Conditional Steps

See the guide [Build a pipeline](build-a-pipeline.md).

---

## Platform Adapters

The library never imports `notebookutils`, `mssparkutils`, or `dbutils` in core modules. Platform-specific behaviour is isolated in adapters.

Every adapter, its methods and `MockPlatformAdapter`'s recorded calls are on the
[platform-adapter reference](../reference/platform-adapters.md); the `platform` values that
choose one are on the [orchestrator reference](../reference/orchestrator.md#loadorchestratorplatform).

### `LocalAdapter` for local testing

See the guide [Test without a cluster](test-without-a-cluster.md), step 3.

### `MockPlatformAdapter` for unit tests

See the guide [Test without a cluster](test-without-a-cluster.md), step 5.

---

## Declarative Query Definition (DQD)

See the guide [Load a table with a DQD](load-a-table-with-a-dqd.md). The DQF engine this section
described was removed (backlog F146).

---

## Structured Logging

Every logging class, function and parameter is on the
[logging reference](../reference/logging.md).

```python
from spark_quick_start.spark_logging import (
    FabricPersistenceAdapter,
    finalize_logs,
    get_logger,
    init_logs,
)

# Initialise at notebook start. A session that writes files needs a destination:
# FabricPersistenceAdapter() on Fabric, DatabricksPersistenceAdapter() on Databricks,
# LocalFilePersistenceAdapter() locally. Without one, pass in_memory=True instead.
init_logs(
    name_prefix="Customer_load",
    lakehouse_dir="abfss://.../Logs",
    level="INFO",
    adapter=FabricPersistenceAdapter(),
)

logger = get_logger("Customer")
logger.info("Starting load")

# ... processing ...

summary = finalize_logs(success=True)
```

For in-memory log aggregation across parent/child notebooks, start the child's session with `init_logs(..., in_memory=True)`, which writes nothing (`NullPersistenceAdapter`), and return `get_logs_json()` in the child's `ExecutionResult.logs`. `create_staging_main_function(..., log_init_fn=init_logs, get_logs_fn=get_logs_json)` does both. `CapturingPersistenceAdapter` is a test double that records calls, not a capture mode.

---

## Testing Without a Cluster

See the guide [Test without a cluster](test-without-a-cluster.md).
