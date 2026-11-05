# Write a custom transformer

At the end of this guide you have a step of your own in a load: a class that takes the rows
flowing through the pipeline and returns them changed, placed where it should run. It is for
someone extending the library. The step runs the same on every platform.

## Before you start

- You can build a pipeline by hand; see [Build a pipeline](build-a-pipeline.md). This guide uses
  its `load_context_json`, `spark` and `platform="fabric"`.
- What a step may and may not do is the
  [transformer-contract rule](../../.claude/rules/transformer-contract.md), and why is the
  [pipeline model](../architecture/pipeline-model.md). This guide follows both.

## Steps

1. **Subclass `ProcessTransformer` and implement `_run`.** It receives the pipeline's state and
   the parsed load context and returns a new state; `state.with_df` gives one with a new
   DataFrame. Here names are trimmed.

   ```python
   from pyspark.sql import functions as F

   from spark_quick_start.orchestration import ProcessTransformer


   class TrimNames(ProcessTransformer):
       def _run(self, state, ctx):
           return state.with_df(state.df.withColumn("name", F.trim("name")))
   ```

   Implement `_run`, never `__call__`: `__call__` is what times the step, records its metrics
   and skips it on a halted pipeline. You may extend `before_run` and `after_run`, calling
   `super()`, as the [transformer-contract rule](../../.claude/rules/transformer-contract.md) says.

2. **Use the services the load gives the step, not ones you create.** When the load runs, the
   step receives `self._spark`, `self._adapter` and `self._logger`; its constructor takes only
   its own options. `self._logger` may be `None` when the step is run on its own, so fall back to
   a new `OrchestratorLogger`:

   ```python
   from spark_quick_start.logger import OrchestratorLogger


   class TrimNamesLogged(ProcessTransformer):
       def _run(self, state, ctx):
           logger = self._logger or OrchestratorLogger()
           logger.info(f"Trimming names for {ctx.global_ctx.entity_name}")
           return state.with_df(state.df.withColumn("name", F.trim("name")))
   ```

   **Read your own settings from `ctx.extensions_ctx`.** The load context's `extensions_context`
   section is the place for data your step needs: a rule, a threshold, a list. The library never
   reads it, and it reaches the step as a read-only `ExtensionsContext`, empty when the load has
   none. Namespace the top-level key, so another team's step does not collide with yours.

   ```python
   class DropShortNames(ProcessTransformer):
       def _run(self, state, ctx):
           minimum = ctx.extensions_ctx.get("acme.names", {}).get("min_length", 1)
           return state.with_df(state.df.filter(F.length("name") >= minimum))
   ```

   with `"extensions_context": {"acme.names": {"min_length": 2}}` in the load context. Read it in
   `_run`, not in the constructor: the context does not exist until the load runs. The paths the
   library resolved are in `ctx.platform_ctx`. Neither object can be assigned to.

3. **Add an instance to the pipeline where it should run.** A step after the source and before
   `SCDTargetTableTransformer` changes the rows before they are compared with the target, so the
   hash and the table see the changed values.

   ```python
   from spark_quick_start.orchestration import (
       CandidateGuardTransformer,
       SCDChangeDetectorTransformer,
       LoadOrchestrator,
       NativeSCDMergeTransformer,
       StagingTransformer,
       SCDTargetTableTransformer,
   )

   result = (
       LoadOrchestrator(load_context_json, spark, platform="fabric")
       .add(StagingTransformer())
       .add(TrimNames())
       .add(SCDTargetTableTransformer())
       .add(SCDChangeDetectorTransformer())
       .add(CandidateGuardTransformer())
       .add(NativeSCDMergeTransformer())
       .run()
   )
   ```

4. **To report a watermark, declare the attributes and set them in `_run`.** A step with
   `hwm_before`, `hwm_after` and `hwm_column` is an `HWMProvider`; no registration is needed. Without
   `hwm_context`, the orchestrator takes the run's watermark from the first step in the pipeline
   whose `hwm_after` is set, so a step placed before the writer replaces the writer's watermark.
   With `hwm_context`, the first step whose `hwm_column` equals its `hwm_column_name` owns the
   watermark even when its `hwm_after` is `None` (F138); the source step reports that column, so
   `StagingTransformer` or `DQDTransformer` keeps it and your step's watermark is not used. Set
   the attributes in `_run`, not only in the constructor. `hwm_after` only chooses which step's
   watermark is reported; it is not the proof that a step ran, which the counts below are.

   Here the watermark is the highest `customer_id` staged; for a real entity it is usually a
   timestamp column.

   ```python
   class HighestCustomerId(ProcessTransformer):
       def __init__(self):
           self.hwm_before = None
           self.hwm_after = None
           self.hwm_column = None

       def _run(self, state, ctx):
           self.hwm_column = "customer_id"
           highest = state.df.agg(F.max("customer_id")).collect()[0][0]
           self.hwm_after = None if highest is None else str(highest)
           return state
   ```

   Three things follow from this shape. An empty batch leaves `hwm_after` as `None`, so, without
   `hwm_context`, the step does not count and the writer's watermark is used. Because the whole provider is taken, the
   run's `hwm_value_before` is this step's `hwm_before`, `None` here, not the writer's. And the
   aggregation is a Spark action over the source rows, paid on every run.

   A writer of your own reports `inserted` and `updated` the same way, as an
   `SCDMetricsProvider`, declaring both counts as `None` in `__init__` and setting them in `_run`: a count that is still `None` is how the orchestrator knows the writer did not run. This is a contract change for existing user code: a custom `SCDMetricsProvider` that initialises its counts to `0` is now harvested whether or not it ran, so initialise them to `None`. The rules are
   in [metrics and watermarks](../architecture/metrics-and-watermarks.md).

## Check it worked

`result.stages` has an entry under your class's name, in the position you added it, and the
target holds the changed values.

```python
print([stage.stage for stage in result.stages])
```

## When it goes wrong

`run()` does not raise: each message below arrives as `result.status == "error"`, with the text in
`result.error_details["error_message"]`.

**`TrimNames() takes no arguments`.** The class was added, not an instance: `.add(TrimNames)`.
The load calls it as a plain step with the DataFrame and the context. Write `.add(TrimNames())`.

**`first transformer must be a SourceTransformer`.** The step was added before the source. A
`ProcessTransformer` goes after it.

**The run's watermark is a value from your step, not the writer's.** Your step sets `hwm_after`
and sits before the writer; the first one wins (step 4).

## See also

- [Transformer reference](../reference/transformers.md): the base classes and the two protocols.
- [Pipeline model](../architecture/pipeline-model.md): state, halting, services and wrappers.
- [Metrics and watermarks](../architecture/metrics-and-watermarks.md): what the orchestrator reads
  from a step, and when.
