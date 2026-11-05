# Build a pipeline

At the end of this guide you have a load whose steps you chose yourself: a source, the steps
that detect and write changes, a step that runs only when you want it, and a source picked at run
time. It is for a user of the library who needs more than `default_pipeline` gives. The steps are
the same on every platform.

## Before you start

- You can run a load with `default_pipeline`: a staging notebook, a load context and a `spark`
  session in scope; see [Run your first load](run-your-first-load.md). This guide uses that
  guide's `load_context_json`.
- Every step used here, and its options, is on the
  [transformer reference](../reference/transformers.md).

## Steps

1. **Start an orchestrator.** It takes the same arguments as `default_pipeline` but has no steps
   yet.

   ```python
   from spark_quick_start.orchestration import (
       CandidateGuardTransformer,
       SCDChangeDetectorTransformer,
       DebugTransformer,
       LoadOrchestrator,
       NativeSCDMergeTransformer,
       StagingTransformer,
       SCDTargetTableTransformer,
   )

   orchestrator = LoadOrchestrator(load_context_json, spark, platform="fabric")
   ```

   On Databricks pass `platform="databricks"`; locally pass `adapter=` your `LocalAdapter`, as in
   [Run your first load](run-your-first-load.md), step 4.

2. **Add the source step first**, then the steps in the order they run: read the target, detect
   changes, check the candidates, write. `add` returns the orchestrator, so the calls chain.

   ```python
   orchestrator = (
       orchestrator
       .add(StagingTransformer())
       .add(SCDTargetTableTransformer())
       .add(SCDChangeDetectorTransformer())
       .add(CandidateGuardTransformer())
       .add(NativeSCDMergeTransformer())
   )
   ```

   This is the pipeline `default_pipeline` builds. The source must be first and the only source;
   the order of the rest is what the load does.

3. **Add a step only when you want it** with `add_if`. This step builds the pipeline again, in
   place of step 2's. The predicate is a `bool`, or a function that takes the parsed
   `LoadContext` and is called when the load runs. Here a sample of the staged rows is printed
   only for a verbose run; `label` names the step in the log.

   ```python
   orchestrator = LoadOrchestrator(load_context_json, spark, platform="fabric")
   orchestrator = (
       orchestrator
       .add(StagingTransformer())
       .add_if(lambda ctx: ctx.logging_ctx.verbose, DebugTransformer("staged", n=5), label="sample")
       .add(SCDTargetTableTransformer())
       .add(SCDChangeDetectorTransformer())
       .add(CandidateGuardTransformer())
       .add(NativeSCDMergeTransformer())
   )
   ```

   The sample goes into the run's log rather than the notebook, and is discarded when
   `logging_context.suppress_stdout_tail` is `true`; pass `n=0` to print no rows. See
   [Inspect a run](inspect-a-run.md).

4. **Pick the source when the load runs** with `set_source`, in place of the source `add`. This
   step builds the pipeline again, in place of step 3's. `set_source` takes a predicate and two
   sources; here a DQD when the load context has one, and staging notebooks otherwise. The DQD
   branch reads its definition from the load context's `dqd_context`; see
   [Load a table with a DQD](load-a-table-with-a-dqd.md).

   ```python
   from spark_quick_start.orchestration import DQDTransformer

   orchestrator = (
       LoadOrchestrator(load_context_json, spark, platform="fabric")
       .set_source(lambda ctx: ctx.dqd_ctx is not None, DQDTransformer(), StagingTransformer())
       .add(SCDTargetTableTransformer())
       .add(SCDChangeDetectorTransformer())
       .add(CandidateGuardTransformer())
       .add(NativeSCDMergeTransformer())
   )
   ```

5. **Run the pipeline you built last.**

   ```python
   result = orchestrator.run()
   ```

## Check it worked

`result.stages` lists one entry per step that ran, in order, under the step's own name. For
step 4's pipeline with staging notebooks in the load context, the names are `StagingTransformer`,
`SCDTargetTableTransformer`, `SCDChangeDetectorTransformer`, `CandidateGuardTransformer` and
`NativeSCDMergeTransformer`: the source `set_source` chose records under its own name. For step
3's on a run that is not verbose, the names are the same; the skipped `DebugTransformer` records
nothing.

```python
print([stage.stage for stage in result.stages])
```

## When it goes wrong

The pipeline is checked when it runs, not when you build it, and a failure comes back as
`result.status == "error"`.

**`first transformer must be a SourceTransformer`.** A process step was added before the source,
or no source was added. Add `StagingTransformer`, `DQDTransformer` or `set_source` first.

**`SourceTransformer found at position`.** A second source was added. A pipeline has exactly one.

**`predicate must be bool or Callable[[LoadContext], bool]`.** This one raises at once, from
`add_if` or `set_source`: the predicate is neither.

**The load ends with `result.early_exit` true and no writer stage.** Not an error:
`CandidateGuardTransformer` found nothing to write, so the steps after it were skipped.

## See also

- [Transformer reference](../reference/transformers.md): every step and its options.
- [Orchestrator reference](../reference/orchestrator.md): `add`, `add_if`, `set_source` and `run`.
- [Pipeline model](../architecture/pipeline-model.md): how steps pass state, halt and are wrapped.
