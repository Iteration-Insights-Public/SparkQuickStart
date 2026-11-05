# Tune performance

At the end of this guide your load does less work: it reads only the target rows a batch can
change, computes each expensive frame once, and pays only for the measurements you use. It is for
a user of the library whose load has grown slow. The steps are the same on every platform; the
reason not to use `.cache()` is Fabric and Databricks serverless, where it is unavailable or has
no effect.

## Before you start

- You can build a pipeline by hand and read its timings; see
  [Build a pipeline](build-a-pipeline.md) and [Inspect a run](inspect-a-run.md). This guide uses
  their `load_context_json`, `spark` and `platform="fabric"`.

## Steps

1. **Measure first.** Run the load and read each step's `duration_ms` and `measure_ms` from
   `result.stages`, as [Inspect a run](inspect-a-run.md) shows. Change only the steps that
   dominate.

2. **Read only the target rows a batch can change.** `SCDIncrementalTargetTableTransformer` reads the
   target filtered to the business keys and source systems in the staged rows, where
   `SCDTargetTableTransformer` reads every current row. Use it only for `Incremental` loads: a `Full`
   load needs the whole target to find deletes, and the step refuses one. It broadcasts the staged
   keys by default; pass `broadcast=False` to let Spark choose the join.

3. **Compute an expensive frame once.** Spark recomputes a frame's whole lineage each time a step
   reads it. `MaterializeFrameTransformer` writes a named frame, `"target"` by default, to a
   scratch Delta table and reads it back, so later reads are cheap; `MaterializeTransformer` does
   the same for the rows flowing through. One after the detector covers both the quality step and
   the writer, which each read the candidates.

   ```python
   from spark_quick_start.orchestration import (
       CandidateGuardTransformer,
       SCDChangeDetectorTransformer,
       LoadOrchestrator,
       NativeSCDMergeTransformer,
       StagingTransformer,
   )
   from spark_quick_start.orchestration.transformers import (
       SCDIncrementalTargetTableTransformer,
       MaterializeFrameTransformer,
       MaterializeTransformer,
   )

   result = (
       LoadOrchestrator(load_context_json, spark, platform="fabric")
       .add(StagingTransformer())
       .add(SCDIncrementalTargetTableTransformer())
       .add(MaterializeFrameTransformer())
       .add(SCDChangeDetectorTransformer())
       .add(MaterializeTransformer())
       .add(CandidateGuardTransformer())
       .add(NativeSCDMergeTransformer())
       .run()
   )
   ```

   By default each materialize step is a write and a read. Set `runtime_context.materialize_strategy`
   to `"cache"` to persist the frame in memory instead (`MEMORY_AND_DISK`, one action to fill it, no
   scratch table). Choose `"cache"` on a job cluster with memory headroom; choose `"delta"` (the
   default) on serverless, for very large frames, or when executor memory is tight. With
   `runtime_context.serverless` true, `"cache"` falls back to `"delta"` and logs it, since persist
   does not work there; the library does not detect serverless, so set that flag yourself.

   Cache mode is not a stable snapshot of the target after the write. The cached frames have lineage
   over the target Delta table, and once the `MERGE` has committed they are recomputed against the
   merged table, so a read after the write sees the merged rows: measured on a local Delta table,
   the writer's `row_count_out` (with `capture_row_counts` on) was 0 in `"cache"` mode and 1 in
   `"delta"` mode, for the same single changed candidate, while every count before the write
   agreed. The target rows written are the same in both modes. A step placed after the writer, or
   a metric read after it, should use `"delta"`, where the scratch table is a snapshot.

   A cached frame is released when `LoadOrchestrator.run()` ends (on success, error or early exit),
   not earlier: while the run lasts it holds executor memory, and `MEMORY_AND_DISK` spills to disk
   under pressure. Prefer `"delta"` for a frame that does not fit. If an executor is lost, its
   cached blocks are recomputed from lineage.

   A materialize step is a pass over the frame either way: it pays off when the frame is large or expensive to
   compute, and costs more than it saves on a small one. In `"delta"` mode, each step writes a scratch table named from
   `staging_table_path`, `global_context.id` and the step's position, which you cannot choose; it is
   dropped and rewritten each run, and kept after the run. Keep `id` unique among the load contexts
   that share a `staging_table_path`, and do not run one context twice at once. The names, the
   lifecycle, the tables left behind when a step moves, and the permissions are in
   [Tables the library creates](../reference/load-context.md#tables-the-library-creates).

4. **Turn off measurements you do not read.** In `runtime_context`, `capture_partitions` is on by
   default and can run a shuffle to report a partition count; set it to `false` when a step's
   `measure_ms` is most of its time. `capture_row_counts` and `capture_plans` are off by default;
   the first costs two Spark actions per step. See
   [metrics and watermarks](../architecture/metrics-and-watermarks.md).

## Check it worked

The load writes the same rows as before, and the steps you changed take less time. Measured
locally over two `Incremental` loads, the pipeline in step 3 recorded all seven steps, wrote the
second load's change and new key as SCD2 would, and left two scratch tables beside the staging
table.

```python
print(result.status, [stage.stage for stage in result.stages])
```

## When it goes wrong

`run()` does not raise: each message below arrives as `result.status == "error"`, with the text in
`result.error_details["error_message"]`.

**`SCDIncrementalTargetTableTransformer cannot be used with load_type='Full'`.** Use
`SCDTargetTableTransformer` for a `Full` load (step 2).

**`Refusing to drop '<path>': it exists but is not a Delta table`.** The scratch path points at
something else, such as a folder of other files; check `staging_table_path` and `id`. The check goes
through the adapter (`path_has_content`), which sees only what the driver's local filesystem shows
with entries, such as a local path or a Databricks `/Volumes/...` path (FUSE-visible). Anything else,
a `/mnt/...` path or a URI (`abfss://`, `dbfs:/`), is not refused: `drop_delta` asks
`delta_table_exists` as it did before the check existed, drops it only when `isDeltaTable` answers
True, and otherwise does nothing. A path that is not a Delta
table is left alone and the step writes its table there. The check is enough because every scratch
path is built from `staging_table_path` and `id` (F129; see
[Tables the library creates](../reference/load-context.md#tables-the-library-creates)): a mistyped path no longer lands on a directory of
someone else's files.

## See also

- [Transformer reference](../reference/transformers.md): the options of the target readers and
  the materialize steps.
- [Metrics and watermarks](../architecture/metrics-and-watermarks.md): what each measurement costs.
- [Inspect a run](inspect-a-run.md): reading the timings.
