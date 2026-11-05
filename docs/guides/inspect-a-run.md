# Inspect a run

At the end of this guide you can tell what a load did and where its time went: its result, a
timing per step, sample rows between steps, each step's query plan, and its log. It is for a user
of the library checking or diagnosing a load. The steps are the same on every platform.

## Before you start

- You can build a pipeline by hand; see [Build a pipeline](build-a-pipeline.md). This guide uses
  its `load_context_json`, `spark` and `platform="fabric"`.

## Steps

1. **Add the inspection steps.** `DebugTransformer` prints sample rows where you put it.
   `MetricsChartTransformer` prints each step's duration, slowest first, and
   `PlanReportTransformer` prints each step's query plan; put both last. They run even when the
   pipeline halts early, which is when you need them most.

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
   from spark_quick_start.orchestration.transformers import MetricsChartTransformer, PlanReportTransformer

   result = (
       LoadOrchestrator(load_context_json, spark, platform="fabric")
       .add(StagingTransformer())
       .add(DebugTransformer("staged", n=2))
       .add(SCDTargetTableTransformer())
       .add(SCDChangeDetectorTransformer())
       .add(CandidateGuardTransformer())
       .add(NativeSCDMergeTransformer())
       .add(MetricsChartTransformer())
       .add(PlanReportTransformer())
       .run()
   )
   ```

   For the plans, set `"capture_plans": true` in the load context's `runtime_context`. Capturing a
   plan runs no Spark action, but the text is large, so it is off by default. `DebugTransformer`
   is not free: it counts each frame it prints and collects `n` rows, two Spark actions, so add it
   with `add_if` or `enabled=` rather than leaving it in a production load.

2. **Read the result.** `status` is `"success"` or `"error"`; on an error, `error_details` holds
   the exception. `early_exit` says the pipeline stopped early, and `early_exit_reason` why.
   `metrics` holds the counts, the watermark and the timing of the whole run.

   ```python
   print(result.status, result.early_exit, result.early_exit_reason)
   print(result.metrics["inserted_count"], result.metrics["updated_count"], result.metrics["duration_seconds"])
   ```

3. **Read each step's metrics.** `result.stages` has one entry per step that ran, in order.
   `duration_ms` is the step's time with its measurement included, and `measure_ms` how much of it
   the measurement took; a large `measure_ms` means reading the partition count ran a shuffle.

   ```python
   for stage in result.stages:
       print(stage.stage, stage.duration_ms, stage.measure_ms)
       if stage.plan:
           print(stage.plan)
   ```

   Read here, after `run()` returns, the timings and plans reach your notebook in full, whatever
   the log settings are.

4. **Read what the run printed.** While the load logs to a destination, everything printed during
   the run, the sample rows, the chart and the plans included, goes into the log rather than to
   the notebook's output, and only its last 50,000 characters are kept. That was measured with
   `LocalAdapter`; the Fabric and Databricks adapters take the same path in the code, but it has not
   been observed on a workspace here. Read it from the result; `result.logs` is empty when there
   was no log destination:

   ```python
   import json

   if result.logs:
       print(json.loads(result.logs)["stdout_tail"])
   ```

   The same log is written as a file under `logging_context.log_file_dir`. Its readers may not be
   the table's: for sensitive data, set `suppress_stdout_tail`, which discards what the run prints
   rather than sending it to the notebook, and leave `DebugTransformer` out or use `n=0` to
   print no rows; see [Write a load context](write-a-load-context.md).

## Check it worked

Measured locally: the stages listed the eight steps in order; the log's `stdout_tail` held the
`DEBUG CHECKPOINT: staged` table with two rows, an `=== Pipeline Stage Duration ===` chart with
`StagingTransformer` slowest, and an `=== Pipeline Query Plans ===` section with a plan for each step
before the report. `PlanReportTransformer` prints before its own step is recorded, so it has no
section of its own: eight stages give seven plans.

## When it goes wrong

**The plan section says no plans were captured.** `capture_plans` is not `true` in
`runtime_context` (step 1).

**Nothing the run printed appears in the notebook.** It went into the log; read `stdout_tail`
(step 4).

**`stdout_tail` is empty.** `logging_context.suppress_stdout_tail` is `true`: what the run printed
was discarded. Read timings and plans from `result.stages` instead (step 3).

**`result.logs` is `""`, or `json.loads` raises `JSONDecodeError`.** The load had no log
destination, as with `MockPlatformAdapter` or an adapter whose `get_log_persistence_adapter`
returns `None`, or its log session failed; printed output then went to the notebook.

**The debug table is missing from `stdout_tail`.** Only the last 50,000 characters are kept, and
the plans, printed last, can push it out. Read plans from `result.stages` (step 3).

**A step takes minutes and most of it is `measure_ms`.** Reading the partition count forced a
shuffle; set `"capture_partitions": false` in `runtime_context` for that pipeline.

## See also

- [Execution-result reference](../reference/execution-result.md): every field of the result and
  of a stage.
- [Metrics and watermarks](../architecture/metrics-and-watermarks.md): what each measurement costs.
- [Transformer reference](../reference/transformers.md): the options of the three inspection steps.
