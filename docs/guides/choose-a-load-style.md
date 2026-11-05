# Choose a load style

At the end of this guide you have picked how your entity's changes are written — an SCD merge,
or an append-only load for events that never change — and built the pipeline for it. It is for a user of the library setting up an entity whose data
does not fit `default_pipeline`. The choice is the same on every platform.

## Before you start

- You can run a load; see [Run your first load](run-your-first-load.md). This guide uses its
  `load_context_json` and `spark`, and `platform="fabric"`, which you change for your platform
  as that guide says.

## Steps

1. **Decide from your data.** Here is what each style leaves in the target after two loads,
   where the second changes customer 1's name and adds customer 3. Measured by running each style
   locally, with `scd_type` `"scd2"`, the default.

   | Style | After the second load | `inserted_count`, `updated_count` |
   |---|---|---|
   | Merge (`default_pipeline`) | Customer 1 has two rows: version 1 closed (`scdCurrentFlag` `N`), version 2 current | `2`, `1` |
   | Append-only | The second load fails, and the table keeps the first load's rows | `0`, `0` |

   - Pick **merge** when you need the entity's history with exactly one current row per key. It
     is the default; for a history of changes, use it with `scd_type` `"scd2"`.
   - Pick **append-only** for events that never change once written. A changed row is treated as
     a data error, and the load fails rather than write a conflicting fact.

2. **Build the pipeline for your style.** Merge has a factory:

   ```python
   from spark_quick_start.orchestration import default_pipeline

   merge_load = default_pipeline(load_context_json, spark, platform="fabric")
   ```

   Append-only is built by hand, with `SCDCandidateVersionGuardTransformer`, which
   refuses a source that stages one key twice and, unlike `CandidateGuardTransformer`, does not stop
   the load when there is nothing new, before the writer:

   ```python
   from spark_quick_start.orchestration import (
       SCDChangeDetectorTransformer,
       LoadOrchestrator,
       StagingTransformer,
       SCDTargetTableTransformer,
   )
   from spark_quick_start.orchestration.transformers import (
       SCDCandidateVersionGuardTransformer,
       NativeSCDAppendOnlyTransformer,
   )

   append_only_load = (
       LoadOrchestrator(load_context_json, spark, platform="fabric")
       .add(StagingTransformer())
       .add(SCDTargetTableTransformer())
       .add(SCDChangeDetectorTransformer())
       .add(SCDCandidateVersionGuardTransformer())
       .add(NativeSCDAppendOnlyTransformer())
   )
   ```

3. **If new nullable columns will be added to the source over time, hash in schema order.** The
   default detector hashes columns in alphabetical order and gives a NULL a placeholder value, so
   adding a column can change every row's hash. `SCDGracefulChangeDetectorTransformer` hashes in the
   order of `staging_table_schema` and drops trailing NULL columns, so a new nullable column at the
   end of the schema does not version rows where it is NULL. Put it in place of
   `SCDChangeDetectorTransformer`:

   ```python
   from spark_quick_start.orchestration import (
       CandidateGuardTransformer,
       SCDGracefulChangeDetectorTransformer,
       NativeSCDMergeTransformer,
   )

   graceful_load = (
       LoadOrchestrator(load_context_json, spark, platform="fabric")
       .add(StagingTransformer())
       .add(SCDTargetTableTransformer())
       .add(SCDGracefulChangeDetectorTransformer())
       .add(CandidateGuardTransformer())
       .add(NativeSCDMergeTransformer())
   )
   ```

   Choose the detector before the first load. After a switch, a row keeps its hash only when its
   hashed columns are already in alphabetical order in the schema and the last of them is not
   NULL, which includes a one-column row whose value is not NULL. Every other row gets a new hash,
   and the next load that receives it writes it as changed.

4. **Run the one you chose.**

   ```python
   result = merge_load.run()
   ```

## Check it worked

Run a second load in which one row changes, and read the target: its rows and the two counts
match the table in step 1 for your style.

```python
print(result.status, result.metrics["inserted_count"], result.metrics["updated_count"])
```

## When it goes wrong

`run()` does not raise: each message below arrives as `result.status == "error"`, with the text in
`result.error_details["error_message"]`.

**`NativeSCDAppendOnlyTransformer: candidates classified as 'changed' for an append-only
entity`.** A row with a known key arrived with different values. For events this is the data
error the style exists to catch; if such changes are legitimate, the entity is not append-only:
use merge with `scd_type` `"scd2"`.

**`staging_context.stages is set but global_context.staging_table_schema is missing or empty`.**
A load with staging notebooks is refused before any step runs. Add the schema to the load context;
see [Write a load context](write-a-load-context.md). A DQD source has no stages, so there the same
gap surfaces later, from the graceful detector, as
`SCDGracefulChangeDetectorTransformer requires staging_table_schema in global_context`.

**An append-only load fails with a message naming duplicate keys and versions.** Two staged rows
share a business key and version: the source sent the same row twice. Fix the staging query.

## See also

- [Transformer reference](../reference/transformers.md): the writers, the two guards and the
  detectors.
- [Orchestrator reference](../reference/orchestrator.md): `default_pipeline`.
- [Build a pipeline](build-a-pipeline.md): building a pipeline by hand.
