# Handle early exit and errors

At the end of this guide your load notebook tells the three outcomes of a load apart: it wrote,
it had nothing to write, or it failed. It fails its own run when the load failed, so the pipeline
or job that called it can act. It is for a user of the library running loads from a pipeline. The
steps are the same on every platform except how the notebook ends, in step 3.

## Before you start

- You can run a load; see [Run your first load](run-your-first-load.md). This guide uses its
  `load_context_json`, `spark` and `platform="fabric"`.

## Steps

1. **Read the outcome from the result.** `run()` does not raise: whatever goes wrong inside it,
   including a load context that is not JSON or a pipeline with no steps, comes back as a result
   with `status` `"error"`. Building the pipeline can raise, as for a `platform` it does not
   know, but running it does not.

   | `status` | `early_exit` | What happened |
   |---|---|---|
   | `"success"` | `False` | The load ran to the end; `metrics` holds what it wrote. |
   | `"success"` | `True` | A step stopped the load because there was nothing to do; `early_exit_reason` says why. Nothing was written to the target. A merge load whose batch holds only deletes already recorded (a flag that stays `"Y"`, or a `Full` load that still omits a deleted key) ends this way too: the delete is not repeated (backlog F142). |
   | `"error"` | `False` | The load failed; `error_details` holds `error_type`, `error_message` and `traceback`. |

   ```python
   from spark_quick_start.orchestration import default_pipeline

   result = default_pipeline(load_context_json, spark, platform="fabric").run()

   if result.status == "error":
       print(result.error_details["error_type"], result.error_details["error_message"])
   elif result.early_exit:
       print("nothing to load:", result.early_exit_reason)
   else:
       print(result.metrics["inserted_count"], result.metrics["updated_count"])
   ```

   A merge load exits early when its batch matches the target, or is empty:
   `CandidateGuardTransformer` finds no candidates and stops the load with `no candidates — source
   matches target, nothing to merge`. It is the only shipped step that stops a load, so an append-only load, which composes
   `SCDCandidateVersionGuardTransformer` instead, runs to the end with nothing new and inserts 0 rows. On an early exit without `hwm_context`, `hwm_value_after` still
   holds the target's watermark, or `None` when there is no target table yet. With
   `hwm_context`, the source step owns it: `StagingTransformer` reports the maximum of the staged
   rows' `hwm_column_name`, or the incoming `hwm_value` when it staged none.

2. **Find the step that failed.** `result.stages` lists the steps that finished, in order; the
   one after the last of them failed. `error_message` is the exception's text: many of the
   library's own begin with the step's name, but not all. A seal refusal begins with the table's
   identifier, a create refusal with `Refusing to create a Delta table at`, and a load-context
   error usually with the section or field it names, so go by `stages` rather than the message's
   first word. An empty `stages` means the load failed before or in its source step: read
   `error_message` to tell a load-context error from a staging one. A failed staging notebook's
   message begins `Staging failed: <n> notebook(s)`, followed by each notebook's name and its own
   error.

   ```python
   print([stage.stage for stage in result.stages])
   ```

3. **End the notebook so its caller sees the outcome.** Raise on an error, so the platform marks
   the run failed and the pipeline or job can stop or retry. Once the load context has parsed,
   the load's log is written before `run()` returns; a context that does not parse, or parses but fails
   validation, leaves no log, only `error_details`. Otherwise exit with the result, so the caller can read it.

   ```python
   if result.status == "error":
       raise RuntimeError(f"{result.error_details['error_type']}: {result.error_details['error_message']}")
   ```

   - **Fabric:** then `notebookutils.notebook.exit(result.to_json())`.
   - **Databricks:** then `dbutils.notebook.exit(result.to_json())`.
   - **Locally:** there is no caller; use `result` as it is.

   Exiting with the result on an error instead would mark the run successful, and every caller
   would have to read `status` to notice the failure. How each platform shows a raised error to
   its caller was not observed on a workspace here.

4. **Stop a load early from a step of your own**, when there is nothing it should do. Return
   `state.with_halted(reason)` from `_run`: the process steps after it are skipped, except those
   built on `AlwaysRunTransformer`, and the result reports an early exit with your reason. A plain
   function added with `.add()` still runs. To stop and
   fail instead, raise. This step checks that at least `minimum` rows were staged; `limit` keeps
   its count to at most `minimum` rows, but it is still a Spark action.

   ```python
   from spark_quick_start.orchestration import ProcessTransformer


   class RequireRows(ProcessTransformer):
       def __init__(self, minimum):
           self.minimum = minimum

       def _run(self, state, ctx):
           staged = state.df.limit(self.minimum).count()
           if staged < self.minimum:
               return state.with_halted(f"only {staged} rows staged, expected at least {self.minimum}")
           return state
   ```

   Put it after the source step, before anything that writes; see
   [Write a custom transformer](write-a-custom-transformer.md).

## Check it worked

Measured locally over one entity: a first load reported `success` with 2 rows inserted; the same
batch again reported `success`, `early_exit` true with the reason in step 1, and four stages; a
staging function that raised `ConnectionError` gave `error`, no stages, and its message after
`Staging failed: 1 notebook(s)`; a first load with nothing staged exited early and created no
table; and `RequireRows(3)` on a two-row batch stopped the load with
`only 2 rows staged, expected at least 3`.

## When it goes wrong

**`Full load aborted — source DataFrame has 0 rows but target has <n> rows`.** A `Full` load was
given no rows, which would mark every target row deleted. Fix the staging, then run again.

**`CandidateGuardTransformer: <n> business key(s) have more than one candidate row`.** The source
has a business key more than once, and each copy would change the target; remove the duplicates
in staging. Or the target already holds more than one current row for that key; check the target
before the staging. A duplicate whose other copy equals the target row is not caught: the changed copy
loads (backlog F134).

**An SCD, append-only or satellite load fails because the target already exists** (Delta's `Cannot write to already existent path` for a path, Spark's `... already exists` for a catalog name).
The target step could not see the table, most often through a passing error such as a
permission or storage hiccup, so the load took itself for a first load and tried to create the
table; creating refuses an existing one, and the table is untouched (backlog F209 for the SCD
merge, F210 for the append-only writer and a Data Vault satellite; a hub or link inserts only
keys it lacks and is not affected). Check access
to the target and run again. Two first loads of one target started together end the same way for
one of them; its rerun is an ordinary incremental load.

**An SCD load returns `status` `"error"` with `<path> is sealed with spark_quick_start.scd_<name> = '<old>', and this load's value ... is '<new>'`.**
The target records the flag values, `default_max_timestamp`, hash separator and session time zone
its rows were written with, and this load's `scd_context` (or, for the separator, the change
detector's constructor argument; for `scd_time_zone`, the Spark session's
`spark.sql.session.timeZone`) gives one differently. The load is refused before anything is written, and
the table is unchanged (backlog F211). Usually a context was edited by mistake: put the sealed
value back. To change it on purpose, rewrite the rows to the new value, then
`ALTER TABLE ... SET TBLPROPERTIES` (or `UNSET`) that property; see the load context reference.
For `scd_time_zone` no `UPDATE` applies and the context is not at fault: run the load in a session
with `spark.sql.session.timeZone` set to the sealed zone (set it explicitly; the default is the
driver JVM's zone). Changing the sealed zone on purpose re-versions every key whose row has a
timestamp column.

**An SCD load succeeds, with a WARNING that the table "could not be sealed", "had no SCD seal", or
"is not a target the seal supports".** None of these is an error; each leaves the load
`success` and its data committed. *Could not be sealed*: the write went through but recording the
seal failed (on Unity Catalog the loading principal needs to own the table or hold `MANAGE`, or a
concurrent writer conflicted); the table stays unsealed and the next load tries again, so a
privilege that is never granted means a warning on every load. A different case, with its own
warning ("another load sealed the table with different values"): a concurrent load sealed the table
while this one wrote it, so it may hold rows written two ways and the next load will be refused;
check the rows before it. *Had no SCD seal*: a table written
before the seal existed was sealed with this load's values. *Not a target the seal
supports*: its name is one the table-property helpers refuse, so it is neither checked nor sealed.

**`ValueError: Refusing to create a Delta table at '<path>'`.** The target path is on the
driver's own filesystem and holds files but no Delta table: another dataset, or what a crashed
first load left behind. Check `target_table_path`; if it is a crashed first load, remove the
directory by hand and run again.

**`early_exit` is true on a first load, and there is no target table.** Nothing was staged; the
load stopped before the write that would have created the table. Check the staging.

**`SCDContext requires 'business_key_column'`, or another message naming a context section.** The
load context is missing a field; see [Write a load context](write-a-load-context.md).

**`JSONDecodeError`.** `load_context_json` is not valid JSON.

**`ValueError: LoadOrchestrator: unknown platform '<name>'` is raised, not returned.** The
`platform` value is misspelt; it takes `fabric`, `databricks` or `local`.

**The calling pipeline reports success although the load failed.** The notebook exited with the
result instead of raising (step 3).

## See also

- [Execution-result reference](../reference/execution-result.md): every field of the result.
- [Inspect a run](inspect-a-run.md): timings, plans and the log of a run.
- [Pipeline model](../architecture/pipeline-model.md): how halting skips the steps after it.
