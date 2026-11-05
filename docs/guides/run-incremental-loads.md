# Run incremental loads

At the end of this guide each run of your load stages only the source rows changed since the last
run, using a watermark taken from a source column and handed from one run to the next. It is for
a user of the library whose entity is loaded from staging notebooks; a load from a
[DQD](load-a-table-with-a-dqd.md) uses the same `hwm_context`. The steps are the same on every platform.

## Before you start

- You can run a load from a staging notebook; see [Run your first load](run-your-first-load.md).
- Your source has a column that grows with every change, such as a last-modified timestamp. Here
  it is `modified_at`.

## Steps

1. **Keep `load_type` as `"Incremental"`**, the default. A `Full` load treats every target row
   missing from the staged rows as deleted, so with a filtered source it marks every row you did
   not stage as deleted (measured locally).

2. **Do not use the writer's watermark to select source rows.** Without `hwm_context`, every
   `NativeSCD*` writer reports the latest `scdEffectiveFrom` in the target, which is the load
   time (`global_context.load_timestamp`, the time of the run by default), not a source value. A source row changed after the staging notebook read the source
   but before the write would fall below it and never be staged. Take the watermark from the
   source column instead.

3. **Name the column in `hwm_context`.** The first run has no `hwm_value`. `StagingTransformer`
   then reports the latest `modified_at` among the staged rows as the run's `hwm_value_after`,
   one Spark action, and keeps `hwm_value` when nothing was staged.

   ```json
   {"hwm_context": {"hwm_column_name": "modified_at"}}
   ```

   The value is formatted by Spark, in the session time zone, and read back by Spark in the same
   zone, so keep `spark.sql.session.timeZone` the same from run to run. Converting it in Python
   instead shifts it by the driver's offset: measured locally, a Denver driver on a UTC session
   turned `11:00:00` into `05:00:00`.

4. **Filter the source in the staging function.** Declare a parameter named `hwm_context`, and
   the function receives the section: both keys, `hwm_value` `None` on the first run. Stage
   everything when there is no value. `modified_at` goes into `staging_table_schema` with the
   other staged columns.

   ```python
   from pyspark.sql import functions as F


   def stage_customers(storage_path, staging_context, global_context, notebook_name, hwm_context):
       df = spark.table("crm.customers").select("customer_id", "name", "modified_at")
       since = hwm_context["hwm_value"]
       return df if since is None else df.filter(F.col("modified_at") > F.lit(since))
   ```

5. **Run the load** with the pipeline you use, such as `default_pipeline`. Nothing else is
   needed: the staging step reports the watermark.

   ```python
   from spark_quick_start.orchestration import default_pipeline

   result = default_pipeline(load_context_json, spark, platform="fabric").run()
   ```

6. **Hand the watermark to the next run.** Store `result.metrics["hwm_value_after"]` wherever
   your pipeline keeps state, and put it in the next run's `hwm_context`:

   ```json
   {"hwm_context": {"hwm_column_name": "modified_at", "hwm_value": "2026-09-02 10:00:00"}}
   ```

## Check it worked

Measured over three local loads: the first staged both rows and reported `hwm_value_after`
`2026-09-02 10:00:00`; after one row changed and one was added, the second, given that value,
staged only those 2 rows and moved the watermark to their latest `modified_at`; the third, with
nothing new, staged 0 rows, ended with `early_exit` true, and kept the watermark.

```python
print(result.metrics["source_count"], result.metrics["hwm_value_before"], result.metrics["hwm_value_after"])
```

## When it goes wrong

**Rows you did not stage appear as deleted.** `load_type` is `"Full"` (step 1).

**Every run stages the whole source.** The next run's `hwm_context` has no `hwm_value`, or the
staging function has no parameter named `hwm_context` (steps 4 and 6).

**The reported watermark is a load time, not a source value.** The load context has no
`hwm_context`, so the writer's `scdEffectiveFrom` is reported (steps 2 and 3).

**`hwm_context: hwm_column_name 'modified_at' is not a column of the staged rows`.** The staging
function does not return the column (step 4).

## See also

- [Metrics and watermarks](../architecture/metrics-and-watermarks.md): how the run's watermark is chosen.
- [Load-context reference](../reference/load-context.md#hwmcontext): `hwm_context`.
- [Load a table with a DQD](load-a-table-with-a-dqd.md): the same watermark without a notebook.
