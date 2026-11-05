# Write a staging notebook

At the end of this guide you have a staging notebook that the load runs: it reads the source,
returns the rows for one entity, and hands back a result with its row counts and its log. It is
for a user of the library connecting a source. The notebook's code is the same on every platform;
only how it receives its argument and ends differs, as
[Run your first load](run-your-first-load.md), step 2, shows.

## Before you start

- You have a load context that lists the notebook in `staging_context.stages`; see
  [Write a load context](write-a-load-context.md).
- A `spark` session is in scope, as it is in a Fabric or Databricks notebook.

## Steps

1. **Write the staging function.** The load calls it with four arguments and expects a DataFrame
   back:

   - `storage_path`: the `data_storage_path` the load was given, for building source paths;
   - `staging_context`: this stage's `staging_context` object from the load context, passed
     through unread, for your own settings, such as a region (the watermark comes as `hwm_context`);
   - `global_context`: the load's `source_system` and this stage's `staging_table_path`, and
     nothing else: `global_context` accepts no keys of your own;
   - `notebook_name`: the running notebook's name, or `None` when the platform cannot report it.

   The function may also declare `extensions_context`, the load's own data, handed over whole;
   see "What the notebook receives" below.

   Return the business columns and the business key, and leave the SCD columns to the load. Add
   `scdDeletedFlag`, `"Y"` for a deleted row, only if the source marks deletes; see
   [Run your first load](run-your-first-load.md), step 3.

   ```python
   from pyspark.sql import functions as F


   def stage_customers(storage_path, staging_context, global_context, notebook_name):
       region = staging_context.get("region", "EU")
       rows = spark.createDataFrame(
           [(1, "Ada", "EU"), (2, "Grace", "US")], "customer_id int, name string, region string"
       )
       return (
           rows.filter(F.col("region") == region)
           .select("customer_id", "name")
       )
   ```

   The `region` it reads is set on the stage's own `staging_context`, in the load context:

   ```json
   {
     "staging_context": {
       "stages": [{"nb_name": "NB_Stage_Customer_US", "staging_context": {"region": "US"}}]
     }
   }
   ```

   **What the notebook receives.** `main` is given one JSON string, `stage_context_str`, with
   a section for each part of the load context a notebook may need:

   | Section | Holds |
   |---|---|
   | `global_context` | `source_system` and this notebook's `staging_table_path` |
   | `staging_context` | the `staging_context` key of this stage's entry in `staging_context.stages`, `{}` when it has none |
   | `extensions_context` | the load's `extensions_context`, whole, `{}` when it has none |
   | `platform_context` | `data_storage_lakehouse_path` and `data_engineering_lakehouse_path`, set by the library |
   | `runtime_context`, `scd_context`, `logging_context` | the load's run identity, SCD column names and logging settings |
   | `hwm_context` | the load's watermark, only when the load has one |

   Your own data goes in the load context's `extensions_context`, not in `global_context`,
   which refuses a key it does not read. Declare `extensions_context` as a parameter, passable by
   keyword, to receive it, as `hwm_context` is received ([Run incremental
   loads](run-incremental-loads.md)):

   ```python
   def stage_with_rules(storage_path, staging_context, global_context, notebook_name, extensions_context):
       rules = extensions_context.get("acme.rules", {})
       return spark.createDataFrame([(1, "Ada")], "customer_id int, name string")
   ```

2. **Log to the result, if you want the notebook's log back in the load.** Pass
   `log_init_fn=init_logs` to the wrapper in the next step: it starts an in-memory session named
   from the load context's `log_file_name`, and `get_logs_json` hands that log back in the result.
   Anything the notebook prints travels back in the log's `stdout_tail`; the rest of `init_logs`'s
   options are on the [logging reference](../reference/logging.md).

   ```python
   from spark_quick_start.spark_logging import get_logs_json, init_logs
   ```

3. **Wrap the function.** `create_staging_main_function` returns `main`, which takes the notebook's
   one argument, `stage_context_str`. `main` clears this stage's staging table, writes the rows
   there with a `NotebookName` column added, and returns an `ExecutionResult` whose
   `source_count` and `target_count` are the rows written, read from the write itself. A failure
   in your function comes back as a result with `status` `"error"`, not an exception. Pass `spark`
   so the table is confirmed to be a Delta table before it is cleared. Leave out `log_init_fn=`
   and `get_logs_fn=` if you skipped step 2.

   ```python
   from spark_quick_start.orchestration.staging_pipeline import create_staging_main_function

   main = create_staging_main_function(
       stage_customers, spark=spark, log_init_fn=init_logs, get_logs_fn=get_logs_json
   )
   ```

   On Databricks also pass `adapter=DatabricksAdapter()`; the default is `FabricAdapter`.

4. **End the notebook with the result.** Read `stage_context_str` as your platform passes it, call
   `main`, and exit with `result.to_json()`, as [Run your first load](run-your-first-load.md),
   step 2, shows for each platform.

## Check it worked

After a load runs the notebook, the stage's table, `<staging_table_path>__<nb_name>`, holds your
rows and a `NotebookName` column, and the notebook's result has `status` `"success"`, the row count
in `metrics["source_count"]`, and, with step 2, a JSON log in `logs` whose `python_logs` include
`Starting staging` and `Written <n> rows`. Measured locally with a one-row stage. The library never
drops that table; what the notebook does to it between runs, and who cleans up after a rename, is in
[Tables the library creates](../reference/load-context.md#tables-the-library-creates).

```python
result = main(stage_context_str)
print(result.status, result.metrics["source_count"])
```

## When it goes wrong

**The load's result says a staging notebook failed.** Read that notebook's own result: its
`error_details` hold the exception your function raised.

**`storage_path` is empty.** The load was run without `data_storage_path`.

## See also

- [Run your first load](run-your-first-load.md): the platform plumbing and a whole load.
- [Run incremental loads](run-incremental-loads.md): filtering by the watermark in `hwm_context`.
- [Logging reference](../reference/logging.md): `init_logs` and the log it returns.
