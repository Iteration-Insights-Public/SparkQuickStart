# Run your first load

At the end of this guide you have an SCD2 Silver table with its first rows, loaded by
`default_pipeline` from one staging notebook, and an `ExecutionResult` that says so. It is for a
user of the library setting up their first entity. The steps are the same on Microsoft Fabric,
Azure Databricks and a local machine; where a step differs, its part for each platform is
labelled.

## Before you start

- The library is installed where your notebooks run, as a wheel or as a copy of `src/`; see
  [Installation](../../README.md#installation).
- A `spark` session with Delta Lake is in scope. Fabric and Databricks notebooks have one.
- You know where the staging and target tables go: a path or a Unity Catalog name for each.
  Replace the example paths below, `/lake/...`, with yours.

## Steps

1. **Write the staging function.** It returns the entity's rows as a DataFrame: the business
   columns and the business key. The library adds every SCD column, the deleted flag included.

   ```python
   def stage_customers(storage_path, staging_context, global_context, notebook_name):
       return spark.createDataFrame(
           [(1, "Ada"), (2, "Grace")],
           "customer_id int, name string",
       )
   ```

2. **Make it a staging notebook.** `create_staging_main_function` wraps the function: it clears
   the staging table the load gives it, writes the rows there, and returns an
   `ExecutionResult`. Name the notebook `NB_Stage_Customer`, the `nb_name` in step 3. The load
   passes it one argument, `stage_context_str`, and reads the result from the notebook's exit
   value.

   ```python
   from spark_quick_start.orchestration.staging_pipeline import create_staging_main_function

   main = create_staging_main_function(stage_customers, spark=spark)
   ```

   - **Fabric:** declare `stage_context_str` in a parameters cell, then end with
     `notebookutils.notebook.exit(main(stage_context_str).to_json())`. The default adapter is
     `FabricAdapter`.
   - **Databricks:** import `DatabricksAdapter` from `spark_quick_start.adapters`, pass
     `adapter=DatabricksAdapter()` to `create_staging_main_function`, read
     the argument with `dbutils.widgets.get("stage_context_str")`, and end with
     `dbutils.notebook.exit(main(stage_context_str).to_json())`.
   - **Locally:** there is no notebook; register the function with a `LocalAdapter` under the
     notebook's name. You use `local_adapter` in step 4.

     ```python
     from spark_quick_start.adapters import LocalAdapter

     staging_adapter = LocalAdapter()
     main = create_staging_main_function(stage_customers, adapter=staging_adapter, spark=spark)
     local_adapter = LocalAdapter(
         notebook_registry={"NB_Stage_Customer": lambda args: main(args["stage_context_str"]).to_json()},
     )
     ```

3. **Write the load context.** Name the staging notebook, the tables and the business key.
   `staging_table_schema` is required and lists the staged columns: the load merges each
   notebook's table into `staging_table_path` against it, and a context with staging stages and no
   schema is refused before any notebook runs.

   A source that marks deleted rows stages a `scdDeletedFlag` column as well, `"Y"` for a deleted
   row, and lists it in the schema: a `"Y"` row whose key is in the target is closed out as
   deleted, a `"Y"` row whose key is not in the target, or whose current row is already deleted, is
   skipped, and NULL or any other value means not deleted. To read the
   signal from another column, name it in `scd_context.source_deleted_flag_column`.

   ```json
   {
     "global_context": {
       "id": "ent_1",
       "entity_name": "Customer",
       "source_system": "CRM",
       "target_table_path": "/lake/silver/customer",
       "staging_table_path": "/lake/staging/customer",
       "staging_table_schema": {
         "type": "struct",
         "fields": [
           {"name": "customer_id", "type": "integer", "nullable": true, "metadata": {}},
           {"name": "name", "type": "string", "nullable": true, "metadata": {}}
         ]
       }
     },
     "staging_context": {"stages": [{"nb_name": "NB_Stage_Customer"}]},
     "scd_context": {"business_key_column": "customer_id"},
     "runtime_context": {"correlation_id": "first-load-1", "pipeline_name": "customer", "application_name": "crm"},
     "logging_context": {"logging_level": "INFO", "log_file_name": "customer_load", "log_file_dir": "/lake/logs"}
   }
   ```

   Every field is on the [load-context reference](../reference/load-context.md).

4. **Run the load.** In the load notebook, put step 3's JSON in a string, `load_context_json`,
   usually a notebook parameter, and pass it to `default_pipeline`.

   ```python
   from spark_quick_start.orchestration import default_pipeline
   ```

   - **Fabric:** run it with `platform="fabric"`, and end the notebook with
     `notebookutils.notebook.exit(result.to_json())` if a pipeline calls it.

     ```python
     result = default_pipeline(load_context_json, spark, platform="fabric").run()
     ```

   - **Databricks:** the same call with `platform="databricks"`.
   - **Locally:** pass the adapter from step 2 instead of a platform.

     ```python
     result = default_pipeline(load_context_json, spark, adapter=local_adapter).run()
     ```

## Check it worked

`result.status` is `"success"`, `result.metrics["inserted_count"]` is `2`, and the target table
has two rows, each with `scdVersion` `1`, `scdCurrentFlag` `"Y"` and `scdDeletedFlag` `"N"`.

```python
print(result.status, result.metrics["inserted_count"])
```

## When it goes wrong

`run()` does not raise: a failure comes back as `result.status == "error"`, with the reason in
`result.error_details["error_message"]`.

**`staging_context.stages is set but global_context.staging_table_schema is missing or empty`.**
Add `staging_table_schema` to `global_context` (step 3). Each notebook writes its own table,
`<staging_table_path>__<nb_name>`, and only the schema-checked merge builds the table the load
reads. All the tables a load writes beside `staging_table_path` are listed in
[Tables the library creates](../reference/load-context.md#tables-the-library-creates).

**`LoadOrchestrator: unknown platform`.** `platform` is not `"fabric"`, `"databricks"` or
`"local"`; this one raises from `default_pipeline` itself, before `run()`.

## See also

- [Load-context reference](../reference/load-context.md): every field of the load context.
- [Orchestrator reference](../reference/orchestrator.md): `default_pipeline` and the `platform` values.
- [Execution-result reference](../reference/execution-result.md): everything `result` holds.
