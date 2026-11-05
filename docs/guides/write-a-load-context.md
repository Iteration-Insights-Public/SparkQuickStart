# Write a load context

At the end of this guide you have the load context for a new entity: one JSON document that tells
the library what to load, from which staging notebooks, into which table and how, checked
before any run. It is for a user of the library setting up an entity. The context is the same on
every platform; only the paths in it differ.

## Before you start

- You know the entity's business key, its source system, and where its staging and target tables
  go. [Run your first load](run-your-first-load.md) shows a whole load with a context like this
  one.
- Every field below has a row on the [load-context reference](../reference/load-context.md). This
  guide covers the choices you make, not every field.

## Steps

1. **Name the entity and its tables** in `global_context`. `id` is required: letters, digits and underscores only, because it is part of the scratch table names, and unique, ignoring case, among load contexts that share a `staging_table_path`
   ([why, and what else is written beside it](../reference/load-context.md#tables-the-library-creates)).
   A path may start with
   `{DataStorageLakehouse}` or `{DataEngineeringLakehouse}`; the load replaces each with the
   `data_storage_path` or `data_eng_path` you pass to it, or with an empty string when you pass
   none, so one context serves every workspace. On Fabric the usual value is a lakehouse's
   `abfsPath`, with tables under `Tables/` and files under `Files/`, as here.
   `staging_table_schema` lists the columns your staging notebooks write; see
   [Run your first load](run-your-first-load.md) for why a staging notebook needs it.

2. **List the staging notebooks** in `staging_context.stages`, one object each, with its
   `nb_name`. They run one by one unless `runtime_context.parallel_execution_enabled` is `1`;
   `max_parallel_staging` then caps how many run at once.

3. **Choose how changes are written** in `scd_context`. `scd_type` is `"scd2"` by default, which
   keeps every version; `"scd1"` overwrites a changed row and flags a deleted one, keeping its columns. `load_type` is `"Incremental"` by
   default; `"Full"` also treats target rows missing from the source as deleted, so use it only
   when every run stages the whole entity. Both values are case-sensitive. A source that marks
   deleted rows stages `scdDeletedFlag`, or names its own column in
   `source_deleted_flag_column`; see [Run your first load](run-your-first-load.md), step 3.

4. **Identify the run** in `runtime_context`: `correlation_id`, `pipeline_name` and
   `application_name`, usually passed in by the pipeline that calls the load.

5. **Set up logging** in `logging_context`. If the data is sensitive, set
   `suppress_stdout_tail` to `true`: anything printed during the run, such as the rows
   `DebugTransformer` shows, is otherwise written to the log file under `log_file_dir`, whose
   readers may not be the table's.

   The whole context for a customer entity with two staging notebooks run in parallel:

   ```json
   {
     "global_context": {
       "id": "ent_1",
       "entity_name": "Customer",
       "source_system": "CRM",
       "target_table_path": "{DataStorageLakehouse}/Tables/customer",
       "staging_table_path": "{DataStorageLakehouse}/Tables/staging_customer",
       "staging_table_schema": {
         "type": "struct",
         "fields": [
           {"name": "customer_id", "type": "integer", "nullable": true, "metadata": {}},
           {"name": "name", "type": "string", "nullable": true, "metadata": {}}
         ]
       }
     },
     "staging_context": {
       "stages": [{"nb_name": "NB_Stage_Customer_EU"}, {"nb_name": "NB_Stage_Customer_US"}]
     },
     "scd_context": {
       "business_key_column": "customer_id",
       "scd_type": "scd2",
       "load_type": "Incremental"
     },
     "runtime_context": {
       "correlation_id": "customer-2026-09-29",
       "pipeline_name": "crm_daily",
       "application_name": "crm",
       "parallel_execution_enabled": 1
     },
     "logging_context": {
       "logging_level": "INFO",
       "log_file_name": "customer_load",
       "log_file_dir": "{DataEngineeringLakehouse}/Files/Logs",
       "suppress_stdout_tail": true
     }
   }
   ```

6. **Check it before a run.** `LoadContextManager` is what a load uses to read the context:
   `parse` reads it and refuses some problems at once, and `validate_all` then checks each section
   and raises the first problem it finds. It starts no Spark session, so you can run it anywhere
   the library and PySpark are installed. Save the JSON above as `customer_load_context.json`, then:

   ```python
   from spark_quick_start.orchestration import LoadContextManager

   with open("customer_load_context.json", encoding="utf-8") as f:
       load_context_json = f.read()

   manager = LoadContextManager(load_context_json)
   manager.parse("abfss://ws@onelake.dfs.fabric.microsoft.com/lh", "")
   manager.validate_all()
   print(manager.global_ctx.target_table_path)
   ```

## Check it worked

`parse` and `validate_all()` return without raising, and the printed path has the placeholder
replaced: `abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Tables/customer`.

## When it goes wrong

Each check stops at the first problem; fix it and run step 6 again. The
[load-context reference](../reference/load-context.md) says which checks `parse` makes and which
`validate_all` makes.

**`SCDContext requires 'business_key_column'`, from `parse`.** `scd_context` has no business key
(step 3).

**`scd_context.scd_type is 'SCD2'; must be one of ['dv_hub', 'dv_link', 'dv_satellite', 'scd1', 'scd2']. Note the comparison is case-sensitive.`, from `validate_all`.** The value is
case-sensitive; write it in lower case. `load_type` is checked the same way, as `Incremental` or
`Full`.

**`GlobalContext: 'parallel_execution_enabled' has moved to runtime_context`, from `parse`.** Move
the field into `runtime_context` (step 4).

**Tables are read or written at `/Tables/customer`, at the root, not under your lakehouse.** The
load was not given `data_storage_path`, so the placeholder was replaced by an empty string. Pass it
to `default_pipeline` or `LoadOrchestrator`, with the lakehouse or storage root.

**The log is written to `/Files/Logs`, at the root.** The load was not given `data_eng_path`, as
step 6 passes `""`, so `{DataEngineeringLakehouse}` in `log_file_dir` was replaced by an empty
string. Pass `data_eng_path` to `default_pipeline` or `LoadOrchestrator`, with the data
engineering lakehouse's root.

## See also

- [Load-context reference](../reference/load-context.md): every field, with its default.
- [Run your first load](run-your-first-load.md): run a load with this context.
