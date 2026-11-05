# Load a table with a DQD

At the end of this guide a table reaches its SCD target with no staging notebook: a Declarative
Query Definition (DQD) in the load context reads it, filters its rows, derives and renames its
columns, and hands them to the load. It is for a user of the library bringing one source to a
target with light, per-row changes; joins and aggregation stay with a staging notebook.
The steps are the same on every platform.

## Before you start

- You can run a load; see [Run your first load](run-your-first-load.md). This guide uses its
  `spark`, and `platform="fabric"`.
- The source is one Delta table, a folder of parquet files, a catalog table or view, or, on
  Fabric, a Warehouse or SQL-endpoint view (`source_kind` `warehouse_view`).

## Steps

1. **Describe the source and its rows in `dqd_context`.** `source_kind` is `delta`, `parquet`,
   `view` or, on Fabric, `warehouse_view` (a view's three-part name, with optional
   `workspace_id` and `lakehouse_id`), and `source_identifier` is the table name or path. `filters` keeps the rows you
   want; `transformations` adds columns, each able to use the ones before it; `selection` is
   the output, in order, where an entry with only `name` selects a column already there. The
   output needs the business key, `scd_context.business_key_column`; the SCD columns are the
   load's. The expression language is on the [DQD definition](../reference/dqd-definition.md)
   page.

   ```json
   {
     "global_context": {
       "id": "ent_1",
       "entity_name": "Customer",
       "source_system": "CRM",
       "target_table_path": "/lake/silver/customer",
       "staging_table_path": "/lake/staging/customer"
     },
     "dqd_context": {
       "source_kind": "delta",
       "source_identifier": "/lake/bronze/customers",
       "filters": [{"operator": "=", "left": {"column": "STATUS"}, "right": {"literal": "active"}}],
       "transformations": [
         {"name": "full_name", "expression": {"function": "concat_ws", "arguments": [
           {"literal": " "}, {"column": "FIRST"}, {"column": "LAST"}]}}
       ],
       "selection": [
         {"name": "customer_id", "expression": {"column": "CUST_ID"}},
         {"name": "full_name"},
         {"name": "birth_date", "expression": {"cast": {"expression": {"column": "DOB"}, "data_type": "date"}}}
       ]
     },
     "scd_context": {"business_key_column": "customer_id"},
     "runtime_context": {"correlation_id": "dqd-load-1", "pipeline_name": "customer", "application_name": "crm"},
     "logging_context": {"logging_level": "INFO", "log_file_name": "customer_load", "log_file_dir": "/lake/logs"}
   }
   ```

   `staging_table_path` is required by the context. A DQD load has no staging notebook
   tables, so it is where the scratch tables go: the DQD one under `eager_materialize_source`
   (step 4), and those of any Materialize step; see
   [Tables the library creates](../reference/load-context.md#tables-the-library-creates).

2. **Read only what changed, if the source has a column that grows with every change.** Add
   `hwm_context` with that column. Leave `hwm_value` out on the first run, which then
   reads everything; pass each run's `hwm_value_after` as the next run's `hwm_value`, and it reads
   only the rows past it. Whenever `hwm_value` is set, the source is filtered by it; the example is
   such a later run's. Keep
   `load_type` `Incremental`: a `Full` load would delete every key the filtered read left out,
   and the load refuses the combination.

   ```json
   {"hwm_context": {"hwm_column_name": "MODIFIED_AT", "hwm_value": "2026-09-02 10:00:00"}}
   ```

3. **Build the pipeline with `DQDTransformer` as its source.** `default_pipeline` stages
   notebooks, so build it by hand, as [Build a pipeline](build-a-pipeline.md) shows. Put the
   step's JSON from step 1 in `load_context_json`.

   ```python
   from spark_quick_start.orchestration import (
       CandidateGuardTransformer,
       SCDChangeDetectorTransformer,
       DQDTransformer,
       LoadOrchestrator,
       NativeSCDMergeTransformer,
       SCDTargetTableTransformer,
   )

   result = (
       LoadOrchestrator(load_context_json, spark, platform="fabric")
       .add(DQDTransformer())
       .add(SCDTargetTableTransformer())
       .add(SCDChangeDetectorTransformer())
       .add(CandidateGuardTransformer())
       .add(NativeSCDMergeTransformer())
       .run()
   )
   ```

4. **Keep later steps off an expensive source.** Without `hwm_context` the DQD step runs no
   Spark action over the source's rows; with it, the step runs one aggregation (the latest
   watermark, the row count, and a check that `hwm_value` fits the column's type), so the source
   is read once more. The pipeline then reads it again for each later action, and on a later load
   the merge reads it several times. Measured locally, in reads of the source, with Delta's
   `materializeSource` set to `none` (a counting UDF makes Delta's default materialize the
   merge's source, one to two reads fewer; whether it does for a Delta source like this guide's
   is not proven here):

   | Pipeline after `DQDTransformer`                      | first load | later load |
   | ---------------------------------------------------- | ---------- | ---------- |
   | target step, detector, merge                         | 1          | 4          |
   | the same with `hwm_context`                          | 3          | 5          |
   | with `CandidateGuardTransformer` (step 3)            | 2          | 5          |
   | with the guard and `hwm_context`                     | 4          | 6          |
   | with the guard, `eager_materialize_source` true      | 1          | 1          |
   | the same with `hwm_context`                          | 1          | 1          |

   The merge step's reads of the source depend on Delta's
   `spark.databricks.delta.merge.materializeSource` and on the runtime's default for it, which
   this library has not checked on Fabric or Databricks: the figures are for `none`, and a runtime that
   materializes the merge's source reads it fewer times. The library neither sets nor reads that
   setting. One of a first load's reads with `hwm_context` has not been traced to an action.
   `eager_materialize_source`, below, is the way to read the source once whatever the setting is.

   A later load here is SCD2 with every key changed. To read an expensive source (a view over a
   warehouse, a costly join) only once, set `eager_materialize_source` in the `dqd_context` of
   step 1. No pipeline change is needed.

   ```json
   {"dqd_context": {"source_kind": "view", "source_identifier": "bronze.crm.customers_view",
                    "eager_materialize_source": true}}
   ```

   The step then writes the filtered, watermarked rows once to a scratch table,
   `{staging_table_path}_{id}_0_dqd`, dropped and rewritten on every run, and the aggregation and
   every later step read that. The source is read once for the write, with or without
   `hwm_context`. On a cheap source, such as a plain Delta table, the write costs more than it
   saves, so leave it off. With the flag on, keep `global_context.id` unique among load contexts
   that share a `staging_table_path`, and do not run one context twice at once; the table is kept
   after the run, and [Tables the library creates](../reference/load-context.md#tables-the-library-creates) says what that means for permissions,
   data and cleanup. The raw source
   columns are written under positional names, so a column name Delta refuses, such as
   `Order Date`, is fine; a `transformations` or `selection` entry that reads `_metadata` is
   refused, since after the round trip it would describe the scratch table's files. A `hwm_value` that does not fit the column's type still fails before the target
   is written, but after the scratch table is. See the flag in
   [`DQDContext`](../reference/load-context.md#dqdcontext). Why the aggregation is neither cached
   nor taken from the write is in `DQDTransformer`'s docstring.

## Check it worked

`result.status` is `"success"` and `result.metrics["inserted_count"]` is the number of rows the
definition kept. Measured locally with a three-row source, one of them not `active`: the first
load inserted 2 rows with `customer_id`, `full_name` and `birth_date`; with `hwm_context`, a
second load given the first's `hwm_value_after` read only the rows changed since.

```python
print(result.status, result.metrics["inserted_count"], result.metrics["hwm_value_after"])
```

## When it goes wrong

A definition's error arrives as `result.status == "error"`, and `error_details["error_message"]`
names the entry, such as `dqd_context.selection[0].expression`.

**`unknown key 'col', which is DQF's; write 'column'`.** The definition uses the keys of DQF, the engine the DQD replaced; the
[DQD definition](../reference/dqd-definition.md) page maps each one.

**`'NAME' is not a column of the source; its columns are [...]`.** The column is misspelt, was
left out of `source_columns`, or is used in a filter before the transformation that adds it.

**`function 'sum' is not allowed in a DQD definition: a DQD maps one row in to one row out`.**
Aggregation, ranking, sorting and exploding rows are not per-row; use a staging notebook.

**`NotImplementedError` and `a warehouse view is a Fabric source`.** A `warehouse_view` was read
on Databricks or locally; read a catalog view with `view`.

**`staging_context.stages must contain at least one staging phase`.** There is no `dqd_context`
in the load context, or it was misspelt.

**`hwm_value '...' cannot be read as the type of hwm_column_name`.** The stored watermark is not
a value of the column's type, such as a malformed timestamp.

**`Refusing to drop '...': it exists but is not a Delta table`.** With `eager_materialize_source`
on, `{staging_table_path}_{id}_0_dqd` already holds something that is not a Delta table, usually
a mistyped or copied `staging_table_path`. Nothing was deleted; point `staging_table_path` at an
empty location, or use another `id`.

**``dqd_context.transformations[i]: reads the source's `_metadata` column, which dqd_context.eager_materialize_source cannot keep: ...``.**
A transformation or selection reads `_metadata`, and the message names which; move that read into `filters`, or turn the flag off.

**A warning that `dqd_context is set alongside staging_context.stages`.** The load context has
both; the pipeline's source step decides which is read. To choose at run time, use
`.set_source(lambda ctx: ctx.dqd_ctx is not None, DQDTransformer(), StagingTransformer())`.

## See also

- [DQD definition](../reference/dqd-definition.md): every expression, predicate and refused
  function.
- [Load-context reference](../reference/load-context.md#dqdcontext): `dqd_context` and
  `hwm_context`.
- [Handle early exit and errors](handle-early-exit-and-errors.md): reading the result.
