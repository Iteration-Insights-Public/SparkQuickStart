# Write a custom source

At the end of this guide a source the library does not ship, such as an application's API, lands
in a raw Delta table that holds the latest state of each record: a source step of your own reads
it incrementally, and `KeyMergeTransformer` keeps one row per key, stamped with the load time.
Bronze then reads that table like any other source. It is for someone extending the library with
a connector. The steps are the same on every platform.

## Before you start

- You can run a load; see [Run your first load](run-your-first-load.md). This guide uses its
  `spark`, and `platform="fabric"`.
- You have code that reads the source's records changed after a given time, and the secret it
  needs is in Key Vault on Fabric or a secret scope on Databricks.
- You know the source's key column and the column that says when a record last changed.

## Steps

1. **Describe the load.** The source's own settings go in `source_context`: `kind` names your
   source, and your own keys go under `properties`, which the library never reads. Keep them
   there rather than beside `kind`: the top level is the library's, and a later release may add a
   key of its own under the same name. `scd_context.scd_type`
   `landing` makes the target a landing table keyed on `business_key_column`.
   `hwm_context.hwm_column_name` is the column that orders a key's rows, and the watermark
   handed from run to run. Name one table per source system and entity in `target_table_path`.
   A landing load still needs `staging_table_path`, which it does not read.

   ```json
   {
     "global_context": {
       "id": "bc_contoso_customer",
       "entity_name": "Customer",
       "source_system": "BC_Contoso",
       "target_table_path": "/lake/raw/bc_contoso_customer",
       "staging_table_path": "/lake/staging/unused"
     },
     "source_context": {
       "kind": "business_central",
       "properties": {
         "vault": "https://contoso-kv.vault.azure.net/",
         "secret": "bc-client-secret",
         "entity": "customers"
       }
     },
     "scd_context": {"scd_type": "landing", "business_key_column": "systemId"},
     "hwm_context": {"hwm_column_name": "lastModifiedDateTime", "hwm_value": "2026-11-01T00:00:00Z"},
     "runtime_context": {"correlation_id": "run-1", "pipeline_name": "land_customer", "application_name": "raw"},
     "logging_context": {"logging_level": "INFO", "log_file_name": "land_customer", "log_file_dir": "/lake/logs"}
   }
   ```

   In the next steps this JSON is the string `load_context_json`.

2. **Write the source step.** Subclass `SourceTransformer`, set `source_kind` to your `kind`,
   and implement `_produce`. Read your settings from `ctx.source_ctx["properties"]`, the secret through the
   adapter's `get_secret`, so the step imports no platform SDK, and the watermark from
   `ctx.hwm_ctx.hwm_value`. Return the records as `state.df`, with the ordering column cast to a
   timestamp, and report the latest value you read as the next watermark.

   ```python
   from pyspark.sql import functions as F

   from spark_quick_start.orchestration import KeyMergeTransformer, LoadOrchestrator, PipelineState
   from spark_quick_start.orchestration.transformers import SourceTransformer


   class CustomerSource(SourceTransformer):
       source_kind = "business_central"

       def __init__(self) -> None:
           self.hwm_before = None
           self.hwm_after = None
           self.hwm_column = None

       def _produce(self, ctx):
           settings = ctx.source_ctx["properties"]
           secret = self._adapter.get_secret(settings["vault"], settings["secret"])
           since = ctx.hwm_ctx.hwm_value
           records = read_changed_records(settings["entity"], secret, since)  # your code
           df = self._spark.createDataFrame(records).withColumn(
               "lastModifiedDateTime", F.to_timestamp("lastModifiedDateTime"))
           latest = df.agg(F.date_format(F.max("lastModifiedDateTime"),
                                         "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")).first()[0]
           self.hwm_column = ctx.hwm_ctx.hwm_column_name
           self.hwm_before = since
           self.hwm_after = latest or since
           return PipelineState(df=df)
   ```

   `read_changed_records` stands for your code; it returns a list of dicts. The `max` is one
   Spark action over the batch; drop it if your API returns the latest value itself. Format it in
   Spark with its offset, as here: a timestamp collected to Python is in the driver's local time,
   so on a driver that is not UTC its bare `isoformat()` would hand the next run the wrong instant
   and skip records.

3. **Run the load.** The source step comes first, then the writer.

   ```python
   result = (
       LoadOrchestrator(load_context_json, spark, platform="fabric")
       .add(CustomerSource())
       .add(KeyMergeTransformer())
       .run()
   )
   ```

   - **Fabric:** `get_secret` reads the Key Vault at `properties.vault`; the workspace identity needs
     access to it.
   - **Databricks:** `get_secret` reads the secret scope named in `properties.vault`; put the scope's name
     there instead of a URL.
   - **Locally:** construct `LocalAdapter(secrets={(vault, secret): value})` and pass it as
     `adapter=`; it reads no environment variable.

## Check it worked

```python
print(result.status, result.metrics["inserted_count"], result.metrics["updated_count"],
      result.metrics["hwm_value_after"])
```

The first run inserts every record and creates the table; a second run with no change in the
source reports `0` and `0`. Hand `hwm_value_after` to the next run as `hwm_context.hwm_value`.
The table has the source's columns and `LoadTS`, the load time. Bronze reads it by `LoadTS`:
give its load context a `dqd_context` with `source_kind` `delta`, this table as
`source_identifier`, and `hwm_context.hwm_column_name` `LoadTS`.

## When it goes wrong

**`CustomerSource reads source_context of kind 'business_central', and the load context's
source_context.kind is ...`.** The step and the load context name different sources; correct
`kind`, or compose the step for it.

**`the load context names no source`.** The load context has no `source_context`, stages or
`dqd_context`; add the `source_context`.

**`KeyMergeTransformer: scd_context.scd_type is 'scd2'; this step is for 'landing' loads
only`.** Set `scd_context.scd_type` to `landing`.

**`the landing batch's ordering column 'lastModifiedDateTime' is string`.** The API returned
text; cast it with `F.to_timestamp` in the source step, as in step 2.

**`the landing batch changes the type of 1 column(s)`.** The source now sends a column with
another type than the table has. Cast it back in the source step, or change the table. A new
column, by contrast, is added to the table, and earlier rows read NULL in it.

**`NotImplementedError: ... does not implement get_secret`.** The adapter is your own and does
not override `get_secret`; override it, or use a shipped adapter.

## See also

- [Load context](../reference/load-context.md): `source_context`, `scd_type` `landing` and
  `load_ts_column`
- [Transformers](../reference/transformers.md): `KeyMergeTransformer` and `SourceTransformer`
- [Platform adapters](../reference/platform-adapters.md): `get_secret`
- [Load a table with a DQD](load-a-table-with-a-dqd.md), for the Bronze load that reads this table
- [Run incremental loads](run-incremental-loads.md), for handing the watermark from run to run
