# Load a Data Vault entity

At the end of this guide one entity is in a Data Vault: a hub holding each customer's key once,
and a satellite holding a new row for every change, delete and reappearance of its attributes,
both loaded from one source table by a Declarative Query Definition (DQD) and never updated. It
is for a user of the library who models a layer as canonical Data Vault 2.0 rather than SCD. The
steps are the same on every platform; only the query a consumer runs on the satellite differs.

## Before you start

- You can load a table with a DQD; see [Load a table with a DQD](load-a-table-with-a-dqd.md).
  This guide uses its `spark`, and `platform="fabric"`.
- You know which source columns make the business key, here `CUST_ID`, and which are the
  descriptive attributes the satellite tracks, here `NAME` and `CITY`.

## Steps

1. **Build the hash key in the DQD, the same way in every load context.** The hash key is yours
   to build upstream; the library stores it and matches on it. Use one canonical expression,
   copied unchanged into the hub's and the satellite's contexts: trim, upper-case, a fixed
   `||` between parts, then `sha2` 256. A key built two ways joins nothing to its hub, and no
   check can see it.

   ```json
   {"name": "HashKey", "expression": {"function": "sha2", "arguments": [
     {"function": "upper", "arguments": [{"function": "trim", "arguments": [
       {"function": "coalesce", "arguments": [
         {"cast": {"expression": {"column": "CUST_ID"}, "data_type": "string"}}, {"literal": ""}]}]}]},
     {"literal": 256}]}}
   ```

   For a key of several columns, join them with `concat_ws` and the literal `||` inside `upper`.

2. **Write the hub's load context.** `scd_type` is `dv_hub` and `business_key_column` is the
   hash key. Select the hash key and the business key as staged, so a reader can check one
   against the other.

   ```json
   {
     "global_context": {
       "id": "customer_h",
       "entity_name": "Customer",
       "source_system": "CRM",
       "target_table_path": "/lake/vault/customer_h",
       "staging_table_path": "/lake/staging/customer"
     },
     "scd_context": {"scd_type": "dv_hub", "business_key_column": "HashKey"},
     "dqd_context": {
       "source_kind": "delta",
       "source_identifier": "/lake/bronze/customers",
       "transformations": [
         {"name": "HashKey", "expression": {"function": "sha2", "arguments": [
           {"function": "upper", "arguments": [{"function": "trim", "arguments": [
             {"function": "coalesce", "arguments": [
               {"cast": {"expression": {"column": "CUST_ID"}, "data_type": "string"}}, {"literal": ""}]}]}]},
           {"literal": 256}]}}
       ],
       "selection": [{"name": "HashKey"}, {"name": "CustomerNo", "expression": {"column": "CUST_ID"}}]
     },
     "runtime_context": {"correlation_id": "vault-1", "pipeline_name": "customer_h", "application_name": "crm"},
     "logging_context": {"logging_level": "INFO", "log_file_name": "customer_h", "log_file_dir": "/lake/logs"}
   }
   ```

3. **Write the satellite's load context.** `scd_type` is `dv_satellite`; the same key
   transformation; the descriptive columns selected; and an `id` of its own, unique among the
   contexts that share its `staging_table_path`, as the hub's does. If the source flags deleted rows, name the
   flag in `source_deleted_flag_column`, select it, and when it says deleted in its own word, as `Op` does with `D`, give that word as `source_deleted_flag_value`; the satellite still writes its own `deleted_flag_value`, `Y` by default. `exclude_from_hash` leaves a column out of
   the hash diff, so a change to it alone writes no row. `load_type` `Full` also records every
   key absent from the source as deleted; keep `Incremental` when the source sends only what
   changed.

   ```json
   {
     "global_context": {
       "id": "customer_crm_s",
       "entity_name": "Customer",
       "source_system": "CRM",
       "target_table_path": "/lake/vault/customer_crm_s",
       "staging_table_path": "/lake/staging/customer"
     },
     "scd_context": {
       "scd_type": "dv_satellite",
       "business_key_column": "HashKey",
       "load_type": "Incremental",
       "source_deleted_flag_column": "Op",
       "source_deleted_flag_value": "D"
     },
     "dqd_context": {
       "source_kind": "delta",
       "source_identifier": "/lake/bronze/customers",
       "transformations": [
         {"name": "HashKey", "expression": {"function": "sha2", "arguments": [
           {"function": "upper", "arguments": [{"function": "trim", "arguments": [
             {"function": "coalesce", "arguments": [
               {"cast": {"expression": {"column": "CUST_ID"}, "data_type": "string"}}, {"literal": ""}]}]}]},
           {"literal": 256}]}}
       ],
       "selection": [
         {"name": "HashKey"},
         {"name": "Name", "expression": {"column": "NAME"}},
         {"name": "City", "expression": {"column": "CITY"}},
         {"name": "Op", "expression": {"column": "OP"}}
       ]
     },
     "runtime_context": {"correlation_id": "vault-1", "pipeline_name": "customer_crm_s", "application_name": "crm"},
     "logging_context": {"logging_level": "INFO", "log_file_name": "customer_crm_s", "log_file_dir": "/lake/logs"}
   }
   ```

   The hash diff is `sha256` by default; set `hash_algorithm` to `md5` for a shorter one before
   the first load. The first write seals the setting on the table, and a later load with
   another is refused.

4. **Run the hub, then the satellite.** Put step 2's JSON in `hub_context` and step 3's in
   `satellite_context`. Each factory composes its style's steps, and refuses a load context of
   the other style before anything runs.

   ```python
   from spark_quick_start.orchestration import dv_hub_pipeline, dv_satellite_pipeline

   hub = dv_hub_pipeline(hub_context, spark, platform="fabric").run()
   satellite = dv_satellite_pipeline(satellite_context, spark, platform="fabric").run()
   ```

   Run them in this order on every load, so the satellite never holds a key its hub lacks. A
   second source of the same entity loads the same hub with its own `source_system`, and a
   satellite of its own (`customer_erp_s`).

5. **Read the current state from the satellite.** A satellite is insert-only, so a key's current
   row is the one with the latest `LoadTS`, and a key whose current row has `IsDeleted` `Y` is
   deleted.

   - **Databricks:**

     ```sql
     SELECT * FROM vault.customer_crm_s
     QUALIFY ROW_NUMBER() OVER (PARTITION BY HashKey ORDER BY LoadTS DESC) = 1
     ```

   - **Fabric** (the SQL endpoint has no `QUALIFY`):

     ```sql
     SELECT * FROM (
       SELECT *, ROW_NUMBER() OVER (PARTITION BY HashKey ORDER BY LoadTS DESC) AS rn
       FROM vault.customer_crm_s
     ) AS s WHERE rn = 1
     ```

   - **Locally:** the Fabric query, through `spark.sql`: open-source Spark 3.5 has no `QUALIFY`.

6. **Cluster the tables on the hash key, if they will be large.** The library creates a Data
   Vault table without clustering, so the satellite's incremental semi-join and the writers'
   `MERGE` on `HashKey` have no layout to skip files by. This is guidance, not something the
   library does or has measured here: whether file skipping follows depends on your runtime and
   table size, so compare a load's duration before and after. Run it once, after the first load,
   against the table the load created. Where the runtime supports liquid clustering:

   ```sql
   ALTER TABLE vault.customer_crm_s CLUSTER BY (HashKey)
   ```

   Then run `OPTIMIZE vault.customer_crm_s` on a schedule of your own, as for Z-ORDER: per the
   platform documentation the `ALTER` only sets the clustering keys, and an insert-only `MERGE`
   may not cluster the rows it writes. Per the same documentation, enabling clustering turns on
   table features (`clustering`, `domainMetadata`), which every writer of the table must then
   support. This is guidance from the platform documentation, not something measured here.

   On a classic runtime without it, compact and co-locate on a schedule of your own:

   ```sql
   OPTIMIZE vault.customer_crm_s ZORDER BY (HashKey)
   ```

   Both statements name a table in the metastore; one that is only a path needs the runtime's
   path form, ``delta.`/lake/vault/customer_crm_s` ``, and not every runtime accepts it for
   `CLUSTER BY`. Do the same for the hub, and for a link on its link key.

## Check it worked

Both results have `status` `"success"`. The hub's `inserted_count` is the number of keys it had
not seen; the satellite's is the number of rows it took, new, changed or deleted, and `0` on a
run where nothing changed. `updated_count` is `0` for both, always.

```python
print(hub.status, hub.metrics["inserted_count"], satellite.status, satellite.metrics["inserted_count"])
```

Measured locally with two customers, four runs: the first inserted 2 satellite rows, an
unchanged rerun 0, a run changing one city and deleting the other customer 2, and the deleted
customer's return 1. The returning row carries the attributes it had, and the delete row between
them carries them too, with `IsDeleted` `Y`.

## When it goes wrong

A load's error arrives as `result.status == "error"`, with the message in
`error_details["error_message"]`; nothing has been written.

**`… key(s) staged more than once, for example:`.** Two source rows produce one hash key, often
two spellings of one business key that the trim and upper-case make equal. Deduplicate in the
source or the DQD's filter.

**`… is sealed with hash settings …, and this load would compute its hash diff with …`.** The
load context's `hash_algorithm` or `hash_separator`, or the session's time zone, differs from
the one the table was first written with. Align the load context with the seal.

**`… has rows but no hash seal, so the settings its hash diff was computed with are unknown`.**
The table was written outside this library, or a load stopped between its first write and the
seal. Find the hash settings its rows were written with, seal them deliberately with the adapter's
`set_table_properties`, then rerun.

**`Refusing to create a Delta table at '…': the path already exists and holds files, but is not a
Delta table`.** The table did not exist, so the writer creates it with `write_delta_create`, and
the path in the load context holds other files: usually a mistyped or copied path. Nothing was
written; correct the path.

A satellite is created only when its target step found no table. If one appears before the write,
such as from a concurrent load, the create fails with Spark's own table-exists error rather than
recording every key again; rerun. A hub or link checks for the table again at the write, so it does
not fail this way.

**`… has columns […] and this load writes […]`.** The selection changed since the table was
created. A Data Vault table is never evolved implicitly; change the table, or start a new
satellite for the new attributes.

**`… are typed differently in the staged rows and in the satellite`.** A selected column changed
type. Cast it in the DQD to the satellite's type.

**`Full load aborted, the source has 0 rows and the satellite has live keys`.** A `Full` load
read nothing, and would have recorded every key deleted. Fix the source and rerun.

**`scd_context.scd_type is 'scd2'; this step is for 'dv_satellite' loads only`.** The load
context was written for SCD, or given to the wrong factory.

## See also

- [Orchestrator reference](../reference/orchestrator.md#dv_satellite_pipeline): what each factory
  composes.
- [Transformers reference](../reference/transformers.md): the hub and satellite steps.
- [Load-context reference](../reference/load-context.md): the Data Vault keys of `scd_context`.
- [The Data Vault design](../architecture/decisions/2026-10-01-data-vault-design.md): why insert
  only, the semantics of each classification, and the checks still owed on a live platform.
- [Load a Data Vault link](load-a-data-vault-link.md): the next guide, connecting two hubs.
