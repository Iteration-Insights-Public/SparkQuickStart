# Load context

The load context is the one JSON string that drives a run. The pipeline that calls the load
notebook writes it, and `LoadOrchestrator` reads it through `LoadContextManager`, which hands each
section to the class that reads it. The source of truth is
`src/spark_quick_start/orchestration/context_manager.py` and the context classes in
`src/spark_quick_start/orchestration/models.py`. Every section and field the library reads has a
row below, and no row names one it does not: `test_the_load_context_reference_matches_the_code`
checks both directions.

Every refusal happens before any staging notebook runs. `LoadContextManager.parse()` raises
`ValueError` on a missing or blank `business_key_column`, on `parallel_execution_enabled` under
`global_context`, on any other key `global_context` does not read, on a `platform_context`, which
the library sets, on an `extensions_context` that is not an object, and on a `dqf_context`, whose
DQF engine was removed: the message names `dqd_context` as the way on. `notebook_timeout_seconds` is read there with `int()`: `90.7` becomes `90`, `"90"` is accepted, and a value `int()` refuses, such as `"abc"` or `null`, raises its `ValueError` or `TypeError`. `validate_all()` then checks the sections in turn and
raises `ValueError` at the first that fails, naming its missing fields or its bad value.

## `LoadContextManager`

The top-level sections.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `global_context` | object | required | What entity to load and where. Read by `GlobalContext`. |
| `staging_context` | object | `{}` | The staging notebooks to run. Read by `StagingContext`. Required unless `dqd_context` is set. |
| `scd_context` | object | required | SCD behaviour and column names. Read by `SCDContext`. |
| `runtime_context` | object | required | Run identity and execution switches. Read by `RuntimeContext`. |
| `logging_context` | object | required | Where and how the run logs. Read by `LoggingContext`. |
| `dqd_context` | object | none | A Declarative Query Definition, read by `DQDTransformer` as the source. Read by `DQDContext`. A load with it needs no staging notebooks. Raises `ValueError` when present and not an object. |
| `hwm_context` | object | none | The load's watermark. Read by `HWMContext`. Handed to each staging notebook and reported by `StagingTransformer`, or applied as a filter and reported by `DQDTransformer`. Raises `ValueError` when present and not an object, and together with a `dqd_context` when `scd_context.load_type` is `Full`. |
| `extensions_context` | object | `{}` | Your own data: policies, rules and parameters for custom transformers and pipeline control. The library never interprets it. Read by `ExtensionsContext`; handed whole to each staging notebook. Raises `ValueError` when present and not an object. |

The placeholders `{DataStorageLakehouse}` and `{DataEngineeringLakehouse}` anywhere in the JSON
are replaced with the two lakehouse paths the caller passes before the JSON is parsed.

A load context that contains a `platform_context` section is refused with `ValueError`: the
library sets it, from those two paths, and exposes it as `PlatformContext`. Any other top-level
key is ignored, as before.

```json
{
  "global_context": {"id": "ent_1", "entity_name": "Customer", "source_system": "CRM",
                     "target_table_path": "silver.customer", "staging_table_path": "bronze.customer",
                     "staging_table_schema": {"type": "struct", "fields": [
                       {"name": "customer_id", "type": "integer", "nullable": true, "metadata": {}}]}},
  "staging_context": {"stages": [{"nb_name": "NB_Stage_Customer"}]},
  "scd_context": {"business_key_column": "customer_id"},
  "runtime_context": {"correlation_id": "abc-123", "pipeline_name": "PL_Load_Customer",
                      "application_name": "EDW"},
  "logging_context": {"logging_level": "INFO", "log_file_name": "Customer_load",
                      "log_file_dir": "{DataEngineeringLakehouse}/Files/Logs"}
}
```

## `GlobalContext`

What entity to load and where.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `id` | `str` | required | Identifies this load; letters, digits and underscores only (`^[A-Za-z0-9_]+$`). It is part of every scratch table's name, lower-cased, so it must be unique, ignoring case, among load contexts that share a `staging_table_path`; see [Tables the library creates](#tables-the-library-creates). Raises `ValueError` naming `id` when missing, empty or of another shape. |
| `entity_name` | `str` | required | The entity being loaded, such as `Customer`. |
| `source_system` | `str` | required | The source system's name, written to the source system column unless `scd_context.source_system_from_source` is true. |
| `target_table_path` | `str` | required | The target Delta table, as a Unity Catalog name or a path. |
| `staging_table_path` | `str` | required | The staging Delta table the load reads. Each staging notebook is given it with `__<nb_name>` appended; see `staging_table_schema` for how those tables reach this one, and [Tables the library creates](#tables-the-library-creates) for every table written beside it. |
| `staging_table_schema` | object | required with `staging_context.stages` | The staging table's schema as Spark StructType JSON. `StagingTransformer` checks every staging notebook's table against it, raising `TypeError` on a mismatch, and merges them into `staging_table_path`. A context with `staging_context.stages` and no schema, or an empty one, raises `ValueError` naming both before any notebook runs. Also required by `SCDGracefulChangeDetectorTransformer`. A DQD source, which has no stages, does not read it. |
| `notebook_timeout_seconds` | `int` | `90` | Timeout for each staging notebook, in seconds. |
| `load_timestamp` | `str` | the current UTC time, ISO 8601 with microseconds | The run's load time T. The SCD writers stamp it as every written row's effective-from, and an `scd2` close-out ends at T less one microsecond; the Data Vault writers stamp it as `LoadTS`. Read as one instant in UTC: it must carry an offset (`Z` or `+00:00`; other offsets are converted), and may carry any number of fractional digits, cut to microseconds, so the seven ADF and Fabric `utcNow()` write are read. A value with no offset, a calendar date with no time (`2026-10-03` or `20261003`), or anything not ISO 8601 fails validation, as does a `scd_context.default_max_timestamp` at or before it. **The library does not check that load times increase.** A T equal to or earlier than the newest effective-from in the SCD target, or the newest `LoadTS` in the satellite, is the user's responsibility. Under `scd2` the closed row's effective-to precedes its effective-from and the new version's interval starts before the old one's, so the intervals invert; under `scd1` an earlier T rewinds the updated or soft-deleted row's effective-from and can lower the reported watermark; an append-only load stamps its rows behind existing ones; on a satellite an equal `LoadTS` makes a real change match an existing (key, `LoadTS`) row and be dropped silently, and an earlier one makes the "latest row" of a key the wrong one. For a rerun or a backfill, pass a later `load_timestamp`. A hub and a link take any T, as a key is inserted once. `LoadEndTS` stays the clock at stamping, so with a backfilled T it minus `LoadTS` is not a duration. |

Any other key raises `ValueError`, naming the key, the keys `global_context` accepts and
`extensions_context`, where your own data goes: a misspelt key is an error, not a value the load
never reads. Only this section is strict. `parallel_execution_enabled` here raises `ValueError`
with its own message: it belongs in `runtime_context`. The two lakehouse paths the library
resolves are not here; they are in [`PlatformContext`](#platformcontext).

`GlobalContext.load_timestamp_utc` is `load_timestamp` as a timezone-aware UTC `datetime`; it raises `ValueError` for a value `validate()` refuses.

## Tables the library creates

A load writes tables beside `global_context.staging_table_path`, in the same schema or directory, and
the library drops only some of them. This section is the one place that lists them, what happens to
them between runs, and what is left for you to clean up. It is read from `delta_utils.py`,
`load_pipeline.py`, `staging_pipeline.py`, `transformers/materialize.py` and `drop_delta` in
`adapters/platform_adapter.py`. Nothing here was run against a live Fabric or Databricks workspace;
statements about what a platform does are marked *unverified here*.

### Names

Every name is the staging identifier with a suffix appended as a plain string, so for a Unity
Catalog name the suffix lands on the table part: `cat.sch.tbl` gives `cat.sch.tbl__NB_Stage_Customer`
and `cat.sch.tbl_cust_a_3_materialize`. For a filesystem path it lands on the last path segment.
`<id>` is `global_context.id`, lower-cased. `<n>` is the step's position in the pipeline; a source step
is at position 0.

| Table | Name | Written by |
|---|---|---|
| Staging notebook table | `<staging_table_path>__<nb_name>`, one per `staging_context.stages` entry | each staging notebook, through `create_staging_main_function` |
| Merged staging table | `<staging_table_path>` itself | `StagingTransformer`, which merges the notebook tables into it (schema-checked, so `staging_table_schema` is required) |
| Materialize scratch table | `<staging_table_path>_<id>_<n>_materialize` | `MaterializeTransformer` |
| Frame scratch table | `<staging_table_path>_<id>_<n>_materialize_<frame_key>` | `MaterializeFrameTransformer` |
| DQD scratch table | `<staging_table_path>_<id>_0_dqd` | `DQDTransformer`, only with `dqd_context.eager_materialize_source` true |

With `runtime_context.materialize_strategy` set to `"cache"` and `serverless` false, the two
Materialize steps write no scratch table and drop none; the table above is written only in the
default `"delta"` mode, or in `"cache"` mode on serverless, which falls back to it. A scratch table
a step wrote in an earlier `"delta"` run is left where it is: see [Tables left behind](#tables-left-behind).

The scratch names are built in one place, `scratch_table_name`, and cannot be chosen. The position
and the step's token keep the steps of one pipeline apart, and `<id>` keeps the loads that share a
`staging_table_path` apart. A load with only a `dqd_context` source has no staging notebook tables
and no merged staging table; `staging_table_path` is still required, and is where its scratch tables
go: the DQD one under `eager_materialize_source`, and those of any Materialize step.

### Lifecycle

- **Scratch tables** are dropped and rewritten on every run: `drop_delta`, then a Delta overwrite,
  then a read-back. They are not removed at the end of a run. Each holds a full copy of its frame
  from the step's last run until the step's next run, and nothing in the library drops it after
  that. Drop rather than overwrite is deliberate: a Delta overwrite leaves the superseded files on
  disk until `VACUUM`, and one scratch path was measured at 45 GB holding 2.3 GB of live data. A
  drop, when it runs, also discards the old schema, so a column change needs no schema evolution.
- **Staging notebook tables** are rewritten by each notebook run and are never dropped by the
  library, including after they are merged. For a path, the notebook first deletes the directory
  recursively and then overwrites it; with `spark=` passed to `create_staging_main_function` it does
  so only when the path is confirmed to be a Delta table, and without `spark` it deletes without
  that check and logs it. For a Unity Catalog name there is no delete: the write overwrites the table,
  and the superseded files stay until `VACUUM`.
- **The merged staging table** is overwritten (with `overwriteSchema`) on every run that has staging
  stages, never deleted first and never dropped, so superseded files stay until `VACUUM`.
- A drop that fails is swallowed by `drop_delta`, unless it fails with `ValueError` (a name
  `_quote_uc_name` refuses), which propagates; after a swallowed failure the write overwrites whatever is there; a
  path it cannot recognise as Delta (a URI or `/mnt/...` path where `isDeltaTable` answers False) is
  not dropped either. Both leave superseded files behind until `VACUUM`. The write is then a Delta
  overwrite without `overwriteSchema`, so it succeeds only while the frame's schema is unchanged: a
  scratch frame with a new or retyped column fails the write with a Delta schema error (measured
  locally 2026-10-05: `AnalysisException` `A schema mismatch detected` for a new column,
  `DELTA_FAILED_TO_MERGE_FIELDS` for a changed type). Drop the table by hand, as in
  [Tables left behind](#tables-left-behind).

### Tables left behind

The library never drops a table it no longer writes. These leave one behind, which keeps holding
its last copy of the data:

- a step's position `<n>` changes, because a step was added or removed before it;
- the step is removed, or the flag that wrote it (`eager_materialize_source`) is turned off;
- `global_context.id` or `staging_table_path` changes;
- a staging notebook is renamed or removed from `staging_context.stages`;
- the pipeline is retired.

Removing them is your job. Find them by the names above, then drop a Unity Catalog table with
`DROP TABLE IF EXISTS cat.sch.tbl_cust_a_3_materialize`, or delete the directory of a path table
(`notebookutils.fs.rm(path, recurse=True)` on Fabric, `dbutils.fs.rm(path, recurse=True)` on
Databricks, which is what the adapters call). Retiring a load means removing its merged staging
table and its notebook tables as well.

### Permissions

The principal that runs the load needs to create, overwrite and drop tables where
`staging_table_path` points. On Unity Catalog that is, as guidance, `CREATE TABLE` on the schema and
ownership of (or `MANAGE` on) the tables so it can drop them; *unverified here*, as it is platform
behaviour. For a path it is write and delete under the parent directory. A principal that can write
but not drop has its drop error swallowed and the scratch table overwritten instead, so superseded
files accumulate and a changed scratch schema fails the write, as the last Lifecycle item says.

### Data and storage

Scratch and staging tables hold copies of your source or candidate rows, including any sensitive
columns, so give them the same access control and retention as the staging data. Each scratch table
is one full copy of its frame. After a drop, the files of a path table are deleted (`delete_path`
removes the directory recursively). For a Unity Catalog managed table the platform may keep the data
for a recovery window, such as Databricks `UNDROP`: *unverified here*, and not something the library
controls.

### Concurrency

`global_context.id` must be unique, ignoring case, among the load contexts that share a
`staging_table_path`, because the name lower-cases it. Two concurrent runs of the same load context
share its scratch tables: one run's drop of a table that the other run's frame still reads returns an
empty result, silently and without an error (F129). Do not run one context twice at once.

### Refusal to drop a non-Delta path

For a local path, the drop refuses a path that exists but is not a Delta table and deletes nothing,
so a mistyped `staging_table_path` cannot delete someone else's directory. The symptom and what
the check can see are in [Tune performance](../guides/tune-performance.md).

## `StagingContext`

The staging notebooks to run.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `stages` | list of objects | `[]` | The staging phases, run in order or in parallel (see `runtime_context.parallel_execution_enabled`). Must hold at least one stage unless `dqd_context` is set. |

## `StagingContext.stages`

One staging phase.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `nb_name` | `str` | required | The staging notebook to run. |
| `path` | `str` | none | A folder prefix: the notebook run is `<path>/<nb_name>`. Present but blank raises `ValueError`. |
| `staging_context` | object | `{}` | Handed to the staging notebook as its own `staging_context`, unread by the library. |

## `SCDContext`

SCD behaviour and the names of the columns the library writes. On the load-context path a
column name comes from the key here, or else from `ColumnNamingPolicy.default()`, whose values are
the defaults below. Under a Data Vault style (`dv_hub`, `dv_satellite`, `dv_link`) the fallback is
`ColumnNamingPolicy.data_vault()` instead, so the defaults below become `HashKey`, `HashDiff`,
`IsDeleted`, `RecordSource`, `CorrelationId` and a `hash_separator` of `||` (sealed on the
table with the algorithm by the Data Vault writers; under every style it must be non-empty
and free of quotes, backslashes, backticks, `$` and control characters). A key that belongs to
the other style is refused, never ignored: an SCD style refuses `hash_algorithm`,
`load_ts_column`, `load_end_ts_column` and `pipeline_run_id_column`; a Data Vault style refuses
`version_column`, `current_flag_column`, `effective_from_column`, `effective_to_column`,
`scd_row_hashing`, `current_flag_value`, `inactive_flag_value` and `default_max_timestamp`. A key
whose value is `null` counts as not given. The SCD steps refuse a Data Vault style: when the
pipeline is assembled, before the source runs, and again at the start of their own work for a
step inside `.add_if()`. A `dv_hub` load runs through `dv_hub_pipeline` and a `dv_satellite` load through
`dv_satellite_pipeline`, whose steps read these keys. See the
[Data Vault design](../architecture/decisions/2026-10-01-data-vault-design.md). A `ColumnNamingPolicy` of your own applies only to code that builds
`SCDContext` itself; see the [column-naming reference](column-naming.md).

| Field | Type | Default | Meaning |
|---|---|---|---|
| `business_key_column` | `str` | required | The staged column that identifies an entity across loads. An SCD target is matched on it together with the source system column; a Data Vault target on it alone, as the hash key built upstream. Missing or blank raises `ValueError`. |
| `scd_type` | `str` | `"scd2"` | `scd1` overwrites a changed row, updating every business column and the current flag, effective-from, hash, deleted flag, version and correlation id columns, and leaving the row key, effective-to and source system columns as written, and flags a deleted row (deleted flag `deleted_flag_value`, `"Y"` by default, effective-from and correlation id stamped) while keeping its business columns, hash and version. Upgrading: SCD1 once ignored deletes, so the first `Full` load after this behaviour flags every target key that went missing in the meantime; each such row gets a new effective-from, so `updated_count` and the watermark jump once. An `Incremental` load flags only the deletes in its own batch: a key whose delete arrived in an earlier batch stays live. The meta columns are told from business columns by the names below, never by a `scd` prefix; `scd2` closes it out and inserts a new version. `dv_hub`, `dv_satellite` and `dv_link` are the Data Vault styles; a `dv_link` load runs through `dv_link_pipeline`. Case-sensitive; any other value raises `ValueError`. |
| `load_type` | `str` | `"Incremental"` | `Incremental` or `Full`. A `Full` load also classifies target rows absent from the source as deleted, unless their current row is already deleted, and raises `ValueError` rather than run when the source is empty and the target is not. Case-sensitive; any other value raises `ValueError`. |
| `exclude_from_hash` | list of `str` | `[]` | Source columns left out of the change hash, so a change to them alone is not a new version. The business key, source system, correlation id, deleted flag and row key columns are always left out, and so is `source_deleted_flag_column` when set. A source that carries the version or hash column raises `ValueError`. |
| `source_deleted_flag_column` | `str` | none | A source column that flags deleted rows, where it equals `source_deleted_flag_value`; NULL or any other value means not deleted. It wins over a staged `deleted_flag_column`, which is then dropped. It is excluded from the hash and from the output. A flagged row with a matching target row is classified as deleted unless the target's current row is already deleted (a NULL target flag counts as not deleted), so a flag that stays set adds one version, not one per load (F142); on a first load, when no target row matches, and when the target row is already deleted, flagged rows are skipped. A flagged row is skipped even if its other columns changed: they return with the undelete. Raises `ValueError` when the source has no such column. |
| `source_system_from_source` | `bool` | `false` | When true, the source system column comes from the source rows, which must then carry it, rather than from `global_context.source_system`. Under a Data Vault style it is an attribute written to `RecordSource`, not part of the key: `StagedKeyGuardTransformer` refuses one key from two sources in a batch. |
| `hash_algorithm` | `str` | `"sha256"` | Data Vault styles only, sealed on the table by `DVHubWriterTransformer`, `DVLinkWriterTransformer` and `DVSatelliteWriterTransformer`, and computing the satellite's diff in `DVSatelliteChangeDetectorTransformer`: the hash diff's algorithm, `sha256` or `md5`, as lower-case hex. Any other value raises `ValueError`. |
| `load_ts_column` | `str` | `"LoadTS"` | Data Vault styles only, written by `DVHubWriterTransformer`, `DVLinkWriterTransformer` and `DVSatelliteWriterTransformer`: the column holding the run's load time, `global_context.load_timestamp`, the same for every row it writes. An empty name raises `ValueError`. |
| `load_end_ts_column` | `str` | `"LoadEndTS"` | Data Vault styles only, written by `DVHubWriterTransformer`, `DVLinkWriterTransformer` and `DVSatelliteWriterTransformer`: the column holding when the writer started writing; an audit stamp of the load, not the end of a row's life. An empty name raises `ValueError`. |
| `pipeline_run_id_column` | `str` | none | Data Vault styles only, written by `DVHubWriterTransformer`, `DVLinkWriterTransformer` and `DVSatelliteWriterTransformer`: when set, the column the writer fills with `runtime_context.pipeline_run_id`. |
| `link_hub_key_columns` | list of `str` | none | `dv_link` only, and required there: the staged columns holding the hash key of each hub the link connects. At least two, distinct ignoring case, none the business key, source system, correlation id or a load column; a NULL or empty value in any of them is refused by `StagedKeyGuardTransformer`. Set under another style, or invalid, raises `ValueError` naming it. |
| `scd_row_hashing` | `bool` | `false` | When true, the row key is `sha2(business_key, 256)`; otherwise it is the business key cast to a string. Applies only to a row key with a name of its own: with `row_key_column` equal to `business_key_column` the key passes through as staged, so it raises `ValueError` rather than have no effect. |
| `hash_separator` | `str` | `"~"`; `"\|\|"` under a Data Vault style | Joins the column values in the string that is hashed: the SCD change hash, or the satellite's hash diff, which seals it on the table with `hash_algorithm`. Non-empty, with no quote, backslash, backtick, `$` or control character, under every style. `SCDChangeDetectorTransformer(hash_separator=...)` reads it when its own argument is `None`, and raises `ValueError` when the argument differs from a value given here. Changing it on an SCD table that holds rows re-versions every key; see the note below the table. Upgrading: an SCD context that sets a non-default `hash_separator` had no effect on the change hash until now; remove the key, or accept a one-time re-version of every key on the next load. |
| `row_key_column` | `str` | `"scdRowKey"` | The target's name for the business key. When it equals `business_key_column`, the staged key passes through unchanged; a spelling that differs from it only by case raises `ValueError`: the reference pipelines build it upstream, source included, as `scdRowKey`, and name it as both. Otherwise the library writes it beside the business key, derived as `scd_row_hashing` says. |
| `hash_column` | `str` | `"scdHash"` | Name of the change hash column. |
| `version_column` | `str` | `"scdVersion"` | Name of the version column. |
| `effective_from_column` | `str` | `"scdEffectiveFrom"` | Name of the column holding when a version became current. |
| `effective_to_column` | `str` | `"scdEffectiveTo"` | Name of the column holding when a version stopped being current. |
| `current_flag_column` | `str` | `"scdCurrentFlag"` | Name of the current-version flag column. |
| `deleted_flag_column` | `str` | `"scdDeletedFlag"` | Name of the deleted flag column. The source may carry it: without `source_deleted_flag_column`, it is then the delete signal, read as that column would be. The library always writes it itself, `deleted_flag_value` or `not_deleted_flag_value`. |
| `source_system_column` | `str` | `"scdSourceSystem"` | Name of the source system column. |
| `correlation_id_column` | `str` | `"scdCorrelationId"` | Name of the column holding the run's correlation id. Under `scd1` an update replaces it, so it names the last run that changed the row; under `scd2` a row keeps the id of the run that wrote it. |
| `current_flag_value` | `str` | `"Y"` | The value the current flag column is written with on a current version, and matched against in the merge. |
| `deleted_flag_value` | `str` | `"Y"` | The value the deleted flag column is written with on a deleted row, and compared with the target's flag to tell a row that is already deleted, or comes back, from a live one. It is not what a source's own delete column is compared with: that is `source_deleted_flag_value`. Upgrading: a context that set `deleted_flag_value` to the value its source uses for a delete, such as `"D"`, must move it to `source_deleted_flag_value`; left here, the library writes `"D"` as the deleted flag, rows already written as `"Y"` read as live, and the next load re-versions them. |
| `inactive_flag_value` | `str` | `"N"` | The value an `scd2` close-out writes into the current flag column on the version it closes. Upgrading: this key, `not_deleted_flag_value` and `default_max_timestamp` had no effect before; a context that set any of them to a non-default value now changes what is written and compared against existing tables, so remove it, or accept the effect. |
| `not_deleted_flag_value` | `str` | `"N"` | The value the deleted flag column is written with on a row that is not deleted. Upgrading: see `inactive_flag_value`. |
| `source_deleted_flag_value` | `str` | the resolved `deleted_flag_value` | What the delete signal is compared with: `source_deleted_flag_column`, or a staged `deleted_flag_column`. Set it when the source says deleted in its own words, such as `"D"` in a CDC `Op` column, while the table keeps its own `deleted_flag_value`. A Data Vault style reads it too. |
| `default_max_timestamp` | `str` | `"2999-12-31 23:59:59.999999"` | The effective-to of an open row, written as an instant in UTC. An ISO 8601 value that must lie after `global_context.load_timestamp`; a value with no offset is read as UTC, where the string it replaced was read in the session's time zone. A calendar date with no time, or anything not ISO 8601, fails validation; an ISO week date such as `2999-W52-1` is accepted, as midnight of that day. Upgrading: a value with no offset in a session whose time zone is not UTC now lands at a different instant than before, when the library read it in the session's time zone; open rows already written keep the old instant. |

**Sealed on the table (F211).** Seven values describe rows already written: `current_flag_value`,
`inactive_flag_value`, `deleted_flag_value`, `not_deleted_flag_value`, `default_max_timestamp`
(as the UTC instant, in ISO 8601), the hash separator the change hash was computed with and the
Spark session time zone (`spark.sql.session.timeZone`, which defaults to the driver JVM's zone; `UTC`
only when the session returns no value) it was computed under: the
change hash casts each column to string, and a timestamp is rendered in that zone. A new
flag value no longer matches the old rows, so a key is loaded again as new or a deleted key reads
as live; a new `default_max_timestamp` leaves older open rows at the old end; a new separator
changes every key's hash, so every key is re-versioned; a new time zone changes the hash of every
row with a timestamp column, with the same result. The zone is sealed on every table, with or
without a timestamp column. It is read once per run, from the session the writer runs in. So the merge and the append-only writers
record them as Delta table properties, `spark_quick_start.scd_<name>`
(`current_flag_value`, `inactive_flag_value`, `deleted_flag_value`, `not_deleted_flag_value`,
`default_max_timestamp`, `hash_separator`, `time_zone`), and compare before every write:

- **A first load** creates the table and seals it after the write.
- **A sealed table and a load with a different value** (for `time_zone`, a session whose zone differs from the sealed one) is refused before anything is written:
  the load returns `status` `"error"` with a `ValueError` naming the table, the property, the
  sealed value and this load's value. The table is unchanged.
- **An existing table with no seal**, written before F211 or left unsealed by a failed seal, is
  not refused, since that would refuse every existing table. Its rows are not counted, so an empty
  one is treated the same. It is sealed after its next successful write with that load's
  values, and a WARNING names them. The rows already there are assumed to have been written with
  them; if they were not, change them (below) before that load.
- **A seal that cannot be written after the write** (a privilege the loading principal lacks, a
  metadata conflict with a concurrent writer) does not fail the load: the data is committed, a
  WARNING names the table, the reason and the values not sealed, and `status` stays `"success"`.
  The table is still unsealed, so the next load tries again. On Unity Catalog,
  `SET TBLPROPERTIES` needs the table's owner or `MANAGE`; without it the table stays unsealed,
  with a warning on every load.
- **Another load sealed the table with different values while this one wrote it** also leaves
  `status` `"success"`, but is not retried: its WARNING says this load's rows were written under
  its own values, so the table may hold rows written two ways, and the next load will be refused.
  Check the rows before it, and change them or the seal as below.
- **A target the library's table-property helpers do not accept** (a hyphenated or
  backtick-quoted name part, a four-part name, a path with a backtick, `$` or control character)
  is neither checked nor sealed: one WARNING names it, and the load writes as it did before F211.
  For a target that exists, this is found before anything is written; for one the load creates,
  it is found at the seal, after the write, and its WARNING says the table was written but not
  sealed, and that every load of it will say so.
- The separator sealed is the one the detector hashed with: `scd_context.hash_separator`, or the
  value given to `SCDChangeDetectorTransformer(hash_separator=...)` when the context does not give
  one. The detector states it on the pipeline state (`PipelineState.hash_separator`); a writer
  with no detector before it seals the context's.
- The cost is one `DESCRIBE DETAIL` (a Spark action on metadata, no data read) before a write to
  an existing target, and one more `delta_table_exists` when no target step ran. A write that
  seals adds one more `DESCRIBE DETAIL` (the compare-and-set re-read) and one Delta commit on table
  metadata: a first load costs one read and the commit, an unsealed table two reads and the commit.
  A sealed table costs one read in all.
- A Data Vault table seals its own settings (`hash_algorithm`, `hash_separator`, the null token
  and the session time zone) and refuses a populated table with no seal; the SCD seal does not
  touch it, and the Data Vault styles do not use the SCD writers.

**Changing a sealed value on purpose** is a manual procedure; the library adds no operation for
it. For a value that is written into rows (a flag value, or `default_max_timestamp` on open
rows), rewrite the rows to the new value first, with an `UPDATE`; for the hash separator and the time zone, there
is no cheap rewrite: every key's hash changes, so every key (for the time zone, every key whose row
has a timestamp column) is re-versioned on the next load, and that is permanent history. Then change the property and run the load with the new context. The
seal is compared as a string, so write the value exactly as the library does: the flag values and
separator as they are in the context, `scd_time_zone` as the session's zone ID (`Europe/Kyiv`), and `scd_default_max_timestamp` as the UTC instant
`YYYY-MM-DDTHH:MM:SS[.ffffff]+00:00` (`Z` or a space in place of the `T` would refuse every later
load). The zone ID is compared as written: `UTC`, `Etc/UTC` and `GMT` count as different although
the hash is the same, and the library does not normalise them. Every platform or session that loads
a table must spell its zone the same way; set `spark.sql.session.timeZone` explicitly in the load's
session if its default differs. A table is named by catalog name or by path:

```sql
ALTER TABLE silver.customer SET TBLPROPERTIES ('spark_quick_start.scd_current_flag_value' = '1');
ALTER TABLE delta.`/lake/silver/customer` SET TBLPROPERTIES ('spark_quick_start.scd_current_flag_value' = '1');
-- or drop the seal, and the next load seals the table with its own values and a warning:
ALTER TABLE silver.customer UNSET TBLPROPERTIES ('spark_quick_start.scd_current_flag_value');
```

On Unity Catalog the statement needs the table's owner or `MANAGE`.

`SCDContext.was_given(key)` is true when the load context gave `key` a value; `null` counts as not given.

`SCDContext.default_max_timestamp_utc` is `default_max_timestamp` as a timezone-aware UTC `datetime`, a value with no offset read as UTC; it raises `ValueError` for a value `validate()` refuses.

A meta column name (hash, version, current flag, deleted flag, effective from, effective to,
correlation id) equal to `business_key_column` or `source_system_column` raises `ValueError`,
because the shadowed business column would drop out of the hash; so does a row key named as the
source system column. The row key may be the business key itself.

To join a table the library wrote, use the row key when it carries the source, as the reference
pipelines build it. A row key derived from a business key that several source systems share is
not unique across them: join on it together with the source system column. In an `scd2` table,
add the current flag for the current version, or the effective dates for a point in time.

## `RuntimeContext`

Run identity and execution switches.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `correlation_id` | `str` | required | Identifies the run across notebooks and in the correlation id column. |
| `pipeline_run_id` | `str` | none | The calling platform's run id. Written only by a Data Vault style that names `scd_context.pipeline_run_id_column`; unused otherwise. |
| `pipeline_name` | `str` | required | The calling pipeline's name. |
| `application_name` | `str` | required | The application the load belongs to. |
| `parallel_execution_enabled` | `int` | `0` | `1` (or `true`) runs the staging notebooks in parallel when there is more than one; any other value runs them one by one. |
| `max_parallel_staging` | `int` | none | In parallel mode, the most staging notebooks run at once. Unset runs them all together. |
| `serverless` | `bool` | `false` | Set on Fabric serverless and Databricks Serverless, where `.cache()` does not work; the library then skips caching. Passed on to each staging notebook. |
| `capture_partitions` | `bool` | `true` | Records each stage's partition counts. Reading them can run a Spark job, reported as `measure_ms`. |
| `capture_row_counts` | `bool` | `false` | Records each stage's input and output row counts: up to two `count()` actions per stage, reported in `measure_ms`. |
| `capture_plans` | `bool` | `false` | Records each stage's query plan, for the plan report. |
| `materialize_strategy` | `str` | `"delta"` | How `MaterializeTransformer` and `MaterializeFrameTransformer` make a frame cheap to re-read. `"delta"` writes it to a scratch Delta table and reads it back. `"cache"` persists it in memory (`MEMORY_AND_DISK`) with one action and writes no table; with `runtime_context.serverless` true it falls back to `"delta"` and logs that at INFO for each Materialize step. Cache mode is not a stable snapshot of the target after the write: the cached frames are recomputed against the merged table, so a read after the write (a row count, a later step) sees the merged rows. Any other value, `null` included, fails validation with a `ValueError` naming the key and the two values. See [Tune performance](../guides/tune-performance.md). |

Two keys that earlier versions read and nothing used, `num_partitions` and `scd_append_only`, are
refused with a `ValueError` naming the key: a load context that sets either, `null` included, fails
in `LoadContextManager.parse()`, before validation, as one setting the moved `parallel_execution_enabled` under `global_context` does. Remove
the key. An append-only load is chosen by building it with `NativeSCDAppendOnlyTransformer`; see
[Choose a load style](../guides/choose-a-load-style.md).

## `LoggingContext`

Where and how the run logs.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `logging_level` | `str` | required | A Python log level name, such as `INFO`. |
| `log_file_name` | `str` | required | Base name of the log file. |
| `log_file_dir` | `str` | required | Directory the log file is written to: a OneLake, workspace or local path. |
| `verbose` | `bool` | `false` | When true, every log line is written. When false, `debug` lines are dropped, and so is an `info` line that matches a fixed list of low-signal phrases and none of a fixed list of always-on ones, unless it carries metrics; anything unrecognised is kept. See [`OrchestratorLogger`](logging.md). |
| `suppress_stdout_tail` | `bool` | `false` | When true, the run's captured stdout is left out of the persisted log. It is still captured, so what the run prints is discarded, not shown in the notebook. Set it for sensitive data: anything printed, such as `DebugTransformer` rows, otherwise lands in the log file. |

## `HWMContext`

The load's high-water mark: the source column that grows with every change, and the value the
previous run reached. The library does not store it: pass the last run's `hwm_value_after` as
the next run's `hwm_value`. When the section is set, each staging notebook receives it as
`hwm_context`, and a staging function gets it as a parameter only when its signature declares
`hwm_context` and it can be passed by keyword; see `create_staging_main_function`.
`StagingTransformer` reports `hwm_value` as the run's `hwm_value_before` and the latest
`hwm_column_name` among the staged rows as `hwm_value_after`. `DQDTransformer` first filters its
source to `hwm_column_name > hwm_value`, the value cast to the column's type, then reports the
same way; so a DQD load with this section must be `Incremental`, and `validate_all` refuses
`Full`. With this section set, the run's
watermark is the step's that reports for `hwm_column_name` (a `StagingTransformer` or
`DQDTransformer` source, or a custom step that declares that column), even when it found none, as on
a first run that staged nothing; it is then never taken from the writer or the target. A pipeline
with no such step falls back to the first `hwm_after` and then to the target.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `hwm_column_name` | `str` | required | The source column the watermark is taken from, matched as the session matches names. `StagingTransformer` and `DQDTransformer` raise `ValueError` when the rows lack it. |
| `hwm_value` | `str` or number | none | The previous run's watermark. Unset on the first run. A number is reported as text. A boolean or an object raises `ValueError`, and so does, in a DQD load, a value that cannot be cast to the column's type. |

Any other key raises `ValueError`, so the removed DQF's `hwmColumnName` is refused rather than
ignored.

## `DQDContext`

A Declarative Query Definition: one source, filtered, derived and projected row by row, as the
load's source, read by `DQDTransformer`. Defined in `orchestration/models.py`, which checks the
shape; the expressions are parsed by `DQDTransformer` before the source is read. The expression
language is on the [DQD definition](dqd-definition.md) page.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `source_kind` | `str` | required | `delta` (a Delta table, by catalog name or path), `parquet` (a path), `view` (a catalog table or view, by name) or `warehouse_view` (a Fabric Warehouse or SQL-endpoint view). Read through `PlatformAdapter.read_delta`, `read_parquet`, `read_table` or `read_warehouse_view`; only `FabricAdapter` reads a `warehouse_view`, and every other adapter raises `NotImplementedError`. |
| `workspace_id` | `str` | none | For `warehouse_view` only: the view's workspace, when it is not the notebook's. Passed to the connector as `workspaceId`. |
| `lakehouse_id` | `str` | none | For `warehouse_view` only: the lakehouse whose SQL endpoint holds the view, when it is not the notebook's. Passed to the connector as `lakehouseId`, as the removed DQF passed `defaultLakehouseId`. A view in a Warehouse item of another workspace has not been checked on Fabric (backlog F140). |
| `source_identifier` | `str` | required | The table, view or path. |
| `source_columns` | list of `str` | `[]` | The columns to read, before anything else. Empty reads every column. |
| `filters` | list of objects | `[]` | Predicates, AND-ed, on the source columns: the removed DQF's filter with its keys renamed. |
| `transformations` | list of objects | `[]` | `{"name", "expression"}` entries, each added as a column in order, so a later one may use an earlier one. |
| `selection` | list of objects | `[]` | `{"name", "expression"}` entries, the output columns in order; an entry with only `name` selects that column. Empty passes every column through. |
| `eager_materialize_source` | `bool` | `false` | When `true`, `DQDTransformer` writes the filtered, watermarked source once to the scratch table `<global_context.staging_table_path>_<global_context.id>_0_dqd` (the id lower-cased), dropped and rewritten each run (see [Tables the library creates](#tables-the-library-creates)), and the watermark's aggregation and every later step read that, so the source is read once. Worth it for an expensive source, not a cheap Delta one. With it on, `global_context.id` must be unique among load contexts that share a `staging_table_path`, and one context must not run twice at once; the same section says why. The raw source columns are written under positional names (`_c0`, `_c1`, ...), so names Delta refuses are safe. Raises `ValueError` when not a boolean (`0`, `1` and `"true"` are refused), when `true` with `staging_table_path` unset, when `source_identifier` is the scratch table itself, and when a `transformations` or `selection` entry reads `_metadata` (filters may). |

Any other key raises `ValueError`, as does an entry without `name`, a transformation without
`expression`, or a name used twice in one list. With an `hwm_context`, the source is also filtered
to `hwm_column_name > hwm_value`, after `filters`.

## `ExtensionsContext`

The user's own data, in the `extensions_context` section: the one place for policies, rules and
parameters that a custom transformer or the pipeline's control reads. The library never
interprets it, and its values are any JSON. It is available to a step as `ctx.extensions_ctx`
and to each staging notebook, whole, as that notebook's `extensions_context` section; a staging
function that declares an `extensions_context` parameter is given it too. The object is
read-only: it holds a deep copy of the section, so an assignment to it raises `TypeError`. Only
the top level is protected; an object nested in a value is a plain `dict` that a step must not
change. By convention, not enforced, namespace the top-level keys, such as `"acme.quality"`, so
two teams' steps do not collide. Keep the section small: it rides in every staging notebook's argument string, and the platform limits on that string are unchecked here. A large rule set belongs in a table or file, with its path in the section. A load without the section has an empty one. See
[Write a custom transformer](../guides/write-a-custom-transformer.md).

| Member | Type | Default | Meaning |
|---|---|---|---|
| `get` | method, `(key, default=None) -> Any` | none | The value for `key`, or `default`. Indexing with `[]` raises `KeyError` for a missing key; `in`, `len` and iteration work as on a mapping. |
| `to_dict` | method, `() -> dict` | none | A deep copy of the section. |
| `from_raw` | classmethod, `(raw) -> ExtensionsContext` | none | Builds one from the parsed section. Raises `ValueError` when `raw` is not an object. |

## `PlatformContext`

What the library knows about where it runs, in the `platform_context` section. You never write it:
`LoadContextManager.parse()` fills it from the two paths the caller gave `LoadOrchestrator`
(`data_storage_path` and `data_eng_path`) and refuses a load context that carries the section.
It is available to a step as `ctx.platform_ctx`, a frozen dataclass, and each staging notebook
receives it as its `platform_context` section; the staging helper takes the `storage_path` it
gives the staging function from there.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `data_storage_lakehouse_path` | `str` | `""` | The data storage lakehouse path; the value `{DataStorageLakehouse}` is replaced with. |
| `data_engineering_lakehouse_path` | `str` | `""` | The data engineering lakehouse path; the value `{DataEngineeringLakehouse}` is replaced with. |
| `to_dict` | method, `() -> dict` | none | Both paths, as a staging notebook receives them. |
