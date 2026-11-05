# Deploy the Databricks reference design

This Databricks Asset Bundle deploys `spark_quick_start` to an Azure Databricks workspace and
loads the repository's sample data from a landing schema into SCD2 tables in a bronze schema.
At the end you will have the schemas, volumes and jobs in your workspace, landing tables seeded
from this repository's `data/` folder, and bronze tables loaded through a DQD source and an SCD2
merge. It is for an engineer with access to a workspace who wants to see the library run end to
end. It is experimental, runs on Databricks only, and uses a classic single-node job cluster.

## What it deploys

Every name below starts with `schema_prefix`, which is `dev_<your short name>_` on the `dev`
target, so deployments by different people do not collide. The catalog is not created; it must
exist.

| Schema | Holds |
|---|---|
| `landing` | The source tables, seeded from the folders uploaded to its `raw` volume |
| `bronze` | The SCD2 history of the landing tables |
| `stage` | Scratch tables the library writes during a load |
| `control` | `load_runs`, one row per load, and the `logs` volume (one folder per day) |
| `silver` | Reserved; nothing writes here yet |

| Job | Does |
|---|---|
| `seed_landing` | Overwrites each landing table from its folder in the `raw` volume, and creates `control.load_runs` if it is missing. A `tables` parameter limits it to some folders. |
| `load_bronze` | Loads the four AdventureWorks tables, one task after another: `person_person`, `person_personphone`, `sales_salesorderheader`, `sales_salesorderdetail`. A failed load stops the rest. |
| `load_iot` | Seeds the IoT landing table from the `raw` volume, then loads `iot_deviceregistry` incrementally, on one cluster. |
| `demo` | The whole first load on one cluster: seeds every landing table, then runs the four AdventureWorks loads and the IoT load, one after another. |

Each load task runs `src/load_entity.py` with one file from `contexts/`. The four
AdventureWorks loads are `Full`: a key that leaves landing is closed in bronze. The IoT load is
`Incremental` with a watermark on `modified_at`: it reads only landing rows with `modified_at`
later than the watermark.

The watermark lives in `control.load_runs`. Each load records `hwm_value_before` and
`hwm_value_after`. The next IoT load starts from the `hwm_value_after` of the latest
*successful* IoT run, so a failed run does not move it, and a run that found no new rows keeps
it. The first run has none and reads every row.

## Before you start

- **A bash shell.** On Windows, use Git Bash: open it from the Start menu, or run
  `"%LOCALAPPDATA%\Programs\Git\bin\bash.exe"` from cmd. Do not type plain `bash` in cmd: it can
  start WSL, which does not see your Windows `databricks` CLI or its profiles.
- **Python with pip**, which builds the library's wheel. `deploy.sh` uses the first of `python3`
  and `python` that runs.
- **The Databricks CLI**, logged in to the workspace as yourself (step 1).
- **A Unity Catalog catalog** with managed storage, its own or the metastore's root.
- **A service principal** in the workspace that the jobs run as. You need its application ID, a
  UUID, not its display name. It needs `USE CATALOG` on the catalog; the bundle grants
  everything inside the schemas it creates:

  ```bash
  databricks grants update catalog <catalog> -p dev --json '{"changes":[{"principal":"<sp-application-id>","add":["USE_CATALOG"]}]}'
  ```

  It also needs the "Allow unrestricted cluster creation" entitlement, or a cluster policy it
  can use.
- **The Service Principal: User role** for you on that service principal (Settings > Identity and
  access > Service principals > the principal > Permissions).
- **VM quota** in the workspace's region for the `node_type` family, 4 cores
  (`Standard_D4ds_v4` by default).
- **For the IoT load:** the local Delta table `data/IoT_DeviceRegistry_delta`, made with
  [`data/generate_iot_device_sample.py`](../../data/generate_iot_device_sample.py):

  ```bash
  python data/generate_iot_device_sample.py --rows 100000 --format delta --partitions 4 --seed 1
  ```

  On Windows, set `HADOOP_HOME` to a folder whose `bin/` holds `winutils.exe` and `hadoop.dll`
  first. Without the folder, the four AdventureWorks tables still deploy and load.

  Run the generator, and `scripts/demo_changes.sh`, which calls it, from the repository's
  `.venv` (PySpark 3.5 and delta-spark 3.2, from `pip install -e ".[dev,local]"`; see
  [Set up for development](../../docs/guides/set-up-for-development.md)). Activate it first, so
  its `python3` is the first on `PATH`. The demo was run that way, writing a table that
  DBR 15.4 reads. A Python with PySpark 4 and delta-spark 4, such as a newer system or conda
  install, was not tried: its Delta writer may add table features DBR 15.4 cannot read.

## Settings

Every value the deployment depends on, where it is set, and its default. Each can be set once
as an environment variable, or given on each command as its argument; the argument wins when
both are set. The environment variable names are the Databricks CLI's own, so
`databricks bundle run` reads them as well as `deploy.sh`.

| Setting | Environment variable | Argument | Required | Default |
|---|---|---|---|---|
| Workspace | `DATABRICKS_CONFIG_PROFILE`: the CLI profile, which holds the workspace host | `-p <profile>` | yes | the profile marked `Default` in `databricks auth profiles` |
| Catalog | `BUNDLE_VAR_catalog` | first argument of `deploy.sh`, or `--var catalog=...` | yes | none |
| Run-as service principal | `BUNDLE_VAR_run_as_sp`: its application ID | second argument of `deploy.sh`, or `--var run_as_sp=...` | yes | none |
| Schema prefix | `BUNDLE_VAR_schema_prefix` | `--var schema_prefix=...` | no | `dev_<your short name>_` |
| Cluster VM type | `BUNDLE_VAR_node_type` | `--var node_type=...` | no | `Standard_D4ds_v4` |
| Databricks Runtime | `BUNDLE_VAR_spark_version` | `--var spark_version=...` | no | `15.4.x-scala2.12`; the library needs 15.4 LTS or later |
| Python for the wheel build | `BUNDLE_VAR_python` | `--var python=...` | no | the first of `python3` and `python` that runs |
| Hadoop for local Spark | `HADOOP_HOME` | none | on Windows, for the IoT generator only | none |

The workspace host is not a setting of its own: `databricks auth login --host` stores it in the
profile (step 1). Find it in the address bar of the workspace, `https://adb-<id>.<n>.azuredatabricks.net`.

Set them once per shell, for example in Git Bash:

```bash
export DATABRICKS_CONFIG_PROFILE=dev
export BUNDLE_VAR_catalog=main
export BUNDLE_VAR_run_as_sp=<sp-application-id>
```

Run the commands below from the repository root.

## Run the demo

Two commands, after "Before you start" and step 1 below. Each runs on one job cluster, which
takes 4–7 minutes to start.

1. **The first load:**

   ```bash
   reference_design/databricks/scripts/deploy.sh --demo
   ```

   It deploys the bundle, uploads `data/`, and runs the `demo` job. On a fresh deployment
   `control.load_runs` then shows `inserted_count` 19972 (`Person_Person`), 19972
   (`Person_PersonPhone`), 31465 (`Sales_SalesOrderHeader`), 121317 (`Sales_SalesOrderDetail`)
   and 100000 (`IoT_DeviceRegistry`, from the generator command above). Run again, it loads
   nothing new: each load exits early with `inserted_count` 0.

2. **A round of changes,** as many times as you like:

   ```bash
   reference_design/databricks/scripts/demo_changes.sh
   ```

   It changes the local `data/IoT_DeviceRegistry_delta`: 1000 new devices and 100 existing ones
   with a new `firmware_version` (`--new N` and `--update M` change the numbers), all with a
   `modified_at` after the last watermark. It then uploads the table and runs `load_iot`. The
   run's row in `load_runs` shows `inserted_count` 1100 (the new devices, and the new versions of
   the updated ones), `updated_count` 100 (the versions they close), and `hwm_value_before`
   equal to the previous run's `hwm_value_after`. Options after the two it reads go to
   `deploy.sh`, such as `-p <profile>`. Run it with the repository's `.venv` active, as
   "Before you start" says for the generator.

   An updated device then has two versions in bronze:

   ```sql
   SELECT device_id, firmware_version, modified_at, scdVersion, scdCurrentFlag, scdEffectiveTo
   FROM <catalog>.dev_<you>_bronze.iot_deviceregistry
   WHERE device_id IN (
     SELECT device_id FROM <catalog>.dev_<you>_bronze.iot_deviceregistry WHERE scdVersion = 2 LIMIT 1)
   ORDER BY scdVersion;
   ```

   Version 1 has `scdCurrentFlag` `N` and an `scdEffectiveTo` at the load; version 2 is current.

The steps below do the same in parts, and the jobs can be run on their own.

## Steps

1. **Log in** with the profile you will deploy with:

   ```bash
   databricks auth login --host https://<workspace>.azuredatabricks.net -p dev
   ```

   `databricks auth profiles` should then show the profile as `Valid: YES`.

2. **Deploy and seed landing:**

   ```bash
   reference_design/databricks/scripts/deploy.sh
   ```

   With no settings in the environment, pass them on the command:
   `reference_design/databricks/scripts/deploy.sh -p dev <catalog> <sp-application-id>`.

   The script builds the wheel and runs `databricks bundle deploy`. It then uploads each
   `data/Person_*` and `data/Sales_*` folder, and `data/IoT_DeviceRegistry_delta`, to the `raw`
   volume and runs `seed_landing`. The IoT folder is taken by that exact name, so larger
   variants beside it, such as `IoT_DeviceRegistry_delta_1M`, stay local. A folder named
   `..._delta` lands without the suffix, so `data/IoT_DeviceRegistry_delta` becomes the table
   `landing.iot_deviceregistry`. A folder already in the volume is removed before its upload, so
   a regenerated table replaces the old one rather than mixing with it; a folder you deleted
   locally stays in the volume. No load runs yet.

3. **Run the loads.** Add `--load` to the command in step 2 to run `load_bronze` after seeding,
   and `--load-iot` to run `load_iot`. To run a job without uploading again:

   ```bash
   cd reference_design/databricks
   databricks bundle run load_iot --target dev
   ```

   This reads the profile, catalog and service principal from the environment; without them, add
   `-p dev --var catalog=<catalog> --var run_as_sp=<sp-application-id>`.

   Use `load_bronze` in place of `load_iot` for the AdventureWorks loads. You can also start
   either job with **Run now** on its page in the workspace's job list.

## Refresh landing only

`--landing-only` uploads and seeds without `bundle deploy`, so the jobs, schemas, volumes, wheel,
bronze tables and `load_runs` stay as they are. `--tables` limits it to the folders you name,
case-insensitive and separated by commas. Without `--tables`, it refreshes every landing table.

A round of the IoT test loop, which `scripts/demo_changes.sh` runs in one command:

1. Add new devices to the local table, and with `--update` change existing ones. The generator
   continues the device numbers and makes every new or changed `modified_at` later than the
   table's latest:

   ```bash
   python data/generate_iot_device_sample.py --rows 10000 --update 100 --format delta --append
   ```

2. Upload only the IoT table, and load it. `load_iot` seeds the landing table itself, so no
   separate `seed_landing` runs:

   ```bash
   reference_design/databricks/scripts/deploy.sh --landing-only --tables IoT_DeviceRegistry --load-iot
   ```

`--landing-only` needs a bundle deployed by this version of the script first: the `tables`
parameter it passes to `seed_landing`, and the `seed_iot` task of `load_iot`, only exist on jobs
deployed with it.

## Check it worked

Read the run records:

```sql
SELECT entity, status, inserted_count, updated_count, hwm_value_before, hwm_value_after, recorded_at
FROM <catalog>.dev_<you>_control.load_runs
ORDER BY recorded_at DESC;
```

A successful load has `status` `success`. For `IoT_DeviceRegistry`, the first run inserts every
landing row and has no `hwm_value_before`. Each later run inserts the rows of the new round, and
its `hwm_value_before` equals the previous run's `hwm_value_after`. A run with nothing new
exits early with `inserted_count` 0 and keeps the watermark.

Each run's log is under `/Volumes/<catalog>/dev_<you>_control/logs/<run date>/`.

## `deploy.sh` options

| Option | Effect |
|---|---|
| `<catalog> <sp-application-id>` | The catalog and the run-as service principal, both or neither. Without them, `BUNDLE_VAR_catalog` and `BUNDLE_VAR_run_as_sp` must be set. |
| `-p`, `--profile <name>` | The CLI profile to use; without it, `DATABRICKS_CONFIG_PROFILE`. With either, the script unsets `ARM_CLIENT_ID`, `ARM_CLIENT_SECRET` and `ARM_TENANT_ID`, which would otherwise take precedence over the profile. |
| `--var <name=value>` | Any other bundle variable, such as `node_type=Standard_D4ds_v4`; repeatable. |
| `--landing-only` | Upload and seed only; no `bundle deploy`. |
| `--tables <a,b>` | Limit the upload and the seeding to these landing folders, such as `IoT_DeviceRegistry`. |
| `--load` | Run `load_bronze` after seeding. |
| `--load-iot` | Run `load_iot` after seeding. `load_iot` seeds the IoT table itself, so with `--tables IoT_DeviceRegistry` alone `seed_landing` is skipped. |
| `--demo` | Run the `demo` job in place of `seed_landing`; needs `data/IoT_DeviceRegistry_delta`. Not with `--tables`, `--load` or `--load-iot`. |

The bundle variables are in [`databricks.yml`](databricks.yml): `catalog`, `run_as_sp`,
`schema_prefix`, `node_type`, `spark_version` and `python`. "Settings" above lists their
environment variables and defaults.

## When it goes wrong

- **`the catalog and the run-as service principal are not set`.** Neither the two arguments nor
  `BUNDLE_VAR_catalog` and `BUNDLE_VAR_run_as_sp` were given. Set the variables in this shell
  (`export` in Git Bash lasts for that window only), or pass both arguments.
- **`Python was not found; run without arguments to install from the Microsoft Store`** during
  `Building spark_quick_start`. On Windows, `python3` is often a Store placeholder. The script
  picks the first of `python3` and `python` that runs; if neither does, install Python, or pass
  `--var python=<path to python>`.
- **`dial tcp: lookup <workspace>.azuredatabricks.net: no such host`.** The workspace is not
  reachable from your network. Connect the VPN or network it needs, and check the host in
  `databricks auth profiles`.
- **403 when the deploy creates the jobs.** You lack the Service Principal: User role on the
  run-as service principal. See "Before you start".
- **The job fails with `PERMISSION_DENIED` before its cluster starts.** The service principal
  may not create clusters. Give it the cluster-creation entitlement or a cluster policy.
- **The cluster fails with `AZURE_QUOTA_EXCEEDED_EXCEPTION`.** The subscription has no core quota
  for the VM family in that region. Check with
  `az vm list-usage --location <region> --subscription <subscription-id> -o table`, or pass
  another family with `--var node_type=...`.
- **`--tables ...: not every name matches a local folder`.** A name has no folder under `data/`.
  Use the landing folder name, without `_delta`, as the message lists them.
- **`seed_landing: tables [...] not under /Volumes/...`.** The job was asked for a folder the raw
  volume does not hold. Run without `--landing-only` once, or upload the folder first.

## See also

- [Load context reference](../../docs/reference/load-context.md): every field of the files in
  `contexts/`, including `hwm_context`.
- [`data/generate_iot_device_sample.py`](../../data/generate_iot_device_sample.py): the IoT
  sample generator; `--help` lists its options.
- [`../local/NB_BI_LoadEntity_IoT_Gen3.ipynb`](../local/NB_BI_LoadEntity_IoT_Gen3.ipynb): the
  same IoT load on a local Spark session.
