# Deploy the Fabric reference design

This reference design deploys `spark_quick_start` to a Microsoft Fabric workspace and loads the
repository's sample data from a landing schema into SCD2 tables in a bronze schema. At the end
you will have a lakehouse with the schemas, an Environment with the library's wheel, four
notebooks, landing tables seeded from this repository's `data/` folder and from IoT data made in
Fabric, and bronze tables loaded through a DQD source and an SCD2 merge. It is the Fabric twin of the
[Databricks reference design](../databricks/README.md), and is for an engineer with a Fabric
workspace who wants to see the library run end to end.

It is experimental. It ran end to end on 2026-10-07 on a Fabric trial capacity (`FTL4`, an F4),
in a new workspace: the first load, a rerun that loaded nothing, and a round of changes, with the
counts below. The design and what was found on the way are in
[the design record](../../docs/architecture/decisions/2026-10-07-fabric-reference-design.md).

## What it deploys

Each developer deploys into their own workspace, so nothing carries a prefix. The workspace is
not created; it must exist on a Fabric capacity.

| Item | Holds |
|---|---|
| `lh_quickstart` | A schema-enabled lakehouse. `Files/raw/` holds the uploaded landing folders, `Files/contexts/` the load contexts, `Files/logs/` one folder of run logs per day |
| `env_quickstart` | An Environment on Runtime 1.3 (Spark 3.5, Python 3.11) with the library's wheel, built from this checkout on every deploy |
| `iot_generate` | Makes the IoT device registry in `Files/raw/IoT_DeviceRegistry`: `mode=initial` creates 100000 devices when the folder is empty, `mode=changes` changes `update` devices and adds `new` ones after the last watermark |
| `seed_landing` | Creates the schemas and `control.load_runs`, and overwrites each landing table from its folder in `Files/raw/`. A `tables` parameter limits it to some folders |
| `load_entity` | Loads one entity: one context file, DQD → SCD2, one row in `control.load_runs` |
| `nb_orchestrate` | Runs a job as one `runMultiple` chain in one session; its `job` parameter is `demo`, `load_bronze`, `load_iot` or `iot_changes` |

| Schema | Holds |
|---|---|
| `landing` | The source tables, seeded from `Files/raw/` |
| `bronze` | The SCD2 history of the landing tables |
| `stage` | Scratch tables the library writes during a load |
| `control` | `load_runs`, one row per load |

| Job (`nb_orchestrate`'s `job`) | Does |
|---|---|
| `load_bronze` | Loads the four AdventureWorks tables, one after another: `person_person`, `person_personphone`, `sales_salesorderheader`, `sales_salesorderdetail`. A failed load stops the rest. |
| `load_iot` | Seeds the IoT landing table from `Files/raw/`, then loads `iot_deviceregistry` incrementally. |
| `iot_changes` | A round of changes: `iot_generate` with `mode=changes`, then `load_iot`'s two steps. Its `new` and `update` parameters, 1000 and 100 by default, size the round. |
| `demo` | The whole first load: makes the IoT data when it is absent, seeds every landing table, then runs the four AdventureWorks loads and the IoT load, one after another. |

The contexts in `contexts/` are the Databricks ones with Fabric names: `landing.<table>`,
`bronze.<table>` and `stage.<table>`, resolved in `lh_quickstart`. The four AdventureWorks loads
are `Full`; the IoT load is `Incremental` with a watermark on `modified_at`, which lives in
`control.load_runs` exactly as on Databricks: the next IoT load starts from the
`hwm_value_after` of the latest *successful* IoT run.

The IoT data is made in Fabric and never uploaded, so the demo needs no local Spark. It has the
columns and value ranges of the repository's
[`data/generate_iot_device_sample.py`](../../data/generate_iot_device_sample.py), which the
Databricks design uses, in logic of its own: each random value is a hash of the row's key and a
seed. Do not copy a local IoT table into `Files/raw/IoT_DeviceRegistry`: its `modified_at`
values would not follow the watermark the loads have recorded.

A context names a table, or a path as an `abfss://` URI or an absolute `/lakehouse/default/...`
path; never a relative `Files/...` path, which the library reads as a table name.

## Before you start

- **A Fabric workspace of your own** on a Fabric capacity, an F SKU or a trial. A Power BI
  Premium Per User capacity runs no Spark. You need the Admin or Member role on it.
- **The Fabric CLI**, signed in as yourself:

  ```bash
  pip install ms-fabric-cli
  ```

  ```bash
  fab auth login
  ```

  Choose the interactive browser login, not a service principal. `fab auth status` should
  show your account.
- **The Azure CLI**, signed in as the same user. `azcopy` and `fabric-cicd` take its token:

  ```bash
  az login
  ```

  `deploy.sh` stops when `fab` and `az` are signed in as different principals. Without that
  check the upload fails with `ArtifactNotFound` or `403`, which do not name the cause.
- **azcopy** on `PATH`. `fab cp` uploads single files only, so the Delta folders go up with
  `azcopy`, on OneLake's dfs endpoint.
- **fabric-cicd and azure-identity** in the Python that runs `deploy.sh`:

  ```bash
  pip install fabric-cicd azure-identity
  ```

On an F4 capacity one Spark session takes most of the capacity: the Environment asks for one
driver and one executor, and the orchestrator runs one notebook at a time. Two people running on
one F4 at once will see the second session refused with HTTP 430.

## Settings

| Setting | Environment variable | Argument | Required | Default |
|---|---|---|---|---|
| Workspace | `FABRIC_WORKSPACE`: its name | the argument of `deploy.sh` | yes | none |

Run the commands below from the repository root, with the repository's `.venv` active.

## Run the demo

1. **The first load:**

   ```bash
   reference_design/fabric/scripts/deploy.sh dev_<you> --demo
   ```

   It builds the wheel, publishes the items with `fabric-cicd` (the Environment's publish takes
   about 5 minutes), uploads the AdventureWorks folders from `data/` and the contexts, and runs
   `nb_orchestrate` with `job=demo`, which makes the IoT data first.
   On a fresh workspace `control.load_runs` then shows `inserted_count` 19972 (`Person_Person`),
   19972 (`Person_PersonPhone`), 31465 (`Sales_SalesOrderHeader`), 121317
   (`Sales_SalesOrderDetail`) and 100000 (`IoT_DeviceRegistry`). Run again, it loads nothing
   new: each load exits early with `inserted_count` 0.

2. **A round of changes,** as many times as you like:

   ```bash
   reference_design/fabric/scripts/demo_changes.sh dev_<you>
   ```

   It runs `nb_orchestrate` with `job=iot_changes`: in Fabric, 1000 new devices and 100 existing
   ones with a new `firmware_version` (`--new N` and `--update M` change the numbers), all with
   a `modified_at` after the last watermark, then the IoT load. Nothing is uploaded. The run's row
   in `load_runs` shows `inserted_count` 1100, `updated_count` 100, and `hwm_value_before` equal
   to the previous run's `hwm_value_after`. The same round runs from the workspace: open
   `nb_orchestrate`, set `job` to `iot_changes`, and **Run all**.

   An updated device then has two versions in bronze:

   ```sql
   SELECT device_id, firmware_version, modified_at, scdVersion, scdCurrentFlag, scdEffectiveTo
   FROM bronze.iot_deviceregistry
   WHERE device_id IN (SELECT device_id FROM bronze.iot_deviceregistry WHERE scdVersion = 2 LIMIT 1)
   ORDER BY scdVersion;
   ```

## Steps

1. **Publish and seed landing:**

   ```bash
   reference_design/fabric/scripts/deploy.sh dev_<you>
   ```

   The script checks that `fab` and `az` are one principal and that the workspace exists, builds
   the wheel into `build/fabric_wheel/`, and runs `scripts/publish_items.py`. That copies
   `workspace/` to `build/fabric_items/` with the wheel in the Environment, and publishes it with
   `fabric-cicd`: the lakehouse, then the Environment, then the notebooks. The notebooks name the
   lakehouse and the Environment by their logical IDs, which `fabric-cicd` replaces with the
   deployed items' IDs. The script then uploads `contexts/` and each `data/Person_*` and
   `data/Sales_*` folder to `Files/`, removing a folder already there first, and runs
   `seed_landing`. No load runs yet, and no IoT data exists until `iot_generate` runs: `demo`
   runs it, or run it from the workspace with `mode=initial`.

2. **Run the loads.** Add `--load` to the command in step 1 to run `job=load_bronze` after
   seeding, and `--load-iot` to run `job=load_iot`. Or open `nb_orchestrate` in the workspace,
   set `job` in its parameters cell, and **Run all**; or run one `load_entity` with its
   `context_file` set, such as `person_person.json`.

## Refresh landing only

`--landing-only` uploads and seeds without publishing, so the notebooks, the Environment, the
bronze tables and `load_runs` stay as they are. `--tables` limits it to the AdventureWorks folders
you name, case-insensitive and separated by commas:

```bash
reference_design/fabric/scripts/deploy.sh dev_<you> --landing-only --tables Person_Person --load
```

The IoT table is not uploaded: a round of changes is `demo_changes.sh`.

## Deploy through Git integration

A second way to deploy, into a workspace of its own: the workspace is connected to this
repository's `main`, folder `reference_design/fabric/workspace/`, and Fabric's **Update all**
brings in the lakehouse, the Environment's settings and the notebooks. Git carries the items
only. The wheel is never committed, and Fabric does not publish an Environment on Update, so
after every Update `deploy.sh --wheel-only` puts the wheel from your checkout into
`env_quickstart` and publishes it. The data still goes up with `deploy.sh --landing-only`.

Never connect a workspace that `deploy.sh` publishes to, and never run `deploy.sh` without
`--wheel-only` or `--landing-only` on a connected one: the two would contend for the same items.
In a connected workspace, Source control shows `env_quickstart` as changed against Git, because
its wheel is not in Git; leave it uncommitted.

**Before you start**, besides what is listed above:

- A new workspace on a Fabric capacity, with you as Admin.
- The tenant setting "Users can synchronize workspace items with GitHub repositories" on.
- A GitHub fine-grained personal access token with **Contents: Read** on the repository.
- `main` pushed: Fabric reads GitHub, not your checkout.

**Steps:**

1. **Connect.** Workspace settings → Git integration → GitHub: add the account with the token,
   then repository `SparkQuickStart`, branch `main`, folder `reference_design/fabric/workspace`,
   and **Connect and sync**, from Git to the workspace. The lakehouse, the Environment and the
   four notebooks appear.
2. **Publish the Environment with the wheel:**

   ```bash
   reference_design/fabric/scripts/deploy.sh <workspace> --wheel-only
   ```

3. **Upload the data and seed landing:**

   ```bash
   reference_design/fabric/scripts/deploy.sh <workspace> --landing-only
   ```

4. **Run the demo** in the workspace: open `nb_orchestrate`, keep `job` as `demo`, **Run all**.
   `control.load_runs` shows the counts under [Run the demo](#run-the-demo). A round of changes
   is the same notebook with `job` set to `iot_changes`. Stop the session afterwards: on an F4 it
   holds the capacity.
5. **Redeploy** after a change on `main`: push `main`, then Source control → **Update all** in
   the workspace, then step 2 again. Step 3 again only when `data/` or `contexts/` changed.

## Check it worked

```sql
SELECT entity, status, inserted_count, updated_count, hwm_value_before, hwm_value_after, recorded_at
FROM control.load_runs
ORDER BY recorded_at DESC;
```

A successful load has `status` `success`. Each run's log is under
`Files/logs/<run date>/` in `lh_quickstart`.

## `deploy.sh` options

| Option | Effect |
|---|---|
| `<workspace>` | The workspace's name. Without it, `FABRIC_WORKSPACE` must be set. |
| `--landing-only` | Upload and seed only; no wheel build and no publish. |
| `--wheel-only` | Build the wheel and publish `env_quickstart` with it, nothing else: the step after each Update in a workspace connected to Git. Not with another option. |
| `--tables <a,b>` | Limit the upload and the seeding to these AdventureWorks folders, such as `Person_Person`. `IoT_DeviceRegistry` is refused: the IoT data is made in Fabric. |
| `--load` | Run `nb_orchestrate` with `job=load_bronze` after seeding. |
| `--load-iot` | Run `nb_orchestrate` with `job=load_iot` after seeding: it loads the IoT data `iot_generate` last made. |
| `--demo` | Run `nb_orchestrate` with `job=demo` in place of `seed_landing`. Not with `--tables`, `--load` or `--load-iot`. |

## Not yet proven

Run on 2026-10-07: `deploy.sh --demo` on a new workspace, again on the same one, then
`demo_changes.sh`, with the counts under [Run the demo](#run-the-demo); and `job=load_bronze`
with a broken context, which stopped the chain at that load. Not yet run:

- `--landing-only`, `--tables`, `--load` and `--load-iot`;
- a capacity other than the F4 trial, and two people on one capacity at once;
- the Git mode, none of it yet: that the nested folder `reference_design/fabric/workspace` is
  accepted when connecting; that a lakehouse created by Update from
  `{"defaultSchema": "dbo"}` has schemas; that the notebooks bind to `lh_quickstart` and
  `env_quickstart` (Microsoft documents the Environment's binding, and the lakehouse's as kept
  unless the notebook setting "Lakehouse Auto-Binding in Git" is on; if not, bind each notebook
  to `lh_quickstart` once by hand); and that Update all keeps the wheel while `main` leaves the
  Environment alone, and what it does when `main` changes the Environment's settings.

## When it goes wrong

- **`fab and az are not signed in as the same user`.** Sign both in as yourself:
  `fab auth login` with the interactive browser login, then `az login`.
- **`workspace ... not found, or not visible to you`.** Check the name with `fab ls`. A
  workspace created while `fab` was signed in as a service principal has no role for you;
  add yourself as Admin with `fab acl set <workspace>.Workspace -I <your object ID> -R Admin`
  while still signed in as it.
- **`azcopy` fails with `ArtifactNotFound` or `403`.** The `az` user has no role on the
  workspace; see the first point.
- **HTTP 430 `TooManyRequestsForCapacity` when a notebook starts.** Another Spark session holds
  the capacity, often the interactive session of a notebook you ran from the workspace, which
  stays up after **Run all** until it times out. Stop it (**Stop session**, or the Monitor hub)
  and run again.
- **`fab job run` reports `Failed` with `System_Cancelled_Session_Statements_Failed`.** A load
  failed; that message says only that the session stopped. The cause is the failed load's row in
  `control.load_runs` (`status` `error`, `error_message`), and the run's snapshot in the Monitor
  hub shows each activity's exception. The loads after it in the chain did not start.
- **`Use mounted path failed because mount point doesn't exists or there are some errors in
  blobfuse`, after a load that succeeded.** Seen on 2026-10-07 with a `HEAD ... 404` on a log
  file's path. If that file is under `Files/logs/<run date>/` in `lh_quickstart`, the log was
  written and the message is Fabric's diagnostic of the existence check before the write, not a
  failure. Why Fabric prints it for one write and not the others is not known.
- **`env_quickstart's publish state is ... not Success`.** The Environment's publish failed or is
  still running; open it in the workspace. A session opened before a publish keeps the old
  library until it is stopped.
- **`publish_items: fabric_cicd is not installed`.** Install it in the Python that runs the
  script: `pip install fabric-cicd azure-identity`.
- **`seed_landing: tables [...] not under Files/raw`.** The folder was never uploaded; run without
  `--landing-only` once. For `IoT_DeviceRegistry`, no IoT data was made yet: run `--demo`, or
  `iot_generate` with `mode=initial`.
- **`iot_generate: no table at Files/raw/IoT_DeviceRegistry; run mode=initial first`.** A round
  of changes before the first load; run `deploy.sh --demo` first.

## See also

- [The design record](../../docs/architecture/decisions/2026-10-07-fabric-reference-design.md):
  why it is this shape, and what the first live run found.
- [Load context reference](../../docs/reference/load-context.md): every field of the files in
  `contexts/`.
- [The Databricks reference design](../databricks/README.md): the same demo on Databricks.
