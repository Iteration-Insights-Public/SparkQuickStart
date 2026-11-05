# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6c9a4b42-877d-4898-b018-941aa5ad2571",
# META       "default_lakehouse_name": "lh_quickstart",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000",
# META       "known_lakehouses": [
# META         {
# META           "id": "6c9a4b42-877d-4898-b018-941aa5ad2571"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "d8ed079d-f1ab-4838-9c70-4a380e5e2cb0",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Orchestrate a job
#
# The Fabric form of the Databricks bundle's jobs: one `runMultiple` DAG in this notebook's
# session, so every activity shares the lakehouse and Environment bound here, as the
# Databricks tasks share one cluster. `job` picks the chain:
#
# - `load_bronze`: the four AdventureWorks loads (Full), chained: a failed load stops the
#   ones after it.
# - `load_iot`: seeds the IoT landing table, then loads it (Incremental).
# - `iot_changes`: a round of changes: `iot_generate` changes `update` devices and adds `new`
#   ones in `Files/raw/IoT_DeviceRegistry`, then the IoT landing table is seeded and loaded.
# - `demo`: the whole first load: makes the IoT data when it is absent, seeds every landing
#   table, then runs the four AdventureWorks loads and the IoT load, chained.

# PARAMETERS CELL ********************

job = "demo"
# job=iot_changes: devices to add, and existing devices to change.
new = 1000
update = 100

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ruff: noqa: F821, E402 - notebookutils is a Fabric notebook global; the parameters cell comes before the imports

from datetime import UTC, datetime

from spark_quick_start.adapters import FabricAdapter

AW_CONTEXTS = (
    "person_person",
    "person_personphone",
    "sales_salesorderheader",
    "sales_salesorderdetail",
)
LOAD_IOT = [("seed_landing", "IoT_DeviceRegistry"), ("load_entity", "iot_deviceregistry")]
CHAINS = {
    "load_bronze": [("load_entity", name) for name in AW_CONTEXTS],
    "load_iot": LOAD_IOT,
    "iot_changes": [("iot_generate", "changes"), *LOAD_IOT],
    "demo": [
        ("iot_generate", "initial"),
        ("seed_landing", ""),
        *(("load_entity", name) for name in AW_CONTEXTS),
        ("load_entity", "iot_deviceregistry"),
    ],
}
if job not in CHAINS:
    raise ValueError(f"nb_orchestrate: job {job!r} is not one of {sorted(CHAINS)}")

# Taken once, so every load of the run logs to one day's folder and shares one correlation ID.
run_date = datetime.now(UTC).date().isoformat()
job_run_id = notebookutils.runtime.context["activityId"]


def activity_args(notebook: str, arg: str) -> tuple[str, dict]:
    """The activity's name and its notebook's parameters."""
    if notebook == "iot_generate":
        return "iot_generate", {"mode": arg, "new": int(new), "update": int(update)}
    if notebook == "seed_landing":
        return "seed_landing", {"tables": arg}
    return arg, {"context_file": f"{arg}.json", "run_date": run_date, "job_run_id": job_run_id}


activities = []
for notebook, arg in CHAINS[job]:
    name, args = activity_args(notebook, arg)
    activities.append({
        "name": name,
        "path": notebook,
        # The default is 90 seconds a cell, shorter than a load.
        "timeoutPerCellInSeconds": 3600,
        "args": args,
        "dependencies": [activities[-1]["name"]] if activities else [],
    })
dag = {
    "activities": activities,
    "timeoutInSeconds": 10800,
    # One at a time: the chain is sequential, and an F4 capacity has room for one session.
    "concurrency": 1,
}
print(f"nb_orchestrate: {job}, run {job_run_id}, {run_date}: " + " -> ".join(a["name"] for a in activities))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


def report(results: dict) -> None:
    """One line per activity: its exit value, or the exception that stopped it."""
    for name, outcome in results.items():
        outcome = outcome or {}
        print(f"  {name}: {outcome.get('exception') or outcome.get('exitVal')}")


# runMultiple raises when an activity fails, with the per-activity results on the exception;
# control.load_runs holds the record of every load either way.
try:
    results = FabricAdapter().run_notebooks_parallel(dag)
except Exception as exc:
    report(getattr(exc, "result", None) or {})
    raise
report(results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
