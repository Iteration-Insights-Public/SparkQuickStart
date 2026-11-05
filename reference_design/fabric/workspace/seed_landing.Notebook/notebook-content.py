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

# # Seed landing
#
# Run by `scripts/deploy.sh` after it copies the repository's `data/Person_*`, `data/Sales_*`
# and `data/IoT_DeviceRegistry_delta` Delta folders into the lakehouse's `Files/raw/`, and as
# the first activity of `nb_orchestrate`; the `tables` parameter limits it to some of them.
# Each folder becomes a landing table in one overwrite, so a table is either the old snapshot
# or the new one, never half of it. Also creates the `landing`, `bronze`, `stage` and
# `control` schemas and the `control.load_runs` table that `load_entity` appends to.

# PARAMETERS CELL ********************

# Raw folder names to seed, such as IoT_DeviceRegistry, case-insensitive, separated by commas,
# semicolons or spaces (scripts/deploy.sh passes semicolons: fab job run -P separates
# parameters with commas); empty seeds them all.
tables = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ruff: noqa: F821, E402 - spark is a Fabric notebook global; the parameters cell comes before the imports

import os
import re

only = {name.lower() for name in re.split(r"[,;\s]+", tables) if name}
# The default lakehouse's Files, mounted on the driver for listing; Spark reads the same
# folders by their relative path.
raw_mount, raw_dir = "/lakehouse/default/Files/raw", "Files/raw"

for schema in ("landing", "bronze", "stage", "control"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema}`")

spark.sql("""
    CREATE TABLE IF NOT EXISTS `control`.`load_runs` (
        entity STRING,
        correlation_id STRING,
        run_date STRING,
        status STRING,
        early_exit BOOLEAN,
        early_exit_reason STRING,
        inserted_count BIGINT,
        updated_count BIGINT,
        duration_seconds DOUBLE,
        error_message STRING,
        recorded_at TIMESTAMP,
        hwm_value_before STRING,
        hwm_value_after STRING
    ) USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

folders = sorted(
    name
    for name in (os.listdir(raw_mount) if os.path.isdir(raw_mount) else [])
    # The IoT folder by its exact name, as deploy.sh uploads it: an older upload of a larger
    # variant may still sit in Files/raw.
    if os.path.isdir(f"{raw_mount}/{name}")
    and (name.startswith(("Person_", "Sales_")) or name == "IoT_DeviceRegistry")
)
if not folders:
    raise FileNotFoundError(
        f"seed_landing: no Person_*, Sales_* or IoT_DeviceRegistry folder under {raw_dir}; run scripts/deploy.sh first"
    )
if only:
    unknown = only - {folder.lower() for folder in folders}
    if unknown:
        raise ValueError(f"seed_landing: tables {sorted(unknown)} not under {raw_dir}; found {folders}")
    folders = [folder for folder in folders if folder.lower() in only]

for folder in folders:
    table = f"`landing`.`{folder.lower()}`"
    # Read as Delta, never as a parquet folder: data/Person_Person also holds a stray
    # persons.parquet that the Delta log does not list, and reading the folder as parquet
    # would load its rows twice.
    df = spark.read.format("delta").load(f"{raw_dir}/{folder}")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    print(f"seeded {table} from {raw_dir}/{folder}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
