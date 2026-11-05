# Databricks notebook source
# MAGIC %md
# MAGIC # Seed landing
# MAGIC
# MAGIC Run by the `seed_landing` job, which `scripts/deploy.sh` starts after copying the
# MAGIC repository's `data/Person_*`, `data/Sales_*` and `data/IoT_DeviceRegistry_delta` Delta folders into the
# MAGIC landing raw volume; the `tables` parameter limits it to some of them. Each folder
# MAGIC becomes a landing table in one overwrite, so a table is either the old snapshot or the
# MAGIC new one, never half of it. Also creates the control schema's `load_runs` table that
# MAGIC `load_entity` appends to.

# COMMAND ----------

# ruff: noqa: F821 - spark and dbutils are Databricks notebook globals

import re

PARAMETERS = ("catalog", "landing_schema", "raw_volume", "control_schema")
for name in (*PARAMETERS, "tables"):
    dbutils.widgets.text(name, "")
params = {name: dbutils.widgets.get(name) for name in PARAMETERS}
missing = [name for name, value in params.items() if not value]
if missing:
    raise ValueError(f"seed_landing: task parameters not set: {missing}")
# Optional: raw folder names to seed, case-insensitive, separated by commas, semicolons or
# spaces (scripts/deploy.sh passes semicolons: `bundle run --params` splits on commas);
# empty seeds them all.
only = {name.lower() for name in re.split(r"[,;\s]+", dbutils.widgets.get("tables")) if name}

catalog, landing, control = params["catalog"], params["landing_schema"], params["control_schema"]
raw_dir = f"/Volumes/{catalog}/{landing}/{params['raw_volume']}"

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{catalog}`.`{control}`.`load_runs` (
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

# COMMAND ----------

folders = sorted(
    entry.name.rstrip("/")
    for entry in dbutils.fs.ls(raw_dir)
    # The IoT folder by its exact name, as deploy.sh uploads it: an older upload of a larger
    # variant may still sit in the volume.
    if entry.name.startswith(("Person_", "Sales_"))
    or entry.name.rstrip("/") == "IoT_DeviceRegistry"
)
if not folders:
    raise FileNotFoundError(
        f"seed_landing: no Person_*, Sales_* or IoT_DeviceRegistry folder under {raw_dir}; run scripts/deploy.sh first"
    )
if only:
    unknown = only - {folder.lower() for folder in folders}
    if unknown:
        raise ValueError(
            f"seed_landing: tables {sorted(unknown)} not under {raw_dir}; found {folders}"
        )
    folders = [folder for folder in folders if folder.lower() in only]

for folder in folders:
    table = f"`{catalog}`.`{landing}`.`{folder.lower()}`"
    # Read as Delta, never as a parquet folder: data/Person_Person also holds a stray
    # persons.parquet that the Delta log does not list, and reading the folder as parquet
    # would load its rows twice.
    df = spark.read.format("delta").load(f"{raw_dir}/{folder}")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    print(f"seeded {table} from {raw_dir}/{folder}")
