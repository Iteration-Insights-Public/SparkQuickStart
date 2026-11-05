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

# # Generate the IoT landing data
#
# Makes the IoT device registry in the lakehouse's `Files/raw/IoT_DeviceRegistry` (Delta), where
# `seed_landing` reads it: the Fabric design's only source of IoT data. It has the columns and
# value ranges of the repository's `data/generate_iot_device_sample.py`, in logic of its own.
#
# - `mode=initial`: `rows` devices, when the folder holds no table yet. An existing table is left
#   as it is, so a rerun of the demo loads nothing new.
# - `mode=changes`: a round. `update` existing devices get a new `firmware_version`, and `new`
#   devices are added after the highest device number; every changed or new row has a
#   `modified_at` after the table's max, the last watermark, so the incremental IoT load reads
#   `inserted_count` new + update and `updated_count` update.
#
# Every random value is a hash of the row's key and the seed, not `rand()`: it is the same
# however often Spark evaluates it, so the MERGE needs no staged source. Pure Spark: no pandas
# and no Python workers.

# PARAMETERS CELL ********************

mode = "initial"
# mode=initial: devices to create.
rows = 100000
# mode=changes: devices to add, and existing devices to change.
new = 1000
update = 100
seed = 1
target = "Files/raw/IoT_DeviceRegistry"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ruff: noqa: F821, E402 - spark is a Fabric notebook global; the parameters cell comes before the imports

import string
from datetime import UTC, datetime, timedelta

from delta.tables import DeltaTable
from pyspark.sql import Column
from pyspark.sql import functions as F

# The value lists of data/generate_iot_device_sample.py.
DEVICE_TYPES = ["Thermostat", "SmokeDetector", "WaterLeakSensor", "MotionSensor", "SmartPlug", "DoorSensor"]
MANUFACTURERS = ["Nexora", "Vantix", "Corelight", "Hearthly", "Sentryon"]
LOCATIONS = ["Kitchen", "Living Room", "Bedroom", "Garage", "Basement",
             "Hallway", "Office", "Attic", "Front Door", "Backyard"]
PREFIX = "DEV"

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def micros(moment: datetime) -> int:
    """`moment` as whole microseconds since the epoch."""
    return (moment - EPOCH) // timedelta(microseconds=1)


INSTALL_FROM = micros(datetime(2019, 1, 1, tzinfo=UTC))
INSTALL_TO = micros(datetime(2026, 1, 1, tzinfo=UTC))
# The run time, taken once: the upper bound of every modified_at this run writes.
NOW = micros(datetime.now(UTC))


def draw(key: Column, salt: int, k: int, n) -> Column:
    """A value in [0, n) from the hash of `key`, `salt` and the draw's number `k`."""
    return F.pmod(F.xxhash64(key, F.lit(salt), F.lit(k)), n)


def pick(key: Column, salt: int, k: int, values: list[str]) -> Column:
    return F.element_at(F.array(*map(F.lit, values)), (draw(key, salt, k, len(values)) + 1).cast("int"))


def firmware(key: Column, salt: int) -> Column:
    """`a.b.c`, a in 1-5, b in 0-20, c in 0-9, as the generator draws it."""
    return F.concat_ws(".", (draw(key, salt, 1, 5) + 1).cast("string"),
                       draw(key, salt, 2, 21).cast("string"), draw(key, salt, 3, 10).cast("string"))


def modified_after(key: Column, salt: int, after: Column) -> Column:
    """A modified_at in (after, NOW], in microseconds."""
    return after + 1 + draw(key, salt, 4, F.lit(NOW) - after)


def devices(start: int, count: int, width: int, salt: int, modified_from: int | None):
    """Devices numbered start..start+count-1, with a modified_at after `modified_from` when given."""
    number = F.col("id")
    status_draw = draw(number, salt, 5, 100)
    status = (F.when(status_draw < 70, "Active").when(status_draw < 80, "Inactive")
              .when(status_draw < 90, "Maintenance").otherwise("Decommissioned"))
    install = F.lit(INSTALL_FROM) + draw(number, salt, 6, INSTALL_TO - INSTALL_FROM)
    after = install if modified_from is None else F.greatest(install, F.lit(modified_from))
    letters = F.concat(*(pick(number, salt, 10 + i, list(string.ascii_uppercase)) for i in range(3)))
    return (
        spark.range(start, start + count)
        .withColumn("status", status)
        .withColumn("install_us", install)
        .select(
            F.concat_ws("-", F.lit(PREFIX), F.lpad(number.cast("string"), width, "0"), letters).alias("device_id"),
            pick(number, salt, 7, DEVICE_TYPES).alias("device_type"),
            pick(number, salt, 8, MANUFACTURERS).alias("manufacturer"),
            firmware(number, salt).alias("firmware_version"),
            F.timestamp_micros("install_us").alias("install_date"),
            pick(number, salt, 9, LOCATIONS).alias("location"),
            "status",
            F.when(F.col("status") == "Decommissioned", F.lit(None)).otherwise(
                F.timestamp_micros(F.col("install_us") + draw(number, salt, 13, F.lit(INSTALL_TO) - F.col("install_us")))
            ).alias("last_seen_at"),
            F.timestamp_micros(modified_after(number, salt, after)).alias("modified_at"),
        )
    )


if mode not in ("initial", "changes"):
    raise ValueError(f"iot_generate: mode {mode!r} is not initial or changes")
exists = DeltaTable.isDeltaTable(spark, target)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if mode == "initial":
    if exists:
        print(f"iot_generate: {target} already holds a table; left as it is")
    else:
        devices(1, rows, max(6, len(str(rows))), seed, None).write.format("delta").mode("overwrite").save(target)
        print(f"iot_generate: wrote {rows} devices to {target}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if mode == "changes":
    if not exists:
        raise ValueError(f"iot_generate: no table at {target}; run mode=initial first")
    table = spark.read.format("delta").load(target)
    # One aggregation: where the round continues. As epoch microseconds, never a collected
    # TIMESTAMP, which PySpark turns into the driver's local wall time.
    number = F.regexp_extract("device_id", rf"^{PREFIX}-(\d+)-[A-Z]{{3}}$", 1)
    point = table.agg(
        F.max(F.when(number != "", number.cast("long"))).alias("max_number"),
        F.max(F.when(number != "", F.length(number))).alias("width"),
        F.unix_micros(F.max("modified_at")).alias("max_modified"),
    ).first()
    max_number, width, max_modified = point["max_number"] or 0, point["width"] or 6, point["max_modified"]
    if max_modified >= NOW:
        raise ValueError("iot_generate: the table's max(modified_at) is not before the run time")
    # The append point in the salt: each round with the same seed changes other devices.
    salt = seed * 1_000_003 + max_number

    if update:
        key = F.col("device_id")
        changed = (
            table.select("device_id")
            .orderBy(F.xxhash64(key, F.lit(salt)))
            .limit(update)
            .select("device_id", firmware(key, salt).alias("firmware_version"),
                    F.timestamp_micros(modified_after(key, salt, F.lit(max_modified))).alias("modified_at"))
        )
        (
            DeltaTable.forPath(spark, target).alias("t")
            .merge(changed.alias("u"), "t.device_id = u.device_id")
            .whenMatchedUpdate(set={"firmware_version": "u.firmware_version", "modified_at": "u.modified_at"})
            .execute()
        )
    if new:
        end = max_number + new
        devices(max_number + 1, new, max(width, len(str(end))), salt, max_modified).write.format("delta").mode("append").save(target)
    print(f"iot_generate: changed {update} and added {new} devices after number {max_number}; "
          f"every modified_at after {EPOCH + timedelta(microseconds=max_modified)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
