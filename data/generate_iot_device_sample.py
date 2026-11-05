"""
generate_iot_device_sample.py — Generate a synthetic IoT device registry sample.

Standalone dev-tool script (not a spark_quick_start library feature): produces
a dimension-style dataset, one row per device, written to Parquet (default) or
Delta format.

Generation and writing are streamed in bounded-size batches (--batch-size) so
peak memory stays roughly constant regardless of --rows — at most one batch's
worth of rows is ever held in memory at a time, whether writing a thousand
rows or a hundred million.

The Parquet path is pure pandas/pyarrow — no Spark needed. The Delta path
lazily starts a local PySpark + delta-spark session only when --format delta
is requested, set up as the reference notebooks set up theirs.

Output defaults to this script's own folder (the repository's data/), where
the IoT reference notebook reads data/IoT_DeviceRegistry_delta.

Usage::

    python generate_iot_device_sample.py --rows 10000
    python generate_iot_device_sample.py --rows 500 --output /tmp/devices.parquet --seed 7
    python generate_iot_device_sample.py --rows 1000000 --format delta --partitions 8
    python generate_iot_device_sample.py --rows 5000000 --format delta --partitions 4 --batch-size 200000
    python generate_iot_device_sample.py --rows 1000 --device-id-prefix DEV2
    python generate_iot_device_sample.py --rows 1000 --device-id-prefix DEV2 --modified-from "2026-10-06 12:00:00"

    # A base Delta table, then rounds of new devices added to it
    python generate_iot_device_sample.py --rows 40000000 --format delta --partitions 4
    python generate_iot_device_sample.py --rows 10000 --format delta --append
    # ... and a round that also changes 100 existing devices (new SCD2 versions)
    python generate_iot_device_sample.py --rows 1000 --update 100 --format delta --append

modified_at is the watermark column (hwm_context.hwm_column_name): non-null,
drawn between the row's install_date and the moment the run started (UTC).
The incremental filter is strict (modified_at > hwm_value), so a round of new
rows must land wholly after the previous max(modified_at). --append does that
by itself, reading the table's max(modified_at) and continuing its device
numbers; for Parquet output, pass the previous max as --modified-from.

An --append round only adds Parquet files and _delta_log commits, so copying
the table folder to the cloud again moves only what the round added.
"""
from __future__ import annotations

import argparse
import random
import string
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

DEVICE_TYPES = [
    "Thermostat", "SmokeDetector", "WaterLeakSensor",
    "MotionSensor", "SmartPlug", "DoorSensor",
]
MANUFACTURERS = ["Nexora", "Vantix", "Corelight", "Hearthly", "Sentryon"]
STATUSES = ["Active", "Inactive", "Maintenance", "Decommissioned"]
LOCATIONS = [
    "Kitchen", "Living Room", "Bedroom", "Garage", "Basement",
    "Hallway", "Office", "Attic", "Front Door", "Backyard",
]

_INSTALL_WINDOW_START = datetime(2019, 1, 1)
_INSTALL_WINDOW_END = datetime(2026, 1, 1)

# Explicit pyarrow schema, reused for every batch. Needed because
# pa.Table.from_pandas() without an explicit schema infers timestamp[ns] for
# last_seen_at specifically when a batch's None values mix with Timestamp
# objects — Spark's Parquet reader only supports [us]/[ms] precision and
# raises "Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))" otherwise.
PARQUET_SCHEMA = pa.schema([
    pa.field("device_id", pa.string(), nullable=False),
    pa.field("device_type", pa.string(), nullable=False),
    pa.field("manufacturer", pa.string(), nullable=False),
    pa.field("firmware_version", pa.string(), nullable=False),
    pa.field("install_date", pa.timestamp("us"), nullable=False),
    pa.field("location", pa.string(), nullable=False),
    pa.field("status", pa.string(), nullable=False),
    pa.field("last_seen_at", pa.timestamp("us"), nullable=True),
    pa.field("modified_at", pa.timestamp("us"), nullable=False),
])


def _random_firmware_version(rng: random.Random) -> str:
    return f"{rng.randint(1, 5)}.{rng.randint(0, 20)}.{rng.randint(0, 9)}"


def _random_datetime(rng: random.Random, start: datetime, end: datetime) -> datetime:
    delta = end - start
    offset_seconds = rng.uniform(0, delta.total_seconds())
    return start + timedelta(seconds=offset_seconds)


def _random_datetime_after(rng: random.Random, after: datetime, end: datetime) -> datetime:
    """A datetime in (after, end], in whole microseconds.

    Drawn as an integer microsecond offset so the strict lower bound survives
    the datetime64[us] cast: a float offset could round back onto `after`, and
    a row equal to the watermark is skipped by the strict hwm filter.
    """
    span_us = (end - after) // timedelta(microseconds=1)
    return after + timedelta(microseconds=rng.randint(1, span_us))


def _utc_now() -> datetime:
    """The run time as naive UTC at microsecond precision, like the other timestamp columns."""
    return datetime.now(UTC).replace(tzinfo=None)


def _generate_batch(
    rng: random.Random, start_index: int, batch_rows: int, id_width: int, prefix: str,
    modified_from: datetime | None, modified_to: datetime,
) -> pd.DataFrame:
    """Generate one batch: device_id values start_index..start_index+batch_rows-1.

    Uses the caller's shared rng instance so the random sequence is identical
    regardless of how generation is chunked — batch boundaries are invisible
    to the RNG stream, which is what makes --seed reproducible independent
    of --batch-size. modified_at is reproducible only for a fixed modified_to.
    """
    records = []
    for i in range(start_index, start_index + batch_rows):
        device_type = rng.choice(DEVICE_TYPES)
        install_date = _random_datetime(rng, _INSTALL_WINDOW_START, _INSTALL_WINDOW_END)
        status = rng.choices(STATUSES, weights=[70, 10, 10, 10], k=1)[0]
        last_seen_at = (
            None if status == "Decommissioned"
            else _random_datetime(rng, install_date, _INSTALL_WINDOW_END)
        )
        # Strictly after modified_from when given, and never before the install.
        modified_after = install_date if modified_from is None else max(install_date, modified_from)
        records.append({
            "device_id": f"{prefix}-{i:0{id_width}d}-{''.join(rng.choices(string.ascii_uppercase, k=3))}",
            "device_type": device_type,
            "manufacturer": rng.choice(MANUFACTURERS),
            "firmware_version": _random_firmware_version(rng),
            "install_date": install_date,
            "location": rng.choice(LOCATIONS),
            "status": status,
            "last_seen_at": last_seen_at,
            "modified_at": _random_datetime_after(rng, modified_after, modified_to),
        })

    df = pd.DataFrame.from_records(records)
    for col in ("install_date", "last_seen_at", "modified_at"):
        df[col] = df[col].astype("datetime64[us]")
    return df


def generate_device_registry_batches(
    rows: int, batch_size: int, seed: int | None = None, prefix: str = "DEV",
    modified_from: datetime | None = None, modified_to: datetime | None = None,
    start_index: int = 1, id_width: int | None = None,
) -> Iterator[pd.DataFrame]:
    """Yield successive device-registry batches covering device numbers
    start_index..start_index+rows-1.

    Batches are contiguous and non-overlapping by construction: start
    accumulates monotonically and is never reset, so no two batches can ever
    claim the same device_id range. id_width is computed once for the whole
    run (not per batch), keeping zero-padding consistent so no two ids can
    collide via different padding widths either. An --append run passes the
    existing table's next number and width, so its ids continue the table's.

    modified_to defaults to the run time (UTC), taken once so every batch
    shares the same upper bound; modified_from, when given, must be before it.
    """
    modified_to = modified_to or _utc_now()
    if modified_from is not None and modified_from >= modified_to:
        raise ValueError(f"modified_from {modified_from} must be before modified_to {modified_to}")
    if rows <= 0:
        return
    rng = random.Random(seed)  # created once — never move this inside the loop
    end = start_index + rows - 1
    id_width = max(id_width or 6, len(str(end)))

    start = start_index
    while start <= end:
        batch_rows = min(batch_size, end - start + 1)
        yield _generate_batch(rng, start, batch_rows, id_width, prefix, modified_from, modified_to)
        start += batch_rows


def generate_device_registry(
    rows: int, seed: int | None = None, prefix: str = "DEV",
    modified_from: datetime | None = None, modified_to: datetime | None = None,
) -> pd.DataFrame:
    """Backward-compatible, unbounded convenience wrapper.

    Materializes the full DataFrame in memory in one batch — fine for small
    rows or ad-hoc/test use, but callers generating large datasets should use
    generate_device_registry_batches() directly instead (main() does).
    """
    if rows <= 0:
        return pd.DataFrame(columns=[f.name for f in PARQUET_SCHEMA])
    return pd.concat(
        list(generate_device_registry_batches(
            rows, batch_size=rows, seed=seed, prefix=prefix,
            modified_from=modified_from, modified_to=modified_to,
        )),
        ignore_index=True,
    )


def _verify_batch_ids_unique(batch_df: pd.DataFrame) -> None:
    """Cheap per-batch uniqueness check (bounded by batch size, not full dataset).

    Cross-batch uniqueness needs no runtime check: it's a static invariant of
    generate_device_registry_batches's contiguous, disjoint id ranges (see its
    docstring) — accumulating a full-run set of ids to double-check would
    reintroduce the O(rows) memory footprint this batching exists to avoid.
    """
    if not batch_df["device_id"].is_unique:
        dupes = batch_df.loc[batch_df["device_id"].duplicated(), "device_id"].tolist()
        raise ValueError(f"device_id is not a valid primary key — duplicates found: {dupes}")


def write_parquet_batches(batches: Iterator[pd.DataFrame], output: Path) -> int:
    """Stream batches into a single physical Parquet file via ParquetWriter.

    Opens the writer once, writes one row-group per batch, closes once —
    at most one batch is ever held in memory. Returns total rows written.

    Note: if a later batch's uniqueness check raises, close() in finally still
    finalizes the file, leaving a valid-but-incomplete Parquet file rather than
    no file at all. Acceptable for a dev tool; not cleaned up automatically.
    """
    output.parent.mkdir(parents=True, exist_ok=True)
    total_rows = 0
    writer = pq.ParquetWriter(str(output), schema=PARQUET_SCHEMA)
    try:
        for batch_df in batches:
            _verify_batch_ids_unique(batch_df)
            table = pa.Table.from_pandas(batch_df, schema=PARQUET_SCHEMA, preserve_index=False)
            writer.write_table(table)
            total_rows += len(batch_df)
    finally:
        writer.close()
    return total_rows


def _spark_session_for_delta():
    """Start a local PySpark session configured for Delta writes.

    Mirrors the Spark session cell of reference_design/local/NB_BI_LoadEntity_*_Gen3:
    configure_spark_with_delta_pip picks the Delta package matching the installed
    delta-spark and this Spark's Scala build, rather than a hard-coded JAR that
    breaks on the next Spark upgrade. On Windows, HADOOP_HOME must point at a
    folder whose bin/ holds winutils.exe and hadoop.dll.
    """
    import os

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
        .appName("generate_iot_device_sample")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # The timestamps are UTC: a UTC session reads and shows them without a
        # shift by the machine's time zone.
        .config("spark.sql.session.timeZone", "UTC")
    )
    if os.name == "nt":
        hadoop_home = os.environ.get("HADOOP_HOME")
        if not hadoop_home:
            raise OSError("Set HADOOP_HOME to a folder whose bin/ holds winutils.exe and hadoop.dll.")
        hadoop_bin = os.path.join(hadoop_home, "bin")
        if hadoop_bin not in os.environ.get("PATH", "").split(os.pathsep):
            os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")
        builder = (
            builder
            .config("spark.hadoop.io.native.lib.available", "false")
            .config("spark.driver.extraLibraryPath", hadoop_bin)
            .config("spark.executor.extraLibraryPath", hadoop_bin)
        )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def _empty_spark_schema():
    """Spark StructType mirroring PARQUET_SCHEMA, for the --rows 0 Delta case."""
    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

    return StructType([
        StructField("device_id", StringType(), nullable=False),
        StructField("device_type", StringType(), nullable=False),
        StructField("manufacturer", StringType(), nullable=False),
        StructField("firmware_version", StringType(), nullable=False),
        StructField("install_date", TimestampType(), nullable=False),
        StructField("location", StringType(), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("last_seen_at", TimestampType(), nullable=True),
        StructField("modified_at", TimestampType(), nullable=False),
    ])


# PARQUET_SCHEMA with the timestamps marked UTC, for the files the Delta path
# stages: Spark reads such a column as TIMESTAMP (an instant), the type the
# table has always had, rather than TIMESTAMP_NTZ.
_DELTA_STAGING_SCHEMA = pa.schema([
    f.with_type(pa.timestamp("us", tz="UTC")) if pa.types.is_timestamp(f.type) else f
    for f in PARQUET_SCHEMA
])


def read_append_point(spark, output: Path, prefix: str) -> tuple[int, int | None, datetime | None]:
    """Where an --append run continues the Delta table at `output`.

    Returns (next device number for `prefix`, its zero-padded width, the
    table's max(modified_at) as naive UTC). One aggregation over the table: a
    full scan, which is the cost of not asking the caller for these values.
    A table without modified_at predates the watermark and is refused: its
    rows have nothing an incremental load could filter on.
    """
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    if not DeltaTable.isDeltaTable(spark, str(output)):
        raise ValueError(f"--append: no Delta table at {output}; create it first with a run without --append")
    df = spark.read.format("delta").load(str(output))
    if "modified_at" not in df.columns:
        raise ValueError(
            f"--append: the table at {output} has no modified_at column (it predates the watermark); "
            "regenerate it with a run without --append"
        )
    # \Q..\E quotes the prefix literally inside the Java regex.
    number = F.regexp_extract("device_id", rf"^\Q{prefix}\E-(\d+)-[A-Z]{{3}}$", 1)
    row = df.agg(
        F.max(F.when(number != "", number.cast("long"))).alias("max_number"),
        F.max(F.when(number != "", F.length(number))).alias("width"),
        # As microseconds since the epoch: PySpark turns a collected TIMESTAMP into the
        # machine's local wall time whatever the session time zone (measured: 6 hours
        # early at UTC-6), which would put the new rows below the watermark.
        F.unix_micros(F.max("modified_at")).alias("max_modified_us"),
    ).first()
    max_us = row["max_modified_us"]
    max_modified_at = None if max_us is None else datetime(1970, 1, 1) + timedelta(microseconds=max_us)
    return (row["max_number"] or 0) + 1, row["width"], max_modified_at


def update_devices(
    spark, output: Path, count: int, seed: int | None, modified_from: datetime, modified_to: datetime,
) -> tuple[int, datetime, datetime]:
    """Give `count` existing devices of the Delta table at `output` a new firmware version.

    Each chosen device gets a new firmware_version and a modified_at in
    (modified_from, modified_to), so an incremental load past the old watermark
    reads it as a changed row: a new SCD2 version. The devices are chosen by a
    hash of device_id salted with `seed`, so a seeded run picks the same ones.

    The new values are staged once as Parquet and merged from there: rand() is
    evaluated once, so the MERGE, which may read its source twice, sees one set
    of values. One MERGE commit. Everything runs in the JVM, as the batch writes
    do (see write_delta_batches). Returns (devices updated, min and max of their
    new modified_at, naive UTC).
    """
    import tempfile

    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    epoch = datetime(1970, 1, 1)
    from_us = (modified_from - epoch) // timedelta(microseconds=1)
    span_us = (modified_to - epoch) // timedelta(microseconds=1) - from_us - 1
    if span_us < 1:
        raise ValueError(f"--update: no time between the table's max(modified_at) {modified_from} "
                         f"and the run time ({modified_to} UTC)")
    salt = seed if seed is not None else random.randrange(2**31)

    def digit(low: int, high: int, k: int):
        return (F.floor(F.rand(salt + k) * (high - low + 1)) + low).cast("int").cast("string")

    with tempfile.TemporaryDirectory(prefix="iot_update_", ignore_cleanup_errors=True) as staging:
        (
            spark.read.format("delta").load(str(output))
            .select("device_id")
            .orderBy(F.xxhash64("device_id", F.lit(salt)))
            .limit(count)
            .select(
                "device_id",
                # As _random_firmware_version draws it.
                F.concat_ws(".", digit(1, 5, 1), digit(0, 20, 2), digit(0, 9, 3)).alias("firmware_version"),
                F.timestamp_micros(
                    F.lit(from_us + 1) + F.floor(F.rand(salt + 4) * span_us).cast("long")
                ).alias("modified_at"),
            )
            .write.parquet(staging + "/updates")
        )
        updates = spark.read.parquet(staging + "/updates")
        row = updates.agg(
            F.count("*").alias("n"),
            # Epoch micros, not TIMESTAMP: see read_append_point.
            F.unix_micros(F.min("modified_at")).alias("min_us"),
            F.unix_micros(F.max("modified_at")).alias("max_us"),
        ).first()
        if row["n"] < count:
            raise ValueError(f"--update {count}: the table at {output} has only {row['n']} devices")
        (
            DeltaTable.forPath(spark, str(output)).alias("t")
            .merge(updates.alias("u"), "t.device_id = u.device_id")
            .whenMatchedUpdate(set={"firmware_version": "u.firmware_version", "modified_at": "u.modified_at"})
            .execute()
        )
    return (
        row["n"],
        epoch + timedelta(microseconds=row["min_us"]),
        epoch + timedelta(microseconds=row["max_us"]),
    )


def write_delta_batches(
    spark, batches: Iterator[pd.DataFrame], output: Path, partitions: int, *, append: bool = False,
) -> int:
    """Stream batches into a Delta table, one commit per batch.

    Without `append`, batch 0 overwrites (resets/creates the table, schema
    included, so a table from before modified_at is replaced too) and every
    later batch appends. With `append`, every batch appends to the table.

    Each batch goes to Spark as a staged Parquet file, never as a pandas
    DataFrame: spark.createDataFrame(pandas) runs Python worker processes,
    which crash on Windows with Spark 4.1 (measured: "Python worker exited
    unexpectedly", with and without Arrow). Reading Parquet stays in the JVM.

    --partitions means "output files per batch" here, not "total files for
    the table" — total files ≈ num_batches × partitions. Simpler than trying
    to hold --partitions as a true total under batching, and predictable: a
    user wanting fewer total files can just pass a smaller --partitions.

    Note: if a later batch's uniqueness check raises, the table is left in a
    queryable-but-truncated state (earlier batches already committed).
    Acceptable for a dev tool; not rolled back automatically.
    """
    import contextlib
    import tempfile

    total_rows = 0
    batch_num = 0
    with tempfile.TemporaryDirectory(prefix="iot_delta_", ignore_cleanup_errors=True) as staging:
        for batch_df in batches:
            _verify_batch_ids_unique(batch_df)
            staged = Path(staging) / f"batch_{batch_num:05d}.parquet"
            pq.write_table(
                pa.Table.from_pandas(batch_df, schema=_DELTA_STAGING_SCHEMA, preserve_index=False),
                str(staged),
            )
            writer = (
                spark.read.schema(_empty_spark_schema()).parquet(staged.as_posix())
                .repartition(partitions)
                .write.format("delta")
            )
            if append or batch_num > 0:
                writer.mode("append").save(str(output))
            else:
                writer.mode("overwrite").option("overwriteSchema", "true").save(str(output))
            with contextlib.suppress(OSError):  # keeps the staging folder at one batch
                staged.unlink()
            total_rows += len(batch_df)
            batch_num += 1

    if batch_num == 0 and not append:
        # --rows 0: no batches were yielded, so there was no "batch 0" to
        # reset the table. Write an empty-but-valid table so an old table
        # at `output` still gets reset, matching the old code's behavior
        # of producing an empty-but-valid file for --rows 0.
        # Built in the JVM: createDataFrame([], schema) runs Python workers too.
        from pyspark.sql import functions as F

        empty_sdf = spark.range(0).select(
            *(F.lit(None).cast(f.dataType).alias(f.name) for f in _empty_spark_schema())
        )
        empty_sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(output))
    return total_rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rows", type=int, required=True,
        help="Number of device rows to generate (dynamic, no default). Must be >= 0.",
    )
    parser.add_argument(
        "--format", choices=["parquet", "delta"], default="parquet",
        help="Output format. 'parquet' writes a single file (no Spark needed); "
             "'delta' writes a Delta table via a local PySpark session.",
    )
    parser.add_argument(
        "--output", type=Path, default=None,
        help="Output path — a file for --format parquet, a directory for "
             "--format delta. Defaults to IoT_DeviceRegistry.parquet or "
             "IoT_DeviceRegistry_delta, depending on --format, next to this script "
             "(the repository's data/ folder).",
    )
    parser.add_argument(
        "--partitions", type=int, default=1,
        help="Number of output files PER BATCH for the Delta table (ignored "
             "for --format parquet, which always writes a single file). "
             "Total files ≈ (rows / batch-size) * partitions.",
    )
    parser.add_argument(
        "--batch-size", type=int, default=200_000,
        help="Rows generated/written per batch, bounding peak memory to roughly "
             "one batch's worth regardless of --rows. Applies to both formats. "
             "For --format delta, prefer at least tens of thousands of rows per "
             "batch — each batch pays a JVM round-trip and a Delta log commit. "
             "Default: 200000.",
    )
    parser.add_argument(
        "--seed", type=int, default=None,
        help="Random seed for reproducible output, independent of --batch-size "
             "(omit for non-deterministic data).",
    )
    parser.add_argument(
        "--device-id-prefix", type=str, default="DEV",
        help="Prefix used in generated device_id values, e.g. 'DEV-0000001-XYZ'. "
             "Useful for keeping separate generation runs' device_ids disjoint "
             "(e.g. when re-staging data against an existing target table). "
             "Default: DEV.",
    )
    parser.add_argument(
        "--modified-from", type=datetime.fromisoformat, default=None,
        help="Exclusive lower bound (UTC) for modified_at, the watermark column, "
             "e.g. '2026-10-06 12:00:00'. Pass the previous run's max(modified_at) "
             "so an incremental load (modified_at > hwm_value) picks up every row "
             "of this run. Default: each row's install_date. Not with --append, "
             "which reads it from the table.",
    )
    parser.add_argument(
        "--append", action="store_true",
        help="Add --rows new devices to the existing Delta table at --output instead "
             "of replacing it (--format delta only). Reads the table's max(modified_at) "
             "and its highest device number for --device-id-prefix, so every new row "
             "is strictly newer than the watermark and continues the numbering.",
    )
    parser.add_argument(
        "--update", type=int, default=0, metavar="M",
        help="With --append: also give M existing devices a new firmware_version and a "
             "modified_at after the table's max(modified_at), so an incremental load reads "
             "them as changed rows (a new SCD2 version). --rows may be 0. Default: 0.",
    )
    args = parser.parse_args()

    if args.rows < 0:
        parser.error("--rows must be >= 0")
    if args.partitions < 1:
        parser.error("--partitions must be >= 1")
    if args.batch_size < 1:
        parser.error("--batch-size must be >= 1")
    if not args.device_id_prefix:
        parser.error("--device-id-prefix must not be empty")
    modified_to = _utc_now()
    if args.modified_from is not None and args.modified_from >= modified_to:
        parser.error(f"--modified-from must be before the run time ({modified_to} UTC)")
    if args.append and args.format != "delta":
        parser.error("--append needs --format delta")
    if args.update < 0:
        parser.error("--update must be >= 0")
    if args.update and not args.append:
        parser.error("--update changes an existing table: it needs --append (and --format delta)")
    if args.append and args.modified_from is not None:
        parser.error("--append reads the watermark from the table; leave out --modified-from")
    if args.partitions != 1 and args.format == "parquet":
        print("Note: --partitions is ignored for --format parquet (always writes a single file).")

    default_name = "IoT_DeviceRegistry.parquet" if args.format == "parquet" else "IoT_DeviceRegistry_delta"
    output = args.output or (Path(__file__).resolve().parent / default_name)

    if args.format == "parquet":
        batches = generate_device_registry_batches(
            args.rows, args.batch_size, seed=args.seed, prefix=args.device_id_prefix,
            modified_from=args.modified_from, modified_to=modified_to,
        )
        total_rows = write_parquet_batches(batches, output)
        print(f"✓ Wrote {total_rows} device rows to: {output}")
        modified_from = args.modified_from
    else:
        spark = _spark_session_for_delta()
        try:
            start_index, id_width, modified_from = 1, None, args.modified_from
            if args.append:
                try:
                    start_index, id_width, modified_from = read_append_point(spark, output, args.device_id_prefix)
                except ValueError as exc:
                    parser.error(str(exc))
                if modified_from is not None and modified_from >= modified_to:
                    parser.error(f"--append: the table's max(modified_at) {modified_from} is not before "
                                 f"the run time ({modified_to} UTC)")
                if args.update:
                    try:
                        updated, first, last = update_devices(
                            spark, output, args.update, args.seed, modified_from, modified_to,
                        )
                    except ValueError as exc:
                        parser.error(str(exc))
                    print(f"✓ Updated {updated} devices: new firmware_version, modified_at in [{first}, {last}] UTC")
                # New rows start after the max read before the update, as the updated ones do.
                print(f"Appending after device number {start_index - 1} and modified_at {modified_from}")
            batches = generate_device_registry_batches(
                args.rows, args.batch_size, seed=args.seed, prefix=args.device_id_prefix,
                modified_from=modified_from, modified_to=modified_to,
                start_index=start_index, id_width=id_width,
            )
            total_rows = write_delta_batches(spark, batches, output, args.partitions, append=args.append)
        finally:
            spark.stop()
        verb = "Appended" if args.append else "Wrote"
        print(f"✓ {verb} {total_rows} device rows to Delta table: {output}")

    print(f"✓ device_id verified unique — valid primary key ({total_rows} distinct values)")
    print(f"✓ modified_at (watermark) in ({modified_from or 'install_date'}, {modified_to}] UTC")


if __name__ == "__main__":
    main()
