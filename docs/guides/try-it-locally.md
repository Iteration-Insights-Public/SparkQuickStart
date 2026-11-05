# Try it locally in 15 minutes

At the end of this guide you have run two SCD2 loads on your own machine, with no cloud account: a
first load of 67 rows of the AdventureWorks `TransactionHistory` table into a Delta target, and a
second load that changes one row, so the target keeps both versions of it. It is for anyone
evaluating the library. Everything runs from a clone of the repository, with the sample data it
ships and a local Spark session.

## Before you start

- Java 17 is installed, and `java -version` prints it. Spark needs it.
- Python 3.11, the version the repository's CI runs, and `git`. The local extra pins
  PySpark 3.5, which does not support every newer Python.
- **Windows only:** `HADOOP_HOME` points at a folder whose `bin/` holds `winutils.exe` and
  `hadoop.dll` matching Spark's Hadoop version. Spark reaches the local file system through them.
- An internet connection for the first run: Spark downloads the Delta Lake package once and
  caches it.

## Steps

1. **Clone the repository and install what the notebook needs.** That is PySpark, `delta-spark`
   and `jupyter`, the versions the `local` extra pins; the library itself is not installed.

   ```bash
   git clone https://github.com/Iteration-Insights-Enterprise/FabricQuickstart.git spark-quick-start
   cd spark-quick-start
   python -m venv .venv
   source .venv/bin/activate
   pip install "pyspark>=3.5,<4" "delta-spark>=3.0,<4" jupyter
   ```

   On Windows, activate with `.venv\Scripts\activate`.

2. **Open the reference notebook.**

   ```bash
   jupyter lab reference_design/local/NB_BI_LoadEntity_Gen3.ipynb
   ```

   It loads the library as a copy of `src/` put on `sys.path`, one of the two ways it ships. The
   load context in its second code cell, under the run switches, is the whole description of the
   load: the source, a Declarative Query Definition that builds the row key, the SCD2 settings and
   the target path.

3. **Run all cells.** Section 5 runs the first load and prints its result:

   ```
   ExecutionResult(status='success', inserted=67, updated=0, source=67, ...)
   ```

   The rows come from the parquet file under `data/Production_TransactionHistory/`, filtered by
   the watermark in `hwm_context` to those modified after 2014-08-01. The cell after it shows the
   newest current rows of the target, each with its SCD columns: `scdVersion` 1,
   `scdCurrentFlag` `Y`, and an effective-from of the load's time.

4. **Read the second load.** Section 6 loads again, with one extra step after the source that
   changes `ActualCost` on the row `AW2022~213376`. Its result reports the change:

   ```
   ExecutionResult(status='success', inserted=1, updated=1, source=67, ...)
   ```

   One row was updated, the old version closed, and one inserted, the new version.

## Check it worked

The last cell shows the history of `AW2022~213376`: version 1 with the original `ActualCost`,
`scdCurrentFlag` `N`, and version 2 with `99.9900` and `scdCurrentFlag` `Y`. Version 2 starts at
the second load's time, and version 1's effective-to is one microsecond before it.

## When it goes wrong

**`[JAVA_GATEWAY_EXITED] Java gateway process exited before sending its port number.`** Spark
cannot start Java; the terminal running Jupyter says why, for example `JAVA_HOME is not set` or
`Unable to locate a Java Runtime`. Install Java 17 and set `JAVA_HOME` to it, then restart the
notebook's kernel.

**`Set HADOOP_HOME to a folder whose bin/ holds winutils.exe and hadoop.dll.`** On Windows, Spark
reaches the local file system through Hadoop's native helpers. Download the ones that match
Spark's Hadoop version, point `HADOOP_HOME` at their folder, and restart the kernel.

**Section 6 updates nothing.** You changed a column that is not hashed. `Quantity` is
listed in `exclude_from_hash`, so changing it makes no new version; change `ActualCost` or another
hashed column.

**On a second run, section 5 reports `inserted=1, updated=1`, and the key has four versions.** The
target persists between runs, under `data/Target/`, so section 5 compares the unpatched source
with the patched target and adds version 3, and section 6 adds version 4. Delete `data/Target/`
and `data/Staging/`, which git ignores, to start from an empty target.

## See also

- [Run your first load](run-your-first-load.md), the same load with a staging notebook, on Fabric,
  Databricks or locally
- [Load a table with a DQD](load-a-table-with-a-dqd.md), the source this notebook uses
- [Load context reference](../reference/load-context.md), every field of the notebook's load
  context
- [Test without a cluster](test-without-a-cluster.md), the next step: run your own loads under
  pytest
