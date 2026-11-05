# Test without a cluster

At the end of this guide you have pytest tests that run your load end to end on your own machine
or in CI: a local Spark session, Delta tables in a temporary folder, your staging code, and a
check of the table the load wrote. It is for a user of the library who wants to test a load
before it runs on Fabric or Databricks. Staging written as a Python function works on every
platform; a staging notebook can be run as a file only if it is a Fabric notebook saved as
`.ipynb`, because only Fabric's `notebookutils` has a local stand-in.

## Before you start

- Java 17 is installed, and the `local` extra with `pytest`. The extra is `pyspark`,
  `delta-spark` and `nbformat`. The platform release wheels do not carry it, or `LocalAdapter`,
  so install from a checkout of `main`, in its root:

  ```bash
  pip install ".[local]" pytest
  ```

  On a copy of `src/` there are no extras: install `"pyspark>=3.5,<4"`, `"delta-spark>=3,<4"`,
  `"nbformat>=5,<6"` and `pytest` yourself. See [Installation](../../README.md#installation).
- You have a load whose staging you can run; see [Run your first load](run-your-first-load.md)
  and [Write a staging notebook](write-a-staging-notebook.md).

## Steps

1. **Start a Spark session with Delta**, once for the whole test run, in `conftest.py`.
   `configure_spark_with_delta_pip` adds the Delta jars that `delta-spark` matches to your PySpark.
   One shuffle partition keeps small tests fast.

   ```python
   import pytest
   from delta import configure_spark_with_delta_pip
   from pyspark.sql import SparkSession


   @pytest.fixture(scope="session")
   def spark():
       builder = (
           SparkSession.builder.master("local[1]")
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
           .config("spark.sql.shuffle.partitions", "1")
       )
       session = configure_spark_with_delta_pip(builder).getOrCreate()
       yield session
       session.stop()
   ```

2. **Point the load context at a temporary folder.** In `test_customer_load.py`, beside
   `conftest.py`, build the load context with every path under pytest's `tmp_path`, so each test
   starts from empty tables. It is the load context from [Run your first load](run-your-first-load.md), step 3.

   ```python
   import json

   from spark_quick_start.adapters import LocalAdapter
   from spark_quick_start.orchestration import default_pipeline
   from spark_quick_start.orchestration.staging_pipeline import create_staging_main_function


   def load_context(tmp_path):
       columns = [("customer_id", "integer"), ("name", "string")]
       return json.dumps({
           "global_context": {
               "id": "ent_1",
               "entity_name": "Customer",
               "source_system": "CRM",
               "target_table_path": f"{tmp_path}/silver/customer",
               "staging_table_path": f"{tmp_path}/staging/customer",
               "staging_table_schema": {
                   "type": "struct",
                   "fields": [{"name": n, "type": t, "nullable": True, "metadata": {}} for n, t in columns],
               },
           },
           "staging_context": {"stages": [{"nb_name": "NB_Stage_Customer"}]},
           "scd_context": {"business_key_column": "customer_id"},
           "runtime_context": {"correlation_id": "test-1", "pipeline_name": "customer", "application_name": "crm"},
           "logging_context": {"logging_level": "INFO", "log_file_name": "customer_load", "log_file_dir": f"{tmp_path}/logs"},
       })
   ```

3. **Run your staging code.** A load runs each stage by notebook name, so the test tells a
   `LocalAdapter` what to run under that name. Choose one of two ways.

   - **A staging function**, on any platform: wrap it as the notebook would and register it. Here
     the function makes its own rows; yours reads a source, so give it a small one the test
     creates.

     ```python
     def stage_customers(spark):
         def stage(storage_path, staging_context, global_context, notebook_name):
             return spark.createDataFrame(
                 [(1, "Ada"), (2, "Grace")], "customer_id int, name string"
             )

         main = create_staging_main_function(stage, adapter=LocalAdapter(), spark=spark)
         return LocalAdapter(notebook_registry={"NB_Stage_Customer": lambda args: main(args["stage_context_str"]).to_json()})
     ```

   - **A Fabric staging notebook file:** save it as `NB_Stage_Customer.ipynb` in a folder beside
     the tests, here `notebooks/`, and add a fixture to `conftest.py`. `activate` puts a stand-in
     for `notebookutils` in place for the test, and `deactivate` removes it; the fixture yields
     the folder, which step 4 hands to `LocalAdapter(notebooks_dir=...)`.

     ```python
     from pathlib import Path

     from spark_quick_start.local_notebookutils import activate, deactivate


     @pytest.fixture
     def fabric_notebooks():
         folder = Path(__file__).parent / "notebooks"
         activate(notebooks_dir=folder)
         yield str(folder)
         deactivate()
     ```

     The file runs in the test's Python process, cell by cell, with `stage_context_str` set after
     the first code cell, or after the cell tagged `parameters`. Two things a Fabric notebook gets
     for free it must do itself: `spark = SparkSession.builder.getOrCreate()` and
     `import notebookutils`; both work unchanged on Fabric. It ends with
     `notebookutils.notebook.exit(main(stage_context_str).to_json())`, as on Fabric. The stand-in
     has `notebookutils.notebook` only, so a notebook that uses `notebookutils.fs`, secrets or any
     other part fails locally.

4. **Run the load and check what it wrote.** Read the target table, not only the status. The
   first test is for a staging function, the second for a notebook file; keep the one for the way
   you chose in step 3.

   ```python
   def test_first_load_writes_every_customer(spark, tmp_path):
       result = default_pipeline(load_context(tmp_path), spark, adapter=stage_customers(spark)).run()

       assert result.status == "success"
       target = spark.read.format("delta").load(f"{tmp_path}/silver/customer")
       assert sorted(row.name for row in target.collect()) == ["Ada", "Grace"]


   def test_the_staging_notebook_file_loads(spark, tmp_path, fabric_notebooks):
       result = default_pipeline(load_context(tmp_path), spark, adapter=LocalAdapter(notebooks_dir=fabric_notebooks)).run()

       assert result.status == "success"
   ```

   To test a second load, run the pipeline again in the same test with new rows, and check the
   versions it wrote.

5. **Test what a failing staging notebook does**, with `MockPlatformAdapter`. It runs nothing: it
   returns the result you give it for a notebook name, and records every call.

   ```python
   from spark_quick_start.adapters import MockPlatformAdapter
   from spark_quick_start.orchestration import ExecutionResult


   def test_a_failed_stage_fails_the_load(spark, tmp_path):
       failed = ExecutionResult(status="error", has_error=True, error_details={"error_message": "source unreachable"})
       adapter = MockPlatformAdapter(notebook_results={"NB_Stage_Customer": failed.to_json()})

       result = default_pipeline(load_context(tmp_path), spark, adapter=adapter).run()

       assert result.status == "error"
       assert "Staging failed: 1 notebook(s)" in result.error_details["error_message"]
       assert adapter.run_calls[0][0] == "NB_Stage_Customer"
   ```

   Use it only for a failing stage. For a notebook it has no result for, it returns success
   without writing a staging table, so the load then fails on a schema mismatch.

## Check it worked

`pytest` passes. Measured locally with these files, a notebook file written as step 3 says,
Java 17 and PySpark 3.5: the three tests passed. In a separate run the notebook file loaded twice
in a row, the second load writing nothing new.

```bash
pytest
```

## When it goes wrong

A notebook's own error comes back inside `result.error_details["error_message"]`, after
`Staging failed: 1 notebook(s)` and the notebook's name.

**`NameError` and `name 'spark' is not defined`.** The notebook file uses the `spark` a Fabric
notebook gets for free; add `spark = SparkSession.builder.getOrCreate()` (step 3).

**`NameError` and `name 'notebookutils' is not defined`.** Add `import notebookutils` to the
notebook (step 3).

**`AttributeError` and `'types.SimpleNamespace' object has no attribute 'fs'`.** The notebook uses
a part of `notebookutils` the stand-in does not have (step 3). Test that notebook's staging
function instead.

**`[PATH_NOT_FOUND] Path does not exist` naming the stage's staging table.** The notebook file
never called `main`, and did not end with `notebookutils.notebook.exit(...)`; the stand-in reports
a notebook that does not exit as succeeded, whatever it wrote, unless it left an
`ExecutionResult` in a variable named `result` (step 3).

**`KeyError` and `no callable registered for notebook`.** The name in
`staging_context.stages` has no registered function and no `.ipynb` of that name in
`notebooks_dir`.

**`getOrCreate()` raises an ivy error naming `io.delta`.** `configure_spark_with_delta_pip` has
Spark download the Delta jars from Maven when the session starts, so a runner with no Maven access,
behind a proxy or offline, cannot start it (step 1); this repository's own test setup records the
same failure, which was not reproduced for this page. Give the runner access through a proxy, or
point `spark.jars.ivy` at a cache that already holds the jars.

**`java.lang.OutOfMemoryError: Java heap space` on a large load.** In local mode the driver and
every task share one JVM heap, 1 GB unless you set it, and a large load's aggregations do not fit
in it. Add `.config("spark.driver.memory", "4g")` to the builder (step 1). The heap is fixed when
the JVM starts, so restart the Python process or the notebook kernel before the next
`getOrCreate()`; on a running session the setting does nothing. Measured locally on a first load
of 40 million rows on 12 cores with `local[*]`: with 1 GB it failed in `CandidateGuardTransformer`,
with 2 GB it failed or passed from run to run, and with 4 GB it loaded every row in 50 to 63
seconds.

**`Schema mismatch for '<staging_table_path>__NB_Stage_Customer'` listing every column as
missing.** No staging table was written: a `MockPlatformAdapter` returned its default success
(step 5).

## See also

- [Platform-adapter reference](../reference/platform-adapters.md): `LocalAdapter`,
  `MockPlatformAdapter` and what each records.
- [Run your first load](run-your-first-load.md): the same load on Fabric and Databricks.
- [Inspect a run](inspect-a-run.md): reading the result a test asserts on.
