# Set up for development

At the end of this guide you have a checkout of this repository in which the tests, the linter
and the type check run, and pass, as they must before you commit. It is for someone changing the library, not using it; to
install the library in a notebook, see [Installation](../../README.md#installation). The commands
are for macOS and Linux shells.

## Before you start

- Git, and access to the repository.
- Python 3.11 or later; CI uses 3.11, the oldest the library supports.
- Java 17 on your `PATH`, for Spark.
- Docker, only for step 5.

## Steps

1. **Clone the repository and create a virtual environment inside it**, then activate it. Branch
   from `main` for your change, never from a `release/*` branch: those are build outputs
   ([release and branching](../../.claude/rules/release-and-branching.md)).

   ```bash
   git clone <repo-url> spark-quick-start
   cd spark-quick-start
   python3.11 -m venv .venv
   source .venv/bin/activate
   ```

   Run every command below with `.venv` active. A `pytest` from another environment, such as
   Anaconda's, can bring a different PySpark, and its Delta tests then fail where the project's
   pass.

2. **Install the library editable, with the `dev` and `local` extras, pinned by the lock.**
   `dev` is pytest, ruff, mypy and the tools the architecture suite uses; `local` is PySpark,
   `delta-spark` and `nbformat`. `requirements.txt` records the exact versions CI installs,
   compiled from `pyproject.toml`; `-c` holds your install to them.

   ```bash
   pip install -e ".[dev,local]" -c requirements.txt
   ```

   Never edit `requirements.txt` by hand; its header gives the command that regenerates it.

3. **Run the fast loop while you work**: the unit tests that start no Spark session, in a few
   seconds. `--no-cov` because the coverage threshold applies to the whole unit suite, not to this
   part of it.

   ```bash
   pytest tests/unit -m "not spark" --no-cov
   ```

   When your change touches the orchestrator, the pipeline factory, a transformer or
   `ExecutionResult`, also run the integration tests, which take a couple of minutes. Step 4
   runs them again.

   ```bash
   pytest tests/integration -n 6 --dist loadfile --no-cov
   ```

4. **Run the gates before you commit**, in this order; each must pass. The first run of a Spark
   test downloads the Delta jars from Maven. `SQS_FAIL_ON_SKIP=1` makes a run fail on a
   skipped test, as CI does: the suite has none, so a skip means Spark or Delta is missing.

   ```bash
   ruff check src tests
   SQS_FAIL_ON_SKIP=1 pytest tests/unit -n 6 --dist loadfile -rs
   SQS_FAIL_ON_SKIP=1 pytest tests/integration -n 6 --dist loadfile --no-cov -rs
   SQS_FAIL_ON_SKIP=1 pytest tests/architecture --no-cov -rs
   scripts/verify_trunk_complete.sh
   python scripts/mypy_baseline.py
   python scripts/check_mermaid.py
   ```

   `-n 6` starts six workers, each its own Spark session of about 1.5 GB; `--dist loadfile` keeps a
   test module on one worker. Measured: going from 4 to 6 workers took the unit and integration
   suites from 219 s to 199 s on 12 cores, and more workers add JVMs for little gain. CI uses
   `-n auto`, which follows the runner's CPUs (2 on the private `ubuntu-latest` today, more on a
   larger runner). The architecture suite runs serially, because xdist cannot carry it. The unit
   run also checks that coverage stays at 85% or more; coverage is measured on the unit suite
   alone. The old gate, one serial `pytest --no-cov` over everything, is given up: it caught a test
   that passes only because another file was imported first, and the split runs do not. It is still
   available, and the way to chase such a failure.

   `mypy_baseline.py` fails only on type errors that are not already recorded in `mypy-baseline.txt`. Its verdict depends
   on the packages installed, and CI type-checks with the `spark` and `runtime` extras instead of
   `local`, so here it warns `environment differs` and can miss an error CI finds. For CI's
   verdict, run it from a second environment made for it, outside the checkout so Git and Docker
   do not pick it up:

   ```bash
   python3.11 -m venv ~/.venvs/spark-quick-start-ci
   ~/.venvs/spark-quick-start-ci/bin/pip install -e ".[dev,spark,runtime]" -c requirements.txt
   ~/.venvs/spark-quick-start-ci/bin/python scripts/mypy_baseline.py
   ```

   The Fabric-only and Databricks-only builds rewrite the checkout, so they run on GitHub, and
   only when started by hand. Start them on `main` before a release, or on your branch after a
   change that could break a platform-filtered tree, such as one to `adapters/__init__.py`:

   ```bash
   gh workflow run platform-builds.yaml -f platform=both --ref main
   ```

   What each gate answers is in [verification](../../.claude/workflow/verification.md), and which suite a new
   test belongs in is in [test classification](../../.claude/rules/test-classification.md).

5. **Optionally, run the suite in Docker, as CI does.** The image installs the lock and fetches
   the Delta jars when it is built, so the tests run with no network. `SQS_FAIL_ON_SKIP=1` turns
   any skipped test into a failure, because the suite has none.

   ```bash
   docker build -t spark-quick-start-test .
   docker run --rm --network none -e SQS_FAIL_ON_SKIP=1 spark-quick-start-test "python3 -m pytest /app/tests/unit -n auto --dist loadfile -rs && python3 -m pytest /app/tests/integration -n auto --dist loadfile --no-cov -rs && python3 -m pytest /app/tests/architecture --no-cov -rs"
   ```

## Check it worked

The runs in step 4 end with no failures and no skips. The number of tests changes with every
test added, so it is not given here; `pytest --collect-only -q` prints today's. Measured on
2026-10-04 on `main`, on macOS with Python 3.11.5 and Java 17, from the repository's `.venv`:
`mypy_baseline.py` warned `environment differs` and reported one new `unused-ignore` in `spark_logging/local_persistence.py`, an environmental difference (this `.venv`
has `delta-spark`, the baseline was recorded without it), so it did not reach the count of recorded
errors that are now fixed. Run it from the second environment of step 4 for that count; it was not
run there for this measurement. Step 5 was not run: it needs a Docker daemon.

## When it goes wrong

**`FAIL Required test coverage of 85.0% not reached` after every test passed.** Coverage is on
for every run, and a part of the suite, such as `tests/architecture` or the fast loop, covers too
little on its own; add `--no-cov` (steps 3 and 4). Only the `pytest tests/unit` run answers to the
threshold.

**Every Spark test skips, with `SparkSession not available in this environment`.** Spark could
not start, usually because Java is missing or not on your `PATH`. Install Java 17 and check
`java -version` in the same shell.

**The Delta tests skip.** Spark could not download the Delta jars, usually because Maven is
unreachable; the rest of the Spark tests still run. Give the machine Maven access and run again, or
use Docker (step 5).

**A Delta test fails in one shell and passes in another.** One of them is not using `.venv`
(step 1).

## See also

- [Repository layout](../architecture/repository-layout.md): what each directory holds.
- [Test without a cluster](test-without-a-cluster.md): testing a load that uses the library.
- [Architecture overview](../architecture/overview.md): how the library is built.
