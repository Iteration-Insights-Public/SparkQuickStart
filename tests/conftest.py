"""conftest.py — pytest-collection-time patch for SparkSession.Builder.master.

On Databricks, a SparkSession is already active (often Spark Connect-backed)
before any test module runs. The ``spark`` fixture below builds one via
``SparkSession.builder.master("local[N]")...getOrCreate()``, which is correct
for local runs but raises ``CANNOT_CONFIGURE_SPARK_CONNECT_MASTER`` on
Databricks, since setting ``spark.master`` is incompatible with an
already-active Spark Connect session.

Patching ``.master()`` to a no-op whenever a session is already active lets
that ``.master(...).getOrCreate()`` chain work in both places: locally it
behaves exactly as before (no active session yet, so the real master is set);
on Databricks it skips setting spark.master and ``.getOrCreate()`` simply
returns the already-active session.

Also exports ``is_serverless_session()`` — tests that build a ``RuntimeContext``
for transformers with a serverless-aware ``.cache()``/``.unpersist()`` branch
(``CandidateGuardTransformer``, ``assert_no_dups``) should pass its result as
``"serverless"`` so the branch actually taken in the test matches the real
compute environment (Spark Connect on Databricks rejects ``.cache()`` outright).
"""
from __future__ import annotations

import os

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.utils import is_remote

_original_master = SparkSession.Builder.master


def _master_or_reuse_active(self: SparkSession.Builder, master: str) -> SparkSession.Builder:
    if SparkSession.getActiveSession() is not None:
        return self
    return _original_master(self, master)


SparkSession.Builder.master = _master_or_reuse_active


# ---------------------------------------------------------------------------
# Session-creation counter.
#
# Spark is a per-JVM singleton, so builder.getOrCreate() hands back the
# existing session rather than building a second one — which means a module
# that builds its own session does not fail loudly, it silently gets someone
# else's, with its static configs dropped on the floor. That is exactly the
# bug the `spark` fixture below was introduced to fix.
#
# Counting actual constructions turns "there is only one session" from a
# property we believe into one the suite asserts. See
# tests/unit/test_suite_invariants.py.
# ---------------------------------------------------------------------------
_original_get_or_create = SparkSession.Builder.getOrCreate
_sessions_created: list[int] = []


def _counting_get_or_create(self: SparkSession.Builder) -> SparkSession:
    before = SparkSession.getActiveSession()
    session = _original_get_or_create(self)
    if before is None or session is not before:
        _sessions_created.append(id(session))
    return session


SparkSession.Builder.getOrCreate = _counting_get_or_create


@pytest.fixture
def sessions_created() -> list[int]:
    """Identities of every SparkSession actually constructed in this process.

    A fixture rather than an importable helper: `from tests.conftest import ...`
    only resolves when the repo root happens to be on sys.path, which is true
    under `python -m pytest` and false under a bare `pytest`. Fixtures are
    resolved by pytest itself and work under either.
    """
    return list(_sessions_created)


# Under pytest-xdist the controller builds no session and a worker's `session.exitstatus` never
# reaches the run's exit code (xdist records the hook's `exitstatus` argument first), so each
# worker reports its count through `config.workeroutput` and the controller fails the run.
_worker_session_counts: list[int] = []


@pytest.hookimpl(optionalhook=True)
def pytest_testnodedown(node: object, error: object) -> None:
    """Controller side of the one-session backstop: collect each worker's construction count."""
    output = getattr(node, "workeroutput", None) or {}
    _worker_session_counts.append(int(output.get("sessions_created", 0)))


def pytest_sessionfinish(session: object, exitstatus: object) -> None:
    """Fail the run if more than one SparkSession was ever constructed in one process.

    tests/unit/test_suite_invariants.py asserts the same thing, but it can only
    see constructions that happened before it ran — a module that builds a
    second session later in the run would slip past it. This hook is the
    end-of-run backstop, and it is where a second construction is actually
    provable. Under xdist each worker reports its count and the controller fails.
    """
    workeroutput = getattr(session.config, "workeroutput", None)
    if workeroutput is not None:  # an xdist worker: report, the controller decides
        workeroutput["sessions_created"] = len(_sessions_created)
        return
    created = max([len(_sessions_created), *_worker_session_counts])
    if created > 1:
        session.exitstatus = 1
        reporter = session.config.pluginmanager.get_plugin("terminalreporter")
        message = (
            f"{created} SparkSessions were constructed in one process; exactly one "
            "is allowed. Take the session-scoped `spark` fixture instead of calling "
            "SparkSession.builder.getOrCreate()."
        )
        if reporter is not None:
            reporter.write_line(message, red=True)
        else:  # pragma: no cover — terminalreporter is disabled (-p no:terminal)
            print(message)

    # SQS_FAIL_ON_SKIP=1 turns a skip into a failure (F39). The suite has no skips, so one
    # means a capability went missing — Delta without its JARs, an import that degraded —
    # and without this it would ride on a green run, which is how F30's "4 skipped" hid for
    # months. xfail is reported separately and does not count. CI sets it; locally it is off.
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    skipped = len(reporter.stats.get("skipped", ())) if reporter is not None else 0
    if os.environ.get("SQS_FAIL_ON_SKIP") == "1" and skipped and session.exitstatus == 0:
        session.exitstatus = 1
        reporter.write_line(
            f"{skipped} test(s) skipped with SQS_FAIL_ON_SKIP=1 — a skip here means a "
            "capability is missing; see the skip reasons above (-rs).",
            red=True,
        )


def is_serverless_session() -> bool:
    """True when the active Spark session is Spark Connect-backed (e.g. Databricks serverless).

    ``.cache()``/``.persist()``/``.unpersist()`` raise ``NOT_SUPPORTED_WITH_SERVERLESS``
    under Spark Connect, so any RuntimeContext built for a serverless-aware
    transformer under test must set ``serverless=True`` when this is True —
    otherwise the test exercises the caching branch that's actually broken
    in the environment the test is running in.
    """
    return is_remote()


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> SparkSession:
    """The one SparkSession for the whole test run.

    Every test that needs a real engine takes this fixture rather than building
    its own. Nine test modules used to each call
    ``SparkSession.builder...getOrCreate()`` with their own settings, which is
    not the isolation it looks like: Spark is a per-JVM singleton, so the first
    caller creates the session and every later one gets that same object back.
    What happened to their settings then depended on the setting:

      * Runtime SQL configs (spark.sql.shuffle.partitions,
        spark.sql.debug.maxToStringFields) *are* applied to the live session —
        so they leaked between modules, last writer winning, and a test tuned
        for shuffle.partitions=1 could run under 2 depending on collection order.
      * Static configs are silently dropped. spark.sql.extensions is static, so
        tests/integration/local/test_local_adapter.py's (then tests/local/) Delta configuration was discarded
        whenever any other module had already created the session, leaving its
        Delta tests running against a session with no Delta support.
      * spark.master is fixed at creation, so test_fabric_dqf.py's (removed with DQF, F146) request for
        local[2] was ignored whenever a local[1] session already existed.

    Building it once here makes the configuration explicit and order-independent.
    The values below are the union of what those modules asked for.

    Delta is configured when a real delta-spark is importable, and the session falls
    back to one without it when that configuration cannot be satisfied — see
    ``_with_delta`` below for both halves and why each is needed.
    """
    def make_builder():
        return (
            SparkSession.builder
            .master("local[2]")
            .appName("spark_quick_start_tests")
            # Keep small-input tests deterministic and fast — no 200-way shuffles.
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.default.parallelism", "2")
            .config("spark.sql.debug.maxToStringFields", "100000")
            # A test's Delta table holds a handful of commits, and Delta rebuilds each
            # snapshot with 50 tasks by default: pure scheduling. Measured 2026-10-02:
            # tests/integration 208 s -> 117 s, the whole suite 276 s -> 172 s, same
            # results (F182). The UI and the progress bar are off for the same reason.
            .config("spark.databricks.delta.snapshotPartitions", "1")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            # The status store keeps the last 1000 SQL executions, UI or not, each with
            # its plan text at maxToStringFields above; a test never reads them (F208).
            .config("spark.sql.ui.retainedExecutions", "50")
            # Without this, spark.sql.warehouse.dir defaults to ./spark-warehouse,
            # relative to wherever pytest was started — so anything creating a
            # database or a managed table drops it in the repo root and it outlives
            # the run. An empty spark-warehouse/victim.db from a since-changed
            # delete-guard test sat in the working tree for weeks that way.
            # A temp dir also stops managed-table state leaking between runs.
            .config("spark.sql.warehouse.dir", warehouse)
        )

    warehouse = str(tmp_path_factory.mktemp("spark-warehouse"))

    # Deliberately a guarded import rather than importlib.util.find_spec():
    # find_spec() consults sys.modules first and raises
    # "ValueError: delta.__spec__ is not set" when a module is present but has
    # no __spec__ — exactly the stub case. Eight modules assign sys.modules["delta"]
    # a `types.ModuleType` stub at import time (not a MagicMock, as this comment
    # claimed before F30), so on a checkout without delta-spark the stub is what is
    # there by the time this fixture runs.
    def _with_delta(b):
        """Add Delta's three configs, or return *b* unchanged if that is impossible.

        Three configs from two sources, and both sources are required — measured on a
        live session, not assumed. `configure_spark_with_delta_pip` sets ONLY
        spark.jars.packages (io.delta:delta-spark_2.12:3.2.0 here, derived from the
        installed version so it cannot drift from requirements.txt). The extension and
        the catalog have to be set by hand; a session with the JARs and neither of them
        fails every write with DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG.

        The guarded import is deliberate and so is its marker. Eight test modules install
        a `types.ModuleType("delta")` stub so they need no delta-spark, and a bare module
        exposes no `configure_spark_with_delta_pip`, so getattr excludes it. The previous
        marker was `hasattr(delta, "__version__")`, which a real delta-spark 3.2.0 also
        fails — its `delta/__init__.py` exports only DeltaTable and
        configure_spark_with_delta_pip — so every genuine install was read as a stub and
        the Delta suite skipped (F30).

        `configure_spark_with_delta_pip` itself is inside the try: it imports the
        importlib_metadata backport, identity-checks the builder type, and raises when
        the delta_spark distribution is not locatable. On a platform where any of those
        does not hold — a Databricks runtime being the case this suite is also run on —
        this must lose Delta, not the session.
        """
        try:
            import delta as _delta

            configure = getattr(_delta, "configure_spark_with_delta_pip", None)
            if not callable(configure):
                return b
            return (
                configure(b)
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                )
            )
        except Exception:  # pragma: no cover — environment-dependent
            return b

    # Delta first, then without it. spark.jars.packages is resolved at JVM launch, so an
    # ivy failure — Maven unreachable, a proxy, an air-gapped runner — makes
    # getOrCreate() raise. Retrying without Delta keeps the ~500 engine tests running and
    # costs only the Delta ones, which skip on their own capability probes.
    #
    # The fallback is the whole point. A single guarded getOrCreate() would have turned a
    # Maven outage into a skip of every spark-marked test with the run still green — which
    # is precisely the pathology F30 was filed about, re-created at a hundred times the
    # scale while purporting to fix it. Caught in review.
    try:
        return _delta_then_plain(make_builder, _with_delta)
    except Exception as exc:  # pragma: no cover — environment-dependent
        pytest.skip(f"SparkSession not available in this environment: {exc}")


def _release_delta_state(session: SparkSession | None, created: list[int]) -> bool:
    """Clear Delta's cached table state on *session* if this process built it; True if cleared.

    Delta keeps a DeltaLog per table path and persists each snapshot's state as an RDD;
    every test writes fresh temporary tables, so these pile up for the whole run (F208).
    Only a session this process built is touched: one that was already active (a
    Databricks cluster's, shared with other notebooks, or a Spark Connect session, which
    has no JVM gateway) is left alone, because `_counting_get_or_create` never counts it.
    """
    if session is None or id(session) not in created:
        return False
    session._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
    return True


@pytest.fixture
def release_delta_state():
    """`_release_delta_state`, as a fixture for the same sys.path reason as `sessions_created`."""
    return _release_delta_state


@pytest.fixture(autouse=True, scope="module")
def _release_delta_state_after_module():
    """Drop Delta's cached table state when a module ends, so the one JVM does not fill.

    Measured on tests/integration/local (2026-10-03, F208), heap after a full GC at each
    module boundary, 1 GB default heap: unchanged, 203 MB rising to 983 MB with 231
    persisted RDDs; with `DeltaLog.clearCache()` at each boundary, a 496 MB peak and no
    persisted RDDs left; with `spark.sql.ui.retainedExecutions` at 50 as well, 467 MB.
    The full suite had begun to fail with `OutOfMemoryError` and `UNABLE_TO_ACQUIRE_MEMORY`.
    A module that never started Spark starts nothing here.
    """
    yield
    try:
        _release_delta_state(SparkSession.getActiveSession(), _sessions_created)
    except Exception:  # pragma: no cover — a session built without Delta has no DeltaLog
        pass


def _delta_then_plain(make_builder, add_delta):
    """A session with Delta if it can be had, else one without it.

    Each attempt gets its own builder. `configure_spark_with_delta_pip` and
    `Builder.config()` mutate the builder they are given and return it, so a fallback on
    the same builder asked ivy for the same JARs and failed the same way: every
    spark-marked test skipped, not the four Delta ones (F44). pyspark's builders hold
    their options per instance, so a fresh one carries none of Delta.
    """
    try:
        return add_delta(make_builder()).getOrCreate()
    except Exception:  # pragma: no cover — environment-dependent
        return make_builder().getOrCreate()


@pytest.fixture(scope="session")
def delta_then_plain():
    """`_delta_then_plain`, exposed as a fixture so a test can reach it however pytest was
    started — `from tests.conftest import ...` resolves only when the repo root is on
    sys.path (see `sessions_created`)."""
    return _delta_then_plain


def pytest_collection_modifyitems(config: object, items: list) -> None:
    """Tag every test that takes the ``spark`` fixture with the ``spark`` marker.

    Applied automatically so the split can't drift out of sync with reality the
    way a hand-maintained list would. Lets the engine-free majority run on its
    own::

        pytest -m "not spark"    # ~535 tests, well under a second
        pytest -m spark          # the tests that need a real engine
    """
    for item in items:
        if "spark" in getattr(item, "fixturenames", ()):
            item.add_marker(pytest.mark.spark)
            # Under pytest-xdist every worker is its own process, hence its own JVM and
            # its own SparkSession: session scope is per worker, not per run, and that is
            # fine. Measured: 1959 unit + integration tests pass with -n 4 and with
            # -n 6 --dist loadfile, each worker one JVM of about 1.5 GB. Engine tests are
            # deliberately not pinned to one worker (an xdist_group did that, and left
            # the whole engine suite serial). `--dist loadfile` keeps every test of a
            # module on one worker, so module-scoped fixtures, such as F208's release of
            # the Delta cache, run once per module as they do in a serial run.
        _mark_scope(item)


_SCOPE_DIRS = {"unit", "integration", "architecture"}


def _mark_scope(item: object) -> None:
    """Stamp the ``unit`` / ``integration`` / ``architecture`` marker from the test's directory.

    Derived from the path rather than hand-applied, for the same reason the
    ``spark`` marker above is: a marker a human has to remember drifts out of
    sync with where the file actually lives, and then ``-m unit`` and
    ``pytest tests/unit`` quietly disagree about what they select.

    The scope axis is about composition, not machinery: a test of one
    transformer against a real SparkSession is a unit test, and a test that
    drives LoadOrchestrator.run() with mocks and no engine at all is an
    integration test. The ``spark`` marker is the separate engine axis, and
    the two compose — ``pytest tests/unit -m "not spark"`` is the inner loop.
    """
    for part in getattr(item, "path", None).parts if getattr(item, "path", None) else ():
        if part in _SCOPE_DIRS:
            item.add_marker(getattr(pytest.mark, part))
            return


def pytest_configure(config: object) -> None:
    """Silence a Databricks-notebook-only warning that the pyproject
    filterwarnings list doesn't reliably suppress there (the notebook-based
    test runner's rootdir/config discovery doesn't always pick up
    pyproject.toml). Registering it here via addinivalue_line is honored
    regardless of how pytest.main() was invoked, since conftest.py is always
    executed.

    dbruntime/safe_oinspect.py (Databricks' own IPython shell internals, not
    our code) calls the deprecated inspect.getargspec() during test runs —
    nothing in this repo can fix that upstream call.
    """
    config.addinivalue_line(
        "filterwarnings",
        "ignore:.*getargspec function is deprecated.*:DeprecationWarning",
    )
