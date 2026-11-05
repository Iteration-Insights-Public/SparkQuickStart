"""Invariants about the test suite itself.

One SparkSession per process, reached through one door. Spark is a per-JVM
singleton, so a second `builder.getOrCreate()` does not fail — it silently
returns the first session with the caller's static configs discarded. These
tests make that failure mode loud instead.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


def _tests_root() -> Path:
    """Return the tests/ directory by walking up to the pyproject.toml marker.

    Walks up rather than counting `.parent` levels — test-classification.md bans
    exactly that; this file was the hit that put the ban in the rule.

    Walks to `pyproject.toml` rather than the nearest `conftest.py`, matching
    `tests/architecture/_arch.py:repo_root()`: there is exactly one conftest.py
    in the tree today, but an ordinary `tests/unit/conftest.py` added later
    would be nearer than `tests/conftest.py` and silently narrow this to
    `tests/unit/` — the scan below would then miss `tests/integration/` and
    `tests/architecture/` entirely while still reporting green.
    """
    here = Path(__file__).resolve()
    for candidate in (here, *here.parents):
        if (candidate / "pyproject.toml").is_file():
            return candidate / "tests"
    raise RuntimeError(f"no pyproject.toml found at or above {here}")


_TESTS_ROOT = _tests_root()

# Files allowed to construct a session: the fixture's home, and this file's own
# references to the pattern it bans.
_ALLOWED = {"conftest.py", "test_suite_invariants.py"}

_BUILDER_PATTERN = re.compile(r"SparkSession\s*\.\s*builder")


def test_exactly_one_spark_session_is_constructed(
    spark: SparkSession, sessions_created: list[int]
) -> None:
    """The whole process shares one session, and it is the fixture's."""
    created = sessions_created

    assert len(created) == 1, (
        f"{len(created)} SparkSessions were constructed in this process; the "
        "session-scoped `spark` fixture must be the only one. A module building "
        "its own session silently inherits this one instead, losing its static "
        "configs (spark.master, spark.sql.extensions)."
    )
    assert created[0] == id(spark)
    assert SparkSession.getActiveSession() is spark


def test_the_session_rebuilds_a_delta_snapshot_in_one_task(
    spark: SparkSession, sessions_created: list[int]
) -> None:
    """F182 — the fixture's session reads a Delta log with one task, not Delta's default 50.

    The tables a test writes hold a handful of commits, so 50 snapshot tasks are pure
    scheduling. Measured on 2026-10-02: `tests/integration` 208 s with the default and
    117 s with 1, the whole suite 276 s and 172 s, with the same results. The UI and the
    console progress bar are off for the same reason. Where the suite reuses a session it
    did not build, as on Databricks, its configuration is not ours to assert.
    """
    if sessions_created != [id(spark)]:
        pytest.skip("the suite reuses a session it did not build; its configuration is not ours")

    settings = {
        key: spark.conf.get(key, None)
        for key in (
            "spark.databricks.delta.snapshotPartitions",
            "spark.ui.enabled",
            "spark.ui.showConsoleProgress",
        )
    }
    assert settings == {
        "spark.databricks.delta.snapshotPartitions": "1",
        "spark.ui.enabled": "false",
        "spark.ui.showConsoleProgress": "false",
    }, settings

def test_no_test_module_builds_its_own_session() -> None:
    """Only conftest may reach for the builder.

    Nine modules once did this. Each got whichever session happened to exist
    first, so tests/integration/local's (then tests/local/) Delta configuration was dropped whenever another
    module had already created one — Delta tests ran against a session with no
    Delta support and still passed.
    """
    offenders = [
        path.relative_to(_TESTS_ROOT).as_posix()
        for path in _TESTS_ROOT.rglob("*.py")
        if path.name not in _ALLOWED
        and _BUILDER_PATTERN.search(path.read_text(encoding="utf-8"))
    ]

    assert not offenders, (
        "these test modules construct their own SparkSession instead of taking "
        f"the session-scoped `spark` fixture: {offenders}"
    )


def test_no_test_is_pinned_to_an_xdist_group(request) -> None:
    """No test carries an xdist group, so every engine test can spread across workers.

    Session scope is per process: under `pytest -n N` each worker is its own JVM with its own
    SparkSession, and CI and the documented local commands rely on that (`--dist loadfile`
    keeps a module on one worker). A `xdist_group("spark")` pin used to put every engine
    test on a single worker, which left the engine suite, nearly all of the wall time,
    serial. This guards against the pin coming back.

    Checks every collected item rather than this one, so it is a statement about the suite
    and not about itself.
    """
    pinned = [i.nodeid for i in request.session.items if i.get_closest_marker("xdist_group")]

    assert not pinned, (
        "these tests are pinned to an xdist group, which keeps them on one worker and "
        f"serialises the engine suite: {pinned}"
    )


class _FakeBuilder:
    """Records its options the way pyspark's Builder does: per instance, mutated in place."""

    def __init__(self, fail_when: str | None = None) -> None:
        self.options: dict[str, str] = {}
        self._fail_when = fail_when

    def config(self, key: str, value: str) -> _FakeBuilder:
        self.options[key] = value
        return self

    def getOrCreate(self) -> dict[str, str]:
        if self._fail_when is not None and self._fail_when in self.options:
            raise RuntimeError(f"unresolvable: {self.options[self._fail_when]}")
        return dict(self.options)


def test_the_delta_fallback_starts_from_a_clean_builder(delta_then_plain) -> None:
    """F44 — when the Delta session cannot be built, the fallback carries none of Delta.

    `configure_spark_with_delta_pip` mutates the builder it is given and returns it, so a
    fallback on that same builder asked ivy for the same JARs and failed the same way —
    every spark test skipped instead of the four Delta ones.
    """
    def add_delta(b: _FakeBuilder) -> _FakeBuilder:
        return b.config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0").config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")

    session = delta_then_plain(lambda: _FakeBuilder(fail_when="spark.jars.packages"), add_delta)
    assert "spark.jars.packages" not in session and "spark.sql.extensions" not in session, session


def test_the_delta_session_is_used_when_it_can_be_built(delta_then_plain) -> None:
    """F44 — the fix must not lose Delta when Delta works."""
    session = delta_then_plain(_FakeBuilder, lambda b: b.config("spark.jars.packages", "x"))
    assert session == {"spark.jars.packages": "x"}


def test_each_builder_access_holds_its_own_options() -> None:
    """F44 — the premise `_delta_then_plain` rests on, asserted on the pyspark that is installed.

    A fresh builder is clean only because `SparkSession.builder` returns a new Builder per
    access and each keeps its own `_options`. Older pyspark exposed one class-level builder;
    on a runtime that still does, the fallback would inherit Delta's config again and the
    two tests above would stay green. Engine-free: no session is created.
    """
    first, second = SparkSession.builder, SparkSession.builder
    assert first is not second, "SparkSession.builder returned the same object twice"
    first.config("spark_quick_start.f44.probe", "1")
    assert "spark_quick_start.f44.probe" not in second._options, (
        "builder options are shared across accesses; a fresh builder is not clean"
    )


class _NoGateway:
    """A session stand-in whose JVM gateway is unreachable, as on Spark Connect."""

    @property
    def _jvm(self):
        raise AssertionError("the sweep reached a session this process did not build")


def test_the_delta_sweep_leaves_a_session_it_did_not_build_alone(release_delta_state):
    # F208: a Databricks cluster's session, or a Spark Connect one, is never counted
    # as built here, so the module-end sweep must not touch it.
    assert release_delta_state(_NoGateway(), created=[]) is False
    assert release_delta_state(None, created=[]) is False


def test_the_delta_sweep_clears_the_session_it_built(release_delta_state, sessions_created):
    session = SparkSession.getActiveSession()
    if session is None or id(session) not in sessions_created:
        pytest.skip("no session built by this process is active")
    assert release_delta_state(session, sessions_created) is True
