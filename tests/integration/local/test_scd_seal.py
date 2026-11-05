"""F211: an SCD table seals its flag values, open-row sentinel, hash separator and time zone.

The merge and the append-only writer record seven values as Delta table properties on the first
write and compare before every later write, refusing a difference with the table unchanged. A
populated table with no seal (one written before F211) is sealed after its next successful write,
with a warning. Against a real local Delta target. Requires a working Delta install.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from spark_quick_start.adapters.local_adapter import LocalAdapter
from spark_quick_start.orchestration import LoadOrchestrator, default_pipeline
from spark_quick_start.orchestration.staging_pipeline import create_staging_main_function
from spark_quick_start.orchestration.transformers import (
    CandidateGuardTransformer,
    NativeSCDAppendOnlyTransformer,
    SCDChangeDetectorTransformer,
    SCDTargetTableTransformer,
    StagingTransformer,
)

_P = "spark_quick_start.scd_"
_DEFAULTS = {
    "current_flag_value": "Y", "inactive_flag_value": "N",
    "deleted_flag_value": "Y", "not_deleted_flag_value": "N",
    "default_max_timestamp": "2999-12-31T23:59:59.999999+00:00", "hash_separator": "~",
    "time_zone": "UTC",
}
# What each sealed value is changed to for the refusal test (as the context spells it).
_CHANGES = {
    "current_flag_value": {"current_flag_value": "1"},
    "inactive_flag_value": {"inactive_flag_value": "0"},
    "deleted_flag_value": {"deleted_flag_value": "D"},
    "not_deleted_flag_value": {"not_deleted_flag_value": "A"},
    "default_max_timestamp": {"default_max_timestamp": "2500-06-01T12:00:00Z"},
    "hash_separator": {"hash_separator": "|"},
}
_TZ = "spark.sql.session.timeZone"


@pytest.fixture(autouse=True)
def _require_working_delta(spark, tmp_path_factory):
    """Skip unless this session can round-trip a Delta table; see test_dqd_load."""
    pytest.importorskip("delta", reason="delta-spark not installed")
    probe = str(tmp_path_factory.mktemp("delta_probe") / "t")
    try:
        spark.range(1).write.format("delta").mode("overwrite").save(probe)
    except Exception as exc:  # pragma: no cover — environment-dependent
        pytest.skip(f"Delta not usable in this Spark session: {type(exc).__name__}")
    yield
    # Releases the DataFrame caches the candidate guard makes in each load of this test; unlike the
    # module-end sweep in tests/conftest.py (F208), which drops Delta's per-module DeltaLog state.
    spark.catalog.clearCache()


@pytest.fixture(autouse=True)
def _session_time_zone_utc(spark):
    """Pin the session zone the seal records (`time_zone`) to UTC; restore it after the test."""
    before = spark.conf.get(_TZ)
    spark.conf.set(_TZ, "UTC")
    yield
    spark.conf.set(_TZ, before)


def _context(root, run, scd_type, **scd):
    return json.dumps({
        "global_context": {
            "id": "customer", "entity_name": "Customer", "source_system": "CRM",
            "target_table_path": f"{root}/silver/customer",
            "staging_table_path": f"{root}/staging/customer",
            "load_timestamp": f"2026-10-01T{8 + run:02d}:00:00+00:00",
            "staging_table_schema": {"type": "struct", "fields": [
                {"name": "customer_id", "type": "integer", "nullable": True, "metadata": {}},
                {"name": "name", "type": "string", "nullable": True, "metadata": {}}]},
        },
        "staging_context": {"stages": [{"nb_name": "NB_Stage_Customer"}]},
        "scd_context": {"business_key_column": "customer_id", "scd_type": scd_type, **scd},
        "runtime_context": {"correlation_id": f"run-{run}", "pipeline_name": "customer", "application_name": "crm"},
        "logging_context": {"logging_level": "INFO", "log_file_name": "customer", "log_file_dir": f"{root}/logs"},
    })


@pytest.fixture
def load(spark, tmp_path):
    """load(rows, style="scd2", logger=None, detector=None, **scd) -> ExecutionResult; style is scd1, scd2 or append.

    `append` composes the append-only writer over an SCD2 context, with `detector` when given;
    `root` replaces the directory the target is under; `load.adapter` is the adapter used.
    Each call is the next hour's load.
    """
    staged = {"rows": [], "n": 0}

    def stage(storage_path, staging_context, global_context, notebook_name):
        return spark.createDataFrame(staged["rows"], "customer_id int, name string")

    main = create_staging_main_function(stage, adapter=LocalAdapter(), spark=spark)
    adapter = LocalAdapter(notebook_registry={"NB_Stage_Customer": lambda a: main(a["stage_context_str"]).to_json()})

    def _load(rows, style="scd2", logger=None, detector=None, root=None, **scd):
        staged["rows"], staged["n"] = rows, staged["n"] + 1
        context = _context(root or tmp_path, staged["n"], "scd2" if style == "append" else style, **scd)
        if style != "append":
            return default_pipeline(context, spark, adapter=adapter, logger=logger).run()
        return (
            LoadOrchestrator(load_context_json=context, spark=spark, adapter=adapter, logger=logger)
            .add(StagingTransformer())
            .add(SCDTargetTableTransformer())
            .add(detector or SCDChangeDetectorTransformer())
            .add(CandidateGuardTransformer())
            .add(NativeSCDAppendOnlyTransformer())
            .run()
        )

    _load.adapter = adapter
    return _load


def _properties(spark, root):
    row = spark.sql(f"DESCRIBE DETAIL delta.`{root}/silver/customer`").first()
    return {k: v for k, v in (row["properties"] or {}).items() if k.startswith(_P)}


def _snapshot(spark, root):
    return sorted(tuple(r) for r in spark.read.format("delta").load(f"{root}/silver/customer").collect())


def _seal_warnings(logger):
    """The warnings about the seal; the target's path (the test's name) must not match them."""
    return [c.args[0] for c in logger.warning.call_args_list
            if any(k in c.args[0] for k in ("sealed", "no SCD seal", "not a target"))]


def _unseal(spark, root):
    keys = ", ".join(f"'{_P}{k}'" for k in _DEFAULTS)
    spark.sql(f"ALTER TABLE delta.`{root}/silver/customer` UNSET TBLPROPERTIES ({keys})")


_STYLES = ["scd1", "scd2", "append"]
# The seal is one code path shared by the three styles, so each case runs on the writer that
# matters for it rather than on all three: every load of the shared local Spark session costs jobs
# and driver heap, and this file adds a few dozen of them to a suite that already runs near its limit.
_MERGE_AND_APPEND = ["scd2", "append"]


@pytest.mark.parametrize("style", _STYLES)
def test_a_first_load_seals_the_seven_values(spark, tmp_path, load, style):
    result = load([(1, "Ada")], style)

    assert result.status == "success", result.error_details
    assert _properties(spark, tmp_path) == {f"{_P}{k}": v for k, v in _DEFAULTS.items()}


@pytest.mark.parametrize(("style", "value"), [
    *[(style, value) for style in _MERGE_AND_APPEND for value in sorted(_CHANGES)],
    ("scd1", "deleted_flag_value"),  # the SCD1 soft delete is its own MERGE SQL
])
def test_a_changed_value_is_refused_before_anything_is_written(spark, tmp_path, load, style, value):
    assert load([(1, "Ada")], style).status == "success"
    before, sealed = _snapshot(spark, tmp_path), _properties(spark, tmp_path)

    result = load([(2, "Grace")], style, **_CHANGES[value])

    assert result.status == "error"
    message = result.error_details["error_message"]
    assert f"{_P}{value}" in message and "silver/customer" in message, message
    assert "UPDATE" in message and "TBLPROPERTIES" in message
    assert _snapshot(spark, tmp_path) == before
    assert _properties(spark, tmp_path) == sealed


@pytest.mark.parametrize("style", _MERGE_AND_APPEND)
def test_an_equal_rerun_is_accepted_and_the_seal_stays(spark, tmp_path, load, style):
    load([(1, "Ada")], style)
    sealed = _properties(spark, tmp_path)

    result = load([(2, "Grace")], style)

    assert result.status == "success", result.error_details
    assert _properties(spark, tmp_path) == sealed


@pytest.mark.parametrize("style", _MERGE_AND_APPEND)
def test_an_unsealed_populated_table_is_sealed_after_the_next_write_with_a_warning(spark, tmp_path, load, style):
    load([(1, "Ada")], style)
    _unseal(spark, tmp_path)
    assert _properties(spark, tmp_path) == {}
    logger = MagicMock()

    result = load([(2, "Grace")], style, logger=logger)

    assert result.status == "success", result.error_details
    assert _properties(spark, tmp_path) == {f"{_P}{k}": v for k, v in _DEFAULTS.items()}
    warnings = _seal_warnings(logger)
    assert len(warnings) == 1 and "silver/customer" in warnings[0], warnings


@pytest.mark.parametrize("style", ["scd2"])
def test_an_unsealed_table_is_sealed_with_the_values_of_the_load_that_finds_it(spark, tmp_path, load, style):
    load([(1, "Ada")], style)
    _unseal(spark, tmp_path)

    load([(2, "Grace")], style, hash_separator="|")

    assert _properties(spark, tmp_path)[f"{_P}hash_separator"] == "|"


@pytest.mark.parametrize("style", ["scd2"])
def test_the_manual_procedure_lets_a_changed_value_through(spark, tmp_path, load, style):
    load([(1, "Ada")], style)
    spark.sql(f"ALTER TABLE delta.`{tmp_path}/silver/customer` "
              f"SET TBLPROPERTIES ('{_P}default_max_timestamp' = '2500-06-01T12:00:00+00:00')")

    result = load([(2, "Grace")], style, default_max_timestamp="2500-06-01T12:00:00Z")

    assert result.status == "success", result.error_details


def test_a_separator_given_to_the_detector_alone_is_the_one_sealed(spark, tmp_path, load):
    """No context key: the constructor's value is what the hash used, so it is what is sealed."""
    result = load([(1, "Ada")], "append", detector=SCDChangeDetectorTransformer(hash_separator="|"))

    assert result.status == "success", result.error_details
    assert _properties(spark, tmp_path)[f"{_P}hash_separator"] == "|"


@pytest.mark.parametrize("style", _MERGE_AND_APPEND)
def test_a_seal_that_cannot_be_written_after_the_write_is_a_warning_and_the_next_load_seals(
        spark, tmp_path, load, monkeypatch, style):
    """The data is committed; failing the load would report an error over a successful write."""
    def refused(spark, identifier, properties):
        raise PermissionError("needs MANAGE on the table")

    monkeypatch.setattr(load.adapter, "set_table_properties", refused)
    logger = MagicMock()

    first = load([(1, "Ada")], style, logger=logger)

    assert first.status == "success", first.error_details
    assert _snapshot(spark, tmp_path) != [] and _properties(spark, tmp_path) == {}
    warnings = _seal_warnings(logger)
    assert len(warnings) == 1, warnings
    (warning,) = warnings
    assert "silver/customer" in warning and "needs MANAGE" in warning and f"{_P}hash_separator" in warning

    monkeypatch.undo()
    second = load([(2, "Grace")], style, logger=MagicMock())
    assert second.status == "success", second.error_details
    assert _properties(spark, tmp_path) == {f"{_P}{k}": v for k, v in _DEFAULTS.items()}


def test_a_target_the_property_helpers_refuse_loads_with_a_warning_and_no_seal(spark, tmp_path, load):
    root = tmp_path / "a`b"  # a backtick cannot be quoted into the SQL the helpers build
    logger = MagicMock()

    result = load([(1, "Ada")], "scd2", logger=logger, root=root)
    again = load([(2, "Grace")], "scd2", logger=logger, root=root, hash_separator="|")

    assert result.status == "success" and again.status == "success", (result.error_details, again.error_details)
    assert len(spark.read.format("delta").load(f"{root}/silver/customer").collect()) == 2
    # the first load found it at the seal, after the write; the second before it
    assert len(_seal_warnings(logger)) == 2 and all("not a target the seal supports" in w for w in _seal_warnings(logger))
    assert not [k for k in spark.read.format("delta").load(f"{root}/silver/customer").columns if "seal" in k]


@pytest.mark.parametrize("style", _MERGE_AND_APPEND)
def test_a_changed_session_time_zone_is_refused_before_anything_is_written(spark, tmp_path, load, style):
    assert load([(1, "Ada")], style).status == "success"
    before, sealed = _snapshot(spark, tmp_path), _properties(spark, tmp_path)
    assert sealed[f"{_P}time_zone"] == "UTC"
    spark.conf.set(_TZ, "Europe/Kyiv")  # restored by the autouse fixture

    result = load([(2, "Grace")], style)

    assert result.status == "error"
    message = result.error_details["error_message"]
    assert f"{_P}time_zone" in message and "'UTC'" in message and "'Europe/Kyiv'" in message, message
    assert "hash" in message and "silver/customer" in message
    assert _snapshot(spark, tmp_path) == before
    assert _properties(spark, tmp_path) == sealed


@pytest.mark.parametrize("style", _MERGE_AND_APPEND)
def test_the_same_session_time_zone_is_accepted(spark, tmp_path, load, style):
    spark.conf.set(_TZ, "Europe/Kyiv")
    assert load([(1, "Ada")], style).status == "success"
    assert _properties(spark, tmp_path)[f"{_P}time_zone"] == "Europe/Kyiv"

    result = load([(2, "Grace")], style)

    assert result.status == "success", result.error_details


def test_a_table_sealed_without_the_time_zone_gets_it_added_with_a_warning(spark, tmp_path, load):
    load([(1, "Ada")], "scd2")
    spark.sql(f"ALTER TABLE delta.`{tmp_path}/silver/customer` UNSET TBLPROPERTIES ('{_P}time_zone')")
    assert f"{_P}time_zone" not in _properties(spark, tmp_path)
    logger = MagicMock()

    result = load([(2, "Grace")], "scd2", logger=logger)

    assert result.status == "success", result.error_details
    assert _properties(spark, tmp_path)[f"{_P}time_zone"] == "UTC"
    warnings = _seal_warnings(logger)
    assert len(warnings) == 1 and f"{_P}time_zone" in warnings[0], warnings
