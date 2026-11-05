"""F160 — the Data Vault satellite's target steps: the latest row per key, whole or for the batch.

`DVSatelliteTargetTransformer` reads every key's latest row, which a `Full` load needs to find
the keys absent from the source; `DVSatelliteIncrementalTargetTransformer` reads only the batch's
keys and refuses `Full`. The frame carries every column of the latest row, so a delete can carry
the attributes forward (the design's semantics table).
"""
from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from conftest import is_serverless_session
from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter
from spark_quick_start.orchestration.models import (
    GlobalContext,
    LoggingContext,
    RuntimeContext,
    SCDContext,
    StagingContext,
)
from spark_quick_start.orchestration.transformers import (
    DVSatelliteIncrementalTargetTransformer,
    DVSatelliteTargetTransformer,
)
from spark_quick_start.orchestration.transformers.base import LoadContext
from spark_quick_start.orchestration.transformers.dv_satellite import latest_rows
from spark_quick_start.orchestration.transformers.state import PipelineState

_SCHEMA = "HashKey string, Name string, HashDiff string, IsDeleted string, LoadTS timestamp, RecordSource string"


def _ctx(style="dv_satellite", load_type="Incremental") -> LoadContext:
    return LoadContext(
        global_ctx=GlobalContext({"id": "ent_1", "entity_name": "Customer", "source_system": "CRM",
                                  "target_table_path": "/vault/customer_crm_s", "staging_table_path": "/s"}),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext({"business_key_column": "HashKey", "scd_type": style, "load_type": load_type}),
        runtime_ctx=RuntimeContext({"correlation_id": "c", "pipeline_name": "p", "application_name": "a",
                                    "capture_partitions": False, "serverless": is_serverless_session()}),
        logging_ctx=LoggingContext({}),
    )


@pytest.fixture
def history(spark):
    t = datetime
    return spark.createDataFrame([
        ("h1", "Ada", "d1", "N", t(2026, 1, 1), "CRM"),
        ("h1", "Ada L.", "d2", "N", t(2026, 2, 1), "CRM"),
        ("h1", "Ada L.", "d2", "Y", t(2026, 3, 1), "CRM"),
        ("h2", "Grace", "d3", "N", t(2026, 1, 1), "CRM"),
        ("h9", "Old", "d9", "N", t(2025, 1, 1), "CRM"),
    ], _SCHEMA)


def _run(step, spark, adapter, staged, load_type="Incremental"):
    step._adapter, step._spark = adapter, spark
    return step(PipelineState(df=staged, frames={}), _ctx(load_type=load_type))


def test_the_full_step_reads_every_keys_latest_row_with_all_its_columns(spark, history):
    adapter = MockPlatformAdapter(existing_tables={"/vault/customer_crm_s"}, delta_read_result=history)
    staged = spark.createDataFrame([("h1",)], "HashKey string")
    frame = _run(DVSatelliteTargetTransformer(), spark, adapter, staged, "Full").frames["target"]
    rows = {r.HashKey: r for r in frame.collect()}
    assert sorted(rows) == ["h1", "h2", "h9"], "a Full load needs the keys the batch lacks too"
    assert (rows["h1"].Name, rows["h1"].HashDiff, rows["h1"].IsDeleted) == ("Ada L.", "d2", "Y")
    assert set(frame.columns) == set(history.columns)


def test_the_incremental_step_reads_only_the_batch_keys(spark, history):
    adapter = MockPlatformAdapter(existing_tables={"/vault/customer_crm_s"}, delta_read_result=history)
    staged = spark.createDataFrame([("h1",), ("h3",)], "HashKey string")
    frame = _run(DVSatelliteIncrementalTargetTransformer(), spark, adapter, staged).frames["target"]
    assert [(r.HashKey, r.LoadTS) for r in frame.collect()] == [("h1", datetime(2026, 3, 1))]


@pytest.mark.parametrize("step", [DVSatelliteTargetTransformer, DVSatelliteIncrementalTargetTransformer],
                         ids=lambda s: s.__name__)
def test_a_missing_satellite_leaves_no_target_frame(spark, step):
    staged = spark.createDataFrame([("h1",)], "HashKey string")
    load_type = "Full" if step is DVSatelliteTargetTransformer else "Incremental"
    out = _run(step(), spark, MockPlatformAdapter(), staged, load_type)
    assert "target" not in out.frames
    assert out.target_exists is False


@pytest.mark.parametrize("step", [DVSatelliteTargetTransformer, DVSatelliteIncrementalTargetTransformer],
                         ids=lambda s: s.__name__)
def test_an_existing_satellite_is_a_present_target_frame(spark, history, step):
    adapter = MockPlatformAdapter(existing_tables={"/vault/customer_crm_s"}, delta_read_result=history)
    staged = spark.createDataFrame([("h1",)], "HashKey string")
    load_type = "Full" if step is DVSatelliteTargetTransformer else "Incremental"
    out = _run(step(), spark, adapter, staged, load_type)
    assert "target" in out.frames
    assert out.target_exists is True


def test_the_incremental_step_refuses_a_full_load_before_reading():
    frame = MagicMock()
    adapter = MockPlatformAdapter(existing_tables={"/vault/customer_crm_s"})
    step = DVSatelliteIncrementalTargetTransformer()
    step._adapter, step._spark = adapter, MagicMock()
    with pytest.raises(ValueError, match="Full.*DVSatelliteTargetTransformer"):
        step(PipelineState(df=frame, frames={}), _ctx(load_type="Full"))
    assert adapter.read_delta_calls == []
    assert not frame.method_calls


@pytest.mark.parametrize("step", [DVSatelliteTargetTransformer, DVSatelliteIncrementalTargetTransformer],
                         ids=lambda s: s.__name__)
@pytest.mark.parametrize("style", ["dv_hub", "scd2"])
def test_a_satellite_step_refuses_another_style(step, style):
    frame = MagicMock()
    adapter = MockPlatformAdapter()
    s = step()
    s._adapter, s._spark = adapter, MagicMock()
    with pytest.raises(ValueError, match=rf"{step.__name__}.*{style}.*'dv_satellite'"):
        s(PipelineState(df=frame, frames={}), _ctx(style))
    assert adapter.delta_table_exists_calls == []
    assert not frame.method_calls


# ── review findings, F160 ────────────────────────────────────────────────────

def test_a_satellite_with_a_map_column_is_read(spark):
    """`max` over a struct needs every field orderable, and a map is not; the latest row is
    chosen by an ordering of orderable columns only, and the map is carried as a value."""
    t = datetime
    satellite = spark.createDataFrame([
        ("h1", {"a": "1"}, t(2026, 1, 1)),
        ("h1", {"a": "2"}, t(2026, 2, 1)),
    ], "HashKey string, Tags map<string,string>, LoadTS timestamp")
    rows = latest_rows(satellite, "HashKey", "LoadTS").collect()
    assert [(r.HashKey, r.Tags, r.LoadTS) for r in rows] == [("h1", {"a": "2"}, t(2026, 2, 1))]


def test_a_tie_on_load_ts_is_broken_by_the_later_columns_in_order(spark):
    """Unreachable once written (the writer merges on key and LoadTS), but deterministic."""
    t = datetime(2026, 1, 1)
    satellite = spark.createDataFrame([("h1", "a", t), ("h1", "b", t)], "HashKey string, HashDiff string, LoadTS timestamp")
    assert latest_rows(satellite, "HashKey", "LoadTS").first().HashDiff == "b"


def test_a_row_without_a_load_ts_never_wins_over_one_with_it(spark):
    satellite = spark.createDataFrame([("h1", "late", None), ("h1", "dated", datetime(2026, 1, 1))],
                                      "HashKey string, HashDiff string, LoadTS timestamp")
    assert latest_rows(satellite, "HashKey", "LoadTS").first().HashDiff == "dated"


def test_the_two_target_steps_are_siblings_not_parent_and_child():
    """api-and-typing, composition over inheritance: the shared read is a function."""
    assert not issubclass(DVSatelliteIncrementalTargetTransformer, DVSatelliteTargetTransformer)
    assert DVSatelliteIncrementalTargetTransformer.data_vault_style == "dv_satellite"
