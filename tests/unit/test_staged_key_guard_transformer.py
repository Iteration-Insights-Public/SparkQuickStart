"""Tests for StagedKeyGuardTransformer (F134, F154).

The guard looks at the staged rows before change detection: a key staged twice, or with a NULL
or empty part, is bad input the library cannot repair, only refuse loudly and early. F134: a
duplicate whose one copy equals the target loaded the other silently, because the detector drops
the unchanged copy first. F154: a NULL business key matched nothing and was inserted again, as
current, on every run.
"""
from __future__ import annotations

import pytest

from conftest import is_serverless_session
from spark_quick_start.orchestration.models import (
    GlobalContext,
    LoggingContext,
    RuntimeContext,
    SCDContext,
    StagingContext,
)
from spark_quick_start.orchestration.transformers.base import LoadContext
from spark_quick_start.orchestration.transformers.staged_key_guard import StagedKeyGuardTransformer
from spark_quick_start.orchestration.transformers.state import PipelineState


def _ctx(*, source_system_from_source: bool = False, scd_type: str = "scd2", **scd) -> LoadContext:
    return LoadContext(
        global_ctx=GlobalContext({
            "id": "ent_1",
            "entity_name": "Customer", "source_system": "CRM",
            "target_table_path": "/tmp/t", "staging_table_path": "/tmp/s",
        }),
        staging_ctx=StagingContext({}),
        scd_ctx=SCDContext({"business_key_column": "id", "scd_type": scd_type,
                            "source_system_from_source": source_system_from_source, **scd}),
        runtime_ctx=RuntimeContext({
            "correlation_id": "c", "pipeline_name": "p", "application_name": "a",
            "serverless": is_serverless_session(),
        }),
        logging_ctx=LoggingContext({}),
    )


def _run(df, **kw):
    state = PipelineState(df=df, frames={})
    return state, StagedKeyGuardTransformer()(state, _ctx(**kw))


# ── valid input passes untouched ─────────────────────────────────────────────

def test_unique_keys_pass_and_return_the_same_state(spark):
    df = spark.createDataFrame([("1", "Ada"), ("2", "Grace")], "id string, name string")
    state, result = _run(df)
    assert result is state


def test_an_empty_source_passes_and_does_not_halt(spark):
    """The detector refuses an empty Full source on purpose; halting here would hide that."""
    df = spark.createDataFrame([], "id string, name string")
    state, result = _run(df)
    assert result is state and not result.halted


def test_one_key_from_two_source_systems_is_two_keys_when_the_rows_carry_it(spark):
    df = spark.createDataFrame([("1", "CRM", "Ada"), ("1", "ERP", "Ada")],
                               "id string, scdSourceSystem string, name string")
    state, result = _run(df, source_system_from_source=True)
    assert result is state


def test_a_non_string_key_is_checked_for_duplicates_and_nulls_only(spark):
    df = spark.createDataFrame([(1, "Ada"), (2, "Grace")], "id int, name string")
    state, result = _run(df)
    assert result is state


# ── duplicates (F134) ────────────────────────────────────────────────────────

def test_a_key_staged_twice_is_refused_with_its_count_and_a_sample(spark):
    df = spark.createDataFrame([("1", "Ada"), ("1", "Ada L."), ("2", "Grace")], "id string, name string")
    with pytest.raises(ValueError) as exc:
        _run(df)
    message = str(exc.value)
    assert "1 key(s) staged more than once" in message
    assert "'1'" in message and "2 rows" in message


def test_an_integer_key_staged_twice_is_refused(spark):
    df = spark.createDataFrame([(7, "a"), (7, "b")], "id int, name string")
    with pytest.raises(ValueError, match="1 key\\(s\\) staged more than once"):
        _run(df)


def test_the_same_key_and_source_system_twice_is_refused_when_the_rows_carry_it(spark):
    df = spark.createDataFrame([("1", "CRM", "Ada"), ("1", "CRM", "Ada L.")],
                               "id string, scdSourceSystem string, name string")
    with pytest.raises(ValueError, match="staged more than once"):
        _run(df, source_system_from_source=True)


# ── NULL and empty keys (F154) ───────────────────────────────────────────────

def test_a_null_business_key_is_refused(spark):
    df = spark.createDataFrame([("1", "Ada"), (None, "Grace")], "id string, name string")
    with pytest.raises(ValueError, match="1 row\\(s\\) with a NULL key"):
        _run(df)


def test_a_null_source_system_is_refused_when_the_rows_carry_it(spark):
    df = spark.createDataFrame([("1", None, "Ada")], "id string, scdSourceSystem string, name string")
    with pytest.raises(ValueError, match="NULL key"):
        _run(df, source_system_from_source=True)


@pytest.mark.parametrize("blank", ["", "   "])
def test_an_empty_string_key_is_refused(spark, blank):
    df = spark.createDataFrame([(blank, "Ada"), ("2", "Grace")], "id string, name string")
    with pytest.raises(ValueError, match="1 row\\(s\\) with an empty key"):
        _run(df)


def test_every_problem_is_reported_at_once(spark):
    df = spark.createDataFrame([("1", "a"), ("1", "b"), (None, "c"), ("", "d")], "id string, name string")
    with pytest.raises(ValueError) as exc:
        _run(df)
    message = str(exc.value)
    assert "staged more than once" in message and "NULL key" in message and "empty key" in message


def test_a_missing_key_column_is_refused_naming_it(spark):
    df = spark.createDataFrame([("Ada",)], "name string")
    with pytest.raises(ValueError, match="'id'"):
        _run(df)


@pytest.mark.parametrize("style", ["dv_hub", "dv_satellite"])
def test_under_a_data_vault_style_one_key_from_two_sources_is_refused(spark, style):
    """F162's review: Data Vault integrates by business key alone, so a key two sources stage in
    one batch would be inserted twice, at one LoadTS, by an insert-only writer that cannot see it."""
    staged = spark.createDataFrame([("k1", "CRM"), ("k1", "ERP")], "id string, RecordSource string")
    step = StagedKeyGuardTransformer()
    with pytest.raises(ValueError, match="staged more than once"):
        step(PipelineState(df=staged, frames={}), _ctx(source_system_from_source=True, scd_type=style))


# ── F167: a link's hub keys ──────────────────────────────────────────────────

_LINK = {"scd_type": "dv_link", "link_hub_key_columns": ["order_hk", "product_hk"]}
_LINK_SCHEMA = "id string, order_hk string, product_hk string"


def test_a_link_hub_key_repeats_across_links_and_passes(spark):
    """A hub key is shared by every link to that hub; only the link key must be unique."""
    staged = spark.createDataFrame([("l1", "o1", "p1"), ("l2", "o1", "p2")], _LINK_SCHEMA)
    state = PipelineState(df=staged, frames={})
    assert StagedKeyGuardTransformer()(state, _ctx(**_LINK)) is state


@pytest.mark.parametrize("bad", [None, "  "], ids=["null", "empty"])
def test_a_null_or_empty_hub_key_is_refused(spark, bad):
    staged = spark.createDataFrame([("l1", "o1", "p1"), ("l2", "o2", bad)], _LINK_SCHEMA)
    with pytest.raises(ValueError, match="product_hk"):
        StagedKeyGuardTransformer()(PipelineState(df=staged, frames={}), _ctx(**_LINK))


def test_a_missing_hub_key_column_is_refused_naming_it(spark):
    staged = spark.createDataFrame([("l1", "o1")], "id string, order_hk string")
    with pytest.raises(ValueError, match="product_hk"):
        StagedKeyGuardTransformer()(PipelineState(df=staged, frames={}), _ctx(**_LINK))
