"""F156 — Data Vault styles in scd_context: the style-dependent defaults and the refusals.

The accepted design (docs/architecture/decisions/2026-10-01-data-vault-design.md) keeps one
`scd_context`, extended: `scd_type` gains `dv_hub`, `dv_satellite` and `dv_link`, the existing
name keys take Data Vault defaults under those styles, a few Data Vault-only keys are added, and a
key that belongs to the other style is refused rather than ignored. Every SCD load context stays
valid and unchanged.
"""
from __future__ import annotations

import pytest

from spark_quick_start.orchestration.models import RuntimeContext, SCDContext

_DV = {"business_key_column": "HashKey"}


# ── SCD styles are unchanged ─────────────────────────────────────────────────

def test_an_scd_context_keeps_every_default_it_had() -> None:
    ctx = SCDContext({"business_key_column": "id"})
    ctx.validate()
    assert (ctx.scd_type, ctx.row_key_column, ctx.hash_column, ctx.deleted_flag_column,
            ctx.source_system_column, ctx.correlation_id_column, ctx.hash_separator) == (
        "scd2", "scdRowKey", "scdHash", "scdDeletedFlag", "scdSourceSystem", "scdCorrelationId", "~")
    assert not ctx.is_data_vault


@pytest.mark.parametrize("key,value", [("hash_algorithm", "md5"), ("load_ts_column", "LoadTS"),
                                       ("load_end_ts_column", "LoadEndTS"),
                                       ("pipeline_run_id_column", "PipelineRunId")])
def test_an_scd_style_refuses_a_data_vault_only_key(key: str, value: str) -> None:
    ctx = SCDContext({"business_key_column": "id", key: value})
    with pytest.raises(ValueError, match=rf"{key}.*Data Vault.*scd2"):
        ctx.validate()


# ── Data Vault defaults ──────────────────────────────────────────────────────

@pytest.mark.parametrize("style", ["dv_hub", "dv_satellite"])
def test_a_data_vault_style_takes_the_data_vault_names(style: str) -> None:
    ctx = SCDContext({**_DV, "scd_type": style})
    ctx.validate()
    assert ctx.is_data_vault
    assert (ctx.row_key_column, ctx.hash_column, ctx.deleted_flag_column, ctx.source_system_column,
            ctx.correlation_id_column, ctx.hash_separator, ctx.hash_algorithm,
            ctx.load_ts_column, ctx.load_end_ts_column, ctx.pipeline_run_id_column) == (
        "HashKey", "HashDiff", "IsDeleted", "RecordSource", "CorrelationId", "||", "sha256",
        "LoadTS", "LoadEndTS", None)


def test_a_data_vault_name_can_still_be_overridden() -> None:
    ctx = SCDContext({**_DV, "scd_type": "dv_satellite", "business_key_column": "customer_hk",
                      "row_key_column": "customer_hk", "hash_column": "hash_diff",
                      "load_ts_column": "load_ts", "hash_algorithm": "md5",
                      "pipeline_run_id_column": "pipeline_run_id"})
    ctx.validate()
    assert (ctx.row_key_column, ctx.hash_column, ctx.load_ts_column, ctx.hash_algorithm,
            ctx.pipeline_run_id_column) == ("customer_hk", "hash_diff", "load_ts", "md5", "pipeline_run_id")


# ── Data Vault refusals ──────────────────────────────────────────────────────

# dv_link was refused as not yet supported until F167; its tests are in test_scd_context_dv_link.py.


@pytest.mark.parametrize("key,value", [
    ("version_column", "v"), ("current_flag_column", "c"), ("effective_from_column", "f"),
    ("effective_to_column", "t"), ("scd_row_hashing", True), ("current_flag_value", "Y"),
    ("inactive_flag_value", "N"), ("default_max_timestamp", "2999-12-31")])
def test_a_data_vault_style_refuses_an_scd_only_key(key: str, value: object) -> None:
    ctx = SCDContext({**_DV, "scd_type": "dv_satellite", key: value})
    with pytest.raises(ValueError, match=rf"{key}.*SCD.*dv_satellite"):
        ctx.validate()


def test_an_unknown_hash_algorithm_is_refused() -> None:
    with pytest.raises(ValueError, match="hash_algorithm 'sha1'.*sha256.*md5"):
        SCDContext({**_DV, "scd_type": "dv_hub", "hash_algorithm": "sha1"}).validate()


def test_a_data_vault_meta_column_named_as_the_business_key_is_refused() -> None:
    with pytest.raises(ValueError, match="load_ts_column='HashKey'"):
        SCDContext({**_DV, "scd_type": "dv_satellite", "load_ts_column": "HashKey"}).validate()


# ── runtime_context.pipeline_run_id ──────────────────────────────────────────

def test_runtime_context_reads_an_optional_pipeline_run_id() -> None:
    base = {"correlation_id": "c", "pipeline_name": "p", "application_name": "a"}
    assert RuntimeContext(base).pipeline_run_id is None
    assert RuntimeContext({**base, "pipeline_run_id": "run-7"}).pipeline_run_id == "run-7"


# ── review findings: null is "not given", blank is refused ───────────────────

def test_a_null_value_counts_as_not_given_in_both_directions() -> None:
    """A template that writes every key, null where unused, must not be refused for it."""
    SCDContext({"business_key_column": "id", "hash_algorithm": None, "load_ts_column": None}).validate()
    ctx = SCDContext({**_DV, "scd_type": "dv_satellite", "version_column": None, "load_ts_column": None})
    ctx.validate()
    assert ctx.load_ts_column == "LoadTS"


@pytest.mark.parametrize("key", ["load_ts_column", "load_end_ts_column"])
@pytest.mark.parametrize("value", ["", "   "])
def test_a_blank_load_timestamp_column_is_refused(key: str, value: str) -> None:
    with pytest.raises(ValueError, match=rf"{key} is empty"):
        SCDContext({**_DV, "scd_type": "dv_hub", key: value}).validate()


def test_an_empty_hash_algorithm_is_refused() -> None:
    with pytest.raises(ValueError, match="hash_algorithm ''"):
        SCDContext({**_DV, "scd_type": "dv_hub", "hash_algorithm": ""}).validate()


@pytest.mark.parametrize("separator", ["", "a'b", "a\\b", "`", "$", "${x}", "\t"])
def test_a_separator_that_cannot_be_sealed_is_refused(separator: str) -> None:
    """F158: the separator is sealed as a table property; refused before any staging runs."""
    with pytest.raises(ValueError, match="hash_separator .* cannot be sealed"):
        SCDContext({**_DV, "scd_type": "dv_satellite", "hash_separator": separator}).validate()


@pytest.mark.parametrize("separator", ["||", "~", "|", "#"])
def test_a_plain_separator_is_accepted(separator: str) -> None:
    SCDContext({**_DV, "scd_type": "dv_satellite", "hash_separator": separator}).validate()

