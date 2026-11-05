from spark_quick_start.orchestration.column_policy import ColumnNamingPolicy


def test_default_uses_scd_prefix():
    p = ColumnNamingPolicy.default()
    assert p.row_key_column == "scdRowKey"
    assert p.hash_column == "scdHash"
    assert p.version_column == "scdVersion"
    assert p.effective_from_column == "scdEffectiveFrom"
    assert p.effective_to_column == "scdEffectiveTo"
    assert p.current_flag_column == "scdCurrentFlag"
    assert p.deleted_flag_column == "scdDeletedFlag"
    assert p.source_system_column == "scdSourceSystem"
    assert p.correlation_id_column == "scdCorrelationId"


def test_custom_policy():
    p = ColumnNamingPolicy(
        row_key_column="natKey",
        hash_column="natHash",
        version_column="natVersion",
        effective_from_column="validFrom",
        effective_to_column="validTo",
        current_flag_column="isCurrent",
        deleted_flag_column="isDeleted",
        source_system_column="sourceSystem",
        correlation_id_column="correlationId",
    )
    assert p.row_key_column == "natKey"
    assert p.hash_column == "natHash"


def test_policy_is_frozen():
    import dataclasses

    import pytest
    p = ColumnNamingPolicy.default()
    with pytest.raises(dataclasses.FrozenInstanceError):
        p.row_key_column = "x"  # type: ignore[misc]


# ── F170: the Data Vault load-audit names and separator have one home ────────
# They are `Constants`, not policy fields: every ColumnNamingPolicy field is read as an SCD meta
# column (load_pipeline._scd1_update_columns), which would stop an SCD1 business column named
# LoadTS from being updated.

def test_the_load_timestamp_names_are_constants_in_both_copies():
    from spark_quick_start.orchestration.models import Constants as ModelsConstants
    from spark_quick_start.orchestration.transformers._constants import Constants as TxConstants

    for c in (ModelsConstants, TxConstants):
        assert (c.DV_LOAD_TS_COLUMN, c.DV_LOAD_END_TS_COLUMN, c.DV_HASH_SEPARATOR) == (
            "LoadTS", "LoadEndTS", "||")


def test_a_data_vault_scd_context_defaults_come_from_those_constants(monkeypatch):
    from spark_quick_start.orchestration import models

    monkeypatch.setattr(models.Constants, "DV_LOAD_TS_COLUMN", "X1")
    monkeypatch.setattr(models.Constants, "DV_LOAD_END_TS_COLUMN", "X2")
    monkeypatch.setattr(models.Constants, "DV_HASH_SEPARATOR", "##")
    ctx = models.SCDContext({"business_key_column": "HashKey", "scd_type": "dv_hub"})
    assert (ctx.load_ts_column, ctx.load_end_ts_column, ctx.hash_separator) == ("X1", "X2", "##")
