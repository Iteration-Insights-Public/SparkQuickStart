"""The columns an SCD1 MERGE updates, chosen by the context's meta-column names (F150). Pure Python."""
from __future__ import annotations

from spark_quick_start.orchestration.column_policy import ColumnNamingPolicy
from spark_quick_start.orchestration.load_pipeline import _scd1_update_columns
from spark_quick_start.orchestration.models import SCDContext

_BASE = {"business_key_column": "customer_id", "scd_type": "scd1"}
_NOT_UPDATED = ("row_key_column", "effective_to_column", "source_system_column")
_REWRITTEN = ("current_flag_column", "effective_from_column", "hash_column", "deleted_flag_column",
              "version_column", "correlation_id_column")


def _all_columns(ctx):
    return ["customer_id", "name", "scd_region", *sorted(_meta(ctx))]


def _meta(ctx):
    return {getattr(ctx, f) for f in ColumnNamingPolicy.__dataclass_fields__}


def test_every_policy_field_is_a_resolved_context_attribute():
    for ctx in (SCDContext(_BASE), SCDContext({**_BASE, "hash_column": "h", "row_key_column": "rk"})):
        names = [getattr(ctx, f) for f in ColumnNamingPolicy.__dataclass_fields__]
        assert len(names) == len(set(names)) == 9 and all(isinstance(n, str) for n in names)


def test_a_per_column_override_replaces_the_default_name_in_the_excluded_set():
    ctx = SCDContext({**_BASE, "hash_column": "h"})
    update = _scd1_update_columns(ctx, ["customer_id", "scdHash", "h"])
    assert "scdHash" in update and update.count("h") == 1  # scdHash is now a business column


def test_a_policy_field_added_later_is_excluded_without_an_edit(monkeypatch):
    import dataclasses

    @dataclasses.dataclass(frozen=True)
    class Wider(ColumnNamingPolicy):
        extra_column: str = "extra"

    ctx = SCDContext(_BASE)
    ctx.extra_column = "extra"
    monkeypatch.setattr("spark_quick_start.orchestration.load_pipeline.ColumnNamingPolicy", Wider)
    assert "extra" not in _scd1_update_columns(ctx, ["name", "extra"])


def test_a_business_column_starting_with_scd_is_updated():
    ctx = SCDContext(_BASE)
    assert {"name", "scd_region", "customer_id"} <= set(_scd1_update_columns(ctx, _all_columns(ctx)))


def test_the_rewritten_meta_columns_are_updated_and_identity_ones_are_not():
    ctx = SCDContext(_BASE)
    update = _scd1_update_columns(ctx, _all_columns(ctx))
    assert {getattr(ctx, f) for f in _REWRITTEN} <= set(update)
    assert not {getattr(ctx, f) for f in _NOT_UPDATED} & set(update)


def test_renamed_meta_columns_are_excluded_by_name():
    ctx = SCDContext({**_BASE, "row_key_column": "rk", "effective_to_column": "valid_to",
                      "source_system_column": "src_sys", "correlation_id_column": "run_id"})
    update = _scd1_update_columns(ctx, _all_columns(ctx))
    assert not {"rk", "valid_to", "src_sys"} & set(update)
    assert "run_id" in update
    assert len(update) == len(set(update))


def test_an_unlisted_candidate_meta_column_is_not_duplicated():
    ctx = SCDContext(_BASE)
    update = _scd1_update_columns(ctx, ["customer_id", "name", ctx.hash_column])
    assert update.count(ctx.hash_column) == 1
