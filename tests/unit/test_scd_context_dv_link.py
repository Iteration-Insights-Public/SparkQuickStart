"""F167 — `dv_link` in scd_context, and `link_hub_key_columns`.

The accepted Data Vault link design (docs/architecture/decisions/2026-10-01-data-vault-link-design.md):
a link's key is `business_key_column`, the link hash key; `link_hub_key_columns` names the staged
columns holding each connected hub's hash key, at least two, distinct, none the key or a load
column. Required under `dv_link`, refused under every other style.
"""
from __future__ import annotations

import pytest

from spark_quick_start.orchestration.models import SCDContext

_LINK = {"business_key_column": "LinkHashKey", "scd_type": "dv_link",
         "link_hub_key_columns": ["OrderHashKey", "ProductHashKey"]}


def test_a_link_context_is_accepted_with_data_vault_defaults() -> None:
    ctx = SCDContext(_LINK)
    ctx.validate()
    assert ctx.is_data_vault
    assert ctx.link_hub_key_columns == ["OrderHashKey", "ProductHashKey"]
    assert (ctx.source_system_column, ctx.load_ts_column) == ("RecordSource", "LoadTS")


@pytest.mark.parametrize("columns", [None, [], ["OrderHashKey"]], ids=["absent", "empty", "one"])
def test_a_link_needs_two_hub_key_columns(columns) -> None:
    data = {**_LINK, "link_hub_key_columns": columns}
    with pytest.raises(ValueError, match="link_hub_key_columns.*two"):
        SCDContext(data).validate()


@pytest.mark.parametrize("columns,named", [
    (["OrderHashKey", "OrderHashKey"], "OrderHashKey"),
    (["OrderHashKey", "orderhashkey"], "orderhashkey"),
    (["OrderHashKey", "LinkHashKey"], "LinkHashKey"),
    (["OrderHashKey", "LoadTS"], "LoadTS"),
    (["OrderHashKey", "RecordSource"], "RecordSource"),
    (["OrderHashKey", ""], "''"),
    (["OrderHashKey", 7], "7"),
], ids=["duplicate", "case-only duplicate", "the link key", "a load column", "the source system",
        "blank", "not a string"])
def test_a_hub_key_column_that_cannot_be_one_is_refused_naming_it(columns, named) -> None:
    with pytest.raises(ValueError, match=rf"link_hub_key_columns.*{named}"):
        SCDContext({**_LINK, "link_hub_key_columns": columns}).validate()


@pytest.mark.parametrize("style", ["scd2", "dv_hub", "dv_satellite"])
def test_another_style_refuses_the_key(style) -> None:
    key = "HashKey" if style.startswith("dv") else "id"
    ctx = SCDContext({"business_key_column": key, "scd_type": style,
                      "link_hub_key_columns": ["A", "B"]})
    with pytest.raises(ValueError, match=rf"link_hub_key_columns.*{style}"):
        ctx.validate()


def test_a_null_value_counts_as_not_given_under_another_style() -> None:
    SCDContext({"business_key_column": "HashKey", "scd_type": "dv_hub", "link_hub_key_columns": None}).validate()


def test_a_string_is_refused_rather_than_split_into_characters() -> None:
    """`list("AB")` is `["A", "B"]`: two plausible column names nobody wrote."""
    with pytest.raises(ValueError, match="link_hub_key_columns.*list"):
        SCDContext({**_LINK, "link_hub_key_columns": "AB"}).validate()
