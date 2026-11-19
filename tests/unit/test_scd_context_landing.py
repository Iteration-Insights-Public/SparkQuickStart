"""scd_type "landing": the keys it takes and refuses, and the steps it admits (F223)."""
from __future__ import annotations

import json

import pytest

from spark_quick_start.orchestration import SCDContext
from spark_quick_start.orchestration.context_manager import LoadContextManager
from spark_quick_start.orchestration.models import _DATA_VAULT_ONLY_KEYS, _SCD_ONLY_KEYS


def _landing(**keys) -> SCDContext:
    ctx = SCDContext({"scd_type": "landing", "business_key_column": "SystemId", **keys})
    ctx.validate()
    return ctx


def test_landing_takes_the_key_and_the_load_time_column():
    ctx = _landing(load_ts_column="LandedAt")
    assert ctx.scd_type == "landing" and ctx.load_ts_column == "LandedAt"
    assert not ctx.is_data_vault


def test_load_ts_column_defaults_to_LoadTS():
    assert _landing().load_ts_column == "LoadTS"


_VALUES = {"scd_row_hashing": True, "default_max_timestamp": "2999-12-31T00:00:00+00:00",
           "hash_algorithm": "md5", "load_type": "Full", "source_system_from_source": True,
           "exclude_from_hash": ["a"], "link_hub_key_columns": ["a", "b"]}


@pytest.mark.parametrize("key", [*_SCD_ONLY_KEYS, *(k for k in _DATA_VAULT_ONLY_KEYS if k != "load_ts_column"),
                                 "load_type", "hash_column", "row_key_column", "source_system_column",
                                 "source_system_from_source", "exclude_from_hash", "hash_separator",
                                 "link_hub_key_columns"])
def test_landing_refuses_the_other_styles_keys(key):
    with pytest.raises(ValueError) as raised:
        _landing(**{key: _VALUES.get(key, "x")})
    assert key in str(raised.value) and "landing" in str(raised.value)


@pytest.mark.parametrize("blank", ["", "   "])
def test_a_blank_load_ts_column_is_refused(blank):
    with pytest.raises(ValueError, match="load_ts_column"):
        _landing(load_ts_column=blank)


def test_a_load_ts_column_named_as_the_key_is_refused():
    with pytest.raises(ValueError, match="collide"):
        _landing(load_ts_column="SystemId")


def test_the_scd_and_data_vault_steps_refuse_landing():
    ctx = _landing()
    with pytest.raises(ValueError, match="landing"):
        ctx.require_scd_style("NativeSCDMergeTransformer")
    with pytest.raises(ValueError, match="landing"):
        ctx.require_data_vault_style("DVHubWriterTransformer", "dv_hub")


def test_require_scd_type_names_the_step_and_both_styles():
    ctx = SCDContext({"business_key_column": "k"})
    with pytest.raises(ValueError) as raised:
        ctx.require_scd_type("KeyMergeTransformer", "landing")
    assert all(word in str(raised.value) for word in ("KeyMergeTransformer", "landing", "scd2"))
    _landing().require_scd_type("KeyMergeTransformer", "landing")


def _manager(**sections) -> LoadContextManager:
    data = {
        "global_context": {"id": "ent_1", "entity_name": "Customer", "source_system": "BC_Contoso",
                           "target_table_path": "/tmp/raw", "staging_table_path": "/tmp/unused"},
        "scd_context": {"scd_type": "landing", "business_key_column": "SystemId"},
        "runtime_context": {"correlation_id": "c", "pipeline_name": "p", "application_name": "a"},
        "logging_context": {"logging_level": "INFO", "log_file_name": "l", "log_file_dir": "/tmp/logs"},
        "source_context": {"kind": "test"},
        **sections,
    }
    return LoadContextManager(json.dumps(data)).parse("", "")


def test_a_landing_load_requires_hwm_context():
    with pytest.raises(ValueError) as raised:
        _manager().validate_all()
    assert "hwm_context" in str(raised.value) and "landing" in str(raised.value)
    _manager(hwm_context={"hwm_column_name": "modified_at"}).validate_all()
