"""F211 — the seal of an SCD table's flag values, open-row sentinel and hash separator.

The comparison logic against `MockPlatformAdapter`: which table states mean "seal after the write",
which are refused and with what message, and that a swallowed existence check cannot be consulted
when the target step has answered.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from spark_quick_start.adapters.platform_adapter import (
    MockPlatformAdapter,
    PlatformAdapter,
    UnsupportedTableIdentifier,
)
from spark_quick_start.orchestration.models import SCDContext
from spark_quick_start.orchestration.transformers._scd_seal import (
    ScdSettings,
    check_scd_seal,
    scd_settings,
    seal_after_write,
    write_scd_seal,
)
from spark_quick_start.orchestration.transformers.state import PipelineState

_T = "/silver/customer"
def _spark(zone="UTC"):
    return SimpleNamespace(conf=SimpleNamespace(get=lambda key: zone))


_SETTINGS = scd_settings(SCDContext({"business_key_column": "id"}), spark=_spark())
_SEAL = _SETTINGS.as_properties()


def _adapter(properties=None, *, exists=True):
    return MockPlatformAdapter(existing_tables={_T} if exists else set(),
                               table_properties={_T: properties} if properties is not None else None)


def test_the_settings_come_from_the_context_with_the_sentinel_as_a_utc_instant():
    assert _SEAL == {
        "spark_quick_start.scd_current_flag_value": "Y", "spark_quick_start.scd_inactive_flag_value": "N",
        "spark_quick_start.scd_deleted_flag_value": "Y", "spark_quick_start.scd_not_deleted_flag_value": "N",
        "spark_quick_start.scd_default_max_timestamp": "2999-12-31T23:59:59.999999+00:00",
        "spark_quick_start.scd_hash_separator": "~",
        "spark_quick_start.scd_time_zone": "UTC",
    }


def test_two_spellings_of_one_sentinel_seal_alike():
    a = scd_settings(SCDContext({"business_key_column": "id", "default_max_timestamp": "2500-06-01T12:00:00Z"}), spark=_spark())
    b = scd_settings(SCDContext({"business_key_column": "id", "default_max_timestamp": "2500-06-01 14:00:00+02:00"}), spark=_spark())
    assert a == b


def test_the_separator_the_detector_used_overrides_the_contexts():
    assert scd_settings(SCDContext({"business_key_column": "id"}), "|", spark=_spark()).hash_separator == "|"


def test_the_time_zone_is_the_sessions_and_falls_back_to_utc_when_unset():
    ctx = SCDContext({"business_key_column": "id"})
    assert scd_settings(ctx, spark=_spark("Europe/Kyiv")).time_zone == "Europe/Kyiv"
    assert scd_settings(ctx, spark=_spark(None)).time_zone == "UTC"
    assert scd_settings(ctx, spark=_spark("")).time_zone == "UTC"


def test_a_sealed_time_zone_that_differs_is_refused_with_both_values_and_the_consequence():
    sealed = {**_SEAL, "spark_quick_start.scd_time_zone": "Europe/Kyiv"}
    with pytest.raises(ValueError) as caught:
        check_scd_seal(_adapter(sealed), MagicMock(), _T, _SETTINGS, target_exists=True, logger=MagicMock())
    message = str(caught.value)
    assert "spark_quick_start.scd_time_zone" in message and "'Europe/Kyiv'" in message and "'UTC'" in message
    assert "session time zone" in message and "every timestamp column" in message
    assert "spark.sql.session.timeZone = 'Europe/Kyiv'" in message and "no UPDATE applies" in message
    assert "Align the load's scd_context" not in message


def test_a_table_sealed_without_the_time_zone_is_completed_not_refused():
    adapter = _adapter({k: v for k, v in _SEAL.items() if not k.endswith("time_zone")})
    assert check_scd_seal(adapter, MagicMock(), _T, _SETTINGS, target_exists=True, logger=MagicMock()) == "unsealed"
    assert write_scd_seal(adapter, MagicMock(), _T, _SETTINGS) == ["spark_quick_start.scd_time_zone"]


@pytest.mark.parametrize("field", sorted(_SEAL))
def test_a_value_that_cannot_be_sealed_is_refused_before_any_write(field):
    values = {k.removeprefix("spark_quick_start.scd_"): v for k, v in _SEAL.items()}
    values[field.removeprefix("spark_quick_start.scd_")] = "it's"
    with pytest.raises(ValueError, match="cannot be sealed"):
        ScdSettings(**values)


def test_an_absent_target_is_a_first_write_and_nothing_is_read():
    adapter = _adapter(exists=False)
    assert check_scd_seal(adapter, MagicMock(), _T, _SETTINGS, target_exists=False, logger=MagicMock()) == "first_write"
    assert adapter.get_table_properties_calls == []


def test_the_target_steps_answer_is_not_asked_twice():
    """A swallowed delta_table_exists must not make an existing, sealed table look absent (F209)."""
    adapter = _adapter(_SEAL, exists=False)  # the mock would answer "exists" for it via its properties
    adapter.delta_table_exists = MagicMock(return_value=False)
    assert check_scd_seal(adapter, MagicMock(), _T, _SETTINGS, target_exists=True, logger=MagicMock()) == "sealed"
    adapter.delta_table_exists.assert_not_called()


def test_no_target_step_falls_back_to_the_existence_check():
    assert check_scd_seal(_adapter(exists=False), MagicMock(), _T, _SETTINGS, target_exists=None, logger=MagicMock()) == "first_write"
    assert check_scd_seal(_adapter(_SEAL), MagicMock(), _T, _SETTINGS, target_exists=None, logger=MagicMock()) == "sealed"


def test_a_matching_seal_needs_nothing_and_costs_one_metadata_read():
    adapter = _adapter({**_SEAL, "delta.appendOnly": "false"})
    assert check_scd_seal(adapter, MagicMock(), _T, _SETTINGS, target_exists=True, logger=MagicMock()) == "sealed"
    assert adapter.get_table_properties_calls == [_T]


def test_a_populated_table_with_no_seal_is_not_refused():
    assert check_scd_seal(_adapter({}), MagicMock(), _T, _SETTINGS, target_exists=True, logger=MagicMock()) == "unsealed"


def test_a_partial_seal_is_completed_not_refused():
    partial = dict(list(_SEAL.items())[:2])
    adapter = _adapter(partial)
    assert check_scd_seal(adapter, MagicMock(), _T, _SETTINGS, target_exists=True, logger=MagicMock()) == "unsealed"
    assert write_scd_seal(adapter, MagicMock(), _T, _SETTINGS) == sorted(set(_SEAL) - set(partial))


@pytest.mark.parametrize("prop", sorted(_SEAL))
def test_a_differing_value_is_refused_naming_table_property_both_values_and_the_procedure(prop):
    adapter = _adapter({**_SEAL, prop: "OTHER"})
    with pytest.raises(ValueError) as caught:
        check_scd_seal(adapter, MagicMock(), _T, _SETTINGS, target_exists=True, logger=MagicMock())
    message = str(caught.value)
    assert _T in message and prop in message and "'OTHER'" in message and repr(_SEAL[prop]) in message
    assert "UPDATE" in message and "TBLPROPERTIES" in message and "load-context.md" in message
    assert adapter.set_table_properties_calls == []


def test_writing_the_seal_sets_each_property_once_and_is_idempotent():
    adapter = _adapter({})
    assert write_scd_seal(adapter, MagicMock(), _T, _SETTINGS) == sorted(_SEAL)
    assert adapter.set_table_properties_calls == [(_T, _SEAL)]
    assert write_scd_seal(adapter, MagicMock(), _T, _SETTINGS) == []
    assert len(adapter.set_table_properties_calls) == 1


class _Refusing(MockPlatformAdapter):
    """An adapter whose property helpers refuse the identifier, as the real ones do."""

    def get_table_properties(self, spark, identifier):
        self.get_table_properties_calls.append(identifier)
        raise UnsupportedTableIdentifier(f"cannot quote {identifier!r}")

    def set_table_properties(self, spark, identifier, properties):
        raise UnsupportedTableIdentifier(f"cannot quote {identifier!r}")


def test_an_existing_target_the_helpers_refuse_is_neither_checked_nor_sealed_and_warns_once():
    adapter, logger = _Refusing(existing_tables={_T}), MagicMock()
    assert check_scd_seal(adapter, MagicMock(), _T, _SETTINGS, target_exists=True, logger=logger) == "unsupported"
    (message,) = [c.args[0] for c in logger.warning.call_args_list]
    assert _T in message and "not a target the seal supports" in message and "cannot quote" in message
    seal_after_write(adapter, MagicMock(), _T, _SETTINGS, "unsupported", logger)
    assert len(logger.warning.call_args_list) == 1


def test_any_other_error_reading_the_properties_still_propagates_before_the_write():
    class _Broken(MockPlatformAdapter):
        def get_table_properties(self, spark, identifier):
            raise ValueError("a different ValueError")

    with pytest.raises(ValueError, match="a different ValueError"):
        check_scd_seal(_Broken(existing_tables={_T}), MagicMock(), _T, _SETTINGS,
                       target_exists=True, logger=MagicMock())


def test_a_first_write_is_not_checked_and_an_unsupported_identifier_found_at_the_seal_is_a_warning():
    adapter, logger = _Refusing(), MagicMock()
    assert check_scd_seal(adapter, MagicMock(), _T, _SETTINGS, target_exists=False, logger=logger) == "first_write"
    assert adapter.get_table_properties_calls == []
    seal_after_write(adapter, MagicMock(), _T, _SETTINGS, "first_write", logger)
    (message,) = [c.args[0] for c in logger.warning.call_args_list]
    assert _T in message and "not a target the seal supports" in message


@pytest.mark.parametrize("target", ["cat.sch-ema.tbl", "`cat`.sch.tbl", "a.b.c.d", "/lake/a`b", "/lake/$x"])
def test_the_real_property_helpers_refuse_these_identifiers_with_the_specific_error(target):
    spark = MagicMock()
    with pytest.raises(UnsupportedTableIdentifier):
        PlatformAdapter.get_table_properties(MockPlatformAdapter(), spark, target)
    with pytest.raises(UnsupportedTableIdentifier):
        PlatformAdapter.set_table_properties(MockPlatformAdapter(), spark, target, {"spark_quick_start.scd_x": "y"})
    spark.sql.assert_not_called()


def test_a_refused_property_value_is_not_mistaken_for_an_unsupported_identifier():
    with pytest.raises(ValueError) as caught:
        PlatformAdapter.set_table_properties(MockPlatformAdapter(), MagicMock(), _T,
                                             {"spark_quick_start.scd_x": "it's"})
    assert not isinstance(caught.value, UnsupportedTableIdentifier)


def test_a_seal_that_fails_after_the_write_warns_with_the_reason_and_the_values_and_does_not_raise():
    adapter, logger = _adapter({}), MagicMock()
    adapter.set_table_properties = MagicMock(side_effect=PermissionError("needs MANAGE"))
    seal_after_write(adapter, MagicMock(), _T, _SETTINGS, "first_write", logger)
    (message,) = [c.args[0] for c in logger.warning.call_args_list]
    assert _T in message and "needs MANAGE" in message and "scd_hash_separator" in message


def test_a_seal_another_load_wrote_first_is_its_own_warning_and_not_a_retry_promise():
    adapter, logger = _adapter({**_SEAL, "spark_quick_start.scd_hash_separator": "|"}), MagicMock()
    seal_after_write(adapter, MagicMock(), _T, _SETTINGS, "unsealed", logger)
    (message,) = [c.args[0] for c in logger.warning.call_args_list]
    assert "another load sealed the table with different values" in message
    assert "two ways" in message and "will be refused" in message and "check the rows" in message
    assert "tries again" not in message
    assert adapter.set_table_properties_calls == []


def test_the_separator_refusal_names_both_places_it_can_come_from():
    adapter = _adapter({**_SEAL, "spark_quick_start.scd_hash_separator": "|"})
    with pytest.raises(ValueError, match=r"scd_context\.hash_separator.*SCDChangeDetectorTransformer"):
        check_scd_seal(adapter, MagicMock(), _T, _SETTINGS, target_exists=True, logger=MagicMock())


def test_writing_the_seal_never_overwrites_a_different_one():
    adapter = _adapter({**_SEAL, "spark_quick_start.scd_hash_separator": "|"})
    with pytest.raises(ValueError, match="another load sealed the table first"):
        write_scd_seal(adapter, MagicMock(), _T, _SETTINGS)
    assert adapter.set_table_properties_calls == []


def test_a_sealed_plan_touches_nothing_after_the_write():
    adapter, logger = _adapter(_SEAL), MagicMock()
    seal_after_write(adapter, MagicMock(), _T, _SETTINGS, "sealed", logger)
    assert adapter.get_table_properties_calls == [] and adapter.set_table_properties_calls == []
    logger.warning.assert_not_called()


def test_a_first_write_is_sealed_silently():
    adapter, logger = _adapter({}), MagicMock()
    seal_after_write(adapter, MagicMock(), _T, _SETTINGS, "first_write", logger)
    assert adapter.set_table_properties_calls == [(_T, _SEAL)]
    logger.warning.assert_not_called()


def test_an_unsealed_table_is_sealed_with_a_warning_naming_the_values():
    adapter, logger = _adapter({}), MagicMock()
    seal_after_write(adapter, MagicMock(), _T, _SETTINGS, "unsealed", logger)
    assert adapter.set_table_properties_calls == [(_T, _SEAL)]
    (message,) = [c.args[0] for c in logger.warning.call_args_list]
    assert _T in message and "no SCD seal" in message and "scd_hash_separator" in message


def test_the_pipeline_state_carries_the_separator_through_every_setter():
    state = PipelineState(df=MagicMock()).with_hash_separator("|")
    assert state.hash_separator == "|"
    assert state.with_df(MagicMock()).with_halted("x").with_debug_columns("c").hash_separator == "|"
    assert PipelineState(df=MagicMock()).hash_separator is None
