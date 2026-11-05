"""F158 — the Data Vault hash diff, and sealing its settings on the table.

`hash_hex` is the hash diff a satellite stores: the columns joined by the separator, NULL as a
token, then sha256 or md5 as lower-case hex, reproducible outside Spark. The seal records the
settings on the table at its first write and refuses a later write that would compute the diff
another way: a satellite hashed two ways reports changes that did not happen.
"""
from __future__ import annotations

import hashlib
from unittest.mock import MagicMock

import pytest

from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter
from spark_quick_start.orchestration.transformers._hashing import (
    HashSettings,
    check_hash_seal,
    hash_hex,
    write_hash_seal,
)

_SHA = HashSettings(algorithm="sha256", separator="||", null_token="<NULL>", time_zone="UTC")
_MD5 = HashSettings(algorithm="md5", separator="||", null_token="<NULL>", time_zone="UTC")


def _digest(text: str, algorithm: str) -> str:
    return hashlib.new(algorithm, text.encode("utf-8")).hexdigest()


# ── hash_hex ─────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("settings", [_SHA, _MD5], ids=["sha256", "md5"])
def test_the_diff_is_the_hex_digest_of_the_joined_columns(spark, settings):
    df = spark.createDataFrame([("Ada", 36, None)], "name string, age int, city string")
    got = df.select(hash_hex(["name", "age", "city"], settings).alias("h")).first()["h"]
    assert got == _digest("Ada||36||<NULL>", settings.algorithm)


def test_the_diff_follows_the_order_it_is_given(spark):
    df = spark.createDataFrame([("a", "b")], "x string, y string")
    xy = df.select(hash_hex(["x", "y"], _SHA)).first()[0]
    yx = df.select(hash_hex(["y", "x"], _SHA)).first()[0]
    assert xy == _digest("a||b", "sha256") and yx == _digest("b||a", "sha256")


def test_an_empty_string_and_null_hash_differently(spark):
    df = spark.createDataFrame([("",), (None,)], "x string")
    empty, null = (r[0] for r in df.select(hash_hex(["x"], _SHA)).collect())
    assert empty != null


def test_unknown_settings_are_refused():
    with pytest.raises(ValueError, match="algorithm"):
        HashSettings(algorithm="sha1", separator="||", null_token="<NULL>", time_zone="UTC")


# ── the seal ─────────────────────────────────────────────────────────────────

_PROPS = {"spark_quick_start.hash_algorithm": "sha256", "spark_quick_start.hash_separator": "||",
          "spark_quick_start.hash_null_token": "<NULL>", "spark_quick_start.hash_time_zone": "UTC"}


def test_a_missing_table_needs_sealing_after_its_first_write():
    adapter = MockPlatformAdapter()
    assert check_hash_seal(adapter, MagicMock(), "/vault/s", _SHA) is True
    assert adapter.get_table_properties_calls == []


def test_a_table_sealed_with_the_same_settings_needs_nothing():
    adapter = MockPlatformAdapter(existing_tables={"/vault/s"}, table_properties={"/vault/s": dict(_PROPS)})
    assert check_hash_seal(adapter, MagicMock(), "/vault/s", _SHA) is False


def test_a_table_sealed_with_other_settings_is_refused_quoting_both():
    adapter = MockPlatformAdapter(existing_tables={"/vault/s"}, table_properties={"/vault/s": dict(_PROPS)})
    with pytest.raises(ValueError, match="(?s)sealed.*sha256.*md5"):
        check_hash_seal(adapter, MagicMock(), "/vault/s", _MD5)


def test_an_empty_unsealed_table_counts_as_a_first_write():
    empty = MagicMock()
    empty.isEmpty.return_value = True
    adapter = MockPlatformAdapter(existing_tables={"/vault/s"}, delta_read_result=empty)
    assert check_hash_seal(adapter, MagicMock(), "/vault/s", _SHA) is True


def test_a_populated_unsealed_table_is_refused_not_sealed_late():
    """The architect's check-then-act risk: a crash between the first write and the seal."""
    populated = MagicMock()
    populated.isEmpty.return_value = False
    adapter = MockPlatformAdapter(existing_tables={"/vault/s"}, delta_read_result=populated)
    with pytest.raises(ValueError, match="has rows but no hash seal"):
        check_hash_seal(adapter, MagicMock(), "/vault/s", _SHA)
    assert adapter.set_table_properties_calls == []


def test_a_partial_seal_is_refused():
    adapter = MockPlatformAdapter(existing_tables={"/vault/s"},
                                  table_properties={"/vault/s": {"spark_quick_start.hash_algorithm": "sha256"}})
    with pytest.raises(ValueError, match="sealed"):
        check_hash_seal(adapter, MagicMock(), "/vault/s", _SHA)


def test_writing_the_seal_sets_every_setting_once():
    adapter = MockPlatformAdapter(existing_tables={"/vault/s"})
    write_hash_seal(adapter, MagicMock(), "/vault/s", _SHA)
    assert adapter.set_table_properties_calls == [("/vault/s", _PROPS)]
    assert check_hash_seal(adapter, MagicMock(), "/vault/s", _SHA) is False


# ── review findings, F158 ────────────────────────────────────────────────────

@pytest.mark.parametrize("field,value", [("separator", ""), ("null_token", ""), ("null_token", "a'b"),
                                         ("null_token", "${x}"), ("separator", "\t"), ("time_zone", "")])
def test_a_setting_that_cannot_be_sealed_is_refused_before_any_write(field, value):
    """Refused after the first write, it would leave a populated table no later load can seal."""
    kwargs = {"algorithm": "sha256", "separator": "||", "null_token": "<NULL>", "time_zone": "UTC", field: value}
    with pytest.raises(ValueError):
        HashSettings(**kwargs)


def test_every_character_the_adapter_refuses_is_refused_here_first():
    """Over all of ASCII, so a character added to either list shows up as a mismatch."""
    from spark_quick_start.adapters.platform_adapter import _property_literals

    def refused(check) -> bool:
        try:
            check()
        except ValueError:
            return True
        return False

    for code in range(128):
        ch = chr(code)
        value = f"a{ch}b"
        by_adapter = refused(lambda v=value: _property_literals({"spark_quick_start.x": v}))
        by_settings = refused(lambda v=value: HashSettings(algorithm="sha256", separator=v,
                                                           null_token="<NULL>", time_zone="UTC"))
        assert by_adapter == by_settings, f"{ch!r}: adapter {by_adapter}, HashSettings {by_settings}"
    for ch in "'\\`$\x00\n\x7f":
        with pytest.raises(ValueError):
            _property_literals({"spark_quick_start.x": f"a{ch}b"})
        with pytest.raises(ValueError):
            HashSettings(algorithm="sha256", separator=f"a{ch}b", null_token="<NULL>", time_zone="UTC")


def test_a_load_in_another_session_time_zone_is_refused():
    """A timestamp is cast to string in the session time zone; the same row would hash differently."""
    adapter = MockPlatformAdapter(existing_tables={"/vault/s"}, table_properties={"/vault/s": dict(_PROPS)})
    berlin = HashSettings(algorithm="sha256", separator="||", null_token="<NULL>", time_zone="Europe/Berlin")
    with pytest.raises(ValueError, match="(?s)sealed.*UTC.*Europe/Berlin"):
        check_hash_seal(adapter, MagicMock(), "/vault/s", berlin)


def test_writing_the_seal_never_overwrites_a_different_one():
    """Two first writers racing, or a table delta_table_exists failed to see."""
    adapter = MockPlatformAdapter(existing_tables={"/vault/s"}, table_properties={"/vault/s": dict(_PROPS)})
    with pytest.raises(ValueError, match="another load sealed it first"):
        write_hash_seal(adapter, MagicMock(), "/vault/s", _MD5)
    assert adapter.set_table_properties_calls == []


def test_writing_the_same_seal_twice_is_a_no_op():
    adapter = MockPlatformAdapter(existing_tables={"/vault/s"}, table_properties={"/vault/s": dict(_PROPS)})
    write_hash_seal(adapter, MagicMock(), "/vault/s", _SHA)
    assert adapter.set_table_properties_calls == []


def test_the_null_token_is_the_one_the_scd_detector_uses():
    import inspect

    from spark_quick_start.orchestration.models import Constants as ModelConstants
    from spark_quick_start.orchestration.transformers import SCDChangeDetectorTransformer
    from spark_quick_start.orchestration.transformers._constants import Constants
    default = inspect.signature(SCDChangeDetectorTransformer).parameters["hash_null_token"].default
    assert default == Constants.HASH_NULL_TOKEN == ModelConstants.HASH_NULL_TOKEN == "<NULL>"
    assert Constants.DV_HASH_ALGORITHMS == ModelConstants.DV_HASH_ALGORITHMS

