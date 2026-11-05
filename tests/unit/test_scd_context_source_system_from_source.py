from __future__ import annotations

from spark_quick_start.orchestration.models import SCDContext


def test_source_system_from_source_defaults_to_false():
    ctx = SCDContext({"business_key_column": "id"})
    assert ctx.source_system_from_source is False


def test_source_system_from_source_can_be_set_true():
    ctx = SCDContext({"business_key_column": "id", "source_system_from_source": True})
    assert ctx.source_system_from_source is True


def test_source_system_from_source_false_explicit():
    ctx = SCDContext({"business_key_column": "id", "source_system_from_source": False})
    assert ctx.source_system_from_source is False
