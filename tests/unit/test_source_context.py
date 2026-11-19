"""source_context: parsing, and the rule that a load context names exactly one kind of source (F222)."""
from __future__ import annotations

import json

import pytest

from spark_quick_start.orchestration import SourceContext
from spark_quick_start.orchestration.context_manager import LoadContextManager

_BASE = {
    "global_context": {"id": "ent_1", "entity_name": "Customer", "source_system": "BC_Contoso",
                       "target_table_path": "/tmp/raw_customer", "staging_table_path": "/tmp/stage"},
    "scd_context": {"business_key_column": "id"},
    "runtime_context": {"correlation_id": "c", "pipeline_name": "p", "application_name": "a"},
    "logging_context": {"logging_level": "INFO", "log_file_name": "l", "log_file_dir": "/tmp/logs"},
}
_STAGES = {"stages": [{"nb_name": "NB_Stage"}]}
_SCHEMA = {"type": "struct", "fields": [{"name": "id", "type": "integer", "nullable": True, "metadata": {}}]}
_DQD = {"source_kind": "delta", "source_identifier": "/tmp/raw", "selection": [{"name": "id", "expression": {"column": "id"}}]}
_SOURCE = {"kind": "business_central", "company": "Contoso"}


def _manager(**sections) -> LoadContextManager:
    data = json.loads(json.dumps(_BASE))
    if "staging_context" in sections:
        data["global_context"]["staging_table_schema"] = _SCHEMA
    data.update(sections)
    return LoadContextManager(json.dumps(data)).parse("", "")


def test_the_section_reads_as_a_mapping_with_a_kind():
    ctx = _manager(source_context=_SOURCE).source_ctx
    assert isinstance(ctx, SourceContext)
    assert ctx.kind == "business_central"
    assert ctx["company"] == "Contoso"
    assert ctx.get("absent", 1) == 1
    assert sorted(ctx) == sorted(_SOURCE) and len(ctx) == 2
    assert ctx.to_dict() == _SOURCE


def test_the_section_is_read_only_and_a_copy():
    raw = {"kind": "k", "nested": {"a": 1}}
    ctx = SourceContext.from_raw(raw)
    raw["kind"] = "changed"
    assert ctx.kind == "k"
    with pytest.raises(TypeError):
        ctx._data["kind"] = "x"  # type: ignore[index]


def test_no_section_gives_none():
    assert _manager(staging_context=_STAGES).source_ctx is None


@pytest.mark.parametrize("raw", [[], "business_central", 1])
def test_a_section_that_is_not_an_object_is_refused(raw):
    with pytest.raises(ValueError, match="source_context must be a JSON object"):
        _manager(source_context=raw)


@pytest.mark.parametrize("kind", [None, "", "   ", 3, ["a"]])
def test_a_missing_empty_or_non_string_kind_is_refused(kind):
    section = {"company": "Contoso"} if kind is None else {"kind": kind}
    with pytest.raises(ValueError, match="source_context.kind"):
        _manager(source_context=section)


def test_a_source_context_alone_passes_validation():
    _manager(source_context=_SOURCE).validate_all()


def test_no_source_at_all_names_the_three_sections():
    with pytest.raises(ValueError) as raised:
        _manager().validate_all()
    for section in ("staging_context.stages", "dqd_context", "source_context"):
        assert section in str(raised.value)


@pytest.mark.parametrize("other", ["staging_context", "dqd_context"])
def test_a_source_context_beside_another_source_is_refused(other):
    sections = {"source_context": _SOURCE, other: _STAGES if other == "staging_context" else _DQD}
    with pytest.raises(ValueError) as raised:
        _manager(**sections).validate_all()
    assert "source_context" in str(raised.value) and other in str(raised.value)


def test_stages_with_a_dqd_context_stay_accepted():
    _manager(staging_context=_STAGES, dqd_context=_DQD).validate_all()


def test_equality_and_pickling():
    import pickle

    ctx = SourceContext.from_raw(_SOURCE)
    assert ctx == SourceContext.from_raw(dict(_SOURCE))
    assert pickle.loads(pickle.dumps(ctx)) == ctx
    assert repr(ctx).startswith("SourceContext(")


def test_staging_refuses_a_context_with_no_stages():
    """Validation no longer requires stages once source_context exists, so the step itself refuses."""
    from spark_quick_start.orchestration.transformers import StagingTransformer

    step = StagingTransformer()
    step._ctx_manager = _manager(source_context=_SOURCE)
    with pytest.raises(ValueError, match="no staging phases"):
        step._produce(None)  # type: ignore[arg-type]
