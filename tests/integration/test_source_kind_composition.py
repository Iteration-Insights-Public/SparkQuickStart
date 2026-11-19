"""F222 — a source step's `source_kind` must match the load context's `source_context.kind`.

Checked when the pipeline is assembled, before the source step runs, including both sources
of a `.set_source()` choice.
"""
from __future__ import annotations

import json

import pytest

from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter
from spark_quick_start.orchestration import LoadOrchestrator
from spark_quick_start.orchestration.transformers import SourceTransformer

_CTX = {
    "global_context": {"id": "ent_1", "entity_name": "Customer", "source_system": "BC_Contoso",
                       "target_table_path": "/tmp/raw_customer", "staging_table_path": "/tmp/stage"},
    "scd_context": {"business_key_column": "id"},
    "runtime_context": {"correlation_id": "c", "pipeline_name": "p", "application_name": "a"},
    "logging_context": {"logging_level": "INFO", "log_file_name": "l", "log_file_dir": "/tmp/logs"},
}


class _Seen(Exception):
    pass


class _BC(SourceTransformer):
    source_kind = "business_central"
    seen_kind: str | None = None

    def _produce(self, ctx):
        type(self).seen_kind = ctx.source_ctx.kind
        raise _Seen


class _Untagged(SourceTransformer):
    def _produce(self, ctx):
        raise _Seen


def _run(spark, source, **sections):
    data = {**_CTX, **sections}
    return LoadOrchestrator(json.dumps(data), spark, adapter=MockPlatformAdapter()).add(source).run()


def test_a_matching_kind_reaches_the_source(spark):
    _BC.seen_kind = None
    result = _run(spark, _BC(), source_context={"kind": "business_central"})
    assert _BC.seen_kind == "business_central"
    assert result.status == "error"  # the source raised _Seen after reading the kind


def test_a_different_kind_is_refused_before_the_source_runs(spark):
    _BC.seen_kind = None
    result = _run(spark, _BC(), source_context={"kind": "salesforce"})
    message = result.error_details["error_message"]
    assert "business_central" in message and "salesforce" in message
    assert _BC.seen_kind is None


def test_a_tagged_source_without_source_context_is_refused(spark):
    stages = {"stages": [{"nb_name": "NB"}]}
    data = {**_CTX, "staging_context": stages}
    data["global_context"] = {**_CTX["global_context"], "staging_table_schema": {"type": "struct", "fields": []}}
    result = LoadOrchestrator(json.dumps(data), spark, adapter=MockPlatformAdapter()).add(_BC()).run()
    assert "source_context" in result.error_details["error_message"]
    assert _BC.seen_kind is None


def test_an_untagged_source_takes_any_kind(spark):
    result = _run(spark, _Untagged(), source_context={"kind": "anything"})
    assert "source_kind" not in result.error_details["error_message"]


@pytest.mark.parametrize("predicate", [True, False])
def test_a_conditional_source_checks_both_branches(spark, predicate):
    _BC.seen_kind = None
    orchestrator = LoadOrchestrator(json.dumps({**_CTX, "source_context": {"kind": "salesforce"}}), spark,
                                    adapter=MockPlatformAdapter())
    result = orchestrator.set_source(predicate, _Untagged(), _BC()).run()
    assert "business_central" in result.error_details["error_message"]
    assert _BC.seen_kind is None

