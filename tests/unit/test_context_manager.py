"""
tests/test_context_manager.py — Unit tests for orchestration/context_manager.py.

No Spark required. All tests are pure Python.
"""
from __future__ import annotations

import json

import pytest

from spark_quick_start.orchestration.context_manager import LoadContextManager, parse_context

# ============================================================================
# FIXTURE — minimal valid context JSON
# ============================================================================

MINIMAL_CTX = {
    "global_context": {
        "id": "ent_1",
        "entity_name": "Person",
        "source_system": "SRC",
        "target_table_path": "/tmp/target",
        "staging_table_path": "/tmp/stage",
        "staging_table_schema": {"type": "struct", "fields": [{"name": "id", "type": "integer",
                                                                 "nullable": True, "metadata": {}}]},
    },
    "staging_context": {"stages": [{"nb_name": "NB_Stage_Person"}]},
    "scd_context": {"business_key_column": "id"},
    "runtime_context": {
        "correlation_id": "corr-abc",
        "pipeline_name": "PL_Person",
        "application_name": "EDW",
    },
    "logging_context": {
        "logging_level": "INFO",
        "log_file_name": "test_log",
        "log_file_dir": "/tmp/logs",
    },
}


def _minimal_json(**overrides) -> str:
    """Return a copy of MINIMAL_CTX serialised to JSON, with optional top-level overrides."""
    ctx = {**MINIMAL_CTX, **overrides}
    return json.dumps(ctx)


def _parsed_manager(**overrides) -> LoadContextManager:
    return LoadContextManager(_minimal_json(**overrides)).parse("", "")


# ============================================================================
# LoadContextManager — parse()
# ============================================================================


class TestLoadContextManagerParse:
    def test_parse_resolves_storage_placeholder(self):
        raw = json.dumps({
            **MINIMAL_CTX,
            "global_context": {
                **MINIMAL_CTX["global_context"],
                "staging_table_path": "{DataStorageLakehouse}/staging/person",
            },
        })
        mgr = LoadContextManager(raw).parse("/resolved_storage", "")
        assert "/resolved_storage" in mgr.global_ctx.staging_table_path

    def test_parse_resolves_eng_placeholder(self):
        raw = json.dumps({
            **MINIMAL_CTX,
            "global_context": {
                **MINIMAL_CTX["global_context"],
                "target_table_path": "{DataEngineeringLakehouse}/Tables/person",
            },
        })
        mgr = LoadContextManager(raw).parse("", "/resolved_eng")
        assert "/resolved_eng" in mgr.global_ctx.target_table_path

    def test_parse_fills_platform_context_with_the_resolved_paths(self):
        mgr = LoadContextManager(_minimal_json()).parse("/data_storage", "/data_eng")
        assert mgr.platform_ctx.data_storage_lakehouse_path == "/data_storage"
        assert mgr.platform_ctx.data_engineering_lakehouse_path == "/data_eng"

    def test_the_paths_are_no_longer_custom_params_of_global_context(self):
        mgr = LoadContextManager(_minimal_json()).parse("/data_storage", "/data_eng")
        assert not hasattr(mgr.global_ctx, "custom_params")

    def test_a_load_context_that_carries_platform_context_is_refused(self):
        raw = _minimal_json(platform_context={"data_storage_lakehouse_path": "/mine"})
        with pytest.raises(ValueError, match="platform_context.*library sets"):
            LoadContextManager(raw).parse("/data_storage", "/data_eng")

    def test_an_empty_platform_context_is_refused_too(self):
        with pytest.raises(ValueError, match="platform_context"):
            LoadContextManager(_minimal_json(platform_context={})).parse("", "")

    def test_without_extensions_context_it_is_empty(self):
        mgr = LoadContextManager(_minimal_json()).parse("", "")
        assert mgr.extensions_ctx.to_dict() == {}

    def test_extensions_context_is_parsed_whole(self):
        ext = {"acme.quality": {"max_null_ratio": 0.02}, "acme.rules": [1, "two", None]}
        mgr = LoadContextManager(_minimal_json(extensions_context=ext)).parse("", "")
        assert mgr.extensions_ctx["acme.quality"] == {"max_null_ratio": 0.02}
        assert mgr.extensions_ctx.to_dict() == ext
        with pytest.raises(TypeError):
            mgr.extensions_ctx["x"] = 1

    def test_placeholders_are_substituted_inside_extensions_context(self):
        raw = _minimal_json(extensions_context={"acme.path": "{DataStorageLakehouse}/rules"})
        mgr = LoadContextManager(raw).parse("/resolved", "")
        assert mgr.extensions_ctx["acme.path"] == "/resolved/rules"

    @pytest.mark.parametrize("bad", [["a"], "text", 3, None])
    def test_extensions_context_that_is_not_an_object_is_refused(self, bad):
        with pytest.raises(ValueError, match="extensions_context must be a JSON object"):
            LoadContextManager(_minimal_json(extensions_context=bad)).parse("", "")

    def test_an_unknown_global_context_key_is_refused_at_parse(self):
        raw = _minimal_json(global_context={**MINIMAL_CTX["global_context"], "my_param": 1})
        with pytest.raises(ValueError, match="my_param.*extensions_context"):
            LoadContextManager(raw).parse("", "")

    def test_unknown_keys_in_other_sections_keep_their_behaviour(self):
        # Only global_context became strict (owner, 2026-10-05).
        raw = _minimal_json(
            staging_context={**MINIMAL_CTX["staging_context"], "extra": 1},
            logging_context={**MINIMAL_CTX["logging_context"], "extra": 1},
            unknown_top_level_section={"a": 1},
        )
        LoadContextManager(raw).parse("", "")  # does not raise

    def test_parse_invalid_json_raises(self):
        mgr = LoadContextManager("not valid json at all }{")
        with pytest.raises((ValueError, Exception)):
            mgr.parse("", "")

    def test_parse_sets_all_five_contexts(self):
        mgr = LoadContextManager(_minimal_json()).parse("", "")
        assert mgr.global_ctx is not None
        assert mgr.staging_ctx is not None
        assert mgr.scd_ctx is not None
        assert mgr.runtime_ctx is not None
        assert mgr.logging_ctx is not None


# ============================================================================
# LoadContextManager — validate_all()
# ============================================================================


class TestLoadContextManagerValidate:
    def test_validate_all_passes_on_valid_json(self):
        mgr = LoadContextManager(_minimal_json()).parse("", "")
        mgr.validate_all()  # must not raise

    def test_validate_before_parse_raises(self):
        mgr = LoadContextManager(_minimal_json())
        with pytest.raises(ValueError, match="parsed before validation"):
            mgr.validate_all()

    @pytest.mark.parametrize("schema", [None, {}])
    def test_staging_stages_without_a_staging_table_schema_are_refused(self, schema):
        """F107: without a schema nothing merges the per-notebook tables into staging_table_path."""
        global_context = {k: v for k, v in MINIMAL_CTX["global_context"].items() if k != "staging_table_schema"}
        if schema is not None:
            global_context["staging_table_schema"] = schema
        mgr = LoadContextManager(json.dumps({**MINIMAL_CTX, "global_context": global_context})).parse("", "")
        with pytest.raises(ValueError, match=r"staging_context\.stages.*staging_table_schema"):
            mgr.validate_all()

    def test_a_dqd_load_without_stages_needs_no_staging_table_schema(self):
        global_context = {k: v for k, v in MINIMAL_CTX["global_context"].items() if k != "staging_table_schema"}
        raw = json.dumps({**MINIMAL_CTX, "global_context": global_context, "staging_context": {},
                          "dqd_context": {"source_kind": "delta", "source_identifier": "/src"}})
        LoadContextManager(raw).parse("", "").validate_all()  # must not raise

    def test_validate_missing_entity_name_raises(self):
        raw = json.dumps({
            **MINIMAL_CTX,
            "global_context": {
                **MINIMAL_CTX["global_context"],
                "entity_name": "",
            },
        })
        mgr = LoadContextManager(raw).parse("", "")
        with pytest.raises(ValueError, match="entity_name"):
            mgr.validate_all()


# ============================================================================
# LoadContextManager — build_staging_phase_context()
# ============================================================================


class TestLoadContextManagerBuildStagingPhaseContext:
    def test_returns_four_required_keys(self):
        mgr = _parsed_manager()
        phase = mgr.build_staging_phase_context(0, "NB_Stage_Person")
        assert "global_context" in phase
        assert "staging_context" in phase
        assert "runtime_context" in phase
        assert "logging_context" in phase

    def test_staging_table_path_includes_nb_name(self):
        mgr = _parsed_manager()
        phase = mgr.build_staging_phase_context(0, "NB_Stage_Person")
        assert phase["global_context"]["staging_table_path"].endswith("__NB_Stage_Person")

    def test_staging_table_path_unaffected_by_path_field(self):
        # path is the notebook execution location, not a table path modifier
        mgr = LoadContextManager(_minimal_json(
            staging_context={"stages": [{"nb_name": "NB_Stage_Person", "path": "/Workspace/Repos/project/NB_Stage_Person"}]}
        )).parse("", "")
        phase = mgr.build_staging_phase_context(0, "NB_Stage_Person")
        assert phase["global_context"]["staging_table_path"].endswith("__NB_Stage_Person")

    def test_invalid_stage_index_raises(self):
        mgr = _parsed_manager()
        with pytest.raises(ValueError, match="Invalid stage index"):
            mgr.build_staging_phase_context(99, "NB_Stage_Person")

    def test_before_parse_raises(self):
        mgr = LoadContextManager(_minimal_json())
        with pytest.raises(ValueError, match="parsed before"):
            mgr.build_staging_phase_context(0, "NB_Stage_Person")


# ============================================================================
# parse_context()
# ============================================================================


class TestParseContext:
    def _stage_ctx_str(self, include_correlation_id: bool = True) -> str:
        runtime: dict = {"pipeline_name": "PL_Person", "application_name": "EDW"}
        if include_correlation_id:
            runtime["correlation_id"] = "corr-abc"
        return json.dumps({
            "global_context": {"source_system": "SRC", "staging_table_path": "/tmp/stage"},
            "staging_context": {"filter_date": "2024-01-01"},
            "logging_context": {"logging_level": "INFO", "log_file_name": "test_log"},
            "runtime_context": runtime,
        })

    def test_returns_six_tuple(self):
        result = parse_context(self._stage_ctx_str(), adapter=None)
        assert len(result) == 6

    def test_global_context_source_system(self):
        gc, *_ = parse_context(self._stage_ctx_str(), adapter=None)
        assert gc["source_system"] == "SRC"

    def test_generates_correlation_id_if_missing(self):
        ctx_str = self._stage_ctx_str(include_correlation_id=False)
        _, _, _, _, cid, _ = parse_context(ctx_str, adapter=None)
        assert cid  # non-empty string
        assert isinstance(cid, str)

    def test_missing_required_key_raises(self):
        # logging_context is absent
        ctx_str = json.dumps({
            "global_context": {"source_system": "SRC"},
            "staging_context": {},
            "runtime_context": {"correlation_id": "c"},
        })
        with pytest.raises(ValueError, match="Missing required context keys"):
            parse_context(ctx_str, adapter=None)


class TestSCDContextIsValidatedUpFront:
    """F4 — a typo'd scd_type used to cost a full staging run before anyone heard about it.

    GlobalContext, StagingContext, RuntimeContext and LoggingContext each own a validate()
    and validate_all() calls all four. SCDContext owned none: it raised only for a missing
    business_key_column, in its constructor, while scd_type and load_type were checked
    inside SCDChangeDetectorTransformer._run() — which runs *after* every staging notebook has
    executed and written Delta tables.

    `configuration-and-context.md` says validation happens once, up front, before any Spark
    work. This is the one context that did not.
    """

    def _ctx_json(self, **scd_overrides):
        """MINIMAL_CTX with scd_context overridden — reusing the module's own valid base.

        Hand-rolling a context here failed on staging validation before SCD validation was
        ever reached, which is itself the point: validate_all() stops at the first failure,
        so a fixture that is invalid for an unrelated reason proves nothing about this one.
        """
        scd = {**MINIMAL_CTX["scd_context"], **scd_overrides}
        return _minimal_json(scd_context=scd)

    def test_validate_all_rejects_an_unknown_scd_type(self):
        cm = LoadContextManager(self._ctx_json(scd_type="scd3")).parse("", "")
        with pytest.raises(ValueError, match="scd_type"):
            cm.validate_all()

    def test_validate_all_rejects_an_unknown_load_type(self):
        cm = LoadContextManager(self._ctx_json(load_type="Partial")).parse("", "")
        with pytest.raises(ValueError, match="load_type"):
            cm.validate_all()

    def test_a_valid_scd_context_passes(self):
        """The negatives above must not be passing because validate_all always raises."""
        cm = LoadContextManager(self._ctx_json(scd_type="scd1", load_type="Full")).parse("", "")
        cm.validate_all()

    def test_the_error_names_the_offending_value(self):
        """errors-and-logging: the message names the value, not just the field."""
        cm = LoadContextManager(self._ctx_json(scd_type="SCD2")).parse("", "")
        with pytest.raises(ValueError, match="SCD2"):
            cm.validate_all()


def test_a_load_context_with_dqf_context_is_refused_with_the_migration() -> None:
    """F146: DQF was removed; a pipeline still sending its section fails loudly, naming the way on."""
    import json

    import pytest

    from spark_quick_start.orchestration.context_manager import LoadContextManager

    context = {
        "global_context": {"id": "ent_1", "entity_name": "E", "source_system": "S", "target_table_path": "/t",
                           "staging_table_path": "/s"},
        "staging_context": {"stages": [{"nb_name": "NB"}]},
        "scd_context": {"business_key_column": "id"},
        "runtime_context": {"correlation_id": "c", "pipeline_name": "p", "application_name": "a"},
        "logging_context": {"logging_level": "INFO", "log_file_name": "f", "log_file_dir": "/l"},
        "dqf_context": {"tables": []},
    }
    with pytest.raises(ValueError, match="dqf_context.*removed.*dqd_context"):
        LoadContextManager(json.dumps(context)).parse("s", "e").validate_all()



# ============================================================================
# load_timestamp and default_max_timestamp (F106)
# ============================================================================


def _validated(load_timestamp=None, default_max_timestamp=None):
    ctx = json.loads(_minimal_json())
    if load_timestamp is not None:
        ctx["global_context"]["load_timestamp"] = load_timestamp
    if default_max_timestamp is not None:
        ctx["scd_context"]["default_max_timestamp"] = default_max_timestamp
    mgr = LoadContextManager(json.dumps(ctx)).parse("", "")
    mgr.validate_all()
    return mgr


class TestLoadTimestampAgainstTheCloseOutSentinel:
    def test_a_load_time_before_the_sentinel_validates(self):
        _validated("2026-10-03T12:00:00Z")

    @pytest.mark.parametrize("sentinel", ["2026-10-03T12:00:00Z", "2026-10-03 12:00:00", "2020-01-01 00:00:00"])
    def test_a_sentinel_not_after_the_load_time_is_refused(self, sentinel):
        with pytest.raises(ValueError, match="default_max_timestamp.*load_timestamp"):
            _validated("2026-10-03T12:00:00Z", sentinel)

    def test_a_malformed_load_timestamp_is_refused_by_validate_all(self):
        with pytest.raises(ValueError, match="global_context.load_timestamp"):
            _validated("2026-10-03T12:00:00")


# ============================================================================
# build_staging_phase_context() — what a staging notebook receives
# ============================================================================


class TestStagingPhaseContext:
    def _phase(self, **overrides):
        mgr = LoadContextManager(_minimal_json(**overrides)).parse("/data_storage", "/data_eng")
        mgr.validate_all()
        return mgr.build_staging_phase_context(0, "NB_Stage_Person")

    def test_the_notebook_receives_extensions_context_whole(self):
        ext = {"acme.quality": {"max_null_ratio": 0.02}}
        assert self._phase(extensions_context=ext)["extensions_context"] == ext

    def test_a_load_without_extensions_still_hands_an_empty_section(self):
        assert self._phase()["extensions_context"] == {}

    def test_the_notebook_receives_platform_context(self):
        assert self._phase()["platform_context"] == {
            "data_storage_lakehouse_path": "/data_storage",
            "data_engineering_lakehouse_path": "/data_eng",
        }

    def test_global_context_no_longer_carries_the_paths(self):
        assert self._phase()["global_context"] == {
            "source_system": "SRC", "staging_table_path": "/tmp/stage__NB_Stage_Person"}
