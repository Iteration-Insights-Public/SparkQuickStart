"""
context_manager.py — LoadContextManager and parse_context.

Contains: LoadContextManager (unified parser for every context section),
    parse_context (raw JSON → 6-tuple for staging notebooks).
Does NOT contain: PySpark logic, platform-specific code, Delta operations,
    or any import from spark_load_orchestration.

Usage::

    from spark_quick_start.orchestration.context_manager import LoadContextManager

    ctx = LoadContextManager(load_context_json)
    ctx.parse(data_storage_path, data_eng_path)
    ctx.validate_all()
"""
from __future__ import annotations

import json
import logging
import uuid
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from spark_quick_start.adapters.platform_adapter import PlatformAdapter

from spark_quick_start.orchestration.models import (
    DQDContext,
    ExtensionsContext,
    GlobalContext,
    HWMContext,
    LoggingContext,
    PlatformContext,
    RuntimeContext,
    SCDContext,
    StagingContext,
)

__all__ = ["LoadContextManager", "parse_context"]


# ============================================================================
# LOAD CONTEXT MANAGER
# ============================================================================

class LoadContextManager:
    """Unified manager for every context section with placeholder resolution."""

    def __init__(self, load_context_json: str) -> None:
        self.raw_json = load_context_json
        self.global_ctx: GlobalContext | None = None
        self.staging_ctx: StagingContext | None = None
        self.scd_ctx: SCDContext | None = None
        self.runtime_ctx: RuntimeContext | None = None
        self.logging_ctx: LoggingContext | None = None
        self.hwm_ctx: HWMContext | None = None
        self.dqd_ctx: DQDContext | None = None
        # Never None: a load without the section has an empty one, and parse() fills both.
        self.extensions_ctx: ExtensionsContext = ExtensionsContext()
        self.platform_ctx: PlatformContext = PlatformContext()
        self._parsed = False

    def parse(self, data_storage_path: str, data_eng_path: str) -> LoadContextManager:
        """Parse JSON, replacing ``{DataStorageLakehouse}`` and ``{DataEngineeringLakehouse}``."""
        processed = (
            self.raw_json
            .replace("{DataStorageLakehouse}", data_storage_path)
            .replace("{DataEngineeringLakehouse}", data_eng_path)
        )
        data = json.loads(processed)

        self.global_ctx = GlobalContext(data.get("global_context", {}))
        self.staging_ctx = StagingContext(data.get("staging_context", {}))
        self.scd_ctx = SCDContext(data.get("scd_context", {}))
        self.runtime_ctx = RuntimeContext(data.get("runtime_context", {}))
        self.logging_ctx = LoggingContext(data.get("logging_context", {}))

        if "dqf_context" in data:
            # F146: the DQF engine was removed. A pipeline that still sends its section is
            # refused here, naming the way on, rather than ignored like an unknown key and left
            # to fail later on missing staging stages.
            raise ValueError(
                "dqf_context: DQF was removed (backlog F146); describe the source as a "
                "dqd_context instead, see docs/reference/dqd-definition.md"
            )

        if "platform_context" in data:
            # The library fills this section from the paths the caller gave; a pipeline that
            # writes it is refused rather than overwritten, so a value there is never trusted.
            raise ValueError(
                "platform_context: the library sets this section (the two lakehouse paths the "
                "caller gave LoadOrchestrator); remove it from the load context. Your own data "
                "goes in extensions_context."
            )
        self.extensions_ctx = ExtensionsContext.from_raw(data.get("extensions_context", {}))

        hwm_raw = data.get("hwm_context")
        if hwm_raw is not None:
            if not isinstance(hwm_raw, dict):
                raise ValueError(
                    f"hwm_context must be a JSON object (dict), got {type(hwm_raw).__name__}"
                )
            self.hwm_ctx = HWMContext(hwm_raw)

        dqd_raw = data.get("dqd_context")
        if dqd_raw is not None:
            if not isinstance(dqd_raw, dict):
                raise ValueError(
                    f"dqd_context must be a JSON object (dict), got {type(dqd_raw).__name__}"
                )
            self.dqd_ctx = DQDContext(dqd_raw)

        # The resolved paths, for the steps and for downstream notebooks
        self.platform_ctx = PlatformContext(
            data_storage_lakehouse_path=data_storage_path,
            data_engineering_lakehouse_path=data_eng_path,
        )

        self._parsed = True
        return self

    def validate_all(self) -> None:
        """Validate all context categories. Raises ValueError on the first failure."""
        if not self._parsed:
            raise ValueError("Load context must be parsed before validation")
        self.global_ctx.validate()
        self.staging_ctx.validate(has_dqd=self.dqd_ctx is not None)
        assert self.global_ctx is not None, "parse() must have run — checked above"
        if self.staging_ctx.stages and not self.global_ctx.staging_table_schema:
            # Each staging notebook writes its own table, `<staging_table_path>__<nb_name>`;
            # only the schema-checked merge turns them into the table the load reads (F107).
            raise ValueError(
                "staging_context.stages is set but global_context.staging_table_schema is "
                "missing or empty: without it nothing merges the staging notebooks' tables into "
                "staging_table_path, so the load would read a table no notebook wrote. Add "
                "staging_table_schema (Spark StructType JSON) to global_context."
                + (" Or remove staging_context.stages if the source is the dqd_context."
                   if self.dqd_ctx is not None else "")
            )
        # Narrowed explicitly rather than added to mypy-baseline.txt, which is a gate file
        # on CLAUDE.md §3's floor. `scd_ctx` is Optional only to describe the pre-parse
        # state — parse() always constructs it — and validate_all() already refuses to run
        # unparsed, so this branch is unreachable. Stated as an assertion rather than
        # silently skipped: skipping would make a missing scd_context pass validation and
        # fail later, which is the whole defect F4 is about.
        assert self.scd_ctx is not None, "parse() must have run — checked above"
        self.scd_ctx.validate()
        if not self.scd_ctx.is_data_vault:
            # The close-out sentinel is the open row's effective-to: at or before the load time it
            # would make every open row look closed (F106). The Data Vault styles have no sentinel.
            sentinel = self.scd_ctx.default_max_timestamp_utc
            load_time = self.global_ctx.load_timestamp_utc
            if sentinel <= load_time:
                raise ValueError(
                    f"scd_context.default_max_timestamp {self.scd_ctx.default_max_timestamp!r} is not "
                    f"after global_context.load_timestamp {self.global_ctx.load_timestamp!r}; the "
                    "sentinel marks a row as open, so it must lie after every load time. Give a "
                    "later default_max_timestamp or an earlier load_timestamp."
                )
        self.runtime_ctx.validate()
        self.logging_ctx.validate()
        if self.hwm_ctx is not None:
            self.hwm_ctx.validate()
        if self.dqd_ctx is not None:
            self.dqd_ctx.validate()
            if self.hwm_ctx is not None and self.scd_ctx.load_type == "Full":
                # A DQD filters its own source by the watermark, and a Full load closes out every
                # target key the source lacks: together they would delete every key unchanged
                # since the last run, permanent SCD history (F139 review).
                raise ValueError(
                    "dqd_context with hwm_context reads only rows past the watermark, and "
                    "scd_context.load_type 'Full' would then delete every other target key; "
                    "use load_type 'Incremental', or remove hwm_context"
                )
            stages = self.staging_ctx.stages if self.staging_ctx is not None else []
            if stages:
                logging.getLogger(__name__).warning(
                    "dqd_context is set alongside staging_context.stages; which one is used "
                    "depends on the source step the pipeline adds, and the other is ignored."
                )

    def build_staging_phase_context(self, stage_index: int, staging_nb_name: str) -> dict[str, Any]:
        """Return the complete context dict for one staging phase notebook."""
        if not self._parsed:
            raise ValueError("Load context must be parsed before building staging phase context")
        if stage_index >= len(self.staging_ctx.stages):
            raise ValueError(f"Invalid stage index: {stage_index}")

        stage = self.staging_ctx.stages[stage_index]
        phase: dict[str, Any] = {
            "global_context": {
                "source_system": self.global_ctx.source_system,
                "staging_table_path": f"{self.global_ctx.staging_table_path}__{staging_nb_name}",
            },
            "extensions_context": self.extensions_ctx.to_dict(),
            "platform_context": self.platform_ctx.to_dict(),
            "staging_context": stage.get("staging_context", {}),
            "runtime_context": {
                "correlation_id": self.runtime_ctx.correlation_id,
                "pipeline_name": self.runtime_ctx.pipeline_name,
                "application_name": self.runtime_ctx.application_name,
                "serverless": self.runtime_ctx.serverless,
            },
            "scd_context": {
                "business_key_column": self.scd_ctx.business_key_column,
                "source_system_column": self.scd_ctx.source_system_column,
                "deleted_flag_column": self.scd_ctx.deleted_flag_column,
                "current_flag_column": self.scd_ctx.current_flag_column,
                "correlation_id_column": self.scd_ctx.correlation_id_column,
            },
            "logging_context": {
                "logging_level": self.logging_ctx.logging_level,
                "log_file_name": self.logging_ctx.log_file_name,
                **({"log_file_dir": self.logging_ctx.log_file_dir} if self.logging_ctx.log_file_dir else {}),
            },
        }
        if self.hwm_ctx is not None:
            phase["hwm_context"] = self.hwm_ctx.to_dict()
        return phase


# ============================================================================
# CONTEXT PARSING  (used by staging notebooks)
# ============================================================================

def parse_context(
    stage_context_str: str,
    adapter: PlatformAdapter | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], str, str | None]:
    """Parse and validate the staging-phase context JSON string.

    Returns:
        (global_context, staging_context, logging_context, runtime_context,
         correlation_id, current_nb_name)

    Raises:
        ValueError: if any of the four required top-level keys are missing.
    """
    current_nb_name = adapter.get_current_notebook_name() if adapter else None

    ctx = json.loads(stage_context_str)
    required = ["global_context", "staging_context", "logging_context", "runtime_context"]
    missing = [k for k in required if k not in ctx]
    if missing:
        raise ValueError(f"Missing required context keys: {missing}")

    runtime_ctx = ctx["runtime_context"]
    correlation_id = runtime_ctx.get("correlation_id") or str(uuid.uuid4())

    return (
        ctx["global_context"],
        ctx["staging_context"],
        ctx["logging_context"],
        runtime_ctx,
        correlation_id,
        current_nb_name,
    )
