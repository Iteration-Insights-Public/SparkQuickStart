"""
_pipeline.py — Delta I/O and SCD merge functions for transformer use.

This module re-exports the pipeline functions that transformers need from
orchestration/load_pipeline.py. Transformers import from here rather than
from orchestration.load_pipeline directly, keeping the transformers package
self-contained at the import level.

orchestration/load_pipeline.py is the implementation home; this module is the
controlled coupling point. Outside the transformers package, callers import
from orchestration.load_pipeline directly (orchestrator.py for log_runtime,
delta_utils.py for capture_write_statistics) — except orchestrator.py's
watermark fallback, which takes get_high_watermark from here.
"""
from __future__ import annotations

from spark_quick_start.orchestration.load_pipeline import (
    capture_merge_statistics,
    capture_write_statistics,
    execute_append_only,
    execute_dv_insert_only,
    execute_scd_merge,
    get_high_watermark,
    load_staged_dataframe,
    load_target_dataframe,
    validate_and_merge_staging_tables,
)

__all__ = [
    "capture_merge_statistics",
    "capture_write_statistics",
    "execute_append_only",
    "execute_dv_insert_only",
    "execute_scd_merge",
    "get_high_watermark",
    "load_staged_dataframe",
    "load_target_dataframe",
    "validate_and_merge_staging_tables",
]
