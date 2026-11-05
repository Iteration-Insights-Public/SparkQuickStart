"""
_dqd.py — the DQD expression library, for transformer use.

The one module inside the package that imports orchestration/dqd (rule: layering §7), as
_pipeline.py is for the Delta layer: a pure re-export seam, so the library stays independent of
the pipeline and the pipeline's coupling to it is one controlled point.
"""
from __future__ import annotations

from spark_quick_start.orchestration.dqd import (
    ColumnRef,
    DQDDefinitionError,
    parse_expression,
    parse_predicate,
    resolve_expression,
    resolve_predicate,
)

__all__ = [
    "ColumnRef",
    "DQDDefinitionError",
    "parse_expression",
    "parse_predicate",
    "resolve_expression",
    "resolve_predicate",
]
