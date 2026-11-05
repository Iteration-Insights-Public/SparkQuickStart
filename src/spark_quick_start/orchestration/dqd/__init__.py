"""
dqd — the Declarative Query Definition expression library.

A DQD brings one source to the load row by row. This package holds its per-row language: DQF's
expressions, predicates and function resolution, with keys renamed to the load context's style
(docs/architecture/decisions/2026-09-29-declarative-query-definition-design.md). A definition is
parsed once into checked nodes, then resolved to Spark columns over a source's columns.

Contains: the node types, DQDDefinitionError, parse_expression, parse_predicate,
    resolve_expression, resolve_predicate, resolve_function, and the function tables.
Does NOT contain: reading a source, the context sections or the transformer (F138, F139). It
    depends on PySpark and the standard library only, never on the rest of spark_quick_start or
    on the removed DQF engine (rule: layering §7).

Usage::
    from spark_quick_start.orchestration.dqd import parse_expression, resolve_expression

    node = parse_expression({"function": "upper", "arguments": [{"column": "name"}]})
    df.withColumn("name_upper", resolve_expression(node, df.columns))
"""
from __future__ import annotations

from spark_quick_start.orchestration.dqd.functions import (
    ALLOWED_FUNCTIONS,
    FORBIDDEN_FUNCTIONS,
    NOT_ROW_FUNCTIONS,
    OPERATORS,
    RAW_VALUE_POSITIONS,
    resolve_function,
)
from spark_quick_start.orchestration.dqd.nodes import (
    Cast,
    ColumnRef,
    Comparison,
    DQDDefinitionError,
    Expression,
    FunctionCall,
    Literal,
    Logical,
    Negation,
    NullCheck,
    Predicate,
)
from spark_quick_start.orchestration.dqd.parse import parse_expression, parse_predicate
from spark_quick_start.orchestration.dqd.resolve import resolve_expression, resolve_predicate

__all__ = [
    "ALLOWED_FUNCTIONS",
    "FORBIDDEN_FUNCTIONS",
    "NOT_ROW_FUNCTIONS",
    "OPERATORS",
    "RAW_VALUE_POSITIONS",
    "Cast",
    "ColumnRef",
    "Comparison",
    "DQDDefinitionError",
    "Expression",
    "FunctionCall",
    "Literal",
    "Logical",
    "Negation",
    "NullCheck",
    "Predicate",
    "parse_expression",
    "parse_predicate",
    "resolve_expression",
    "resolve_function",
    "resolve_predicate",
]
