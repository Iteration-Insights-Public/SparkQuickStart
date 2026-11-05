"""
nodes.py — the parsed form of a DQD expression or predicate.

Contains: DQDDefinitionError, ColumnRef, Literal, Cast, FunctionCall, Comparison, NullCheck,
    Logical, Negation, and the Expression and Predicate unions.
Does NOT contain: parsing (parse.py) or resolution to Spark columns (resolve.py). A node holds
    what the definition said, already checked; it never sees raw JSON again and never touches
    Spark.

Usage::
    Comparison("=", ColumnRef("status"), Literal("active"))
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Union

__all__ = [
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
]


class DQDDefinitionError(ValueError):
    """A DQD definition that cannot be read, named by where it is in the definition."""


@dataclass(frozen=True)
class ColumnRef:
    """`{"column": name}`. A dotted name reads a struct field when no column has the whole name.

    `path` says where in the definition the reference was written, for the error a missing
    column raises; it is not part of the node's value.
    """

    name: str
    path: str = field(default="", compare=False)


@dataclass(frozen=True)
class Literal:
    """`{"literal": value}`. A list is kept as a tuple and becomes an array."""

    value: Any


@dataclass(frozen=True)
class Cast:
    """`{"cast": {"expression": ..., "data_type": ...}}`."""

    expression: Expression
    data_type: str


@dataclass(frozen=True)
class FunctionCall:
    """`{"function": name, "arguments": [...]}`, the name lower-cased."""

    name: str
    arguments: tuple[Expression, ...]


@dataclass(frozen=True)
class Comparison:
    """A binary predicate: `=`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `like` or `starts_with`."""

    operator: str
    left: Expression
    right: Expression
    null_safe: bool = False


@dataclass(frozen=True)
class NullCheck:
    """`is_null` or `is_not_null` on one expression."""

    operator: str
    left: Expression


@dataclass(frozen=True)
class Logical:
    """`and` or `or` of two predicates."""

    operator: str
    left: Predicate
    right: Predicate


@dataclass(frozen=True)
class Negation:
    """`not` of one predicate."""

    expression: Predicate


Predicate = Union[Comparison, NullCheck, Logical, Negation]
# A predicate is also an expression: DQF lets one stand wherever a value may, as a boolean.
Expression = Union[ColumnRef, Literal, Cast, FunctionCall, Predicate]
