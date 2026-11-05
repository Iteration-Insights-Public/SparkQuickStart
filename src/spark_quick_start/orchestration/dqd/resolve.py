"""
resolve.py — turn parsed DQD nodes into Spark columns over one source's columns.

Contains: resolve_expression, resolve_predicate.
Does NOT contain: parsing (parse.py), which function names are allowed (functions.py), or any
    reading of data. Resolution builds column expressions only; it runs no Spark action.

Usage::
    resolve_expression(parse_expression({"column": "name"}), df.columns)
"""
from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from pyspark.sql import functions as F

from spark_quick_start.orchestration.dqd.functions import (
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

if TYPE_CHECKING:
    from pyspark.sql import Column

__all__ = ["resolve_expression", "resolve_predicate"]


class _Scope:
    """The source's columns, looked up as Spark does: case-insensitively unless the session sets
    `spark.sql.caseSensitive`, and refusing a name two columns share but for case."""

    def __init__(self, columns: Sequence[str], case_sensitive: bool) -> None:
        self.columns = list(columns)
        self.case_sensitive = case_sensitive
        self._by_lower: dict[str, list[str]] = {}
        for name in self.columns:
            self._by_lower.setdefault(name.lower(), []).append(name)

    def find(self, name: str) -> str | None:
        if name in self.columns:
            return name
        if self.case_sensitive:
            return None
        matches = self._by_lower.get(name.lower(), [])
        if len(matches) > 1:
            raise DQDDefinitionError(f"'{name}' matches more than one column of the source: {matches}")
        return matches[0] if matches else None


def _quoted(name: str) -> Column:
    """A column by its exact name, dots and backticks included."""
    return F.col("`" + name.replace("`", "``") + "`")


# Hidden columns a source has but does not list in `df.columns`: the file and Delta metadata
# struct, which replaces `input_file_name()` on Unity Catalog shared and serverless compute.
_HIDDEN_COLUMNS = ("_metadata",)


def _column(node: ColumnRef, scope: _Scope) -> Column:
    if node.name.split(".")[0] in _HIDDEN_COLUMNS:
        return F.col(node.name)  # Spark resolves it, as DQF's plain col() does
    whole = scope.find(node.name)
    if whole is not None:
        return _quoted(whole)
    parts = node.name.split(".")
    for split in range(1, len(parts)):
        base = scope.find(".".join(parts[:split]))
        if base is not None:
            column = _quoted(base)
            for field_name in parts[split:]:
                column = column.getField(field_name)
            return column
    where = f"{node.path}: " if node.path else ""
    raise DQDDefinitionError(
        f"{where}'{node.name}' is not a column of the source; its columns are {scope.columns}")


def _literal(node: Literal, scope: _Scope) -> Column:
    if isinstance(node.value, tuple):
        return F.array(*[F.lit(v) for v in node.value])
    return F.lit(node.value)


def _cast(node: Cast, scope: _Scope) -> Column:
    return _resolve(node.expression, scope).cast(node.data_type)


def _argument(call: FunctionCall, position: int, scope: _Scope) -> Any:
    """A column, or the raw value where the function takes one and the definition gave a literal."""
    argument = call.arguments[position]
    if position in RAW_VALUE_POSITIONS.get(call.name, ()) and isinstance(argument, Literal):
        return list(argument.value) if isinstance(argument.value, tuple) else argument.value
    return _resolve(argument, scope)


def _function(node: FunctionCall, scope: _Scope) -> Column:
    arguments = [_argument(node, i, scope) for i in range(len(node.arguments))]
    if node.name == "when":
        condition, value, *otherwise = arguments
        column = F.when(condition, value)
        return column.otherwise(otherwise[0]) if otherwise else column
    if node.name == "cast":
        return arguments[0].cast(arguments[1])  # type: ignore[no-any-return]
    if node.name in OPERATORS:
        return OPERATORS[node.name](*arguments)  # type: ignore[no-any-return]
    return resolve_function(node.name)(*arguments)  # type: ignore[no-any-return]


def _comparison(node: Comparison, scope: _Scope) -> Column:
    left = _resolve(node.left, scope)
    if node.operator in ("like", "starts_with"):
        pattern = node.right.value  # type: ignore[union-attr]  # parse.py: a literal string
        return left.like(pattern) if node.operator == "like" else left.startswith(pattern)
    if node.operator == "in":
        if isinstance(node.right, Literal) and isinstance(node.right.value, tuple):
            return left.isin(*node.right.value)
        return F.array_contains(_resolve(node.right, scope), left)
    right = _resolve(node.right, scope)
    if node.operator == "=" and node.null_safe:
        return left.eqNullSafe(right)
    return OPERATORS[node.operator](left, right)  # type: ignore[no-any-return]


def _null_check(node: NullCheck, scope: _Scope) -> Column:
    left = _resolve(node.left, scope)
    return left.isNull() if node.operator == "is_null" else left.isNotNull()


def _logical(node: Logical, scope: _Scope) -> Column:
    left, right = _resolve(node.left, scope), _resolve(node.right, scope)
    return left & right if node.operator == "and" else left | right


def _negation(node: Negation, scope: _Scope) -> Column:
    return ~_resolve(node.expression, scope)


# One resolver per node kind: a new kind is one entry here and one function above.
_RESOLVERS: dict[type, Callable[[Any, _Scope], Column]] = {
    ColumnRef: _column,
    Literal: _literal,
    Cast: _cast,
    FunctionCall: _function,
    Comparison: _comparison,
    NullCheck: _null_check,
    Logical: _logical,
    Negation: _negation,
}


def _resolve(node: Expression, scope: _Scope) -> Column:
    return _RESOLVERS[type(node)](node, scope)


def resolve_expression(node: Expression, columns: Sequence[str], *, case_sensitive: bool = False) -> Column:
    """The Spark column a parsed DQD expression stands for, over a source with these columns.

    Args:
        node: A node from `parse_expression`.
        columns: The source's column names at this point, e.g. `df.columns`, including any
            transformation already added.
        case_sensitive: The session's `spark.sql.caseSensitive`; Spark's default is False.

    Returns:
        A Column; building it runs no Spark action.

    Raises:
        DQDDefinitionError: for a column the source does not have, or a name two of its
            columns share but for case.
    """
    return _resolve(node, _Scope(columns, case_sensitive))


def resolve_predicate(node: Predicate, columns: Sequence[str], *, case_sensitive: bool = False) -> Column:
    """The boolean Spark column a parsed DQD predicate stands for.

    Args:
        node: A node from `parse_predicate`.
        columns: The source's column names at this point, e.g. `df.columns`.
        case_sensitive: The session's `spark.sql.caseSensitive`; Spark's default is False.

    Returns:
        A boolean Column; building it runs no Spark action.

    Raises:
        DQDDefinitionError: as `resolve_expression`.
    """
    return _resolve(node, _Scope(columns, case_sensitive))
