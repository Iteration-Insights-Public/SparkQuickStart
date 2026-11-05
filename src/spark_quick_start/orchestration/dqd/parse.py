"""
parse.py — turn a DQD expression or predicate from JSON into checked nodes, once.

Contains: parse_expression, parse_predicate.
Does NOT contain: anything that needs Spark or a source's columns. Every shape rule is checked
    here, before a Spark session is touched, so a malformed definition fails at validation
    with the place it is in; a column the source lacks is resolve.py's to report.

Where a DQD is stricter than DQF, on purpose: it has no `{"expr": ...}` wrapper node; a node
    with two kinds (`column` and `literal`) is refused, where DQF picks one by precedence;
    `null_safe` is accepted on `=` only, where DQF ignores it elsewhere; `like` and
    `starts_with` take a literal string only; and `in` against a single literal is refused
    here, where DQF and Spark refuse it later, at analysis.

Usage::
    parse_expression({"function": "upper", "arguments": [{"column": "name"}]})
"""
from __future__ import annotations

from collections.abc import Callable
from typing import Any

from spark_quick_start.orchestration.dqd.functions import OPERATORS, resolve_function
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

__all__ = ["parse_expression", "parse_predicate"]

# DQF's spelling of a key, and what to write instead. A definition written in DQF's keys is
# refused with the replacement, rather than read as an unknown shape.
_DQF_KEYS = {
    "col": "'column'", "lit": "'literal'", "fn": "'function'", "args": "'arguments'",
    "op": "'operator'", "dataType": "'data_type'",
    "expr": "the inner expression itself: a DQD has no wrapper node ('not' takes 'expression')",
    "as": "'name' on the transformation or selection entry",
}
_KINDS = ("column", "literal", "cast", "function", "operator")
_COMPARISONS = frozenset({"=", "!=", ">", ">=", "<", "<=", "in", "like", "starts_with"})
_NULL_CHECKS = frozenset({"is_null", "is_not_null"})
_LOGICAL = frozenset({"and", "or"})
_LITERAL_PATTERN_OPERATORS = frozenset({"like", "starts_with"})


def _fail(path: str, message: str) -> DQDDefinitionError:
    return DQDDefinitionError(f"{path}: {message}")


def _check_keys(node: dict[str, Any], allowed: set[str], path: str) -> None:
    for key in node:
        if key in allowed:
            continue
        if key in _DQF_KEYS:
            raise _fail(path, f"unknown key '{key}', which is DQF's; write {_DQF_KEYS[key]}")
        raise _fail(path, f"unknown key '{key}'; the keys here are {sorted(allowed)}")


def _required(node: dict[str, Any], key: str, path: str) -> Any:
    if key not in node:
        raise _fail(path, f"missing required key '{key}'")
    return node[key]


def _non_empty_string(value: Any, path: str) -> str:
    if not isinstance(value, str) or not value:
        raise _fail(path, "must be a non-empty string")
    return value


def _object(node: Any, path: str) -> dict[str, Any]:
    if not isinstance(node, dict):
        raise _fail(path, f"must be an object, not {type(node).__name__}")
    return node


def _column(node: dict[str, Any], path: str) -> ColumnRef:
    _check_keys(node, {"column"}, path)
    return ColumnRef(_non_empty_string(node["column"], f"{path}.column"), path=path)


def _literal(node: dict[str, Any], path: str) -> Literal:
    _check_keys(node, {"literal"}, path)
    value = node["literal"]
    return Literal(tuple(value) if isinstance(value, list) else value)


def _cast(node: dict[str, Any], path: str) -> Cast:
    _check_keys(node, {"cast"}, path)
    body = _object(node["cast"], f"{path}.cast")
    _check_keys(body, {"expression", "data_type"}, f"{path}.cast")
    expression = parse_expression(_required(body, "expression", f"{path}.cast"), path=f"{path}.cast.expression")
    data_type = _non_empty_string(_required(body, "data_type", f"{path}.cast"), f"{path}.cast.data_type")
    return Cast(expression, data_type)


def _function(node: dict[str, Any], path: str) -> FunctionCall:
    _check_keys(node, {"function", "arguments"}, path)
    name = _non_empty_string(node["function"], f"{path}.function").lower()
    raw = node.get("arguments", [])
    if not isinstance(raw, list):
        raise _fail(f"{path}.arguments", "must be a list")
    arguments = tuple(parse_expression(a, path=f"{path}.arguments[{i}]") for i, a in enumerate(raw))
    if name == "when":
        if len(arguments) not in (2, 3):
            raise _fail(path, f"'when' takes 2 or 3 arguments (a condition, a value, and an optional "
                              f"otherwise), not {len(arguments)}")
    elif name in OPERATORS or name == "cast":
        if len(arguments) != 2:
            raise _fail(path, f"'{name}' takes exactly 2 arguments, not {len(arguments)}")
    else:
        try:
            resolve_function(name)
        except DQDDefinitionError as exc:
            raise _fail(f"{path}.function", str(exc)) from None
    return FunctionCall(name, arguments)


def _predicate_operand(node: Any, path: str) -> Predicate:
    if not isinstance(node, dict) or "operator" not in node:
        raise _fail(path, "must be a predicate: an object with an 'operator'")
    return parse_predicate(node, path=path)


def parse_predicate(node: Any, *, path: str = "predicate") -> Predicate:
    """Parse one DQD predicate: DQF's operators, with `op` renamed `operator`.

    Args:
        node: The predicate as decoded JSON.
        path: Where it sits in the definition, for the error message.

    Returns:
        A Comparison, NullCheck, Logical or Negation.

    Raises:
        DQDDefinitionError: naming the place and what is wrong.
    """
    node = _object(node, path)
    operator = _non_empty_string(_required(node, "operator", path), f"{path}.operator").lower()
    if operator in _LOGICAL:
        _check_keys(node, {"operator", "left", "right"}, path)
        return Logical(operator,
                       _predicate_operand(_required(node, "left", path), f"{path}.left"),
                       _predicate_operand(_required(node, "right", path), f"{path}.right"))
    if operator == "not":
        _check_keys(node, {"operator", "expression"}, path)
        return Negation(_predicate_operand(_required(node, "expression", path), f"{path}.expression"))
    if operator in _NULL_CHECKS:
        _check_keys(node, {"operator", "left"}, path)
        return NullCheck(operator, parse_expression(_required(node, "left", path), path=f"{path}.left"))
    if operator in _COMPARISONS:
        _check_keys(node, {"operator", "left", "right", "null_safe"} if operator == "="
                    else {"operator", "left", "right"}, path)
        left = parse_expression(_required(node, "left", path), path=f"{path}.left")
        right = parse_expression(_required(node, "right", path), path=f"{path}.right")
        if operator in _LITERAL_PATTERN_OPERATORS and not (
                isinstance(right, Literal) and isinstance(right.value, str)):
            raise _fail(f"{path}.right", f"'{operator}' takes a literal string pattern")
        if operator == "in" and isinstance(right, Literal) and not isinstance(right.value, tuple):
            raise _fail(f"{path}.right", "'in' takes a literal list or an array expression, not a "
                                         "single literal")
        null_safe = node.get("null_safe", False)
        if not isinstance(null_safe, bool):
            raise _fail(f"{path}.null_safe", "must be true or false")
        return Comparison(operator, left, right, null_safe=null_safe)
    raise _fail(f"{path}.operator", f"unknown operator '{operator}'; the operators are "
                                    f"{sorted(_COMPARISONS | _NULL_CHECKS | _LOGICAL | {'not'})}")


_PARSERS: dict[str, Callable[[dict[str, Any], str], Expression]] = {
    "column": _column, "literal": _literal, "cast": _cast, "function": _function,
}


def parse_expression(node: Any, *, path: str = "expression") -> Expression:
    """Parse one DQD expression: DQF's grammar with its keys renamed to the context's style.

    An object is exactly one of `column`, `literal`, `cast`, `function` or, standing for a
    boolean value, a predicate's `operator`.

    Args:
        node: The expression as decoded JSON.
        path: Where it sits in the definition, for the error message.

    Returns:
        The parsed node.

    Raises:
        DQDDefinitionError: naming the place and what is wrong.
    """
    node = _object(node, path)
    kinds = [k for k in _KINDS if k in node]
    if len(kinds) != 1:
        for key in node:
            if key in _DQF_KEYS:
                raise _fail(path, f"unknown key '{key}', which is DQF's; write {_DQF_KEYS[key]}")
        raise _fail(path, f"must have exactly one of {list(_KINDS)}, not {sorted(node)}")
    if kinds[0] == "operator":
        return parse_predicate(node, path=path)
    return _PARSERS[kinds[0]](node, path)
