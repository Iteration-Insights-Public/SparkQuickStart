"""
functions.py — which Spark functions a DQD definition may call, and how their arguments pass.

Contains: FORBIDDEN_FUNCTIONS, NOT_ROW_FUNCTIONS, OPERATORS, RAW_VALUE_POSITIONS,
    resolve_function.
Does NOT contain: expression resolution (resolve.py). This module answers "may this name be
    called, and is this argument a column or a raw value" and nothing else.

Usage::
    resolve_function("upper")  # pyspark.sql.functions.upper
"""
from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any

from pyspark.sql import functions as F

from spark_quick_start.orchestration.dqd.nodes import DQDDefinitionError

__all__ = [
    "ALLOWED_FUNCTIONS",
    "FORBIDDEN_FUNCTIONS",
    "NOT_ROW_FUNCTIONS",
    "OPERATORS",
    "RAW_VALUE_POSITIONS",
    "resolve_function",
]

# Functions that run arbitrary code or SQL. The same set the removed DQF engine refused (its
# _FORBIDDEN_FUNCTIONS), for the same reason: `reflect` was shown to return the Spark
# driver's OS user from an ordinary definition, and a definition is configuration, not code.
FORBIDDEN_FUNCTIONS = frozenset({
    "reflect", "try_reflect", "java_method", "try_java_method",   # JVM method invocation
    "expr",                                                        # SQL from a string
    "udf", "pandas_udf", "call_udf", "call_function",              # user-supplied code
    "unwrap_udt",                                                  # user-defined type internals
    "broadcast",                                                   # a DataFrame hint, not a column
})

# Functions that are not one row in, one row out: aggregates, window functions, sort orders
# and generators. A DQD brings one table to its target row by row (the design's non-goals), so
# these are refused where DQF allows them. Lower-case, because names are looked up lower-cased;
# a name absent from the installed PySpark is harmless here. Like FORBIDDEN_FUNCTIONS this is a
# named set, so a future aggregate is admitted until it is added: re-check on a Spark upgrade.
NOT_ROW_FUNCTIONS = frozenset({
    # aggregates
    "any_value", "approx_count_distinct", "approx_percentile", "array_agg", "avg", "bit_and", "bit_or", "bit_xor",
    "bitmap_and_agg", "bitmap_construct_agg", "bitmap_or_agg", "bool_and", "bool_or", "collect_list",
    "collect_set", "corr", "count", "count_distinct", "count_if", "count_min_sketch",
    "covar_pop", "covar_samp", "every", "first", "first_value", "grouping", "grouping_id",
    "histogram_numeric", "hll_sketch_agg", "hll_union_agg", "kll_sketch_agg_bigint",
    "kll_sketch_agg_double", "kll_sketch_agg_float", "kurtosis", "last", "last_value", "listagg", "listagg_distinct",
    "max", "max_by", "mean", "median", "min", "min_by", "mode", "percentile",
    "percentile_approx", "product", "regr_avgx", "regr_avgy", "regr_count", "regr_intercept",
    "regr_r2", "regr_slope", "regr_sxx", "regr_sxy", "regr_syy", "skewness", "some", "std",
    "stddev", "stddev_pop", "stddev_samp", "schema_of_variant_agg", "string_agg", "string_agg_distinct", "sum", "sum_distinct", "theta_intersection_agg",
    "theta_sketch_agg", "theta_union_agg", "try_avg", "try_sum", "var_pop", "var_samp", "variance",
    # window functions
    "cume_dist", "dense_rank", "lag", "lead", "nth_value", "ntile", "percent_rank", "rank",
    "row_number", "session_window", "window", "window_time",
    # sort orders
    "asc", "asc_nulls_first", "asc_nulls_last", "desc", "desc_nulls_first", "desc_nulls_last",
    # generators: one row in, many out
    "explode", "explode_outer", "inline", "inline_outer", "json_tuple", "posexplode",
    "posexplode_outer", "stack",
})

# The comparison and arithmetic operators a `function` may name. Each takes exactly two
# arguments; `=` and `==` are plain equality, as in DQF (null-safe equality is the
# predicate's `null_safe`).
OPERATORS: dict[str, Callable[[Any, Any], Any]] = {
    "=": lambda a, b: a == b,
    "==": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
    ">": lambda a, b: a > b,
    ">=": lambda a, b: a >= b,
    "<": lambda a, b: a < b,
    "<=": lambda a, b: a <= b,
    "+": lambda a, b: a + b,
    "-": lambda a, b: a - b,
    "*": lambda a, b: a * b,
    "/": lambda a, b: a / b,
    "%": lambda a, b: a % b,
}

# Argument positions a function takes as a raw Python value rather than a column, when the
# definition gives a literal there: a separator, a format, a pattern, a count. DQF's table
# (its literal_arg_positions), carried over less one entry: DQF passed `substr`'s
# position and length raw, and PySpark 3.5's `substr` takes columns only, so every literal
# `substr` failed in DQF with NOT_COLUMN_OR_STR (F141). Here they pass as literal columns.
# Plus one DQF lacked: `sha2`'s bit length, which PySpark takes as an int only, so the Data
# Vault hash key, `sha2(…, 256)`, can be built in a DQD (F159).
RAW_VALUE_POSITIONS: dict[str, set[int]] = {
    "cast": {1},
    "concat_ws": {0},
    "date_format": {1},
    "format_string": {0},
    "format_number": {1},
    "regexp_replace": {1, 2},
    "regexp_extract": {1, 2},
    "substring": {1, 2},
    "date_add": {1},
    "date_sub": {1},
    "add_months": {1},
    "split": {1},
    "trim": {1},
    "ltrim": {1},
    "rtrim": {1},
    "round": {1},
    "bround": {1},
    "sha2": {1},
}


def _build_allowed_functions() -> frozenset[str]:
    """Every public pyspark.sql function a DQD may call: DQF's rule, less NOT_ROW_FUNCTIONS.

    Derived from the installed PySpark, as DQF's is, so a runtime's own function set is used
    rather than a frozen list that would be wrong on the other platform's version.
    """
    allowed = set()
    for name in dir(F):
        if name.startswith("_") or name in FORBIDDEN_FUNCTIONS or name in NOT_ROW_FUNCTIONS:
            continue
        obj = getattr(F, name, None)
        if obj is None or inspect.ismodule(obj) or not callable(obj):
            continue
        if not (getattr(obj, "__module__", "") or "").startswith("pyspark.sql"):
            continue
        allowed.add(name)
    return frozenset(allowed)


ALLOWED_FUNCTIONS: frozenset[str] = _build_allowed_functions()


def resolve_function(name: str) -> Callable[..., Any]:
    """The pyspark.sql.functions callable a DQD `function` names, or a refusal saying why.

    Args:
        name: The function name from the definition, in any case.

    Returns:
        The callable from pyspark.sql.functions.

    Raises:
        DQDDefinitionError: if the name runs arbitrary code, is not one row in and one row
            out, or is not a pyspark.sql.functions column function.
    """
    key = name.lower()
    if key in FORBIDDEN_FUNCTIONS:
        raise DQDDefinitionError(
            f"function '{key}' is not allowed in a DQD definition: it can run arbitrary code "
            "(JVM reflection, raw SQL or a user-defined function), and a definition is "
            "configuration, not code"
        )
    if key in NOT_ROW_FUNCTIONS:
        raise DQDDefinitionError(
            f"function '{key}' is not allowed in a DQD definition: a DQD maps one row in to one "
            f"row out, and '{key}' aggregates, ranks, sorts or generates rows"
        )
    if key not in ALLOWED_FUNCTIONS:
        raise DQDDefinitionError(
            f"function '{key}' is not a pyspark.sql.functions column function a DQD definition "
            "may call"
        )
    return getattr(F, key)  # type: ignore[no-any-return]
