"""
validation.py — DataFrame and schema validation utilities.

Contains: schema_to_primitive_map, compare_schemas_primitive_ignore_order.
Does NOT contain: platform-specific code, Delta merge logic,
    or any import from spark_load_orchestration.
"""
from __future__ import annotations

from typing import Any

from pyspark.sql.types import ArrayType, MapType, StructField, StructType


def _base_type_str(field_: StructField) -> str:
    dt = field_.dataType
    if isinstance(dt, (StructType, ArrayType, MapType)):
        raise ValueError(
            f"Complex type not supported in schema comparison for '{field_.name}': {dt.simpleString()}"
        )
    return dt.simpleString()


def schema_to_primitive_map(schema: StructType, case_insensitive: bool = True) -> dict[str, str]:
    """Return a ``{column_name: type_string}`` map of all primitive fields in *schema*."""
    result: dict[str, str] = {}
    for f in schema.fields:
        key = f.name.lower() if case_insensitive else f.name
        if key in result:
            raise ValueError(f"Duplicate column after case-normalisation: '{f.name}'")
        result[key] = _base_type_str(f)
    return result


def compare_schemas_primitive_ignore_order(
    actual: StructType,
    expected: StructType,
    case_insensitive: bool = True,
) -> tuple[bool, dict[str, Any]]:
    """Compare two StructType schemas for compatibility, ignoring column order.

    Returns:
        (ok, report) — *ok* is True when schemas match exactly.
        *report* has keys ``"missing"``, ``"extra"``, ``"type_mismatches"``.
    """
    a = schema_to_primitive_map(actual, case_insensitive)
    e = schema_to_primitive_map(expected, case_insensitive)

    missing = sorted(set(e) - set(a))
    extra = sorted(set(a) - set(e))
    type_mismatches = [
        {"column": k, "expected": e[k], "actual": a[k]}
        for k in sorted(set(a) & set(e))
        if a[k] != e[k]
    ]
    ok = not (missing or extra or type_mismatches)
    return ok, {"missing": missing, "extra": extra, "type_mismatches": type_mismatches}
