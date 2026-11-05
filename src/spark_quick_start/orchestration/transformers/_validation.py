"""
_validation.py — DataFrame validation helpers for the transformers package.

Contains only assert_no_dups. The schema-comparison helpers
(schema_to_primitive_map, compare_schemas_primitive_ignore_order) used to be
duplicated here; they live in their single canonical home,
orchestration/validation.py — import them from there.
"""
from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def assert_no_dups(
    df: DataFrame,
    source_label: str,
    key_columns: list[str],
    sample_n: int = 5,
    serverless: bool = False,
) -> None:
    """Raise ValueError if *df* has duplicate rows on *key_columns*.

    Args:
        df: DataFrame to check.
        source_label: Human-readable label used in the error message.
        key_columns: Columns to check. No default deliberately — callers must
            pass the active ColumnNamingPolicy's column names explicitly. A
            hardcoded fallback here previously risked silent wrong results
            under a custom naming policy.
        sample_n: Number of duplicate key examples to include in the error.
        serverless: When True, skips DataFrame caching (not supported on Databricks Serverless /
            Spark Connect). When False (default), caches to avoid scanning the duplicate-key
            result twice.
    """
    dup_keys = (
        df.groupBy(key_columns)
        .count()
        .filter(F.col("count") > 1)
        .withColumnRenamed("count", "dup_count")
    )

    if not serverless:
        dup_keys = dup_keys.cache()

    # Released on every path (clean, raise, or a failing action) — the cache is only ever held for these two actions.
    try:
        dup_count = dup_keys.count()
        if dup_count == 0:
            return
        samples = dup_keys.limit(sample_n).collect()
    finally:
        if not serverless:
            dup_keys.unpersist()
    lines = (
        [f"Duplicate records in '{source_label}'.",
         f"Key columns: {key_columns}",
         f"Duplicate key groups: {dup_count}"]
        + [str(r.asDict()) for r in samples]
    )
    raise ValueError("\n".join(lines))
