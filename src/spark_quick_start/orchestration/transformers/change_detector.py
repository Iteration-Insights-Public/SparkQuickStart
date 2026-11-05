"""
change_detector.py — SCDChangeDetectorTransformer.

Pure-PySpark SCD change detection. Zero fabric_scd2 dependency.

Output contract per row (unchanged rows dropped):
  - all source business columns
  - {hash_column}     (long)   — xxhash64 over non-excluded business columns;
                                 order and null strategy depend on transformer variant:
                                 SCDChangeDetectorTransformer uses alphabetical sort with
                                 NULL sentinels; SCDGracefulChangeDetectorTransformer uses
                                 staging_table_schema declaration order and drops
                                 trailing NULLs.
  - {version_column}  (long)   — 1 for new; tgt+1 for SCD2 changed; 1 for SCD1 changed; tgt for deleted
  - _prev_scdVersion (long) — NULL for new; tgt.version for changed/deleted
  - {row_key_column}  (string) — business_key cast to string (raw, default) or sha2(business_key, 256)
                       when SCDContext.scd_row_hashing=True; name set by SCDContext.row_key_column.
                       When row_key_column is business_key_column (F148, case 1), not written:
                       the staged key passes through with its value and type
  - {deleted_flag_column} (string) — deleted_flag_value / not_deleted_flag_value ("Y"/"N" by default)
  - {source_system_column} (string) — from global_ctx or source column
  - _change_type (string) — "new" | "changed" | "deleted"

Column names are configured via ColumnNamingPolicy passed to SCDContext.

The delete signal (F127): SCDContext.source_deleted_flag_column when configured, otherwise the
deleted flag column when the source carries it, otherwise none. A row is deleted only where the
signal equals source_deleted_flag_value (the resolved deleted_flag_value unless given), compared
null-safely: NULL or any other value is not deleted.
The staged deleted flag is never carried to the output; the one flag column is always written here.

When there is a signal:
  - The control column is excluded from hash computation.
  - On first load: rows where the signal equals source_deleted_flag_value are dropped
    (no target row exists to close out).
  - On incremental/full load: rows where the signal equals source_deleted_flag_value
    AND a matching target row exists whose current row is not already deleted are emitted as
    _change_type="deleted". Rows with the flag set but no matching target are silently skipped,
    and so are rows whose target row is already flagged deleted (F142): a delete that is repeated
    adds no version. A NULL target flag counts as not deleted. On a Full load the same holds for
    a key absent from the source. A batch of only such rows leaves the guard no candidate, so the
    load halts early with no write. A not-deleted source row against a deleted target row is
    still "changed" (the undelete).
  - The control column is NOT present in the output schema.

Audit fields (effective_from, effective_to, current_flag, correlation_id)
are NOT stamped here — those are the responsibility of each merger.
"""
from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from spark_quick_start.orchestration.transformers._aliases import SCD_RENAMES, deprecated_aliases
from spark_quick_start.orchestration.transformers._constants import (
    CHANGE_TYPE as _CHANGE_TYPE,
)
from spark_quick_start.orchestration.transformers._constants import INTERNAL_COLUMNS, Constants
from spark_quick_start.orchestration.transformers._constants import (
    PREV_VERSION as _PREV_VERSION,
)
from spark_quick_start.orchestration.transformers.base import LoadContext, ProcessTransformer
from spark_quick_start.orchestration.transformers.state import PipelineState

if TYPE_CHECKING:
    from pyspark.sql import Column

    from spark_quick_start.orchestration.models import SCDContext


def _graceful_hash_expr(
    schema_cols: list[str],
    null_token: str,
    separator: str,
) -> Column:
    """Return a static when()-chain Column for trailing-null-trimmed hash.

    Scans schema_cols right-to-left. The first (rightmost) non-null column
    defines the slice boundary; columns up to that index are concatenated with
    interior NULLs replaced by null_token. Trailing NULLs are omitted entirely.

    Hash of all-null row: xxhash64('') — stable, no sentinel ambiguity.
    """
    if not schema_cols:
        raise ValueError("_graceful_hash_expr requires at least one column in schema_cols")
    coalesced = [F.coalesce(F.col(c).cast("string"), F.lit(null_token)) for c in schema_cols]
    # Fallback when every column is null
    expr: Column = F.xxhash64(F.lit(""))
    # Build conditions from right-to-left.
    # Branch i fires when schema_cols[i] is non-null AND all cols after i are null
    # (i.e., i is the index of the rightmost non-null column).
    # We accumulate by wrapping: innermost = rightmost column branch.
    for i in range(len(schema_cols) - 1, -1, -1):
        all_after_null = [F.col(schema_cols[j]).isNull() for j in range(i + 1, len(schema_cols))]
        condition = F.col(schema_cols[i]).isNotNull()
        for c in all_after_null:
            condition = condition & c
        expr = F.when(
            condition,
            F.xxhash64(F.concat_ws(separator, *coalesced[: i + 1])),
        ).otherwise(expr)
    return expr


class SCDChangeDetectorTransformer(ProcessTransformer):
    """Detect SCD changes and return a minimal candidates DataFrame using pure PySpark.

    Reads state.df (staged source) and state.frames["target"] (Silver current-flag
    rows stashed by SCDTargetTableTransformer; an absent key, `target_exists` False, is a
    first load, as is an existing empty frame). Classifies each row as new, changed, or
    deleted. Unchanged rows are dropped. Computes scdHash, scdVersion, _prev_scdVersion, and
    scdRowKey unless the row key is the business key itself (F148). Does NOT stamp audit fields — those belong to each merger implementation.

    Args:
        exclude_from_hash: Additional columns to exclude from hash computation beyond
            the default set (business key, source system, correlation ID, deleted flag,
            scdRowKey, scdHash, scdVersion).
        hash_null_token: Sentinel substituted for SQL NULL before hashing.
        hash_separator: Separator for concat_ws in the hash expression. `None`, the default,
            reads `scd_context.hash_separator` at run time (``"~"`` unless the context gives
            one). A value given here that differs from one the context gives raises
            `ValueError` when the step runs: two separators for one table would hash its rows
            two ways. Changing the separator of a table that holds rows re-versions every key
            (F106).
    """

    # F156: refused under a Data Vault style, at composition (LoadOrchestrator) and in _run.
    scd_style_only = True

    def __init__(
        self,
        *,
        exclude_from_hash: Sequence[str] | None = None,
        hash_null_token: str = Constants.HASH_NULL_TOKEN,
        hash_separator: str | None = None,
        debug_hash_string_column: str | None = None,
    ) -> None:
        self._exclude_from_hash = list(exclude_from_hash or [])
        self._hash_null_token = hash_null_token
        self._hash_separator = hash_separator
        self._debug_hash_string_column = debug_hash_string_column

    def _ordered_hash_cols(
        self,
        src_columns: list[str],
        excluded: set[str],
        ctx: LoadContext,
    ) -> list[str]:
        """Return columns to hash in the order used by this strategy (alphabetical)."""
        return sorted(c for c in src_columns if c not in excluded)

    def _resolve_separator(self, s: SCDContext) -> str:
        """The separator this run hashes with: the constructor's, else the context's (F106).

        Resolved per run and passed down, never stored, so one step reused under two contexts
        hashes each with its own.

        Raises:
            ValueError: when the constructor's differs from one the context gives.
        """
        if self._hash_separator is None:
            return s.hash_separator
        if s.was_given("hash_separator") and s.hash_separator != self._hash_separator:
            raise ValueError(
                f"{type(self).__name__}: hash_separator {self._hash_separator!r} was given to the "
                f"step, but scd_context.hash_separator is {s.hash_separator!r}. One table's rows "
                "must hash one way: give it in one place, or make them equal."
            )
        return self._hash_separator

    def _hash_string_expr(self, hash_cols: list[str], separator: str) -> Column:
        """Return the concatenated string Column that is fed into the hash function.

        Used for the optional ``debug_hash_string_column`` so the pre-hash value
        is visible in the output DataFrame for troubleshooting.
        """
        return F.concat_ws(
            separator,
            *[F.coalesce(F.col(c).cast("string"), F.lit(self._hash_null_token))
              for c in hash_cols],
        )

    def _hash_column_expr(self, hash_cols: list[str], separator: str) -> Column:
        """Return a Column expression for scdHash given the ordered column list."""
        return F.xxhash64(self._hash_string_expr(hash_cols, separator))

    def _run(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
        ctx.scd_ctx.require_scd_style(type(self).__name__)
        from spark_quick_start.logger import OrchestratorLogger
        logger = self._logger or OrchestratorLogger()

        s = ctx.scd_ctx
        bk = s.business_key_column
        ss = s.source_system_column

        # Guard: reject inbound SCD metadata
        scd_meta = {s.version_column, s.hash_column}
        collisions = scd_meta & set(state.df.columns)
        if collisions:
            raise ValueError(
                f"{type(self).__name__}: staged DataFrame already contains SCD metadata "
                f"columns {sorted(collisions)}. Remove them upstream."
            )
        if s.scd_type not in ("scd1", "scd2"):
            raise ValueError(
                f"{type(self).__name__}: unsupported scd_type '{s.scd_type}'. "
                f"Expected 'scd1' or 'scd2'."
            )

        # Guard: required source columns must be present before any Spark ops
        _missing = [
            col for col in [bk, s.source_deleted_flag_column]
            if col is not None and col not in state.df.columns
        ]
        if _missing:
            raise ValueError(
                f"{type(self).__name__}: required source column(s) missing from staged "
                f"DataFrame: {_missing}. Available: {list(state.df.columns)}"
            )

        # F127: the delete signal. An explicit source_deleted_flag_column wins; otherwise a staged
        # deleted flag column is the signal when present; otherwise there is none. A row is
        # deleted only where the signal equals source_deleted_flag_value, compared null-safely, so a
        # NULL or any other value means not deleted (closes F108). The staged flag is never
        # carried to the output: the detector writes the one flag column, deleted_flag_value or
        # not_deleted_flag_value, which is also what it compares the target's flag with (F106).
        # Spark and Delta match column names case-insensitively, so a staged `ScdDeletedFlag` is
        # the flag too: found by case, and dropped under its own spelling, or it would reach the
        # output beside the one the detector writes.
        staged_flag = next((c for c in state.df.columns if c.lower() == s.deleted_flag_column.lower()), None)
        signal = s.source_deleted_flag_column or staged_flag
        dropped = {s.deleted_flag_column} | ({staged_flag} if staged_flag else set()) | ({signal} if signal else set())
        is_source_deleted = (
            F.coalesce(F.col(signal) == F.lit(s.source_deleted_flag_value), F.lit(False))
            if signal else F.lit(False)
        )

        # Source preparation
        src = state.df
        if not s.source_system_from_source:
            src = src.withColumn(ss, F.lit(ctx.global_ctx.source_system))

        # scdRowKey — raw BK by default; SHA-256 when scd_row_hashing=True. When the row key
        # is the business key itself (F148, case 1), the staged key passes through as it is:
        # it is what the target is matched on, so it reaches the join unchanged.
        if s.row_key_column != bk:
            row_key_expr = (
                F.sha2(F.col(bk).cast("string"), 256)
                if s.scd_row_hashing
                else F.col(bk).cast("string")
            )
            src = src.withColumn(s.row_key_column, row_key_expr)

        # scdHash — over sorted non-excluded columns
        excluded = (
            {bk, ss, s.correlation_id_column, s.deleted_flag_column, s.row_key_column}
            | scd_meta
            | set(self._exclude_from_hash)
            | set(s.exclude_from_hash)
        )
        excluded = excluded | dropped
        hash_cols = self._ordered_hash_cols(list(src.columns), excluded, ctx)
        separator = self._resolve_separator(s)
        if self._debug_hash_string_column:
            src = src.withColumn(self._debug_hash_string_column, self._hash_string_expr(hash_cols, separator))
        src = src.withColumn(s.hash_column, self._hash_column_expr(hash_cols, separator))

        # Join and classify
        tgt = state.frames.get(Constants.TARGET_FRAME_KEY)
        first_load = tgt is None or tgt.isEmpty()

        if first_load:
            logger.info("First load — all source rows classified as new")
            src_for_first_load = src.filter(~is_source_deleted)
            # The signal and the staged flag are control columns, not business columns.
            src_col_refs = [F.col(c) for c in src_for_first_load.columns if c not in dropped]
            candidates = (src_for_first_load
                .select(
                    *src_col_refs,
                    F.lit("new").alias(_CHANGE_TYPE),
                    F.lit(1).cast("long").alias(s.version_column),
                    F.lit(None).cast("long").alias(_PREV_VERSION),
                    F.lit(s.not_deleted_flag_value).alias(s.deleted_flag_column),
                )
            )
        else:
            # Prefix tgt columns to avoid join ambiguity
            tgt_r = tgt
            for col_name in tgt.columns:
                tgt_r = tgt_r.withColumnRenamed(col_name, f"_tgt_{col_name}")

            join_cond = (
                (src[bk] == tgt_r[f"_tgt_{bk}"]) &
                (src[ss] == tgt_r[f"_tgt_{ss}"])
            )
            joined = src.join(tgt_r, join_cond, "left")

            is_new = F.col(f"_tgt_{bk}").isNull() & ~is_source_deleted
            is_changed = (~F.col(f"_tgt_{bk}").isNull()) & ~is_source_deleted & (
                (F.col(s.hash_column) != F.col(f"_tgt_{s.hash_column}"))
                | (F.col(f"_tgt_{s.deleted_flag_column}") == F.lit(s.deleted_flag_value))
            )

            if s.scd_type == "scd2":
                new_version = (
                    F.when(is_new, F.lit(1).cast("long"))
                    .when(is_changed, (F.col(f"_tgt_{s.version_column}") + 1).cast("long"))
                )
            else:  # scd1
                new_version = F.when(is_new | is_changed, F.lit(1).cast("long"))

            prev_version = (
                F.when(is_new, F.lit(None).cast("long"))
                .otherwise(F.col(f"_tgt_{s.version_column}").cast("long"))
            )

            change_type = (
                F.when(is_new, F.lit("new"))
                .when(is_changed, F.lit("changed"))
            )

            # The signal and the staged flag are control columns, not business columns.
            src_col_refs = [F.col(c) for c in src.columns if c not in dropped]
            active = (joined
                .filter(is_new | is_changed)
                .select(
                    *src_col_refs,
                    new_version.alias(s.version_column),
                    prev_version.alias(_PREV_VERSION),
                    change_type.alias(_CHANGE_TYPE),
                    F.lit(s.not_deleted_flag_value).alias(s.deleted_flag_column),
                )
            )

            # Source-deleted: rows where the signal = source_deleted_flag_value and tgt row exists
            # F142: skip when the target's current row is already deleted. Compared to what the
            # target is written with, not to the source's signal; NULL counts as not deleted.
            tgt_already_deleted = F.col(f"_tgt_{s.deleted_flag_column}").eqNullSafe(
                F.lit(s.deleted_flag_value)
            )
            is_deleted_from_src = (
                F.col(f"_tgt_{bk}").isNotNull() & is_source_deleted & ~tgt_already_deleted
            )
            src_deleted = (joined
                .filter(is_deleted_from_src)
                .select(
                    *src_col_refs,
                    F.col(f"_tgt_{s.version_column}").cast("long").alias(s.version_column),
                    F.col(f"_tgt_{s.version_column}").cast("long").alias(_PREV_VERSION),
                    F.lit("deleted").alias(_CHANGE_TYPE),
                    F.lit(s.deleted_flag_value).alias(s.deleted_flag_column),
                )
            )

            if s.load_type == "Full" and src.isEmpty():
                tgt_count = tgt.count()
                raise ValueError(
                    f"{type(self).__name__}: Full load aborted — source DataFrame has 0 rows "
                    f"but target has {tgt_count} rows. Merging would delete all target records. "
                    "Fix staging before re-running."
                )

            if s.load_type == "Full":
                src_keys = src.select(F.col(bk), F.col(ss))
                # Select only columns present in active (minus the synthetic ones) from tgt
                # to prevent audit columns (scdEffectiveFrom, scdEffectiveTo, etc.) leaking in.
                tgt_business_cols = [
                    c for c in active.columns
                    if c not in INTERNAL_COLUMNS
                    and c in tgt.columns
                ]
                # F142: a row already deleted is not deleted again. `active` still joins the
                # unfiltered target, so an undelete is unaffected.
                assert tgt is not None  # not first_load, so the target frame exists
                tgt_live = tgt.filter(
                    ~F.col(s.deleted_flag_column).eqNullSafe(F.lit(s.deleted_flag_value))
                )
                deleted = (tgt_live
                    .join(src_keys, [bk, ss], "left_anti")
                    .select(*tgt_business_cols)
                    .withColumn(_CHANGE_TYPE, F.lit("deleted"))
                    .withColumn(_PREV_VERSION, F.col(s.version_column).cast("long"))
                    .withColumn(s.deleted_flag_column, F.lit(s.deleted_flag_value))
                )
                # `deleted` is built from target columns, so a debug column (F111) is absent from it:
                # NULL for a key that is not in the source.
                candidates = active.unionByName(src_deleted).unionByName(deleted, allowMissingColumns=True)
            else:
                candidates = active.unionByName(src_deleted)

        # F211: the writer seals the separator the hash really used, which a constructor value
        # (no context key) can make differ from the context's.
        result = state.with_df(candidates).with_hash_separator(separator)
        # F111: the debug column rides on the candidates for inspection; the writer drops it.
        return result.with_debug_columns(self._debug_hash_string_column) if self._debug_hash_string_column else result


class SCDGracefulChangeDetectorTransformer(SCDChangeDetectorTransformer):
    """Schema-order, trailing-null-trimming variant of SCDChangeDetectorTransformer.

    Uses the field declaration order from ``staging_table_schema`` (not alphabetical
    sort) and drops trailing NULL columns from the hash so that adding a new nullable
    column to the schema never triggers spurious SCD2 version bumps on rows where
    the new column is NULL.

    Interior NULLs (where a non-null column follows in schema order) are preserved
    using the standard ``hash_null_token`` sentinel — identical to the classic strategy.

    Requires ``ctx.global_ctx.staging_table_schema`` to be set (the same StructType
    JSON dict StagingTransformer merges the staging tables against). All other constructor
    parameters are inherited from ``SCDChangeDetectorTransformer``.

    Example::

        # Before — alphabetical order, trailing NULLs sentinelled
        SCDChangeDetectorTransformer()

        # After — schema declaration order, trailing NULLs dropped
        SCDGracefulChangeDetectorTransformer()
    """

    def _ordered_hash_cols(
        self,
        src_columns: list[str],
        excluded: set[str],
        ctx: LoadContext,
    ) -> list[str]:
        """Return hash columns in staging_table_schema declaration order."""
        schema_json = ctx.global_ctx.staging_table_schema
        if not schema_json:
            raise ValueError(
                f"{type(self).__name__} requires staging_table_schema in global_context. "
                "Set it to a StructType JSON dict, or use SCDChangeDetectorTransformer "
                "for alphabetical-sort hashing."
            )
        from pyspark.sql.types import StructType as _StructType
        declared = _StructType.fromJson(schema_json).fieldNames()
        src_set = set(src_columns)
        return [c for c in declared if c not in excluded and c in src_set]

    def _hash_string_expr(self, hash_cols: list[str], separator: str) -> Column:
        """Return the trailing-null-trimmed concatenated string (pre-hash) for debugging."""
        if not hash_cols:
            return F.lit("")
        coalesced = [F.coalesce(F.col(c).cast("string"), F.lit(self._hash_null_token)) for c in hash_cols]
        expr: Column = F.lit("")
        for i in range(len(hash_cols) - 1, -1, -1):
            all_after_null = [F.col(hash_cols[j]).isNull() for j in range(i + 1, len(hash_cols))]
            condition = F.col(hash_cols[i]).isNotNull()
            for c in all_after_null:
                condition = condition & c
            expr = F.when(condition, F.concat_ws(separator, *coalesced[: i + 1])).otherwise(expr)
        return expr

    def _hash_column_expr(self, hash_cols: list[str], separator: str) -> Column:
        """Return the graceful trailing-null-trimmed hash expression."""
        return _graceful_hash_expr(hash_cols, self._hash_null_token, separator)


# F155: the pre-rename names still resolve, with a DeprecationWarning (see _aliases.py).
__getattr__ = deprecated_aliases(__name__, globals(), SCD_RENAMES)
