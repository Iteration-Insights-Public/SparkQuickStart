"""
dqd.py — DQDTransformer: a Declarative Query Definition as the load's source.

Contains: DQDTransformer.
Does NOT contain: the expression language (orchestration/dqd, reached through _dqd.py), joins,
    aggregation or sorting (a DQD maps one row in to one row out), or the Fabric Warehouse
    connector call, which is FabricAdapter's (F140). The design is
    docs/architecture/decisions/2026-09-29-declarative-query-definition-design.md.

Usage::
    LoadOrchestrator(load_context_json, spark, platform="fabric").add(DQDTransformer())...
"""
from __future__ import annotations

import dataclasses
import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pyspark.sql import functions as F

from spark_quick_start.orchestration.transformers._dqd import (
    ColumnRef,
    DQDDefinitionError,
    parse_expression,
    parse_predicate,
    resolve_expression,
    resolve_predicate,
)
from spark_quick_start.orchestration.transformers.base import LoadContext, SourceTransformer
from spark_quick_start.orchestration.transformers.materialize import (
    _materialize,
    scratch_table_name,
)
from spark_quick_start.orchestration.transformers.state import PipelineState

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from spark_quick_start.orchestration.models import DQDContext

__all__ = ["DQDTransformer"]


@dataclass(frozen=True)
class _Entry:
    """A named transformation or selection entry, its expression parsed."""

    name: str
    expression: Any
    path: str


class DQDTransformer(SourceTransformer):
    """Read one source and shape it row by row, as the load context's `dqd_context` says.

    A definition given to the constructor replaces `dqd_context`, but the load context must
    still have a `dqd_context` or staging stages to pass validation.

    The definition is parsed in full before the source is read, so a malformed one fails with
    the place it is in and no Spark work done. Then, in order: read by `source_kind` through the
    `PlatformAdapter`; keep `source_columns`; apply `filters`; apply the `hwm_context` watermark,
    `hwm_column_name > hwm_value`; add `transformations` in order; project `selection`. Column
    names are matched as the session matches them (`spark.sql.caseSensitive`).

    A SourceMetricsProvider when the load has an `hwm_context`: `source_count` (and
    `target_count`) is the number of rows passed on, counted in the same aggregation as the
    watermark. Without `hwm_context` nothing is counted and both stay None.

    An HWMProvider when the load has an `hwm_context`: `hwm_before` is its `hwm_value` as text,
    and `hwm_after` the latest `hwm_column_name` in the filtered source, formatted by Spark in
    the session time zone, kept when nothing is new.

    With `dqd_context.eager_materialize_source` true, the filtered, watermarked source is written
    once to the scratch table `{staging_table_path}_{id}_0_dqd` (dropped and rewritten on every
    run; `id` is `global_context.id`, and a source step is at position 0), and the watermark's
    aggregation, then every later step, reads that table. The name is built by
    `scratch_table_name`, as the Materialize steps' are, so no other step's can equal it; it relies
    on an `id` that is unique, ignoring case, among the load contexts sharing a
    `staging_table_path`; two concurrent runs of one context still share scratch tables (F129). The raw source columns are written under positional names (`_c0`, ...) and renamed
    back on the read, so names Delta refuses are safe. The value must be a boolean,
    `staging_table_path` set and different from `source_identifier` once suffixed, and no
    `transformations` or `selection` entry may read `_metadata` (it would describe the scratch
    files; filters run before the write and may), or `ValueError` is raised before any Spark work.

    Cost (`spark-delta-cost` rule 3): without `hwm_context` the step runs no Spark action over
    the source's rows (opening a Delta table or inferring a parquet schema still runs jobs), and
    the source is read only by what the pipeline runs after it. With `hwm_context` it runs one, a
    single aggregation over the filtered source, `max` of the watermark column, the row count and
    the check that `hwm_value` casts to the column's type, so the source is read once more than
    without. Each later action reads it again. Measured in scans of the source, with Delta's
    `materializeSource` at "none" (a counting UDF makes "auto" materialize the merge's source,
    one to two scans fewer, so a deterministic source may see fewer; not proven for Delta sources):

    =============================================  ==========  ==========
    Pipeline after this step                       first load  later load
    =============================================  ==========  ==========
    target step, detector, merge                    1 (3 hwm)  4 (5 hwm)
    the same with `CandidateGuardTransformer`       2 (4 hwm)  5 (6 hwm)
    the guard and `MaterializeTransformer` next     1 (2 hwm)  1 (2 hwm)
    =============================================  ==========  ==========

    The merge step's number of passes over the source depends on Delta's
    `spark.databricks.delta.merge.materializeSource` and on the runtime's default for it, which
    this library has not checked on Fabric or Databricks (F200): the figures above are for "none", the
    setting left at "auto" may show fewer, and the library neither sets nor reads it. Only
    `eager_materialize_source` reads the source once whatever the setting is.

    A first load's merge step reads it once, but twice with `hwm_context`: one pass beyond the
    aggregation's, not traced. On a later
    load (SCD2, every key changed) the merge step itself reads it several times, among them
    `NativeSCDMergeTransformer`'s close-out probe `needs_closeout.isEmpty()`; `capture_partitions`
    was measured to change nothing. The aggregation is not cached, because `.cache()` is
    ineffective on serverless (rule 1). It is not taken from the write instead, for two reasons:
    the first action after this step need not be the write, since the close-out probe, or the
    append-only writer's `changed.isEmpty()`, runs first and sees only part of the rows, so a max
    observed there would be partial; and the cast check must fail before anything is written.
    `eager_materialize_source` removes both, and is the answer when the source is expensive (a
    view, a Warehouse view, a wide join): it adds one source read, the scratch write, and the
    aggregation and every later action read the scratch table instead, so the source is read once
    in total, with or without `hwm_context`, the guard and a later load (measured, below). Leave
    it off for a cheap Delta source: the extra write costs more than the reads it saves. The
    malformed-`hwm_value` check still fails before the target is written, but after the scratch
    table is. Measured with the flag, same method as the table:

    =============================================  ==========  ==========
    `eager_materialize_source` true, after this    first load  later load
    =============================================  ==========  ==========
    the guard, no `hwm_context`                     1           1
    the guard and `hwm_context`                     1           1
    =============================================  ==========  ==========

    Not proven here: what the scratch write costs on Fabric or Databricks serverless, or with a
    Unity Catalog name as `staging_table_path`.

    The step names no SCD column: the business key and the deleted flag are checked and read
    downstream through the context's column map (the design's decision 11).

    Args:
        dqd_definition: A definition to use instead of the load context's `dqd_context`, in the
            same shape.
    """

    def __init__(self, dqd_definition: dict[str, Any] | None = None) -> None:
        self._dqd_definition = dqd_definition
        # HWMProvider: declared always, set only in _produce (rule: transformer-contract).
        self.hwm_before: str | None = None
        self.hwm_after: str | None = None
        self.hwm_column: str | None = None
        # SourceMetricsProvider (F144): the rows passed on, counted in the watermark's aggregation,
        # so only with hwm_context; None otherwise means "not computed", not zero rows.
        self.source_count: int | None = None
        self.target_count: int | None = None

    def _definition(self, ctx: LoadContext) -> DQDContext:
        from spark_quick_start.orchestration.models import DQDContext  # the pure-data class

        if self._dqd_definition is not None:
            definition = DQDContext(self._dqd_definition)
            definition.validate()
            return definition
        if ctx.dqd_ctx is None:
            raise ValueError(
                "DQDTransformer: the load context has no dqd_context and none was given to the "
                "constructor"
            )
        return ctx.dqd_ctx

    def _produce(self, ctx: LoadContext) -> PipelineState:
        definition = self._definition(ctx)
        filters = [parse_predicate(f, path=f"dqd_context.filters[{i}]") for i, f in enumerate(definition.filters)]
        transformations = [
            _Entry(e["name"], parse_expression(e["expression"], path=f"dqd_context.transformations[{i}].expression"),
                   f"dqd_context.transformations[{i}]")
            for i, e in enumerate(definition.transformations)
        ]
        selection = [
            _Entry(e["name"], parse_expression(e["expression"], path=f"dqd_context.selection[{i}].expression")
                   if "expression" in e else ColumnRef(e["name"], path=f"dqd_context.selection[{i}].name"),
                   f"dqd_context.selection[{i}]")
            for i, e in enumerate(definition.selection)
        ]
        scratch_path = self._scratch_path(definition, ctx)
        if scratch_path is not None:
            self._refuse_metadata([*transformations, *selection])
        spark, adapter = self._spark, self._adapter
        if spark is None or adapter is None:
            raise RuntimeError(
                "DQDTransformer: no Spark session or adapter; run it through LoadOrchestrator, "
                "which injects both at .run() time"
            )
        case_sensitive = str(spark.conf.get("spark.sql.caseSensitive", "false")).lower() == "true"

        if definition.source_kind == "warehouse_view":
            df = adapter.read_warehouse_view(spark, definition.source_identifier,
                                             workspace_id=definition.workspace_id,
                                             lakehouse_id=definition.lakehouse_id)
        else:
            readers = {"delta": adapter.read_delta, "parquet": adapter.read_parquet, "view": adapter.read_table}
            df = readers[definition.source_kind](spark, definition.source_identifier)
        if definition.source_columns:
            # Aliased to the name as written, so a struct field read here ("address.city") is
            # found by that whole name by the filters, the watermark and the transformations.
            df = df.select(*[self._column(c, df.columns, case_sensitive, "dqd_context.source_columns").alias(c)
                             for c in definition.source_columns])
        for predicate in filters:
            df = df.filter(resolve_predicate(predicate, df.columns, case_sensitive=case_sensitive))
        df, previous = self._filter_watermark(df, ctx, case_sensitive)
        if scratch_path is not None:
            df = self._round_trip(spark, adapter, df, scratch_path)
        self._aggregate_watermark(df, ctx, case_sensitive, previous)
        for entry in transformations:
            df = df.withColumn(entry.name, resolve_expression(entry.expression, df.columns,
                                                              case_sensitive=case_sensitive))
        if selection:
            df = df.select(*[resolve_expression(e.expression, df.columns, case_sensitive=case_sensitive).alias(e.name)
                             for e in selection])
        return PipelineState(df=df)

    @staticmethod
    def _column(name: str, columns: list[str], case_sensitive: bool, path: str) -> Column:
        return resolve_expression(ColumnRef(name, path=path), columns, case_sensitive=case_sensitive)

    def _scratch_path(self, definition: DQDContext, ctx: LoadContext) -> str | None:
        """The scratch table for `eager_materialize_source`, or None when it is off; no Spark work."""
        if not definition.eager_materialize_source:
            return None
        base = ctx.global_ctx.staging_table_path
        if not isinstance(base, str) or not base.strip():
            raise ValueError(
                "DQDTransformer: dqd_context.eager_materialize_source needs "
                "global_context.staging_table_path, where the scratch table goes; it is not set"
            )
        path = scratch_table_name(ctx, self._stage_index, "dqd")
        if definition.source_identifier == path:
            raise ValueError(
                f"DQDTransformer: dqd_context.source_identifier {path!r} is the eager_materialize_source "
                "scratch table, which is dropped before the source is written to it; use another "
                "global_context.staging_table_path or id"
            )
        return path

    @classmethod
    def _refuse_metadata(cls, entries: list[_Entry]) -> None:
        """Raise when an entry reads `_metadata`: after the scratch round trip it would describe the scratch files."""
        for entry in entries:
            if cls._reads_metadata(entry.expression):
                raise ValueError(
                    f"{entry.path}: reads the source's `_metadata` column, which "
                    "dqd_context.eager_materialize_source cannot keep: after the scratch round trip it "
                    "would describe the scratch table's files, not the source's. Filters may use it, "
                    "as they run before the write; or turn eager_materialize_source off"
                )

    @classmethod
    def _reads_metadata(cls, node: Any) -> bool:
        if isinstance(node, ColumnRef):
            return node.name.startswith("_metadata")
        if isinstance(node, tuple):
            return any(cls._reads_metadata(n) for n in node)
        if dataclasses.is_dataclass(node) and not isinstance(node, type):
            return any(cls._reads_metadata(getattr(node, f.name)) for f in dataclasses.fields(node))
        return False

    @staticmethod
    def _round_trip(spark: Any, adapter: Any, df: DataFrame, path: str) -> DataFrame:
        """Write `df` to the scratch table and read it back, under positional names; one action.

        The raw source frame, before selection, is what is written, so its column names are
        whatever the source has: names Delta refuses (a space, or `,;{}()=`) or that differ only in
        case. Written as `_c0`, `_c1`, ..., and renamed back on the read-back by position, none
        of that reaches Delta.
        """
        names = df.columns
        scratch = _materialize(spark, adapter, df.toDF(*[f"_c{i}" for i in range(len(names))]), path)
        return scratch.toDF(*names)

    def _filter_watermark(
        self, df: DataFrame, ctx: LoadContext, case_sensitive: bool
    ) -> tuple[DataFrame, Column | None]:
        """Filter by `hwm_context`, set `hwm_column` and `hwm_before`; no Spark action.

        Returns the frame and the previous value cast to the column's type, or None when there is
        none to compare with. Without `hwm_context` every provider attribute is reset.
        """
        hwm = ctx.hwm_ctx
        if hwm is None:
            self.hwm_before = self.hwm_after = self.hwm_column = None
            self.source_count = self.target_count = None
            return df, None
        try:
            column = self._column(hwm.hwm_column_name, df.columns, case_sensitive, "hwm_context.hwm_column_name")
        except DQDDefinitionError as exc:
            raise ValueError(f"hwm_context: {exc}") from None
        self.hwm_column = hwm.hwm_column_name
        self.hwm_before = None if hwm.hwm_value is None else str(hwm.hwm_value)
        # The previous value is cast to the column's type explicitly: left to Spark's coercion, a
        # malformed value becomes NULL on a non-ANSI session, the filter matches nothing, and
        # every run succeeds having loaded nothing (F139 review, measured). It is a try_cast, so
        # a malformed value is NULL on an ANSI session too (the Spark 4 default), where a plain
        # cast raises its own CAST_INVALID_INPUT instead of our refusal (F165). The NULL check
        # rides on the one aggregation, so it costs no extra action.
        if hwm.hwm_value is None:
            return df, None
        if isinstance(hwm.hwm_value, float) and not math.isfinite(hwm.hwm_value):
            raise ValueError(
                f"hwm_context: hwm_value {hwm.hwm_value!r} cannot be read as the type of "
                f"hwm_column_name '{hwm.hwm_column_name}': it is not a finite number"
            )
        previous = self._try_cast_literal(hwm.hwm_value, df.select(column).schema[0].dataType)
        return df.filter(column > previous), previous

    @staticmethod
    def _try_cast_literal(value: str | int | float, data_type: Any) -> Column:
        """`value` as `data_type`, NULL when it does not fit, on an ANSI or a non-ANSI session; no action.

        SQL `try_cast`, because `Column.try_cast` is PySpark 4 only. A string goes in as hex
        decoded in the expression, so no quote or backslash in it is read as SQL. A number keeps
        its numeric type, as `F.lit(value).cast(...)` did: an int is an integer literal and a
        float a DOUBLE, so `5.0` still reads as 5 in an int column and an epoch number as a
        timestamp. The caller has refused NaN and infinity.
        """
        if isinstance(value, int):
            literal = repr(value)
        elif isinstance(value, float):
            literal = f"CAST('{value!r}' AS DOUBLE)"
        else:
            literal = f"decode(unhex('{value.encode('utf-8').hex()}'), 'UTF-8')"
        return F.expr(f"try_cast({literal} AS {data_type.simpleString()})")

    def _aggregate_watermark(
        self, df: DataFrame, ctx: LoadContext, case_sensitive: bool, previous: Column | None
    ) -> None:
        """Set `hwm_after` and the counts from one `max`/count aggregation; none without `hwm_context`.

        `df` is the filtered frame, or its scratch-table copy under `eager_materialize_source`; the
        column is resolved again on it by name.
        """
        hwm = ctx.hwm_ctx
        if hwm is None:
            return
        column = self._column(hwm.hwm_column_name, df.columns, case_sensitive, "hwm_context.hwm_column_name")
        checks = [F.max(column).cast("string"), F.count(F.lit(1))] + (
            [previous.isNull()] if previous is not None else [])
        row = df.agg(*checks).collect()[0]
        if previous is not None and row[2]:
            raise ValueError(
                f"hwm_context: hwm_value {hwm.hwm_value!r} cannot be read as the type of "
                f"hwm_column_name '{hwm.hwm_column_name}'"
            )
        self.hwm_after = self.hwm_before if row[0] is None else row[0]
        # The rows after the filters and the watermark, before transformations and selection,
        # which keep one row for each; mirrored to target_count as SourceMetricsProvider asks.
        self.source_count = self.target_count = int(row[1])
