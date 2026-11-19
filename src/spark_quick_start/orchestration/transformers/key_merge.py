"""
key_merge.py — KeyMergeTransformer.

Contains: the writer of a landing table (F223): the latest state of each key, merged on the
    business key, stamped with the load time, with no SCD column and no history.
Does NOT contain: the source step, which is the user's own (`source_context`), or the Delta
    work, which is `execute_key_merge` in load_pipeline.py, reached through `_pipeline.py`.

A landing table is the raw layer of a medallion architecture: a source's records as they are,
one row per key, refreshed incrementally. Bronze then reads it as any other source, by its
load-time column.
"""
from __future__ import annotations

from typing import ClassVar

from spark_quick_start.orchestration.transformers._pipeline import execute_key_merge
from spark_quick_start.orchestration.transformers.base import LoadContext, ProcessTransformer
from spark_quick_start.orchestration.transformers.state import PipelineState

_STYLE = "landing"


class KeyMergeTransformer(ProcessTransformer):
    """Merge the batch into a landing table on the business key; the latest row of a key wins.

    For a load whose `scd_context.scd_type` is `landing`: the key is
    `scd_context.business_key_column`, the ordering column `hwm_context.hwm_column_name`, and
    the load time, `global_context.load_timestamp`, is stamped in `scd_context.load_ts_column`.
    The batch's rows of one key are reduced to the latest; a key the table holds is updated only
    by a newer row; a column the table lacks is added and a retyped one refused; an empty first
    batch creates no table. See `execute_key_merge`.

    Reports `inserted` and `updated` from the write's own Delta commit (`SCDMetricsProvider`).
    It reports no watermark: the source step knows the latest value it read, at no cost.

    Cost: with `check_keys`, one aggregation over the batch for NULL keys and ordering values;
    one shuffle for the dedup; the table's existence; on a first load `isEmpty()`, the create and
    one history read; otherwise a schema read, one history read for the version before the
    write, the MERGE (with an `ALTER TABLE` first for a new column) and one history read for the
    counts. No `count()`, no cache.

    Args:
        check_keys: refuse a batch with a NULL key or ordering value, at the cost of the one
            aggregation. A NULL key matches nothing and would be inserted again on every run.
    """

    required_scd_type: ClassVar[str] = _STYLE

    def __init__(self, *, check_keys: bool = True) -> None:
        self.check_keys = check_keys
        self.inserted: int | None = None  # None until _run sets it (F117)
        self.updated: int | None = None

    def _run(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
        from spark_quick_start.logger import OrchestratorLogger

        ctx.scd_ctx.require_scd_type(type(self).__name__, _STYLE)
        if ctx.hwm_ctx is None:
            raise ValueError(
                f"{type(self).__name__}: a landing load orders each key's rows by "
                "hwm_context.hwm_column_name, and the load context has no hwm_context."
            )
        assert self._spark is not None and self._adapter is not None
        target = ctx.global_ctx.target_table_path
        assert target is not None, "validate_all() requires global_context.target_table_path"
        self.inserted, self.updated = execute_key_merge(
            self._spark, self._adapter, state.df, target,
            key=ctx.scd_ctx.business_key_column,
            ordering=ctx.hwm_ctx.hwm_column_name,
            load_ts_column=ctx.scd_ctx.load_ts_column,
            load_ts=ctx.global_ctx.load_timestamp_utc,
            logger=self._logger or OrchestratorLogger(),
            check_nulls=self.check_keys,
        )
        return state
