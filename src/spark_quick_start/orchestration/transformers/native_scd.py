"""
native_scd.py — NativeSCDMergeTransformer and NativeSCDAppendOnlyTransformer.

Consume the candidates DataFrame produced by SCDChangeDetectorTransformer, stamp
SCD2 audit fields (scdEffectiveFrom, scdEffectiveTo, scdCurrentFlag, scdCorrelationId),
build SCD2 close-out rows, and delegate to existing execute_scd_merge /
execute_append_only helpers from load_pipeline.py.

No fabric_scd2 dependency.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from pyspark.sql import functions as F

from spark_quick_start.orchestration.models import SCDContext
from spark_quick_start.orchestration.transformers._constants import (
    CHANGE_TYPE as _CHANGE_TYPE,
)
from spark_quick_start.orchestration.transformers._constants import INTERNAL_COLUMNS, Constants
from spark_quick_start.orchestration.transformers._constants import (
    PREV_VERSION as _PREV_VERSION,
)
from spark_quick_start.orchestration.transformers._pipeline import (
    capture_merge_statistics,
    capture_write_statistics,
    execute_append_only,
    execute_scd_merge,
    get_high_watermark,
)
from spark_quick_start.orchestration.transformers._scd_seal import (
    check_scd_seal,
    scd_settings,
    seal_after_write,
)
from spark_quick_start.orchestration.transformers.base import LoadContext, ProcessTransformer
from spark_quick_start.orchestration.transformers.protocols import HWMProvider
from spark_quick_start.orchestration.transformers.state import PipelineState

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def _stamp_audit(df: DataFrame, s: SCDContext, scd_effective_from: datetime, correlation_id: str) -> DataFrame:
    """Stamp scdEffectiveFrom, scdEffectiveTo (MAX), scdCurrentFlag, scdCorrelationId.

    scd_effective_from must be a timezone-aware datetime — F.lit() serializes it as UTC,
    avoiding session-timezone drift on non-UTC Spark clusters. So is the open row's
    effective-to, `scd_context.default_max_timestamp` read as a UTC instant (F106; spark-delta-cost
    6): the string the shipped default used to be was read in the session's time zone.
    """
    return (
        df
        .withColumn(s.effective_from_column, F.lit(scd_effective_from).cast("timestamp"))
        .withColumn(s.effective_to_column, F.lit(s.default_max_timestamp_utc).cast("timestamp"))
        .withColumn(s.current_flag_column, F.lit(s.current_flag_value))
        .withColumn(s.correlation_id_column, F.lit(correlation_id))
    )


def _warn_watermark_unread(logger: Any, when: str, target_path: str | None, exc: Exception) -> None:
    """Log that the watermark read around a write failed; the load goes on.

    The write has committed (or the read precedes it), so a failed read costs only the
    watermark, which the orchestrator then reads for itself or reports as None; it must not
    fail the load. The counts do not depend on it (F117).
    """
    logger.warning(
        f"Could not read the watermark {when} the write to {target_path}: "
        f"{type(exc).__name__}: {exc}. The load continues; hwm_value_{when} may be empty."
    )


def _drop_internal(df: DataFrame, debug_columns: frozenset[str] = frozenset()) -> DataFrame:
    """Drop pipeline-internal and declared debug columns before registering as a staging view (F111)."""
    return df.drop(*[c for c in (*INTERNAL_COLUMNS, *debug_columns) if c in df.columns])


class NativeSCDMergeTransformer(ProcessTransformer, HWMProvider):
    """Stamp SCD2 audit fields, build close-out rows, and execute Delta MERGE.

    Reads candidates from state.df (produced by SCDChangeDetectorTransformer). Stamps
    scdEffectiveFrom, scdEffectiveTo, scdCurrentFlag, scdCorrelationId. For SCD1, a
    `deleted` candidate is a soft delete: the matched target row is flagged deleted and
    stamped with effective-from and the correlation id, and keeps its business columns
    (counted in `updated`). For SCD2,
    builds close-out rows by joining changed candidates against state.frames["target"]
    on (business_key, _prev_scdVersion, source_system). Delegates to execute_scd_merge.

    Implements HWMProvider — LoadOrchestrator reads hwm_before/after/column after the
    pipeline completes. A source transformer registered earlier in the pipeline takes
    precedence because LoadOrchestrator picks the first HWMProvider with a non-None
    hwm_after.

    After __call__: .inserted, .updated, .hwm_before, .hwm_after, .hwm_column are set (the
    counts are None before it runs).

    Requires self._spark to be set (injected by the pipeline runner).

    Time: every row written is stamped effective-from with `global_context.load_timestamp`, the
    run's load time T, and an SCD2 close-out ends at T less one microsecond (F106). The step does
    not check that T increases; a T equal to or earlier than the target's newest effective-from
    is the user's responsibility. Under SCD2 the closed row's effective-to precedes its
    effective-from and the new version's interval starts before the old one's, so the intervals
    invert (the MERGE never rewrites effective-from, so the watermark does not move). Under SCD1
    an earlier T rewinds the updated or soft-deleted row's effective-from and can lower the
    reported watermark. The append-only writer stamps rows behind existing ones. Pass a later T
    to rerun or backfill.

    Seal (F211): before the write the step reads the target's table properties once (one metadata
    action, no data read; skipped when the target step stated the target absent) and compares the
    seven values the rows depend on (flag values, `default_max_timestamp`, the hash separator the
    detector used, from `state.hash_separator`, and the session time zone) with the seal. A target the property helpers do not
    accept is neither checked nor sealed (one WARNING; for a target this load creates, found at the seal). After a successful write it seals a target
    this load created, or one that held rows with no seal (a WARNING for the latter). A failure to
    seal is a WARNING and the load stays `success`; the next load retries.
    Cost: a sealed target costs one `DESCRIBE DETAIL`; a first write one (the compare-and-set
    re-read) and one Delta commit on table metadata; an unsealed one two reads and the commit; with
    no target step, one more `delta_table_exists`.

    Cost (F9): before the MERGE the step takes at most one Spark action on the candidates' lineage:
    under SCD2, one `isEmpty()` on the changed-or-deleted candidates, which answers whether there
    is close-out work, whether the target frame is required, and whether the deleted-version rows
    are built; under SCD1 none (the soft-delete rows are staged lazily). A first load, and a batch
    of new rows only, still pay that one probe under SCD2. Each pass re-runs the lineage
    upstream of the step (staging read, target join, hashing), so place a
    `MaterializeTransformer` before it when that lineage is expensive. Then the MERGE itself, one
    `delta_table_exists`, the target's max effective-from before the write when it exists, and
    after it, and the commit-metadata reads for the counts. The step takes no cache and no `count()`.

    Raises:
        ValueError: When the target is sealed with a value that differs from this load's: raised
            before anything is written, naming the table, the property and both values (F211).
            For SCD2, when candidates are classified `changed` or `deleted`
            but state.frames holds no target frame (`state.target_exists` is False, or
            None when no target step ran; the message says which). Either classification
            implies an existing target, so the combination means something replaced the
            state between change detection and the merge; proceeding would skip
            the close-out and leave two rows flagged current for one business key.
            Unreachable from a pipeline built by pipeline_factory (F24).
    """

    # F156: refused under a Data Vault style, at composition (LoadOrchestrator) and in _run.
    scd_style_only = True

    def __init__(self) -> None:
        # None until _run sets them: the orchestrator reads the counts of the writer whose
        # counts are not None, which is how it tells the branch that ran from one that did not (F117).
        self.inserted: int | None = None
        self.updated: int | None = None
        self.hwm_before: str | None = None
        self.hwm_after: str | None = None
        self.hwm_column: str | None = None

    def _run(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
        ctx.scd_ctx.require_scd_style(type(self).__name__)
        from spark_quick_start.logger import OrchestratorLogger
        logger = self._logger or OrchestratorLogger()

        s = ctx.scd_ctx
        bk = s.business_key_column
        ss = s.source_system_column
        target_path = ctx.global_ctx.target_table_path
        correlation_id = ctx.runtime_ctx.correlation_id
        assert correlation_id, "RuntimeContext.validate() requires correlation_id (F116)"  # no fallback

        # Bind timestamps as Python UTC datetimes — F.lit() serializes UTC offset, keeping
        # effective_from and effective_to consistent regardless of Spark session timezone.
        scd_effective_from = ctx.global_ctx.load_timestamp_utc  # the run's load time T (F106)
        scd_effective_to_closeout = scd_effective_from - timedelta(microseconds=1)

        candidates = state.df

        # Resolve target frame once — used by both SCD2 close-out blocks below
        tgt = state.frames.get(Constants.TARGET_FRAME_KEY)

        # Build renamed target once to avoid duplicate rename loops in Groups B and C
        tgt_r: DataFrame | None = None
        if tgt is not None:
            tgt_r = tgt
            for col_name in tgt.columns:
                tgt_r = tgt_r.withColumnRenamed(col_name, f"_tgt_{col_name}")

        # ── Group A: new + changed with audit fields ──────────────────────────
        group_a = _stamp_audit(
            candidates.filter(F.col(_CHANGE_TYPE).isin("new", "changed")),
            s, scd_effective_from, correlation_id,
        )

        merged = group_a

        # ── SCD1 soft delete (F110): deleted candidates are staged beside group A, stamped the
        # way an update is. execute_scd_merge matches them to the target row and flags it,
        # leaving its business columns, hash and version. No close-out, no new version. No
        # Spark action: the candidates are counted by the MERGE's own metrics, as `updated`.
        if s.scd_type == "scd1":
            soft_deletes = _stamp_audit(
                candidates.filter(F.col(_CHANGE_TYPE) == "deleted"),
                s, scd_effective_from, correlation_id,
            )
            merged = merged.unionByName(soft_deletes, allowMissingColumns=True)

        # ── Group B: SCD2 close-out rows for changed + deleted keys ──────────────
        # Both "changed" and "deleted" candidates require closing out the current
        # target row (current flag = inactive_flag_value, effective-to = T-1μs) regardless of
        # load type — the old version must no longer be current before the new one
        # is inserted.
        if s.scd_type == "scd2":
            needs_closeout = candidates.filter(F.col(_CHANGE_TYPE).isin("changed", "deleted"))
            # One Spark action for the whole scd2 path, consumed three times below:
            # this skip, the assertion, and Group D's skip. Previously there were two
            # (`needs_closeout.isEmpty()` here, short-circuited away on first load, and
            # `deleted_candidates.isEmpty()` in Group D, which ran unconditionally), so
            # this is one fewer on the normal path and equal on first load — the shape
            # spark-delta-cost §3 prescribes, one probe answering several questions.
            has_closeout_work = not needs_closeout.isEmpty()

            # A candidate classified `changed` or `deleted` with no target frame is a
            # contradiction, not a state to handle. Both classifications happen inside
            # the else of `first_load = tgt is None or tgt.isEmpty()`
            # (change_detector.py, where first_load is set), so either one proves the frame was present and
            # non-empty when the row was classified.
            #
            # Without this, the close-out below is skipped — its condition is
            # `tgt_r is not None` — while Group A still stamps the changed row current
            # at prev+1 and Group D still inserts the deleted row at prev+1. The MERGE
            # then matches on (bk, ss, version, currentFlag='Y') (load_pipeline.py:129),
            # the new version does not match the target's live one, and
            # whenNotMatchedInsertAll fires: two rows flagged current for one business
            # key. Silent, self-perpetuating, and per F25 a later run cannot recover it.
            #
            # Covering both kinds rather than `deleted` alone is the point. Guarding
            # only deletes would fire only when a batch happened to contain one, so the
            # same broken pipeline would pass in dev and corrupt in prod depending on
            # data content — the least useful firing condition a guard can have.
            if has_closeout_work and tgt_r is None:
                what = (
                    "a target step stated the target does not exist (target_exists=False)"
                    if state.target_exists is False
                    else "no target step ran (target_exists=None)"
                )
                raise ValueError(
                    "NativeSCDMergeTransformer received candidates classified "
                    "'changed' or 'deleted' but there is no target frame under "
                    f"state.frames[{Constants.TARGET_FRAME_KEY!r}]: {what}. Either classification "
                    "can only happen against an existing target, so this combination "
                    "cannot arise from a pipeline built by pipeline_factory; a custom "
                    "ProcessTransformer has replaced the state between change detection "
                    "and the merge. Proceeding would close out nothing "
                    "and insert a second row flagged current for the same business key."
                )

            if tgt_r is not None and has_closeout_work:
                closeout_keys = needs_closeout.select(
                    F.col(bk).alias("_key_bk"),
                    F.col(ss).alias("_key_ss"),
                    F.col(_PREV_VERSION).alias("_key_ver"),
                )
                close_outs = (
                    closeout_keys.join(
                        tgt_r,
                        (F.col("_key_bk") == F.col(f"_tgt_{bk}")) &
                        (F.col("_key_ss") == F.col(f"_tgt_{ss}")) &
                        (F.col("_key_ver") == F.col(f"_tgt_{s.version_column}")),
                        "inner",
                    )
                    .select([F.col(f"_tgt_{c}").alias(c) for c in tgt.columns])
                    .withColumn(s.current_flag_column, F.lit(s.inactive_flag_value))
                    .withColumn(
                        s.effective_to_column,
                        F.lit(scd_effective_to_closeout).cast("timestamp"),
                    )
                )
                merged = merged.unionByName(close_outs, allowMissingColumns=True)

        # ── Group D: new soft-delete version rows (deleted → insert new version) ──
        # Each deleted entity needs a new version row (version = prev + 1, deleted_flag = Y)
        # in addition to the close-out from Group C.  The scdHash from SCDChangeDetectorTransformer
        # is already correct for both source-deleted and full-load-deleted candidates.
        if s.scd_type == "scd2":
            deleted_candidates = candidates.filter(F.col(_CHANGE_TYPE) == "deleted")
            # Reuses the single probe above. `deleted` is a subset of `changed ∪ deleted`,
            # so a batch with changes but no deletes now reaches a lazy no-op
            # unionByName of an empty filter instead of paying an eager isEmpty() to
            # avoid it — a plan branch on a scan that already happens, traded for one
            # fewer action. The target-presence assertion above has already run, so this
            # block can no longer be reached with tgt_r None.
            if has_closeout_work:
                new_deleted = deleted_candidates.withColumn(
                    s.version_column, F.col(_PREV_VERSION) + 1
                )
                group_d = _stamp_audit(new_deleted, s, scd_effective_from, correlation_id)
                merged = merged.unionByName(group_d, allowMissingColumns=True)

        # ── Register staging view and execute MERGE ───────────────────────────
        final = _drop_internal(merged, state.debug_columns)
        view_name = f"_fqs_native_merge_{uuid.uuid4().hex}"
        final.createOrReplaceTempView(view_name)

        assert self._adapter is not None and self._spark is not None and target_path, \
            "services are injected at run() time (transformer-contract)"
        settings = scd_settings(s, state.hash_separator, spark=self._spark)
        seal_plan = check_scd_seal(self._adapter, self._spark, target_path, settings,
                                   target_exists=state.target_exists, logger=logger)

        if self._adapter.delta_table_exists(self._spark, target_path):
            try:
                self.hwm_before = get_high_watermark(self._spark, target_path, self._adapter, hwm_column=s.effective_from_column)
            except Exception as exc:
                _warn_watermark_unread(logger, "before", target_path, exc)

        pre_version = execute_scd_merge(self._spark, s, target_path, view_name, self._adapter, logger,
                                        target_exists=state.target_exists)
        self.inserted, self.updated, _ = capture_merge_statistics(
            self._spark, target_path, pre_version, self._adapter, logger
        )
        seal_after_write(self._adapter, self._spark, target_path, settings, seal_plan, logger)
        try:
            self.hwm_after = get_high_watermark(self._spark, target_path, self._adapter, hwm_column=s.effective_from_column)
        except Exception as exc:
            _warn_watermark_unread(logger, "after", target_path, exc)
        self.hwm_column = s.effective_from_column

        return state

    def after_run(self, state: PipelineState, ctx: LoadContext) -> None:
        super().after_run(state, ctx, extra_metrics={"inserted": self.inserted, "updated": self.updated})


class NativeSCDAppendOnlyTransformer(ProcessTransformer, HWMProvider):
    """Append-only load for immutable event data — never updated or deleted.

    Reads candidates from state.df (produced by SCDChangeDetectorTransformer).
    Stamps the same SCD audit columns as NativeSCDMergeTransformer (keeps
    target schema consistent across entities), but writes via a raw Delta
    append — no .merge() call, no second target-table scan. Trusts
    SCDChangeDetectorTransformer's "new" classification as authoritative rather
    than re-verifying it.

    Raises ValueError if any candidate is classified "changed" — a same-key,
    different-hash row is a data-quality anomaly for data declared immutable,
    not a valid update; silently appending it would create two conflicting
    facts under one key with no SCD columns to explain which is current.

    Implements HWMProvider — see NativeSCDMergeTransformer for priority rules, and for how the
    run's load time stamps effective-from (F106; it is not checked to increase).

    The target step's answer (`state.target_exists`) decides between creating and appending
    (F210): False creates through `errorifexists`, so a target the step wrongly missed is refused
    and the load fails with it untouched, rather than receiving rows it already holds.

    Seal (F211), as NativeSCDMergeTransformer, including its cost: one metadata read of the
    target's table properties before the write, a refusal (`ValueError`, nothing written) when a
    sealed value differs from this load's, and a seal after a successful write of a target this
    load created or that had none. A seal that cannot be written, or an unsupported target, is a
    WARNING and the load stays `success`.

    After __call__: .inserted, .hwm_before, .hwm_after, .hwm_column are set.

    Requires self._spark to be set (injected by the pipeline runner).
    """

    # F156: refused under a Data Vault style, at composition (LoadOrchestrator) and in _run.
    scd_style_only = True

    def __init__(self) -> None:
        # None until _run sets them (F117); `updated` is then always 0, as this load style never
        # updates an existing row. Declared rather than omitted so the class satisfies
        # SCDMetricsProvider, letting LoadOrchestrator read a real attribute instead of probing
        # with getattr(tx, "updated", 0), which cannot tell "no such attribute" from "zero".
        self.inserted: int | None = None
        self.updated: int | None = None
        self.hwm_before: str | None = None
        self.hwm_after: str | None = None
        self.hwm_column: str | None = None

    def _run(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
        ctx.scd_ctx.require_scd_style(type(self).__name__)
        from spark_quick_start.logger import OrchestratorLogger
        logger = self._logger or OrchestratorLogger()

        s = ctx.scd_ctx
        target_path = ctx.global_ctx.target_table_path
        correlation_id = ctx.runtime_ctx.correlation_id
        assert correlation_id, "RuntimeContext.validate() requires correlation_id (F116)"  # no fallback
        scd_effective_from = ctx.global_ctx.load_timestamp_utc  # the run's load time T (F106)

        changed = state.df.filter(F.col(_CHANGE_TYPE) == "changed")
        if not changed.isEmpty():
            sample = [r[s.business_key_column] for r in changed.limit(5).collect()]
            raise ValueError(
                f"{type(self).__name__}: candidates classified as 'changed' for an "
                f"append-only entity — immutable events must not change. "
                f"Sample business keys: {sample}"
            )

        insert_rows = _stamp_audit(
            state.df.filter(F.col(_CHANGE_TYPE) == "new"),
            s, scd_effective_from, correlation_id,
        )
        final = _drop_internal(insert_rows, state.debug_columns)
        view_name = f"_fqs_native_append_only_{uuid.uuid4().hex}"
        final.createOrReplaceTempView(view_name)

        assert self._adapter is not None and self._spark is not None and target_path, \
            "services are injected at run() time (transformer-contract)"
        settings = scd_settings(s, state.hash_separator, spark=self._spark)
        seal_plan = check_scd_seal(self._adapter, self._spark, target_path, settings,
                                   target_exists=state.target_exists, logger=logger)

        if self._adapter.delta_table_exists(self._spark, target_path):
            try:
                self.hwm_before = get_high_watermark(self._spark, target_path, self._adapter, hwm_column=s.effective_from_column)
            except Exception as exc:
                _warn_watermark_unread(logger, "before", target_path, exc)

        pre_version = execute_append_only(self._spark, target_path, view_name, self._adapter, logger,
                                          target_exists=state.target_exists)
        self.inserted = capture_write_statistics(self._spark, target_path, pre_version, self._adapter, logger)
        self.updated = 0
        seal_after_write(self._adapter, self._spark, target_path, settings, seal_plan, logger)

        try:
            self.hwm_after = get_high_watermark(self._spark, target_path, self._adapter, hwm_column=s.effective_from_column)
        except Exception as exc:
            _warn_watermark_unread(logger, "after", target_path, exc)
        self.hwm_column = s.effective_from_column

        return state

    def after_run(self, state: PipelineState, ctx: LoadContext) -> None:
        super().after_run(state, ctx, extra_metrics={"inserted": self.inserted})
