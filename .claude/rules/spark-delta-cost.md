---
name: spark-delta-cost
description: Spark/Delta execution rules earned from measured production pain — caching, counts, actions, instrumentation, destructive ops.
---

# Spark, Delta, and cost discipline

Enforced by: none — judgement.

No test, by choice: a pattern cannot tell a violation from a sanctioned use (a `count()` that
puts a number in an abort message), and the violations that matter are not lexical (a `.cache()`
three frames from its serverless gate). `/review-pr` is the enforcement. These rules come from
measured production pain; do not undo them casually.

1. **`.cache()` / `.persist()` are unavailable or ineffective on Fabric serverless and
   Databricks Serverless.** Every cache call is gated on `ctx.runtime_ctx.serverless`.
   Prefer a design needing no cache; when a frame is genuinely read twice, use the
   scratch-table round trip (`MaterializeTransformer` / `MaterializeFrameTransformer`).
2. **Never add a `df.count()` for metrics.** Read counts from the write's own Delta commit
   metadata (`operationMetrics['numOutputRows']` via `capture_write_statistics` /
   `capture_merge_statistics`). A count is a second full pass over the lineage.
3. **Any Spark action is a cost decision.** Say so in the docstring; make it optional when it
   is a guard rather than the work itself (composed in or left out, as `CandidateGuardTransformer`,
   or a flag, as `runtime_context.capture_partitions`); prefer one aggregation answering several
   questions over several actions.
4. **Instrumentation must be attributable.** `_TransformerBase.after_run` reports
   `measure_ms` separately from `duration_ms` because `_safe_num_partitions()` can
   materialize an AQE shuffle (117s observed). Volume/plan capture is opt-in via
   `runtime_context.capture_partitions` / `capture_row_counts` / `capture_plans`.
5. **Destructive operations are guarded, never best-effort.** `drop_delta()` refuses a
   path that exists but is not a Delta table; `delete_staging_data(spark=...)` does the
   same. Identifiers interpolated into DDL go through `_quote_uc_name()` (strict 1–3 part
   name, backtick-quoted). Never build SQL by f-string without that.
6. Timestamps written to Delta are timezone-aware UTC `datetime` objects passed to
   `F.lit()` — never naive, never session-timezone dependent.
7. Concurrency conflicts retry through `run_with_delta_retry`, which re-raises anything
   that is not a Delta append conflict.
