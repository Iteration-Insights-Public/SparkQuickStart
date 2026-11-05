---
name: spark-performance
description: General PySpark hygiene — actions, UDFs, partitioning, joins, shuffle width, MERGE predicates, schema handling.
---

# Spark performance hygiene

This is the **standard** half of the Spark pair. `.claude/rules/spark-delta-cost.md` is the
rule half: seven items earned from measured production pain, where a violation is a bug. The
items here are conventions — a violation is a review comment, and there are legitimate
exceptions. Keeping them apart is the point. Folding nine hygiene bullets into the rule would
make its own sentence — *"these exist because of measured production pain"* — false of most of
its contents.

Both halves are enforced by `/review-pr` judgement, not by a test. A grep cannot tell a
`.count()` on an abort path from a `.count()` for metrics.

## Where the rule wins

Three of these overlap the rule, and the rule's version is the binding one:

- **Caching.** The rule requires every `.cache()` / `.persist()` to be gated on
  `ctx.runtime_ctx.serverless` — caching is unavailable or a no-op on Fabric serverless and
  Databricks Serverless. "Cache when justified and unpersist afterwards" is not sufficient
  advice here and following it alone produces a cache that silently does nothing. See
  `spark-delta-cost` rule 1, and prefer the scratch-table round trip
  (`MaterializeTransformer` / `MaterializeFrameTransformer`). Where the non-serverless branch
  does cache, release the frame in a `try/finally`, so every path (result, early return, exception)
  unpersists — `CandidateGuardTransformer._run` and `assert_no_dups` are the shape. That half of the advice is not in the rule and would
  otherwise have been lost when these items moved out of an agent file.
- **Counts for metrics.** The rule forbids adding a `df.count()` to produce a metric; read
  the number out of the write's own Delta commit metadata instead (`spark-delta-cost` rule 2).
- **Any new action is a cost decision**, stated in the docstring and made toggleable when it
  is a guard rather than the work itself (`spark-delta-cost` rule 3).

## The hygiene items

1. **Avoid unnecessary `collect()`, `toPandas()`, and driver-side loops.** Each pulls the
   distributed result onto one machine. Where a driver-side value is genuinely needed, take
   one aggregation that answers every question at once rather than one action per question.
2. **Do not re-walk the same lineage.** A DataFrame read twice is computed twice unless
   something materialises it, and on serverless the something cannot be a cache. Note this is
   the shape behind open finding F9 — `native_scd` walks the candidate lineage three times.
3. **Prefer column expressions and DataFrame transformations over Python UDFs.** A Python UDF
   serialises every row across the JVM/Python boundary and is opaque to the optimiser. Reach
   for `pyspark.sql.functions` first; where a UDF is unavoidable, say in the docstring why.
4. **Do not `repartition()` / `coalesce()` speculatively.** Both are explicit shuffles or
   explicit parallelism cuts. AQE already coalesces post-shuffle partitions; a hand-tuned
   number that helped one dataset usually hurts the next one.
5. **Make joins explicit and check the keys for duplicate amplification.** State the join
   type rather than taking the default, and know whether the right side is unique on the join
   key. A silently non-unique right side multiplies rows, and the symptom appears downstream
   as a count that is wrong rather than as an error — the same failure shape the
   `CandidateGuardTransformer` one-candidate-per-key check exists to catch.
6. **Prefer the narrower transformation.** A wide transformation shuffles; a narrow one does
   not. Where both express the same intent, the narrow one is the default.
7. **Keep Delta `MERGE` predicates selective and deterministic.** A predicate that cannot
   prune files makes the merge read the whole table, and a non-deterministic one makes the
   result depend on which files were scanned.
8. **Make schema handling explicit.** State the schema or the projection rather than relying
   on inference and column order; `compare_schemas_primitive_ignore_order` exists because
   drift is expected here, not exceptional.

## Instrumentation

Measuring costs something too. `_TransformerBase.after_run` reports `measure_ms` separately
from `duration_ms` because `_safe_num_partitions()` can materialise an AQE shuffle — 117s
observed. Volume and plan capture are opt-in via `runtime_context.capture_partitions` /
`capture_row_counts` / `capture_plans`. If you add instrumentation, its cost belongs in
`measure_ms`, not in the span it is measuring.
