"""candidate_guard.py — CandidateGuardTransformer.

Validates that the candidates DataFrame produced by SCDChangeDetectorTransformer
contains at most one row per business key (+ source system). Raises ValueError
with a sample of the duplicate keys if the invariant is violated.

Place this transformer between SCDChangeDetectorTransformer and the merger:

    .add(SCDChangeDetectorTransformer())
    .add(CandidateGuardTransformer())
    .add(NativeSCDMergeTransformer())
"""
from __future__ import annotations

from pyspark.sql import functions as F

from spark_quick_start.orchestration.transformers._constants import CHANGE_TYPE
from spark_quick_start.orchestration.transformers.base import LoadContext, ProcessTransformer
from spark_quick_start.orchestration.transformers.state import PipelineState


class CandidateGuardTransformer(ProcessTransformer):
    """Assert that candidates contain at most one row per (business_key, source_system).

    This is the pipeline's quality step, and it owns the check outright — the detector
    classifies and does not validate (F29). Two shapes reach it: on Full loads the
    key-absence delete path and the hash-change path are independent, so one key can
    appear as both 'changed' and 'deleted'; and a target that already holds two rows
    flagged current for one key fans the detector's left join out to two candidates,
    which is the state F25 describes and which no earlier check could see.

    The invariant is a property of the *pipeline*, not of the detector.
    `default_pipeline` includes this step; an append-only load built by hand composes
    `SCDCandidateVersionGuardTransformer` instead, which groups on the version as well and
    does not halt on zero candidates.

    Raises:
        ValueError: when any (business_key, source_system) combination has more
            than one candidate row. The error message includes a sample of the
            offending keys and their change types.

    Args:
        sample_n: Number of duplicate key examples to include in the error message.

    Performance note: the happy path (no duplicates) touches the uncached
    ``candidates`` DataFrame with exactly one Spark action — a single
    ``groupBy(bk, ss)`` aggregation whose result yields both the duplicate-key
    count and the total candidate count via a second, cheap aggregation over
    the already-grouped (key-cardinality-sized) result. Earlier this called
    ``candidates.count()`` separately for the total, which forced Spark to
    re-walk the *entire* upstream lineage (source scan + every transform) a
    second time — expensive at scale (tens of millions of rows). Deriving the
    total from the grouped result needs no cache, so it holds on serverless too.
    The rare error path (duplicates found) reads the grouped result a second
    time to fetch samples: from the cache off serverless, by re-running the
    aggregation on serverless, where nothing is cached (see Cost below) —
    acceptable since it's already raising an exception.

    Cost (F152), what the step charges every load: one grouped aggregation over all candidates,
    by (business key, source system), that also runs `collect_list` of each key's change types,
    which feed only the error message's samples. `collect_list` forces an ObjectHashAggregate,
    which falls back to sort-based aggregation past 128 keys a task, so on a large first load the
    guard is a real stage, not a footnote. Off serverless the grouped result is cached (one
    result of key-cardinality size, released before the step returns); on serverless nothing is
    cached and the error path re-runs the aggregation. A plain per-key count with the change
    types fetched only on the error path was measured on a 40 million-row load (16 s against
    28 s for the guard at 4 GB; one run each, local[*], 12 cores) and not adopted: its write failed in 3 of 3 runs at 2 GB, against 1
    of 3 unchanged, and no mechanism was found. The step is optional. It is composed in by
    `default_pipeline`; a pipeline built by hand that leaves it out loses the duplicate-key
    check and the halt on zero candidates.
    """

    def __init__(self, *, sample_n: int = 5) -> None:
        self._sample_n = sample_n

    def _run(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
        from spark_quick_start.logger import OrchestratorLogger
        logger = self._logger or OrchestratorLogger()

        s = ctx.scd_ctx
        bk = s.business_key_column
        ss = s.source_system_column
        change_type_col = CHANGE_TYPE

        candidates = state.df
        serverless = ctx.runtime_ctx.serverless
        per_key = candidates.groupBy(bk, ss).agg(
            F.count("*").alias("_dup_count"),
            F.collect_list(change_type_col).alias("_change_types"),
        )

        if not serverless:
            per_key = per_key.cache()

        # The cache is released on every path — result, early return and exception — so a
        # failing action cannot leave the aggregate pinned for the rest of the session.
        try:
            stats = per_key.agg(
                F.sum("_dup_count").alias("candidate_count"),
                F.sum(F.when(F.col("_dup_count") > 1, 1).otherwise(0)).alias("dup_key_count"),
            ).collect()[0]
            candidate_count = int(stats["candidate_count"] or 0)
            dup_count = int(stats["dup_key_count"] or 0)

            if dup_count == 0:
                logger.info(
                    f"CandidateGuardTransformer: {candidate_count} candidates, "
                    "all business keys unique."
                )
                if candidate_count == 0:
                    return state.with_halted(
                        "no candidates — source matches target, nothing to merge"
                    )
                return state

            samples = per_key.filter(F.col("_dup_count") > 1).limit(self._sample_n).collect()
        finally:
            if not serverless:
                per_key.unpersist()
        lines = [
            # Parenthesised so the three fragments read as one deliberate message
            # rather than looking like list entries missing their commas (ISC004).
            (
                f"CandidateGuardTransformer: {dup_count} business key(s) have more than "
                f"one candidate row. Each key must produce exactly one candidate "
                f"(new | changed | deleted) before merging."
            ),
            f"Key columns: [{bk!r}, {ss!r}]",
        ] + [
            f"  {r[bk]!r} / {r[ss]!r} → {r['_change_types']} ({r['_dup_count']} rows)"
            for r in samples
        ]
        raise ValueError("\n".join(lines))
