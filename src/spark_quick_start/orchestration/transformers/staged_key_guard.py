"""
staged_key_guard.py — StagedKeyGuardTransformer.

Contains: StagedKeyGuardTransformer, the check on the staged rows' keys that runs right after
    the source step: no key staged twice, no NULL key part, no empty string key.
Does NOT contain: the candidate check after change detection (candidate_guard.py), or any repair:
    bad keys are refused, never deduplicated or defaulted.

Place it straight after the source:

    .add(DQDTransformer())
    .add(StagedKeyGuardTransformer())
    .add(SCDTargetTableTransformer())

F134: `CandidateGuardTransformer` counts candidates, and the detector drops a staged copy equal
to the target first, so a duplicate with one unchanged copy loaded the other silently. F154: a
NULL business key matches nothing on either side, so it was inserted again, as current, on every
run. Both are only visible on the staged rows.
"""
from __future__ import annotations

from pyspark.sql import Column, Row
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from spark_quick_start.orchestration.transformers.base import LoadContext, ProcessTransformer
from spark_quick_start.orchestration.transformers.state import PipelineState

__all__ = ["StagedKeyGuardTransformer"]


class StagedKeyGuardTransformer(ProcessTransformer):
    """Refuse staged rows whose key is duplicated, NULL or empty, before change detection.

    The key is `scd_context.business_key_column`, with the source system column beside it when
    `source_system_from_source` is true (the rows must then carry it), except under a Data Vault
    style, which integrates by business key alone. Under `dv_link` the hub key columns
    (`link_hub_key_columns`) are checked for NULL and empty too, never for duplicates: a hub key
    repeats across the links to its hub by design (F167). Otherwise a source-system
    column the rows carry is ignored, because the detector overwrites it with the run's one
    `global_context.source_system`, and the business key alone decides.

    Cost: one Spark action over the staged rows, a `groupBy` on the key answering all three
    questions at once. It is the source's whole lineage again, which is why the step is opt-in
    for SCD pipelines rather than composed by `default_pipeline`. The error path pays one more
    pass per kind of problem it samples, at most three (duplicates, empty keys, and under
    `dv_link` NULL or empty hub keys). An empty source passes untouched and is never halted here, so the
    detector's refusal of an empty `Full` source still fires.

    Args:
        sample_n: How many offending keys of each kind the error message shows.

    Raises:
        ValueError: when a key column is missing, or when any key is staged more than once,
            has a NULL part, or is an empty string after trimming; one message lists every
            problem found, with counts and samples.
    """

    def __init__(self, *, sample_n: int = 5) -> None:
        self._sample_n = sample_n

    def _run(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
        from spark_quick_start.logger import OrchestratorLogger
        logger = self._logger or OrchestratorLogger()

        s = ctx.scd_ctx
        staged = state.df
        key = [s.business_key_column]
        # Data Vault integrates by business key alone: one key from two sources is one key, which
        # an insert-only writer would take twice at one LoadTS (F162's review).
        if s.source_system_from_source and not s.is_data_vault:
            key.append(s.source_system_column)
        # Columns that must be present but may repeat: a link's hub keys (F167).
        required = list(s.link_hub_key_columns) if s.scd_type == "dv_link" else []
        missing = [c for c in [*key, *required] if c not in staged.columns]
        if missing:
            raise ValueError(
                f"{type(self).__name__}: the staged rows have no key column(s) {missing}; "
                f"their columns are {staged.columns}."
            )

        is_null = _any_of([F.col(c).isNull() for c in key])
        # Only a string key can be empty; NULL compares as NULL here and is counted as NULL.
        is_empty = F.coalesce(_any_of([F.trim(F.col(c)) == F.lit("") for c in key
                                       if isinstance(staged.schema[c].dataType, StringType)]),
                              F.lit(False))

        # The required columns are row-level, so each group counts its own bad rows; the one
        # aggregation still answers every question.
        bad_required = _any_of([F.col(c).isNull() for c in required]
                               + [F.trim(F.col(c)) == F.lit("") for c in required
                                  if isinstance(staged.schema[c].dataType, StringType)])
        per_key = staged.groupBy(*key).agg(
            F.count(F.lit(1)).alias("_rows"),
            F.sum(F.when(F.coalesce(bad_required, F.lit(True)), 1).otherwise(0)).alias("_bad_required"))
        stats = per_key.agg(
            F.sum(F.when(F.col("_rows") > 1, 1).otherwise(0)).alias("duplicate_keys"),
            F.sum(F.when(is_null, F.col("_rows")).otherwise(0)).alias("null_rows"),
            F.sum(F.when(is_empty, F.col("_rows")).otherwise(0)).alias("empty_rows"),
            F.sum("_bad_required").alias("bad_required_rows"),
        ).collect()[0]
        duplicate_keys = int(stats["duplicate_keys"] or 0)
        null_rows = int(stats["null_rows"] or 0)
        empty_rows = int(stats["empty_rows"] or 0)
        bad_required_rows = int(stats["bad_required_rows"] or 0)

        if not (duplicate_keys or null_rows or empty_rows or bad_required_rows):
            logger.info(f"{type(self).__name__}: every staged key is present, non-empty and unique.")
            return state

        names = ", ".join(repr(c) for c in key)
        hub_keys = f" or their hub keys {required}" if bad_required_rows else ""
        lines = [(f"{type(self).__name__}: the staged rows' key [{names}]{hub_keys} is unusable; "
                  "fix the source or the staging step, the library will not choose for you.")]
        if duplicate_keys:
            samples = per_key.filter(F.col("_rows") > 1).limit(self._sample_n).collect()
            lines.append(f"{duplicate_keys} key(s) staged more than once, for example:")
            lines += [f"  {_render(r, key)} ({r['_rows']} rows)" for r in samples]
        if null_rows:
            lines.append(f"{null_rows} row(s) with a NULL key part.")
        if empty_rows:
            samples = per_key.filter(is_empty).limit(self._sample_n).collect()
            lines.append(f"{empty_rows} row(s) with an empty key, for example:")
            lines += [f"  {_render(r, key)} ({r['_rows']} rows)" for r in samples]
        if bad_required_rows:
            samples = per_key.filter(F.col("_bad_required") > 0).limit(self._sample_n).collect()
            lines.append(f"{bad_required_rows} row(s) with a NULL or empty hub key in "
                         f"{required}, for example at the link key(s):")
            lines += [f"  {_render(r, key)}" for r in samples]
        raise ValueError("\n".join(lines))


def _any_of(conditions: list[Column]) -> Column:
    result = F.lit(False)
    for condition in conditions:
        result = result | condition
    return result


def _render(row: Row, key: list[str]) -> str:
    return " / ".join(repr(row[c]) for c in key)
