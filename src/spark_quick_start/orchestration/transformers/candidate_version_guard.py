"""candidate_version_guard.py — SCDCandidateVersionGuardTransformer.

The quality step of an append-only load built by hand. It asserts one candidate per
(business_key, source_system, version) and raises on a *dirty source*: two rows sharing
one business key, which on a first load both land at version 1 and so collide.

Raise-only, deliberately. `CandidateGuardTransformer` also halts on zero candidates,
which changes `ExecutionResult.early_exit` and drops the writer's stage; an append-only
load with nothing new is meant to run to the end and insert 0 rows, so this step never
halts.

That grouping is the check `SCDChangeDetectorTransformer` used to run behind a
`validate_no_dups` flag. F29 removed it from there, because whether a duplicate is
acceptable depends on which pipeline is running and the detector cannot know that. This
is the same assertion, composed into the pipeline that needs it.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from spark_quick_start.orchestration.transformers._aliases import SCD_RENAMES, deprecated_aliases
from spark_quick_start.orchestration.transformers._validation import assert_no_dups
from spark_quick_start.orchestration.transformers.base import ProcessTransformer

if TYPE_CHECKING:
    from spark_quick_start.orchestration.transformers.base import LoadContext
    from spark_quick_start.orchestration.transformers.state import PipelineState

__all__ = ["SCDCandidateVersionGuardTransformer"]


class SCDCandidateVersionGuardTransformer(ProcessTransformer):
    """Assert one candidate per (business_key, source_system, version).

    Rejects a dirty source, whose duplicates share a version, and does not halt on zero
    candidates, unlike `CandidateGuardTransformer`, which also groups on the key pair alone
    and halts when there is nothing to merge.

    The append-only load built by hand composes this (see
    docs/guides/choose-a-load-style.md); `default_pipeline` composes
    `CandidateGuardTransformer`.

    Args:
        sample_n: How many offending keys to include in the error message.

    Raises:
        ValueError: when any (business_key, source_system, version) has more than one
            candidate row. The message names the key columns and samples the offenders.

    Cost: one Spark action — the `groupBy().count()` inside `assert_no_dups`, which
    caches only when `ctx.runtime_ctx.serverless` is False. It replaces the action the
    change detector used to pay for the same assertion, so a pipeline composing this is
    no more expensive than before F29.
    """

    # F156: refused under a Data Vault style, at composition (LoadOrchestrator) and in _run.
    scd_style_only = True

    def __init__(self, *, sample_n: int = 5) -> None:
        self._sample_n = sample_n

    def _run(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
        ctx.scd_ctx.require_scd_style(type(self).__name__)
        s = ctx.scd_ctx
        # version_column is not a source column — unlike the business key and the source
        # system it is not validated by SourceTransformer._validate_source_columns, and it
        # exists only because SCDChangeDetectorTransformer produced it. Composed before the
        # detector, or after a step that projects it away, the groupBy below would raise a
        # bare "cannot resolve 'scdVersion'". This transformer is newly public, so a
        # composition mistake is the likely mistake.
        if s.version_column not in state.df.columns:
            raise ValueError(
                f"{type(self).__name__} requires the {s.version_column!r} column, which "
                "SCDChangeDetectorTransformer produces — this step must be composed after it. "
                f"Columns present: {sorted(state.df.columns)}. For a pipeline with no "
                "version column, CandidateGuardTransformer groups on the business key and "
                "source system alone."
            )
        assert_no_dups(
            state.df,
            ctx.global_ctx.entity_name,
            key_columns=[s.business_key_column, s.source_system_column, s.version_column],
            sample_n=self._sample_n,
            serverless=ctx.runtime_ctx.serverless,
        )
        return state


# F155: the pre-rename names still resolve, with a DeprecationWarning (see _aliases.py).
__getattr__ = deprecated_aliases(__name__, globals(), SCD_RENAMES)
