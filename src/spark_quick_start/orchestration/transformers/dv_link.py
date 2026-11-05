"""
dv_link.py — DVLinkTargetTransformer, DVLinkWriterTransformer.

Contains: the two steps of a Data Vault link load (the link design of 2026-10-01): reading the
    link's keys the batch carries, and inserting the relationships it lacks (F168).
Does NOT contain: the shared logic, `keys_in_target` and `insert_new_keys` in dv_hub.py, which
    the hub steps call too; the hash keys, built upstream (decision 5); a link satellite, which
    is `dv_satellite` keyed on the link hash key; or a factory (pipeline_factory.py).

A link holds one row per relationship between hubs, ever: no update, no delete. Its key,
`business_key_column`, is the link hash key; the hub hash keys it carries are named by
`link_hub_key_columns`, which `StagedKeyGuardTransformer` refuses NULL or empty. Two thin steps
over the hub's functions rather than generalised or subclassed hub steps, so each step's
`data_vault_style` stays one string (the design, option b).
"""
from __future__ import annotations

from spark_quick_start.orchestration.transformers.base import LoadContext, ProcessTransformer
from spark_quick_start.orchestration.transformers.dv_hub import insert_new_keys, keys_in_target
from spark_quick_start.orchestration.transformers.state import PipelineState

__all__ = ["DVLinkTargetTransformer", "DVLinkWriterTransformer"]

_STYLE = "dv_link"


class DVLinkTargetTransformer(ProcessTransformer):
    """Read the link's keys that the staged batch carries into `state.frames["target"]`.

    A semi-join against the staged link keys whatever the `load_type`: a link never deletes, so
    it is never read as a whole. When the link does not exist yet the key is absent and `state.target_exists` is False (F28). Lazy: no Spark action.
    """

    data_vault_style = _STYLE

    def _run(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
        ctx.scd_ctx.require_data_vault_style(type(self).__name__, _STYLE)
        frame = keys_in_target(self._adapter, self._spark, ctx, state.df)
        return state.with_target(frame)


class DVLinkWriterTransformer(ProcessTransformer):
    """Insert the staged relationships the link lacks, stamped with the load columns; never update.

    As the hub writer, through the same function: the rows whose link key the target frame does
    not hold, every staged column kept (the source step selects the link key and the hub keys),
    plus `RecordSource`, `LoadTS`, `LoadEndTS`, the correlation id and, when named, the pipeline
    run id. The hash settings are sealed at the first write and a load whose settings differ is
    refused (`ValueError`) before it writes, as for the hub. The write is an insert-only MERGE
    on the link key alone, retried on a concurrent append: its predicate re-checks the live
    table, so a retry cannot insert a relationship twice. A MERGE on the hub keys too was
    rejected by the design: with an inconsistently built link key it would insert a second row
    under one key, silently.

    Reports `inserted` from the commit, `updated` 0 (`SCDMetricsProvider`), and its run's
    `LoadTS` as `hwm_after` with no `hwm_before` (`HWMProvider`), as the hub writer does.

    The step does not check that the link's hub keys exist in their hubs (F169): a link relies on
    run order, so every hub it references is loaded before the link, or earlier in the same
    orchestration. A key with no hub row is loaded as it is, an orphan.

    Cost: as the hub writer: one existence check and one metadata read for the seal (plus one
    `isEmpty()` on an unsealed existing table), the anti-join and write, one read of the
    commit's metrics.
    """

    data_vault_style = _STYLE

    def __init__(self) -> None:
        self.inserted: int | None = None  # None until _run sets it (F117)
        self.updated: int | None = None
        self.hwm_before: str | None = None
        self.hwm_after: str | None = None
        self.hwm_column: str | None = None

    def _run(self, state: PipelineState, ctx: LoadContext) -> PipelineState:
        ctx.scd_ctx.require_data_vault_style(type(self).__name__, _STYLE)
        self.inserted, load_ts = insert_new_keys(self._adapter, self._spark, self._logger, ctx, state)
        self.updated = 0
        self.hwm_column = ctx.scd_ctx.load_ts_column
        self.hwm_before = None
        self.hwm_after = load_ts.strftime("%Y-%m-%d %H:%M:%S.%f")
        return state
