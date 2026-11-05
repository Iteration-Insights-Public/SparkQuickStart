---
name: data-contract
description: How transformers pass data to each other — routing columns, named frames, debug columns, the hash separator and cached frames. Each name has one home; a violation is a silent empty filter, not an exception.
---

# Inter-transformer data contract

Enforced by: tests/architecture/test_data_contract.py

Transformers pass data to each other through five channels, each a contract:

1. **Routing columns on `state.df`** — `_change_type` and `_prev_scdVersion`. Written by
   `SCDChangeDetectorTransformer` and, `_change_type` only, by
   `DVSatelliteChangeDetectorTransformer`; read by `CandidateGuardTransformer` and the
   `NativeSCD*` writers; stripped by `_drop_internal()` (inline in the satellite writer) before
   any write.
2. **Named frames on `state.frames`** — only `"target"`, written by the SCD and Data Vault
   target steps and read by the change
   detectors, `NativeSCDMergeTransformer`, `MaterializeFrameTransformer`, and the hub and link
   writers.
3. **Debug columns on `state.debug_columns`** — declared by the detector (`with_debug_columns`,
   when `debug_hash_string_column` is set); dropped by the `NativeSCD*` writers in
   `_drop_internal()`, and by yours. User-chosen names: no single-definition rule.
4. **`PipelineState.hash_separator`** — the hash's separator: written by the detector
   (`with_hash_separator`), read by the `NativeSCD*` writers to seal, and yours; `None`
   falls back to `scd_context.hash_separator`. A field: no single-definition rule.
5. **`PipelineState.cached`** — frames a Materialize step persisted (`with_cached`); a
   step that fails after caching releases its own. A field: no single-definition rule.

The rules:

- **Each of these names is defined once**, in `transformers/_constants.py`, and imported
  everywhere else: a repeated literal desynchronises silently.
- **A name is configurable on all sides or on none.** The frames key on none (F2):
  `Constants.TARGET_FRAME_KEY`, taken by no constructor; `MaterializeFrameTransformer(frame_key=...)`
  defaults to it.
- **The target step states whether the target exists.** `PipelineState.target_exists` is `None`
  until a target step runs, then `True` (frame present) or `False` (key absent); `frames` never holds `None` (F28). A `changed` or
  `deleted` classification implies a present, non-empty frame; `NativeSCDMergeTransformer` raises when it is absent (F24). An empty
  table is a first load (`tgt is None or tgt.isEmpty()`).
- **`ExecutionResult.error_details` is built only by `ErrorDetails`**;
  nothing else writes `error_type` (F114).
- **Routing and debug columns never reach Silver or a Data Vault table.** Keep the stripping
  that holds the first rule's failure latent.
