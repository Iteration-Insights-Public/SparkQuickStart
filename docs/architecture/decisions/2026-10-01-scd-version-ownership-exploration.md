# Exploration — who owns the SCD version, and whether a table needs one

**Audience:** public
**Status:** explored, not adopted, 2026-10-01
**Date:** 2026-10-01
**Tracked as:** F128

## Question

The owner ruled on 2026-09-30 that `ChangeDetectorTransformer` must be agnostic to `scd_type`,
which is meant to drive only how rows are persisted. Where should the version then be derived,
and does a target need a version column at all? A backlog entry for it (F155) was opened and
withdrawn the same day at the owner's word; this record keeps the discussion.

## Options considered

- **A separate step** (working name `SCDVersionTransformer`) between the detector and the
  guards writes the version from `_prev_scdVersion`, the change type and `scd_type`. The
  detector stops reading `scd_type`; guards and writers are unchanged; custom writers that read
  the version from the candidates keep working. A hand-built pipeline without the step has no
  version, so the writers must refuse that clearly.
- **The writers derive it**, from one shared function. Purest fit for "scd_type is about
  persistence". `CandidateVersionGuardTransformer` runs before any writer, so it calls the same
  function or groups on `_prev_scdVersion` instead, which is not equivalent: a key both changed
  and deleted has two versions today but one previous version. Custom writers lose the column.
- **Both**: the shared function, and the step as a thin wrapper over it.
- **No version at all.** Make the row identity `(business key, source system, effective_from)`,
  as `(HashKey, LoadDate)` in Data Vault, and the version an optional decoration. The owner's
  warehouse does not use the version.

## What was learned

- `scd_type` is read by `SCDContext.validate`, by the writers (`native_scd.py`: the SCD2
  close-out and the deleted-version rows) and by the MERGE (`load_pipeline.py`:
  `_build_merge_condition`, the SCD1 `update_set`), all about persistence; and by
  `ChangeDetectorTransformer`, which derives the version (SCD2 `prev + 1` for a changed row,
  SCD1 `1` for new and changed) and re-checks the permitted values.
- The SCD1 version of `1` is what makes a changed row match an existing one in the append
  MERGE and vanish silently (F128).
- The version is load-bearing under SCD2: the MERGE matches on it to tell the close-out row
  (same version as the target's current row: update) from the new version (`prev + 1`:
  insert); append under SCD2 relies on it to insert rather than match; the close-out finds the
  target row by `_prev_scdVersion`; `CandidateVersionGuardTransformer` tells an append fan-out
  from a dirty source by it.
- Without a version, `effective_from` can carry the identity: a close-out row carries the
  target row's `effective_from`, a new version the run's own, so
  `ON bk, ss, effective_from, current = 'Y'` separates them as the version does. The target
  frame holds current rows only, one per key, so the close-out needs only business key and
  source system. Not verified: exact timestamp equality across a Delta round trip on Fabric and
  Databricks; two runs sharing an `effective_from` for one key are assumed impossible. The
  append fan-out guard would need another discriminator.
- Unit tests assert the detector's `scdVersion` in many places; they would move to the step or
  the writers.

## Why not now

The owner was not ready to decide where the version lives or whether it stays mandatory, and
asked to withdraw the backlog entry.

## What would reopen it

F128's decision, a target without a version column, or a second methodology needing its own
row identity (see [the key-identity exploration](2026-10-01-key-identity-exploration.md)).
Start from the readers of the version listed above.
