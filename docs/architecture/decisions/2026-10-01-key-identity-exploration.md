# Exploration — key identity: the business key, the row key and the consumer's join

**Audience:** public
**Status:** explored, not adopted, 2026-10-01
**Date:** 2026-10-01
**Tracked as:** F147, F148, F149, F150

## Question

Which column identifies an entity, to the load and to whoever reads the target? The question
came from F147: two reference notebooks failed validation because their business key and the
default row key share the name `scdRowKey`. The discussion that followed (2026-09-30) asked
what the two keys mean, which one a consumer joins on, and whether the model stays open to
other target methodologies, Data Vault among them.

## Options considered

- **Two names, one identity.** `business_key_column` names the key in the staged frame,
  `row_key_column` its name in the target. Case 1: both the same, and the staged key passes
  through. Case 2: different names, and the key is derived from the source column. *Adopted in
  part:* F148 made case 1 valid and made it the reference pipelines' convention.
- **Case 2 as its own step.** A process step after the source writes the key to
  `business_key_column` from given source columns, optionally hashed, so every later step sees
  case 1. Preferred by the owner to giving the existing transformers the responsibility. Not
  built: no source needs it yet (F149).
- **The consumer's identity is business key plus source system.** Documented by F148 on the
  load-context reference; no data changes. The row key alone is not unique across source
  systems that share a business key.
- **The row key as the full entity key, source included.** One column a consumer can join on,
  the same shape as a Data Vault `HashKey`. Would change existing row-key values, so only as a
  mode switched on, never as a new default.
- **The library builds the key from the business key columns and the source system**, so the
  two cannot disagree. The larger change; not pursued.
- **An optional row key** (`row_key_column: null`): write no row key when the business key
  already is the entity key. Raised twice, for collisions and for Data Vault; not opened.
- **A Data Vault satellite profile.** A satellite has a `HashKey`, no exposed business key, a
  `HashDiff`, a `LoadDate`, and is insert-only. Four questions separate the methodologies: what
  a row is matched on, how a change is seen, how the current row is found, and what is written.

## What was learned

- The business key is what the load matches on, on both sides: the detector's join
  (`change_detector.py`, `join_cond`), the incremental target filter (`target.py`), the
  close-out (`native_scd.py`) and the MERGE (`load_pipeline.py`, `_build_merge_condition`). The
  row key is written by `ChangeDetectorTransformer` and read by nothing in the library.
- In the reference pipelines the staged key already carries the source, built upstream as
  `AW2022~A~<id>` (staging) or `AW2022~<id>` (DQD), while `global_context.source_system` is `CS`
  for both staging phases: the source is described twice, and the two descriptions disagree.
  `scd_context.source_system_from_source` exists to describe it once.
- The owner confirmed that several source systems can share a business key. A row key derived
  from the business key alone is then not unique across them.
- A satellite fits the matching model when `business_key_column` names the `HashKey` built
  upstream. What does not fit today: the row key would be a redundant column, `scdHash` is
  xxhash64 where Data Vault usually wants MD5 or SHA, the current row is found by a current flag
  rather than the latest `LoadDate`, and the writers add versions, flags and end dates a
  satellite does not have.

## Why not now

The owner settled case 1 (F148) and the convention, and asked to keep the rest as ideas: no
target in this repo needs a second methodology or a source-carrying row key yet.

## What would reopen it

A Data Vault target, a consumer joining on the row key across source systems, or a table where
the source system in the key and in `scdSourceSystem` must agree. Start from F149 for case 2 and
from the four questions above for a second methodology.
