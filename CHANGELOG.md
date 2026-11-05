# Changelog

Every change a user of the library would notice, newest first. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html); before 1.0, a minor version may break
the API, and such changes are marked **Breaking**. Each version is released per platform, as
`v<version>-fabric` and `v<version>-databricks`, from version 0.4.0 on.

## [Unreleased] — 0.4.0

### Added

- Data Vault 2.0 loads: hubs, links and satellites, insert-only, through `dv_hub_pipeline`,
  `dv_link_pipeline` and `dv_satellite_pipeline`, chosen by `scd_type` in `scd_context`.
- Declarative Query Definitions: `DQDTransformer` loads one source (Delta, parquet, a catalog
  view or a Fabric Warehouse view) with per-row changes and no staging notebook.
- `hwm_context`, a source watermark shared by staging and DQD loads.
- `StagedKeyGuardTransformer`, which refuses duplicate, NULL and empty staged keys.
- `extensions_context` for your own data, and `platform_context` for the paths the library
  resolves.
- `runtime_context.materialize_strategy`: a Delta round trip or an in-memory cache, and every
  frame a step cached is released when the run ends.
- SCD tables seal their settings (flag values, open-row sentinel, hash separator, session time
  zone) at the first write, and a later load whose settings differ is refused.
- The run's `load_timestamp` is stamped as the effective-from and load time.
- Opt-in stage metrics, query plans and row counts; `source_count` and `target_count` read from
  the staging write's Delta commit metadata, or from the watermark aggregation on a DQD load.
- Reference designs: a Databricks Asset Bundle and a Fabric workspace demo.

### Changed

- **Breaking:** Python 3.11 or later is required.
- **Breaking:** the SCD-specific steps carry an `SCD` prefix, so the `stage` values in
  `ExecutionResult` carry the new class names.
- **Breaking:** `global_context` refuses unknown keys, and requires `global_context.id`;
  `custom_params` is replaced by `extensions_context`.
- `load_timestamp` must be a readable instant with a UTC offset, and for SCD styles
  `default_max_timestamp` must be later than it; anything else is refused.
- **Breaking:** a staging stage without a `staging_table_schema` is refused before any notebook
  runs.
- **Breaking:** under SCD1, a deleted candidate now soft-deletes the matched row (deleted flag
  set, effective-from and correlation id restamped); before, it was not written.

### Deprecated

- `TargetTableTransformer`, `IncrementalTargetTableTransformer`, `ChangeDetectorTransformer`,
  `GracefulChangeDetectorTransformer` and `CandidateVersionGuardTransformer` still import, with a
  warning, until 0.5.0; use the `SCD`-prefixed names.

### Removed

- **Breaking:** the legacy DQF engine.
- **Breaking:** the `append` load style; merge and append-only loads remain.
- **Breaking:** `runtime_context.num_partitions` and `scd_append_only`, which nothing read, are
  refused.

## [0.3.6] — 2026-06-25

- `ConditionalSourceTransformer`: choose the source step at run time.

## [0.3.5] — 2026-06-24

- A path for staging notebooks.

## [0.3.4] — 2026-06-22

- No release notes were recorded for this version.

## [0.3.2] — 2026-06-22

- No release notes were recorded for this version.

## [0.3.0] — 2026-06-21

- No release notes were recorded for this version.

## [0.2.1] — 2026-06-11

- No release notes were recorded for this version.

## [0.2.0] — 2026-06-05

- No release notes were recorded for this version.

## [0.1.1] — 2026-06-03

- No release notes were recorded for this version.

## [0.1.0] — 2026-06-03

- First release.
