# Changelog

Every change a user of the library would notice, newest first. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html); before 1.0, a minor version may break
the API, and such changes are marked **Breaking**. What breaks a load context is defined under
Compatibility in [the load-context reference](docs/reference/load-context.md#compatibility). From
0.4.1 on, each version is one release for all platforms, tagged `v<version>`.

## [Unreleased]

## [0.6.0] — 2026-11-18

### Changed

- Installation points at the public repository, `Iteration-Insights-Public/SparkQuickStart`,
  which publishes source only: install with `pip` from `git+https` pinned to a commit, or copy
  `src/` from a clone. The README and the guides no longer link a release download.
- Feedback goes through GitHub Issues, with a bug and an enhancement form, and questions through
  GitHub Discussions; the repository does not accept pull requests. `CONTRIBUTING.md`,
  `SUPPORT.md` and `CODE_OF_CONDUCT.md` are published and say how, and commercial help is at
  https://iterationinsights.com/contact-us/. Conduct reports go to
  `opensource@iterationinsights.com`.
- The top level of `source_context` is the library's, `kind` today, and a source step's own keys
  go under `properties`, as the custom-source guide now shows. A key beside `kind` is still
  accepted and handed to the step, but a later release may add a key of its own under that name.
- **Breaking:** `ExecutionResult.metrics["hwm_value_after"]` moves past a batch only when a writer
  wrote it. When the pipeline halted, or a registered writer did not run, it now equals
  `hwm_value_before`; before, it moved to the latest of the rows the source read, and a caller
  that stored it, as the incremental-load guide says, skipped those rows for good. After a halt
  because the batch matches the target, the next run reads the same rows again, and the rows read
  grow with each such run until a writer runs: a source that updates its watermark column without
  changing the compared columns is re-read from the last write on every run. A custom source that
  owns the watermark column must set `hwm_before` to the `hwm_context.hwm_value` it was given;
  one that leaves it `None` reports `None` after a halt, and a caller that stores it reads the
  whole source again on the next run.
- `SECURITY.md` asks for the commit you run, or the version if you installed a release, and the
  package metadata no longer carries a maintainer's email address.

### Removed

- **Breaking:** the old names of the five SCD steps, `TargetTableTransformer`,
  `IncrementalTargetTableTransformer`, `ChangeDetectorTransformer`,
  `GracefulChangeDetectorTransformer` and `CandidateVersionGuardTransformer`, deprecated in 0.4.0
  and due to go in 0.5.0, which shipped them still: importing one now raises `ImportError`. Use
  the `SCD`-prefixed names.

### Fixed

- A second `run()` on one `LoadOrchestrator` no longer reports the previous run's counts and
  watermark for a step this run skipped: each step's reported counts and watermark are reset
  before the steps run. A custom step that sets its counts or watermark only in `__init__`, not in
  `_run()` or `_produce()`, now reports `None` for them on every run.

## [0.5.0] — 2026-11-13

### Added

- The public API is defined and pinned: every name in the `__all__` of `spark_quick_start`,
  `orchestration`, `orchestration.transformers`, `adapters`, `logger` and `spark_logging`, with its
  signature and the subclass hooks and injected services a subclass relies on. A change that
  removes or changes any of it is marked **Breaking** here.
- `PlatformAdapter.get_secret(scope, key)` reads a secret through the platform's secret store:
  a Key Vault on Fabric, a secret scope on Databricks. `LocalAdapter` and `MockPlatformAdapter`
  take an explicit `secrets` mapping.
- An optional `source_context` section configures a source step of your own: one required key,
  `kind`, and any keys of the step's. A load context now names one source, staging notebooks, a
  `dqd_context` or a `source_context`; `SourceTransformer.source_kind` declares the kind a step
  reads, and the orchestrator refuses a mismatch before any source runs.
- `scd_context.scd_type` `landing` and `KeyMergeTransformer`: a landing table holding the latest
  state of each key, merged on `business_key_column`, ordered by `hwm_context.hwm_column_name`
  and stamped with the load time in `load_ts_column`. A new source column is added to the table;
  a retyped one is refused. `PlatformAdapter.add_delta_columns` adds nullable columns.

### Changed

- `staging_context.stages` is not required when `source_context` is given, and `StagingTransformer`
  refuses an empty list itself when it runs.

## [0.4.1] — 2026-11-13

### Changed

- Releases are no longer built per platform: one release per version, tagged `v<version>`,
  carries one wheel for Fabric, Databricks and local use, and replaces the separate
  `-fabric` and `-databricks` releases. The wheel now includes `LocalAdapter` and
  `local_notebookutils/`; the `local` extra installs what they need.

## [0.4.0] — 2026-11-05

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
