---
name: configuration-and-context
description: One JSON load context, each section validating itself. Where new config goes and how SCD column names resolve.
---

# Configuration and context

- All runtime input arrives as **one JSON string** with five required sections
  (`global_context`, `staging_context`, `scd_context`, `runtime_context`,
  `logging_context`), the optional `hwm_context`, `dqd_context` and `extensions_context`, and
  `platform_context`, which the library fills and a load context may not carry. Parsing and
  placeholder substitution (`{DataStorageLakehouse}`, `{DataEngineeringLakehouse}`) live in
  `LoadContextManager`.
- Each context class owns its own `validate()` and raises `ValueError` naming every
  missing key at once. Validation happens once, up front, before any Spark work.
- New configuration goes in the context class that owns the concern — e.g. execution
  concurrency lives in `RuntimeContext`. Moving a field is a breaking change: leave a
  rejecting branch with a migration message (see `GlobalContext.parallel_execution_enabled`).
- SCD meta column names resolve **per-column JSON override > `ColumnNamingPolicy` >
  `ColumnNamingPolicy.default()`**. `ColumnNamingPolicy` is the single source of truth;
  never hardcode `"scdHash"` and friends in new code. Every policy field is an SCD meta column
  (an SCD1 MERGE never updates a column with that name), so a name that only non-SCD styles
  write goes in `Constants` instead: the Data Vault load-audit names and hash separator are
  `Constants.DV_*`.
- **The user's own data goes in `extensions_context`**, read as `ctx.extensions_ctx`; the library
  never interprets it, and a step namespaces its keys (`"acme.quality"`). Do not add a bespoke
  passthrough mechanism. `global_context` is strict: an unknown key raises `ValueError`.
  What the library computes about where it runs goes in `PlatformContext` (`ctx.platform_ctx`),
  never in a section the user writes.
- Prefer explicit config objects over globals. The one module global in the tree
  (`spark_logging._CURRENT`) is documented legacy — do not add more.
