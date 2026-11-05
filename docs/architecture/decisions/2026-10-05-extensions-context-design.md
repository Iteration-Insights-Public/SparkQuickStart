# Design — extensions_context and platform_context

**Audience:** public
**Status:** accepted by the owner, 2026-10-05. The owner decided every point under *Decisions* in
conversation on that date; this record writes them down, including the staging function
parameter, settled after review on the same date.
**Date:** 2026-10-05
**Depends on:** [the Declarative Query Definition design](2026-09-29-declarative-query-definition-design.md), whose `hwm_context` section and the way a staging function receives it this design follows.

## Overview

Every key of `global_context` that the library does not reserve is today kept as a custom
parameter (`GlobalContext.custom_params`, read with `get_custom_param()` and `has_custom_param()`),
and `LoadContextManager.parse` adds the two lakehouse paths the adapter resolves to the same
dictionary. That one dictionary holds two things with different owners: data the user chose to
put in the load context, and paths the library computed. It also swallows a misspelt reserved key
without a word. This design gives each owner its own section, makes `global_context` strict, and
removes the dictionary.

## Problem

- **Two owners in one dictionary.** `custom_params` carries the user's keys and, injected over
  them, `data_storage_lakehouse_path` and `data_engineering_lakehouse_path`. A user key of that
  name is silently overwritten. A reader of a staging notebook cannot tell which entries the
  library wrote. Verified 2026-10-05: no example or reference notebook sets a `global_context` key
  of its own, so in practice only the two library-injected paths live there.
- **Silent typo swallowing.** `global_context` accepts any key. `staging_table_pat` for
  `staging_table_path` is not an error; it is a custom parameter, and the load fails later on the
  missing required key, or worse, runs on a default (`notebook_timeout_seconds` misspelt keeps the
  default of 90 seconds).
- **No sanctioned place for a transformer's own data.** A custom transformer that needs a policy,
  a rule set or a parameter has had to use `custom_params`, which was never documented as that
  place.

## Decisions

1. **A new top-level section `extensions_context`** is the place for the user's own data:
   policies, rules, and parameters for custom transformers and pipeline control. The library
   never interprets it. It must be a JSON object when present, else `ValueError`; its values are
   any JSON. It is read-only on the load context as `ctx.extensions_ctx`, an `ExtensionsContext`
   with a read-only mapping view over a deep copy, `get(key, default=None)`, `__getitem__`,
   `__contains__` and `to_dict()`. Convention, not enforced: namespace the top-level keys, such
   as `"acme.quality": {...}`, so two teams' steps do not collide. Every staging notebook is
   given the whole section as its own `extensions_context` section.
2. **A new section `platform_context`, filled by the library and never by the user:**
   `data_storage_lakehouse_path` and `data_engineering_lakehouse_path`, the two paths the caller
   passes to `LoadOrchestrator`. The `{DataStorageLakehouse}` and `{DataEngineeringLakehouse}`
   placeholder substitution stays as it is. It is read-only on the load context as
   `ctx.platform_ctx`, a `PlatformContext`. A load context JSON that contains `platform_context`
   is refused with `ValueError`: the library sets it. Staging notebooks receive it as a
   `platform_context` section, and the staging helper reads `data_storage_lakehouse_path` from it
   instead of from `global_context`.
3. **`global_context` refuses unknown keys**, with a `ValueError` naming the key, the keys it
   accepts and where user data goes: `extensions_context`. `custom_params`, `get_custom_param`
   and `has_custom_param` are removed. Only `global_context` becomes strict; every other
   section's treatment of unknown keys stays exactly as it is.
4. **`LoadContext` stays frozen.** It gains two fields, `extensions_ctx` and `platform_ctx`,
   which the orchestrator builds, as it carries `hwm_ctx` and `dqd_ctx`. Unlike those two they
   are never `None`: a load without the section has an empty `ExtensionsContext`, and a
   `LoadContext` built by hand without either gets an empty one, so a step may read
   `ctx.extensions_ctx.get(...)` without a guard.
5. **No migration branch beyond a refusal that names the way on**, because the first release is
   not out. A `global_context` key that was a custom parameter fails at parse with the message of
   decision 3.

## Non-goals

- The library does not validate, interpret, version or default anything inside
  `extensions_context`.
- The library does not enforce namespacing of its keys.
- No change to any other section, to the SCD output, or to a platform adapter.

## Deferred

The owner decided both of these are later, not never:

- **A `validate_context(ctx)` hook on transformers**, so a step could check its own part of
  `extensions_context` before any Spark work. Until it exists, a custom step checks what it reads
  when it runs.
- **An `ExecutionResult.extensions` return channel**, so a step could hand data back to the
  calling pipeline. Until it exists, a custom step reports through its own logging and the
  existing metrics.

## Proposed API

```json
{
  "global_context": {"id": "ent_1", "entity_name": "Customer"},
  "extensions_context": {"acme.quality": {"max_null_ratio": 0.02}}
}
```

A custom step reads `ctx.extensions_ctx.get("acme.quality", {})`, and any step reads
`ctx.platform_ctx.data_storage_lakehouse_path`. A staging notebook receives
`global_context`, `staging_context`, `runtime_context`, `scd_context`, `logging_context`,
`extensions_context`, `platform_context` and, when the load has one, `hwm_context`.
`global_context` there carries `source_system` and the notebook's own `staging_table_path`, as
before, and no longer the two paths or any custom parameter.

## Internal design

- `ExtensionsContext` and `PlatformContext` live in `orchestration/models.py`, pure data like the
  other context classes. `PlatformContext` is a frozen dataclass of the two paths.
  `ExtensionsContext` holds a `MappingProxyType` over a deep copy of the JSON object, so a
  mutation of the parsed JSON afterwards does not reach it and an assignment to it raises
  `TypeError`. The protection is the top level's: an object nested inside a value is a plain
  `dict`, as it was parsed, and a step must not change it. `to_dict()` returns a deep copy.
- `LoadContextManager.parse` refuses `platform_context`, checks `extensions_context` is an
  object, builds both objects, and keeps the placeholder substitution. `GlobalContext.__init__`
  refuses unknown keys, as it already refuses `parallel_execution_enabled`.
- `build_staging_phase_context` adds `extensions_context` (always, `{}` when the load has none)
  and `platform_context`, and drops the custom parameters from the phase's `global_context`.
- `create_staging_main_function` reads `data_storage_lakehouse_path` from the received
  `platform_context`. It passes `extensions_context` to a staging function by keyword when the
  function's signature declares it, the rule `hwm_context` already follows, so a four-parameter
  function is called exactly as before.

## Platform-specific behavior

None. The two paths are values the caller already passes; no new `PlatformAdapter` method.

## Error handling

| Input | Result |
|---|---|
| `extensions_context` that is not a JSON object | `ValueError` naming the section and the type found |
| `platform_context` present in the load context | `ValueError`: the library sets it; remove it |
| an unknown key in `global_context` | `ValueError` naming the key, the accepted keys, and `extensions_context` |

All three are raised by `LoadContextManager.parse`, before any staging notebook runs.

## Rejected alternatives

The owner saw each of these and chose against it.

- **The paths as reserved `global_context` fields.** Smallest change, but it keeps one section
  holding both what the user writes and what the library computes, and a user value would still
  be overwritten. A separate section says who writes it.
- **Every section strict.** The same typo protection everywhere, but the owner limited this
  change to `global_context`: the other sections' behaviour is untouched here, and making them
  strict is its own decision with its own consequences for contexts already written.
- **Staging notebooks not receiving `extensions_context`.** A notebook that applies a rule from
  the user's data would then need the data passed a second way. The whole section goes to every
  notebook.
- **The validation hook and the return channel now.** Both are real needs, but each adds a
  public surface to the transformer contract; they wait for a use that shapes them.

## Testing strategy

Unit tests for the parse, the read-only view, the three refusals, the staging phase context and
the staging helper's source of the storage path; an integration test in which a custom
`ProcessTransformer` reads `ctx.extensions_ctx` during a real run and a staging function
receives the extensions. The reference page is held against the code by
`test_the_load_context_reference_matches_the_code`.

## Documentation plan

`docs/reference/load-context.md` (the two new containers, `global_context` strictness, the
custom-parameter text removed), `docs/reference/transformers.md` (the two `LoadContext` fields),
`docs/guides/write-a-staging-notebook.md`, `docs/guides/write-a-custom-transformer.md`, and
`.claude/standards/configuration-and-context.md`. `docs/architecture/overview.md` does not list
the context sections and does not change. `README.md` does not describe the sections and does not
change.

## What cannot be proven here

- A real Fabric or Databricks pipeline writing `extensions_context` and a staging notebook
  receiving it through `notebookutils` or `dbutils`: the local suite runs the notebook function
  directly and passes the JSON string itself.
- Fabric's and Databricks's limits on a notebook's argument string. The whole `extensions_context`
  rides in the argument of every staging notebook, so a large section could exceed a limit; the
  limits are unchecked here, and the reference page tells users to keep the section small.
- That a load-context JSON already written by a pipeline outside this repository carries no
  `global_context` key that is not reserved. Verified 2026-10-05 only for the examples and
  reference notebooks in this repository.

## Open questions

None. The one left after the first draft — whether the function wrapped by
`create_staging_main_function` may declare an `extensions_context` parameter, as it may
`hwm_context` — the owner settled on 2026-10-05: it stays. Without it a notebook's function could
read the section only by parsing its own argument string.
