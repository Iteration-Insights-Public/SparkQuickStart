---
name: transformer-contract
description: The contract every pipeline transformer obeys — templates, immutability, service injection, and capability protocols.
---

# Transformer contract

Enforced by: tests/architecture/test_transformer_contract.py

- A pipeline is `SourceTransformer` at position 0, then `ProcessTransformer`s, as
  `LoadOrchestrator._run_pipeline` enforces; keep that validation.
- Subclasses implement `_produce(ctx)` / `_run(state, ctx)`. **Never override
  `produce()` / `__call__()`** — those are the templates that own
  `before_run → work → after_run` and the metrics.
- Override `before_run` / `after_run` only to *extend*; always call `super()`.
- `ProcessTransformer.__call__` short-circuits when `state.halted`. A step that must run
  on a halted pipeline (reporting only, no mutation) subclasses
  `AlwaysRunTransformer` — the only sanctioned bypass (wrappers delegate:
  `_WRAPPERS`).
- `PipelineState` is immutable by convention: return a new state via
  its `with_*` methods.
- `LoadContext` is `frozen=True` and its nested context objects are read-only to
  transformers.
- Services (`_spark`, `_adapter`, `_logger`, `_metrics_collector`, `_stage_index`) are
  injected at `.run()` time, not `__init__`. A transformer must not require them in its
  constructor and must tolerate `self._logger is None` (`self._logger or OrchestratorLogger()`).
- Halting is data, not an exception: `state.with_halted(reason)` surfaces as
  `ExecutionResult.early_exit` / `early_exit_reason`.

## Optional capabilities = protocols, not base classes

`HWMProvider`, `SCDMetricsProvider`, `SourceMetricsProvider` are `@runtime_checkable`
`Protocol`s in `transformers/protocols.py`. To report a watermark or counts,
declare the attributes — no registration, subclassing, or `orchestrator.py` edit.

Three invariants the harvest depends on; keep them in any new provider:

- Set the provider attributes **inside `_run()`/`_produce()`**, never only in `__init__`.
  For `SCDMetricsProvider`, `inserted` and `updated` both not None is the proof that a
  writer ran, which disambiguates mutually-exclusive `.add_if()` branches; declare them
  `None` in `__init__`. For `HWMProvider`, `hwm_after is not None` selects the watermark
  source only (F117).
- Declare every protocol attribute in `__init__` (counts as `None`), including ones that
  are structurally zero, which `_run()` then sets (an append-only writer sets
  `updated = 0` inside `_run()`). The orchestrator reads declared
  attributes, never `getattr(..., default)`, and must not conflate "missing" or "not run"
  with "zero".
- Wrappers (`ConditionalTransformer`, `ConditionalSourceTransformer`) are resolved by
  `_unwrap()` before the scan. A new wrapper must be added there or its inner
  transformer's metrics vanish.
