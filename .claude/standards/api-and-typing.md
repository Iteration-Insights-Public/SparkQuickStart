---
name: api-and-typing
description: Signature, typing, and public-surface conventions for functions, classes, and dataclasses.
---

# API and typing

- The floor is Python 3.11: `requires-python`, mypy's `python_version` and ruff's
  `target-version` say so, and `tests/architecture/test_instruction_integrity.py` holds them
  together (F191). Syntax and library newer than that is out of bounds in `src/` — PEP 695
  generics and `type` aliases, `typing.override` (3.12). `X | Y`, `ExceptionGroup`,
  `tomllib` and `typing.Self` are in bounds.
- Full type hints on every public function, method, and dataclass field. No bare `Any`
  where a real type exists; `Any` for platform handles is fine.
- Keyword-only arguments (`*`) for optional behaviour flags
  (`CandidateGuardTransformer(*, sample_n=5)`).
- Prefer pure functions and small composable classes. Free functions live in
  `load_pipeline.py` / `delta_utils.py` / `validation.py`; stateful steps are transformers.
- Dataclasses for data (`frozen=True` when the value is config). Classes with behaviour
  get explicit `__init__`.
- Public API is re-exported from the package `__init__.py`. Importers should never need a
  private module path.

## Composition over inheritance

Inherit only from a base built to be extended, and only through the extension points it names.
Everything else is composed.

- **Sanctioned bases.** A transformer subclasses `SourceTransformer`, `ProcessTransformer` or
  `AlwaysRunTransformer` and implements `_produce` / `_run`, the template method's hooks
  (`.claude/rules/transformer-contract.md`). A platform implements `PlatformAdapter`; a log
  backend implements `LogPersistenceAdapter`. These are the hierarchies; do not start another.
- **No concrete-on-concrete subclassing.** `SCDGracefulChangeDetectorTransformer`, which extends
  `SCDChangeDetectorTransformer` by overriding the hash hooks the detector keeps for that purpose
  (`_ordered_hash_cols`, `_hash_string_expr`, `_hash_column_expr`), is the one case. A second
  is a review question: say why a wrapper or a constructor argument would not do.
- **Capabilities are protocols.** A step that reports a watermark or counts declares the
  attributes of `HWMProvider`, `SCDMetricsProvider` or `SourceMetricsProvider`; it does not
  need a base class or a registration. Naming the protocol among the bases, as the
  `NativeSCD*` writers do, documents it and adds nothing.
- **Behaviour is combined by wrapping, not by subclassing.** A conditional step is a
  `ConditionalTransformer` around another (`.add_if`), a choice of source is a
  `ConditionalSourceTransformer` (`.set_source`), and a pipeline is a sequence of steps built
  by `pipeline_factory.py`. A new concern is a new step in the pipeline, not a new
  responsibility of an existing one.
- **Dependencies are passed in.** Services reach a transformer at `.run()` time, and the
  orchestrator takes a `PlatformAdapter` object; nothing reaches a platform or a service by
  inheriting from it.
