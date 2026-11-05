---
name: testing
description: What a test suite addition looks like here — MockPlatformAdapter, spark marker, boundary assertions, coverage gate.
---

# Testing

- Three suites split by scope: `tests/unit/` and `tests/integration/`, each with its own
  `fabric/`, `databricks/`, `local/` subdirectory for platform-scoped tests (those are
  stripped by platform-filtered release builds), and `tests/architecture/`, which reads the
  source tree rather than running it. Which suite a test belongs in is a rule, not a
  preference — see `.claude/rules/test-classification.md`.
- A unit test's adapter is `MockPlatformAdapter`, or a `PlatformAdapter` subclass with the
  abstract methods stubbed when it needs the base class's defaults. `LocalAdapter` writes to real
  storage, so a test using it is integration and lives under `tests/integration/local/`, which
  platform-filtered builds strip with the adapter. No test needs Fabric or Databricks. A unit
  test may take the `spark` fixture when the unit needs an engine:
  Spark is an engine, not a unit of ours (`test-classification.md`). Any test taking the
  fixture is auto-marked `spark`; the fast suite is `-m 'not spark'`.

## Mocks

A unit test exercises one unit of ours, and the rule lets every other unit of ours be mocked or
supplied as a value. What to mock, and what not to, follows from that:

- **Never mock the unit under test.** A `patch.object` on the class or function a test is about
  replaces the code the test claims to prove. Patching a seam it calls is fine (the Delta
  helpers, the logger, or `LoadOrchestrator._resolve_adapter` in a pipeline-factory test, where
  the factory is the unit); patching a method of the class under test is a review question.
- **An adapter is `MockPlatformAdapter`, never a bare `MagicMock`.** The mock has only
  `PlatformAdapter`'s methods and records each call with its identifiers, and a few methods
  refuse as the real ones do (`write_delta_create`, the table properties); a `MagicMock`
  accepts any method, including one `PlatformAdapter` does not have, so a renamed adapter method
  passes. Its reads return a `MagicMock` unless configured: set `delta_read_result` when the
  frame is read.
- **A `MagicMock` stands in for a DataFrame only where the test proves nothing read it** (a
  step that must refuse before touching its input pins `capture_partitions` off and asserts
  `not frame.method_calls`). A frame that only passes through a step's `__call__` or
  `produce()` is read by the base hooks (`rdd.getNumPartitions()`); allow that one call with
  `only_partition_probe` from `tests/unit/_frames.py`, never in a refusal test (F180).
  Where the behaviour depends on the data, use the `spark` fixture and real rows.
- Every new transformer gets: a unit test with `MockPlatformAdapter`, and a pipeline
  integration test asserting its `StageMetrics` entry and any protocol attributes it sets.
- Test the boundary, not the implementation: assert on `ExecutionResult`, recorded adapter
  calls, and `state`/`frames` — not on internal call order.
- Coverage gate is `fail_under = 85` over `src/`, with documented omissions
  (platform adapters, `spark_logging/*`, `load_pipeline.py`).
  Do not extend the omit list to dodge a gate.
- A bug fix lands with the test that fails without it.
- **No test requires a live Fabric or Databricks workspace** unless explicitly requested.
  Platform behaviour is covered through `MockPlatformAdapter` and the platform-scoped
  suites, not against a real runtime.
- **Any test walking `__subclasses__()` must filter to the shipped package**
  (`cls.__module__.startswith("spark_quick_start")`). `tests/` defines dozens of ad-hoc
  `SourceTransformer`/`ProcessTransformer` subclasses inline for its own fixtures, and
  `__subclasses__()` returns every subclass the interpreter has ever seen, not just this
  package's — one such fixture (`_HookedSource`, reusing the name `inserted` for something
  unrelated) was enough to fail an architecture assertion that had nothing to do with it,
  and only when the whole suite ran in one process.
- **When the thing being checked is Python, scan the AST, not the source text.** A comment
  naming a banned pattern and a docstring illustrating one are both invisible to an AST walk
  and both get flagged by a line scan — a rule's enforcement must not punish the prose that
  explains the rule. Text scanning is still correct for Markdown and shell, which have no
  AST; there the same question still applies — could this match an illustrative example
  rather than the real thing?

## Dimensions to cover

Not a checklist to satisfy mechanically — these are the axes where this library has
historically broken. For a change touching the load path, ask which of them it moves:

| Dimension | What it means here |
|---|---|
| Pure Python unit | Context parsing, `ColumnNamingPolicy` resolution, `ExecutionResult` round-trip — no Spark at all |
| PySpark DataFrame | Hashing, change classification, join semantics, column ordering |
| Adapter mock | Which adapter calls were made, with what identifiers — assert on `MockPlatformAdapter`'s recorded call lists |
| Delta load behaviour | Merge vs append vs append-only, close-out rows, version increments, HWM before/after |
| Error handling | The `ValueError` message names the offending value; `run()` returns `status="error"` rather than raising |
| Empty input | Zero staged rows, zero candidates — the `CandidateGuardTransformer` halt path and `early_exit` surfacing |
| Null handling | NULL sentinels in the hash, `SCDGracefulChangeDetectorTransformer`'s trailing-NULL trimming, all-null rows |
| Duplicate keys | `assert_no_dups` and `CandidateGuardTransformer` — one candidate per `(business_key, source_system)` |
| Schema drift | A new nullable column must not bump SCD2 versions; `compare_schemas_primitive_ignore_order` reports missing/extra/type-mismatch |
| First load | Target table absent — `target_df is None` flows through change detection and the merge |
| Serverless | `runtime_ctx.serverless=True` skips every cache path without changing results |
