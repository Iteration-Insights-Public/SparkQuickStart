---
name: test-classification
description: Which suite a test belongs in. Scope decides, not machinery — and a misfiled test silently changes what the coverage gate means.
---

# Test classification

Enforced by: tests/architecture/test_test_classification.py

Three suites, split by **scope**, one directory each: `tests/unit/` (with `fabric/`,
`databricks/`, `local/`), `tests/integration/` (with `local/`), and `tests/architecture/`.
`tests/conftest.py` holds the shared SparkSession and stamps markers.

## The rule

> **unit** — exactly one of *our* units under test; every other unit of ours is mocked or
> supplied as a value.
> **integration** — two or more of our units wired together, both real.
> **architecture** — asserts on the shape of the source tree (imports, module contents,
> declared attributes, instruction files), reading source as text or an AST. Takes no fixture,
> starts no engine, imports a unit only to inspect it — or drives a pure-Python unit of ours,
> with no fixture and no engine, only to observe what it reads. One use, sanctioned (F65):
> `context_schema` in `tests/architecture/_examples.py`. A second use changes this sentence first.

Spark is not one of our units: a real `SparkSession` is an engine, like the interpreter.

## Decision procedure

Ask in order; the first **yes** wins:

0. Asserts on the shape of the source tree, with no fixture and no engine? → **architecture**
1. Calls `LoadOrchestrator.run()` or `_run_pipeline()`? → **integration**
2. Wires two or more transformers into a chain? → **integration**
3. Asserts on `ExecutionResult`, `result.stages`, or the metric harvest? → **integration**
4. A real, un-mocked unit of ours behind the thing under test (an adapter writing to actual
   storage, a real `LoadContextManager` feeding a real transformer)? → **integration**

Otherwise → **unit**. Not criteria: taking the `spark` fixture, avoiding Spark, using
`MockPlatformAdapter`, being slow, or the file's name. Engine is a separate axis with its own
marker.

## Mechanics

- `unit`, `integration` and `architecture` markers are stamped from the directory by
  `tests/conftest.py`, never by hand; the `spark` marker from the fixture list.
- A platform-specific test goes under `unit/<platform>/` or `integration/<platform>/`, and its
  path into `scripts/filter_platform_release.sh` in the same change.
- Nothing in `tests/` locates a repo file by climbing two or more levels from `__file__`
  (`.parent`, `.parents[k]`, `dirname`, `..`); walk up to the marker.
- `tests/architecture/` is stripped by platform-filtered builds (both `EXCLUDE_PATHS` arrays):
  its assertions describe the complete trunk.
- Coverage answers to the **unit** suite alone; the integration suite blocks merge on its own.
  The commands are in `.claude/standards/tooling-and-gates.md`.
