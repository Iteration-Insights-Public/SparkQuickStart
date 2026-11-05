---
name: layering
description: Dependency direction and module boundaries inside src/spark_quick_start. Violation = architectural bug.
---

# Layering and dependency direction

Enforced by: tests/architecture/test_layering.py

Every solid dependency points downward or inward into an abstraction. Nothing points back up.
The diagram lives in `CLAUDE.md` §2; these are the enforceable rules behind it.

1. Dependencies point **downward and inward only**. Core never imports a concrete adapter.
2. `orchestration/` may import `adapters.platform_adapter` (the ABC) — never a concrete
   adapter at module scope (except `create_staging_main_function`'s default).
3. `transformers/` reach the Delta/SCD layer **only** through `transformers/_pipeline.py`
   and constants **only** through `transformers/_constants.py`. Those two files are pure
   re-export seams; keep them that way. Exception: `staging.py`
   (its docstring says why).
4. `orchestration/validation.py` is the single home for schema comparison.
   `transformers/_validation.py` holds `assert_no_dups` and nothing else.
5. *(Retired, F146.)* The DQF engine is gone; `test_dqf_removed.py` keeps it out. The number
   stays so that 6 and 7 keep theirs.
6. `adapters/` never imports `orchestration/` at module scope. Where it must, the import is
   function-local.
7. The DQD expression library, `orchestration/dqd/`, is **self-contained**: it imports only
   PySpark (not its pandas, Connect or ML subpackages), an allow-list of standard-library
   modules and itself — never the rest of `spark_quick_start` or a third-party package — and uses
   no builtin that imports or evaluates by string. Only the transformers' seam `_dqd` imports
   it, as they reach Delta through `_pipeline.py` (F139). Reflection built from strings is
   beyond the text check and left to review.
