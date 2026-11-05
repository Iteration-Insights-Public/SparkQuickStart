---
name: module-layout
description: How a module in src/spark_quick_start is shaped — future annotations, docstring header, __all__, import placement.
---

# Module layout

- `from __future__ import annotations` at the top of every module (44/57 today; add it to
  any file you touch).
- Module docstring follows the house header:

  ```
  """
  name.py — one-line purpose.

  Contains: <public names>.
  Does NOT contain: <the boundaries this module deliberately refuses>.

  Usage::
      <smallest real example>
  """
  ```

  The `Does NOT contain:` line is the cheapest boundary documentation in the repo.
  Extend it to every module you create. `[partially adopted — 16/57]`
- Every public module declares `__all__`, alphabetically sorted.
- Typing-only imports go under `if TYPE_CHECKING:`. PySpark types in signatures are
  string annotations via the `__future__` import — do not import `DataFrame` at runtime
  just for a hint.
- Function-local imports are allowed for exactly two reasons: breaking an import cycle, or
  deferring an optional/platform dependency. Both require a comment saying which.
