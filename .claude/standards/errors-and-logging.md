---
name: errors-and-logging
description: Fail loudly with actionable messages; one place swallows exceptions; log through OrchestratorLogger, never print.
---

# Errors and logging

- Fail loudly on programming and configuration errors: `ValueError` with the offending
  value, the reason, and what to do about it. Error text is a UX surface — messages in
  this tree name the file, the column, and the fix. Keep that bar.
- `LoadOrchestrator.run()` is the only place that swallows exceptions. It captures the
  traceback into `ExecutionResult(status="error")` and finalizes logs in `finally`. Nothing
  below it may silently swallow.
- Bare `except Exception: pass` is permitted **only** around a platform/JVM probe whose
  failure is genuinely non-fatal, and must carry a comment saying so. `BLE001` is currently
  in the deferred-ignore list; do not add to the count.
- Log through `OrchestratorLogger`, never `print`. Numeric facts go in the `metrics=[...]`
  payload; large text (query plans) goes to `debug()`. Mark always-on lines
  `important=True`.
- Never log row data at INFO. `logging_context.suppress_stdout_tail` exists because
  captured stdout ends up in a log file with different read permissions than the source
  table.
