---
name: docstrings-and-comments
description: Google-style docstrings on public surfaces; comments explain why, and carry the measurement when they encode an incident.
---

# Docstrings and comments

- Google-style docstrings with `Args:` / `Returns:` / `Raises:` on everything public.
  Non-obvious behaviour goes in the docstring, not a comment above the call site.
- Comment the *why*, especially the non-obvious performance or correctness reason
  (this tree does it well — keep the standard). A comment that restates the code is noise.
- Feature work ships with a usage example, not only tests: a `Usage::` block in the module
  or class docstring, plus the `README.md` / `docs/` section if the feature is part of the
  public surface. A feature nobody can copy-paste into a notebook is not finished.
- When a fix encodes a past incident, record the measurement in the comment
  ("16 → 98 partitions, 117s"). That is what stops the next person reverting it.
