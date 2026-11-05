---
name: release-and-branching
description: Branch from main, never from release/*. Platform-filtered builds and version sourcing.
---

# Release and branching

Enforced by: tests/architecture/test_release_filter.py

- **Never branch from `release/*`.** Release branches are build outputs produced by
  `scripts/filter_platform_release.sh` + the sync workflow. Branch from `main`.
- `scripts/filter_platform_release.sh` is the single source of truth for what
  "fabric-only" and "databricks-only" mean. If you add a platform-specific file, add it
  there in the same change, or `verify_trunk_complete.sh` will be lying.
- Version lives only in `pyproject.toml`; `__version__` reads it back via
  `importlib.metadata`. Never hardcode a version string — and never *depend* on it
  either; see `.claude/rules/dual-shipping.md`.
