---
name: release-and-branching
description: Branch from main. One release per version, tagged v<version>, cut from main. Version sourcing.
---

# Release and branching

Enforced by: tests/architecture/test_ci_workflows.py

- **Branch from `main`.** There is one core build for every platform; no branch is a build
  output.
- **One release per version, tagged `v<version>`, cut by `release.yaml` from `main` only,** at
  a commit whose push run passed, never twice for one version. `publish-public.yaml` requires
  that tag and exports the commit it names.
- Version lives only in `pyproject.toml`; `__version__` reads it back via
  `importlib.metadata`. Never hardcode a version string — and never *depend* on it
  either; see `.claude/rules/dual-shipping.md`.
