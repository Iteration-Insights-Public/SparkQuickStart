---
name: dual-shipping
description: The library ships as a wheel AND as a copied ./src directory. Both must keep working; this constrains imports, deps, and metadata use.
---

# Dual shipping — wheel and source copy

Enforced by: tests/architecture/test_dual_shipping.py

The library reaches target environments two ways:

- **as a wheel** — `pip install spark_quick_start-<version>-py3-none-any.whl`
- **as a source copy** — `./src/` dropped into the environment (lakehouse Files, Workspace
  folder, repo checkout) and put on `sys.path`; see the source-copy snippet under
  Installation in `README.md`.

The source-copy path means **there is no install step**: no dependency resolution, no
package metadata, no build hook. Consequences, all binding:

- **Pure Python only.** No package data, no non-`.py` assets, no `importlib.resources`,
  no build-time code generation, no compiled extension.
- **Zero mandatory third-party imports beyond PySpark** (`dependencies = []`). Every
  optional dependency is a guarded import that degrades or raises a message naming the
  install — the existing `delta-spark` pattern: `try/except ImportError` → `None` at
  module scope, then a `RuntimeError` with the install hint at the point of use
  (`platform_adapter.resolve_delta_table`). Never a bare top-level `import delta`.
- **Never rely on installed metadata at runtime.** `__version__` resolves to
  `"0.0.0+unknown"` on a source copy (`PackageNotFoundError` branch in `__init__.py`), so
  no behaviour, gate, or compatibility check may branch on it.
- **No entry points, no console scripts, no plugin discovery** — none of it exists on a
  source copy.
- Imports are absolute (`from spark_quick_start...`) and the package root stays
  `src/spark_quick_start/`. Nothing may depend on the repo layout above `src/`.
- The defensive adapter imports in `adapters/__init__.py` and `spark_logging/__init__.py`
  serve this path too: a source copy may have had platform files stripped like a filtered wheel.

**Shippability test for any change:** it works installed from the wheel, *and* as `./src/`
on `sys.path` with nothing installed but PySpark. A feature needing a dependency a source
copy will not have degrades with a clear message rather than failing at import.
