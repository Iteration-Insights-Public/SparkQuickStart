---
name: platform-boundary
description: Where platform SDKs may appear, and how new platform capability is added. Portability is the product.
---

# Platform boundary — hard rules

Enforced by: tests/architecture/test_platform_boundary.py

Any change that works on one platform only is a bug unless it sits behind an adapter.

1. **No `notebookutils`, `mssparkutils`, `dbutils`, or `databricks.sdk` outside**
   `adapters/*_adapter.py` and `spark_logging/*_persistence.py`. One exception, and only
   this one: `local_notebookutils/`, the repo's stub, which injects a platform SDK into
   `sys.modules` for local runs rather than importing one (the test's `_EXEMPT_DIRS`, F26).
   A new exemption changes this sentence first, then the test.
2. Those imports are always **lazy** (inside the method body), never module top-level.
   The package must import cleanly with zero platform SDKs present.
3. New platform capability = **new method on `PlatformAdapter`**, with a default
   implementation when one can be written portably, plus an override per platform and a
   `MockPlatformAdapter` recording stub.
4. Concrete adapters are imported defensively in `adapters/__init__.py`
   (`try/except ImportError` → `None` → dropped from `__all__`). Platform-filtered release
   builds physically delete adapter files; the package must still import. Same pattern in
   `spark_logging/__init__.py`.
5. Adapter resolution from a platform string is lazy via `_PLATFORM_MAP` + `importlib`.
   Adding a platform means one map entry, not a new top-level import.
6. Identifier routing (Unity Catalog name vs filesystem path) belongs in
   `platform_adapter._is_uc_name()` and the Delta helpers around it. Callers pass an
   identifier and stay ignorant of which it is.
7. The runtime services that must go through an adapter, never be called directly from
   core: **file operations, notebook execution, notebook context, notebook exit, secrets,
   log persistence destination, and table path / identifier resolution.** A new need in
   this list is rule 3, not an exception to rule 1.
