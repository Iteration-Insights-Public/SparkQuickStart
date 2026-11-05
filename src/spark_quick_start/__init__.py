"""
spark_quick_start — Microsoft Fabric & Databricks data engineering utilities.

**Orchestration v2 packages (new):**

    from spark_quick_start.orchestration import LoadOrchestrator, ExecutionResult
    from spark_quick_start.orchestration import ColumnNamingPolicy
    from spark_quick_start.adapters import PlatformAdapter, MockPlatformAdapter
    from spark_quick_start.adapters import FabricAdapter, DatabricksAdapter, LocalAdapter
    from spark_quick_start.logger import OrchestratorLogger

**Stable domain modules:**

    from spark_quick_start.spark_logging import init_logs, get_logger, finalize_logs

"""
import sys
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

# A source copy has no install step to apply `requires-python`, so the floor is checked
# here, before any submodule imports and fails on a newer-only name (F191, dual-shipping).
if sys.version_info < (3, 11):  # noqa: UP036 — the check is for interpreters ruff's target excludes
    raise ImportError(
        "spark_quick_start requires Python 3.11 or later; this interpreter is "
        f"{sys.version_info[0]}.{sys.version_info[1]}"
    )

from spark_quick_start.orchestration import ColumnNamingPolicy

# Re-exported deliberately: `from spark_quick_start import ColumnNamingPolicy`
# is part of the public surface. Declared here so it reads as an export rather
# than an unused import (F401).
__all__ = ["ColumnNamingPolicy"]

# Single source of truth is pyproject.toml's `version`; this reads back what
# was actually installed rather than repeating the literal here. The two used
# to be maintained by hand and had drifted three minor versions apart.
try:
    __version__ = _pkg_version("spark_quick_start")
except PackageNotFoundError:  # pragma: no cover — source checkout, not installed
    __version__ = "0.0.0+unknown"
__author__ = "Val M"
