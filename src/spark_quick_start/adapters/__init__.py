"""Platform adapters for the spark_quick_start orchestration layer.

Import from here for a stable, platform-agnostic entry point::

    from spark_quick_start.adapters import (
        PlatformAdapter, MockPlatformAdapter,
        FabricAdapter, DatabricksAdapter, LocalAdapter,
    )

Concrete adapters (``FabricAdapter``, ``DatabricksAdapter``, ``LocalAdapter``)
are optional at the file level: platform-filtered release builds ship only
the adapter(s) relevant to that platform, so each is imported defensively and
resolves to ``None`` (and is dropped from ``__all__``) when its module isn't
present in this checkout.
"""
from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter, PlatformAdapter

__all__ = ["MockPlatformAdapter", "PlatformAdapter"]

try:
    from spark_quick_start.adapters.local_adapter import LocalAdapter
except ImportError:
    LocalAdapter = None  # type: ignore[assignment,misc]
else:
    __all__.append("LocalAdapter")

try:
    from spark_quick_start.adapters.databricks_adapter import DatabricksAdapter
except ImportError:
    DatabricksAdapter = None  # type: ignore[assignment,misc]
else:
    __all__.append("DatabricksAdapter")

try:
    from spark_quick_start.adapters.fabric_adapter import FabricAdapter
except ImportError:
    FabricAdapter = None  # type: ignore[assignment,misc]
else:
    __all__.append("FabricAdapter")
