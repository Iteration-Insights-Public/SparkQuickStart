"""
_aliases.py — deprecated names that still resolve, with a warning.

Contains: `deprecated_aliases`, which builds a module-level `__getattr__` (PEP 562) mapping an
    old name to the object now exported under a new one, and the map of the names F155
    renamed.
Does NOT contain: any transformer. An alias is never in `__all__`: a star import and the
    reference pages see the new names only.

F155 gave the five SCD-specific steps an `SCD` prefix, so the Data Vault steps beside them are
named alike. Notebooks outside this repo import the old names; each keeps working, warning with
the name to use, until the aliases are removed in release 0.5.0.

Usage::

    __getattr__ = deprecated_aliases(__name__, globals(), SCD_RENAMES)
"""
from __future__ import annotations

import warnings
from collections.abc import Callable, Mapping
from typing import Any

__all__ = ["REMOVED_IN", "SCD_RENAMES", "deprecated_aliases"]

# The release that drops the aliases; stated in every warning, never compared with __version__.
REMOVED_IN = "0.5.0"

# Old name -> new name (F155, 2026-10-01).
SCD_RENAMES: Mapping[str, str] = {
    "TargetTableTransformer": "SCDTargetTableTransformer",
    "IncrementalTargetTableTransformer": "SCDIncrementalTargetTableTransformer",
    "ChangeDetectorTransformer": "SCDChangeDetectorTransformer",
    "GracefulChangeDetectorTransformer": "SCDGracefulChangeDetectorTransformer",
    "CandidateVersionGuardTransformer": "SCDCandidateVersionGuardTransformer",
}


def deprecated_aliases(
    module_name: str, namespace: Mapping[str, Any], renames: Mapping[str, str]
) -> Callable[[str], Any]:
    """Build a module `__getattr__` that resolves each old name in *renames* to its new one.

    Args:
        module_name: The module's `__name__`, for the error on an unknown attribute.
        namespace: The module's `globals()`, where the new names are looked up when asked for,
            so the alias always returns the class the module exports.
        renames: Old name to new name; only the entries whose new name the module defines
            are served.

    Returns:
        A function to assign to the module's `__getattr__`. It warns with
        `DeprecationWarning` naming the replacement, and raises `AttributeError` for any other
        name, as a module without it would.
    """

    def __getattr__(name: str) -> Any:
        new = renames.get(name)
        if new is not None and new in namespace:
            warnings.warn(
                f"{name} is deprecated; use {new}. The old name is removed in {REMOVED_IN}.",
                DeprecationWarning,
                stacklevel=2,
            )
            return namespace[new]
        raise AttributeError(f"module {module_name!r} has no attribute {name!r}")

    return __getattr__
