"""
test_dual_shipping.py — executable form of .claude/rules/dual-shipping.md.

Contains: guarded-import, package-purity, entry-point and metadata assertions.
Does NOT contain: platform containment — that is test_platform_boundary.py.

The rule exists because the library also ships as ./src/ copied onto sys.path,
where there is no install step: no dependency resolution, no package metadata,
no build hook.
"""
from __future__ import annotations

import ast
import sys

import _arch

PKG = "spark_quick_start"

# Everything a source copy is guaranteed to have: the stdlib and PySpark.
_ALWAYS_AVAILABLE = set(sys.stdlib_module_names) | {PKG, "pyspark", "__future__"}


def test_no_unguarded_third_party_import_at_module_scope() -> None:
    """Every optional dependency is a try/except ImportError, never a bare import."""
    offenders = []
    for path in _arch.package_files():
        guarded = _arch.import_error_guarded(path)
        for name, node, module_scope in _arch.imports(path):
            head = name.split(".")[0]
            if not module_scope or not head or head in _ALWAYS_AVAILABLE:
                continue
            if node.lineno in guarded:
                continue
            offenders.append(f"{_arch.rel(path)}:{node.lineno} imports {name}")
    assert not offenders, (
        "an unguarded third-party import at module scope. A source copy has "
        "nothing installed but PySpark, so this raises at import time. Use the "
        "delta-spark pattern: try/except ImportError -> None at module scope, then "
        f"a RuntimeError naming the install at the point of use: {offenders}"
    )


def test_the_package_contains_only_python_files() -> None:
    """Pure Python only — no package data, no assets, no importlib.resources."""
    extras = sorted(
        _arch.rel(p)
        for p in _arch.SRC.rglob("*")
        if p.is_file() and p.suffix != ".py" and "__pycache__" not in p.parts
    )
    assert not extras, (
        "a non-.py file is shipped inside the package. A source copy has no "
        f"package-data mechanism, so this file will not be there: {extras}"
    )


def test_no_entry_points_are_declared() -> None:
    """No console scripts, no plugin discovery — none of it exists on a source copy."""
    text = (_arch.ROOT / "pyproject.toml").read_text(encoding="utf-8")
    banned = [
        section
        for section in ("[project.scripts]", "[project.gui-scripts]", "[project.entry-points")
        if section in text
    ]
    assert not banned, (
        f"pyproject.toml declares entry points, which a source copy never sees: {banned}"
    )


def test_no_behaviour_branches_on_the_installed_version() -> None:
    """__version__ is '0.0.0+unknown' on a source copy, so nothing may depend on it."""
    offenders = [
        f"{_arch.rel(path)}:{node.lineno}"
        for path in _arch.package_files()
        if not (path.name == "__init__.py" and path.parent == _arch.SRC)
        for node in ast.walk(_arch.tree(path))
        if (isinstance(node, ast.Name) and node.id == "__version__") or (
            isinstance(node, ast.Attribute) and node.attr == "__version__"
        )
    ]
    assert not offenders, (
        "a module reads __version__. It resolves to '0.0.0+unknown' on a source "
        f"copy, so no behaviour, gate, or compatibility check may branch on it: {offenders}"
    )


def test_every_import_of_the_package_is_absolute() -> None:
    """Relative imports tie the package to a layout a source copy may not have."""
    offenders = [
        f"{_arch.rel(path)}:{node.lineno} (level={node.level})"
        for path in _arch.package_files()
        for node in ast.walk(_arch.tree(path))
        if isinstance(node, ast.ImportFrom) and node.level
    ]
    assert not offenders, f"relative import found; this package uses absolute only: {offenders}"
