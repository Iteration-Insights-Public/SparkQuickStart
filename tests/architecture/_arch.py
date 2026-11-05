"""
_arch.py — source-tree helpers for the architecture suite.

Contains: repo anchors (ROOT, SRC, CLAUDE_DIR), file enumeration, AST
    helpers, the case-sensitive existence check and the symlink probe, used by every test module
    in this directory.
Does NOT contain: any assertion. Helpers only — a rule lives in its own module.

Imported as a bare module (``import _arch``) rather than a package member:
tests/ has no __init__.py anywhere, so pytest prepends each test file's own
directory to sys.path. The leading underscore keeps it out of collection.
"""
from __future__ import annotations

import ast
import os
import tempfile
from pathlib import Path

_MARKER = "pyproject.toml"


def repo_root() -> Path:
    """Return the directory holding pyproject.toml.

    Walks up to the marker rather than counting ``.parent`` levels, which
    test-classification.md bans: the unit/integration split moved every test one
    level deeper and broke the one place that counted.
    """
    here = Path(__file__).resolve()
    for candidate in (here, *here.parents):
        if (candidate / _MARKER).is_file():
            return candidate
    raise RuntimeError(f"no {_MARKER} found at or above {here}")


ROOT = repo_root()
SRC = ROOT / "src" / "spark_quick_start"
CLAUDE_DIR = ROOT / ".claude"

def package_files() -> list[Path]:
    """Every .py file shipped in the package, sorted."""
    return sorted(SRC.rglob("*.py"))


def rel(path: Path) -> str:
    """Repo-relative POSIX path — the form every failure message uses."""
    return path.relative_to(ROOT).as_posix()


def resolves_case_sensitively(base: Path, candidate: str) -> bool:
    """True when *candidate* exists under *base* with exactly this spelling.

    ``Path.exists()`` is case-insensitive on macOS and case-sensitive on the
    Linux runner CI uses, so it would let an assertion pass locally and fail in
    CI — the filesystem version of the interpreter trap tooling-and-gates.md
    warns about. The repo's readme was once ``readme.MD`` while pyproject named
    ``README.md``, which "existed" here and did not there (F45). Any assertion whose value depends on
    correctly detecting a missing path must resolve through this, not
    ``Path.exists()`` — the ruling applies to every such site, not just the one
    it was first written for.
    """
    current = base
    for part in Path(candidate).parts:
        if not current.is_dir():
            return False
        if part not in {entry.name for entry in current.iterdir()}:
            return False
        current = current / part
    return True


def tree(path: Path) -> ast.Module:
    """Parse *path* into an AST."""
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def imports(path: Path) -> list[tuple[str, ast.AST, bool]]:
    """Every import in *path* as ``(module_name, node, is_module_scope)``.

    ``module_name`` is the dotted name for ``import x.y`` and the ``from``
    target for ``from x.y import z`` — the thing a boundary rule cares about.

    ``is_module_scope`` is False only inside a function body. An import inside a
    class body, an ``if``, or a ``try`` is still executed at import time, so it
    counts as module scope: that is precisely what platform-boundary rule 2 is
    about.
    """
    found: list[tuple[str, ast.AST, bool]] = []

    def walk(node: ast.AST, module_scope: bool) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.Import):
                found.extend((alias.name, child, module_scope) for alias in child.names)
            elif isinstance(child, ast.ImportFrom):
                found.append((child.module or "", child, module_scope))
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                walk(child, module_scope=False)
            else:
                walk(child, module_scope)

    walk(tree(path), module_scope=True)
    return found


def _handler_names(expr: ast.expr | None) -> set[str]:
    if isinstance(expr, ast.Name):
        return {expr.id}
    if isinstance(expr, ast.Tuple):
        return {e.id for e in expr.elts if isinstance(e, ast.Name)}
    return set()


def import_error_guarded(path: Path) -> set[int]:
    """Line numbers of imports wrapped in ``try/except ImportError``.

    This is the pattern dual-shipping.md mandates for every optional
    dependency and for the defensive concrete-adapter imports, so several rules
    need to ask "was this one guarded?".
    """
    guarded: set[int] = set()
    for node in ast.walk(tree(path)):
        if not isinstance(node, ast.Try):
            continue
        catches = set()
        for handler in node.handlers:
            catches |= _handler_names(handler.type)
        if not catches & {"ImportError", "ModuleNotFoundError"}:
            continue
        for stmt in node.body:
            for inner in ast.walk(stmt):
                if isinstance(inner, (ast.Import, ast.ImportFrom)):
                    guarded.add(inner.lineno)
    return guarded


def symlink_unavailable() -> str | None:
    """Why this host cannot create a symlink, or None when it can (F105).

    Windows without Developer Mode or elevation refuses `os.symlink` with WinError 1314. A
    mutation that needs a symlink is skipped there, with this reason, and the mutations
    around it still run. Probed per call, not cached: it costs one link in a temp directory.
    """
    with tempfile.TemporaryDirectory() as tmp:
        target = Path(tmp) / "target"
        target.write_text("", encoding="utf-8")
        try:
            os.symlink(target, Path(tmp) / "link")
        except (OSError, NotImplementedError) as exc:
            return f"this host cannot create a symlink ({exc}); the symlink mutations were not run"
    return None
