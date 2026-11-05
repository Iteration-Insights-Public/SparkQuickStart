"""
test_platform_boundary.py — executable form of .claude/rules/platform-boundary.md.

Contains: SDK containment, lazy-import enforcement, defensive adapter imports,
    platform resolution, and the MockPlatformAdapter completeness check.
Does NOT contain: dependency direction — that is test_layering.py.
"""
from __future__ import annotations

import ast
import fnmatch
import os
import re
import subprocess
import sys

import _arch
import pytest

PKG = "spark_quick_start"

# The only two globs where a platform SDK may be named at all.
_SANCTIONED = (
    "src/spark_quick_start/adapters/*_adapter.py",
    "src/spark_quick_start/spark_logging/*_persistence.py",
)

# local_notebookutils/ is this repo's own stub package, not a platform SDK.
_EXEMPT_DIRS = ("src/spark_quick_start/local_notebookutils/",)


def _is_sdk(name: str) -> bool:
    head = name.split(".")[0]
    # "databricks" catches both `import databricks.sdk` (head == "databricks")
    # and a bare `import databricks` (head == "databricks" too, name == head).
    return head in {"notebookutils", "mssparkutils", "dbutils", "databricks"}


def _sanctioned(rel_path: str) -> bool:
    if rel_path.startswith(_EXEMPT_DIRS):
        return True
    if rel_path == "src/spark_quick_start/adapters/platform_adapter.py":
        return False
    return any(fnmatch.fnmatch(rel_path, pattern) for pattern in _SANCTIONED)


def test_no_platform_sdk_outside_the_sanctioned_globs() -> None:
    """Rule platform-boundary §1 — 'No exceptions'.

    Two floors before the assertion, because F1's removal of this test's
    `xfail(strict=True)` made it the only guard on the rule. Under strict xfail an
    empty scan was caught — it would have XPASSed. Plain, it is silent, and this
    exact shape has now been found three times: F2 left `len(holders) <= 1`
    calibrated for its pre-fix world, F3 needed two floors added on review, and
    `test_instruction_integrity.py` sets the in-repo precedent with a count floor.

    The second floor is a positive control, which a count alone does not give: if
    `_is_sdk` or `_arch.imports` regressed, `offenders` would stay empty and the
    rule would report clean over a package full of violations.
    """
    scanned = list(_arch.package_files())
    assert len(scanned) >= 40, (
        f"the scan reached only {len(scanned)} modules; _arch.package_files() globs a "
        "hardcoded src/spark_quick_start, so a moved package makes it return nothing "
        "and this assertion pass over an empty set"
    )
    detector_sees = [
        _arch.rel(path)
        for path in scanned
        if any(_is_sdk(name) for name, _, _ in _arch.imports(path))
    ]
    # databricks_persistence.py no longer imports an SDK itself: it resolves dbutils through
    # databricks_adapter._get_dbutils (F120). The positive controls are the files that still do.
    for known in (
        "src/spark_quick_start/spark_logging/fabric_persistence.py",
        "src/spark_quick_start/adapters/databricks_adapter.py",
    ):
        assert known in detector_sees, (
            f"the detector no longer sees the SDK import in {known}, which is a known sanctioned "
            "one. _is_sdk or _arch.imports has regressed, and a clean result below would mean "
            f"nothing. Seen: {detector_sees}"
        )

    offenders = []
    for path in scanned:
        rel_path = _arch.rel(path)
        if _sanctioned(rel_path):
            continue
        for name, node, _ in _arch.imports(path):
            if _is_sdk(name):
                offenders.append(f"{rel_path}:{node.lineno} imports {name}")
    assert not offenders, (
        "a platform SDK is imported outside adapters/*_adapter.py and "
        "spark_logging/*_persistence.py. New platform capability is a new method "
        f"on PlatformAdapter, not a new import (rule: platform-boundary §1,3): {offenders}"
    )


def test_platform_sdk_imports_are_function_local() -> None:
    """Rule platform-boundary §2 — the package imports with zero SDKs present."""
    offenders = []
    for path in _arch.package_files():
        rel_path = _arch.rel(path)
        if rel_path.startswith(_EXEMPT_DIRS):
            continue
        for name, node, module_scope in _arch.imports(path):
            if _is_sdk(name) and module_scope:
                offenders.append(f"{rel_path}:{node.lineno} imports {name} at module scope")
    assert not offenders, (
        "a platform SDK is imported at module scope. These imports are always "
        f"inside the method body (rule: platform-boundary §2): {offenders}"
    )


def test_the_package_imports_with_no_platform_sdk_installed() -> None:
    """The consequence of §2, proven rather than inferred.

    Runs in a subprocess with a meta-path finder that refuses every platform
    SDK, then imports the package and every submodule.
    """
    script = (
        "import sys, pkgutil, importlib\n"
        "BANNED = ('notebookutils', 'mssparkutils', 'dbutils', 'databricks')\n"
        # find_module() was removed from the import protocol in Python 3.12 and has been
        # ignored since 3.4 when find_spec() is present, so a shim for it was dead code on
        # every interpreter this package supports (F19 item 5).
        "class Blocker:\n"
        "    def find_spec(self, name, path=None, target=None):\n"
        "        if name.split('.')[0] in BANNED:\n"
        "            raise ImportError('blocked by the architecture suite: ' + name)\n"
        "        return None\n"
        "sys.meta_path.insert(0, Blocker())\n"
        "import spark_quick_start as p\n"
        "n = 0\n"
        "for m in pkgutil.walk_packages(p.__path__, p.__name__ + '.'):\n"
        "    if 'local_notebookutils' in m.name:\n"
        "        continue\n"
        "    importlib.import_module(m.name)\n"
        "    n += 1\n"
        "print(f'imported {n}')\n"
    )
    # Inherit the real environment and only override PYTHONPATH. A hand-built
    # two-variable env is not portable — the interpreter may need HOME, TMPDIR or
    # a venv's own variables, and a failure there would look like a boundary
    # violation. The Blocker above is what enforces the rule, not the env.
    env = {**os.environ, "PYTHONPATH": str(_arch.ROOT / "src")}
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=_arch.ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        "the package does not import with zero platform SDKs present, which is "
        "what every source-copy deployment starts from (rules: platform-boundary §2, "
        f"dual-shipping):\n{result.stdout}\n{result.stderr}"
    )
    match = re.search(r"^imported (\d+)$", result.stdout, flags=re.MULTILINE)
    imported = int(match.group(1)) if match else 0
    assert imported > 0, (
        "the child process reported importing zero submodules, so this test proved "
        "nothing about the boundary — either pkgutil.walk_packages found no modules "
        f"or the child exited before printing its count:\n{result.stdout}\n{result.stderr}"
    )


def test_concrete_adapter_imports_are_defensive() -> None:
    """Rule platform-boundary §4 — a platform-filtered build must still import."""
    for rel_path in (
        "src/spark_quick_start/adapters/__init__.py",
        "src/spark_quick_start/spark_logging/__init__.py",
    ):
        path = _arch.ROOT / rel_path
        guarded = _arch.import_error_guarded(path)
        unguarded = [
            f"{rel_path}:{node.lineno} imports {name}"
            for name, node, _ in _arch.imports(path)
            if name.startswith(PKG)
            and ("_adapter" in name or "_persistence" in name)
            and "platform_adapter" not in name
            and "persistence_adapter" not in name
            and node.lineno not in guarded
        ]
        assert not unguarded, (
            "a concrete platform module is imported without a try/except ImportError "
            "guard. filter_platform_release.sh physically deletes these files; the "
            f"package must still import (rule: platform-boundary §4): {unguarded}"
        )


def test_platform_resolution_fails_with_a_value_error_not_an_import_error() -> None:
    """A stripped adapter must produce the same class of message as an unknown platform."""
    import importlib
    from unittest.mock import patch

    from spark_quick_start.orchestration.orchestrator import _PLATFORM_MAP, LoadOrchestrator

    real = importlib.import_module

    for platform, dotted in _PLATFORM_MAP.items():
        module_path = dotted.rsplit(".", 1)[0]

        def fake(name, *args, _target=module_path, **kwargs):
            if name == _target:
                raise ModuleNotFoundError(f"No module named {name!r}")
            return real(name, *args, **kwargs)

        with patch("importlib.import_module", fake), pytest.raises(ValueError) as excinfo:
            LoadOrchestrator._resolve_adapter(platform)

        assert platform in str(excinfo.value), (
            "the error does not name the platform that could not be resolved"
        )


def test_mock_adapter_overrides_every_abstract_method() -> None:
    """Rule platform-boundary §3 — every capability has a recording stub."""
    from spark_quick_start.adapters.platform_adapter import MockPlatformAdapter, PlatformAdapter

    abstract = set(getattr(PlatformAdapter, "__abstractmethods__", set()))
    missing = [name for name in sorted(abstract) if name not in MockPlatformAdapter.__dict__]
    assert not missing, (
        "MockPlatformAdapter does not implement every abstract method, so a unit "
        f"test cannot exercise the capability (rule: platform-boundary §3): {missing}"
    )


def test_every_platform_map_entry_names_a_real_module_path() -> None:
    """A typo in _PLATFORM_MAP is only discoverable at runtime otherwise.

    No "does the file even exist" skip: that tolerance existed only for a
    platform-filtered checkout, and the architecture suite as a whole is now
    stripped from filtered builds (release-and-branching, filter_platform_
    release.sh), so this test never runs there at all. On the trunk this
    module path must always resolve — a missing file is exactly the typo this
    test exists to catch, in the other half of the dotted path.
    """
    from spark_quick_start.orchestration.orchestrator import _PLATFORM_MAP

    bad = []
    for platform, dotted in _PLATFORM_MAP.items():
        module_path, _, class_name = dotted.rpartition(".")
        expected = _arch.ROOT / "src" / (module_path.replace(".", "/") + ".py")
        if not expected.exists():
            bad.append(f"{platform} → {dotted} (module path does not exist: {_arch.rel(expected)})")
            continue
        source = _arch.tree(expected)
        names = {n.name for n in ast.walk(source) if isinstance(n, ast.ClassDef)}
        if class_name not in names:
            bad.append(f"{platform} → {dotted} (class not found in {_arch.rel(expected)})")
    assert not bad, f"_PLATFORM_MAP points at a class that does not exist: {bad}"
