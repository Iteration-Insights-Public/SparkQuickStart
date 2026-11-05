"""F146 — the legacy DQF engine is gone, and stays gone.

`fabric_dqf.py` and `DQFTransformer` were removed on 2026-09-30, after the Declarative Query
Definition (F137-F140) replaced them. The DQD keeps DQF's spellings only as refusals that say
what to write instead, and its reference page keeps a mapping table for migration; DQF named in
a comment, a docstring or a message is fine.

Out of reach, and left to review: the engine brought back under another name, imported by that
name. A name-based check cannot tell `legacy_query.py` from any other module.
"""
from __future__ import annotations

import ast
import re
import tempfile
from itertools import pairwise
from pathlib import Path

import _arch

_ENGINE = "fabric_dqf"


def _module_parts(path: Path, root: Path) -> list[str]:
    parts = list(path.relative_to(root).parts)
    parts[-1] = parts[-1].split(".")[0]
    return [p.lower() for p in parts]


def _is_dqf_module(parts: list[str]) -> bool:
    """A module or package named fabric_dqf anywhere, or named dqf under transformers."""
    return _ENGINE in parts or any(a == "transformers" and b == "dqf" for a, b in pairwise(parts))


def _names_dqf(dotted: str) -> bool:
    return _is_dqf_module([p.lower() for p in dotted.split(".") if p])


def _dqf_tree_problems(root: Path) -> list[str]:
    """Every DQF module or import under *root*, the directory holding the spark_quick_start package.

    Paths are compared by name, lower-cased, so the verdict does not depend on the file system's
    case sensitivity. Relative imports are resolved against the importing file's package, and a
    `from X import Y` counts `X.Y`; a string that is exactly a dotted module name naming DQF, as
    `importlib.import_module` would take, is reported too.
    """
    problems = []
    for path in sorted(root.rglob("*")):
        if "__pycache__" in path.parts or not (path.is_dir() or path.suffix in {".py", ".pyi", ".so", ".pyd"}):
            continue
        parts = _module_parts(path, root)
        if _is_dqf_module(parts):
            problems.append(f"{path.relative_to(root).as_posix()} is a DQF module")
    for path in sorted(root.rglob("*.py")):
        package = _module_parts(path, root)[:-1]
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError as exc:
            problems.append(f"{path.relative_to(root).as_posix()}: does not parse: {exc.msg}")
            continue
        for node in ast.walk(tree):
            names: list[str] = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                base = package[: len(package) - (node.level - 1)] if node.level else []
                module = ".".join([*base, *([node.module] if node.module else [])])
                names = [module, *(f"{module}.{a.name}" for a in node.names)]
            elif isinstance(node, ast.Constant) and isinstance(node.value, str) \
                    and re.fullmatch(r"[\w.]+", node.value) and _names_dqf(node.value):
                names = [node.value]
            if any(_names_dqf(n) for n in names):
                problems.append(f"{path.relative_to(root).as_posix()}:{node.lineno} reaches {names[0] or names[-1]}")
    return problems


def _dqf_exports(names: list[str]) -> list[str]:
    """Exported names that advertise DQF, in any case."""
    return [n for n in names if "dqf" in n.lower()]


def test_nothing_of_dqf_is_left_in_the_package() -> None:
    problems = _dqf_tree_problems(_arch.SRC.parent)
    assert not problems, f"DQF is back (F146): {problems}"


def test_no_package_exports_a_dqf_name() -> None:
    import spark_quick_start
    import spark_quick_start.orchestration as orchestration
    import spark_quick_start.orchestration.transformers as transformers

    names = [n for m in (spark_quick_start, orchestration, transformers) for n in getattr(m, "__all__", [])]
    assert not _dqf_exports(names), f"a DQF name is exported (F146): {_dqf_exports(names)}"


def test_every_dqf_check_can_fail() -> None:
    """F146 — anti-vacuity: each form the adversary found is reported; each valid one is not.

    `DQFMigrationError`-style exports are reported on purpose: an exported name that says DQF
    reads as DQF being supported."""
    trees = {
        "the module": {"spark_quick_start/fabric_dqf.py": ""},
        "the module in upper case": {"spark_quick_start/FABRIC_DQF.py": ""},
        "a package": {"spark_quick_start/fabric_dqf/__init__.py": ""},
        "a namespace package": {"spark_quick_start/fabric_dqf/engine.py": ""},
        "the transformer module": {"spark_quick_start/orchestration/transformers/dqf.py": ""},
        "the transformer as a package": {"spark_quick_start/orchestration/transformers/dqf/__init__.py": ""},
        "a compiled module": {"spark_quick_start/fabric_dqf.cpython-311-darwin.so": ""},
        "from the root": {"spark_quick_start/orchestration/x.py": "from spark_quick_start import fabric_dqf\n"},
        "relatively": {"spark_quick_start/orchestration/x.py": "from .. import fabric_dqf\n"},
        "relatively from a module": {"spark_quick_start/orchestration/x.py": "from ..fabric_dqf import build_dataframe\n"},
        "the transformer relatively": {"spark_quick_start/orchestration/transformers/__init__.py": "from . import dqf\n"},
        "the transformer by name": {"spark_quick_start/orchestration/transformers/__init__.py":
                                    "from .dqf import DQFTransformer\n"},
        "the transformer from its parent": {"spark_quick_start/orchestration/__init__.py":
                                            "from spark_quick_start.orchestration.transformers import dqf\n"},
        "a submodule": {"spark_quick_start/x.py": "import spark_quick_start.orchestration.transformers.dqf.core\n"},
        "by string": {"spark_quick_start/x.py": "import importlib\nimportlib.import_module('spark_quick_start.fabric_dqf')\n"},
        "by __import__": {"spark_quick_start/x.py": "__import__('spark_quick_start.fabric_dqf', fromlist=['x'])\n"},
    }
    silent = []
    for label, files in trees.items():
        with tempfile.TemporaryDirectory() as tmp:
            for rel, text in files.items():
                (Path(tmp) / rel).parent.mkdir(parents=True, exist_ok=True)
                (Path(tmp) / rel).write_text(text)
            if not _dqf_tree_problems(Path(tmp)):
                silent.append(label)
    assert not silent, f"these DQF forms were not reported: {silent}"
    controls = {
        "a migration module": {"spark_quick_start/orchestration/dqd/fabric_dqf_migration.py": "",
                               "spark_quick_start/orchestration/dqd/__init__.py": "from . import fabric_dqf_migration\n"},
        "a dqf_utils module": {"spark_quick_start/orchestration/transformers/dqf_utils.py": ""},
        "DQF in a docstring": {"spark_quick_start/x.py": '"""The removed fabric_dqf.py engine."""\nX = "DQF\'s key"\n'},
        "a dqd module": {"spark_quick_start/orchestration/transformers/dqd.py": "from spark_quick_start.orchestration import dqd\n"},
    }
    noisy = {}
    for label, files in controls.items():
        with tempfile.TemporaryDirectory() as tmp:
            for rel, text in files.items():
                (Path(tmp) / rel).parent.mkdir(parents=True, exist_ok=True)
                (Path(tmp) / rel).write_text(text)
            if _dqf_tree_problems(Path(tmp)):
                noisy[label] = _dqf_tree_problems(Path(tmp))
    assert not noisy, f"these valid forms were reported: {noisy}"
    assert _dqf_exports(["DQFTransformer"]) and _dqf_exports(["DqfTransformer"]) and _dqf_exports(["DQFMigrationError"])
    assert not _dqf_exports(["DQDTransformer", "LoadOrchestrator"])
