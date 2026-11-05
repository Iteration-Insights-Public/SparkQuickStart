"""
test_layering.py — executable form of .claude/rules/layering.md.

Contains: assertions on dependency direction, the transformer seams, and the
    self-contained DQD library.
Does NOT contain: platform-SDK containment — that is test_platform_boundary.py.
"""
from __future__ import annotations

import ast
import posixpath
import re
from pathlib import Path

import _arch

PKG = "spark_quick_start"

# Layer 1 — pure data. These import no Spark engine and no adapter at all.
_CORE = {
    "orchestration/models.py",
    "orchestration/column_policy.py",
    "orchestration/metrics.py",
    "orchestration/validation.py",
    "orchestration/transformers/state.py",
}

_CONCRETE_ADAPTERS = (
    f"{PKG}.adapters.fabric_adapter",
    f"{PKG}.adapters.databricks_adapter",
    f"{PKG}.adapters.local_adapter",
)


def _under(*parts: str) -> list[Path]:
    """Every package file beneath SRC/<parts>."""
    prefix = _arch.SRC.joinpath(*parts)
    return [p for p in _arch.package_files() if prefix in p.parents]


def test_core_imports_no_adapter_at_all() -> None:
    """Layer 1 never reaches the abstraction layer. CLAUDE.md §2's ⛔ edge."""
    offenders = []
    for path in _arch.package_files():
        if _arch.rel(path).removeprefix("src/spark_quick_start/") not in _CORE:
            continue
        for name, node, _ in _arch.imports(path):
            if name.startswith(f"{PKG}.adapters"):
                offenders.append(f"{_arch.rel(path)}:{node.lineno} imports {name}")
    assert not offenders, (
        "pure-data modules import the adapter layer; dependencies point down and "
        f"inward only (rule: layering §1): {offenders}"
    )


def test_orchestration_imports_only_the_adapter_abc() -> None:
    """orchestration/ may import platform_adapter, never a concrete adapter."""
    offenders = []
    for path in _under("orchestration"):
        for name, node, module_scope in _arch.imports(path):
            if name.startswith(_CONCRETE_ADAPTERS) and module_scope:
                offenders.append(f"{_arch.rel(path)}:{node.lineno} imports {name}")
    assert not offenders, (
        "orchestration imports a concrete adapter at module scope; only "
        "adapters.platform_adapter (the ABC) is allowed (rule: layering §2). "
        f"Resolution goes through _PLATFORM_MAP + importlib: {offenders}"
    )


def test_adapters_do_not_import_orchestration_at_module_scope() -> None:
    """adapters/ never imports orchestration/ at module scope (rule: layering §6)."""
    offenders = []
    for path in _under("adapters"):
        for name, node, module_scope in _arch.imports(path):
            if name.startswith(f"{PKG}.orchestration") and module_scope:
                offenders.append(f"{_arch.rel(path)}:{node.lineno} imports {name}")
    assert not offenders, (
        "adapters imports orchestration at module scope. The one sanctioned case "
        "(MockPlatformAdapter._default_success) is function-local and commented: "
        f"{offenders}"
    )


def test_transformers_reach_delta_only_through_the_pipeline_seam() -> None:
    """Rule layering §3 — _pipeline.py is the single controlled coupling point."""
    seam = _arch.SRC / "orchestration" / "transformers" / "_pipeline.py"
    banned = (f"{PKG}.orchestration.load_pipeline", f"{PKG}.orchestration.delta_utils")
    offenders = []
    for path in _under("orchestration", "transformers"):
        if path == seam:
            continue
        for name, node, _ in _arch.imports(path):
            if name in banned:
                offenders.append(f"{_arch.rel(path)}:{node.lineno} imports {name}")
    assert not offenders, (
        "a transformer imports the Delta/SCD layer directly instead of through "
        f"transformers/_pipeline.py (rule: layering §3): {offenders}"
    )


def test_transformers_import_constants_only_through_the_constants_seam() -> None:
    """Constants comes from transformers/_constants.py, never orchestration.models."""
    seam = _arch.SRC / "orchestration" / "transformers" / "_constants.py"
    offenders = []
    for path in _under("orchestration", "transformers"):
        if path == seam:
            continue
        for node in ast.walk(_arch.tree(path)):
            is_from_models = (
                isinstance(node, ast.ImportFrom)
                and node.module == f"{PKG}.orchestration.models"
            )
            if is_from_models and any(a.name == "Constants" for a in node.names):
                offenders.append(f"{_arch.rel(path)}:{node.lineno}")
    assert not offenders, (
        "a transformer imports Constants from orchestration.models; it must come "
        f"from transformers/_constants.py (rule: layering §3): {offenders}"
    )


def test_the_pipeline_seam_only_re_exports() -> None:
    """_pipeline.py holds imports and __all__ — no definitions of its own."""
    seam = _arch.SRC / "orchestration" / "transformers" / "_pipeline.py"
    defined = [
        node.name
        for node in ast.iter_child_nodes(_arch.tree(seam))
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    ]
    assert not defined, (
        "transformers/_pipeline.py defines names instead of re-exporting them. "
        f"It is a seam, not an implementation home (rule: layering §3): {defined}"
    )


def test_the_validation_seam_holds_exactly_one_public_name() -> None:
    """_validation.py holds assert_no_dups and nothing else (rule: layering §4)."""
    seam = _arch.SRC / "orchestration" / "transformers" / "_validation.py"
    public = sorted(
        node.name
        for node in ast.iter_child_nodes(_arch.tree(seam))
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and not node.name.startswith("_")
    )
    assert public == ["assert_no_dups"], (
        "transformers/_validation.py must hold assert_no_dups and nothing else; "
        "schema comparison lives in orchestration/validation.py (rule: layering §4). "
        f"Found: {public}"
    )


def test_no_module_outside_adapters_imports_a_private_platform_adapter_name() -> None:
    """F6 — identifier routing belongs behind the adapter, not imported through it.

    `delta_utils.py` imported the private `_is_uc_name` from `adapters.platform_adapter`
    and made the routing decision itself. `platform-boundary.md` rule 6 puts that decision
    behind the adapter precisely so "callers pass an identifier and stay ignorant of which
    it is" — and a layer-3 module importing a layer-2 private symbol is the shape that
    turns the next routing change into a two-place edit.

    An underscore name is the module's own business. Importing one across a layer boundary
    is a declaration that the boundary's public surface was insufficient, which is a
    request for a new adapter method rather than a licence to reach past it — rule 3.

    Scoped to `platform_adapter` rather than all of `adapters/`, because that module is the
    ABC every layer above is allowed to import; the concrete adapters are already covered
    by `test_orchestration_never_imports_a_concrete_adapter`.
    """
    offenders = []
    for path in _arch.package_files():
        rel_path = _arch.rel(path)
        if "/adapters/" in rel_path:
            continue
        for node in ast.walk(_arch.tree(path)):
            if not isinstance(node, ast.ImportFrom) or not node.module:
                continue
            if not node.module.endswith("adapters.platform_adapter"):
                continue
            offenders.extend(
                f"{rel_path}:{node.lineno} imports {alias.name}"
                for alias in node.names
                if alias.name.startswith("_")
            )
    assert not offenders, (
        "a module outside adapters/ imports a private name from platform_adapter. "
        "Identifier routing and anything else behind that boundary is reached through a "
        f"public PlatformAdapter method (rules: platform-boundary §3,6): {offenders}"
    )


# --- rule layering §7: the DQD library is self-contained (F137) -------------------------------

_DQD = f"{PKG}.orchestration.dqd"
_DQD_DIR = "src/spark_quick_start/orchestration/dqd/"
_DQD_SEAM = "src/spark_quick_start/orchestration/transformers/_dqd.py"
# The standard-library modules the library may import: an allow-list, not "the standard
# library", because many stdlib modules import or run a name given as a string (pickle, pydoc,
# pkgutil, runpy, timeit, unittest.mock, logging.config...) and a text check cannot follow one.
_DQD_STDLIB = frozenset({"__future__", "collections", "collections.abc", "dataclasses", "datetime",
                         "decimal", "enum", "functools", "inspect", "itertools", "re", "typing"})
# pyspark subpackages that need third-party packages (pandas, pyarrow, grpc, numpy) at run time.
_DQD_PYSPARK_DENIED = ("pyspark.pandas", "pyspark.sql.connect", "pyspark.sql.pandas", "pyspark.ml",
                       "pyspark.mllib")
# Names that import, evaluate or reach the builtins by string. Refused as a bare name: an
# attribute of the same name (`re.compile`, a method `.eval`) is someone else's and is allowed.
_DQD_REFUSED_NAMES = frozenset({"__import__", "__builtins__", "__loader__", "__spec__", "eval", "exec",
                                "compile", "breakpoint", "globals", "vars", "get_type_hints"})
_DQD_REFUSED_ATTRIBUTES = frozenset({"__import__", "__builtins__", "__loader__", "__spec__", "__globals__",
                                     "__subclasses__", "get_type_hints", "get_annotations", "_evaluate",
                                     "import_module"})


def _allowed_in_dqd(module: str) -> bool:
    if module in _DQD_STDLIB or module == _DQD or module.startswith(_DQD + "."):
        return True
    return (module == "pyspark" or module.startswith("pyspark.")) and not any(
        module == d or module.startswith(d + ".") for d in _DQD_PYSPARK_DENIED)


def _parsed(rel: str, source: str) -> tuple[ast.AST | None, list[str]]:
    try:
        return ast.parse(source), []
    except (SyntaxError, ValueError) as exc:  # a null byte is a ValueError on Python 3.10
        return None, [f"{rel}: does not parse: {exc}"]


def _dqd_import_problems(rel: str, source: str) -> list[str]:
    """The ways a file of the DQD library can visibly reach beyond PySpark and a few stdlib modules.

    Every import is read, wherever it sits. A relative import may only stay in the package (level
    1; it has no subpackages). The standard library is an allow-list, and names that import,
    evaluate or reach the builtins by string are refused, whether used, imported by name or
    named as a parameter: the check does not track scopes, and the library has no reason to use
    one. Out of reach, and said so in rule 7: reflection built from strings through an allowed
    module, such as `getattr(re, "__builti" + "ns__")` or `F.sys.modules[...]`.
    """
    tree, problems = _parsed(rel, source)
    if tree is None:
        return problems
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            problems += [f"{rel}:{node.lineno} imports {a.name}" for a in node.names if not _allowed_in_dqd(a.name)]
        elif isinstance(node, ast.ImportFrom):
            refused = [a.name for a in node.names if a.name in _DQD_REFUSED_NAMES | _DQD_REFUSED_ATTRIBUTES]
            if refused:
                problems.append(f"{rel}:{node.lineno} imports {refused[0]}, which can import or run anything")
            if node.level > 1:
                problems.append(f"{rel}:{node.lineno} imports from {'.' * node.level}{node.module or ''}, "
                                "outside the package")
            elif node.level == 0 and not _allowed_in_dqd(node.module or ""):
                problems.append(f"{rel}:{node.lineno} imports from {node.module}")
        elif isinstance(node, ast.Name) and node.id in _DQD_REFUSED_NAMES:
            problems.append(f"{rel}:{node.lineno} uses {node.id}, which can import or run anything")
        elif isinstance(node, ast.arg) and node.arg in _DQD_REFUSED_NAMES:
            problems.append(f"{rel}:{node.lineno} names a parameter {node.arg}, a refused builtin")
        elif isinstance(node, ast.Attribute) and node.attr in _DQD_REFUSED_ATTRIBUTES:
            problems.append(f"{rel}:{node.lineno} uses .{node.attr}, which can import or run anything")
    return problems


def _absolute(rel: str, node: ast.ImportFrom) -> str:
    """The module a relative `from` import names, resolved against the file's package."""
    if node.level == 0:
        return node.module or ""
    parts = rel.removeprefix("src/").removesuffix(".py").split("/")
    package = parts[:-1]  # a module's package is its directory, for __init__.py too
    base = package[: len(package) - (node.level - 1)] if node.level > 1 else package
    return ".".join([*base, *([node.module] if node.module else [])])


def _names_dqd(module: str) -> bool:
    return module == _DQD or module.startswith(_DQD + ".")


def _dqd_consumer_problems(rel: str, source: str) -> list[str]:
    """A package module outside the library that reaches it, other than the transformer seam.

    Relative imports are resolved against the file's package; `from X import Y` counts `X.Y`.
    An attribute chain is resolved through the names the file's imports bind, so `o.dqd` after
    `import spark_quick_start.orchestration as o` is reported and `self.dqd` or
    `transformers.dqd` is not. A string naming the library (`importlib.import_module`,
    `sys.modules`) is reported too, so a docstring naming it is: say "the DQD library" instead.
    """
    rel = posixpath.normpath(rel)
    if rel == _DQD_SEAM or rel.startswith(_DQD_DIR):
        return []
    tree, problems = _parsed(rel, source)
    if tree is None:
        return problems
    bound: dict[str, str] = {}  # a name the file's imports bind -> the module it stands for
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                bound[a.asname or a.name.split(".")[0]] = a.name if a.asname else a.name.split(".")[0]
        elif isinstance(node, ast.ImportFrom):
            for a in node.names:
                bound[a.asname or a.name] = f"{_absolute(rel, node)}.{a.name}"
    for node in ast.walk(tree):
        names: list[str] = []
        if isinstance(node, ast.Import):
            names = [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            module = _absolute(rel, node)
            names = [module, *(f"{module}.{a.name}" for a in node.names)]
        elif isinstance(node, ast.Attribute) and node.attr == "dqd":
            chain, value = ["dqd"], node.value
            while isinstance(value, ast.Attribute):
                chain.insert(0, value.attr)
                value = value.value
            if isinstance(value, ast.Name) and value.id in bound \
                    and _names_dqd(".".join([bound[value.id], *chain])):
                problems.append(f"{rel}:{node.lineno} reaches the library through {value.id}")
        elif isinstance(node, ast.Constant) and isinstance(node.value, str) and (
                _DQD in node.value or re.fullmatch(r"\.+([A-Za-z_]\w*\.)*dqd(\..*)?", node.value)):
            problems.append(f"{rel}:{node.lineno} names the DQD library in a string")
        hits = [n for n in names if _names_dqd(n)]
        if hits:  # one report per statement: `from .dqd import x` names the library twice
            problems.append(f"{rel}:{node.lineno} imports {hits[0]}")
    return problems


def test_the_dqd_library_is_self_contained() -> None:
    """Rule layering §7 — the DQD library imports only PySpark, a few stdlib modules and itself,
    and inside the package only the transformer seam imports it."""
    library = _under("orchestration", "dqd")
    problems = [] if library else ["the DQD library (orchestration/dqd/) has no files"]
    for path in _arch.package_files():
        source = path.read_text(encoding="utf-8")
        problems += _dqd_import_problems(_arch.rel(path), source) if path in library \
            else _dqd_consumer_problems(_arch.rel(path), source)
    assert not problems, f"the DQD library is not self-contained (rule: layering §7): {problems}"


def test_every_dqd_import_check_can_fail() -> None:
    """Rule layering §7 — anti-vacuity: each form the F137 adversaries found is reported, and each
    valid form is not. Two valid forms are reported on purpose, as the docstrings say: a parameter
    named like a refused builtin, and a string naming the library outside it."""
    lib = f"{_DQD_DIR}x.py"
    library_forms = {
        "getattr on builtins": 'import builtins\ngetattr(builtins, "__import__")("delta")\n',
        "builtins.__dict__": 'import builtins\nbuiltins.__dict__["__imp" + "ort__"]("delta")\n',
        "operator.methodcaller": 'import operator\nimport builtins\n'
                                 'operator.methodcaller("__getattribute__", "__import__")(builtins)("delta")\n',
        "importlib through sys.modules": 'import sys\ngetattr(sys.modules["importlib"], "import_module")("delta")\n',
        "ctypes": "import ctypes\nctypes.pythonapi.PyImport_ImportModule(b'delta')\n",
        "pkgutil.resolve_name": 'import pkgutil\npkgutil.resolve_name("delta.tables:DeltaTable")\n',
        "pydoc.locate": 'import pydoc\npydoc.locate("spark_quick_start.adapters")\n',
        "unittest loader": 'import unittest\nunittest.TestLoader().loadTestsFromName("x")\n',
        "mock.patch": 'from unittest import mock\nmock.patch("delta.tables.DeltaTable").start()\n',
        "logging.config": 'import logging.config\nlogging.config.dictConfig({})\n',
        "pickle": 'import pickle\npickle.loads(b"")\n',
        "warnings": 'import warnings\nwarnings.warn("x")\n',
        "breakpoint": 'breakpoint()\n',
        "timeit": 'import timeit\ntimeit.timeit("import delta", number=1)\n',
        "code": 'import code\ncode.InteractiveInterpreter().runsource("import delta")\n',
        "cProfile": 'import cProfile\ncProfile.run("import delta")\n',
        "trace": 'import trace\ntrace.Trace().run("import delta")\n',
        "doctest": 'import doctest\ndoctest.run_docstring_examples(">>> import delta", {})\n',
        "get_type_hints": 'import typing\ndef f(x: "__imp__"): pass\ntyping.get_type_hints(f)\n',
        "runpy": 'import runpy\nrunpy.run_module("delta")\n',
        "sys.modules": 'import sys\nm = sys.modules["spark_quick_start.orchestration.models"]\n',
        "pyspark.pandas": "import pyspark.pandas as ps\n",
        "pyspark connect": "from pyspark.sql.connect.session import SparkSession\n",
        "a stdlib module added after 3.10": "import tomllib\n",
        "importlib.util": "import importlib.util\n",
        "import_module by name": "from importlib import import_module as im\n",
        "__import__ in a lambda": "f = lambda: __import__('delta')\n",
        "exec by attribute": "import builtins\nrun = builtins.exec\n",
        "a parent package, relatively": "from .. import models\n",
        "the package's own models": "from spark_quick_start.orchestration.models import *\n",
        "the package root": "import spark_quick_start\n",
        "a third-party package": "import delta\n",
        "__builtins__": "x = __builtins__\n",
        "globals": 'x = globals()["y"]\n',
        "vars": "x = vars()\n",
        "eval": 'x = eval("1")\n',
        "__loader__": "x = __loader__\n",
        "__subclasses__": "x = object.__subclasses__()\n",
        "get_type_hints imported by name": "from typing import get_type_hints as h\n",
        "inspect.get_annotations": "import inspect\ninspect.get_annotations(f, eval_str=True)\n",
        "ForwardRef._evaluate": 'import typing\ntyping.ForwardRef("x")._evaluate({}, {}, frozenset())\n',
    }
    # Each is reported once; "operator.methodcaller" imports two refused modules, and
    # `from importlib import import_module` is both a refused module and a refused name.
    expected = dict.fromkeys(library_forms, 1) | {"operator.methodcaller": 2, "import_module by name": 2}
    wrong = {label: _dqd_import_problems(lib, source) for label, source in library_forms.items()
             if len(_dqd_import_problems(lib, source)) != expected[label]}
    assert not wrong, f"these library forms were not reported as expected {expected}: {wrong}"
    assert _dqd_import_problems(lib, "x = 1\x00") and _dqd_import_problems(lib, "print 'x'")
    library_controls = {
        "re.compile": 'import re\nP = re.compile("x")\n',
        "a method named eval": "class A:\n    def eval(self):\n        pass\nA().eval()\n",
        "relative within the package": "from . import x as y\nfrom .foo import *\nfrom .a import b\n",
        "pyspark": "import pyspark.sql.functions as F\nfrom pyspark.sql import DataFrame\n",
        "typing only": "from __future__ import annotations\nfrom typing import TYPE_CHECKING\n"
                       "if TYPE_CHECKING:\n    from pyspark.sql import Column\n",
        "the library itself": "from spark_quick_start.orchestration.dqd.nodes import Literal\n",
    }
    noisy = {label: _dqd_import_problems(lib, s) for label, s in library_controls.items() if _dqd_import_problems(lib, s)}
    assert not noisy, f"these valid library forms were reported: {noisy}"
    assert _dqd_import_problems(lib, "def f(compile=None):\n    return compile\n"), "reported on purpose"

    orch, tx, ad = (f"src/spark_quick_start/{d}/foo.py" for d in
                    ("orchestration", "orchestration/transformers", "adapters"))
    consumer_forms = {
        "transformers: from ..dqd": (tx, "from ..dqd import x\n"),
        "transformers: from .. import dqd": (tx, "from .. import dqd\n"),
        "orchestration: from . import dqd": (orch, "from . import dqd\n"),
        "orchestration: from .dqd": (orch, "from .dqd import x\n"),
        "orchestration: from .dqd.sub": (orch, "from .dqd.sub import x\n"),
        "adapters: from ..orchestration.dqd": (ad, "from ..orchestration.dqd import x\n"),
        "adapters: from ..orchestration import dqd": (ad, "from ..orchestration import dqd\n"),
        "attribute through the root": (orch, "from spark_quick_start import orchestration\norchestration.dqd.x\n"),
        "attribute through an alias": (orch, "import spark_quick_start.orchestration as o\no.dqd.run()\n"),
        "import_module by name": (orch, "import importlib\nimportlib.import_module('spark_quick_start.orchestration.dqd')\n"),
        "import_module relatively": (orch, "import importlib\nimportlib.import_module('.dqd', __package__)\n"),
        "__import__": (orch, "__import__('spark_quick_start.orchestration.dqd')\n"),
        "exec": (orch, "exec('import spark_quick_start.orchestration.dqd')\n"),
        "sys.modules": (orch, "import sys\nsys.modules['spark_quick_start.orchestration.dqd']\n"),
        "a path through the library": (f"{_DQD_DIR}../models.py", "from spark_quick_start.orchestration.dqd import x\n"),
        "import as": (orch, "import spark_quick_start.orchestration.dqd as x\n"),
        "from the parent": (orch, "from spark_quick_start.orchestration import (models, dqd)\n"),
        "star": (orch, "from spark_quick_start.orchestration.dqd import *\n"),
        "in a function": (orch, "def f():\n    import spark_quick_start.orchestration.dqd.parse\n"),
    }
    wrong = {label: _dqd_consumer_problems(*case) for label, case in consumer_forms.items()
             if len(_dqd_consumer_problems(*case)) != 1}
    assert not wrong, f"these consumer forms were not reported exactly once: {wrong}"
    consumer_controls = {
        "a similarly named module": (orch, ("import spark_quick_start.orchestration.dqd_helpers\n"
                                            "from spark_quick_start.orchestration import dqd_helpers\n")),
        "another package's dqd": (orch, "import dqd\nfrom other.orchestration.dqd import x\n"),
        "an alias named dqd": (orch, "import spark_quick_start.orchestration.models as dqd\ndqd.Constants\n"),
        "the seam": (_DQD_SEAM, "from spark_quick_start.orchestration.dqd import parse_expression\n"),
        "the library": (f"{_DQD_DIR}parse.py", "from spark_quick_start.orchestration.dqd.nodes import Literal\n"),
        "an attribute named dqd": (orch, "class A:\n    def f(self):\n        return self.dqd\n"),
        "a transformer module named dqd": (tx, ("from spark_quick_start.orchestration import transformers\n"
                                                "transformers.dqd.DQDTransformer\n")),
    }
    noisy = {label: _dqd_consumer_problems(*case) for label, case in consumer_controls.items()
             if _dqd_consumer_problems(*case)}
    assert not noisy, f"these valid consumer forms were reported: {noisy}"
    assert _dqd_consumer_problems(orch, '"""See spark_quick_start.orchestration.dqd."""\n'), "reported on purpose"
