"""
test_test_classification.py — executable form of .claude/rules/test-classification.md.

Contains: marker-hygiene, suite-membership and path-resolution assertions.
Does NOT contain: the one-SparkSession invariant — that stays in
    tests/unit/test_suite_invariants.py, where it has run since the incident.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

import _arch
import pytest

_TESTS = _arch.ROOT / "tests"
_SUITES = {"unit", "integration", "architecture"}

_HAND_APPLIED = re.compile(r"@pytest\.mark\.(unit|integration|architecture)\b")


def _test_modules() -> list[Path]:
    return sorted(p for p in _TESTS.rglob("test_*.py") if "__pycache__" not in p.parts)


def test_no_scope_marker_is_hand_applied() -> None:
    """A hand-applied marker drifts, and then -m unit and pytest tests/unit disagree."""
    offenders = [
        f"{_arch.rel(path)}:{i}"
        for path in _test_modules()
        for i, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1)
        if _HAND_APPLIED.search(line)
    ]
    assert not offenders, (
        "a scope marker is applied by hand. tests/conftest.py stamps unit / "
        "integration / architecture from the directory so the two cannot disagree "
        f"(rule: test-classification): {offenders}"
    )


def test_every_test_module_lives_in_a_known_suite() -> None:
    """Scope is decided by the directory, so every test file must be in one."""
    strays = [
        _arch.rel(path)
        for path in _test_modules()
        if not _SUITES & set(path.relative_to(_TESTS).parts)
    ]
    assert not strays, (
        "a test module sits outside tests/unit, tests/integration and "
        f"tests/architecture, so no scope marker is stamped for it: {strays}"
    )


_PATH_CALLS = {"Path", "PurePath", "PosixPath", "WindowsPath", "PurePosixPath", "PureWindowsPath",
               "abspath", "realpath", "normpath", "str", "fspath"}
_SAME_PATH_METHODS = {"resolve", "absolute", "expanduser"}
# A level count no reader can fix: a variable, negative or computed index, or a slice. It is
# reported, as two or more levels would be.
_UNKNOWN = 2


def _name(func: ast.expr) -> str | None:
    return func.id if isinstance(func, ast.Name) else func.attr if isinstance(func, ast.Attribute) else None


def _dot_dots(nodes: list[ast.expr]) -> int:
    """The `..` segments among *nodes*, as path operands: in string constants, and `os.pardir`."""
    pardirs = sum(1 for n in nodes if _name(n) == "pardir")
    return pardirs + sum(part == ".." for n in nodes if isinstance(n, ast.Constant) and isinstance(n.value, str)
                         for part in n.value.replace("\\", "/").split("/"))


def _levels(node: ast.expr, names: dict[str, int] | None = None) -> int | None:
    """How many directory levels *node* climbs from this module's own `__file__`, or None.

    Only a climb rooted on the bare name `__file__` counts: that is the path that moves when
    the test does. `.parent` is one level, `.parents[k]` is k + 1, a nested `dirname` is one,
    and a `..` segment or `os.pardir` in an operand of `/`, `joinpath` or a `join` is one.
    *names* maps a variable assigned such a path to its count, one assignment deep, so
    `HERE = Path(__file__).parent` then `HERE.parent` is two levels (F181 review: `main`'s
    check caught `HERE.parent.parent`, and anchoring on `__file__` alone had dropped it).
    """
    names = names or {}
    if isinstance(node, ast.Name):
        return 0 if node.id == "__file__" else names.get(node.id)
    if isinstance(node, ast.Attribute) and node.attr == "parent":
        inner = _levels(node.value, names)
        return None if inner is None else inner + 1
    if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Attribute) and node.value.attr == "parents":
        inner = _levels(node.value.value, names)
        if inner is None:
            return None
        index = node.slice
        if isinstance(index, ast.Constant) and type(index.value) is int and index.value >= 0:
            return inner + index.value + 1
        return inner + _UNKNOWN
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
        inner = _levels(node.left, names)
        return None if inner is None else inner + _dot_dots([node.right])
    if isinstance(node, ast.Call):
        name = _name(node.func)
        if name in _SAME_PATH_METHODS and isinstance(node.func, ast.Attribute):
            return _levels(node.func.value, names)
        if name == "joinpath" and isinstance(node.func, ast.Attribute):
            inner = _levels(node.func.value, names)
            return None if inner is None else inner + _dot_dots(node.args)
        if name == "join" and len(node.args) == 1 and isinstance(node.args[0], (ast.List, ast.Tuple)):
            parts = node.args[0].elts  # os.sep.join([dirname(__file__), "..", ".."])
            inner = _levels(parts[0], names) if parts else None
            return None if inner is None else inner + _dot_dots(parts[1:])
        if node.args and name in (_PATH_CALLS | {"dirname", "join"}):
            inner = _levels(node.args[0], names)
            if inner is None:
                return None
            return inner + (name == "dirname") + _dot_dots(node.args[1:])
    return None


def _parent_counts(tree: ast.AST) -> list[int]:
    """Line numbers where *tree* climbs two or more levels from `__file__` by counting.

    Scans the AST rather than the source text. A line-based regex was tried first and
    rejected: it flagged a comment in tests/unit/test_version.py that *names* the banned
    pattern to explain why that file walks up to a marker instead — a good comment,
    punished by the checker. Comments are not nodes in the AST at all, and a pattern
    written inside a string literal is a single ``ast.Constant``, so both false-positive
    classes disappear without an exclusion list.

    One expression is one report, at its outermost climbing node; a variable that holds a
    climb is followed one assignment deep, and is reported where it climbs, not where it is
    merely used. Out of reach, and left to review (F181's adversary): a variable reassigned
    to itself (``p = p.parent``), a climb through a loop, a lambda or ``getattr``, and
    slicing ``__file__`` as a string.
    """
    names = {target.id: count
             for node in ast.walk(tree) if isinstance(node, ast.Assign)
             for target in node.targets if isinstance(target, ast.Name)
             if (count := _levels(node.value)) is not None}
    flagged = [n for n in ast.walk(tree)
               if isinstance(n, ast.expr) and not isinstance(n, ast.Name) and (_levels(n, names) or 0) >= 2]
    inner = {id(c) for n in flagged for c in ast.walk(n) if c is not n}
    return sorted(n.lineno for n in flagged if id(n) not in inner)


def _python_modules() -> list[Path]:
    """Every Python file under tests/: helpers and conftest.py, not only collected modules."""
    return sorted(p for p in _TESTS.rglob("*.py") if "__pycache__" not in p.parts)


def test_nothing_locates_a_repo_file_by_counting_parents() -> None:
    """Walk up to the marker file instead — the split broke the one place that counted."""
    offenders = [
        f"{_arch.rel(path)}:{line}"
        for path in _python_modules()
        for line in _parent_counts(_arch.tree(path))
    ]
    assert not offenders, (
        "a test locates a repo file by counting levels up from __file__. The unit/integration "
        "split moved every test one level deeper and broke exactly this; walk up to "
        f"pyproject.toml instead, as tests/architecture/_arch.py does: {offenders}"
    )


# Each form climbs two or more levels from `__file__`; the check must report it once.
_PARENT_MUTATIONS = [
    "Path(__file__).parent.parent",
    "Path(__file__).parent.parent.parent",
    "Path(__file__).resolve().parents[2]",
    "Path(__file__).parents[1] / 'src'",
    "Path(__file__).parents[-1]",
    "Path(__file__).parents[1:]",
    "Path(__file__).parents[N]",
    "PurePath(__file__).parents[2]",
    "Path(__file__).parent.parents[0]",
    "Path(__file__).parents[0].parent",
    "os.path.dirname(os.path.dirname(__file__))",
    "os.path.dirname(os.path.dirname(os.path.abspath(__file__)))",
    "dirname(dirname(__file__))",
    "Path(__file__).parent / '..' / 'src'",
    "Path(__file__).parent.joinpath('..', '..')",
    "(Path(__file__) / '../../src').resolve()",
    "os.path.join(os.path.dirname(__file__), '..', '..')",
    "os.path.join(os.path.dirname(__file__), '../..')",
    "os.path.normpath(os.path.join(__file__, '../../..'))",
    "sys.path.insert(0, str(Path(__file__).parent / '..' / '..' / 'src'))",
    # F181 review:
    "HERE = Path(__file__).resolve().parent\nROOT = HERE.parent.parent",
    "HERE = Path(__file__).parent\nROOT = HERE.parent",
    "os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)",
    "os.sep.join([os.path.dirname(__file__), '..', '..'])",
]

# Each form is sanctioned: one level, the walk-up loop, the pattern named in text, or a
# climb that does not start from this module's own file.
_PARENT_CONTROLS = [
    "Path(__file__).parent / 'data'",
    "Path(__file__).parents[0]",
    "os.path.dirname(__file__)",
    "Path(__file__).parent / 'fixtures' / 'a.json'",
    "for candidate in (here, *here.parents):\n    pass",
    "next(p for p in Path(__file__).parents if (p / 'pyproject.toml').is_file())",
    "x = '.parent.parent'",
    "x = 'Path(__file__).parents[2]'",
    "# Path(__file__).parents[2] is banned\nx = 1",
    "(tmp_path / 'a' / 'b').parent.parent",
    "(tmp_path / 'a' / 'b').parents[1]",
    "node.parent.parent",
    "Path(sys.modules['pkg'].__file__).resolve().parents[1]",
    "link = '../guide.md'",
    "here = Path(__file__).resolve()\nnext(p for p in here.parents if (p / 'pyproject.toml').is_file())",
    "HERE = Path(__file__).parent\nconfig = HERE / 'config.json'",
    "ROOT = Path(__file__).parent\nprint(ROOT)",
]


@pytest.mark.parametrize("source", _PARENT_MUTATIONS)
def test_the_parent_count_check_reports_each_counting_form(source: str) -> None:
    """F181 — `parents[N]` counts levels exactly as `.parent.parent` does."""
    assert len(_parent_counts(ast.parse(source))) == 1, source


@pytest.mark.parametrize("source", _PARENT_CONTROLS)
def test_the_parent_count_check_passes_each_sanctioned_form(source: str) -> None:
    assert _parent_counts(ast.parse(source)) == [], source
