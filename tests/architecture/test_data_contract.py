"""
test_data_contract.py — executable form of .claude/rules/data-contract.md.

Contains: single-definition assertions for the routing columns and the frames key.
Does NOT contain: the transformer lifecycle — that is test_transformer_contract.py.
"""
from __future__ import annotations

import ast

import _arch
import pytest

from spark_quick_start.orchestration.transformers._constants import (
    CHANGE_TYPE,
    INTERNAL_COLUMNS,
    PREV_VERSION,
    Constants,
)

_TRANSFORMERS = _arch.SRC / "orchestration" / "transformers"
_CONSTANTS = _TRANSFORMERS / "_constants.py"


def _docstring_constants(tree: ast.AST) -> set[int]:
    """The `ast.Constant` nodes that are docstrings, by identity.

    A docstring is the first statement of a module, class or function when that
    statement is a bare string expression. Identity rather than value, because two
    distinct nodes can hold equal strings.
    """
    found: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        if not node.body or not isinstance(node.body[0], ast.Expr):
            continue
        value = node.body[0].value
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            found.add(id(value))
    return found


def _modules_containing(literal: str) -> list[str]:
    """Every transformer module whose *code* uses *literal* as a string constant.

    An AST walk, not a source-text regex, per `.claude/standards/testing.md`: "when
    the thing being checked is Python, scan the AST, not the source text… a rule's
    enforcement must not punish the prose that explains the rule." The original
    version of this helper matched a quoted occurrence anywhere in the file,
    docstrings and comments included, which made it strictly a false-positive
    generator in this package — `target.py` alone writes ``frames["target"]`` six
    times in prose, so the habit is established. The first docstring explaining
    `_drop_internal()` with ``df.drop("_change_type", "_prev_scdVersion")`` would
    have turned both assertions below permanently red, and the cheapest way out of
    a permanently red assertion is deleting the explanation.

    Comments are invisible to an AST walk, which is the point. Docstrings are not,
    so they are excluded explicitly.

    Stricter than the sibling `test_the_target_frames_key_has_a_single_source`'s
    three-shape walk in the direction that mattered for F2: any string constant
    counts, including a **default argument value**, which is exactly the fourth home
    that F2's review found after its own author had missed it.
    """
    holders = []
    for path in sorted(_TRANSFORMERS.glob("*.py")):
        tree = _arch.tree(path)
        docstrings = _docstring_constants(tree)
        if any(
            isinstance(node, ast.Constant)
            and node.value == literal
            and id(node) not in docstrings
            for node in ast.walk(tree)
        ):
            holders.append(_arch.rel(path))
    return sorted(holders)


def test_change_type_literal_is_defined_once() -> None:
    """The routing column's name lives in _constants.py and nowhere else."""
    holders = _modules_containing("_change_type")
    assert holders == [_arch.rel(_CONSTANTS)], (
        "the _change_type literal appears outside transformers/_constants.py. A rename "
        "in one module produces a silently empty filter downstream, not an error "
        f"(rule: data-contract): {holders}"
    )


def test_prev_version_literal_is_defined_once() -> None:
    """Same contract, same single home."""
    holders = _modules_containing("_prev_scdVersion")
    assert holders == [_arch.rel(_CONSTANTS)], (
        "the _prev_scdVersion literal appears outside transformers/_constants.py "
        f"(rule: data-contract): {holders}"
    )


def _is_target_string(node: ast.expr | None) -> bool:
    """True if *node* is the string constant ``"target"``."""
    return isinstance(node, ast.Constant) and node.value == "target"


def _uses_target_frames_literal(module: ast.Module) -> bool:
    """True if *module*'s AST hardcodes the frames key ``"target"`` in code.

    Matches exactly the three shapes the frames key is actually used in:
    ``*.frames["target"]``, ``*.frames.get("target")``, and
    ``*.with_frame("target", ...)``. A bare string constant anywhere else —
    a docstring, a comment-adjacent prose sentence, or the
    ``target_frame_key: str = "target"`` parameter default — does not count.
    The default is deliberately excluded: once F2 is fixed it becomes a
    reference to a named constant rather than a literal, and counting it here
    would just move the false-alarm problem instead of removing it.
    """
    for node in ast.walk(module):
        if isinstance(node, ast.Subscript):
            if isinstance(node.value, ast.Attribute) and node.value.attr == "frames":
                if _is_target_string(node.slice):
                    return True
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            is_frames_get = node.func.attr == "get" and (
                isinstance(node.func.value, ast.Attribute) and node.func.value.attr == "frames"
            )
            is_with_frame = node.func.attr == "with_frame"
            if is_frames_get or is_with_frame:
                if node.args and _is_target_string(node.args[0]):
                    return True
    return False


def test_the_target_frames_key_has_a_single_source() -> None:
    """Configurable on all sides or on none.

    Resolved to *none* (F2, landed 2026-09-26). ChangeDetectorTransformer (now
    SCDChangeDetectorTransformer, F155) took a `target_frame_key` parameter while
    NativeSCDMergeTransformer and TargetTableTransformer (now SCDTargetTableTransformer)
    baked the literal in, and no caller anywhere set it.
    Setting it left the detector reading an absent frame, which
    `change_detector.py` treats as a first load — so every row was classified
    `new` at version 1, with an INFO line stating that as fact. The key now lives
    once, as `Constants.TARGET_FRAME_KEY`, and the parameter is gone.

    Scans the AST rather than the source text. A source-text regex for
    ``frames["target"]``/``frames.get("target")``/``with_frame("target", ...)``
    also matches those same shapes inside a docstring — this codebase's own
    docstrings describe the mechanism using that exact syntax (e.g.
    ``change_detector.py``: "state.frames[\"target\"]"). Once F2 is fixed the
    code-level literal goes away but the prose describing it may not, so a
    text-based scan would keep this test red forever — the permanent
    false-alarm failure mode. An AST walk that only matches a real
    ``ast.Subscript``/``ast.Call`` node cannot see a docstring at all: a
    docstring is a single ``ast.Constant`` whose value is the whole string,
    never a node the parser produces for the code shapes below.
    """
    holders = sorted(
        _arch.rel(path)
        for path in sorted(_TRANSFORMERS.glob("*.py"))
        if _uses_target_frames_literal(_arch.tree(path))
    )
    # Anti-vacuity: the scan looks for modules that do NOT use the constant, so
    # deleting the constant would make it trivially pass while every call site
    # broke. Assert the single home exists before asserting nothing bypasses it.
    assert isinstance(Constants.TARGET_FRAME_KEY, str) and Constants.TARGET_FRAME_KEY, (
        "Constants.TARGET_FRAME_KEY is the single home this assertion is about; "
        "without it there is nothing for the call sites to agree on"
    )
    modules = sorted(_TRANSFORMERS.glob("*.py"))
    assert len(modules) >= 10, (
        f"the scan reached only {len(modules)} transformer modules; a renamed package "
        "directory makes glob return nothing silently"
    )

    # Zero, not "at most one". `<= 1` was calibrated against the pre-fix world,
    # where native_scd.py and target.py both held the literal so len == 2 failed
    # and the strict xfail passed. Post-fix the correct state is none, and this is
    # now the only guard — the armed xfail that was the other one is gone, so `<= 1`
    # would silently readmit exactly one reintroduced literal, desynchronised from
    # the constant and failing as an absent frame read as a first load.
    assert holders == [], (
        "a transformer names the 'target' frames key as a literal instead of "
        f"importing Constants.TARGET_FRAME_KEY, which is its single home: {holders}"
    )


def test_the_routing_column_names_have_not_drifted() -> None:
    """F3's one real risk: the move changes a value instead of relocating it.

    The two assertions above check *where* the names are defined. Neither checks
    *what* they are, so a typo introduced while relocating them would satisfy both
    — one home, wrong string — and the failure would surface as an empty filter
    rather than an error: `_drop_internal()` would stop stripping the column, or
    the merger would find no close-out target row.

    Written as literals on purpose. Comparing the constant to itself, or to
    anything imported from the same module, would assert nothing. These two strings
    are also the keys that eleven assertions in
    `tests/unit/test_change_detector_transformer.py` read off result rows as
    hardcoded dict keys — an independent oracle that predates this fix and would
    fail loudly on drift. This assertion is the cheap, fast half of that pair.
    """
    assert CHANGE_TYPE == "_change_type"
    assert PREV_VERSION == "_prev_scdVersion"
    assert frozenset({"_change_type", "_prev_scdVersion"}) == INTERNAL_COLUMNS


_ERROR_KEY = "error_type"
_READERS = {"get", "pop"}


def _error_shape_sites(tree: ast.AST) -> list[int]:
    """Lines where *tree* writes the `error_details` shape's `error_type` key by hand.

    The key is the shape's signature: every hand-built variant names it, with or without
    `traceback` (F114 adversary: a dict that drops `traceback` was the drift the single
    source exists to stop). Outside the `ErrorDetails` class body the string may only be
    *read* — `d["error_type"]`, `d.get(...)`, `d.pop(...)`, `"error_type" in d` — or sit in
    a docstring; any other appearance is a write: a dict key, a comprehension pair, a
    subscript store, `.update`/`.setdefault`, a constant a key is later taken from. So is an
    `error_type=` keyword to anything but `ErrorDetails`, and an `error_type` field on any
    other class. Out of reach, left to review: a key spelt by an f-string or a `+`, and the
    shape written inside a JSON string. A mapping that only names the key, such as a schema
    or a rename table, is reported too; none exists, and one would need a deliberate line.
    """
    exempt: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == "ErrorDetails":
            exempt |= {id(n) for n in ast.walk(node)}
    parents = {id(c): n for n in ast.walk(tree) for c in ast.iter_child_nodes(n)}
    sites: list[int] = []
    for node in ast.walk(tree):
        if id(node) in exempt:
            continue
        if isinstance(node, ast.Constant) and node.value == _ERROR_KEY:
            parent = parents.get(id(node))
            read = (
                isinstance(parent, ast.Expr)  # a docstring or a bare string statement
                or (isinstance(parent, ast.Subscript) and parent.slice is node
                    and isinstance(parent.ctx, ast.Load))
                or (isinstance(parent, ast.Call) and isinstance(parent.func, ast.Attribute)
                    and parent.func.attr in _READERS and parent.args[:1] == [node])
                or (isinstance(parent, ast.Compare) and parent.left is node
                    and all(isinstance(op, (ast.In, ast.NotIn)) for op in parent.ops))
            )
            if not read:
                sites.append(node.lineno)
        elif isinstance(node, ast.Call) and any(k.arg == _ERROR_KEY for k in node.keywords):
            name = node.func.attr if isinstance(node.func, ast.Attribute) else getattr(node.func, "id", None)
            owner = node.func.value if isinstance(node.func, ast.Attribute) else None
            if name != "ErrorDetails" and getattr(owner, "id", None) != "ErrorDetails":
                sites.append(node.lineno)
        elif isinstance(node, ast.AnnAssign) and getattr(node.target, "id", None) == _ERROR_KEY:
            sites.append(node.lineno)
    return sorted(set(sites))


def test_error_details_dicts_are_built_only_by_error_details() -> None:
    """F114 — the `error_details` shape has one source, `ErrorDetails` in
    `orchestration/models.py`; nothing else under src/ writes its `error_type` key."""
    holders = [f"{_arch.rel(path)}:{line}"
               for path in _arch.package_files() for line in _error_shape_sites(_arch.tree(path))]
    assert holders == [], (
        f"these build an error_details dict by hand instead of ErrorDetails(...).to_dict(): {holders}")


# F114's adversary: each writes the shape by hand and must be reported.
_ERROR_SHAPE_MUTATIONS = [
    'd = {"error_type": t, "error_message": m, "traceback": tb}',
    'd = {"error_type": t, "error_message": m}',
    "d = dict(error_type=t, error_message=m, traceback=tb)",
    'd = {k: v for k, v in (("error_type", t), ("traceback", tb))}',
    'base = {"error_type": t}\nd = {**base, "traceback": tb}',
    'ET = "error_type"\nd = {ET: t}',
    'd = {"error_message": m}\nd["error_type"] = t',
    "d = {}\nd.update(error_type=t)",
    'd = {}\nd.setdefault("error_type", t)',
    'd = dict(zip(("error_type", "error_message"), (t, m)))',
    'd = dict.fromkeys(["error_type", "traceback"])',
    "class ED(TypedDict):\n    error_type: str",
    'class ExecutionResult:\n    def f(self):\n        return {"error_type": 1}',
]

# Reads, prose and the class itself are not writes.
_ERROR_SHAPE_CONTROLS = [
    'x = d["error_type"]',
    'x = d.get("error_type")',
    'x = d.pop("error_type", None)',
    'ok = "error_type" in d',
    'def f():\n    """Returns a dict with "error_type" and "traceback"."""',
    "# {'error_type': ...}\nx = 1",
    'class ErrorDetails:\n    error_type: str\n    def to_dict(self):\n        return {"error_type": self.error_type}',
    "d = ErrorDetails(error_type=t, error_message=m, traceback=tb).to_dict()",
    "d = ErrorDetails.from_exception(exc).to_dict()",
    'x = {"type": t, "message": m, "traceback": tb}',
]


@pytest.mark.parametrize("source", _ERROR_SHAPE_MUTATIONS)
def test_the_error_shape_check_reports_each_hand_built_form(source: str) -> None:
    assert _error_shape_sites(ast.parse(source)), source


@pytest.mark.parametrize("source", _ERROR_SHAPE_CONTROLS)
def test_the_error_shape_check_passes_reads_and_the_class(source: str) -> None:
    assert _error_shape_sites(ast.parse(source)) == [], source
