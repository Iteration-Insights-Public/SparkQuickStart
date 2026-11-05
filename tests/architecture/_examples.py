"""
_examples.py — the Python and load-context JSON examples in README.md and docs/, checked
against the code.

Contains: `python_problems` (every call it can resolve, bound against the real signature, and
    every library name it can check), `context_problems` (every load-context JSON example held
    to the keys the library reads), and `context_schema` (those keys, recorded while the
    library's own parser reads a load context).
Does NOT contain: the pinned exemptions or any assertion; test_documentation.py owns both.

**Python.** A page's Python blocks are one script, read top to bottom (the writing-guides
skill, F63). A call is resolved when its target is a name imported from spark_quick_start (a
function, or a class through its constructor), an attribute of one, or a method on a value
whose class is known: a variable assigned from a constructor, or what a resolved call is
annotated to return, such as `.add()` returning the orchestrator. Such a call is bound with
`inspect.signature(...).bind`, so an unknown keyword, a missing required argument or too many
positionals is a problem. So is an import of a name the library lacks, a missing attribute of
a library module or class, a misspelt package name, and instantiating a Protocol or an
abstract class. A call it cannot resolve is counted, not failed (the owner's decision,
2026-09-28).

Names are tracked without scopes, so a name is trusted only while nothing rebinds it in a way
this reader does not follow: a name bound anywhere except a top-level import or a top-level
`name = …` (a loop or tuple target, a walrus, a comprehension, a parameter, `except … as`, a
match capture, or any binding inside a function or class) is unknown on the whole page. That
one rule replaced a list of forms the first version got wrong (F65 adversary).

What it does not check: argument types, whether the example runs, attributes of instances
(they may be set at runtime), calls on anything it cannot type, `super()`, and subclasses'
constructors.
"""
from __future__ import annotations

import ast
import builtins
import difflib
import functools
import importlib
import inspect
import json
import re
import subprocess
import sys
import types
import typing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest import mock

import _docs

_PACKAGE = "spark_quick_start"
PYTHON = frozenset({"python", "py", "python3"})
JSON = frozenset({"json", "jsonc", "json5"})
_BODIES = ("body", "orelse", "finalbody", "handlers", "cases")
_MAGIC = re.compile(r"^[ \t]*[%!]")  # %pip, !ls: valid in a Fabric or Databricks cell


def language(raw: str) -> str:
    """The language a fence's first info word names: `json,linenums` and `{.json}` are json."""
    match = re.match(r"[{.]*([a-z0-9_+-]*?[a-z0-9])(?:[^a-z0-9]|$)", raw)
    return match.group(1) if match else ""


@dataclass
class Report:
    """What one page's examples showed: problems as (line, message), and the counts."""

    problems: list[tuple[int, str]] = field(default_factory=list)
    resolved: int = 0
    unresolved: int = 0
    unchecked_json: int = 0
    unparsed: int = 0  # blocks that did not parse, so nothing in them was checked
    unbound: set[str] = field(default_factory=set)  # names called or dereferenced, but bound nowhere on the page


@dataclass(frozen=True)
class _Instance:
    cls: type


@dataclass(frozen=True)
class _Method:
    owner: type
    name: str
    on_instance: bool


_UNKNOWN = object()
_FOREIGN = object()  # imported from outside the library, or a builtin: not the library's API
_SELF = getattr(typing, "Self", None)  # 3.11+


@functools.cache
def _type_checking_names(module_name: str) -> dict[str, Any]:
    """The names a module imports under `if TYPE_CHECKING:`, imported now, so its string
    return annotations can be read (F65 adversary: get_type_hints failed on every function
    with one such parameter)."""
    module = sys.modules.get(module_name)
    try:
        source = inspect.getsource(module) if module is not None else ""
    except (OSError, TypeError):
        return {}
    names: dict[str, Any] = {}
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.If) and "TYPE_CHECKING" in ast.unparse(node.test):
            for child in node.body:
                if isinstance(child, ast.ImportFrom) and child.module and child.level == 0:
                    for alias in child.names:
                        try:
                            names[alias.asname or alias.name] = getattr(importlib.import_module(child.module),
                                                                        alias.name)
                        except (ImportError, AttributeError):
                            continue
    return names


def _return_type(fn: Any, owner: type | None) -> Any:
    """What a call to *fn* returns, as this reader tracks values: a library class's instance, or unknown."""
    try:
        ret = inspect.get_annotations(fn).get("return")
    except (TypeError, ValueError):
        return _UNKNOWN
    if isinstance(ret, str):
        # Only the return annotation is resolved, through typing's own reader, against the
        # function's globals plus its module's TYPE_CHECKING imports.
        namespace = {**_type_checking_names(getattr(fn, "__module__", "") or ""), **getattr(fn, "__globals__", {})}

        def stand_in() -> None: ...

        stand_in.__annotations__ = {"return": ret}
        try:
            ret = typing.get_type_hints(stand_in, globalns=namespace).get("return")
        except Exception:
            return _UNKNOWN
    if _SELF is not None and ret is _SELF:
        return _Instance(owner) if owner is not None else _UNKNOWN
    if isinstance(ret, type) and ret.__module__.startswith(_PACKAGE):
        return _Instance(ret)
    return _UNKNOWN


@functools.cache
def _bound_at_runtime(imports: tuple[str, ...], module: str, attr: str) -> bool:
    """Whether `module.attr` exists after running *imports* in a fresh interpreter.

    A submodule is an attribute of its package only once something imports it, and this
    process has imported everything already (F65 adversary: `spark_quick_start.spark_logging`
    passed once any page had imported it, and raises in a fresh notebook).
    """
    src = str(Path(sys.modules[_PACKAGE].__file__).resolve().parents[1])
    code = "\n".join([f"import sys; sys.path.insert(0, {src!r})", *imports, f"import {module} as _m",
                      f"print(hasattr(_m, {attr!r}))"])
    done = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=120)
    if done.returncode != 0:
        # The environment failed, not the example: say so, rather than blame the page.
        raise RuntimeError(f"could not check {module}.{attr} in a fresh interpreter: {done.stderr[-2000:]}")
    return done.stdout.strip().endswith("True")


def _unstable_names(tree: ast.Module) -> set[str]:
    """Every name bound anywhere except by a top-level import or a top-level `name = …`."""
    trusted = {id(t) for node in tree.body if isinstance(node, (ast.Assign, ast.AnnAssign))
               for t in (node.targets if isinstance(node, ast.Assign) else [node.target]) if isinstance(t, ast.Name)}
    top = {id(node) for node in tree.body}
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Del)) and id(node) not in trusted:
            names.add(node.id)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            a = node.args
            names.update(arg.arg for arg in [*a.posonlyargs, *a.args, *a.kwonlyargs, a.vararg, a.kwarg] if arg)
            if not isinstance(node, ast.Lambda) and id(node) not in top:
                names.add(node.name)
        elif isinstance(node, ast.ClassDef) and id(node) not in top:
            names.add(node.name)
        elif isinstance(node, (ast.Import, ast.ImportFrom)) and id(node) not in top:
            names.update((a.asname or a.name).split(".")[0] for a in node.names)
        elif isinstance(node, (ast.ExceptHandler, ast.MatchAs, ast.MatchStar)) and node.name:
            names.add(node.name)
        elif isinstance(node, ast.MatchMapping) and node.rest:
            names.add(node.rest)
        elif isinstance(node, (ast.Global, ast.Nonlocal)):
            names.update(node.names)
    return names


class _Page:
    def __init__(self, report: Report) -> None:
        self.env: dict[str, Any] = {}
        self.unstable: set[str] = set()
        # Only top-level imports are replayed in the fresh interpreter: an import inside a
        # function that is never called binds nothing (F65 review).
        self.imports: list[str] = []
        self.top: set[int] = set()
        self.report = report
        self.seen: dict[int, Any] = {}
        self.trees: list[ast.Module] = []
        self.offset = 0

    def problem(self, node: ast.AST, message: str) -> None:
        self.report.problems.append((self.offset + getattr(node, "lineno", 1) - 1, message))

    # -- values -----------------------------------------------------------------------
    def value(self, node: ast.expr) -> Any:
        if isinstance(node, ast.Name):
            if node.id in self.unstable or node.id == "super":
                return _UNKNOWN
            if node.id in self.env:
                return self.env[node.id]
            if hasattr(builtins, node.id):
                return _FOREIGN
            self.report.unbound.add(node.id)
            return _UNKNOWN
        if isinstance(node, ast.Attribute):
            return self.attribute(node)
        if isinstance(node, ast.Call):
            return self.call(node)
        return _UNKNOWN

    def attribute(self, node: ast.Attribute) -> Any:
        if id(node) in self.seen:
            return self.seen[id(node)]
        result = self._attribute(node)
        self.seen[id(node)] = result
        return result

    def _attribute(self, node: ast.Attribute) -> Any:
        base = self.value(node.value)
        if base is _FOREIGN or base is _UNKNOWN:
            return base
        if inspect.ismodule(base):
            found = getattr(base, node.attr, None)
            if found is None or (inspect.ismodule(found) and found.__name__ == f"{base.__name__}.{node.attr}"
                                 and not _bound_at_runtime(tuple(self.imports), base.__name__, node.attr)):
                self.problem(node, f"`{ast.unparse(node)}`: {base.__name__} has no attribute {node.attr!r}")
                return _UNKNOWN
            return found
        if isinstance(base, type):
            raw = inspect.getattr_static(base, node.attr, _UNKNOWN)
            if raw is _UNKNOWN:
                self.problem(node, f"`{ast.unparse(node)}`: {base.__qualname__} has no attribute {node.attr!r}")
                return _UNKNOWN
            if isinstance(raw, type):
                return raw
            if inspect.isfunction(raw) or isinstance(raw, (staticmethod, classmethod)):
                return _Method(base, node.attr, on_instance=False)
            return _UNKNOWN
        if isinstance(base, _Instance):
            raw = inspect.getattr_static(base.cls, node.attr, None)
            if isinstance(raw, property) and raw.fget is not None:
                return _return_type(raw.fget, base.cls)
            if raw is not None and (inspect.isfunction(raw) or isinstance(raw, (staticmethod, classmethod))):
                return _Method(base.cls, node.attr, on_instance=True)
            return _UNKNOWN
        return _UNKNOWN

    # -- calls ------------------------------------------------------------------------
    @staticmethod
    def _signature(target: Any) -> tuple[inspect.Signature, str, Any] | None:
        """The signature a call to *target* is bound against, its display name, and what it returns."""
        try:
            if isinstance(target, type):
                return inspect.signature(target), target.__qualname__, _Instance(target)
            if isinstance(target, _Method):
                raw = inspect.getattr_static(target.owner, target.name)
                name = f"{target.owner.__qualname__}.{target.name}"
                if isinstance(raw, staticmethod):
                    return inspect.signature(raw.__func__), name, _return_type(raw.__func__, target.owner)
                if isinstance(raw, classmethod):
                    return (inspect.signature(getattr(target.owner, target.name)), name,
                            _return_type(raw.__func__, target.owner))
                if inspect.isfunction(raw):
                    sig = inspect.signature(raw)
                    if target.on_instance:
                        sig = sig.replace(parameters=list(sig.parameters.values())[1:])
                    return sig, name, _return_type(raw, target.owner)
                return None
            if inspect.isfunction(target):
                return inspect.signature(target), target.__qualname__, _return_type(target, None)
        except (TypeError, ValueError):
            return None
        return None

    def call(self, node: ast.Call) -> Any:
        if id(node) in self.seen:
            return self.seen[id(node)]
        self.seen[id(node)] = _UNKNOWN
        for arg in [*node.args, *(k.value for k in node.keywords)]:
            self.visit_expr(arg)
        target = self.value(node.func)
        result: Any = _UNKNOWN
        if target is _FOREIGN:
            result = _FOREIGN
        elif isinstance(target, type) and getattr(target, "_is_protocol", False):
            self.report.resolved += 1
            self.problem(node, f"`{ast.unparse(node.func)}(...)`: {target.__qualname__} is a Protocol; "
                               "it cannot be instantiated")
        elif isinstance(target, type) and inspect.isabstract(target):
            self.report.resolved += 1
            self.problem(node, f"`{ast.unparse(node.func)}(...)`: {target.__qualname__} is abstract; "
                               "it cannot be instantiated")
        else:
            found = self._signature(target) if target is not _UNKNOWN else None
            if found is None:
                self.report.unresolved += 1
            else:
                sig, name, result = found
                self.report.resolved += 1
                self._bind(node, sig, name)
        self.seen[id(node)] = result
        return result

    def _bind(self, node: ast.Call, sig: inspect.Signature, name: str) -> None:
        keywords = [k.arg for k in node.keywords if k.arg]
        dynamic = any(isinstance(a, ast.Starred) for a in node.args) or any(k.arg is None for k in node.keywords)
        callee = ast.unparse(node.func)
        if dynamic:
            # `*`/`**` can supply anything, but a literal keyword still has to exist.
            if not any(p.kind is p.VAR_KEYWORD for p in sig.parameters.values()):
                named = {n for n, p in sig.parameters.items() if p.kind is not p.POSITIONAL_ONLY}
                for keyword in keywords:
                    if keyword not in named:
                        self.problem(node, f"`{callee}(...)` does not bind to {name}: "
                                           f"got an unexpected keyword argument {keyword!r}")
            return
        try:
            sig.bind(*[None] * len(node.args), **dict.fromkeys(keywords))
        except TypeError as exc:
            self.problem(node, f"`{callee}(...)` does not bind to {name}: {exc}")

    def visit_expr(self, node: ast.AST) -> None:
        for child in ast.walk(node):
            if isinstance(child, ast.Call):
                self.call(child)
            elif isinstance(child, ast.Attribute) and isinstance(child.ctx, ast.Load):
                self.attribute(child)

    # -- statements -------------------------------------------------------------------
    def _import(self, node: ast.Import | ast.ImportFrom) -> None:
        modules = [node.module or ""] if isinstance(node, ast.ImportFrom) else [a.name for a in node.names]
        for module in modules:
            root = module.split(".")[0]
            if root != _PACKAGE and difflib.SequenceMatcher(
                    None, root.lower().replace("_", ""), _PACKAGE.replace("_", "")).ratio() >= 0.85:
                self.problem(node, f"`{module}` is not the library's package; it is `{_PACKAGE}`")
                for alias in node.names:
                    self.env[(alias.asname or alias.name).split(".")[0]] = _UNKNOWN
                return
        if isinstance(node, ast.ImportFrom):
            if node.level != 0 or not node.module or node.module.split(".")[0] != _PACKAGE:
                for alias in node.names:
                    self.env[alias.asname or alias.name] = _FOREIGN
                return
            try:
                module = importlib.import_module(node.module)
            except ImportError:
                self.problem(node, f"`from {node.module} import …`: no such module in the library")
                return
            for alias in node.names:
                if alias.name == "*":
                    continue
                try:
                    self.env[alias.asname or alias.name] = getattr(module, alias.name)
                except AttributeError:
                    try:
                        self.env[alias.asname or alias.name] = importlib.import_module(f"{node.module}.{alias.name}")
                    except ImportError:
                        self.problem(node, f"`from {node.module} import {alias.name}`: no such name in the library")
                        self.env[alias.asname or alias.name] = _UNKNOWN
            if id(node) in self.top:
                self.imports.append(ast.unparse(node))
            return
        for alias in node.names:
            bound = alias.asname or alias.name.split(".")[0]
            if alias.name.split(".")[0] != _PACKAGE:
                self.env[bound] = _FOREIGN
                continue
            try:
                module = importlib.import_module(alias.name)
            except ImportError:
                self.problem(node, f"`import {alias.name}`: no such module in the library")
                self.env[bound] = _UNKNOWN
                continue
            self.env[bound] = module if alias.asname else importlib.import_module(_PACKAGE)
            if id(node) in self.top:
                self.imports.append(ast.unparse(node))

    def statement(self, node: ast.stmt | ast.ExceptHandler | ast.match_case) -> None:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            self._import(node)
            return
        for name, value in ast.iter_fields(node):
            if name not in _BODIES:
                for child in value if isinstance(value, list) else [value]:
                    if isinstance(child, ast.AST):
                        self.visit_expr(child)
        if isinstance(node, ast.Assign):
            value = self.value(node.value)
            for target in node.targets:
                if isinstance(target, ast.Name):
                    self.env[target.id] = value
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.value is not None:
            self.env[node.target.id] = self.value(node.value)
        for name in _BODIES:
            for child in getattr(node, name, []) or []:
                if isinstance(child, (ast.stmt, ast.ExceptHandler, ast.match_case)):
                    self.statement(child)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            self.env[node.name] = _UNKNOWN


def _names_the_library(content: str) -> bool:
    """Whether a block parses as Python and imports from, or names, the library."""
    try:
        tree = ast.parse(_strip_magics(content))
    except (SyntaxError, RecursionError, MemoryError, ValueError):
        return False
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and (node.module or "").split(".")[0] == _PACKAGE:
            return True
        if isinstance(node, ast.Import) and any(a.name.split(".")[0] == _PACKAGE for a in node.names):
            return True
        if isinstance(node, ast.Name) and node.id == _PACKAGE:
            return True
    return False


def _strip_magics(content: str) -> str:
    return "\n".join("" if _MAGIC.match(line) else line for line in content.split("\n"))


def python_problems(text: str) -> Report:
    """Every problem in a page's Python blocks, read as one script, and the call counts."""
    report = Report()
    page = _Page(report)
    blocks = []
    for line, raw, content in _docs.fences(text):
        tag = language(raw)
        if tag not in PYTHON:
            # A Python example in a block tagged otherwise is read too, and reported: it would
            # otherwise be neither checked nor counted (F65 review).
            if tag in JSON or not _names_the_library(content):
                continue
            report.problems.append((line, f"a Python example in a block tagged {tag or 'nothing'!r}; tag it python"))
        try:
            tree = ast.parse(_strip_magics(content))
        except SyntaxError as exc:
            report.unparsed += 1
            report.problems.append((line + (exc.lineno or 1) - 1, f"a Python block does not parse: {exc.msg}"))
            continue
        except (RecursionError, MemoryError):
            report.unparsed += 1
            report.problems.append((line, "a Python block is nested too deeply to check"))
            continue
        blocks.append((line, tree))
    for _, tree in blocks:
        page.unstable |= _unstable_names(tree)
    for line, tree in blocks:
        page.offset = line
        # One memo per block, and every tree kept alive: the memo is keyed by id(), which is
        # unique only while its node lives (a freed tree's ids were reused by the next block).
        page.seen = {}
        page.trees.append(tree)
        page.top = {id(node) for node in tree.body}
        try:
            for node in tree.body:
                page.statement(node)
        except RecursionError:
            report.problems.append((line, "a Python block is nested too deeply to check"))
    return report


# --- load-context JSON ----------------------------------------------------------------

# Where a load context holds free-form content, not fields the library reads: a staging
# table's column types and the context handed to a staging notebook. Each must still be an
# object.
OPAQUE = frozenset({"global_context.staging_table_schema", "staging_context.stages[].staging_context"})
_STAGES = "staging_context.stages[]"
# A load context that validates: every required field set, so the library's own reading runs
# to the end. Its values are irrelevant; which keys are read is what is recorded.
_MINIMAL = {
    "global_context": {"id": "ent_1", "entity_name": "e", "source_system": "s", "target_table_path": "t",
                       "staging_table_path": "p", "staging_table_schema": {"type": "struct", "fields": []}},
    "staging_context": {"stages": [{"nb_name": "nb"}]},
    "scd_context": {"business_key_column": "k"},
    "runtime_context": {"correlation_id": "c", "pipeline_name": "p", "application_name": "a"},
    "logging_context": {"logging_level": "INFO", "log_file_name": "f", "log_file_dir": "d"},
    "hwm_context": {"hwm_column_name": "m", "hwm_value": "v"},
    "dqd_context": {"source_kind": "delta", "source_identifier": "t"},
}


class _Recording(dict):
    """A dict that notes every key read from it by `get`, `[]`, `pop` or `setdefault`.

    `in` is not a read: GlobalContext tests for `parallel_execution_enabled` only to refuse it.
    """

    def __init__(self, data: dict[str, Any], path: str, log: dict[str, set[str]]) -> None:
        super().__init__(data)
        self._path, self._log = path, log
        log.setdefault(path, set())

    def _note(self, key: object) -> None:
        if isinstance(key, str):
            self._log[self._path].add(key)

    def get(self, key: Any, default: Any = None) -> Any:
        self._note(key)
        return super().get(key, default)

    def __getitem__(self, key: Any) -> Any:
        self._note(key)
        return super().__getitem__(key)

    def pop(self, key: Any, *default: Any) -> Any:
        self._note(key)
        return super().pop(key, *default)

    def setdefault(self, key: Any, default: Any = None) -> Any:
        self._note(key)
        return super().setdefault(key, default)


def context_schema() -> dict[str, set[str]]:
    """The keys the library reads, by path: "" for the top level, each section name, and
    "staging_context.stages[]" for a stage.

    Recorded, not scanned: `LoadContextManager` parses, validates and builds a staging phase
    context from a load context whose dicts note each key read. So a read through an alias, a
    helper or `self._data` is found, and an unrelated variable named `stage` is not (F65
    adversary, against a lexical scan). A read on a branch this context does not take is not
    found, and shows up as a false alarm on the example that uses it, not as a silent pass.
    """
    from spark_quick_start.orchestration import context_manager

    log: dict[str, set[str]] = {}
    sections = {name: _Recording(body, name, log) for name, body in _MINIMAL.items() if name != "staging_context"}
    stages = [_Recording(stage, _STAGES, log) for stage in _MINIMAL["staging_context"]["stages"]]
    sections["staging_context"] = _Recording({"stages": stages}, "staging_context", log)
    top = _Recording(sections, "", log)
    fake_json = types.SimpleNamespace(loads=lambda _text: top, dumps=json.dumps)
    with mock.patch.object(context_manager, "json", fake_json):
        manager = context_manager.LoadContextManager("{}").parse("storage", "engineering")
        manager.validate_all()
        manager.build_staging_phase_context(0, "nb")
    return log


def _looks_like_section(key: str, sections: set[str]) -> bool:
    # `global_ctx` is how the code names a section's object, so it is the likeliest misspelling.
    folded = re.sub(r"[^a-z0-9]", "", key.lower())
    folded = folded[:-3] + "context" if folded.endswith("ctx") else folded
    return (folded in {re.sub(r"[^a-z0-9]", "", s) for s in sections}
            or re.search(r"(_context|Context|_ctx|Ctx)$", key) is not None)


def _object_problems(value: Any, path: str, schema: dict[str, set[str]], line: int,
                     problems: list[tuple[int, str]]) -> None:
    where = path or "the load context"
    if not isinstance(value, dict):
        problems.append((line, f"`{path}` is {type(value).__name__}; the library reads it as an object"))
        return
    if path in OPAQUE:
        return
    known = schema.get(path)
    if known is None:
        return
    for key in value:
        child = f"{path}.{key}" if path else key
        if key not in known:
            problems.append((line, f"{where} has a key {key!r} the library does not read"))
        elif child in OPAQUE or not path:
            _object_problems(value[key], child, schema, line, problems)
        elif path == "staging_context" and key == "stages":
            if not isinstance(value[key], list):
                problems.append((line, f"`{child}` is {type(value[key]).__name__}; the library reads a list"))
                continue
            for item in value[key]:
                _object_problems(item, _STAGES, schema, line, problems)


def _load(content: str) -> tuple[Any, list[str]]:
    """The parsed JSON and every key repeated within one object (json.loads keeps only the last)."""
    repeated: list[str] = []

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        seen: set[str] = set()
        for key, _ in items:
            if key in seen:
                repeated.append(key)
            seen.add(key)
        return dict(items)

    return json.loads(content, object_pairs_hook=pairs), repeated


def context_problems(text: str, schema: dict[str, set[str]]) -> Report:
    """Every problem in a page's load-context JSON, and how many JSON blocks it left unchecked.

    A JSON object is a load context when a top-level key is, or looks like, a section name
    (`global_context`, `globalContext`, `anything_context`), so misspelt sections are reported,
    not skipped. A JSON block that does not parse is a problem. A load context in a block not
    tagged `json` is a problem too, and is checked as well.
    """
    report = Report()
    for line, raw, content in _docs.fences(text):
        tag = language(raw)
        if tag in PYTHON:
            continue
        try:
            doc, repeated = _load(content)
        except (ValueError, RecursionError) as exc:
            if tag in JSON:
                report.problems.append((line, f"a JSON block does not parse: {exc}"))
            continue
        is_context = isinstance(doc, dict) and any(_looks_like_section(k, schema[""]) for k in doc)
        if tag not in JSON:
            if not is_context:
                continue
            report.problems.append((line, f"a load context in a block tagged {tag or 'nothing'!r}; tag it json"))
        if not is_context:
            report.unchecked_json += 1
            continue
        report.problems += [(line, f"a JSON object repeats the key {key!r}; only its last value is read")
                            for key in repeated]
        _object_problems(doc, "", schema, line, report.problems)
    return report
