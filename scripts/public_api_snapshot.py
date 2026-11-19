#!/usr/bin/env python3
"""Render the core's public API, and compare it with its committed snapshot (F220).

The public API is every name in the ``__all__`` of the packages below, with its signature: a
package that extends the core by composition imports these names and nothing else
(docs/architecture/decisions/2026-11-13-core-extension-readiness-design.md, change 3). The
snapshot, tests/architecture/public_api.txt, is a gate: an added line is a minor release, a
changed or removed one is a breaking change, and editing the file by hand is never a fix.

One line per member: ``<qualified name> <kind> <detail>``. For a class: its constructor; its
bases, abstract members, dataclass flags and whether a protocol is runtime-checkable; every
public method, property (getter signature, setter or not), cached property and static or class
method it has or inherits from this package; the subclass hooks; the dunders a caller reaches
through syntax (`__call__`, `with`, `[]`, `in`, iteration, equality); the annotations a subclass
relies on (its public fields and the services the orchestrator injects at ``.run()``); the public
attributes its ``__init__`` sets; the class attributes a subclass declares; and the value of a
public constant. Every value is rendered stably: sets sorted, no memory address. The check-
adversary run of 2026-11-13 set this list; tests/architecture/test_public_api.py holds each of
its findings as a mutation.

The concrete adapters and persistence adapters are exported only when their module imports
(``adapters/__init__.py``); the snapshot names them explicitly and refuses to render without
one, so a missing file is an error rather than a snapshot with lines missing.

Usage:
    python scripts/public_api_snapshot.py            # compare; non-zero exit on a difference
    python scripts/public_api_snapshot.py --write    # rewrite the snapshot from the code
"""
from __future__ import annotations

import argparse
import ast
import functools
import importlib
import inspect
import sys
import textwrap
from pathlib import Path
from types import ModuleType
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
SNAPSHOT = REPO_ROOT / "tests" / "architecture" / "public_api.txt"

PACKAGES = (
    "spark_quick_start",
    "spark_quick_start.orchestration",
    "spark_quick_start.orchestration.transformers",
    "spark_quick_start.adapters",
    "spark_quick_start.logger",
    "spark_quick_start.spark_logging",
    # Every staging notebook calls create_staging_main_function; the shim is its public path.
    "spark_quick_start.spark_load_orchestration",
)
# Exported only when their module imports; a full checkout always has them.
CONDITIONAL = {
    "spark_quick_start.adapters": ("DatabricksAdapter", "FabricAdapter", "LocalAdapter"),
    "spark_quick_start.spark_logging": (
        "DatabricksPersistenceAdapter", "FabricPersistenceAdapter", "LocalFilePersistenceAdapter",
    ),
}
# Private by name, public by contract: a subclass in another package overrides or reads them.
HOOKS = ("_produce", "_run", "before_run", "after_run")
SERVICES = ("_spark", "_adapter", "_logger", "_metrics_collector", "_stage_index")
DECLARATIONS = ("scd_style_only", "data_vault_style", "source_kind", "required_scd_type")
# Called through syntax, not by name: removing one breaks `with`, `[]`, `in` or a call.
DUNDERS = ("__call__", "__enter__", "__exit__", "__getitem__", "__setitem__", "__contains__",
           "__iter__", "__len__", "__eq__", "__hash__")

_SIMPLE = (str, int, float, bool, type(None), tuple, frozenset, set, list, dict)


def _stable(value: Any) -> str:
    """A repr that is the same in every run: sets sorted, no memory address."""
    if isinstance(value, (set, frozenset)):
        inner = ", ".join(sorted(_stable(v) for v in value))
        return f"{type(value).__name__}({{{inner}}})" if value else f"{type(value).__name__}()"
    if isinstance(value, (list, tuple)):
        inner = ", ".join(_stable(v) for v in value)
        return f"[{inner}]" if isinstance(value, list) else f"({inner}{',' if len(value) == 1 else ''})"
    if isinstance(value, dict):
        return "{" + ", ".join(f"{_stable(k)}: {_stable(v)}" for k, v in
                               sorted(value.items(), key=lambda kv: _stable(kv[0]))) + "}"
    if inspect.isfunction(value) or inspect.isbuiltin(value):
        return f"<function {value.__qualname__}>"
    text = repr(value)
    return f"<{type(value).__name__}>" if " at 0x" in text else text


class _Shown:
    def __init__(self, text: str) -> None:
        self.text = text

    def __repr__(self) -> str:
        return self.text


def _signature(obj: Any) -> str:
    try:
        signature = inspect.signature(obj)
    except (TypeError, ValueError):
        return "(?)"
    parameters = [p if p.default is p.empty else p.replace(default=_Shown(_stable(p.default)))
                  for p in signature.parameters.values()]
    return str(signature.replace(parameters=parameters))


def _ours(owner: type) -> bool:
    return owner.__module__.split(".")[0] == "spark_quick_start"


def _instance_attributes(function: Any) -> list[str]:
    """The public names `__init__` assigns on `self`: a protocol attribute often lives there."""
    try:
        tree = ast.parse(textwrap.dedent(inspect.getsource(function)))
    except (OSError, TypeError, SyntaxError):
        return []
    names = set()
    for node in ast.walk(tree):
        targets = node.targets if isinstance(node, ast.Assign) else (
            [node.target] if isinstance(node, (ast.AnnAssign, ast.AugAssign)) else [])
        for target in targets:
            if (isinstance(target, ast.Attribute) and isinstance(target.value, ast.Name)
                    and target.value.id == "self" and not target.attr.startswith("_")):
                names.add(target.attr)
    return sorted(names)


_PUBLIC_NAMES: dict[int, str] = {}


def _public_name(obj: type) -> str:
    """The first public path *obj* is exported under, or where it is defined when it is not."""
    return _PUBLIC_NAMES.get(id(obj), f"{obj.__module__}.{obj.__qualname__}")


def _class_lines(qualified: str, cls: type) -> list[str]:
    lines = [f"{qualified} class{_signature(cls)}"]
    bases = ", ".join(_public_name(b) for b in cls.__bases__)
    lines.append(f"{qualified} bases ({bases})")
    abstract = sorted(getattr(cls, "__abstractmethods__", ()))
    if abstract:
        lines.append(f"{qualified} abstract ({', '.join(abstract)})")
    params = getattr(cls, "__dataclass_params__", None)
    if params is not None:
        lines.append(f"{qualified} dataclass frozen={params.frozen} eq={params.eq} order={params.order}")
    if getattr(cls, "_is_protocol", False):
        lines.append(f"{qualified} protocol runtime_checkable={getattr(cls, '_is_runtime_protocol', False)}")
    owners = [k for k in cls.__mro__ if _ours(k)]
    seen: set[str] = set()
    for owner in owners:
        for name, value in vars(owner).items():
            if name in seen:
                continue
            seen.add(name)
            public = not name.startswith("_")
            if name in DECLARATIONS:
                lines.append(f"{qualified}.{name} declaration {_stable(value)}")
            elif not (public or name in HOOKS or name in DUNDERS):
                continue
            elif isinstance(value, property):
                setter = " setter" if value.fset is not None else ""
                lines.append(f"{qualified}.{name} property{_signature(value.fget)}{setter}")
            elif isinstance(value, functools.cached_property):
                lines.append(f"{qualified}.{name} cached_property{_signature(value.func)}")
            elif isinstance(value, (staticmethod, classmethod)):
                kind = type(value).__name__
                lines.append(f"{qualified}.{name} {kind}{_signature(getattr(cls, name))}")
            elif callable(value) and not inspect.isclass(value):
                lines.append(f"{qualified}.{name} method{_signature(value)}")
            elif inspect.isclass(value):
                lines.append(f"{qualified}.{name} nested class")
            elif isinstance(value, _SIMPLE):
                lines.append(f"{qualified}.{name} constant {_stable(value)}")
            else:
                lines.append(f"{qualified}.{name} {type(value).__name__}")
    annotated: dict[str, str] = {}
    for owner in owners:
        for name, annotation in vars(owner).get("__annotations__", {}).items():
            if (not name.startswith("_") or name in SERVICES) and name not in annotated:
                annotated[name] = annotation if isinstance(annotation, str) else repr(annotation)
    lines += [f"{qualified}.{name} annotation {annotation}" for name, annotation in annotated.items()]
    instance = set()
    for owner in owners:
        init = vars(owner).get("__init__")
        if init is not None:
            instance.update(_instance_attributes(init))
    lines += [f"{qualified}.{name} instance attribute" for name in sorted(instance - set(annotated))]
    return lines


def _member_lines(qualified: str, obj: Any) -> list[str]:
    if inspect.isclass(obj):
        return _class_lines(qualified, obj)
    if callable(obj):
        return [f"{qualified} function{_signature(obj)}"]
    if isinstance(obj, _SIMPLE):
        return [f"{qualified} constant {_stable(obj)}"]
    return [f"{qualified} {type(obj).__name__}"]


def _exported(module: ModuleType) -> list[str]:
    names = set(getattr(module, "__all__", ()))
    for name in CONDITIONAL.get(module.__name__, ()):
        if getattr(module, name, None) is None:
            raise RuntimeError(
                f"{module.__name__}.{name} is absent: the snapshot describes the full trunk, so "
                "render it from a checkout that has every adapter file."
            )
        names.add(name)
    return sorted(names)


def render() -> str:
    """The public API, one line per member, sorted."""
    lines: list[str] = []
    exported = []
    for package in PACKAGES:
        module = importlib.import_module(package)
        exported += [(f"{package}.{name}", getattr(module, name)) for name in _exported(module)]
    _PUBLIC_NAMES.clear()
    for qualified, obj in exported:
        if inspect.isclass(obj):
            _PUBLIC_NAMES.setdefault(id(obj), qualified)
    for qualified, obj in exported:
        lines += _member_lines(qualified, obj)
    return "\n".join(sorted(lines)) + "\n"


def _split_top(text: str) -> list[str]:
    """Split a parameter list on the commas outside brackets and quotes."""
    parts, depth, quote, start = [], 0, "", 0
    for i, ch in enumerate(text):
        if quote:
            quote = "" if ch == quote else quote
        elif ch in "'\"":
            quote = ch
        elif ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(text[start:i].strip())
            start = i + 1
    tail = text[start:].strip()
    return [*parts, tail] if tail else parts


def _parameters(line: str) -> tuple[str, list[str], str] | None:
    """``(head, parameters, return)`` of a signature line, or None for another kind of line."""
    open_at = line.find("(")
    if open_at < 0:
        return None
    depth = 0
    for i in range(open_at, len(line)):
        depth += {"(": 1, ")": -1}.get(line[i], 0)
        if depth == 0:
            return line[:open_at], _split_top(line[open_at + 1:i]), line[i + 1:]
    return None


def _only_optional_added(old: str, new: str) -> bool:
    """True when *new* is *old* with only optional parameters added where no caller notices.

    A parameter with a default breaks no caller when it is keyword-only, or when no existing
    positional parameter follows it; a required one, or one inserted before an existing
    positional parameter, can. The old parameters must keep their text and their order.
    """
    before, after = _parameters(old), _parameters(new)
    if before is None or after is None or before[0] != after[0] or before[2] != after[2]:
        return False
    old_params, new_params = before[1], after[1]
    if [p for p in new_params if p in old_params] != old_params:
        return False
    extra = [i for i, p in enumerate(new_params) if p not in old_params]
    if not extra:
        return False
    for i in extra:
        param = new_params[i]
        if param != "*" and "=" not in param:
            return False
        keyword_only = any(p.startswith("*") and not p.startswith("**") for p in new_params[:i])
        # A positional caller fills `*args` too, so a parameter inserted before it shifts them; a
        # bare `*` inserted before an existing parameter makes it keyword-only.
        later_positional = [p for p in new_params[i + 1:] if p in old_params and not p.startswith("**")]
        if not keyword_only and later_positional:
            return False
    return True


def compare(snapshot: str, current: str) -> list[str]:
    """What differs between the committed snapshot and the code, or [] when nothing does.

    A line whose signature only gained optional parameters no caller notices is a compatible
    change, except a method of a class meant for subclassing or a subclass hook, whose overrides
    it breaks. Any other removed or changed line is a breaking change; added lines alone are a
    minor release.
    """
    old = {line.rstrip() for line in snapshot.splitlines() if line.strip()}
    new = {line.rstrip() for line in current.splitlines() if line.strip()}
    removed, added = sorted(old - new), sorted(new - old)
    # A class with abstract members is meant to be subclassed: the core calls its methods on an
    # override, which breaks when the core passes an argument the override lacks. So do the
    # subclass hooks of any class.
    overridden = {line.split(" ", 1)[0] for line in old | new if " abstract (" in line}
    compatible = {}
    for line in removed:
        member = line.split(" ", 1)[0]
        owner, _, name = member.rpartition(".")
        if " method(" in line and (owner in overridden or name in HOOKS):
            continue
        head = _parameters(line)
        for candidate in added:
            if head and candidate.startswith(head[0] + "(") and _only_optional_added(line, candidate):
                compatible[line] = candidate
                break
    problems = [f"removed or changed (Breaking): {line}" for line in removed if line not in compatible]
    problems += [f"changed, optional parameters added (minor): {new_line}" for new_line in compatible.values()]
    problems += [f"added (minor): {line}" for line in added if line not in compatible.values()]
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--write", action="store_true", help="rewrite the snapshot from the code")
    args = parser.parse_args()
    current = render()
    if args.write:
        SNAPSHOT.write_text(current, encoding="utf-8")
        print(f"wrote {SNAPSHOT.relative_to(REPO_ROOT)} ({current.count(chr(10))} lines)")
        return 0
    problems = compare(SNAPSHOT.read_text(encoding="utf-8"), current)
    for problem in problems:
        print(problem)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
