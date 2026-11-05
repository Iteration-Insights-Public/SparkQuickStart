#!/usr/bin/env python3
"""Find every mermaid diagram in the repository's Markdown and check its static rules.

A diagram is a fenced code block whose info string is ``mermaid``, found with markdown-it-py
(a [dev] dependency: run this from the dev venv) configured as tests/architecture/_docs.py
configures it, so a block is found exactly where GitHub renders one. A hand-rolled fence
scanner is what F55 and F56 replaced there.

The Markdown in scope is what git tracks or would track: ``git ls-files --cached --others
--exclude-standard``, so gitignored scratch never fails the check and a new, not-yet-added
page does not escape it. Files are selected by every extension GitHub renders as Markdown,
case-insensitively, in Python — a git pathspec is case-sensitive even on macOS (F57
adversary). Files are read as ``utf-8-sig``: a byte-order mark before the first fence hid it.

Static rules (F57):
- every block opens with a sanctioned diagram type, after optional front matter, ``%%``
  comments and ``%%{init}%%`` directives;
- no ``linkStyle`` by edge index. Indices count edges in source order, so adding one edge
  above restyles the wrong line silently; ``linkStyle default`` is fine.

Usage:
    python scripts/check_mermaid.py          # check; non-zero exit on any problem
    python scripts/check_mermaid.py --list   # print path:line of every block, one per line

tests/architecture/test_documentation.py imports this module and asserts both. Rendering is
not checked by machine (F58, closed wontfix): ``--list`` names each diagram to preview by hand
on GitHub or in the mermaid live editor.
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

from markdown_it import MarkdownIt

REPO_ROOT = Path(__file__).resolve().parent.parent

# The types this repo draws with. Deliberately short: `graph` is the legacy spelling of
# `flowchart`, `stateDiagram` v1 is superseded by v2, and the *-beta types may not exist in
# the mermaid version GitHub renders with.
SANCTIONED_TYPES = (
    "flowchart",
    "sequenceDiagram",
    "classDiagram",
    "stateDiagram-v2",
    "erDiagram",
    "gitGraph",
    "timeline",
)

# The extensions github-markup renders as Markdown.
MARKDOWN_SUFFIXES = frozenset({".md", ".markdown", ".mdown", ".mkdn", ".mkd", ".mdwn", ".mkdown"})

_MD = MarkdownIt("commonmark").enable(["table", "strikethrough"])
_FRONT_MATTER = re.compile(r"\A---[ \t]*\n.*?\n---[ \t]*(?:\n|\Z)", re.DOTALL)
# A directive may span lines, and a declaration may follow it on the same line.
_DIRECTIVE = re.compile(r"%%\{.*?\}%%", re.DOTALL)
# ";" separates statements like a newline, so a statement starts at either. Case-insensitive
# on purpose: a false alarm on a node named "linkstyle" costs less than a missed restyle.
_INDEX_LINK_STYLE = re.compile(r"(?:^|;)\s*linkStyle\s+(?!default\b)\S", re.IGNORECASE)


def markdown_files(root: Path = REPO_ROOT) -> list[Path]:
    """Every Markdown file git tracks or would track under *root*, sorted.

    Raises when git cannot list them: a check that found no files would pass vacuously.
    """
    listed = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        cwd=root, capture_output=True, check=True,
    ).stdout.decode("utf-8")
    return sorted({root / name for name in listed.split("\0")
                   if Path(name).suffix.lower() in MARKDOWN_SUFFIXES and (root / name).is_file()})


def mermaid_blocks(text: str) -> list[tuple[int, str]]:
    """Every mermaid fence in *text* as (line of the opening fence, block source).

    The language is matched case-insensitively: a block that may render must be checked, and
    checking one that does not costs nothing.
    """
    return [(token.map[0] + 1 if token.map else 1, token.content)
            for token in _MD.parse(text)
            if token.type == "fence" and token.info.lower().split(maxsplit=1)[:1] == ["mermaid"]]


def block_problems(source: str) -> list[str]:
    """Why a mermaid block breaks a static rule; empty when it does not."""
    problems = []
    body = _DIRECTIVE.sub("", _FRONT_MATTER.sub("", source))
    lines = [line.strip() for line in body.splitlines()]
    declaration = next((line for line in lines if line and not line.startswith("%%")), "")
    kind = declaration.split()[0].rstrip(";:") if declaration else ""
    if kind not in SANCTIONED_TYPES:
        problems.append(f"declares {kind or 'nothing'!r}; sanctioned types are {', '.join(SANCTIONED_TYPES)}")
    for offset, line in enumerate(source.splitlines(), start=1):
        if _INDEX_LINK_STYLE.search(line):
            problems.append(f"block line {offset} styles edges by index ({line.strip()}); "
                            "indices shift when an edge is added — use a distinct arrow shape instead")
    return problems


def repository_blocks(root: Path = REPO_ROOT) -> list[tuple[str, int, str]]:
    """Every mermaid block in the repository as (repo-relative path, line, source)."""
    return [(path.relative_to(root).as_posix(), line, source)
            for path in markdown_files(root)
            for line, source in mermaid_blocks(path.read_text(encoding="utf-8-sig"))]


def main(argv: list[str] | None = None, root: Path = REPO_ROOT) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--list", action="store_true", help="print path:line of every block and exit")
    args = parser.parse_args(argv)
    blocks = repository_blocks(root)
    if args.list:
        for path, line, _ in blocks:
            print(f"{path}:{line}")
        return 0
    problems = [f"{path}:{line}: {why}" for path, line, source in blocks for why in block_problems(source)]
    for problem in problems:
        print(problem, file=sys.stderr)
    print(f"{len(blocks)} mermaid block(s) checked, {len(problems)} problem(s).")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
