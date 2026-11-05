"""
test_documentation.py — the shape of the project's documentation (README.md and docs/).

Contains: the sanctioned docs/ tree, the legacy documents still waiting to be dissolved,
    every relative link and image in README.md and docs/ resolving, every document and
    image reachable from the root README.md through a README.md index in each directory, and
    the static rules for mermaid diagrams (scripts/check_mermaid.py holds them; this runs it).
    It also holds the code examples to the code (F65): every Python example's calls bound
    against the real signatures, and every load-context JSON example's keys against what the
    context classes read (_examples.py does the reading).
Does NOT contain: anything else about what the documents say — that is review, and the
    skills in .claude/skills/ that the documentation plan adds.

The owner fixed four homes on 2026-09-27: README.md for the introduction, installation and
basic usage; docs/architecture/ for architecture, abstractions, design decisions and
diagrams, with dated records in docs/architecture/decisions/; docs/guides/ for task-oriented
guides; docs/reference/ for stable configuration and API reference; plus docs/images/.
"""
from __future__ import annotations

import collections
import dataclasses
import json
import posixpath
import re
import types
import typing
import warnings
from pathlib import Path
from urllib.parse import unquote

import _arch
import _docs
import _examples
import pytest

_DOCS = _arch.ROOT / "docs"
_TOP = {"architecture", "guides", "reference", "images"}
_REQUIRED = {"architecture", "guides"}
_PAGE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*\.md$")
# [0-9], not \d: \d matches any Unicode digit, and Arabic-Indic digits passed (F53 adversary).
_RECORD = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}-[a-z0-9]+(?:-[a-z0-9]+)*\.md$")
# Machine-local noise, never documentation. Only these are ignored: a blanket "any dotfile"
# rule also hid `docs/.drafts/guide.md`, which git tracks on every machine (F53 adversary).
_NOISE = {".DS_Store", "Thumbs.db"}
_IMAGE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*\.(?:png|svg|jpg)$")
_INDEX = "README.md"
# Moved into docs/guides/ by F53 and dissolved guide by guide; deleted by F97.
_LEGACY = ("guides/user-guide.md", "guides/orchestration-user-guide.md")


def _entries(directory: Path, rel: str) -> tuple[list[Path], list[str]]:
    """A directory's entries minus named noise, and the problems found reading them.

    A symlink is reported, never followed: it puts the content outside the tree the check
    vouches for — the adversary pointed `docs/guides/x.md` at `/etc/hosts` and a symlinked
    `docs/guides/` at an empty directory elsewhere, and both passed. An unreadable directory
    is a problem, not a traceback.
    """
    try:
        found = sorted(p for p in directory.iterdir() if p.name not in _NOISE)
    except OSError as exc:
        return [], [f"{rel}/ cannot be read: {exc.__class__.__name__}"]
    problems = [f"{rel}/{p.name} is a symlink; documentation must live in the tree itself"
                for p in found if p.is_symlink()]
    return [p for p in found if not p.is_symlink()], problems


def _page_problem(entry: Path, rel: str, pattern: re.Pattern[str]) -> str | None:
    """Why a file is not a well-named page for its directory, or None."""
    name = entry.name
    if name == _INDEX:
        return None
    if name.lower() == _INDEX.lower():
        return f"{rel}/{name} — the index is spelled exactly {_INDEX}"
    if pattern is not _RECORD and _RECORD.fullmatch(name):
        return f"{rel}/{name} is a dated record; records live in docs/architecture/decisions/"
    if not pattern.fullmatch(name):
        return f"{rel}/{name} is not named like {pattern.pattern}"
    if pattern is _RECORD:
        import datetime

        try:
            datetime.date.fromisoformat(name[:10])
        except ValueError:
            return f"{rel}/{name} does not start with a real date"
    return None


def _flat_pages(directory: Path, rel: str, pattern: re.Pattern[str]) -> list[str]:
    entries, problems = _entries(directory, rel)
    for entry in entries:
        if entry.is_dir():
            problems.append(f"{rel}/{entry.name}/ is a directory; {rel}/ holds pages only")
        elif (why := _page_problem(entry, rel, pattern)) is not None:
            problems.append(why)
    return problems


def _docs_tree_problems(docs: Path) -> list[str]:
    """Every way docs/ departs from the sanctioned tree (F53).

    Unknown entries are reported, never ignored: a document that lands anywhere else is
    exactly what this check exists to catch. Names are kebab-case pages; records in
    decisions/ carry their date first; each directory may hold a README.md index.
    """
    label = _arch.rel(docs) if docs.is_relative_to(_arch.ROOT) else str(docs)
    if docs.is_symlink():
        return [f"{label} is a symlink; documentation must live in the tree itself"]
    if not docs.is_dir():
        return [f"{label} is missing"]
    found, problems = _entries(docs, "docs")
    top = {e.name: e for e in found}
    # docs/README.md is the hub the root README links to (F56); every other name is a home.
    for name in sorted(set(top) - _TOP - {_INDEX}):
        problems.append(f"docs/{name} is outside the sanctioned tree {sorted(_TOP)}")
    for name in sorted(_REQUIRED - set(top)):
        problems.append(f"docs/{name}/ is missing")
    if _INDEX in top and not top[_INDEX].is_file():
        problems.append(f"docs/{_INDEX} is a directory; the hub index must be a file")
    for name, entry in sorted(top.items()):
        if name in _TOP and not entry.is_dir():
            problems.append(f"docs/{name} is a file; it must be a directory")
    for name in ("guides", "reference"):
        if name in top and top[name].is_dir():
            problems += _flat_pages(docs / name, f"docs/{name}", _PAGE)
    if "images" in top and top["images"].is_dir():
        images, bad = _entries(docs / "images", "docs/images")
        problems += bad + [f"docs/images/{e.name} is not an image named like {_IMAGE.pattern}"
                           for e in images if e.is_dir() or not _IMAGE.fullmatch(e.name)]
    arch = docs / "architecture"
    if "architecture" in top and arch.is_dir():
        entries, bad = _entries(arch, "docs/architecture")
        problems += bad
        for entry in entries:
            if entry.is_dir():
                if entry.name != "decisions":
                    problems.append(f"docs/architecture/{entry.name}/ is not a sanctioned subdirectory")
            elif (why := _page_problem(entry, "docs/architecture", _PAGE)) is not None:
                problems.append(why)
        if any(e.name == "decisions" and e.is_dir() for e in entries):
            problems += _flat_pages(arch / "decisions", "docs/architecture/decisions", _RECORD)
    return problems


def test_docs_holds_only_the_sanctioned_tree() -> None:
    """F53 — every document has one of the owner's four homes, named so a reader can find it."""
    problems = _docs_tree_problems(_DOCS)
    assert not problems, f"docs/ departs from the sanctioned tree: {problems}"


def _tree(root: Path, files: list[str]) -> Path:
    for rel in files:
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        if not rel.endswith("/"):
            path.write_text("x", encoding="utf-8")
    return root


def test_every_docs_tree_check_can_fail() -> None:
    """F53 — anti-vacuity: one mutation per rule, each reported alone; valid forms clean."""
    import tempfile

    good = _GOOD_TREE
    mutations = {
        "a stray top-level file": [*good, "USER_GUIDE.md"],
        "a stray top-level directory": [*good, "agentic-setup/README.md"],
        "guides missing": [g for g in good if not g.startswith("guides/")],
        "a nested guide": [*good, "guides/dqf/querying.md"],
        "a guide not in kebab case": [*good, "guides/User_Guide.md"],
        "a reference page with capitals": [*good, "reference/LoadContext.md"],
        "an undated record": [*good, "architecture/decisions/audit.md"],
        "a stray architecture subdirectory": [*good, "architecture/records/x.md"],
        "a non-image in images": [*good, "images/notes.md"],
        "images as a file": [g for g in good if not g.startswith("images/")] + ["images"],
        # F53's adversary:
        "a dated record as an architecture page": [*good, "architecture/2026-09-25-x.md"],
        "a dated record as a guide": [*good, "guides/2026-09-25-x.md"],
        "a record dated in Arabic-Indic digits": [*good, "architecture/decisions/\u0662\u0660\u0662\u0666-09-25-x.md"],
        "a record with an impossible date": [*good, "architecture/decisions/9999-99-99-x.md"],
        "a hidden directory of docs": [*good, ".drafts/guide.md"],
        "a hidden guide": [*good, "guides/.secret-guide.md"],
        "a lowercase readme.md beside the index": [*good, "reference/readme.md"],
        # F56's adversary:
        "the hub index as a directory": [g for g in good if g != "README.md"] + ["README.md/junk.txt"],
    }
    wrong = {}
    for label, files in mutations.items():
        with tempfile.TemporaryDirectory() as tmp:
            got = _docs_tree_problems(_tree(Path(tmp) / "docs", files))
        if len(got) != 1:
            wrong[label] = got
    assert not wrong, f"these mutations were not reported by exactly one check: {wrong}"
    with tempfile.TemporaryDirectory() as tmp:
        docs = _tree(Path(tmp) / "docs", [*good, ".DS_Store", "guides/Thumbs.db"])
        assert _docs_tree_problems(docs) == [], "valid tree with machine-local noise was reported"
        assert _docs_tree_problems(Path(tmp) / "nope") != [], "a missing docs/ was not reported"


_GOOD_TREE = ["README.md", "architecture/overview.md", "architecture/README.md",
              "architecture/decisions/2026-09-25-architecture-audit.md", "architecture/decisions/README.md",
              "guides/building-a-pipeline.md", "guides/README.md", "reference/load-context.md",
              "images/architecture.png"]


def test_a_symlinked_docs_entry_is_reported() -> None:
    """F53 — a symlink in or as docs/ is reported, never followed.

    Its own test so a host that cannot make a symlink skips it with the reason, under
    `-rs`, and CI's SQS_FAIL_ON_SKIP fails if the Linux image ever loses symlinks (F105).
    """
    import os
    import tempfile

    if (reason := _arch.symlink_unavailable()) is not None:
        pytest.skip(reason)
    good, wrong = _GOOD_TREE, {}
    with tempfile.TemporaryDirectory() as tmp:
        outside = Path(tmp) / "outside.md"
        outside.write_text("x", encoding="utf-8")
        # A symlinked required directory is also missing as a directory, so it is reported twice.
        for label, link, target, count in (("a symlinked guide", "guides/x.md", outside, 1),
                                           ("a symlinked guides directory", "guides", Path(tmp), 2),
                                           ("a symlinked record", "architecture/decisions/2026-01-01-x.md", outside, 1)):
            docs = _tree(Path(tmp) / label.replace(" ", "-") / "docs",
                         [g for g in good if not (link == "guides" and g.startswith("guides/"))])
            os.symlink(target, docs / link)
            got = _docs_tree_problems(docs)
            if len(got) != count or not any("symlink" in g for g in got):
                wrong[label] = got
        docs = _tree(Path(tmp) / "plain" / "docs", good)
        os.symlink(docs, Path(tmp) / "linked-docs")
        if "symlink" not in " ".join(got := _docs_tree_problems(Path(tmp) / "linked-docs")):
            wrong["a symlinked docs/"] = got
    assert not wrong, f"these symlink mutations were not reported as expected: {wrong}"


_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")
# GitHub highlights lines of a non-Markdown file (or a Markdown file shown with ?plain=1) from
# these fragments, and honours the theme fragments on an image; nothing else resolves.
_LINE_ANCHOR = re.compile(r"^L[0-9]+(?:-L[0-9]+)?$")
_THEME_ANCHOR = re.compile(r"^gh-(?:dark|light)-mode-only$")


def _documents(root: Path) -> list[Path]:
    """README.md and every Markdown page under docs/, symlinks left to the tree check."""
    readme = [root / _INDEX] if (root / _INDEX).is_file() else []
    docs = root / "docs"
    pages = sorted(p for p in docs.rglob("*.md") if not p.is_symlink()) if docs.is_dir() else []
    return readme + pages


def _local_target(root: Path, page: Path, dest: str) -> str | None:
    """The repository-relative path *dest* points at from *page*, or None for a URL or nothing.

    Not checked for existence: the path may leave the repository or name nothing.
    """
    dest = dest.strip()
    if not dest or _SCHEME.match(dest) or dest.startswith("//"):
        return None
    path = unquote(re.sub(r"\\(.)", r"\1", dest.partition("#")[0].partition("?")[0]))
    if not path:
        return page.relative_to(root).as_posix()
    here = page.parent.relative_to(root).as_posix()
    return posixpath.normpath(path.lstrip("/") if path.startswith("/") else posixpath.join(here, path))


def _link_problem(root: Path, page: Path, dest: str) -> str | None:
    """Why *dest*, written in *page*, does not resolve inside *root*, or None.

    Absolute URLs are someone else's to keep alive and are not followed. Everything else is a
    path inside the repository, resolved case-sensitively the way GitHub and the Linux runner
    do: a leading ``/`` from the repository root, otherwise from the page's directory. A
    fragment must name a heading or HTML id in the Markdown page it lands on; on any other
    file only GitHub's line anchors resolve.
    """
    dest = dest.strip()
    if not dest:
        return "an empty destination"
    if _SCHEME.match(dest) or dest.startswith("//"):
        return None
    path, _, fragment = dest.partition("#")
    query = path.partition("?")[2]
    target = _local_target(root, page, dest)
    assert target is not None
    if target == ".." or target.startswith("../"):
        return f"{dest} leaves the repository"
    if target != "." and not _arch.resolves_case_sensitively(root, target):
        return f"{dest} does not resolve (looked for {target})"
    if not fragment:
        return None
    full = root / target
    if full.is_dir() and (full / _INDEX).is_file():
        # GitHub's tree view renders the directory's README.md, headings and all.
        full, target = full / _INDEX, posixpath.join(target, _INDEX)
    if full.is_file() and _LINE_ANCHOR.fullmatch(fragment) and (full.suffix != ".md" or "plain=1" in query):
        return None
    if full.is_file() and full.suffix == ".md":
        try:
            text = full.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            return f"{dest} — {target} cannot be read: {exc.__class__.__name__}"
        if unquote(fragment) not in _docs.anchors(text):
            return f"{dest} — no heading or id in {target} has the anchor #{fragment}"
        return None
    if full.is_file() and _THEME_ANCHOR.fullmatch(fragment):
        return None
    return f"{dest} — #{fragment} cannot resolve on {target}, which is not a Markdown page"


def _link_problems(root: Path) -> list[str]:
    """Every relative link or image in README.md and docs/ that a reader would find broken."""
    problems = []
    for page in _documents(root):
        label = page.relative_to(root).as_posix()
        try:
            text = page.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            problems.append(f"{label} cannot be read: {exc.__class__.__name__}")
            continue
        for line, dest in _docs.link_destinations(text):
            if (why := _link_problem(root, page, dest)) is not None:
                problems.append(f"{label}:{line}: {why}")
    return problems


def test_every_relative_link_and_image_resolves() -> None:
    """F55 — a link or image in README.md or docs/ lands on a file and anchor that exist."""
    documents = _documents(_arch.ROOT)
    assert _arch.ROOT / _INDEX in documents, "the root README.md is not scanned"
    assert len(documents) >= 5, "found almost no documents; the scan is looking in the wrong place"
    problems = _link_problems(_arch.ROOT)
    assert not problems, f"broken relative links or images: {problems}"


def test_every_link_check_can_fail() -> None:
    """F55 — anti-vacuity: each broken form is reported alone; each valid form is not."""
    import tempfile

    base = {
        "README.md": "# Project\n\nSee [guides](docs/guides/README.md).\n",
        "LICENSE": "MIT\n",
        "src/app.py": "x = 1\n",
        "docs/guides/README.md": "# Guides\n\n## Getting started\n\nSetup\n-----\n\n"
                                 "### Interpreting `ExecutionResult`\n\n## `__init__` and _private helpers\n\n"
                                 "## Save &amp; load `Map<K, V>`\n\n> ## Quoted\n",
        "docs/images/diagram.png": "png",
    }
    valid = [
        "[l](../../LICENSE)", "![d](../images/diagram.png)", "[a](#getting-started)",
        "[s](#setup)", "[x](README.md#guides)", "[r](/src/app.py#L1-L3)", "[w](https://example.com/x)",
        "[m](mailto:a@b.c)", "[d](../)", "[t](<../images/diagram.png> \"title\")",
        "[ref]: ../../LICENSE", '<img src="../images/diagram.png">', "`[code](nowhere.md)`",
        "```\n[fenced](nowhere.md)\n```", "<!-- [hidden](nowhere.md) -->", "[![b](../images/diagram.png)](#guides)",
        "[p](../images/diagram%2Epng)",
        # F55's adversary:
        "[c](#interpreting-executionresult)", "[u](#__init__-and-_private-helpers)",
        "[e](#save--load-mapk-v)", "[q](#quoted)", "Text[^1].\n\n[^1]: Some note.",
        "![t](../images/diagram.png#gh-dark-mode-only)", "[dir](./#getting-started)",
        "[pl](README.md?plain=1#L3)", '<a data-href="nope.md" href="#guides">x</a>',
        # F55's reviewer:
        '<img srcset="../images/diagram.png 1x, ../images/diagram.png 2x">',
        "Alpha\nbeta\n---\n\n[x](#alphabeta)",
        # F56's adversary: text on GitHub, so not a link to check.
        "[r]: nope.md", "Para.\n\n    [x](nope.md)", "<div>\n[x](nope.md)\n</div>", "<pre>\n[x](nope.md)\n</pre>",
        "Some text\n[r]: nope.md", "<!-- start\n\n[x](nope.md)", '[y](#guides "<a href=\'nope.md\'>")',
        '\\<a href="nope.md">x</a>', "Text[^n].\n\n[^n]: See.", "## Foo\n## Foo\n## Foo 1\n\n[x](#foo-1-1) [y](#foo-1)",
    ]
    broken = {
        "a missing file": "[x](nope.md)",
        "a missing image": "![x](../images/nope.png)",
        "wrong case": "[x](../../license)",
        "a missing anchor": "[x](#nope)",
        "a missing anchor in another page": "[x](../../README.md#nope)",
        "an anchor on a non-Markdown file": "[x](/src/app.py#top)",
        "a path out of the repository": "[x](../../../outside.md)",
        "a broken reference definition": "[r]: ../nope.md\n\nSee [r].",
        "a broken HTML image": '<img src="nope.png">',
        "a broken HTML link": "<a href='nope.md'>x</a>",
        "an image nested in a broken link": "[![b](../images/diagram.png)](nope.md)",
        "a broken link in a table": "| a | [x](nope.md) |",
        "a broken link after an unclosed code span": "`not code [x](nope.md)",
        "a broken link in a list": "- item [x](nope.md)",
        "a broken link after a closed fence": "```\ncode\n```\n[x](nope.md)",
        "a broken link in a quote": "> [x](nope.md)",
        # F55's adversary:
        "an anchor that drops a heading's code": "[x](#interpreting)",
        "a link after a comment opener in code": "Use `<!--` to open.\n\n[x](nope.md)\n\nClose `-->`.",
        "a quoted '>' before an HTML src": '<img alt="a > b" src="nope.png">',
        "an anchor only a data-id provides": '<div data-id="ghost"></div>\n\n[x](#ghost)',
        "a broken reference definition in a quote": "> [r]: nope.md\n>\n> See [r].",
        "a theme fragment on a Markdown page": "[x](README.md#gh-dark-mode-only)",
        "a line anchor on a rendered Markdown page": "[x](README.md#L3)",
        # F56's reviewer: a footnote's body is prose, links included.
        "a broken link in a footnote": "Text[^n].\n\n[^n]: See [x](nope.md).",
        "a broken link on a footnote's continuation line": "Text[^n].\n\n[^n]: First\n    [x](nope.md)",
        # F55's reviewer:
        "an empty destination": "[x]()",
        "a broken srcset candidate": '<img srcset="../images/diagram.png 1x, nope.png 2x">',
        "a fragment on a directory without an index": "[x](../images/#a)",
        "an underline under a list item": "- Item\n---\n\n[x](#item)",
        "an underline under a quote": "> Note\n---\n\n[x](#note)",
        "a spaced thematic break under a paragraph": "Para\n- - -\n\n[x](#para)",
        "only the last line of a setext heading": "Alpha\nbeta\n---\n\n[x](#beta)",
        "a duplicate suffix github-slugger skips": "## Bar\n## Bar\n## Bar 1\n\n[x](#bar-2)",
    }

    def problems(extra: str, page: str = "docs/guides/README.md", files: dict[str, bytes] | None = None) -> list[str]:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for rel_path, body in base.items():
                (root / rel_path).parent.mkdir(parents=True, exist_ok=True)
                (root / rel_path).write_text(body, encoding="utf-8")
            for rel_path, raw in (files or {}).items():
                (root / rel_path).parent.mkdir(parents=True, exist_ok=True)
                (root / rel_path).write_bytes(raw)
            target = root / page
            target.parent.mkdir(parents=True, exist_ok=True)
            before = target.read_text(encoding="utf-8") if target.exists() else ""
            target.write_text(before + "\n" + extra + "\n", encoding="utf-8")
            return _link_problems(root)

    wrong = {form: got for form in valid if (got := problems(form))}
    assert not wrong, f"valid forms were reported: {wrong}"
    wrong = {label: got for label, form in broken.items() if len(got := problems(form)) != 1}
    assert not wrong, f"these broken forms were not reported exactly once: {wrong}"
    # Every scanned place, and the unreadable-file branches (invalid UTF-8, not chmod: portable).
    placed = {
        "a broken link in the root README.md": problems("[x](nope.md)", page="README.md"),
        "a broken link in a nested page": problems("[x](../nope.md)", page="docs/reference/deep.md"),
        "an unreadable link target": problems("[x](/src/bad.md#a)", files={"src/bad.md": b"# A\xff\n"}),
        "an unreadable scanned page": problems("", files={"docs/guides/bad.md": b"# A\xff\n"}),
    }
    wrong = {label: got for label, got in placed.items() if len(got) != 1}
    assert not wrong, f"these placed forms were not reported exactly once: {wrong}"
    # The line a problem names: the base page is 14 lines and a blank separates the mutation,
    # so the mutation's first line is 16. Without counting the tag's newline the second read 16.
    lines = {
        "a link on a paragraph's second line": (problems("Text\n[x](nope.md)"), 17),
        "a link after a multi-line inline tag": (problems('Text <span\ntitle="t">s</span> [x](nope.md)'), 17),
    }
    wrong = {label: got for label, (got, line) in lines.items()
             if len(got) != 1 or not got[0].startswith(f"docs/guides/README.md:{line}: ")}
    assert not wrong, f"these problems did not name the right line: {wrong}"


def _reachability_problems(root: Path) -> list[str]:
    """Every docs/ directory without its README.md index, and every file under docs/ that no
    chain of links from the root README.md reaches (F56).

    Links are followed only out of README.md and pages under docs/ — a page reached from
    anywhere else is not reached by a reader of the documentation. A link to a directory
    reaches its README.md, as GitHub's tree view renders it. docs/images/ holds images only,
    so it carries no index; its images must be reached like any page.

    A missing or symlinked docs/, and symlinks inside it, are left to the tree check, which
    reports them; a link whose target cannot even be probed is left to the link check.
    """
    docs = root / "docs"
    if not docs.is_dir() or docs.is_symlink():
        return []
    directories = [docs, *sorted(d for d in docs.rglob("*") if d.is_dir() and not d.is_symlink())]
    # Case-sensitively: on macOS a lone readme.md "is" README.md (F56 adversary).
    problems = [f"{_label(root, d)}/ has no {_INDEX} index" for d in directories
                if not d.is_relative_to(docs / "images") and not _arch.resolves_case_sensitively(d, _INDEX)]
    reached: set[str] = set()
    queue = [_INDEX]
    while queue:
        page = queue.pop()
        if page in reached:
            continue
        reached.add(page)
        path = root / page
        if path.suffix != ".md" or not path.is_file() or not (page == _INDEX or page.startswith("docs/")):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue  # reported by the link check
        for _, dest in _docs.link_destinations(text):
            target = _local_target(root, path, dest)
            if target is None:
                continue
            try:
                if (root / target).is_dir() and (root / target / _INDEX).is_file():
                    target = posixpath.join(target, _INDEX)
            except OSError:
                continue  # a name too long or a locked directory: the link check reports it
            queue.append(target)
    files = sorted(f for f in docs.rglob("*") if f.is_file() and not f.is_symlink() and f.name not in _NOISE)
    problems += [f"{_label(root, f)} is not reachable from the root {_INDEX}" for f in files
                 if _label(root, f) not in reached]
    return problems


def _label(root: Path, path: Path) -> str:
    return path.relative_to(root).as_posix()


def test_every_document_is_reachable_from_the_readme() -> None:
    """F56 — every page and image under docs/ is reachable from README.md, index by index."""
    problems = _reachability_problems(_arch.ROOT)
    assert not problems, f"documentation a reader cannot reach: {problems}"


def test_every_reachability_check_can_fail() -> None:
    """F56 — anti-vacuity: each orphan or missing index is reported; the valid tree is clean."""
    import tempfile

    base = {
        "README.md": "# P\n\n[Docs](docs/) [C](CONTRIBUTING.md) [S](src/)\n\n![d](docs/images/diagram.png)\n"
                     "![e](docs/images/dark/diagram.png)\n",
        "docs/README.md": "# Docs\n\n[A](architecture/README.md) [G](guides/)\n",
        "docs/architecture/README.md": "# A\n\n[o](overview.md) [d](decisions/README.md)\n",
        "docs/architecture/overview.md": "# O\n",
        "docs/architecture/decisions/README.md": "# D\n\n[r](2026-01-01-x.md)\n",
        "docs/architecture/decisions/2026-01-01-x.md": "# X\n",
        "docs/guides/README.md": "# G\n\n[b](building.md#b)\n",
        "docs/guides/building.md": "# B\n",
        "docs/images/diagram.png": "png",
        "docs/images/dark/diagram.png": "png",
        "docs/guides/.DS_Store": "noise",
        "CLAUDE.md": "[z](docs/guides/z.md)\n",
        "CONTRIBUTING.md": "[z](docs/guides/z.md)\n",
        "src/README.md": "[z](../docs/guides/z.md)\n",
    }
    readme = base["README.md"]
    # Each mutation, and exactly what it must report.
    mutations: dict[str, tuple[dict[str, str | None], set[str]]] = {
        "an orphan page": ({"docs/guides/orphan.md": "# O\n"}, {"docs/guides/orphan.md"}),
        "an orphan image": ({"docs/images/orphan.png": "png"}, {"docs/images/orphan.png"}),
        "an orphan record": ({"docs/architecture/decisions/2026-01-02-y.md": "# Y\n"},
                             {"docs/architecture/decisions/2026-01-02-y.md"}),
        # CLAUDE.md, CONTRIBUTING.md and src/README.md all link z.md; none is documentation.
        "a page linked only from outside the docs": ({"docs/guides/z.md": "# Z\n"}, {"docs/guides/z.md"}),
        "a page linked only from an orphan": ({"docs/guides/z.md": "# Z\n", "docs/guides/orphan.md": "[z](z.md)\n"},
                                              {"docs/guides/z.md", "docs/guides/orphan.md"}),
        "a directory without an index": ({"docs/reference/load-context.md": "# L\n",
                                          "docs/README.md": "[r](reference/load-context.md) [A](architecture/) [G](guides/)\n"},
                                         {"docs/reference/"}),
        "the docs hub without an index": ({"docs/README.md": None, "README.md": readme.replace("(docs/)", "(docs/guides/)")
                                           + "[a](docs/architecture/)\n"}, {"docs/"}),
        **{f"a page 'linked' only by {label}": (
            {"docs/guides/orphan.md": "# O\n", "docs/guides/README.md": "[b](building.md#b)\n\n" + form + "\n"},
            {"docs/guides/orphan.md"})
           for label, form in {
               "an unused reference definition": "[orphan]: orphan.md",
               "an indented code block": "Para.\n\n    [x](orphan.md)",
               "an HTML block": "<div>\n[x](orphan.md)\n</div>",
               "a pre block": "<pre>\n[x](orphan.md)\n</pre>",
               "a definition inside a paragraph": "Some text\n[orphan]: orphan.md",
               "an unclosed comment": "<!-- start\n\n[x](orphan.md)",
               "a link title": '[y](building.md "<a href=\'orphan.md\'>")',
               "an escaped tag": '\\<a href="orphan.md">x</a>',
               "a nested link": "[[x](building.md)](orphan.md)",
               "a footnote whose link is broken": "Text[^n].\n\n[^n]: See [x](nope-orphan.md).",
           }.items()},
        "a nested directory without an index": ({"docs/guides/deep/x.md": "# X\n",
                                                 "docs/guides/README.md": "[b](building.md#b) [x](deep/x.md)\n"},
                                                {"docs/guides/deep/"}),
    }
    wrong = {}
    cases = {"the valid tree": ({}, set()), **mutations}
    for label, (change, expected) in cases.items():
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for rel_path, body in {**base, **change}.items():
                if body is None:
                    continue
                (root / rel_path).parent.mkdir(parents=True, exist_ok=True)
                (root / rel_path).write_text(body, encoding="utf-8")
            got = _reachability_problems(root)
        if {problem.split(" ", 1)[0] for problem in got} != expected or len(got) != len(expected):
            wrong[label] = got
    assert not wrong, f"these trees were not reported as expected: {wrong}"


def _check_mermaid() -> types.ModuleType:
    """scripts/check_mermaid.py as a module: the rules live once, in the script CI runs."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("check_mermaid", _arch.ROOT / "scripts" / "check_mermaid.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _mermaid_problems(kind: str) -> list[str]:
    check = _check_mermaid()
    blocks = check.repository_blocks()
    assert blocks, "no mermaid block found; CLAUDE.md §2 has one, so the scan is broken"
    return [f"{path}:{line}: {why}" for path, line, source in blocks
            for why in check.block_problems(source) if kind in why]


def test_no_index_based_linkStyle() -> None:
    """F57 — no diagram styles an edge by its index, which shifts when an edge is added."""
    problems = _mermaid_problems("by index")
    assert not problems, f"mermaid blocks style edges by index: {problems}"


def test_every_mermaid_block_declares_a_sanctioned_type() -> None:
    """F57 — every diagram opens with one of the types this repo draws with."""
    problems = _mermaid_problems("sanctioned types")
    assert not problems, f"mermaid blocks with an unsanctioned type: {problems}"


def test_the_mermaid_list_names_every_block() -> None:
    """F57 — `--list`, the diagrams to preview by hand (F58), names exactly the blocks checked here."""
    import subprocess
    import sys

    check = _check_mermaid()
    listed = subprocess.run([sys.executable, str(_arch.ROOT / "scripts" / "check_mermaid.py"), "--list"],
                            cwd=_arch.ROOT, capture_output=True, text=True, check=True).stdout.splitlines()
    assert listed == [f"{path}:{line}" for path, line, _ in check.repository_blocks()]
    assert any(entry.startswith("CLAUDE.md:") for entry in listed), f"CLAUDE.md's diagram is missing: {listed}"


def test_every_mermaid_check_can_fail() -> None:
    """F57 — anti-vacuity: each rule and each finding rule, mutated, reports; valid forms do not."""
    import os
    import subprocess
    import tempfile
    from unittest import mock

    check = _check_mermaid()
    good = "flowchart TD\n    A --> B\n    A -.-x|\"never\"| C\n"
    valid = {
        "a flowchart": good,
        "front matter, a directive and a comment first": "---\ntitle: T\n---\n%%{init: {}}%%\n%% note\n" + good,
        "a sequence diagram": "sequenceDiagram\n    A->>B: hi\n",
        "linkStyle default": good + "    linkStyle default stroke:#333\n",
        "a trailing semicolon": "stateDiagram-v2;\n    [*] --> A\n",
        # F57's adversary:
        "config front matter with a spaced opener": "---  \nconfig:\n  theme: dark\n---\n" + good,
        "a multi-line directive": '%%{\n  init: {"theme": "dark"}\n}%%\n' + good,
        "a directive then the type on one line": "%%{init: {}}%% " + good,
        "gitGraph with a colon": "gitGraph:\n    commit\n",
    }
    broken = {
        "the legacy graph keyword": "graph TD\n    A --> B\n",
        "an unsanctioned type": "pie\n    \"a\": 1\n",
        "an empty block": "\n%% only a comment\n",
        "a single index": good + "    linkStyle 0 stroke:#d33\n",
        "a list of indices": good + "    linkStyle 1,2 stroke:#d33\n",
        # F57's adversary:
        "an index mid-block": "flowchart TD\n    linkStyle 0 stroke:#d33\n    A --> B\n",
        "an index after a semicolon": "flowchart TD\n    A --> B; linkStyle 0 stroke:red\n",
        "an index in capitals": good + "    LINKSTYLE 0 stroke:red\n",
        "a sanctioned word as a prefix": "flowchartX TD\n    A --> B\n",
        "a banned type after a directive": "%%{init: {}}%% graph TD\n    A --> B\n",
    }
    wrong = {label: got for label, block in valid.items() if (got := check.block_problems(block))}
    wrong |= {label: got for label, block in broken.items() if len(got := check.block_problems(block)) != 1}
    assert not wrong, f"block rules misjudged: {wrong}"
    fences = {
        "a backtick fence": ("```mermaid\nflowchart TD\n```\n", [1]),
        "a tilde fence": ("~~~mermaid\nflowchart TD\n~~~\n", [1]),
        "a fence in a list item": ("- item\n\n  ```mermaid\n  flowchart TD\n  ```\n", [3]),
        "a capitalised language": ("```Mermaid\nflowchart TD\n```\n", [1]),
        "an info string with attributes": ("```mermaid title\nflowchart TD\n```\n", [1]),
        "an indented code block": ("Para.\n\n    ```mermaid\n    flowchart TD\n    ```\n", []),
        "inside another fence": ("````md\n```mermaid\nflowchart TD\n```\n````\n", []),
        "another language": ("```mermaidx\nflowchart TD\n```\n", []),
    }
    wrong = {label: got for label, (text, lines) in fences.items()
             if (got := [line for line, _ in check.mermaid_blocks(text)]) != lines}
    assert not wrong, f"mermaid fences misfound: {wrong}"
    # The developer's global core.excludesFile must not decide what this temp repo selects.
    isolated = {"GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"}
    with tempfile.TemporaryDirectory() as tmp, mock.patch.dict(os.environ, isolated):
        root = Path(tmp)
        subprocess.run(["git", "init", "-q"], cwd=root, check=True)
        names = ("tracked.md", "untracked.md", "scratch/ignored.md", "notes.txt", "a.MD", "b.Md",
                 "c.markdown", "d.MARKDOWN", "e.mdown", "f.mkd")
        for name in names:
            (root / name).parent.mkdir(parents=True, exist_ok=True)
            (root / name).write_text("x", encoding="utf-8")
        (root / ".gitignore").write_text("scratch/\n", encoding="utf-8")
        subprocess.run(["git", "add", "tracked.md"], cwd=root, check=True)
        found = [p.relative_to(root).as_posix() for p in check.markdown_files(root)]
        expected = ["a.MD", "b.Md", "c.markdown", "d.MARKDOWN", "e.mdown", "f.mkd", "tracked.md", "untracked.md"]
        assert found == expected, f"markdown files misselected: {found}"
        # The CLI's verdict and a byte-order mark before the first fence.
        (root / "tracked.md").write_bytes("\ufeff```mermaid\ngraph TD\n```\n".encode())
        assert check.main([], root=root) == 1, "check mode passed a banned diagram type"
        (root / "tracked.md").write_text("```mermaid\n" + good + "```\n", encoding="utf-8")
        assert check.main([], root=root) == 0, "check mode failed a valid diagram"


def test_every_reference_page_follows_the_format() -> None:
    """F61 — every page under docs/reference/ reads cleanly as containers and members, the one
    format the reference-versus-code tests (F67 onward) read. Holds nothing until the first
    page lands, and then holds every page from that moment."""
    reference = _DOCS / "reference"
    pages = sorted(p for p in reference.glob("*.md") if p.name != _INDEX) if reference.is_dir() else []
    problems = [f"{_arch.rel(page)}: {why}" for page in pages
                for why in _docs.reference_entries(page.read_text(encoding="utf-8"))[1]]
    assert not problems, f"reference pages depart from the format: {problems}"


@pytest.mark.xfail(strict=True, raises=AssertionError,
                   reason="F53 moved the legacy guides into docs/guides/; F97 deletes them")
def test_no_legacy_document_remains() -> None:
    """F97 removes this marker: strict mode turns the XPASS into a failure once they are gone."""
    remaining = [rel for rel in _LEGACY if (_DOCS / rel).exists()]
    assert not remaining, f"legacy documents still to be dissolved into their homes: {remaining}"


# --- F65: examples bind to the real API ---------------------------------------------------

# Pinned exemptions, by page and message (lines drift; messages do not). Only the two legacy
# guides may hold any: they are rewritten guide by guide in Stage E and deleted by F97, and
# the owner chose to pin them rather than fix them here (2026-09-28). A problem not listed
# fails, and so does a listed one that no longer occurs, so an exemption leaves the moment
# its example is fixed.
_EXAMPLE_EXEMPTIONS: dict[str, list[str]] = {}


def _example_pages(root: Path) -> list[Path]:
    """README.md and every page under docs/ except the dated records, which are history and
    quote code as it was."""
    decisions = root / "docs" / "architecture" / "decisions"
    return [p for p in _documents(root) if not p.is_relative_to(decisions)]


class ExampleCoverage(Warning):
    """How many calls the binding check could resolve on a page. Not a failure: the owner
    chose to count what cannot be resolved rather than fail on it (2026-09-28)."""


def _example_report(page: Path, schema: dict[str, set[str]]) -> tuple[list[tuple[int, str]], str]:
    text = page.read_text(encoding="utf-8")
    python = _examples.python_problems(text)
    contexts = _examples.context_problems(text, schema)
    coverage = (f"{_arch.rel(page)}: {python.resolved} calls bound, {python.unresolved} not resolvable, "
                f"{python.unparsed} Python blocks that do not parse, names called or dereferenced but never bound "
                f"{sorted(python.unbound)}, {contexts.unchecked_json} JSON blocks that are not load contexts")
    return sorted(python.problems + contexts.problems), coverage


def _ratchet_problems(found: dict[str, list[tuple[int, str]]], pinned: dict[str, list[str]]) -> list[str]:
    """Every problem not pinned, and every pinned exemption that no longer occurs."""
    problems = [f"{rel} is pinned, but only the legacy guides may carry exemptions"
                for rel in sorted(pinned) if rel not in {f"docs/{g}" for g in _LEGACY}]
    for rel in sorted(set(found) | set(pinned)):
        have = collections.Counter(message for _, message in found.get(rel, []))
        want = collections.Counter(pinned.get(rel, []))
        allowance = collections.Counter(want)
        for line, message in found.get(rel, []):
            if allowance[message] > 0:
                allowance[message] -= 1
            else:
                problems.append(f"{rel}:{line}: {message}")
        problems += [f"{rel}: pinned exemption no longer occurs, remove it: {message}"
                     for message in sorted((want - have).elements())]
    return problems


def test_every_example_binds_to_the_real_api() -> None:
    """F65 — every Python example calls what exists, as it is declared, and every load-context
    JSON example uses only keys the library reads. Pinned exemptions hold the legacy guides."""
    schema = _examples.context_schema()
    found: dict[str, list[tuple[int, str]]] = {}
    for page in _example_pages(_arch.ROOT):
        problems, coverage = _example_report(page, schema)
        warnings.warn(coverage, ExampleCoverage, stacklevel=1)
        if problems:
            found[_arch.rel(page)] = problems
    problems = _ratchet_problems(found, _EXAMPLE_EXEMPTIONS)
    assert not problems, f"examples that do not match the code: {problems}"


def test_the_context_schema_is_read_from_the_code() -> None:
    """F65 — anti-vacuity for the recording: every section has fields, fields that are certainly
    read are found, and a key the library only tests for in order to refuse it is not. An empty
    recording would pass every JSON example."""
    schema = _examples.context_schema()
    sections = {"global_context", "staging_context", "scd_context", "runtime_context", "logging_context",
                "hwm_context", "dqd_context"}
    # `extensions_context` is read whole and holds the user's keys, so it has none recorded of its own.
    assert schema[""] == sections | {"extensions_context"}, f"the recorded sections are not the load context's: {schema['']}"
    empty = [path for path in [*sections, "staging_context.stages[]"] if not schema.get(path)]
    assert not empty, f"the recording found no fields for {empty}"
    samples = {"runtime_context": "serverless", "scd_context": "business_key_column",
               "global_context": "entity_name", "logging_context": "log_file_name",
               "staging_context.stages[]": "nb_name"}
    missing = {path: key for path, key in samples.items() if key not in schema[path]}
    assert not missing, f"the recording missed fields the code certainly reads: {missing}"
    assert "parallel_execution_enabled" not in schema["global_context"],         "a key global_context refuses was recorded as one it reads"
    assert not schema.get("extensions_context"), (
        "the library read keys inside extensions_context; design decision 1 says the library "
        "never interprets it")
    assert "platform_context" not in schema[""], "a section the library refuses was recorded as one it reads"


def test_every_example_check_can_fail() -> None:
    """F65 — anti-vacuity: each departure, alone, is reported exactly once, and valid forms
    are not."""
    schema = _examples.context_schema()

    def py(*blocks: str) -> list[tuple[int, str]]:
        return _examples.python_problems("".join(f"```python\n{b}```\n\n" for b in blocks)).problems

    imports = "from spark_quick_start.spark_logging import init_logs\n"
    orch = "from spark_quick_start.orchestration import LoadOrchestrator\n"
    valid = {
        "a valid call": py(imports + "init_logs(level='INFO')\n"),
        "an import in an earlier block": py(imports, "init_logs(level='INFO')\n"),
        "a call inside a with block": py(imports + "with open('x') as f:\n    init_logs(level='INFO')\n"),
        "a call on a foreign module": py("import json\njson.dumps({}, indent=2, whatever=1)\n"),
        "an unknown receiver": py("spark.table('t', nonsense=1)\n"),
    }
    wrong: dict[str, object] = {label: got for label, got in valid.items() if got}
    python_mutations = {
        "an unknown keyword": py(imports + "init_logs(name='x')\n"),
        "an unknown keyword in a later block": py(imports, "init_logs(name='x')\n"),
        "too many positionals": py(imports + "init_logs('x')\n"),
        "a missing required argument": py(orch + "LoadOrchestrator('{}')\n"),
        "a method on a constructed value": py(orch + "o = LoadOrchestrator('{}', spark)\no.run(nonsense=1)\n"),
        "a method on a builder chain": py(orch + "LoadOrchestrator('{}', spark).add(None).run(nonsense=1)\n"),
        "a name the library does not have": py("from spark_quick_start.orchestration import NoSuchThing\n"),
        "a module the library does not have": py("import spark_quick_start.no_such_module\n"),
        "an attribute a module does not have": py("import spark_quick_start.orchestration as o\no.no_such_name()\n"),
        "a block that does not parse": py(".add(X())\n"),
        "a call inside a function body": py(imports + "def f():\n    init_logs(name='x')\n"),
        "a call inside a for body": py(imports + "for i in []:\n    init_logs(name='x')\n"),
    }
    wrong |= {label: got for label, got in python_mutations.items() if len(got) != 1}
    good = {"global_context": {"entity_name": "e"}, "runtime_context": {"serverless": True},
            "staging_context": {"stages": [{"nb_name": "n", "staging_context": {"free": 1}}]}}

    def js(doc: object, raw: str | None = None) -> list[tuple[int, str]]:
        body = raw if raw is not None else json.dumps(doc)
        return _examples.context_problems(f"```json\n{body}\n```\n", schema).problems

    if js(good):
        wrong["the valid load context"] = js(good)
    if js({"not": "a load context"}):
        wrong["a JSON block that is not a load context"] = js({"not": "a load context"})
    json_mutations = {
        "an unknown section": js({**good, "nonsense_context": {}}),
        "an unknown field": js({**good, "runtime_context": {"serverles": True}}),
        "an unknown global field": js({**good, "global_context": {"entity_name": "e", "staging_tabel_path": "p"}}),
        "an unknown stage key": js({**good, "staging_context": {"stages": [{"nb_name": "n", "timeout": 5}]}}),
        "a section that is not an object": js({**good, "runtime_context": []}),
        "stages that are not a list": js({**good, "staging_context": {"stages": {}}}),
        "a JSON block that does not parse": js(None, raw='{"global_context": {},}'),
    }
    wrong |= {label: got for label, got in json_mutations.items() if len(got) != 1}
    legacy = f"docs/{_LEGACY[0]}"
    ratchet = {
        "a problem not pinned": _ratchet_problems({legacy: [(1, "m")]}, {}),
        "a pinned exemption that no longer occurs": _ratchet_problems({}, {legacy: ["m"]}),
        "a second copy of a pinned problem": _ratchet_problems({legacy: [(1, "m"), (2, "m")]}, {legacy: ["m"]}),
        "an exemption on a page that is not legacy": _ratchet_problems({"README.md": [(1, "m")]},
                                                                        {"README.md": ["m"]}),
    }
    if _ratchet_problems({legacy: [(1, "m")]}, {legacy: ["m"]}):
        wrong["a pinned problem that still occurs"] = _ratchet_problems({legacy: [(1, "m")]}, {legacy: ["m"]})
    wrong |= {label: got for label, got in ratchet.items() if len(got) != 1}
    assert not wrong, f"the example checks misjudged: {wrong}"


def test_the_examples_bind_against_this_checkout() -> None:
    """F65 — the binding check imports the library to read its signatures, so it must import
    this checkout's src/, not an installed copy. On the author's machine a stale 0.3.7 from
    another checkout was first on the path outside pytest, and reported a name this tree
    exports as missing."""
    import spark_quick_start

    here = Path(spark_quick_start.__file__).resolve()
    source = (_arch.ROOT / "src" / "spark_quick_start").resolve()
    assert here.is_relative_to(source), f"the examples are bound against {here}, not {source}"


def test_every_adversary_form_is_judged() -> None:
    """F65 — the check-adversaries' bypasses, each reported once; their false alarms, clean;
    and their crashes, returned rather than raised. The forms a reader must not trust, such as
    a rebound name, are counted as unresolved, never bound."""
    schema = _examples.context_schema()

    def report(*blocks: str) -> _examples.Report:
        return _examples.python_problems("".join(f"```python\n{b}```\n\n" for b in blocks))

    tx = "from spark_quick_start.orchestration import transformers as tx\n"
    orch = "from spark_quick_start.orchestration import LoadOrchestrator\n"
    bag = "class Bag:\n    def add(self):\n        return 0\n"
    once = {
        "a missing attribute passed to a function": tx + "print(tx.NoSuchThing)\n",
        "a missing attribute in isinstance": tx + "isinstance(1, tx.NoSuchProtocol)\n",
        "a missing attribute as a statement": tx + "tx.NoSuchThing\n",
        "a missing attribute as a class base": tx + "class C(tx.NoSuchBase):\n    pass\n",
        "a missing attribute as a default": tx + "def f(a=tx.NoSuch):\n    pass\n",
        "a missing attribute as a keyword value": orch + tx + "LoadOrchestrator('j', None, adapter=tx.NoSuchThing)\n",
        "a misspelt package": "from spark_quickstart.orchestration import LoadOrchestrator\n",
        "a Protocol instantiated": "from spark_quick_start.orchestration.transformers import HWMProvider\n"
                                   "HWMProvider(anything=1)\n",
        "an abstract class instantiated": "from spark_quick_start.adapters import PlatformAdapter\nPlatformAdapter()\n",
        "a bad call in a default": orch + "def f(o=LoadOrchestrator(bogus=1)):\n    pass\n",
        "a bad call in a return annotation": orch + "def f() -> LoadOrchestrator(bogus=1):\n    pass\n",
        "a bad call as an except type": orch + "try:\n    pass\nexcept LoadOrchestrator(bogus=1):\n    pass\n",
        "a bad call in a match guard": orch + "match 1:\n    case 1 if LoadOrchestrator(bogus=1):\n        pass\n",
        "a submodule the package does not import": "import spark_quick_start\nspark_quick_start.spark_logging.init_logs()\n",
        "a call on what a factory returns": "from spark_quick_start.orchestration import default_pipeline\n"
                                            "default_pipeline('j', None).add(1, 2, 3)\n",
        "a misspelt attribute of a class": "from spark_quick_start.orchestration import ColumnNamingPolicy\n"
                                           "ColumnNamingPolicy.defualt()\n",
        "a literal keyword beside *args": orch + "LoadOrchestrator(*[], bogus=1)\n",
    }
    wrong: dict[str, object] = {label: got.problems for label, text in once.items()
                                if len((got := report(text)).problems) != 1}
    # Another page importing spark_logging first must not change the verdict (F65 adversary).
    report("from spark_quick_start.spark_logging import init_logs\n")
    _examples._bound_at_runtime.cache_clear()  # or the cached answer is all this would test
    if len(report(once["a submodule the package does not import"]).problems) != 1:
        wrong["a submodule, after another page imported it"] = "not reported"
    counted = {
        "a name rebound inside a function": orch + "def helper():\n    LoadOrchestrator = dict\n"
                                                   "    return LoadOrchestrator()\nLoadOrchestrator('{}', spark, bogus=1)\n",
        "a name rebound by a loop": orch + "o = LoadOrchestrator('{}', spark)\n" + bag + "for o in [Bag()]:\n    o.add(1)\n",
        "a name rebound by unpacking": orch + "o = LoadOrchestrator('{}', spark)\n" + bag + "o, n = Bag(), 0\no.add(1)\n",
        "super() in a subclass": "from spark_quick_start.orchestration import ColumnNamingPolicy\n"
                                 "class P(ColumnNamingPolicy):\n    def __init__(self):\n        super().__init__(bogus=1)\n",
        # F65's review: an import inside a function makes the package name untrusted on the
        # page, so the access is counted; before, it was replayed as if it ran.
        "a submodule imported only inside a function": "import spark_quick_start\n"
            "def f():\n    import spark_quick_start.spark_logging\nspark_quick_start.spark_logging.init_logs()\n",
    }
    for label, text in counted.items():
        got = report(text)
        if got.problems or got.unresolved < 1:
            wrong[label] = (got.problems, got.unresolved)
    clean = {
        "a loop variable of another type": orch + "o = LoadOrchestrator('{}', spark)\n" + bag + "for o in [Bag()]:\n    o.add()\n",
        "a comprehension variable": orch + "o = LoadOrchestrator('{}', spark)\n" + bag + "[o.add() for o in [Bag()]]\n",
        "a parameter": orch + "o = LoadOrchestrator('{}', spark)\ndef f(o):\n    return o.add()\n",
        "a walrus": orch + "o = LoadOrchestrator('{}', spark)\n" + bag + "if (o := Bag()):\n    o.add()\n",
        "a shadowing lambda": orch + "f = lambda LoadOrchestrator: LoadOrchestrator(bogus=1)\n",
        "a shadowing except name": orch + "try:\n    pass\nexcept Exception as LoadOrchestrator:\n    LoadOrchestrator(bogus=1)\n",
        "a notebook magic": "%pip install spark_quick_start\nimport json\n",
        "a star import": "from spark_quick_start.orchestration import *\n",
    }
    wrong |= {label: got.problems for label, text in clean.items() if (got := report(text)).problems}
    untagged = _examples.python_problems("```\n" + orch + "LoadOrchestrator('{}', spark, bogus=1)\n```\n")
    if len(untagged.problems) != 2:  # the tag, and the call it now checks
        wrong["a Python example in an untagged block"] = untagged.problems
    if "ctx" not in report(orch + "LoadOrchestrator('{}', spark).add_if(ctx.x, None)\n").unbound:
        wrong["a name used but never bound"] = "not counted"
    for label, text in {"a long chain": orch + "LoadOrchestrator('{}', spark)" + ".add(1)" * 400 + "\n",
                        "a deep attribute chain": "import spark_quick_start as s\ns" + ".a" * 3000 + "\n"}.items():
        try:
            report(text)
        except RecursionError:
            wrong[label] = "raised RecursionError"

    def ctx(body: str, tag: str = "json") -> list[tuple[int, str]]:
        return _examples.context_problems(f"```{tag}\n{body}\n```\n", schema).problems

    valid = json.dumps({"runtime_context": {"serverless": True}})
    json_once = {
        "every section misspelt": ctx('{"globalContext": {"entityName": "e"}}'),
        "a section named as the code's attribute": ctx('{"global_ctx": {"entity_name": "e"}}'),
        "a load context tagged javascript": ctx(valid, "javascript"),
        "a load context with no tag": ctx(valid, ""),
        "a tag with attributes after a comma": ctx('{"runtime_context": {"bogus": 1}}', "json,linenums"),
        "a repeated section": ctx('{"runtime_context": {"serverles": true}, "runtime_context": {"serverless": true}}'),
        "a removed dqf_context": ctx('{"dqf_context": {}}'),
        "a staging_table_schema that is not an object": ctx('{"global_context": {"staging_table_schema": [1]}}'),
        "nesting too deep to parse": ctx("[" * 100000),
    }
    wrong |= {label: got for label, got in json_once.items() if len(got) != 1}
    assert not wrong, f"the example checks misjudged adversary forms: {wrong}"


# --- F67: the load-context reference page matches the code ------------------------------

_LOAD_CONTEXT_PAGE = _DOCS / "reference" / "load-context.md"
# Which container on the page holds which recorded path (F65's `context_schema`). A container
# is the class that reads that part of the load context; a stage has no class of its own, so
# it is named by the attribute that holds the list.
_LOAD_CONTEXT_CONTAINERS = {
    "LoadContextManager": "",
    "GlobalContext": "global_context",
    "StagingContext": "staging_context",
    "StagingContext.stages": "staging_context.stages[]",
    "SCDContext": "scd_context",
    "RuntimeContext": "runtime_context",
    "LoggingContext": "logging_context",
    "HWMContext": "hwm_context",
    "DQDContext": "dqd_context",
    # The two sections below are not read key by key: the user's `extensions_context` is read whole
    # and `platform_context` is built by the library. Their rows are the class's public surface.
    "ExtensionsContext": "extensions_context",
    "PlatformContext": "platform_context",
}


def _extension_and_platform_surface() -> dict[str, set[str]]:
    """What the page lists for the two classes that are not read key by key: each one's public
    names, the fields of the frozen dataclass and the methods of the mapping view."""
    from spark_quick_start.orchestration.models import ExtensionsContext, PlatformContext

    return {"extensions_context": {n for n in dir(ExtensionsContext) if not n.startswith("_")},
            "platform_context": _class_members(PlatformContext)}


def _reference_diff_problems(containers: dict[str, list[str]], schema: dict[str, set[str]],
                             mapping: dict[str, str]) -> list[str]:
    """Every way a reference page's containers and members depart from the recorded keys, in
    both directions: a key the library reads with no row, and a row the library does not read."""
    # A recorded path the mapping does not name would have its keys demanded nowhere (F67 adversary).
    problems = [f"the library reads keys at `{path or '(top level)'}`, which no container is mapped to"
                for path in sorted(set(schema) - set(mapping.values()))]
    problems += [f"no container `{name}` for {path or 'the top-level sections'}"
                 for name, path in mapping.items() if name not in containers]
    problems += [f"container `{name}` is not one of {sorted(mapping)}" for name in containers if name not in mapping]
    for name, members in containers.items():
        if name not in mapping:
            continue
        read = schema.get(mapping[name], set())
        problems += [f"`{name}` has no row for `{key}`, which the code has" for key in sorted(read - set(members))]
        problems += [f"`{name}` lists `{key}`, which the code does not have" for key in sorted(set(members) - read)]
    return problems


def test_the_load_context_reference_matches_the_code() -> None:
    """F67 — every key the library reads has a row on docs/reference/load-context.md, and every
    row names a key it reads. The keys are recorded from the library's own parser (F65)."""
    assert _LOAD_CONTEXT_PAGE.is_file(), f"{_arch.rel(_LOAD_CONTEXT_PAGE)} is missing"
    containers, format_problems = _docs.reference_entries(_LOAD_CONTEXT_PAGE.read_text(encoding="utf-8"))
    assert not format_problems, f"the page departs from the reference format: {format_problems}"
    schema = {**_examples.context_schema(), **_extension_and_platform_surface()}
    problems = _reference_diff_problems(containers, schema, _LOAD_CONTEXT_CONTAINERS)
    assert not problems, f"the load-context reference and the code disagree: {problems}"


def test_every_reference_diff_check_can_fail() -> None:
    """F67 — anti-vacuity: each departure, alone, is reported exactly once."""
    schema = {"": {"a_context"}, "a_context": {"x", "y"}}
    mapping = {"Top": "", "A": "a_context"}
    good = {"Top": ["a_context"], "A": ["x", "y"]}
    assert _reference_diff_problems(good, schema, mapping) == [], "the matching page was reported"
    mutations = {
        "a container missing": {"Top": ["a_context"]},
        "a container the mapping does not know": {**good, "B": ["z"]},
        "a key with no row": {**good, "A": ["x"]},
        "a row the library does not read": {**good, "A": ["x", "y", "z"]},
    }
    wrong = {label: got for label, page in mutations.items()
             if len(got := _reference_diff_problems(page, schema, mapping)) != 1}
    unmapped = _reference_diff_problems(good, {**schema, "b_context": {"z"}}, mapping)
    if len(unmapped) != 1:
        wrong["a recorded path no container is mapped to"] = unmapped
    assert not wrong, f"the reference diff misjudged: {wrong}"


# --- F68: the column-naming reference page matches the code -----------------------------

_COLUMN_NAMING_PAGE = _DOCS / "reference" / "column-naming.md"


def _class_members(cls: type) -> set[str]:
    """A dataclass's public surface as a reference page lists it: every public name reachable on
    the class, inherited or not, plus every constructor argument and dataclass field, private ones
    included, since a user passes them. Names starting with `_` are not listed, dunders included.

    One rule rather than a list of descriptor types: a list missed inherited methods, `InitVar`
    arguments, `cached_property`, wrapped methods and class constants (F68 adversary). It is proven
    only for a dataclass whose bases are `object` or classes of our own, and refuses anything else:
    attributes set in `__init__` are not on the class, and a builtin base such as `NamedTuple` or
    `Exception` would add its own public names (F68 review).
    """
    import inspect

    if not dataclasses.is_dataclass(cls) or any(base.__module__ == "builtins" for base in cls.__mro__[1:-1]):
        raise TypeError(f"_class_members reads a dataclass on plain bases; {cls!r} is not one. "
                        "Extend it, with a probe, before a page relies on it.")
    reachable = {name for name in dir(cls) if not name.startswith("_")}
    fields = {f.name for f in dataclasses.fields(cls)} if dataclasses.is_dataclass(cls) else set()
    try:
        arguments = {name for name, p in inspect.signature(cls).parameters.items()
                     if p.kind not in (p.VAR_POSITIONAL, p.VAR_KEYWORD)}
    except (TypeError, ValueError):
        arguments = set()
    return reachable | fields | arguments


def test_the_column_naming_reference_matches_the_code() -> None:
    """F68 — every field and public method of ColumnNamingPolicy has a row on
    docs/reference/column-naming.md, and every row names one."""
    from spark_quick_start.orchestration import ColumnNamingPolicy

    assert _COLUMN_NAMING_PAGE.is_file(), f"{_arch.rel(_COLUMN_NAMING_PAGE)} is missing"
    containers, format_problems = _docs.reference_entries(_COLUMN_NAMING_PAGE.read_text(encoding="utf-8"))
    assert not format_problems, f"the page departs from the reference format: {format_problems}"
    name = ColumnNamingPolicy.__name__
    problems = _reference_diff_problems(containers, {name: _class_members(ColumnNamingPolicy)}, {name: name})
    assert not problems, f"the column-naming reference and the code disagree: {problems}"


def test_the_class_member_reader_can_fail() -> None:
    """F68 — anti-vacuity for `_class_members`: it finds fields, constructor arguments, methods
    of every kind, properties and constants, inherited ones included, and leaves out private
    names and dunders.

    `dataclasses` is imported at module level: under `from __future__ import annotations` a
    dataclass recognises the string "dataclasses.InitVar[...]" only through the module's globals, and a
    function-local import made the probe's InitVars ordinary fields (seen proving this test).
    """
    import functools
    from typing import ClassVar

    class Mixin:
        def mixed(self) -> None: ...

    @dataclasses.dataclass(frozen=True)
    class Base(Mixin):
        inherited: str = "x"

        def base_method(self) -> None: ...

    @dataclasses.dataclass(frozen=True)
    class Probe(Base):
        field_a: str = "a"
        _private_field: str = "p"
        legacy: dataclasses.InitVar[str | None] = None
        # Each found by one term alone, so deleting either term fails this test (F68 review):
        # a field outside the constructor with no class default, and a private constructor-only
        # argument with no default.
        derived: str = dataclasses.field(init=False)
        _token: dataclasses.InitVar[str] = dataclasses.field(kw_only=True)
        KIND: ClassVar[str] = "k"
        CONSTANT = "c"

        def method(self) -> None: ...

        @functools.cached_property
        def cached(self) -> str:
            return ""

        @functools.cache  # noqa: B019 — a probe, never called
        def memo(self) -> None: ...

        def __call__(self) -> None: ...

        @classmethod
        def factory(cls) -> None: ...

        @staticmethod
        def helper() -> None: ...

        @property
        def view(self) -> str:
            return ""

        def _private(self) -> None: ...

    found = _class_members(Probe)
    expected = {"inherited", "base_method", "mixed", "field_a", "_private_field", "legacy", "derived", "_token",
                "KIND", "CONSTANT", "method", "cached", "memo", "factory", "helper", "view"}
    # A private field is still a constructor argument; `_private` and `__call__` are not listed.
    assert found == expected, f"the member reader found {sorted(found)}, not {sorted(expected)}"
    import typing

    class NotADataclass:
        pass

    for refused in (NotADataclass, typing.NamedTuple("Row", [("a", str)])):
        try:
            _class_members(refused)
        except TypeError:
            continue
        raise AssertionError(f"_class_members read {refused!r}, which it is not proven for")


# --- F69: the transformer reference page matches the code -------------------------------

_TRANSFORMERS_PAGE = _DOCS / "reference" / "transformers.md"


def _is_protocol(cls: type) -> bool:
    """Whether `cls` is itself a protocol, not a class that implements one. `_is_protocol` is read
    from the class's own namespace: through inheritance a concrete subclass reads its protocol's
    `True` whenever the protocol's `__init_subclass__` did not reset it (F69 adversary)."""
    import typing

    return cls.__dict__.get("_is_protocol") is True and typing.Protocol in cls.__mro__


def _protocol_members(cls: type) -> set[str]:
    """A protocol's declared surface: every annotated attribute and every public name defined on
    it or on a protocol it extends. `typing.Protocol`, `Generic` and `object` add nothing.
    Dunder members such as `__call__` are not listed: only typing's private API names them
    exactly, and the package's protocols declare attributes alone (F69 adversary, declined)."""
    import inspect
    import typing

    members: set[str] = set()
    for klass in cls.__mro__:
        if klass in (typing.Protocol, typing.Generic, object) or not _is_protocol(klass):
            continue
        members |= set(inspect.get_annotations(klass))
        members |= {name for name in klass.__dict__ if not name.startswith("_")}
    return members


def _constructor_parameters(cls: type) -> tuple[set[str], str | None]:
    """The parameters a user passes to `cls(...)`, read from the `__init__` the call reaches, or
    the reason they cannot be read.

    Not `inspect.signature(cls)`, which guesses: it follows `__wrapped__`, trusts a class's
    `__signature__`, and resolves a protocol's placeholder `__init__(*args, **kwargs)` when a
    protocol is listed before the real base (F69 adversary). A class's `__signature__` is ignored
    here, since the `__init__` read is what the call runs, and that `__init__` is read from its
    code by `_function_parameters` (F72). Anything that is not one plain `__init__` function is
    reported: a metaclass `__call__`, a `__new__` of its own, a non-function `__init__`, one with
    no positional `self`, and `*args` or `**kwargs`, which a wrapping decorator shows as.
    """

    if type(cls).__call__ is not type.__call__:
        return set(), f"its metaclass `{type(cls).__name__}` defines `__call__`"
    if cls.__new__ is not object.__new__:
        return set(), "it defines `__new__`"
    init = next(klass.__dict__["__init__"] for klass in cls.__mro__ if "__init__" in klass.__dict__
                and getattr(klass.__dict__["__init__"], "__name__", "") != "_no_init_or_replace_init")
    if init is object.__init__:
        return set(), None
    return _function_parameters(init, what="`__init__`", bound=True)


def _function_parameters(fn: object, *, what: str = "it", bound: bool = False) -> tuple[set[str], str | None]:
    """The parameters a caller passes to one plain function, or the reason they cannot be read;
    with `bound`, the first parameter (`self`) is left out. Extracted from
    `_constructor_parameters` for F72, which reads module-level functions the same way.

    Read from the function's code object, which is what a call is checked against, not from
    `inspect.signature`, which prefers `__signature__`, `__text_signature__`, `__wrapped__` and a
    `partialmethod` over the code (F72 adversary). So a decorated function is read as the
    wrapper a caller actually calls. Anything but a plain `def` is reported, as are `*args`,
    `**kwargs`, and a bound function with no positional first parameter to drop.
    """
    import inspect

    if not isinstance(fn, types.FunctionType):
        return set(), f"{what} is not a plain function"
    code = fn.__code__
    if code.co_flags & (inspect.CO_VARARGS | inspect.CO_VARKEYWORDS):
        return set(), f"{what} takes `*args` or `**kwargs`, which name no parameter a page can list"
    if bound and code.co_argcount < 1:
        return set(), f"{what} has no positional first parameter to bind"
    names = code.co_varnames[:code.co_argcount + code.co_kwonlyargcount]
    return set(names[1:] if bound else names), None


def _module_reference_schema(module: types.ModuleType) -> tuple[dict[str, set[str]], list[str]]:
    """What a module's reference page must list, as container → members, and every name the
    reader cannot judge.

    The owner's shape for F69: one container named for the module, holding every name in
    `__all__`; one per exported protocol, holding its declared attributes; and one per exported
    class whose constructor takes parameters, holding those parameters. A class whose
    constructor takes none has no container, because a container needs at least one row.
    Whatever has no list of members to hold a page to is reported, never skipped: a name that is
    not a class, a constructor `_constructor_parameters` cannot read, a protocol with no members,
    a repeated name, and a name that would take the module's own container.
    """
    exported = getattr(module, "__all__", None)
    if not isinstance(exported, (list, tuple)) or not all(isinstance(n, str) for n in exported):
        return {}, [f"`{module.__name__}` has no `__all__` of names to hold a page to"]
    schema: dict[str, set[str]] = {module.__name__: set(exported)}
    problems = [f"`{name}` appears {count} times in `__all__`"
                for name, count in collections.Counter(exported).items() if count > 1]
    for name in dict.fromkeys(exported):
        cls = getattr(module, name, None)
        if name == module.__name__:
            problems.append(f"`{name}` is exported under the module's own name, which names its container")
        elif not isinstance(cls, type):
            problems.append(f"`{name}` is exported but is not a class, so its members cannot be read")
        elif _is_protocol(cls):
            if members := _protocol_members(cls):
                schema[name] = members
            else:
                problems.append(f"`{name}` is a protocol that declares no public member a page can list")
        else:
            parameters, unreadable = _constructor_parameters(cls)
            if unreadable:
                problems.append(f"`{name}`'s constructor cannot be read: {unreadable}")
            elif parameters:
                schema[name] = parameters
    return schema, problems


def test_the_transformer_reference_matches_the_code() -> None:
    """F69 — every name the transformers package exports has a row on
    docs/reference/transformers.md, every constructor parameter and protocol attribute has a row
    under its class, and every row names one."""
    from spark_quick_start.orchestration import transformers

    assert _TRANSFORMERS_PAGE.is_file(), f"{_arch.rel(_TRANSFORMERS_PAGE)} is missing"
    containers, format_problems = _docs.reference_entries(_TRANSFORMERS_PAGE.read_text(encoding="utf-8"))
    assert not format_problems, f"the page departs from the reference format: {format_problems}"
    schema, unreadable = _module_reference_schema(transformers)
    assert not unreadable, f"the transformers package has names the reference cannot hold: {unreadable}"
    problems = _reference_diff_problems(containers, schema, {name: name for name in schema})
    assert not problems, f"the transformer reference and the code disagree: {problems}"


def test_the_module_reference_reader_can_fail() -> None:
    """F69 — anti-vacuity for `_module_reference_schema`: a probe module with one export of every
    kind the reader distinguishes, each found by exactly one branch; the adversary's valid forms
    read correctly; and each unreadable form, including every bypass the adversary found, reported
    exactly once."""
    import enum
    import functools
    import inspect
    from typing import Protocol, runtime_checkable

    @runtime_checkable
    class BaseProto(Protocol):
        inherited_attr: int

    @runtime_checkable
    class Proto(BaseProto, Protocol):
        attr: str | None

        def method(self) -> None: ...

    class Takes:
        def __init__(self, positional: int, *, keyword: str = "k") -> None: ...

    class TakesNothing:
        pass

    # A protocol listed before the real base: its placeholder `__init__(*args, **kwargs)` comes
    # first in the MRO and must be skipped (adversary false alarm).
    class ProtoFirst(Proto, Takes):
        pass

    class ProtoFirstNothing(Proto, TakesNothing):
        pass

    # A protocol whose `__init_subclass__` skips `super()` leaves its `_is_protocol` visible to
    # a concrete subclass through inheritance; the subclass is still a class (adversary bypass).
    class SkipsSuper(Protocol):
        tag: str

        def __init_subclass__(cls, **kwargs: object) -> None: ...

    class InheritsFlag(SkipsSuper):
        def __init__(self, a: int) -> None: ...

    # A plain class that sets `_is_protocol` is not a protocol: typing.Protocol is not a base.
    def fake_init(self: object, a: int) -> None: ...

    Fake = type("Fake", (), {"_is_protocol": True, "attr": 1, "__init__": fake_init})

    probe = types.ModuleType("probe")
    for export in (Proto, Takes, TakesNothing, ProtoFirst, ProtoFirstNothing, InheritsFlag, Fake):
        setattr(probe, export.__name__, export)
    probe.__all__ = ["Proto", "Takes", "TakesNothing", "ProtoFirst", "ProtoFirstNothing", "InheritsFlag", "Fake"]
    expected = {"probe": set(probe.__all__),
                "Proto": {"inherited_attr", "attr", "method"},
                "Takes": {"positional", "keyword"},
                "ProtoFirst": {"positional", "keyword"},
                "InheritsFlag": {"a"},
                "Fake": {"a"}}
    schema, problems = _module_reference_schema(probe)
    assert (schema, problems) == (expected, []), f"the reader found {schema}, {problems}"

    # The adversary's bypasses, each now a form the reader must report.
    def deco(f):
        @functools.wraps(f)
        def wrapper(self, *args, extra=None, **kwargs):
            return f(self, *args, **kwargs)
        return wrapper

    class Wrapped:
        @deco
        def __init__(self, a: int) -> None: ...

    class SignatureLie:
        def __init__(self, *args: object, **kwargs: object) -> None: ...

    SignatureLie.__signature__ = inspect.Signature(  # type: ignore[attr-defined]
        [inspect.Parameter("x", inspect.Parameter.KEYWORD_ONLY)])

    class InitSignature:
        def __init__(self, *args: object, **kwargs: object) -> None: ...

    InitSignature.__init__.__signature__ = inspect.Signature(  # type: ignore[attr-defined]
        [inspect.Parameter("self", inspect.Parameter.POSITIONAL_OR_KEYWORD),
         inspect.Parameter("x", inspect.Parameter.KEYWORD_ONLY)])

    class DescriptorInit:
        class _Init:
            def __get__(self, obj: object, typ: type) -> object:
                return lambda a: None
        __init__ = _Init()

    class Callish(Protocol):
        def __call__(self, x: int) -> int: ...

    class Colour(enum.Enum):
        RED = 1

    class Variadic:
        def __init__(self, *args: object, **kwargs: object) -> None: ...

    class Meta(type):
        def __call__(cls, *args: object, **kwargs: object) -> object:
            return super().__call__(*args, **kwargs)

    class MetaCalled(metaclass=Meta):
        def __init__(self, a: int) -> None: ...

    class NewOwn:
        def __new__(cls, *args: object, **kwargs: object) -> NewOwn:
            return super().__new__(cls)

        def __init__(self, a: int) -> None: ...

    def unreadable(extra: dict[str, object], name: str = "probe", names: list[str] | None = None) -> list[str]:
        module = types.ModuleType(name)
        for key, value in extra.items():
            setattr(module, key, value)
        module.__all__ = names if names is not None else list(extra)
        return _module_reference_schema(module)[1]

    mutations = {
        "an __init__ that carries a __signature__": unreadable({"InitSignature": InitSignature}),
        "a wrapped __init__": unreadable({"Wrapped": Wrapped}),
        "a __signature__ over a variadic constructor": unreadable({"SignatureLie": SignatureLie}),
        "an __init__ that is a descriptor": unreadable({"DescriptorInit": DescriptorInit}),
        "a protocol with only dunder members": unreadable({"Callish": Callish}),
        "an Enum": unreadable({"Colour": Colour}),
        "a constructor with *args and **kwargs": unreadable({"Variadic": Variadic}),
        "a metaclass __call__": unreadable({"MetaCalled": MetaCalled}),
        "a __new__ of its own": unreadable({"NewOwn": NewOwn}),
        "a name repeated in __all__": unreadable({"Takes": Takes}, names=["Takes", "Takes"]),
        "an export named after the module": unreadable({"probe": Takes}),
        "a function exported": unreadable({"helper": len}),
        "a constant exported": unreadable({"LIMIT": 3}),
        "a name in __all__ the module lacks": unreadable({"Missing": None}),
        "no __all__": _module_reference_schema(types.ModuleType("bare"))[1],
        "an __all__ that is not names": _module_reference_schema(
            types.SimpleNamespace(__name__="odd", __all__=[1]))[1],  # type: ignore[arg-type]
    }
    wrong = {label: got for label, got in mutations.items() if len(got) != 1}
    assert not wrong, f"the module reader misjudged: {wrong}"


# --- F70: the platform-adapter reference page matches the code --------------------------

_PLATFORM_ADAPTERS_PAGE = _DOCS / "reference" / "platform-adapters.md"


def _public_names(cls: type) -> set[str]:
    """Every public name a class declares, inherited ones included: its methods, properties and
    constants, and its annotated attributes, which an implementer sets. Read from each class's
    own namespace along the MRO, not from `dir()`, which a metaclass `__dir__` can rewrite (F70
    adversary). Names starting with `_` are not listed."""
    import inspect

    names: set[str] = set()
    for klass in cls.__mro__:
        if klass is object:
            continue
        names |= set(vars(klass)) | set(inspect.get_annotations(klass))
    return {name for name in names if not name.startswith("_")}


def _instance_attributes(cls: type) -> tuple[set[str], str | None]:
    """The public attributes a freshly constructed instance carries in its `__dict__`, or the
    reason they cannot be read.

    Only a fresh instance built with no arguments is read, as the owner chose: an attribute
    created later, or only for a non-default argument, is not seen. A class that can hide
    instance attributes from `vars()` is reported rather than read: slots, or an attribute hook
    (`__getattr__`, `__getattribute__`, `__dir__`) anywhere below `object` (F70 adversary).
    """
    hooks = ("__getattr__", "__getattribute__", "__dir__")
    for klass in cls.__mro__:
        if klass is object:
            continue
        if vars(klass).get("__slots__"):
            return set(), f"`{klass.__name__}` declares `__slots__`, whose attributes `vars()` does not show"
        if hook := next((h for h in hooks if h in vars(klass)), None):
            return set(), f"`{klass.__name__}` defines `{hook}`, which can hide instance attributes"
    try:
        instance = cls()
    except Exception as exc:  # any failure means the attributes cannot be read
        return set(), f"`{cls.__name__}()` cannot be constructed with no arguments: {exc!r}"
    if not hasattr(instance, "__dict__"):
        return set(), f"`{cls.__name__}` instances have no `__dict__` to read"
    return {name for name in vars(instance) if not name.startswith("_")}, None


def _adapter_reference_schema() -> tuple[dict[str, set[str]], list[str]]:
    """What docs/reference/platform-adapters.md must list: F69's module schema for the adapters
    package, plus a `PlatformAdapter` container of its public names, plus, in
    `MockPlatformAdapter`'s container, every public name a Mock has that `PlatformAdapter` does
    not: its recording attributes and any method, property or constant of its own (the owner's
    shape, 2026-09-28; widened by the F70 adversary)."""
    from spark_quick_start import adapters

    schema, problems = _module_reference_schema(adapters)
    interface = _public_names(adapters.PlatformAdapter)
    # Joined, not replaced: a constructor parameter added to either class needs its row too.
    schema["PlatformAdapter"] = schema.get("PlatformAdapter", set()) | interface
    recorded, unreadable = _instance_attributes(adapters.MockPlatformAdapter)
    if unreadable:
        problems.append(unreadable)
    mock = schema.get("MockPlatformAdapter", set()) | ((recorded | _public_names(adapters.MockPlatformAdapter)) - interface)
    # A container needs at least one row, so a Mock with nothing of its own has none (F70 adversary).
    if mock:
        schema["MockPlatformAdapter"] = mock
    return schema, problems


def test_the_platform_adapter_reference_matches_the_code() -> None:
    """F70 — every name the adapters package exports, every public method of PlatformAdapter,
    every constructor parameter, and every recording list of MockPlatformAdapter has a row on
    docs/reference/platform-adapters.md, and every row names one."""
    assert _PLATFORM_ADAPTERS_PAGE.is_file(), f"{_arch.rel(_PLATFORM_ADAPTERS_PAGE)} is missing"
    containers, format_problems = _docs.reference_entries(_PLATFORM_ADAPTERS_PAGE.read_text(encoding="utf-8"))
    assert not format_problems, f"the page departs from the reference format: {format_problems}"
    schema, unreadable = _adapter_reference_schema()
    assert not unreadable, f"the adapters package has names the reference cannot hold: {unreadable}"
    problems = _reference_diff_problems(containers, schema, {name: name for name in schema})
    assert not problems, f"the platform-adapter reference and the code disagree: {problems}"


def test_the_adapter_readers_can_fail() -> None:
    """F70 — anti-vacuity for `_public_names`, `_instance_attributes` and their composition: each
    finds exactly its kind of member, inherited ones included and private ones left out; each
    unreadable form, including the adversary's, is reported; and the composition gives a Mock's
    own names a row and an empty Mock no container."""
    import abc
    from unittest import mock

    from spark_quick_start import adapters

    class Base(abc.ABC):
        LIMIT = 3
        declared: str  # an interface attribute an implementer sets; only annotated here

        @abc.abstractmethod
        def abstract(self) -> None: ...

        def inherited(self) -> None: ...

        def _private(self) -> None: ...

    class Probe(Base):
        def abstract(self) -> None: ...

        @property
        def view(self) -> int:
            return 0

        @classmethod
        def factory(cls) -> None: ...

        @staticmethod
        def helper() -> None: ...

    found = _public_names(Probe)
    assert found == {"LIMIT", "declared", "abstract", "inherited", "view", "factory", "helper"}, \
        f"public names: {sorted(found)}"

    class Lying(abc.ABCMeta):
        def __dir__(cls) -> list[str]:
            return ["only_this"]

    class DirLies(metaclass=Lying):
        def run(self) -> None: ...

    assert _public_names(DirLies) == {"run"}, f"a metaclass __dir__ steered the reader: {_public_names(DirLies)}"

    class Records:
        LIMIT = 1

        def __init__(self) -> None:
            self.calls: list[str] = []
            self._hidden = 0
            self.LIMIT = 2  # shadows a class constant, and is still an instance attribute

    attributes, problem = _instance_attributes(Records)
    assert (attributes, problem) == ({"calls", "LIMIT"}, None), f"instance attributes: {attributes}, {problem}"

    class NeedsArgs:
        def __init__(self, a: int) -> None: ...

    class Slotted:
        __slots__ = ("calls",)

        def __init__(self) -> None:
            self.calls: list[str] = []

    class DictBase:
        pass

    class SlotsOverDict(DictBase):
        __slots__ = ("calls",)

    class Hooked:
        def __getattr__(self, name: str) -> object:
            return {}

    class Raises:
        def __init__(self) -> None:
            raise RuntimeError("no")

    unreadable = {"a constructor that needs arguments": NeedsArgs, "instances with __slots__": Slotted,
                  "slots over an inherited __dict__": SlotsOverDict, "a __getattr__ hook": Hooked,
                  "a constructor that raises": Raises}
    wrong = {label: got for label, cls in unreadable.items() if (got := _instance_attributes(cls))[1] is None}
    assert not wrong, f"the instance reader read what it cannot: {wrong}"

    # The composition: a Mock's own property and a class-level recording list get rows; an
    # unreadable Mock is forwarded; a Mock with nothing of its own gets no container.
    class OwnNames(adapters.PlatformAdapter):
        run_calls: tuple[str, ...] = ()

        def run_notebook(self, name: str, timeout: int, args: dict) -> str: ...
        def run_notebooks_parallel(self, dag: dict) -> dict: ...
        def delete_path(self, path: str) -> None: ...
        def get_current_notebook_name(self) -> str | None: ...

        @property
        def last_call(self) -> str:
            return ""

    class NothingOwn(OwnNames):
        run_calls = None  # type: ignore[assignment]
        last_call = None  # type: ignore[assignment]

    def composed(fake: type) -> tuple[set[str] | None, list[str]]:
        with mock.patch.object(adapters, "MockPlatformAdapter", fake):
            schema, problems = _adapter_reference_schema()
        return schema.get("MockPlatformAdapter"), problems

    own = composed(OwnNames)
    assert own == ({"run_calls", "last_call"}, []), f"a Mock's own names: {own}"
    forwarded = composed(NeedsArgs)[1]
    assert any("cannot be constructed" in problem for problem in forwarded), f"not forwarded: {forwarded}"
    with mock.patch.object(adapters.PlatformAdapter, "run_calls", None, create=True), \
            mock.patch.object(adapters.PlatformAdapter, "last_call", None, create=True):
        empty = composed(NothingOwn)
    assert empty == (None, []), f"a Mock with nothing of its own: {empty}"


# --- F71: the execution-result reference page matches the code --------------------------

_EXECUTION_RESULT_PAGE = _DOCS / "reference" / "execution-result.md"


def test_the_execution_result_reference_matches_the_code() -> None:
    """F71 — every field and public method of ExecutionResult, ExecutionMetrics, StageMetrics and
    ErrorDetails has a row on docs/reference/execution-result.md, and every row names one. The
    classes are read by F68's `_class_members`, which refuses anything but a dataclass on plain
    bases."""
    from spark_quick_start.orchestration import ErrorDetails, ExecutionMetrics, ExecutionResult
    from spark_quick_start.orchestration.metrics import StageMetrics

    assert _EXECUTION_RESULT_PAGE.is_file(), f"{_arch.rel(_EXECUTION_RESULT_PAGE)} is missing"
    containers, format_problems = _docs.reference_entries(_EXECUTION_RESULT_PAGE.read_text(encoding="utf-8"))
    assert not format_problems, f"the page departs from the reference format: {format_problems}"
    schema = {cls.__name__: _class_members(cls) for cls in (ExecutionResult, ExecutionMetrics, StageMetrics, ErrorDetails)}
    problems = _reference_diff_problems(containers, schema, {name: name for name in schema})
    assert not problems, f"the execution-result reference and the code disagree: {problems}"


# --- F72: the orchestrator reference page matches the code ------------------------------

_ORCHESTRATOR_PAGE = _DOCS / "reference" / "orchestrator.md"


def _orchestrator_reference_schema() -> tuple[dict[str, set[str]], list[str]]:
    """What docs/reference/orchestrator.md must list: `LoadOrchestrator`'s constructor
    parameters and public names; `LoadOrchestrator.platform`, the keys of `_PLATFORM_MAP` that
    the `platform` argument accepts; and the parameters of every pipeline factory the package
    exports (every `*_pipeline` in `spark_quick_start.orchestration.__all__`, F159)."""
    import spark_quick_start.orchestration as orchestration
    from spark_quick_start.orchestration import LoadOrchestrator
    from spark_quick_start.orchestration.orchestrator import _PLATFORM_MAP

    problems: list[str] = []
    parameters, unreadable = _constructor_parameters(LoadOrchestrator)
    problems += [f"`LoadOrchestrator`'s constructor cannot be read: {unreadable}"] if unreadable else []
    schema = {"LoadOrchestrator": parameters | _public_names(LoadOrchestrator),
              "LoadOrchestrator.platform": set(_PLATFORM_MAP)}
    # Keyed by the exported name, not `__name__`, which a decorator can rename (F72 adversary).
    factories = {n: getattr(orchestration, n) for n in orchestration.__all__ if n.endswith("_pipeline")}
    missing = {"default_pipeline"} - set(factories)
    problems += [f"the package no longer exports {sorted(missing)}"] if missing else []
    for name, factory in factories.items():
        found, unreadable = _function_parameters(factory, what=f"`{name}`")
        problems += [unreadable] if unreadable else []
        if getattr(factory, "__name__", None) != name:
            problems.append(f"`{name}` is a function named `{getattr(factory, '__name__', None)}`, so it is not the one defined")
        schema[name] = found
    return schema, problems


def test_the_orchestrator_reference_matches_the_code() -> None:
    """F72 — every constructor parameter and public method of LoadOrchestrator, every platform
    string it accepts, and every parameter of the pipeline factories has a row on
    docs/reference/orchestrator.md, and every row names one."""
    assert _ORCHESTRATOR_PAGE.is_file(), f"{_arch.rel(_ORCHESTRATOR_PAGE)} is missing"
    containers, format_problems = _docs.reference_entries(_ORCHESTRATOR_PAGE.read_text(encoding="utf-8"))
    assert not format_problems, f"the page departs from the reference format: {format_problems}"
    schema, unreadable = _orchestrator_reference_schema()
    assert not unreadable, f"the orchestrator API has members the reference cannot hold: {unreadable}"
    problems = _reference_diff_problems(containers, schema, {name: name for name in schema})
    assert not problems, f"the orchestrator reference and the code disagree: {problems}"


_PIPELINE_STATE_PAGE = _DOCS / "reference" / "pipeline-state.md"


def test_the_pipeline_state_reference_matches_the_code() -> None:
    """F28 — every field and public method of PipelineState has a row on
    docs/reference/pipeline-state.md, and every row names one."""
    from spark_quick_start.orchestration import PipelineState

    assert _PIPELINE_STATE_PAGE.is_file(), f"{_arch.rel(_PIPELINE_STATE_PAGE)} is missing"
    containers, format_problems = _docs.reference_entries(_PIPELINE_STATE_PAGE.read_text(encoding="utf-8"))
    assert not format_problems, f"the page departs from the reference format: {format_problems}"
    schema = {"PipelineState": _public_names(PipelineState)}
    problems = _reference_diff_problems(containers, schema, {name: name for name in schema})
    assert not problems, f"the pipeline-state reference and the code disagree: {problems}"


def test_the_function_parameter_reader_can_fail() -> None:
    """F72 — anti-vacuity for `_function_parameters`: a plain function's parameters of every
    kind are read, `bound` drops exactly the first, and each unreadable form is reported."""
    import functools
    import inspect

    def plain(a: int, /, b: int, *, c: int = 1) -> None: ...

    assert _function_parameters(plain) == ({"a", "b", "c"}, None), _function_parameters(plain)
    assert _function_parameters(plain, bound=True) == ({"b", "c"}, None), _function_parameters(plain, bound=True)

    def variadic(*args: int, **kwargs: int) -> None: ...

    @functools.wraps(plain)
    def wrapped(*args: int, **kwargs: int) -> None: ...

    def signed(*args: int) -> None: ...

    signed.__signature__ = inspect.Signature([inspect.Parameter("x", inspect.Parameter.KEYWORD_ONLY)])  # type: ignore[attr-defined]

    unreadable = {"*args and **kwargs": variadic, "a wrapped function": wrapped, "a __signature__": signed,
                  "a builtin": len, "a functools.partial": functools.partial(plain, 1),
                  "a callable object": type("Callable", (), {"__call__": lambda self, a: None})()}
    # The adversary's forms: `inspect` would trust `__text_signature__` and a `partialmethod`
    # over the code; each must now be read from the code (a control) or reported.
    def texted(a: int, b: int) -> None: ...

    texted.__text_signature__ = "(x)"  # type: ignore[attr-defined]

    def text_hides_variadic(*args: int, **kwargs: int) -> None: ...

    text_hides_variadic.__text_signature__ = "(a, b)"  # type: ignore[attr-defined]

    def malformed(a: int) -> None: ...

    malformed.__text_signature__ = "(((("  # type: ignore[attr-defined]

    def target(self: object, a: int, b: int, *, c: int = 1) -> None: ...

    class Partial:
        pm = functools.partialmethod(target, 1)

    def unwrapped_decorator(f: object) -> object:
        def inner(load_context_json: str, spark: object) -> None: ...
        return inner

    controls = {"a __text_signature__ over plain code": (texted, {"a", "b"}),
                "a malformed __text_signature__": (malformed, {"a"}),
                "a decorator without wraps, read as the wrapper": (unwrapped_decorator(plain), {"load_context_json", "spark"})}
    misread = {label: got for label, (fn, want) in controls.items() if (got := _function_parameters(fn)) != (want, None)}
    assert not misread, f"the function reader misread plain code: {misread}"
    # Carries a code object but is not a function, as a compiled callable can (adversary form).
    code_carrier = type("CodeCarrier", (), {"__code__": plain.__code__, "__call__": lambda self, *a: None})()
    unreadable |= {"a __text_signature__ hiding *args": text_hides_variadic, "a partialmethod fetched from its class": Partial.pm,
                   "a callable carrying __code__": code_carrier}
    wrong = {label: got for label, fn in unreadable.items() if (got := _function_parameters(fn))[1] is None}
    assert not wrong, f"the function reader read what it cannot: {wrong}"

    class NoSelf:
        def __init__() -> None: ...  # type: ignore[misc]

    class KeywordSelf:
        def __init__(*, self: object, a: int) -> None: ...  # type: ignore[misc]

    unbindable = {label: got for label, cls in {"an __init__ with no parameters": NoSelf,
                                                 "an __init__ whose self is keyword-only": KeywordSelf}.items()
                  if (got := _constructor_parameters(cls))[1] is None}
    assert not unbindable, f"the constructor reader bound what has no self: {unbindable}"

    # The schema keys a factory by its exported name and reports one a decorator renamed.
    from unittest import mock

    from spark_quick_start import orchestration

    with mock.patch.object(orchestration, "default_pipeline", unwrapped_decorator(plain)):
        renamed = _orchestrator_reference_schema()[1]
    assert any("named `inner`" in problem for problem in renamed), f"a renamed factory was not reported: {renamed}"


# --- F73: the logging reference page matches the code -----------------------------------

_LOGGING_PAGE = _DOCS / "reference" / "logging.md"


def _defined_public_names(module: types.ModuleType) -> tuple[set[str], list[str]]:
    """The public classes and functions a package defines, for a package with no `__all__`:
    every public name whose value is a class or function whose `__module__` is the package or
    one of its submodules.

    Left out: private names, modules, `None` (a filtered build's placeholder), builtin functions
    (a package cannot define one), typing constructs (`TYPE_CHECKING`, aliases, unions,
    `TypeVar`s and the like) and `__future__` features, and classes and functions from outside
    this library. Reported: a class or function whose `__module__` is elsewhere in this library,
    which is a wrapper or a moved definition the reader cannot place (F73 adversary), and any
    other public value, such as a constant, since the reader cannot tell whether it is API.
    Declined: a class whose `__module__` claims the standard library, as `make_dataclass`
    leaves it; F121's `__all__` is the real fix.
    """
    typing_modules = {"typing", "types", "typing_extensions", "collections.abc", "_collections_abc", "__future__"}
    names: set[str] = set()
    problems: list[str] = []
    for name, value in vars(module).items():
        if name.startswith("_") or name == "TYPE_CHECKING" or value is None or isinstance(value, types.ModuleType):
            continue
        if isinstance(value, types.BuiltinFunctionType):
            continue
        # Classes first: a Protocol defined here has a metaclass from `typing`, and the typing
        # skip below must not reach it.
        if isinstance(value, (type, types.FunctionType)):
            owner = getattr(value, "__module__", None)
            if not isinstance(owner, str):
                problems.append(f"`{name}` has no module name to place it by")
            elif owner == module.__name__ or owner.startswith(module.__name__ + "."):
                names.add(name)
            elif owner.split(".")[0] == module.__name__.split(".")[0]:
                problems.append(f"`{name}` comes from `{owner}`, elsewhere in this library; the reader cannot tell a "
                                "re-export from a wrapped or moved definition")
            continue
        if type(value).__module__ in typing_modules:
            continue
        problems.append(f"`{name}` is a public {type(value).__name__} the reader cannot classify")
    return names, problems


def test_spark_logging_declares_its_public_names() -> None:
    """F121 — `spark_logging` names its surface in `__all__`, as `adapters` does.

    On the full trunk `__all__` holds exactly what the package defines publicly, once each.
    Leaving out an adapter a filtered build removed is a branch the trunk never takes; it is
    simulated in tests/unit/test_optional_persistence_imports.py.
    """
    from spark_quick_start import spark_logging

    exported = getattr(spark_logging, "__all__", None)
    assert isinstance(exported, list), "spark_logging has no `__all__`"
    assert len(exported) == len(set(exported)), f"a name repeats in `__all__`: {exported}"
    defined, unclassified = _defined_public_names(spark_logging)
    assert not unclassified, unclassified
    assert set(exported) == defined, (
        f"`__all__` and what spark_logging defines differ: only in `__all__` "
        f"{sorted(set(exported) - defined)}, only defined {sorted(defined - set(exported))}")


def _logging_reference_schema() -> tuple[dict[str, set[str]], list[str]]:
    """What docs/reference/logging.md must list: `OrchestratorLogger`; a catalogue of what
    `spark_quick_start.spark_logging` defines; for each of those, its members — a dataclass's
    fields, a function's parameters, a class's constructor parameters and public names, and for
    a `LogPersistenceAdapter` subclass only what it adds to the interface, plus the recording
    attributes of one that constructs with no arguments. A member set that is empty gets no
    container, since a container needs a row."""
    from spark_quick_start import spark_logging
    from spark_quick_start.logger import OrchestratorLogger

    problems: list[str] = []

    def class_members(cls: type, interface: set[str]) -> set[str]:
        if dataclasses.is_dataclass(cls):
            try:
                return _class_members(cls)
            except TypeError as exc:
                problems.append(f"`{cls.__name__}` cannot be read: {exc}")
                return set()
        parameters, unreadable = _constructor_parameters(cls)
        if unreadable:
            problems.append(f"`{cls.__name__}`'s constructor cannot be read: {unreadable}")
        members = parameters | (_public_names(cls) - interface)
        if interface:
            # A double that constructs with no arguments, optional parameters included, has its
            # recording attributes read (F73 adversary). One that needs arguments is a real
            # adapter whose attributes mirror its parameters; failing to build it is expected.
            recorded, unreadable = _instance_attributes(cls)
            if unreadable and not parameters:
                problems.append(unreadable)
            members |= recorded - interface
        return members

    catalogue, unclassified = _defined_public_names(spark_logging)
    problems += unclassified
    schema: dict[str, set[str]] = {"OrchestratorLogger": class_members(OrchestratorLogger, set()),
                                   "spark_quick_start.spark_logging": catalogue}
    interface = _public_names(spark_logging.LogPersistenceAdapter)
    for name in sorted(catalogue):
        value = getattr(spark_logging, name)
        if isinstance(value, types.FunctionType):
            members, unreadable = _function_parameters(value, what=f"`{name}`")
            problems.extend([unreadable] if unreadable else [])
        elif value is spark_logging.LogPersistenceAdapter:
            members = interface
        else:
            members = class_members(value, interface if issubclass(value, spark_logging.LogPersistenceAdapter) else set())
        if name in schema:
            problems.append(f"`{name}` in spark_logging takes the name of a container already on the page")
        elif members:
            schema[name] = members
    return schema, problems


def test_the_logging_reference_matches_the_code() -> None:
    """F73 — every member of OrchestratorLogger and of what spark_logging defines has a row on
    docs/reference/logging.md, and every row names one."""
    assert _LOGGING_PAGE.is_file(), f"{_arch.rel(_LOGGING_PAGE)} is missing"
    containers, format_problems = _docs.reference_entries(_LOGGING_PAGE.read_text(encoding="utf-8"))
    assert not format_problems, f"the page departs from the reference format: {format_problems}"
    schema, unreadable = _logging_reference_schema()
    assert not unreadable, f"the logging API has members the reference cannot hold: {unreadable}"
    problems = _reference_diff_problems(containers, schema, {name: name for name in schema})
    assert not problems, f"the logging reference and the code disagree: {problems}"


def test_the_logging_readers_can_fail() -> None:
    """F73 — anti-vacuity for `_defined_public_names` and the logging composition: a package's
    own classes and functions are found, imports and private names left out, an unclassifiable
    public name reported; and the composition gives an interface implementation only what it
    adds, a no-argument double its recording attributes, and an empty member set no container."""
    import abc
    import logging
    import typing
    from unittest import mock

    from spark_quick_start import spark_logging

    probe = types.ModuleType("probe")
    probe.json = json  # a module
    probe.Logger = logging.Logger  # a class another package defines
    probe.Any = typing.Any  # a typing construct
    probe.Filtered = None  # a filtered build's placeholder

    def helper() -> None: ...

    class Own:
        pass

    class Sub:
        pass

    helper.__module__ = Own.__module__ = "probe"
    Sub.__module__ = "probe.sub"
    probe.helper, probe.Own, probe.Sub, probe._private = helper, Own, Sub, Own
    found, problems = _defined_public_names(probe)
    assert (found, problems) == ({"helper", "Own", "Sub"}, []), f"defined names: {found}, {problems}"
    probe.LIMIT = 3
    assert len(_defined_public_names(probe)[1]) == 1, "a public constant was not reported"
    del probe.LIMIT

    # Adversary controls: typing constructs, builtins and a Protocol defined here.
    import math
    from typing import Protocol

    class Declared(Protocol):
        attr: int

    Declared.__module__ = "probe"
    controls = {"TYPE_CHECKING": typing.TYPE_CHECKING, "Rows": list[dict], "Maybe": str | None,
                "T": typing.TypeVar("T"), "RunId": typing.NewType("RunId", str), "floor": math.floor,
                "Declared": Declared}
    for key, value in controls.items():
        setattr(probe, key, value)
    found, problems = _defined_public_names(probe)
    assert (found, problems) == ({"helper", "Own", "Sub", "Declared"}, []), f"controls: {found}, {problems}"

    # Adversary mutations: each reported exactly once.
    def elsewhere() -> None: ...

    elsewhere.__module__ = "spark_quick_start.common"
    renamed = type("Renamed", (), {})
    renamed.__module__ = 3
    for key, value in {"elsewhere": elsewhere, "Renamed": renamed}.items():
        mutated = types.ModuleType("spark_quick_start.probe")
        setattr(mutated, key, value)
        assert len(_defined_public_names(mutated)[1]) == 1, f"{key} was not reported"

    class Interface(abc.ABC):
        @abc.abstractmethod
        def write(self) -> None: ...

    class Double(Interface):
        def __init__(self) -> None:
            self.written: list[str] = []

        def write(self) -> None: ...

        def extra(self) -> None: ...

    class Plain(Interface):
        def write(self) -> None: ...

    with mock.patch.object(spark_logging, "LogPersistenceAdapter", Interface), \
            mock.patch.object(spark_logging, "Double", Double, create=True), \
            mock.patch.object(spark_logging, "Plain", Plain, create=True):
        Double.__module__ = Plain.__module__ = Interface.__module__ = spark_logging.__name__
        schema, problems = _logging_reference_schema()
    assert problems == [], problems
    assert schema.get("Double") == {"written", "extra"}, f"a double: {schema.get('Double')}"
    assert "Plain" not in schema, f"an implementation adding nothing got a container: {schema.get('Plain')}"
    assert schema.get("LogPersistenceAdapter") == {"write"}, f"the interface: {schema.get('LogPersistenceAdapter')}"

    class OptionalDouble(Interface):
        def __init__(self, fail: bool = False) -> None:
            self.written: list[str] = []

        def write(self) -> None: ...

    @dataclasses.dataclass
    class LogFailure(Exception):
        reason: str = ""

    def clash(y: int) -> None: ...

    class Unbuildable(Interface):
        def __init__(self) -> None:
            raise RuntimeError("no")

        def write(self) -> None: ...

    OptionalDouble.__module__ = LogFailure.__module__ = clash.__module__ = spark_logging.__name__
    Unbuildable.__module__ = spark_logging.__name__
    with mock.patch.object(spark_logging, "LogPersistenceAdapter", Interface), \
            mock.patch.object(spark_logging, "OptionalDouble", OptionalDouble, create=True), \
            mock.patch.object(spark_logging, "Unbuildable", Unbuildable, create=True), \
            mock.patch.object(spark_logging, "LogFailure", LogFailure, create=True), \
            mock.patch.object(spark_logging, "OrchestratorLogger", clash, create=True):
        schema, problems = _logging_reference_schema()
    assert schema.get("OptionalDouble") == {"fail", "written"}, f"an optional-parameter double: {schema.get('OptionalDouble')}"
    assert sum("LogFailure" in p for p in problems) == 1, f"a dataclass on Exception: {problems}"
    assert sum("takes the name of a container" in p for p in problems) == 1, f"a name clash: {problems}"
    assert sum("Unbuildable" in p for p in problems) == 1, f"a double that cannot be built: {problems}"
    assert "debug" in schema["OrchestratorLogger"], "the clash replaced OrchestratorLogger's container"


# --- F75: the architecture overview is a diagram in text, not a raster image ------------

_OVERVIEW_PAGE = _DOCS / "architecture" / "overview.md"


class _CodeNames(typing.NamedTuple):
    """What the library's source defines, for the code-name checks (F75, F76)."""

    names: set[str]  # classes, functions and names the package imports
    members: dict[str, set[str]]  # each class's members, inherited and `self.` attributes included
    strings: set[str]  # identifier-shaped string literals: column and key names, never callables


def _defined_code_names() -> _CodeNames:
    """Read from the AST of src/spark_quick_start/ only, never the rest of src/ (F76
    adversary). A class's members are its methods, the names its body assigns or annotates, the
    attributes its methods set on `self`, and the members of its bases defined in the package."""
    import ast

    names: set[str] = set()
    strings: set[str] = set()
    own: dict[str, set[str]] = collections.defaultdict(set)
    bases: dict[str, list[str]] = {}
    for path in (_arch.ROOT / "src" / "spark_quick_start").rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                names.add(node.name)
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                names |= {(alias.asname or alias.name).split(".")[0] for alias in node.names}
            elif isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value.isidentifier():
                strings.add(node.value)
            if not isinstance(node, ast.ClassDef):
                continue
            bases[node.name] = [b.id if isinstance(b, ast.Name) else getattr(b, "attr", "") for b in node.bases]
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    own[node.name].add(item.name)
                    own[node.name] |= {target.attr for sub in ast.walk(item)
                                       for target in getattr(sub, "targets", [getattr(sub, "target", None)])
                                       if isinstance(target, ast.Attribute) and isinstance(target.value, ast.Name)
                                       and target.value.id == "self"}
                elif isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
                    own[node.name].add(item.target.id)
                elif isinstance(item, ast.Assign):
                    own[node.name] |= {target.id for target in item.targets if isinstance(target, ast.Name)}

    def inherited(name: str, seen: frozenset[str] = frozenset()) -> set[str]:
        if name in seen:
            return set()
        return own.get(name, set()).union(*(inherited(base, seen | {name}) for base in bases.get(name, [])))

    # The Delta commit metrics the library reads inside SQL strings, and the map that holds them
    # (F77 review): named on a page only while the code still reads them.
    keys = {key for path in (_arch.ROOT / "src" / "spark_quick_start").rglob("*.py")
            for key in re.findall(r"operationMetrics\['(\w+)'\]", path.read_text(encoding="utf-8"))}
    names |= keys | ({"operationMetrics"} if keys else set())
    return _CodeNames(names, {name: inherited(name) for name in bases}, strings)


# Names a page may use that PySpark and Delta define, not this library: three classes, and the
# DataFrame action pages cost out (F77). Commit-metric keys are read from the source instead.
_EXTERNAL_CODE_NAMES = {"DataFrame", "SparkSession", "DeltaTable", "count"}


def _code_name_problems(texts: list[str], defined: _CodeNames) -> tuple[list[str], int]:
    """Every code name in `texts` that the source does not define, and how many were checked.

    A code name is an identifier with a lower-case letter and a later capital
    (`LoadOrchestrator`, `DQFTransformer`, `_TransformerBase`), or one written as a call
    (`to_json()`, `run ()`); `Owner.member` must be a member of that class. Declined (F75
    adversary): a misspelling with one capital (`Loadorchestrator`) cannot be told from a prose
    word; field names and keys are not read.
    """
    import builtins

    names, members, strings = defined
    classes: set[str] = set()
    calls: set[str] = set()
    pairs: set[tuple[str, str]] = set()
    # Each text alone: joined, a name at the end of one would read as a call when the next
    # begins with `(` (F76).
    for text in texts:
        identifiers = re.findall(r"(?<![\w.])[A-Za-z_][A-Za-z0-9_]*", text)
        # A class named through its module path, `orchestration.LoadOrchestrator` (F76 adversary).
        identifiers += re.findall(r"(?<=[a-z0-9_]\.)[A-Z][A-Za-z0-9_]*", text)
        classes |= {w for w in identifiers if re.search(r"[a-z]", w) and re.search(r"[A-Z]", w[1:])}
        calls |= set(re.findall(r"(?<![\w.])([A-Za-z_][A-Za-z0-9_]*)\s*\(", text))
        pairs |= set(re.findall(r"(?<![\w.])([A-Z][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)", text))
    known = names | _EXTERNAL_CODE_NAMES | set(dir(builtins))
    # A string the code uses can name a column or key, never a callable (F76 adversary), and
    # never a class: a stale `__all__` entry would otherwise keep a deleted class alive (F76 review).
    columns = {s for s in strings if s.lstrip("_")[:1].islower()}
    undefined = (classes - known - columns) | (calls - known)
    problems = [f"`{name}` is named but nothing under src/ defines it" for name in sorted(undefined)]
    problems += [f"`{owner}.{member}` is named but `{owner}` has no member `{member}`"
                 for owner, member in sorted(pairs) if owner in members and member not in members[owner]]
    return problems, len(classes | calls | pairs)


def _diagram_name_problems(block: str, defined: _CodeNames) -> tuple[list[str], int]:
    """`_code_name_problems` over a diagram's labels. Only quoted text is read: node and edge
    labels, which this repo always quotes; ids, `style`, `classDef` and `click` lines are not
    labels (F75 adversary)."""
    return _code_name_problems(re.findall(r'"([^"]*)"', block), defined)


def _raster_overview_mentions(pages: dict[str, str]) -> list[str]:
    """The pages, by repository-relative path, that still name the old raster overview, in any
    letter case. A dated record under a `decisions` directory is history and may name the image
    it replaced; an undated file there is not exempt (F75 adversary)."""
    dated = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}-")
    return sorted(path for path, text in pages.items()
                  if not ("decisions" in Path(path).parts[:-1] and dated.match(Path(path).name))
                  and re.search(r"(?<![\w-])architecture\.png", text, re.IGNORECASE))


def test_the_architecture_overview_is_a_mermaid_diagram() -> None:
    """F75 — docs/architecture/overview.md draws the system as a mermaid block, which the mermaid
    rules check and a diff can review; every code name it draws exists; and no document but a
    dated record shows the old raster overview."""
    assert _OVERVIEW_PAGE.is_file(), f"{_arch.rel(_OVERVIEW_PAGE)} is missing"
    blocks = [source for path, _, source in _check_mermaid().repository_blocks() if path == _arch.rel(_OVERVIEW_PAGE)]
    assert blocks, f"{_arch.rel(_OVERVIEW_PAGE)} has no mermaid block"
    defined = _defined_code_names()
    for block in blocks:
        problems, checked = _diagram_name_problems(block, defined)
        assert checked >= 5, f"the name reader found only {checked} code names in the overview diagram"
        assert not problems, f"the overview diagram names code that does not exist: {problems}"
    pages = {_arch.rel(page): page.read_text(encoding="utf-8") for page in _documents(_arch.ROOT)}
    raster = _raster_overview_mentions(pages)
    assert not raster, f"documents still show the raster overview: {raster}"


def test_the_diagram_name_check_can_fail() -> None:
    """F75 — anti-vacuity for `_diagram_name_problems` and `_raster_overview_mentions`: each kind
    of code name is selected and an undefined one reported once; the adversary's valid diagram
    text stays clean; and only a dated record may name the raster image."""
    defined = _CodeNames({"LoadOrchestrator", "DQFTransformer", "PipelineState", "to_json", "_TransformerBase",
                          "with_df"}, {"PipelineState": {"with_df"}}, set())
    good = ('A["<b>LoadOrchestrator</b><br/>Notebook or job · MERGE · SCD2 · JSON"] --> B["DQFTransformer"]\n'
            'B -->|"to_json()"| C["PipelineState.with_df · DataFrame · SparkSession"]\n'
            'NB(Notebook) --> PureData\nstyle A fill:rgb(255,0,0)\nclassDef x fill:#FfA0b1\nD["_TransformerBase"]')
    assert _diagram_name_problems(good, defined) == ([], 8), _diagram_name_problems(good, defined)
    mutations = {"an undefined class": 'A["DebugStepTransformer"]',
                 "an undefined all-caps-led class": 'A["DQFReader"]',
                 "an undefined private class": 'A["_TransformerBse"]',
                 "an underscored class": 'A["Load_Orchestrator"]',
                 "an undefined call": 'A -->|"from_yaml()"| B',
                 "a call with a space": 'A["to_jsn ()"]',
                 "a member the class lacks": 'A["PipelineState.with_dff"]',
                 "a member on the wrong class": 'A["PipelineState.to_json()"]'}
    wrong = {label: got for label, block in mutations.items() if len(got := _diagram_name_problems(block, defined)[0]) != 1}
    assert not wrong, f"the diagram name check misjudged: {wrong}"
    pages = {"README.md": "![x](docs/images/architecture.png)",
             "docs/a.md": "![x](../images/Architecture.PNG)",
             "docs/architecture/decisions/README.md": "the image architecture.png",
             "docs/architecture/decisions/2026-01-01-x.md": "replaced docs/images/architecture.png",
             "docs/guides/b.md": "system-architecture.png is another image"}
    found = _raster_overview_mentions(pages)
    assert found == ["README.md", "docs/a.md", "docs/architecture/decisions/README.md"], found


# --- F76: every architecture page names real code ---------------------------------------


def _architecture_pages(docs: Path = _DOCS) -> list[Path]:
    """Every Markdown file under docs/architecture/, any depth, `.md` in any case, except a dated
    record under a `decisions` directory, which is history and may name code since gone. The
    exemption is the one F75's raster check uses (F76 adversary)."""
    dated = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}-")
    return sorted(page for page in (docs / "architecture").rglob("*")
                  if page.is_file() and page.suffix.lower() in {".md", ".markdown"}
                  and not ("decisions" in page.relative_to(docs).parts[:-1] and dated.match(page.name)))


# The Stage D pages written so far; each finding adds its page here in its red step.
_REQUIRED_ARCHITECTURE_PAGES = ("overview.md", "pipeline-model.md", "metrics-and-watermarks.md",
                                "platform-abstraction.md")


def test_every_architecture_page_names_real_code() -> None:
    """F76 — every code name in an architecture page's code spans and diagram labels is defined
    under src/, so a page that outlives a rename fails here rather than misleading a reader."""
    missing = [name for name in _REQUIRED_ARCHITECTURE_PAGES if not (_DOCS / "architecture" / name).is_file()]
    assert not missing, f"architecture pages the plan requires are missing: {missing}"
    defined = _defined_code_names()
    blocks = _check_mermaid().repository_blocks()
    problems = []
    for page in _architecture_pages():
        text = page.read_text(encoding="utf-8")
        labels = [label for path, _, source in blocks if path == _arch.rel(page)
                  for label in re.findall(r'"([^"]*)"', source)]
        found, checked = _code_name_problems(_docs.code_spans(text) + labels, defined)
        problems += [f"{_arch.rel(page)}: {problem}" for problem in found]
        if page.name == "pipeline-model.md":
            # Anti-vacuity on the real page: its diagram's labels are found and names are read.
            assert labels, "pipeline-model.md's diagram labels were not read"
            assert checked >= 10, f"only {checked} code names were read on pipeline-model.md"
    assert (_DOCS / "architecture" / "pipeline-model.md") in _architecture_pages(), "pipeline-model.md is not selected"
    assert not problems, f"architecture pages name code that does not exist: {problems}"


def test_the_code_span_reader_can_fail() -> None:
    """F76 — anti-vacuity for `_docs.code_spans`: spans in prose, headings, tables, lists and
    link text are read; fenced and indented code are not."""
    text = ("# The `Heading` span\n\nProse `A` and `B`.\n\n- item `C`\n\n| x |\n|---|\n| `D` |\n\n"
            "```python\nnot_a_span = `E`\n```\n\n    indented `F`\n\n[`L`](x.md) `G`\n")
    assert _docs.code_spans(text) == ["Heading", "A", "B", "C", "D", "L", "G"], _docs.code_spans(text)


def test_the_code_name_check_reads_each_span_alone() -> None:
    """F76 — `_code_name_problems` judges each text on its own, so a name ending one span is not
    read as a call because the next span starts with `(`; builtins, imported names and the string
    names the code uses (columns, keys) are known, the last never as a call; a class through its
    module path is read; an undefined name is still reported."""
    defined = _CodeNames({"LoadContext", "ThreadPoolExecutor"}, {"LoadContext": {"with_df"}},
                         {"scdVersion", "_prevVersion", "ScdKey"})
    clean = ["logging_context", "(DataFrame, LoadContext)", "ValueError", "scdVersion", "_prevVersion", "ThreadPoolExecutor",
             "LoadContext.with_df", "orchestration.LoadContext"]
    assert _code_name_problems(clean, defined)[0] == [], _code_name_problems(clean, defined)
    mutations = {"a misspelt column": "scdVersoin", "a key used as a call": "scdVersion()",
                 "a class known only as a string": "ScdKey",
                 "a class through its module path": "orchestration.LoadContextt"}
    wrong = {label: got for label, span in mutations.items()
             if len(got := _code_name_problems([*clean, span], defined)[0]) != 1}
    assert not wrong, f"the code-name check misjudged: {wrong}"


def test_the_code_name_source_reader_can_fail() -> None:
    """F76 — anti-vacuity for `_defined_code_names`: it reads only the package, inherits members,
    sees `self.` attributes, imports and string names, and keeps a string out of the names."""
    defined = _defined_code_names()
    assert "after_run" in defined.members["SCDChangeDetectorTransformer"], "inherited members were not read"
    assert "selected_source" in defined.members["ConditionalSourceTransformer"], "self. attributes were not read"
    assert "ThreadPoolExecutor" in defined.names, "imported names were not read"
    assert "scdVersion" in defined.strings and "scdVersion" not in defined.names, "a string became a name"
    assert {"operationMetrics", "numOutputRows"} <= defined.names, "the commit-metric keys were not read"


def test_the_architecture_page_selector_can_fail() -> None:
    """F76 — anti-vacuity for `_architecture_pages`: pages at any depth and in any case of `.md`
    are selected, a dated record is not, and an undated file under `decisions` is."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        docs = Path(tmp)
        for name in ("a.md", "B.MD", "sub/c.md", "decisions/README.md", "decisions/2026-01-01-x.md", "notes.txt"):
            (docs / "architecture" / name).parent.mkdir(parents=True, exist_ok=True)
            (docs / "architecture" / name).write_text("x", encoding="utf-8")
        found = [page.relative_to(docs / "architecture").as_posix() for page in _architecture_pages(docs)]
    assert found == ["B.MD", "a.md", "decisions/README.md", "sub/c.md"], found


# --- F79: every decision record has one row in its index, in date order -----------------

_DECISIONS = _DOCS / "architecture" / "decisions"


def _decision_index_problems(records: list[str], index_text: str) -> list[str]:
    """Every way docs/architecture/decisions/README.md departs from the records beside it: a
    record with no row, one with two, a row naming no record here, a row reaching a record in
    another directory, an impossible date, and rows out of date order.

    A row is a link in the first cell of a table body row, read in document order with the
    Markdown parser; a mention in prose or in a later cell is not a row (F79 adversary). A row is
    judged when its file name starts with a date, in any letter case. Records are the names the
    docs-tree check allows there (`_RECORD`), which also refuses `.MD` and subdirectories.
    """
    import datetime

    dated = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}-", re.IGNORECASE)
    records = [name for name in records if _RECORD.fullmatch(name)]
    problems: list[str] = []
    rows: list[str] = []
    for dest in _docs.table_row_links(index_text):
        path = dest.split("#")[0].removeprefix("./")
        if not dated.match(posixpath.basename(path)):
            continue
        if path != posixpath.basename(path):
            problems.append(f"the row `{dest}` reaches a record outside this directory")
            continue
        rows.append(path)
    problems += [f"`{name}` has no row in the decisions index" for name in sorted(set(records) - set(rows))]
    problems += [f"`{name}` has {rows.count(name)} rows" for name in sorted(set(rows)) if rows.count(name) > 1]
    problems += [f"the index has a row for `{name}`, which is not a record here" for name in sorted(set(rows) - set(records))]
    for name in records:
        try:
            datetime.date.fromisoformat(name[:10])
        except ValueError:
            problems.append(f"`{name}` is named for a date that does not exist")
    dates = [name[:10] for name in rows]
    if dates != sorted(dates):
        problems.append(f"the index is not in date order, oldest first: {rows}")
    return problems


def test_every_decision_record_has_a_row_in_its_index() -> None:
    """F79 — the decisions index is the architecture's design-decisions page (owner, 2026-09-29),
    so every dated record in docs/architecture/decisions/ has exactly one row there, oldest first."""
    records = sorted(p.name for p in _DECISIONS.glob("*.md") if p.name != _INDEX)
    assert records, "no decision records found; the scan is broken"
    problems = _decision_index_problems(records, (_DECISIONS / _INDEX).read_text(encoding="utf-8"))
    assert not problems, f"the decisions index disagrees with the records: {problems}"


def test_the_decision_index_check_can_fail() -> None:
    """F79 — anti-vacuity for `_decision_index_problems`: each departure, including the
    adversary's, reported once; a same-day order the index chose, prose mentions and other pages'
    links left alone."""
    records = ["2026-01-01-a.md", "2026-02-01-b.md"]
    head = "| File | What |\n|---|---|\n"
    good = head + "| [a](2026-01-01-a.md) | x |\n| [b](./2026-02-01-b.md) | supersedes [a](2026-01-01-a.md) |\n\n[guide](../x.md)\n"
    assert _decision_index_problems(records, good) == [], _decision_index_problems(records, good)
    same_day = ["2026-01-01-z.md", "2026-01-01-a.md"]
    assert _decision_index_problems(same_day, head + "| [z](2026-01-01-z.md) |\n| [a](2026-01-01-a.md) |\n") == [], \
        "one day's order was judged"
    mutations = {
        "a record with no row": good.replace("| [b](./2026-02-01-b.md) | supersedes [a](2026-01-01-a.md) |\n", ""),
        "a record only in prose": good.replace("| [b](./2026-02-01-b.md) | supersedes [a](2026-01-01-a.md) |\n", "")
                                  + "\nsee [b](2026-02-01-b.md)\n",
        "a record with two rows": good.replace("| [a](2026-01-01-a.md) | x |\n",
                                               "| [a](2026-01-01-a.md) | x |\n| [a](2026-01-01-a.md#x) | y |\n"),
        "a row naming no record": good + "\n" + head + "| [c](2026-03-01-C.MD) | z |\n",
        "a row reaching another directory": good.replace("| [b](./2026-02-01-b.md)",
                                                         "| [b](../other/2026-02-01-b.md) |\n| [b](2026-02-01-b.md)"),
        "rows out of date order in one table": head + "| [b](2026-02-01-b.md) |\n| [a](2026-01-01-a.md) |\n",
    }
    wrong = {label: got for label, text in mutations.items() if len(got := _decision_index_problems(records, text)) != 1}
    assert not wrong, f"the decision index check misjudged: {wrong}"
    impossible = _decision_index_problems([*records, "2026-02-30-c.md"], good + "\n" + head + "| [c](2026-02-30-c.md) |\n")
    assert len(impossible) == 1, impossible
    # Controls: a link in a header row is not a row, and a name the tree check refuses is not a record.
    header_only = "| [a](2026-01-01-a.md) | What |\n|---|---|\n| [b](2026-02-01-b.md) | y |\n"
    assert len(_decision_index_problems(records, header_only)) == 1, _decision_index_problems(records, header_only)
    assert _decision_index_problems([*records, "notes.md"], good) == [], "a non-record name was judged"


_AUDIENCE = re.compile(r"^\*\*Audience:\*\* (internal|public)\s*$", re.M)


def test_every_decision_record_declares_its_audience() -> None:
    """Open-core publication model (2026-10-28): every record carries `**Audience:** internal`
    or `**Audience:** public`, so the public export's strip-list excludes the internal ones
    mechanically. A new record with no marker is reported, never treated as public by default."""
    records = sorted(p for p in _DECISIONS.glob("*.md") if p.name != _INDEX)
    assert records, "no decision records found; the scan is broken"
    bad = [_arch.rel(p) for p in records if not _AUDIENCE.search(p.read_text(encoding="utf-8"))]
    assert not bad, f"decision records with no `**Audience:** internal|public` line: {bad}"


def test_the_audience_check_can_fail() -> None:
    """Anti-vacuity: the marker is read only as its own header line with a known value."""
    assert _AUDIENCE.search("**Audience:** internal")
    assert _AUDIENCE.search("x\n**Audience:** public\ny")
    assert not _AUDIENCE.search("**Audience:** secret")
    assert not _AUDIENCE.search("**Status:** accepted by the owner, 2026-10-28")
    assert not _AUDIENCE.search("prose with **Audience:** internal inline")


# --- F81: the repository layout names every module of the package -----------------------

_LAYOUT_PAGE = _DOCS / "architecture" / "repository-layout.md"
_PACKAGE = _arch.ROOT / "src" / "spark_quick_start"


def _layout_problems(rows: list[str], modules: set[str], exists: typing.Callable[[str], bool]) -> list[str]:
    """Every way the layout page's module table departs from the package: a module with no row,
    a row naming no path, and a row given twice. Rows are paths relative to the package, as the
    first column spells them; a directory row ends with `/`."""
    problems = [f"`{m}` has no row in the module table" for m in sorted(modules - set(rows))]
    problems += [f"`{row}` is not a path in the package" for row in rows if not exists(row)]
    problems += [f"`{row}` has {rows.count(row)} rows" for row in sorted(set(rows)) if rows.count(row) > 1]
    return problems


def test_the_repository_layout_names_every_module() -> None:
    """F81 — every module of the package except `__init__.py`, and every subpackage, has a row in
    the layout page's "Module responsibilities" table, and every row is the canonical spelling of
    one of them."""
    tables = [table for table in _docs.tables(_LAYOUT_PAGE.read_text(encoding="utf-8"))
              if table and table[0] and table[0][0] == "Module"]
    assert len(tables) == 1, f"expected one table headed Module, found {len(tables)}"
    rows = [row[0] for row in tables[0][1:]]
    modules = {p.relative_to(_PACKAGE).as_posix() for p in _PACKAGE.rglob("*.py")
               if p.name != "__init__.py" and "__pycache__" not in p.parts}
    assert len(modules) > 30, f"only {len(modules)} modules found; the scan is broken"
    # A row must be the canonical spelling of a module or a package directory, compared as text:
    # `Path.exists()` accepted `..`, absolute paths, an empty cell and, on macOS, any letter case,
    # and let one module have two rows under two spellings (F81 adversary).
    packages = {p.parent.relative_to(_PACKAGE).as_posix() + "/" for p in _PACKAGE.rglob("__init__.py")
                if p.parent != _PACKAGE and "__pycache__" not in p.parts}
    problems = _layout_problems(rows, modules | packages, (modules | packages).__contains__)
    assert not problems, f"the repository layout disagrees with the package: {problems}"


def test_the_layout_check_can_fail() -> None:
    """F81 — anti-vacuity for `_layout_problems`: each departure, alone, reported once."""
    modules = {"a.py", "pkg/b.py", "pkg/"}
    paths = modules
    good = ["a.py", "pkg/", "pkg/b.py"]
    assert _layout_problems(good, modules, paths.__contains__) == []
    mutations = {"a module with no row": ["a.py", "pkg/"], "a package with no row": ["a.py", "pkg/b.py"],
                 "a row naming no path": [*good, "c.py"],
                 "a row given twice": [*good, "a.py"]}
    wrong = {label: got for label, rows in mutations.items()
             if len(got := _layout_problems(rows, modules, paths.__contains__)) != 1}
    assert not wrong, f"the layout check misjudged: {wrong}"


# --- Stage E: the planned guides exist and are indexed ----------------------------------

# The guides written so far, in the order the owner approved (2026-09-29); each finding adds
# its guide here in its red step.
_REQUIRED_GUIDES = ("run-your-first-load.md", "write-a-load-context.md", "build-a-pipeline.md",
                    "choose-a-load-style.md", "write-a-custom-transformer.md",
                    "run-incremental-loads.md", "write-a-staging-notebook.md",
                    "inspect-a-run.md", "tune-performance.md", "test-without-a-cluster.md", "set-up-for-development.md", "handle-early-exit-and-errors.md",
                    "load-a-table-with-a-dqd.md")


def test_every_planned_guide_exists_and_is_indexed() -> None:
    """Stage E — each guide the plan has reached exists under docs/guides/ and has a row in its
    index, linked from the first cell as the decisions index is."""
    guides = _DOCS / "guides"
    # A row must link the guide beside the index, as F79's rows must (its adversary's B2).
    rows = {dest.split("#")[0].removeprefix("./") for dest in _docs.table_row_links((guides / _INDEX).read_text(encoding="utf-8"))}
    missing = [name for name in _REQUIRED_GUIDES if not (guides / name).is_file()]
    unindexed = [name for name in _REQUIRED_GUIDES if name not in rows]
    assert not missing and not unindexed, f"guides missing: {missing}; guides with no index row: {unindexed}"
