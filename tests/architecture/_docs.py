"""
_docs.py — Markdown reading helpers for the documentation checks.

Contains: link and image destinations as a reader of the rendered page can follow them, the
    GitHub heading anchors a page offers, a page's tables as rendered text, its fenced code
    blocks (`fences`), and a reference page's containers and members (`reference_entries`, the
    format the writing-reference skill defines).
Does NOT contain: any assertion. Helpers only — the rules live in test_documentation.py,
    test_skill_contract.py and test_agent_contract.py.

Parsed with markdown-it-py (a [dev] dependency, test-only — nothing under src/ imports it),
a CommonMark parser with GitHub's tables and strikethrough enabled. A link counts only when
the parser renders it as one: code, the Markdown text inside raw HTML blocks, unused
reference definitions, escaped brackets and nested links are text, exactly as GitHub shows
them. A hand-rolled reader was tried first and lost to two adversary passes in a row, each
finding further CommonMark forms it misread (F55, F56).
"""
from __future__ import annotations

import re
import unicodedata

from markdown_it import MarkdownIt
from markdown_it.token import Token

_MD = MarkdownIt("commonmark").enable(["table", "strikethrough"])

# GitHub renders footnotes; CommonMark reads ``[^1]: Note.`` as a reference definition whose
# destination is ``Note.``. Only the marker is removed, so the note parses as a paragraph and
# a link inside it — or on a continuation line — is still checked (F56 review).
_FOOTNOTE_DEF = re.compile(r"^ {0,3}\[\^[^\]\n]+\]:[ \t]*", re.MULTILINE)
# An HTML comment, or one never closed, which hides everything after it.
_COMMENT = re.compile(r"<!--.*?(?:-->|\Z)", re.DOTALL)
_TAG_BODY = r"(?:[^>\"']|\"[^\"]*\"|'[^']*')*?"
# (?<![\w-]) keeps data-href and data-id out; quoted values may hold ">" (F55 adversary).
_HTML_ATTR = re.compile(r"<(?:a|img|source)\b" + _TAG_BODY
                        + r"(?<![\w-])(href|src|srcset)\s*=\s*(\"[^\"]*\"|'[^']*'|[^\s>]+)", re.IGNORECASE)
_HTML_ID = re.compile(r"<[a-z]" + _TAG_BODY + r"(?<![\w-])(?:id|name)\s*=\s*(\"[^\"]*\"|'[^']*'|[^\s>]+)",
                      re.IGNORECASE)


def _tokens(text: str) -> list[Token]:
    text = _FOOTNOTE_DEF.sub("", text)
    return _MD.parse(text)


def fences(text: str) -> list[tuple[int, str, str]]:
    """Every fenced code block: its first content line (1-based), language, and content.

    The language is the first word of the info string, lower-cased; empty when there is none.
    Indented code blocks carry no language and are not returned.
    """
    found = []
    for token in _tokens(text):
        if token.type == "fence" and token.map is not None:
            words = token.info.split()
            found.append((token.map[0] + 2, words[0].lower() if words else "", token.content))
    return found


def _unquote(raw: str) -> str:
    raw = raw.strip()
    if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in "\"'":
        return raw[1:-1]
    return raw


def _without_comments(html: str) -> str:
    return _COMMENT.sub(lambda m: re.sub(r"[^\n]", " ", m.group(0)), html)


def _html_destinations(html: str, first_line: int) -> list[tuple[int, str]]:
    """``href``/``src``/``srcset`` values in raw HTML, comments removed, as (line, dest)."""
    html = _without_comments(html)
    found = []
    for match in _HTML_ATTR.finditer(html):
        line = first_line + html.count("\n", 0, match.start())
        value = _unquote(match.group(2))
        if match.group(1).lower() == "srcset":
            found += [(line, part.split()[0]) for part in value.split(",") if part.split()]
        else:
            found.append((line, value))
    return found


def link_destinations(text: str) -> list[tuple[int, str]]:
    """Every link and image destination a reader of *text* can follow, as (line, dest).

    Markdown links and images as the parser resolves them (reference links included, their
    unused definitions not), and ``href``/``src``/``srcset`` in raw HTML outside comments.

    The line is the paragraph's first line plus the line breaks before the link, including
    those inside a multi-line inline tag. A code span spread over lines is the one miss: the
    parser turns its newline into a space, so later links in that paragraph read one early.
    """
    found: list[tuple[int, str]] = []
    for token in _tokens(text):
        first = token.map[0] + 1 if token.map else 1
        if token.type == "html_block":
            found += _html_destinations(token.content, first)
        if token.type != "inline":
            continue
        line = first
        for child in token.children or []:
            if child.type in ("softbreak", "hardbreak"):
                line += 1
            elif child.type == "link_open":
                found.append((line, str(child.attrGet("href") or "")))
            elif child.type == "image":
                found.append((line, str(child.attrGet("src") or "")))
            elif child.type == "html_inline":
                found += _html_destinations(child.content, line)
                line += child.content.count("\n")
    return sorted(found)


def slug(heading: str) -> str:
    """GitHub's anchor for a heading's rendered text (github-slugger, without the suffix).

    Lowercased; every character that is not a letter, mark, number, connector punctuation,
    space or hyphen dropped; each space becomes a hyphen.
    """
    kept = (ch for ch in heading.lower()
            if ch in " -" or unicodedata.category(ch)[0] in "LMN" or unicodedata.category(ch) == "Pc")
    return "".join(kept).replace(" ", "-")


def _rendered_text(inline: Token) -> str:
    """The text content GitHub renders for an inline run: text and code, no markup.

    An image contributes nothing (an ``<img>`` has no text content); a soft break renders
    as a newline, which the slug then drops.
    """
    parts = []
    for child in inline.children or []:
        if child.type in ("text", "code_inline"):
            parts.append(child.content)
        elif child.type in ("softbreak", "hardbreak"):
            parts.append("\n")
    return "".join(parts).strip()


def anchors(text: str) -> set[str]:
    """Every fragment a link into *text* can target: heading slugs and HTML ids/names.

    A repeated slug gets ``-1``, ``-2`` … exactly as github-slugger numbers it: every
    candidate is checked against every slug already emitted, so ``Foo``, ``Foo``, ``Foo 1``
    give ``foo``, ``foo-1``, ``foo-1-1``.
    """
    occurrences: dict[str, int] = {}
    found: set[str] = set()
    tokens = _tokens(text)
    for index, token in enumerate(tokens):
        if token.type == "heading_open":
            original = result = slug(_rendered_text(tokens[index + 1]))
            while result in occurrences:
                occurrences[original] += 1
                result = f"{original}-{occurrences[original]}"
            occurrences[result] = 0
            found.add(result)
        if token.type == "html_block":
            html = token.content
        elif token.type == "inline":
            html = "".join(c.content for c in token.children or [] if c.type == "html_inline")
        else:
            continue
        found |= {_unquote(m.group(1)) for m in _HTML_ID.finditer(_without_comments(html))}
    return found


def tables(text: str) -> list[list[list[str]]]:
    """Every table in *text*, as rows of cells in rendered text, the header row first.

    A cell's code span reads as its content (`` `opus` `` is ``opus``), so a table can name
    identifiers the way the docs write them.
    """
    found: list[list[list[str]]] = []
    row: list[str] | None = None
    tokens = _tokens(text)
    for index, token in enumerate(tokens):
        if token.type == "table_open":
            found.append([])
        elif token.type == "tr_open":
            row = []
        elif token.type in ("th_open", "td_open") and row is not None:
            following = tokens[index + 1]
            row.append(_rendered_text(following) if following.type == "inline" else "")
        elif token.type == "tr_close" and row is not None and found:
            found[-1].append(row)
            row = None
    return found


def table_row_links(text: str) -> list[str]:
    """The link destinations in the first cell of every body row of every table in *text*, in
    document order. A link elsewhere in a row, in a header or in prose is not a row's (F79)."""
    found: list[str] = []
    tokens = _tokens(text)
    in_body = False
    cell = 0
    for index, token in enumerate(tokens):
        if token.type == "tbody_open":
            in_body = True
        elif token.type == "tbody_close":
            in_body = False
        elif token.type == "tr_open":
            cell = 0
        elif token.type == "td_open" and in_body:
            cell += 1
            following = tokens[index + 1]
            if cell == 1 and following.type == "inline":
                found += [child.attrGet("href") or "" for child in following.children or []
                          if child.type == "link_open"]
    return found


def code_spans(text: str) -> list[str]:
    """Every inline code span in a Markdown document, in order, as the parser reads it: spans in
    headings, tables and lists included, fenced and indented code blocks not."""
    return [child.content for token in _tokens(text) if token.type == "inline"
            for child in token.children or [] if child.type == "code_inline"]


def _single_code_span(inline: Token) -> str | None:
    """The code span that is the whole of *inline*, or None when there is anything else.

    Whitespace-only text beside the span is ignored: a non-breaking space before it renders
    as nothing a reader would take for part of the name.
    """
    children = [c for c in inline.children or [] if not (c.type == "text" and not c.content.strip())]
    if len(children) == 1 and children[0].type == "code_inline":
        return children[0].content
    return None


def _has_code_span(inline: Token) -> bool:
    return any(c.type == "code_inline" for c in inline.children or [])


def _name_problem(kind: str, name: str) -> str | None:
    if not name or name != name.strip():
        return f"{kind} name {name!r} is empty or padded with spaces"
    return None


def reference_entries(text: str) -> tuple[dict[str, list[str]], list[str]]:
    """A reference page's containers and their members, and every way it departs from the format.

    The format (F61, the writing-reference skill), exact so that nothing a reader sees escapes
    the reference-versus-code tests (F67 onward):

    - a container is a level-2 heading whose whole text is one code span; a heading with a code
      span in it and anything else, or at another level, is reported — one stray character
      would otherwise switch off every check for its section;
    - its members table comes directly under it, before any other heading and any other table;
      the header row's first cell holds no code span, the body has at least one row, and each
      row's first cell is exactly one code span, the member's name;
    - a table under a prose sub-heading of a container whose first cell is a code span reads as
      more members, so it is reported rather than silently skipped;
    - raw HTML (anything left once comments are removed) and footnotes are reported: either can
      show or hide a row in a way this reader does not model.
    """
    env: dict = {}
    tokens = _MD.parse(text, env)
    containers: dict[str, list[str]] = {}
    problems: list[str] = []
    # A footnote is a `[^label]` in prose or a `[^label]: …` definition, which core CommonMark
    # files as a reference; code spans and fences that merely contain `[^` are not footnotes.
    prose = [c.content for tok in tokens if tok.type == "inline" for c in tok.children or [] if c.type == "text"]
    if any(re.search(r"\[\^[^\]\s]+\]", s) for s in prose) or \
            any(str(key).startswith("^") for key in env.get("references", {})):
        problems.append("footnotes are not allowed on a reference page")
    owner: str | None = None
    section: str | None = None  # the container whose h2 section this is, through its sub-headings
    state = "none"  # "await" a members table, "in" it, "after" it (before the next heading)
    header = cell = False
    body_rows = 0
    column = 0
    for index, token in enumerate(tokens):
        kind = token.type
        if kind == "html_block" and _without_comments(token.content).strip():
            problems.append(f"raw HTML on a reference page: {token.content.strip()[:40]!r}")
        elif kind == "inline" and any(c.type == "html_inline" and _without_comments(c.content).strip()
                                      for c in token.children or []):
            problems.append(f"inline HTML on a reference page: {token.content[:40]!r}")
        if kind == "heading_open":
            if owner is not None and state == "await":
                problems.append(f"container `{owner}` has no members table before the next heading")
            owner, state = None, "none"
            if token.tag in ("h1", "h2"):
                section = None
            inline = tokens[index + 1]
            name = _single_code_span(inline)
            if name is None:
                if _has_code_span(inline):
                    problems.append(f"heading {inline.content!r} holds a code span and more; a container "
                                    "heading is exactly one code span, a prose heading holds none")
                continue
            if token.tag != "h2":
                problems.append(f"container `{name}` is an {token.tag}; containers are level-2 headings")
                continue
            if (why := _name_problem("container", name)) is not None:
                problems.append(why)
            if name in containers:
                problems.append(f"container `{name}` appears twice")
            containers.setdefault(name, [])
            owner, state, section = name, "await", name
        elif kind == "table_open" and owner is not None:
            if state == "after":  # reported once per container, however many tables follow
                problems.append(f"container `{owner}` has a second table before the next heading")
            state = "in" if state == "await" else "extra"
            body_rows = 0
        elif kind == "table_open" and section is not None:
            state = "sub"
        elif kind == "thead_open":
            header = True
        elif kind == "thead_close":
            header = False
        elif kind == "tr_open":
            column = 0
            body_rows += 0 if header else 1
        elif kind in ("th_open", "td_open"):
            column += 1
            cell = column == 1
        elif kind == "inline" and cell and state == "sub" and not header:
            cell = False
            if _single_code_span(token) is not None:
                problems.append(f"a table of code-named rows sits under a sub-heading of `{section}`; "
                                "its members belong in the container's one members table")
                state = "sub-reported"
        elif kind == "inline" and cell and state == "in":
            cell = False
            if header:
                if _has_code_span(token):
                    problems.append(f"the members table of `{owner}` has a code span in its header row")
                continue
            member = _single_code_span(token)
            if member is None:
                problems.append(f"a member of `{owner}` is not a single code span: {token.content!r}")
            elif (why := _name_problem("member", member)) is not None:
                problems.append(why)
            elif member in containers[owner]:  # type: ignore[index]
                problems.append(f"`{owner}` lists member `{member}` twice")
            else:
                containers[owner].append(member)  # type: ignore[index]
        elif kind == "table_close" and owner is not None:
            if state == "in" and body_rows == 0:
                problems.append(f"the members table of `{owner}` has no rows")
            state = "after" if state == "in" else "extra-done"
        elif kind == "table_close":
            state = "none"
    if owner is not None and state == "await":
        problems.append(f"container `{owner}` has no members table")
    return containers, problems
