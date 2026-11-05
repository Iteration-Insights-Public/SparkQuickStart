"""
_markdown.py — reading a Markdown instruction file as it renders, for the architecture checks.

Contains: `blank_unrendered` (fenced code and HTML comments blanked, unclosed blocks reported),
    `blank_indented_code`, `strip_frontmatter`, `rendered_headings` and `heading_text`, shared by
    the lessons check (F174), the Python floor check (F171), the reviewer's front-door check
    (F176) and the testing-standard check (F172).
Does NOT contain: any check; a check imports these and judges what they return.

A check that reads a selector narrower than what renders stays green over what it no longer
reads (F174); these helpers are the one reading of "as rendered" the checks share.
"""
from __future__ import annotations

import re

__all__ = [
    "CODE_SPAN",
    "FENCE_OPEN",
    "blank_indented_code",
    "blank_unrendered",
    "heading_text",
    "rendered_headings",
    "strip_frontmatter",
]

# F174: every heading a renderer would show, so a lesson written in another shape is reported
# rather than left out of the walk. An ATX heading may be indented up to three spaces and sit in a
# blockquote or a list item; a setext heading is a text line underlined with = or -.
ATX_HEADING = re.compile(r"^ {0,3}(?:(?:>|[-*+]|\d+[.)])[ \t]*)*(?P<hashes>#{1,6})(?:[ \t]+(?P<text>.*?))?[ \t#]*$")
SETEXT_UNDERLINE = re.compile(r"^ {0,3}(?:=+|-+)[ \t]*$")
FENCE_OPEN = re.compile(r"^ {0,3}(?P<run>`{3,}|~{3,})")
FENCE_CLOSE = re.compile(r"^ {0,3}(?P<run>`{3,}|~{3,})[ \t]*$")
CODE_SPAN = re.compile(r"(`+).*?\1")

def blank_unrendered(text: str) -> tuple[list[str], list[str]]:
    """The file's lines with everything that renders nothing blanked, and what could not be read.

    One pass, so the heading scan and the field search read the same text (F174 review). A
    fence closes only on a bare fence of its own character at least as long as the opener
    (CommonMark); fenced lines are blanked, markers included. Outside fences, HTML comments are
    blanked, a `<!--` inside an inline code span is not one. A leading byte-order mark is
    dropped, as GitHub drops it. An unclosed fence or comment is reported: it would otherwise
    hide the rest of the file from the check.
    """
    lines = text.lstrip("\ufeff").split("\n")
    out: list[str] = []
    problems: list[str] = []
    fence: tuple[str, int, int] | None = None   # (character, length, line number)
    comment_from: int | None = None
    for number, raw in enumerate(lines, 1):
        line = raw.rstrip("\r")
        if fence is not None:
            m = FENCE_CLOSE.match(line)
            if m and m.group("run")[0] == fence[0] and len(m.group("run")) >= fence[1]:
                fence = None
            out.append("")
            continue
        if comment_from is None and (m := FENCE_OPEN.match(line)) is not None:
            run = m.group("run")
            if run[0] != "`" or "`" not in line[m.end():]:
                fence = (run[0], len(run), number)
                out.append("")
                continue
        kept, pos = [], 0
        masked = CODE_SPAN.sub(lambda c: " " * len(c.group(0)), line)
        while pos < len(line):
            if comment_from is not None:
                end = masked.find("-->", pos)
                if end < 0:
                    kept.append(" " * (len(line) - pos))
                    pos = len(line)
                else:
                    kept.append(" " * (end + 3 - pos))
                    pos, comment_from = end + 3, None
            else:
                start = masked.find("<!--", pos)
                if start < 0:
                    kept.append(line[pos:])
                    pos = len(line)
                else:
                    kept.append(line[pos:start])
                    pos, comment_from = start, number
        out.append("".join(kept))
    if fence is not None:
        problems.append(f"a code fence opened on line {fence[2]} is never closed")
    if comment_from is not None:
        problems.append(f"an HTML comment opened on line {comment_from} is never closed")
    return out, problems


def rendered_headings(lines: list[str]) -> list[tuple[int, int, str]]:
    """(line index, level, line) of every heading in already-blanked lines, in order (F174)."""
    headings = []
    for i, line in enumerate(lines):
        if (m := ATX_HEADING.match(line)) is not None and line.strip():
            headings.append((i, len(m.group("hashes")), line))
        elif line.strip() and i + 1 < len(lines) and SETEXT_UNDERLINE.match(lines[i + 1]):
            headings.append((i, 1 if "=" in lines[i + 1] else 2, line))
    return headings


def heading_text(line: str) -> str:
    """A heading line's text with its container markers, hashes and inline markup removed."""
    line = line.rstrip("\r")
    m = ATX_HEADING.match(line)
    text = (m.group("text") or "") if m else line.strip()
    return re.sub(r"[*_`\[\]]", "", text).strip()


def strip_frontmatter(text: str) -> str:
    """*text* below its YAML frontmatter, in any of the shapes the parser accepts: a BOM, CRLF,
    trailing spaces on a fence, an empty block, a closing fence at the end of the file."""
    lines = text.lstrip("\ufeff").replace("\r\n", "\n").split("\n")
    if not lines or lines[0].rstrip() != "---":
        return "\n".join(lines)
    for i in range(1, len(lines)):
        if lines[i].rstrip() == "---":
            return "\n".join(lines[i + 1:])
    return ""


def blank_indented_code(lines: list[str]) -> list[str]:
    """Indented code blocks blanked: a line of four or more leading spaces after a blank line,
    where the last text above was not a list item or its continuation."""
    out, in_code, last_text = [], False, ""
    for i, line in enumerate(lines):
        indented = line.startswith("    ") and line.strip()
        after_blank = i > 0 and not lines[i - 1].strip()
        in_list = bool(re.match(r" {0,3}(?:[-*+]|\d+[.)])\s|  ", last_text))
        if indented and (in_code or (after_blank and not in_list)):
            in_code = True
            out.append("")
            continue
        if line.strip():
            in_code = False
            last_text = line
        out.append(line)
    return out
