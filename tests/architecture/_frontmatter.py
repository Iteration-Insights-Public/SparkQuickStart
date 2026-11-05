"""
_frontmatter.py — the YAML frontmatter of .claude/ agent and skill files, read fail closed.

Contains: `block` (the text between the fences, or why it cannot be trusted), `problem` (why
    the frontmatter cannot be read the way Claude Code reads it), and `read` (the parsed
    mapping, values as strings or lists of strings).
Does NOT contain: any assertion, or any rule about what a field must say — that belongs to
    each contract test (test_agent_contract.py, test_skill_contract.py).

Moved out of test_agent_contract.py by F60 so that skills are read by the same fail-closed
reader F103 built for agents.
"""
from __future__ import annotations

import re

from _yaml import load_mapping as _load_yaml

_FENCE = re.compile(r"---[ \t]*")
# Line breaks YAML 1.2 does not break on but str.splitlines() does, or a bare CR: a file that
# holds one can be read one way here and another way by Claude Code (F103 adversary).
_ODD_BREAK = re.compile("\r(?!\n)|[\x85\u2028\u2029\x0b\x0c\x1c\x1d\x1e]")
_STRING_FIELDS = ("name", "description", "model")
# The fields that grant tools: an agent's `tools`, a command's `allowed-tools` (F104).
_TOOL_FIELDS = ("tools", "allowed-tools")


def block(path) -> tuple[str | None, str | None]:
    """The YAML between the opening and closing `---` fences, or why it cannot be trusted.

    Fails closed wherever Claude Code may find a different block than this does. Observed in
    Claude Code 2.1.274 on 2026-09-27 (F103 adversary, from the binary): the closing fence is
    the first `---` *anywhere* after the opening one, not a line of its own. So a `---` inside
    a description would end the frontmatter there — dropping the `model:` and `tools:` below
    it, which means the session's model and every tool — while a line-based reader reads on.
    Rather than copy that reader, any `---` inside the block is reported. So is any line
    break other than LF or CRLF. A byte-order mark is dropped and an opening fence may carry
    trailing blanks: both observed in the same 2.1.274 read (it strips a BOM first, and its
    opening fence is `^---\s*\n`).
    """
    # Bytes, not read_text(): universal newlines would turn a bare CR into LF and hide it.
    try:
        text = path.read_bytes().decode("utf-8-sig")
    except (OSError, UnicodeDecodeError) as exc:
        return None, f"cannot be read: {exc.__class__.__name__}"
    if _ODD_BREAK.search(text):
        return None, "a line break other than LF or CRLF; readers disagree on where lines end"
    lines = text.replace("\r\n", "\n").split("\n")
    if not lines or not _FENCE.fullmatch(lines[0]):
        return None, "no opening `---` fence"
    end = next((i for i in range(1, len(lines)) if _FENCE.fullmatch(lines[i])), None)
    if end is None:
        return None, "no closing `---` fence"
    block = "\n".join(lines[1:end])
    if "---" in block:
        return None, "`---` inside the frontmatter; Claude Code ends the frontmatter at the first one"
    return block, None


def problem(path) -> str | None:
    """Why *path*'s frontmatter cannot be read the way Claude Code reads it, or None."""
    text, why = block(path)
    if text is None:
        return why
    doc, why = _load_yaml(text)
    if doc is None:
        return why
    for field in _STRING_FIELDS:
        if field in doc and not (isinstance(doc[field], str) and doc[field].strip() == doc[field] and doc[field]):
            return f"`{field}:` is {doc[field]!r}; it must be a non-empty string with no surrounding spaces"
    for key in _TOOL_FIELDS:
        tools = doc.get(key)
        if key in doc and not (isinstance(tools, str) or
                               (isinstance(tools, list) and all(isinstance(item, str) for item in tools))):
            return f"`{key}:` is {tools!r}; it must be a string or a list of strings"
        # A string names tools separated by commas; a list names one tool per item. Anything
        # else — `Read Grep`, or `'Grep, Bash'` as one list item — is read differently by
        # different readers, so the grant is ambiguous and reported rather than interpreted.
        items = [tools] if isinstance(tools, str) else tools if isinstance(tools, list) else []
        for item in items:
            meant = _split(item, ",")[0] if isinstance(tools, str) else [item.strip()]
            balanced = _split(item, ",")[1]
            read = [name for name in _split(item, ", ")[0] if name]  # Claude Code drops empty names
            if not balanced or "" in meant or read != meant:
                return f"`{key}:` entry {item!r} is ambiguous; name one tool per comma-separated entry or list item"
    return None


def _split(text: str, separators: str) -> tuple[list[str], bool]:
    """*text* split on *separators* outside parentheses, and whether its parentheses balance.

    Claude Code 2.1.274, read from the binary on 2026-10-02 (F104 adversary), splits a tool
    list with one flag:
    `(` sets it, `)` clears it, and a space or comma outside it ends a name (`", "` here). A
    lookahead for some later `)` agreed with it on `Bash(git diff:*)` and not on a stray `)`:
    `Write x)` read here as one name and there as `Write` and `x)`. Split on `","` alone, the
    result is the reading this repo means; the caller reports any difference, and any nested
    or unbalanced parenthesis, as ambiguous. Names are stripped; empty ones are kept.
    """
    names, current, inside, depth, balanced = [], "", False, 0, True
    for ch in text:
        if ch == "(":
            inside, depth = True, depth + 1
            balanced = balanced and depth == 1
        elif ch == ")":
            inside, depth = False, depth - 1
            balanced = balanced and depth == 0
        if not inside and ch in separators:
            names.append(current.strip())
            current = ""
        else:
            current += ch
    names.append(current.strip())
    return names, balanced and depth == 0


def read(path) -> dict[str, str | list[str]]:
    """The frontmatter as YAML reads it, after `block` rules out the forms where
    Claude Code would read something else (F103).

    Parsed, not read line by line. The old reader took every `key:` line it saw, so an
    indented `model: opus` inside a `description: |` block overwrote the real top-level
    `model:` and the model-policy check (F59) compared the policy with the wrong value.
    Duplicate keys are refused by the strict loader. Frontmatter that cannot be trusted reads
    as empty here and is reported by each contract's parse test.
    """
    text, _ = block(path)
    doc = _load_yaml(text)[0] if text is not None else None
    if not isinstance(doc, dict):
        return {}
    return {str(key): [str(item) for item in value] if isinstance(value, list) else ("" if value is None else str(value))
            for key, value in doc.items()}
