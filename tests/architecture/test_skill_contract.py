"""
test_skill_contract.py — the .claude/skills/ definitions are the pinned set and load as meant.

Contains: the skill roster, each skill's layout and frontmatter, and the mermaid skill's
    agreement with scripts/check_mermaid.py.
Does NOT contain: whether a skill's advice is good. That is judgement, and `/review-pr` owns it.

Claude Code loads a project skill from `.claude/skills/<name>/SKILL.md` exactly; `name`
defaults to the directory, `description` is what decides when it is used and is cut at 1,536
characters, and SKILL.md is best kept under 500 lines with detail in supporting files (the
skills documentation, read 2026-09-27). This repo is stricter where a looser form would be
ambiguous: `name` must be written and equal the directory, and a skill does not choose a
model — it runs on the model of the agent that uses it (agent-models.md, F59).
"""
from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import _arch
import _docs
import pytest
from _frontmatter import problem as _frontmatter_problem
from _frontmatter import read as _frontmatter

_SKILLS_DIR = _arch.CLAUDE_DIR / "skills"
# The roster, pinned: adding a skill is a deliberate act and gets a deliberate line here.
_SKILLS = frozenset({"mermaid-diagrams", "writing-architecture-docs", "writing-guides", "writing-reference"})
# Which agent preloads which skills, pinned (F64). Claude Code skips a listed skill that does
# not exist without an error (the sub-agents documentation, read 2026-09-28), so a misspelt or
# renamed skill would stop loading silently. An agent not named here preloads nothing.
_PRELOADS = {
    "documentation-engineer": frozenset(
        {"mermaid-diagrams", "writing-architecture-docs", "writing-guides", "writing-reference"}),
}
_AGENTS_DIR = _arch.CLAUDE_DIR / "agents"
_SKILL_FILE = "SKILL.md"
_DESCRIPTION_LIMIT = 1536
_LINE_LIMIT = 500
# An allowlist, not a blocklist: `model:` is not the only way to move a skill off its agent's
# model or out of reach. `context: fork` with `agent:` runs it as another agent; `effort`,
# `allowed-tools`, `paths`, `hooks`, `disable-model-invocation` and `user-invocable` change
# how, when or whether it runs (F60 adversary). A field is allowed by a deliberate line here.
_ALLOWED_FIELDS = frozenset({"name", "description", "when_to_use"})


def _is_noise(name: str) -> bool:
    return name == ".DS_Store" or name.startswith("._")


def _utf16_length(text: str) -> int:
    """Length as a JavaScript string counts it: Claude Code measures in UTF-16 units."""
    return len(text.encode("utf-16-le")) // 2


def _one_skill_problems(skill: Path, name: str) -> list[str]:
    """Every way one roster skill's directory and SKILL.md depart from the contract."""
    label = f"skills/{name}"
    if not skill.is_dir() or skill.is_symlink():
        return [f"{label} is not a directory"]
    entries = [e for e in skill.iterdir() if not _is_noise(e.name)]
    problems = [f"{label}/{e.name} is not a top-level .md file; a skill holds SKILL.md and .md supporting files"
                for e in sorted(entries)
                if e.name != _SKILL_FILE and (e.is_dir() or e.is_symlink() or e.suffix != ".md")]
    path = skill / _SKILL_FILE
    if _SKILL_FILE not in {e.name for e in entries} or not path.is_file() or path.is_symlink():
        return [*problems, f"{label}/ has no {_SKILL_FILE} file of its own (the name is case-sensitive; no symlink)"]
    why = _frontmatter_problem(path)
    if why is not None:
        return [*problems, f"{label}/{_SKILL_FILE}: {why}"]
    fields = _frontmatter(path)
    if fields.get("name") != name:
        problems.append(f"{label}/{_SKILL_FILE} declares name {fields.get('name')!r}; it must be {name!r}")
    extra = sorted(set(fields) - _ALLOWED_FIELDS)
    if extra:
        problems.append(f"{label}/{_SKILL_FILE} sets {extra}; allowed fields are {sorted(_ALLOWED_FIELDS)}")
    description = str(fields.get("description", ""))
    shown = _utf16_length(description) + _utf16_length(str(fields.get("when_to_use", "")))
    if not description.strip():
        problems.append(f"{label}/{_SKILL_FILE} has no description, which is what decides when it is used")
    elif shown > _DESCRIPTION_LIMIT:
        problems.append(f"{label}/{_SKILL_FILE}: description and when_to_use are {shown} UTF-16 units together; "
                        f"Claude Code cuts them at {_DESCRIPTION_LIMIT}")
    lines = len(path.read_text(encoding="utf-8").splitlines())
    if lines > _LINE_LIMIT:
        problems.append(f"{label}/{_SKILL_FILE} is {lines} lines; keep it under {_LINE_LIMIT} "
                        "and move detail into supporting files")
    return problems


def _skill_problems(directory: Path, roster: frozenset[str]) -> list[str]:
    """Every way .claude/skills/ departs from the pinned roster and the skill contract (F60)."""
    if not directory.is_dir():
        return [f"{directory.name}/ is missing"]
    entries = {e.name: e for e in directory.iterdir() if not _is_noise(e.name)}
    problems = [f"skills/{name} is not a skill on the roster {sorted(roster)}" for name in sorted(set(entries) - roster)]
    problems += [f"skills/{name}/ is on the roster but missing" for name in sorted(roster - set(entries))]
    for name in sorted(roster & set(entries)):
        problems += _one_skill_problems(entries[name], name)
    return problems


def test_the_skill_roster_is_the_pinned_one() -> None:
    """F60 — every skill on disk is on the roster, and each loads as the contract says."""
    problems = _skill_problems(_SKILLS_DIR, _SKILLS)
    assert not problems, f".claude/skills/ departs from the skill contract: {problems}"


def _preload_problems(directory: Path, roster: frozenset[str], pins: dict[str, frozenset[str]]) -> list[str]:
    """Every way the agents' `skills:` preloads depart from the pinned `_PRELOADS` (F64).

    `skills:` must be a YAML list, the only form the sub-agents documentation gives. Each item
    names a skill on the roster, and each agent's set equals its pin. Read fail closed where
    Claude Code's reading is not documented (F64 adversary): agent files below a subdirectory
    are read too, a key that differs from `skills` only in case or spacing is reported, and so
    is a pin that names a skill off the roster, which would otherwise drop out of the comparison.
    """
    agents = sorted(directory.rglob("*.md"))
    problems = [f"agents/{name}.md is pinned in _PRELOADS but missing"
                for name in sorted(set(pins) - {a.stem for a in agents})]
    problems += [f"_PRELOADS pins {name!r} for agents/{agent}.md, which is not a skill on the roster"
                 for agent, names in sorted(pins.items()) for name in sorted(names - roster)]
    for agent in agents:
        label = f"agents/{agent.relative_to(directory).as_posix()}"
        why = _frontmatter_problem(agent)
        if why is not None:
            problems.append(f"{label}: {why}")
            continue
        fields = _frontmatter(agent)
        problems += [f"{label} has a key {key!r}; the preload field is spelt exactly `skills`"
                     for key in sorted(fields) if key != "skills" and key.strip().casefold() == "skills"]
        declared = fields.get("skills")
        if declared is None:
            names: list[str] = []
        elif isinstance(declared, list):
            names = declared
        else:
            problems.append(f"{label} gives `skills:` as {declared!r}; it must be a YAML list, one skill per item")
            continue
        problems += [f"{label} preloads {n!r} more than once" for n in sorted(set(names)) if names.count(n) > 1]
        problems += [f"{label} preloads {n!r}, which is not a skill on the roster {sorted(roster)}"
                     for n in sorted(set(names) - roster)]
        expected = pins.get(agent.stem, frozenset())
        if set(names) & roster != expected & roster:
            problems.append(f"{label} preloads {sorted(set(names) & roster)}; _PRELOADS pins {sorted(expected)}")
    return problems


def test_every_agent_preloads_exactly_its_pinned_skills() -> None:
    """F64 — documentation-engineer preloads the four documentation skills, and no agent
    preloads a skill that does not exist."""
    problems = _preload_problems(_AGENTS_DIR, _SKILLS, _PRELOADS)
    assert not problems, f"agent skill preloads depart from _PRELOADS: {problems}"


def test_every_preload_check_can_fail() -> None:
    """F64 — anti-vacuity: each departure, alone, is reported exactly once."""
    import tempfile

    roster = frozenset({"alpha", "beta"})
    pins = {"docs": frozenset({"alpha", "beta"})}
    good = "---\nname: docs\ndescription: d\nmodel: sonnet\nskills:\n  - alpha\n  - beta\n---\n\nBody.\n"
    plain = "---\nname: other\ndescription: d\nmodel: sonnet\n---\n"

    def problems(docs: str | None, other: str = plain, extra: dict[str, str] | None = None,
                 pinned: dict[str, frozenset[str]] = pins) -> list[str]:
        with tempfile.TemporaryDirectory() as tmp:
            files = {"docs.md": docs, "other.md": other, **(extra or {})}
            for rel, text in files.items():
                if text is not None:
                    (Path(tmp) / rel).parent.mkdir(parents=True, exist_ok=True)
                    (Path(tmp) / rel).write_text(text, encoding="utf-8")
            return _preload_problems(Path(tmp), roster, pinned)

    assert problems(good) == [], "the valid preload was reported"
    assert problems(good.replace("skills:\n  - alpha\n  - beta", "skills: [alpha, beta]")) == [], \
        "a flow-style YAML list was reported"
    mutations = {
        "the pinned agent missing": problems(None),
        "a pinned skill not preloaded": problems(good.replace("  - beta\n", "")),
        "an extra name that is not a skill": problems(good.replace("  - beta\n", "  - beta\n  - gamma\n")),
        "skills as a comma string": problems(good.replace("skills:\n  - alpha\n  - beta", "skills: alpha, beta")),
        "a skill listed twice": problems(good.replace("  - beta\n", "  - beta\n  - beta\n")),
        "an unpinned agent preloading": problems(good, other=plain.replace("model: sonnet\n",
                                                                           "model: sonnet\nskills:\n  - alpha\n")),
        "no skills on the pinned agent": problems(good.replace("skills:\n  - alpha\n  - beta\n", "")),
        # On the unpinned agent, so only the parse branch can report it (F64 review).
        "frontmatter that does not parse": problems(good, other="---\nname: [other\n---\n"),
        # F64's adversary:
        "a pin naming a skill off the roster": problems(good, pinned={"docs": frozenset({"alpha", "beta", "gamma"})}),
        "an agent in a subdirectory preloading": problems(good, extra={"sub/nested.md": plain.replace(
            "model: sonnet\n", "model: sonnet\nskills:\n  - alpha\n")}),
        "the key in another case": problems(good, other=plain.replace("model: sonnet\n",
                                                                      "model: sonnet\nSkills:\n  - alpha\n")),
    }
    # A misspelt pinned skill is two faults: it names no skill, and the pin goes unmet.
    twice = {
        "a misspelt skill": problems(good.replace("  - beta\n", "  - betta\n")),
        "a skill in another case": problems(good.replace("  - beta\n", "  - Beta\n")),
    }
    wrong = {label: got for label, got in mutations.items() if len(got) != 1}
    wrong |= {label: got for label, got in twice.items() if len(got) != 2}
    assert not wrong, f"these preload mutations were not reported the expected number of times: {wrong}"


def _sanctioned_types() -> tuple[str, ...]:
    spec = importlib.util.spec_from_file_location("check_mermaid", _arch.ROOT / "scripts" / "check_mermaid.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return tuple(module.SANCTIONED_TYPES)


_TYPE_HEADER = ["Diagram type", "Use it for"]


_RAW_HEADER = re.compile(r"^[ \t]*\|?[ \t]*\**Diagram type\**[ \t]*\|[ \t]*\**Use it for", re.M)
_RAW_TYPE_CELL = re.compile(r"`?[A-Za-z][\w-]*`?")


def _raw_table_problems(text: str) -> list[str]:
    """The table as the model reads it: raw text, not rendered Markdown.

    A skill reaches the model as raw SKILL.md, so a second table in an HTML comment or a fence,
    an extra cell the renderer drops, or a comment inside a cell all teach the model something
    the rendered check cannot see (F60 adversary).
    """
    headers = _RAW_HEADER.findall(text)
    if len(headers) != 1:
        return [f"the raw skill text holds {len(headers)} `Diagram type | Use it for` header lines, not one"]
    lines = text[_RAW_HEADER.search(text).start():].splitlines()  # type: ignore[union-attr]
    problems = []
    for line in lines[2:]:
        if not line.strip().startswith("|"):
            break
        cells = [c.strip() for c in re.split(r"(?<!\\)\|", line.strip().strip("|"))]
        if len(cells) != 2:
            problems.append(f"the type row {line.strip()!r} has {len(cells)} cells, not 2")
        elif not _RAW_TYPE_CELL.fullmatch(cells[0]):
            problems.append(f"the type cell {cells[0]!r} is not a bare diagram type")
    return problems


def _mermaid_skill_problems(text: str, sanctioned: tuple[str, ...]) -> list[str]:
    """The mermaid skill's type table must name exactly the types check_mermaid.py allows,
    in the raw text the model reads as well as in the rendered table."""
    raw = _raw_table_problems(text)
    if raw:
        return raw
    tables = [t for t in _docs.tables(text) if t and t[0] == _TYPE_HEADER]
    if len(tables) != 1:
        return [f"the mermaid skill needs exactly one {_TYPE_HEADER} table, found {len(tables)}"]
    rows = tables[0][1:]
    named = [row[0] for row in rows]
    problems = [f"the skill lists {kind!r} more than once" for kind in sorted(set(named)) if named.count(kind) > 1]
    if set(named) != set(sanctioned):
        problems.append(f"the skill's types {sorted(set(named))} are not check_mermaid.py's {sorted(sanctioned)}")
    problems += [f"the skill gives no use for {row[0]!r}" for row in rows if len(row) < 2 or not row[1].strip()]
    return problems


def test_the_mermaid_skill_teaches_exactly_the_sanctioned_types() -> None:
    """F60 — the skill and the static check agree on which diagram types this repo draws."""
    path = _SKILLS_DIR / "mermaid-diagrams" / _SKILL_FILE
    assert path.is_file(), f"{_arch.rel(path)} is missing"
    problems = _mermaid_skill_problems(path.read_text(encoding="utf-8"), _sanctioned_types())
    assert not problems, f"the mermaid skill and check_mermaid.py disagree: {problems}"


def test_every_skill_check_can_fail() -> None:
    """F60 — anti-vacuity: each rule, mutated alone, is reported exactly once."""
    import tempfile

    good = "---\nname: {name}\ndescription: Draws diagrams.\n---\n\n# Body\n"
    roster = frozenset({"alpha"})

    def problems(files: dict[str, str | bytes | None], skills: frozenset[str] = roster,
                 link: tuple[str, str] | None = None) -> list[str]:
        import os

        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp) / "skills"
            directory.mkdir()
            for rel, body in {"alpha/SKILL.md": good.format(name="alpha"), **files}.items():
                if body is None:
                    continue
                (directory / rel).parent.mkdir(parents=True, exist_ok=True)
                if rel.endswith("/"):
                    (directory / rel).mkdir(exist_ok=True)
                elif isinstance(body, bytes):
                    (directory / rel).write_bytes(body)
                else:
                    (directory / rel).write_text(body, encoding="utf-8")
            if link is not None:
                outside = Path(tmp) / "outside.md"
                outside.write_text(good.format(name="alpha"), encoding="utf-8")
                os.symlink(outside if link[1] == "outside" else directory / link[1], directory / link[0])
            return _skill_problems(directory, skills)

    assert problems({}) == [], "the valid skill was reported"
    assert problems({"alpha/reference.md": "detail"}) == [], "a supporting file was reported"
    assert problems({"._alpha": "appledouble", "alpha/._SKILL.md": "x"}) == [], "macOS noise was reported"
    assert problems({"alpha/SKILL.md": "---\nname: alpha\ndescription: d\nwhen_to_use: w\n---\n"}) == [], \
        "when_to_use was reported"
    mutations: dict[str, tuple[dict[str, str | None], frozenset[str]]] = {
        "a skill off the roster": ({"beta/SKILL.md": good.format(name="beta")}, roster),
        "a roster skill missing": ({}, frozenset({"alpha", "gamma"})),
        "a file where a skill should be": ({"alpha/SKILL.md": None, "alpha": "a file"}, roster),
        "a lowercase skill.md": ({"alpha/SKILL.md": None, "alpha/skill.md": good.format(name="alpha")}, roster),
        "frontmatter that does not parse": ({"alpha/SKILL.md": "---\nname: [alpha\n---\n"}, roster),
        "a name off its directory": ({"alpha/SKILL.md": good.format(name="beta")}, roster),
        "no name": ({"alpha/SKILL.md": "---\ndescription: d\n---\n"}, roster),
        "no description": ({"alpha/SKILL.md": "---\nname: alpha\n---\n"}, roster),
        "a description past the limit": ({"alpha/SKILL.md": f"---\nname: alpha\ndescription: {'x' * 1537}\n---\n"}, roster),
        "a model of its own": ({"alpha/SKILL.md": good.format(name="alpha").replace("---\n\n", "model: opus\n---\n\n")},
                               roster),
        "too long": ({"alpha/SKILL.md": good.format(name="alpha") + "line\n" * 500}, roster),
        # F60's adversary:
        "forked onto another agent": ({"alpha/SKILL.md": good.format(name="alpha").replace(
            "---\n\n", "context: fork\nagent: Explore\n---\n\n")}, roster),
        "an effort of its own": ({"alpha/SKILL.md": good.format(name="alpha").replace("---\n\n", "effort: low\n---\n\n")},
                                 roster),
        "description and when_to_use past the limit": (
            {"alpha/SKILL.md": f"---\nname: alpha\ndescription: {'d' * 1000}\nwhen_to_use: {'w' * 600}\n---\n"}, roster),
        "a description past the limit in UTF-16": (
            {"alpha/SKILL.md": f"---\nname: alpha\ndescription: {chr(0x1F98B) * 800}\n---\n"}, roster),
        "a subdirectory in a skill": ({"alpha/sub/SKILL.md": good.format(name="alpha")}, roster),
        "a non-Markdown supporting file": ({"alpha/notes.txt": "see nowhere/x.md"}, roster),
        "SKILL.md as a directory": ({"alpha/SKILL.md": None, "alpha/SKILL.md/": ""}, roster),
        "SKILL.md that is not UTF-8": ({"alpha/SKILL.md": b"---\nname: alpha\ndescription: \xff\n---\n"}, roster),
    }
    wrong = {label: got for label, (files, skills) in mutations.items() if len(got := problems(files, skills)) != 1}
    assert not wrong, f"these mutations were not reported exactly once: {wrong}"
    sanctioned = ("flowchart", "sequenceDiagram")
    table = "| Diagram type | Use it for |\n|---|---|\n| `flowchart` | flow |\n| `sequenceDiagram` | calls |\n"
    assert _mermaid_skill_problems(table, sanctioned) == [], "the valid type table was reported"
    type_mutations = {
        "no table": "",
        "two tables": table + "\n" + table,
        "a type missing": table.replace("| `sequenceDiagram` | calls |\n", ""),
        "an extra type": table + "| `pie` | shares |\n",
        "a type twice": table + "| `flowchart` | again |\n",
        "a type with no use": table.replace("| calls |", "|  |"),
        # F60's adversary: what the model reads raw, the renderer hides.
        "a second table in a comment": table + "\n<!--\n" + table.replace("flow |", "flow |\n| `graph` | old |") + "-->\n",
        "a second table in a fence": table + "\n```\n" + table + "```\n",
        "an extra cell the renderer drops": table.replace("| `flowchart` | flow |", "| `flowchart` | flow | `graph` too |"),
        "a comment inside a type cell": table.replace("| `flowchart` |", "| `flowchart`<!-- graph --> |"),
    }
    wrong = {label: got for label, text in type_mutations.items()
             if len(got := _mermaid_skill_problems(text, sanctioned)) != 1}
    assert not wrong, f"these type-table mutations were not reported exactly once: {wrong}"


# Not template.md: .gitignore ignores temp*.md (test_no_skill_file_is_ignored_by_git).
_REFERENCE_TEMPLATE = _SKILLS_DIR / "writing-reference" / "skeleton.md"


def test_the_reference_skill_template_parses() -> None:
    """F61 — the template a reference page is copied from is read cleanly by the format's parser,
    the same one the reference-versus-code tests (F67 onward) will read every page with."""
    assert _REFERENCE_TEMPLATE.is_file(), f"{_arch.rel(_REFERENCE_TEMPLATE)} is missing"
    containers, problems = _docs.reference_entries(_REFERENCE_TEMPLATE.read_text(encoding="utf-8"))
    assert not problems, f"the reference template breaks its own format: {problems}"
    assert len(containers) >= 2 and all(len(members) >= 2 for members in containers.values()), (
        f"the template should show at least two containers of two members each: {containers}")


def test_every_reference_format_check_can_fail() -> None:
    """F61 — anti-vacuity for `_docs.reference_entries`: each departure is reported once, and
    what is not a container is not read as one."""
    good = ("# P\n\n## `A`\n\n| Field | Meaning |\n|---|---|\n| `x` | 1 |\n| `y` | 2 |\n\n"
            "### Notes\n\n| a | b |\n|---|---|\n| c | d |\n\n## See also\n\n| n | m |\n|---|---|\n| q | r |\n")
    assert _docs.reference_entries(good) == ({"A": ["x", "y"]}, []), _docs.reference_entries(good)
    ignored = {
        "a container inside a fence": "# P\n\n```\n## `A`\n```\n",
    }
    # Pinned valid forms: a non-breaking space before a span is not part of the name; comments,
    # code spans holding `[^` and prose tables of plain text under a sub-heading are fine.
    accepted = {
        "a non-breaking space": ("## `A`\n\n| F |\n|---|\n| \u00a0`x` |\n", {"A": ["x"]}),
        "a comment block and an inline comment": ("<!-- note -->\n\n## `A`\n\n| F | M |\n|---|---|\n| `x` | 1 <!-- c --> |\n",
                                                  {"A": ["x"]}),
        "a regex in a code span": ("## `A`\n\n| F | Default |\n|---|---|\n| `x` | `[^0-9]` |\n", {"A": ["x"]}),
    }
    for label, (text, expected) in accepted.items():
        assert _docs.reference_entries(text) == (expected, []), (label, _docs.reference_entries(text))
    wrong: dict[str, object] = {label: got for label, text in ignored.items()
                                if "A" in (got := _docs.reference_entries(text))[0]}
    mutations = {
        "a container with no table": good.replace("| Field | Meaning |\n|---|---|\n| `x` | 1 |\n| `y` | 2 |\n\n", ""),
        "a member that is plain text": good.replace("| `y` | 2 |", "| y | 2 |"),
        "a member with prose beside its code": good.replace("| `y` | 2 |", "| `y` (deprecated) | 2 |"),
        "a member listed twice": good.replace("| `y` | 2 |", "| `x` | 2 |"),
        "a container twice": good + "\n## `A`\n\n| F |\n|---|\n| `z` |\n",
        # F61's adversary: each of these hid a row from the diff, or switched a section off.
        "a heading with prose around its code": good.replace("## `A`", "## The `A` class"),
        "a bold container": good.replace("## `A`", "## **`A`**"),
        "a linked container": good.replace("## `A`", "## [`A`](#a)"),
        "a container at another level": good.replace("## `A`", "### `A`"),
        "a second table under a container": good.replace("| `y` | 2 |\n", "| `y` | 2 |\n\n| F | M |\n|---|---|\n| `z` | 3 |\n"),
        "a member in the header row": good.replace("| Field | Meaning |", "| `ghost` | Meaning |"),
        "a members table with no rows": good.replace("| `x` | 1 |\n| `y` | 2 |\n", ""),
        "the last container with no table": good + "\n## `B`\n\nProse only.\n",
        "a padded member": good.replace("| `y` | 2 |", "| `  y  ` | 2 |"),
        "raw HTML": good.replace("## See also", "<details>\n<summary>s</summary>\n</details>\n\n## See also"),
        "a footnote": good + "\nText[^1].\n\n[^1]: Note.\n",
        # F61's review:
        "HTML after a comment on one line": good.replace("## See also", "<!-- --><table><tr><td><code>g</code></td></tr></table>\n\n## See also"),
        "inline HTML in a cell": good.replace("| `y` | 2 |", "| `y` | <span>2</span> |"),
        "members under a sub-heading": good.replace("### Notes\n\n| a | b |\n|---|---|\n| c | d |",
                                                    "### Deprecated\n\n| F | M |\n|---|---|\n| `old` | gone |"),
        "a padded container": good.replace("## `A`", "## `  A  `"),
        "a code-span title": good.replace("# P", "# `P`"),
        "a third table": good.replace("| `y` | 2 |\n", "| `y` | 2 |\n\n| F |\n|---|\n| `z` |\n\n| G |\n|---|\n| `w` |\n"),
        "a container whose table sits under a sub-heading": good.replace(
            "## `A`\n\n| Field | Meaning |\n|---|---|\n| `x` | 1 |\n| `y` | 2 |\n\n### Notes\n\n| a | b |\n|---|---|\n| c | d |\n",
            "## `A`\n\n### Notes\n\n| Field | Meaning |\n|---|---|\n| `x` | 1 |\n"),
    }
    # A table moved under a sub-heading is two faults: the container lost its table, and a
    # table of members sits where it is not read.
    twice = {"a container whose table sits under a sub-heading"}
    wrong |= {label: got for label, text in mutations.items()
              if len((got := _docs.reference_entries(text))[1]) != (2 if label in twice else 1)}
    assert not wrong, f"the reference format parser misjudged: {wrong}"


def _ignored(paths: list[Path], root: Path) -> list[str]:
    """Which of *paths* git would not commit — a file a local run reads but a clean checkout lacks."""
    import subprocess

    if not paths:
        return []
    done = subprocess.run(["git", "check-ignore", "--no-index", "--stdin", "-z"], cwd=root,
                          input="\0".join(str(p.relative_to(root)) for p in paths).encode(),
                          capture_output=True)
    return sorted(name for name in done.stdout.decode("utf-8").split("\0") if name)


def test_no_skill_file_is_ignored_by_git() -> None:
    """F61 — every file in a skill is one git commits. `template.md` matched `.gitignore`'s
    `temp*.md`: every test passed here, and a clean checkout would have had no template."""
    files = sorted(p for p in _SKILLS_DIR.rglob("*") if p.is_file())
    assert files, "found no skill files; the scan is broken"
    ignored = _ignored(files, _arch.ROOT)
    assert not ignored, f"skill files git ignores, so a clean checkout lacks them: {ignored}"
    assert _ignored([_arch.ROOT / ".claude" / "skills" / "x" / "template.md"], _arch.ROOT), \
        "the ignore check found nothing even for a name .gitignore matches; it is broken"


def test_a_symlinked_skill_file_is_reported() -> None:
    """F60 — a SKILL.md symlinked outside the repo is reported once, never followed.

    Its own test so a host that cannot make a symlink skips it with the reason, under
    `-rs`, and CI's SQS_FAIL_ON_SKIP fails if the Linux image ever loses symlinks (F105).
    """
    import os
    import tempfile

    if (reason := _arch.symlink_unavailable()) is not None:
        pytest.skip(reason)
    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp) / "skills"
        (directory / "alpha").mkdir(parents=True)
        outside = Path(tmp) / "outside.md"
        outside.write_text("---\nname: alpha\ndescription: Draws diagrams.\n---\n\n# Body\n", encoding="utf-8")
        os.symlink(outside, directory / "alpha" / "SKILL.md")
        got = _skill_problems(directory, frozenset({"alpha"}))
    assert len(got) == 1, f"SKILL.md symlinked outside the repo was not reported once: {got}"


def test_the_mutation_checks_run_where_a_symlink_cannot_be_made() -> None:
    """F105 — on Windows without Developer Mode `os.symlink` raises WinError 1314.

    The symlink mutations used to sit inside the two anti-vacuity tests, so on such a host
    each errored before asserting the mutations around it. Simulated by making every
    symlink fail: both anti-vacuity tests must still run and pass, and each symlink test
    must skip, not error and not pass.
    """
    from unittest import mock

    import test_documentation

    denied = OSError(22, "A required privilege is not held by the client", None, 1314)
    with mock.patch("os.symlink", side_effect=denied):
        test_every_skill_check_can_fail()
        test_documentation.test_every_docs_tree_check_can_fail()
        for symlink_test in (test_a_symlinked_skill_file_is_reported,
                             test_documentation.test_a_symlinked_docs_entry_is_reported):
            with pytest.raises(pytest.skip.Exception, match="cannot create a symlink"):
                symlink_test()
