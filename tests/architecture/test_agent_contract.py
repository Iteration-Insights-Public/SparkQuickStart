"""
test_agent_contract.py — the .claude/agents/ definitions say true things.

Contains: frontmatter completeness, symmetry between an agent's read-only claim
and its tool grant, and agreement between a `## Dispatched from` section and the
commands it names — in all three directions.
Does NOT contain: whether an agent's advice is good. That is judgement, and
`/review-pr` owns it. This module only asserts the definitions are not lying.

Written after the 2026-09-26 agent audit, which found by hand: eight agents
claiming a `/fix` phase that dispatched nothing, two "reads and reports" agents
holding Write and Edit, and eight agents paraphrasing the rules instead of citing
them. A hand-found defect with no assertion behind it comes back.

The first version of this module was itself reviewed and found vacuous. Its
read-only check keyed on a 43-character prose substring, so rewording the bullet
dropped it to zero subjects and the assertion passed green while both agents held
every tool — reproduced before the rewrite. Every assertion here now states what
it must reach, and fails when it reaches less.
"""
from __future__ import annotations

import re
import tempfile
from pathlib import Path

import _arch
import _docs
import pytest
from _frontmatter import _split
from _frontmatter import block as _frontmatter_block
from _frontmatter import problem as _frontmatter_problem
from _frontmatter import read as _frontmatter
from _markdown import (
    blank_indented_code,
    blank_unrendered,
    heading_text,
    rendered_headings,
    strip_frontmatter,
)
from _yaml import load_mapping as _yaml_mapping

_AGENTS = sorted((_arch.CLAUDE_DIR / "agents").glob("*.md"))
# Recursive and case-blind on the suffix, as Claude Code 2.1.274 loads commands, read from
# the binary on 2026-10-02 (F104 adversary), not seen at runtime: it walks
# subdirectories (namespaced `/sub:name`) and takes any case of `.md` (F104 adversary).
def _command_files(directory: Path) -> list[Path]:
    return sorted(p for p in directory.rglob("*") if p.suffix.lower() == ".md" and not p.is_dir())


_COMMANDS = _command_files(_arch.CLAUDE_DIR / "commands")

# Tools that change the tree or run code. Claude Code 2.1.285, read from the binary on 2026-10-02
# (F190 and F196 adversaries), not seen at runtime: PowerShell and Monitor run a shell;
# `mcp__workspace` is the cloud sessions' workspace bash; MultiEdit is in the write-tool sets
# though no such tool is defined; CronCreate, RemoteTrigger, Workflow (alias RunWorkflow) and
# AppifactRepl are marked as executing code.
_SHELL_AND_WRITE_TOOLS = frozenset({
    "Bash", "Write", "Edit", "NotebookEdit", "MultiEdit", "PowerShell", "Monitor", "mcp__workspace",
    "mcp__workspace__bash", "CronCreate", "RemoteTrigger", "Workflow", "RunWorkflow", "AppifactRepl"})
# An agent that may not write holds none of these, nor Agent or Task (its alias), with which it
# could start a sub-agent that writes (F196). Commands may grant Agent: /discuss dispatches its
# read-only discussant with it.
_WRITE_TOOLS = _SHELL_AND_WRITE_TOOLS | {"Agent", "Task"}
# What JavaScript's String.prototype.trim() removes, which Claude Code applies to each tool name
# (same source): Python's whitespace without U+001C–U+001F and U+0085, plus U+FEFF.
_JS_SPACE = "".join(c for c in map(chr, range(0x3001))
                    if c.isspace() and c not in "\x1c\x1d\x1e\x1f\x85") + "\ufeff"


def _js_trim(text: str) -> str:
    """*text* trimmed as JavaScript's `trim()` trims it."""
    return text.strip(_JS_SPACE)

# The advisor roster, pinned. Deriving it from the files alone is what made the
# first version vacuous: both sides of the comparison could empty together and
# the assertion would still pass. Adding a read-only agent is a deliberate act
# and gets a deliberate line here.
_READ_ONLY = frozenset({"data-platform-architect", "delta-lake-specialist", "discussant", "pr-reviewer"})

_READS_AND_REPORTS = re.compile(r"reads and reports", re.I)


# A tools entry is split on commas and whitespace outside parentheses; `Bash(git diff:*)` is
# the Bash tool, scoped.
_TOOL_SPLIT = re.compile(r"[,\s]+(?![^()]*\))")


def _tools(path) -> set[str]:
    """The tool names an agent is granted, as a superset: an empty set means no `tools:` line.

    Split on commas and whitespace outside parentheses — the widest reading, so a write tool
    hidden in an ambiguous entry still counts; `_frontmatter_problem` reports the ambiguity
    itself — and each name trimmed as Claude Code trims it, so a byte-order mark beside `Bash`
    is Bash (F196). A scoped grant counts as its tool (`Bash(git diff:*)` is Bash); `*` counts as
    every write tool; so does any MCP entry, whose server's tools nothing here can list. Two
    frontmatter keys grant without `tools:` (F196 adversary, from the same binary): `memory:`
    adds Write and Edit, and `hooks:` runs a command. Stated limit: `tools: []`, which grants
    nothing, reads here as no `tools:` line, so such an agent is refused as unrestricted.
    """
    meta = _frontmatter(path)
    declared = meta.get("tools", "")
    entries = declared if isinstance(declared, list) else [declared]
    tokens = {_js_trim(_js_trim(token).split("(", 1)[0])
              for entry in entries for token in _TOOL_SPLIT.split(_js_trim(entry)) if _js_trim(token)}
    granted = (tokens - {"*"}) | (_WRITE_TOOLS if "*" in tokens else set())
    granted |= {"Bash"} if any(t.startswith("mcp__") for t in granted) else set()
    granted |= {"Write", "Edit"} if meta.get("memory") else set()
    granted |= {"Bash"} if "hooks" in meta else set()
    return granted


def _dispatched_from(path) -> str:
    """The body of the `## Dispatched from` section, or '' if absent."""
    match = re.search(
        r"^## Dispatched from\n(.*?)(?=\n## |\Z)",
        path.read_text(encoding="utf-8"),
        re.S | re.M,
    )
    return match.group(1) if match else ""


def _mentions(text: str, agent_name: str) -> bool:
    """Whole-token match, so `delta-lake-specialist` does not hit inside a word."""
    return re.search(rf"(?<![\w-]){re.escape(agent_name)}(?![\w-])", text) is not None


def test_the_agent_and_command_directories_are_populated() -> None:
    """Anti-vacuity: every other assertion here iterates these two lists."""
    assert len(_AGENTS) >= 5, f"expected the agent definitions, found {len(_AGENTS)}"
    assert len(_COMMANDS) >= 3, f"expected the command definitions, found {len(_COMMANDS)}"
    on_disk = {agent.stem for agent in _AGENTS}
    assert on_disk >= _READ_ONLY, (
        f"the pinned advisor roster names agents that are not in the tree: "
        f"{sorted(_READ_ONLY - on_disk)}. If an advisor was deliberately removed, "
        "remove it from _READ_ONLY in the same change."
    )


def test_every_agent_declares_name_description_and_model() -> None:
    """A missing `model:` is not a syntax error — it silently inherits the session's.

    That is the expensive failure: a mechanical single-file edit dispatched onto
    the most capable and most expensive model, with nothing in the file to say it
    happened. All nine agents lacked the field until the 2026-09-26 audit.
    """
    problems: list[str] = []
    for agent in _AGENTS:
        fields = _frontmatter(agent)
        problems.extend(
            f"{_arch.rel(agent)} is missing `{key}:`"
            for key in ("name", "description", "model")
            if not fields.get(key)
        )
        if fields.get("name") and fields["name"] != agent.stem:
            problems.append(
                f"{_arch.rel(agent)} declares name {fields['name']!r} but its filename "
                f"says {agent.stem!r}; the filename is the address a dispatch uses"
            )
    assert not problems, f"agent frontmatter is incomplete: {problems}"


def test_the_read_only_claim_and_the_tool_grant_are_the_same_set() -> None:
    """A prose "Must not: Edit files" facing a model that also holds Edit.

    `data-platform-architect` and `delta-lake-specialist` both said "This agent
    reads and reports" while their frontmatter granted every tool. A reviewer
    that can edit is a reviewer that can quietly fix what it was asked to judge
    and then report the code clean — and nothing in the transcript would show it.

    Asserted as a set equality against a pinned roster, not as an implication.
    The implication form ("anything claiming read-only must be restricted") is
    satisfied by claiming nothing, which is how the first version of this test
    passed while both agents held every tool.
    """
    claims = {agent.stem for agent in _AGENTS if _READS_AND_REPORTS.search(agent.read_text(encoding="utf-8"))}
    restricted = {
        agent.stem for agent in _AGENTS if _tools(agent) and not (_tools(agent) & _WRITE_TOOLS)
    }

    assert claims == _READ_ONLY, (
        "the agents whose prose claims they only read and report are not the pinned "
        f"advisor roster. Claiming: {sorted(claims)}. Pinned: {sorted(_READ_ONLY)}. "
        "Either the claim was reworded out of a file that still must not write, or an "
        "agent gained the claim without being added to _READ_ONLY."
    )
    assert restricted == _READ_ONLY, (
        "the agents restricted by `tools:` are not the pinned advisor roster. "
        f"Restricted: {sorted(restricted)}. Pinned: {sorted(_READ_ONLY)}. An advisor "
        "without a restrictive `tools:` line holds every tool, Edit included."
    )
    for name in sorted(_READ_ONLY):
        agent = next(a for a in _AGENTS if a.stem == name)
        granted = _tools(agent)
        assert granted and not granted & _WRITE_TOOLS, (
            f"{_arch.rel(agent)} is granted {sorted(granted & _WRITE_TOOLS)}"
        )


@pytest.mark.parametrize("tool", [
    "PowerShell", "Monitor", "mcp__workspace__bash", "mcp__workspace", "MultiEdit", "Bash", "Write",
    "Edit", "NotebookEdit",
    # F196's adversary:
    "Bash\ufeff", "\ufeffBash", "mcp__workspace__*", "mcp__github", "CronCreate", "RemoteTrigger",
    "Workflow", "AppifactRepl", "Agent", "Task", "Agent(general-purpose)"])
def test_a_shell_or_write_tool_makes_an_agent_writable(tool: str) -> None:
    """F196 — an agent granted any tool that runs a shell or writes is not read-only.

    The set once held Bash, Write, Edit and NotebookEdit; F190's adversary read PowerShell,
    Monitor and the cloud workspace bash as shells in Claude Code 2.1.285's binary.
    """
    with tempfile.TemporaryDirectory() as tmp:
        agent = Path(tmp) / "advisor.md"
        agent.write_text(f"---\nname: advisor\ntools: Read, {tool}\n---\n\nBody.\n", encoding="utf-8")
        assert _tools(agent) & _WRITE_TOOLS, f"{tool!r} was not counted as a write tool"


@pytest.mark.parametrize("extra", ["memory: project", "hooks:\n  Stop:\n    - hooks:\n        - type: command\n          command: touch x"])
def test_a_frontmatter_key_that_grants_without_tools_makes_an_agent_writable(extra: str) -> None:
    """F196 adversary — `memory:` adds Write and Edit, and `hooks:` runs a command, whatever `tools:` says."""
    with tempfile.TemporaryDirectory() as tmp:
        agent = Path(tmp) / "advisor.md"
        agent.write_text(f"---\nname: advisor\ntools: Read, Grep\n{extra}\n---\n\nBody.\n", encoding="utf-8")
        assert _tools(agent) & _WRITE_TOOLS, f"{extra!r} was not counted"


@pytest.mark.parametrize("tools", ["Read, Grep, Glob", "Read, bash", "Read, KillShell, WebFetch, TodoWrite"])
def test_read_tools_leave_an_agent_read_only(tools: str) -> None:
    """F196 — controls: read tools, a lower-case name Claude Code matches to nothing, and tools
    that change no file."""
    with tempfile.TemporaryDirectory() as tmp:
        agent = Path(tmp) / "advisor.md"
        agent.write_text(f"---\nname: advisor\ntools: {tools}\n---\n\nBody.\n", encoding="utf-8")
        assert not _tools(agent) & _WRITE_TOOLS, sorted(_tools(agent) & _WRITE_TOOLS)


def test_dispatch_claims_agree_with_the_commands_in_all_three_directions() -> None:
    """Eight agents claimed a `/fix` phase. fix.md had never mentioned an agent.

    The wiring pointed one way, and the wrong way — so on the one live `/fix` run
    every phase ran unassisted and no document said that was a deviation. Three
    directions, because each catches what the others cannot:

    1. a **claimed** command must exist and must name the agent back. The
       original defect: an agent asserting a dispatcher that does not dispatch it.
    2. a **disclaimed** command (a `Not /x` paragraph) must *not* name the agent.
       The opposite drift: a command quietly starting to dispatch an agent that
       documents, with a reason, why it should not be.
    3. every agent a **command** names must claim or disclaim that command. The
       direction the first version missed entirely: adding `refactoring-engineer`
       to `fix.md` failed nothing, because the scan started from the agents and
       an unmentioned command was simply never visited.

    Direction 3 also settles what "names it" means: a command that mentions an
    agent dispatches it. To say an agent must *not* be used, do not name it —
    say it in the agent's own disclaimer, where direction 2 enforces it. A
    command reading "never dispatch X" would satisfy a substring test for X, and
    resolving that by parsing negation is guesswork; the convention removes the
    ambiguity instead.
    """
    problems: list[str] = []
    claimed: dict[str, set[str]] = {}
    disclaimed: dict[str, set[str]] = {}

    for agent in _AGENTS:
        section = _dispatched_from(agent)
        if not section.strip():
            problems.append(f"{_arch.rel(agent)} has no `## Dispatched from` section")
            continue
        claimed[agent.stem], disclaimed[agent.stem] = set(), set()
        for paragraph in section.split("\n\n"):
            commands = set(re.findall(r"`/([a-z-]+)`", paragraph))
            if not commands:
                continue
            # lstrip() first: the captured section starts at the blank line after
            # the heading, so the leading paragraph begins with a newline and a
            # bare startswith() read a leading disclaimer as a claim.
            stripped = paragraph.strip().lstrip("*")
            bucket = disclaimed if stripped.startswith("Not ") else claimed
            bucket[agent.stem] |= commands

        both = claimed[agent.stem] & disclaimed[agent.stem]
        if both:
            problems.append(
                f"{_arch.rel(agent)} both claims and disclaims {sorted(both)}; "
                "one paragraph per command, and the disclaimer paragraph starts with 'Not '"
            )

    for name, commands in claimed.items():
        for command in sorted(commands):
            path = _arch.CLAUDE_DIR / "commands" / f"{command}.md"
            if not path.is_file():
                problems.append(f"{name} names /{command}, which is not in .claude/commands/")
            elif not _mentions(path.read_text(encoding="utf-8"), name):
                problems.append(
                    f"{name} claims /{command} dispatches it, but {command}.md never mentions it"
                )

    for name, commands in disclaimed.items():
        for command in sorted(commands):
            path = _arch.CLAUDE_DIR / "commands" / f"{command}.md"
            if path.is_file() and _mentions(path.read_text(encoding="utf-8"), name):
                problems.append(f"{name} says 'Not /{command}' but {command}.md names it")

    for command in _COMMANDS:
        text = command.read_text(encoding="utf-8")
        for agent in _AGENTS:
            if not _mentions(text, agent.stem):
                continue
            if command.stem not in claimed.get(agent.stem, set()) | disclaimed.get(agent.stem, set()):
                problems.append(
                    f"{_arch.rel(command)} names {agent.stem}, but that agent's "
                    "`## Dispatched from` section neither claims nor disclaims this command"
                )

    assert problems == [], f"an agent's dispatch claim disagrees with the command: {problems}"


# The owner's policy (F59), pinned: a role decides the model. Changing a pair is a policy
# change — this line and .claude/workflow/agent-models.md move together.
_ROLE_MODELS = {"judgement": "opus", "implementation": "sonnet", "mechanical": "haiku"}
# The owner kept refactoring-engineer on opus: deciding what to restructure is judgement.
_PINNED_ROLES = {"refactoring-engineer": "judgement"}
_MODEL_POLICY = _arch.CLAUDE_DIR / "workflow" / "agent-models.md"
_ROLE_HEADER = ["Role", "Model", "When"]
_AGENT_HEADER = ["Agent", "Role", "Model", "Why"]


def _policy_tables(text: str) -> tuple[list[list[str]] | None, list[list[str]] | None, list[str]]:
    """The roles table and the agents table, found by their exact header rows."""
    problems, roles, agents = [], None, None
    for table in _docs.tables(text):
        header = table[0] if table else []
        if header[:1] in (_ROLE_HEADER[:1], _AGENT_HEADER[:1]) and header not in (_ROLE_HEADER, _AGENT_HEADER):
            # A near-miss header is a second policy the check would silently ignore (F59 adversary).
            problems.append(f"a table headed {header} is neither {_ROLE_HEADER} nor {_AGENT_HEADER}")
        # The first table of each kind is the policy; a second is reported, never merged.
        if header == _ROLE_HEADER:
            if roles is not None:
                problems.append("the policy has two roles tables")
            else:
                roles = table[1:]
        elif header == _AGENT_HEADER:
            if agents is not None:
                problems.append("the policy has two agents tables")
            else:
                agents = table[1:]
    return roles, agents, problems


def _agent_directory_problems(directory: Path) -> list[str]:
    """Anything under .claude/agents/ the `*.md` scan would miss but the harness might load."""
    return [f".claude/agents/{entry.name} is not a top-level .md agent; the model policy cannot see it"
            for entry in sorted(directory.iterdir())
            if entry.name != ".DS_Store" and not (entry.is_file() and entry.suffix == ".md")]


def _disallowed_tools_problems(paths: list[Path]) -> list[str]:
    """`disallowedTools` narrows a grant the read-only rule reads as complete; not covered yet."""
    return [f"{path.name} uses disallowedTools, which the read-only rule does not read"
            for path in paths if "disallowedTools" in _frontmatter(path)]


def _model_policy_problems(policy: str | None, agents: dict[str, tuple[str, set[str]]]) -> list[str]:
    """Every way an agent's model departs from the written role policy (F59).

    *agents* maps each agent on disk to its frontmatter `model:` and its tool grant. The
    roles table must be exactly _ROLE_MODELS; the agents table must name every agent on disk
    once and nothing else, with a known role, that role's model, a reason, and the model the
    agent's own frontmatter declares — the file the harness reads. A read-only advisor is
    judgement by definition: it exists to decide, not to type.
    """
    if policy is None:
        return [f"{_arch.rel(_MODEL_POLICY)} is missing; which model a role gets is written nowhere"]
    roles, table, problems = _policy_tables(policy)
    if roles is None or table is None:
        return [*problems, f"the policy needs a {_ROLE_HEADER} table and an {_AGENT_HEADER} table"]
    written = {row[0]: row[1] for row in roles if len(row) >= 2}
    if written != _ROLE_MODELS or len(roles) != len(_ROLE_MODELS):
        problems.append(f"the roles table says {written}; the owner's policy is {_ROLE_MODELS}")
    seen: dict[str, int] = {}
    for row in table:
        name, role, model, why = [*row, "", "", "", ""][:4]
        seen[name] = seen.get(name, 0) + 1
        if name not in agents:
            problems.append(f"the policy lists {name!r}, which is not an agent on disk")
            continue
        declared, tools = agents[name]
        if role not in _ROLE_MODELS:
            problems.append(f"{name} has role {role!r}; roles are {sorted(_ROLE_MODELS)}")
            continue
        if model != _ROLE_MODELS[role]:
            problems.append(f"{name} is {role} but the policy gives it {model!r}, not {_ROLE_MODELS[role]}")
        if declared != model:
            problems.append(f"{name}'s frontmatter declares model {declared!r}; the policy says {model!r}")
        if not why.strip():
            problems.append(f"{name} has no reason for its role")
        if not tools & _WRITE_TOOLS and tools and role != "judgement":
            problems.append(f"{name} is a read-only advisor but its role is {role!r}, not judgement")
        if name in _PINNED_ROLES and role != _PINNED_ROLES[name]:
            problems.append(f"{name} must be {_PINNED_ROLES[name]} (the owner's ruling), not {role!r}")
    problems += [f"the policy lists {name} {count} times" for name, count in seen.items() if count > 1]
    problems += [f"{name} is on disk but not in the policy" for name in sorted(set(agents) - set(seen))]
    return problems


def test_every_agent_model_matches_its_role() -> None:
    """F59 — each agent runs on the model its written role gives it, and the policy covers all."""
    agents = {path.stem: (str(_frontmatter(path).get("model", "")), _tools(path)) for path in _AGENTS}
    assert len(agents) >= 5, "found almost no agents; the scan is broken"
    policy = _MODEL_POLICY.read_text(encoding="utf-8") if _MODEL_POLICY.is_file() else None
    problems = _model_policy_problems(policy, agents) + _agent_directory_problems(_arch.CLAUDE_DIR / "agents")
    problems += _disallowed_tools_problems(_AGENTS)
    assert not problems, f"agent models depart from the written policy: {problems}"


def test_every_model_policy_check_can_fail() -> None:
    """F59 — anti-vacuity: each rule, mutated alone, is reported exactly once."""
    agents = {"pr-reviewer": ("opus", {"Read", "Grep"}), "test-engineer": ("sonnet", {"Read", "Bash"}),
              "refactoring-engineer": ("opus", set())}
    roles = ("| Role | Model | When |\n|---|---|---|\n| judgement | `opus` | deciding |\n"
             "| implementation | `sonnet` | building |\n| mechanical | `haiku` | edits |\n")
    rows = {"pr-reviewer": "| pr-reviewer | judgement | `opus` | reviews |",
            "test-engineer": "| `test-engineer` | implementation | sonnet | writes tests |",
            "refactoring-engineer": "| refactoring-engineer | judgement | opus | owner's ruling |"}

    def policy(roles_text: str = roles, **replace: str) -> str:
        body = "\n".join(replace.get(k, v) for k, v in rows.items())
        return f"# P\n\n{roles_text}\n| Agent | Role | Model | Why |\n|---|---|---|---|\n{body}\n"

    assert _model_policy_problems(policy(), agents) == [], "the valid policy was reported"
    mutations = {
        "no policy file": (None, agents),
        "a role remapped": (policy(roles.replace("`sonnet` | building", "`opus` | building")), agents),
        "an extra role": (policy(roles + "| other | opus | x |\n"), agents),
        "no roles table": (policy(""), agents),
        "a model off its role": (policy(**{"test-engineer": "| test-engineer | implementation | opus | x |"}),
                                 {**agents, "test-engineer": ("opus", set())}),
        "an unknown role": (policy(**{"test-engineer": "| test-engineer | senior | sonnet | x |"}), agents),
        "frontmatter disagrees": (policy(), {**agents, "test-engineer": ("haiku", set())}),
        "no reason": (policy(**{"test-engineer": "| test-engineer | implementation | sonnet |  |"}), agents),
        "a read-only implementer": (policy(**{"pr-reviewer": "| pr-reviewer | implementation | sonnet | x |"}),
                                    {**agents, "pr-reviewer": ("sonnet", {"Read", "Grep"})}),
        "refactoring-engineer demoted": (policy(**{"refactoring-engineer": "| refactoring-engineer | implementation | sonnet | x |"}),
                                         {**agents, "refactoring-engineer": ("sonnet", set())}),
        "an agent missing from the policy": (policy(), {**agents, "new-agent": ("opus", set())}),
        "a ghost agent in the policy": (policy(), {k: v for k, v in agents.items() if k != "test-engineer"}),
        # F59's adversary:
        "two roles tables": (policy(roles + "\n" + roles), agents),
        "two agents tables": (policy() + "\n| Agent | Role | Model | Why |\n|---|---|---|---|\n"
                              + rows["pr-reviewer"] + "\n", agents),
        "a near-miss agents header": (policy() + "\n| Agent | Role | Model | Reason |\n|---|---|---|---|\n"
                                      "| test-engineer | mechanical | haiku | r |\n", agents),
        "a role row twice": (policy(roles + "| judgement | `opus` | again |\n"), agents),
        "an agent listed twice": (policy(**{"test-engineer": rows["test-engineer"] + "\n" + rows["test-engineer"]}), agents),
    }
    wrong = {label: got for label, (text, disk) in mutations.items() if len(got := _model_policy_problems(text, disk)) != 1}
    assert not wrong, f"these mutations were not reported exactly once: {wrong}"
    # Each write tool alone makes an implementer writable; without it the agent is read-only.
    for tool in sorted(_WRITE_TOOLS):
        writable = {**agents, "test-engineer": ("sonnet", {"Read", tool})}
        assert _model_policy_problems(policy(), writable) == [], f"{tool} was not counted as a write tool"
    read_only = {**agents, "test-engineer": ("sonnet", {"Read", "Grep"})}
    assert len(_model_policy_problems(policy(), read_only)) == 1, "a read-only implementer was not reported"
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp)
        (directory / "a.md").write_text("x", encoding="utf-8")
        assert _agent_directory_problems(directory) == [], "a top-level .md agent was reported"
        (directory / "sub").mkdir()
        (directory / "b.MD").write_text("x", encoding="utf-8")
        assert len(_agent_directory_problems(directory)) == 2, "a nested or .MD agent went unseen"
        narrowed = directory / "narrowed.md"
        narrowed.write_text("---\nname: narrowed\nmodel: sonnet\ndisallowedTools: Edit\n---\n", encoding="utf-8")
        assert len(_disallowed_tools_problems([directory / "a.md", narrowed])) == 1, "disallowedTools went unseen"


def test_frontmatter_reads_as_yaml() -> None:
    """F103 — `_frontmatter` reads YAML, and reports every form Claude Code may read otherwise.

    Each read case was misread by the old line-by-line reader (F59's adversary, B1 to B6). B3 is
    the dangerous one: an indented `model:` inside a block scalar overwrote the real top-level
    value, so the model-policy check compared the policy with the wrong model. Every read case
    must also raise no problem, and every reported case must be reported for its own reason.
    """
    import tempfile

    read: dict[str, tuple[str, dict[str, object]]] = {
        "B1 a comment after the tools": ("tools: Read, Grep # , Bash\n", {"tools": {"Read", "Grep"}}),
        "B2 tools as a scalar on the next line": ("tools:\n  Read, Grep, Glob\n", {"tools": {"Read", "Grep", "Glob"}}),
        "B3 a nested model": ("model: haiku\nmetadata:\n  model: opus\n", {"model": "haiku"}),
        "B4 a model line inside a description": ("model: haiku\ndescription: |\n  Runs on\n  model: opus\n",
                                                 {"model": "haiku"}),
        "a quoted model": ('model: "opus"\n', {"model": "opus"}),
        "a comment after the model": ("model: opus # judgement\n", {"model": "opus"}),
        "a flow-list tools": ("tools: [Read, Bash]\n", {"tools": {"Read", "Bash"}}),
        "a block-list tools": ("tools:\n  - Read\n  - Grep\n", {"tools": {"Read", "Grep"}}),
        "a quoted tools": ("tools: 'Read, Grep'\n", {"tools": {"Read", "Grep"}}),
        "a wildcard counts as every write tool": ("tools: Read, *\n", {"tools": {"Read"} | _WRITE_TOOLS}),
        "a scoped grant counts as its tool": ("tools: Read, Bash(git diff:*)\n", {"tools": {"Read", "Bash"}}),
    }
    reported: dict[str, tuple[bytes, str]] = {
        "unparseable YAML": (b"---\nname: [x\n---\n", "not valid YAML"),
        "no closing fence": (b"---\nname: x\n", "no closing"),
        "an empty block": (b"---\n---\n", "not a YAML mapping"),
        "B6 a duplicate model key": (b"---\nname: x\nmodel: haiku\nmodel: opus\n---\n", "duplicate key"),
        "a --- inside a value": (b"---\nname: x\ndescription: a --- b\nmodel: opus\n---\n", "`---` inside"),
        "a --- line in a block scalar": (b"---\nname: x\ndescription: |\n  a\n  ---\nmodel: opus\n---\n", "`---` inside"),
        "a bare CR inside the block": (b"---\nname: x\ndescription: a\rmodel: haiku\n---\n", "line break"),
        "a NEL inside a value": ("---\nname: x\ndescription: a\x85model: haiku\n---\n".encode(), "line break"),
        "a non-string description": (b"---\nname: x\ndescription: [x]\n---\n", "`description:`"),
        "a non-string model": (b"---\nname: x\nmodel: [opus]\n---\n", "`model:`"),
        "a padded model": (b"---\nname: x\nmodel: ' opus '\n---\n", "`model:`"),
        "tools as a mapping": (b"---\nname: x\ntools: {Read: true}\n---\n", "`tools:` is"),
        "space-separated tools": (b"---\nname: x\ntools: Read Grep\n---\n", "ambiguous"),
        "a comma inside a list item": (b"---\nname: x\ntools: [Read, 'Grep, Bash']\n---\n", "ambiguous"),
        "a space inside a block item": (b"---\nname: x\ntools:\n  - Read Edit\n---\n", "ambiguous"),
        # F104 adversary: a stray `)` once made the space look parenthesised.
        "a stray parenthesis hides a second tool": (b"---\nname: x\ntools: Read, Write x)\n---\n", "ambiguous"),
        "an unbalanced parenthesis alone": (b"---\nname: x\ntools: Read, Write)\n---\n", "ambiguous"),
        "an unbalanced parenthesis in a list item": (b"---\nname: x\ntools:\n  - Write)\n---\n", "ambiguous"),
    }
    wrong: dict[str, object] = {}
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "agent.md"
        for label, (body, expected) in read.items():
            path.write_text(f"---\nname: agent\n{body}---\n\nmodel: sonnet\n", encoding="utf-8")
            got = {"model": _frontmatter(path).get("model"), "tools": _tools(path)}
            if _frontmatter_problem(path) is not None or any(got[k] != v for k, v in expected.items()):
                wrong[label] = (got, _frontmatter_problem(path))
        for label, (raw, fragment) in {
            "B5 a closing fence with a trailing space": (b"---\nname: x\nmodel: haiku\n--- \n\nmodel: opus\n", "haiku"),
            "a byte-order mark": ("\ufeff---\nname: x\nmodel: opus\n---\n".encode(), "opus"),
            "CRLF": (b"---\r\nname: x\r\nmodel: opus\r\n---\r\n", "opus"),
        }.items():
            path.write_bytes(raw)
            if _frontmatter_problem(path) is not None or _frontmatter(path).get("model") != fragment:
                wrong[label] = (_frontmatter(path), _frontmatter_problem(path))
        for label, (raw, reason) in reported.items():
            path.write_bytes(raw)
            if reason not in (_frontmatter_problem(path) or ""):
                wrong[label] = _frontmatter_problem(path)
    assert not wrong, f"frontmatter misread: {wrong}"


def test_every_agent_frontmatter_parses() -> None:
    """F103 — an agent whose frontmatter is not valid YAML is reported, not read as empty."""
    problems = [f"{_arch.rel(agent)}: {why}" for agent in _AGENTS if (why := _frontmatter_problem(agent))]
    assert not problems, f"agent frontmatter does not parse: {problems}"



# ── F104: a command's frontmatter parses, and sets only what a command may ────────

# An allowlist, as for skills (F60): Claude Code merged custom commands into skills, so a
# command's frontmatter can set `model:`, `context: fork` or `effort:` exactly as a skill's
# can, and each moves the command off the session's model or out of its reach. A field is
# allowed by a deliberate line here.
_COMMAND_FIELDS = frozenset({"description", "argument-hint", "allowed-tools"})
# Tools that change the tree or run code (F190). Unscoped in a command's `allowed-tools`, each
# moves the command past the session's permission prompt for it. What follows here and in
# `_unscoped_grant` is Claude Code 2.1.285, read from the binary on 2026-10-02 (F190
# adversary), not seen at runtime: PowerShell and Monitor run a shell; `mcp__workspace` is the
# built-in workspace bash of cloud sessions; MultiEdit is in the write-tool sets though no such
# tool is defined.
_UNSCOPED_REFUSED = _SHELL_AND_WRITE_TOOLS
# Tools no rule content is known to narrow (F196 review): no matcher for them was found, so a
# scoped spelling such as `CronCreate(x)` is refused as the tool itself.
_UNSCOPABLE = frozenset({"CronCreate", "RemoteTrigger", "Workflow", "RunWorkflow", "AppifactRepl"})


def _unscoped_grant(name: str) -> str | None:
    """The refused tool *name* grants without a real scope, or None (F190).

    Read as Claude Code's rule parser reads a rule (see `_UNSCOPED_REFUSED` for the source):
    `Tool()` and `Tool(*)` are the bare tool; a Bash or PowerShell pattern that starts with `*`
    matches every command (`Bash(:*)`, an empty prefix, matches none, so it passes); a file
    glob reaches past a scope when it starts at `//` (the filesystem root) or `~`, climbs with
    `..`, or has a wildcard or nothing as its first segment once `./` and the project-relative
    `/` are dropped. A scope that names a prefix (`Bash(git diff:*)`, `Edit(docs/**)`) is
    allowed, even one broad in effect (`Bash(bash:*)`): that is F195's question.
    """
    name = _js_trim(name)
    tool, _, rest = name.partition("(")
    if tool not in _UNSCOPED_REFUSED:
        return None
    if tool in _UNSCOPABLE:
        return name
    content = _js_trim(rest[:-1]) if rest.endswith(")") else None
    if content is None or content in ("", "*"):
        return tool
    if tool in ("Bash", "PowerShell", "Monitor") and content.startswith("*"):
        return f"{tool}({content})"
    if tool in ("Edit", "Write", "NotebookEdit", "MultiEdit"):
        if content.startswith(("//", "~")):
            return f"{tool}({content})"
        path = content.removeprefix("/")
        while path.startswith("./"):
            path = path[2:]
        first = path.split("/", 1)[0]
        if "*" in first or first in ("", ".", ".."):
            return f"{tool}({content})"
    return None


def _command_problems(path: Path) -> list[str]:
    """Every way one command's frontmatter departs from the contract; none without one.

    A command need not have frontmatter. Whether it has one is decided from its first line
    alone, before the fail-closed reader: a first line that starts with `---` opens a block,
    and Claude Code's opener (`^---\\s*\\n`, JavaScript `\\s`) also takes a no-break or other
    Unicode space after the dashes, which the reader's narrower fence refuses. So such a
    line is reported, never read as "no frontmatter" (F104 adversary: `---\\u00a0` then
    `model: opus` passed here; the opener was read from the 2.1.274 binary on 2026-10-02,
    not seen applied at runtime).
    """
    try:
        first = path.read_bytes().decode("utf-8-sig").split("\n", 1)[0]
    except (OSError, UnicodeDecodeError) as exc:
        return [f"cannot be read: {exc.__class__.__name__}"]
    if not first.startswith("---"):
        return []
    if (why := _frontmatter_problem(path)) is not None:
        return [why]
    doc, _ = _yaml_mapping(_frontmatter_block(path)[0] or "")
    keys = list(doc or {})
    if any(not isinstance(key, str) for key in keys):
        return [f"a key that is not a string: {[k for k in keys if not isinstance(k, str)]!r}"]
    extra = sorted(set(keys) - _COMMAND_FIELDS)
    if extra:
        return [f"sets {extra}; a command may set only {sorted(_COMMAND_FIELDS)}"]
    if "argument-hint" in doc and not isinstance(doc["argument-hint"], str):
        return [f"`argument-hint:` is {doc['argument-hint']!r}; quote it, or it is shown as something else"]
    granted = doc.get("allowed-tools", [])
    entries = [granted] if isinstance(granted, str) else granted
    names = [n for entry in entries for n in _split(entry, ", ")[0] if n]
    unscoped = sorted({g for n in names if (g := _unscoped_grant(n)) is not None})
    if unscoped:
        return [(f"`allowed-tools:` grants {unscoped} without a scope; a command may pre-approve a "
                 "tree-changing tool only scoped, such as `Bash(git diff:*)` (F190)")]
    if any("*" in name.split("(", 1)[0] for entry in entries for name in entry.split(",")):
        # The 2.1.274 binary's allowed-tools reader, read on 2026-10-02 (F104 adversary),
        # collapses a list holding `*` to `["*"]`; what the permission layer then grants was
        # not traced, so this refuses the grant rather than say what it does.
        return ["`allowed-tools:` grants `*`, which the reader collapses to a wildcard"]
    return []


def test_every_command_frontmatter_parses() -> None:
    """F104 — a command's frontmatter is read fail closed, as an agent's is (F103)."""
    assert len(_COMMANDS) >= 3, f"expected the command definitions, found {len(_COMMANDS)}"
    problems = [f"{_arch.rel(c)}: {why}" for c in _COMMANDS for why in _command_problems(c)]
    # A nested command is addressed as /sub:name, which an agent's `/name` claim cannot name,
    # so the dispatch check would match it by its bare stem (F104 review). Refused until
    # claims can name it.
    problems += [f"{_arch.rel(c)}: a nested command; agents' claims cannot address /sub:name"
                 for c in _COMMANDS if c.parent != _arch.CLAUDE_DIR / "commands"]
    assert not problems, f"command frontmatter departs from the contract: {problems}"


def test_every_command_frontmatter_check_can_fail() -> None:
    """F104 — anti-vacuity: each form is reported exactly once, each valid one not at all."""
    good = "---\ndescription: Does a thing\nargument-hint: <id>\nallowed-tools:\n  - Read\n  - Bash(git diff:*)\n---\n\n# Body\n"
    mutations = {
        "unparseable YAML": "---\ndescription: [x\n---\n",
        "no closing fence": "---\ndescription: x\n",
        "a model of its own": good.replace("---\n\n", "model: opus\n---\n\n"),
        "forked onto another agent": good.replace("---\n\n", "context: fork\n---\n\n"),
        "an effort of its own": good.replace("---\n\n", "effort: low\n---\n\n"),
        "space-separated allowed-tools": "---\ndescription: x\nallowed-tools: Read Grep\n---\n",
        "a comma inside an allowed-tools item": "---\ndescription: x\nallowed-tools: [Read, 'Grep, Bash']\n---\n",
        "allowed-tools as a mapping": "---\ndescription: x\nallowed-tools: {Read: true}\n---\n",
        "a non-string description": "---\ndescription: [x]\n---\n",
        # F104's adversary:
        "a no-break space after the opening fence": "---\u00a0\nmodel: opus\n---\nbody\n",
        "an ideographic space after the opening fence": "---\u3000\ncontext: fork\n---\nbody\n",
        "a stray parenthesis hides a second tool": "---\ndescription: x\nallowed-tools: Read, Write x)\n---\n",
        "a stray parenthesis in a list item": "---\ndescription: x\nallowed-tools:\n  - Write x)\n---\n",
        "a separator after a nested parenthesis": "---\ndescription: x\nallowed-tools: Bash(a(b) Write ,c)\n---\n",
        "a wildcard grant": "---\ndescription: x\nallowed-tools: '*'\n---\n",
        "a wildcard among others": "---\ndescription: x\nallowed-tools: [Read, '*']\n---\n",
        "a key that is not a string": "---\ndescription: x\n1: a\n---\n",
        "an unbalanced parenthesis, nothing else wrong": "---\ndescription: x\nallowed-tools: Read, Write)\n---\n",
        "an unbalanced parenthesis in a list item": "---\ndescription: x\nallowed-tools:\n  - Write)\n---\n",
        "an argument-hint that is a list": "---\ndescription: x\nargument-hint: [opus | sonnet]\n---\n",
        # F190: a command may not pre-approve a tree-changing tool without a scope. Each label
        # in this block must be reported by the F190 rule itself (`_F190_LABELS` below).
        "an unscoped Bash": "---\ndescription: x\nallowed-tools: Read, Bash\n---\n",
        "an unscoped Write in a list": "---\ndescription: x\nallowed-tools:\n  - Read\n  - Write\n---\n",
        "an unscoped Edit": "---\ndescription: x\nallowed-tools: [Edit]\n---\n",
        "an unscoped NotebookEdit": "---\ndescription: x\nallowed-tools: NotebookEdit\n---\n",
        # F190's adversary, from the 2.1.285 binary.
        "empty parentheses": "---\ndescription: x\nallowed-tools: Bash()\n---\n",
        "a star scope": "---\ndescription: x\nallowed-tools: [\"Bash(*)\"]\n---\n",
        "a double-star Bash scope": "---\ndescription: x\nallowed-tools: Bash(**)\n---\n",
        "a spaced star Bash scope": "---\ndescription: x\nallowed-tools: [\"Bash( * )\"]\n---\n",
        "a Bash scope starting with a star": "---\ndescription: x\nallowed-tools: [\"Bash(* --version)\"]\n---\n",
        "an Edit over the whole tree": "---\ndescription: x\nallowed-tools: Edit(**)\n---\n",
        "an Edit from the filesystem root": "---\ndescription: x\nallowed-tools: Edit(//**)\n---\n",
        "an Edit over home": "---\ndescription: x\nallowed-tools: Edit(~/**)\n---\n",
        "a Write star scope": "---\ndescription: x\nallowed-tools: Write(*)\n---\n",
        "a byte-order mark after the name": "---\ndescription: x\nallowed-tools: \"Bash\\ufeff\"\n---\n",
        "PowerShell": "---\ndescription: x\nallowed-tools: PowerShell\n---\n",
        "Monitor": "---\ndescription: x\nallowed-tools: Monitor\n---\n",
        "the workspace bash": "---\ndescription: x\nallowed-tools: mcp__workspace__bash\n---\n",
        "the workspace server": "---\ndescription: x\nallowed-tools: mcp__workspace\n---\n",
        "an Edit over the whole tree from ./": "---\ndescription: x\nallowed-tools: Edit(./**)\n---\n",
        "an Edit from the project root over all of it": "---\ndescription: x\nallowed-tools: Edit(/**)\n---\n",
        "an unscoped CronCreate": "---\ndescription: x\nallowed-tools: CronCreate\n---\n",
        "a Workflow with a scope nothing narrows": "---\ndescription: x\nallowed-tools: Workflow(x)\n---\n",
        "an Edit climbing out": "---\ndescription: x\nallowed-tools: Edit(../**)\n---\n",
    }
    controls = {
        "the valid frontmatter": good,
        "no frontmatter at all": "# A command\n\nBody.\n",
        "allowed-tools as a string": "---\ndescription: x\nallowed-tools: Read, Grep, Bash(git log:*)\n---\n",
        "a flow list": "---\ndescription: x\nallowed-tools: [Read, Agent]\n---\n",
        "a scoped Bash with a wildcard argument": "---\ndescription: x\nallowed-tools: Bash(git log:*)\n---\n",
        "scoped Bash calls in a list": "---\ndescription: x\nallowed-tools:\n  - Bash(git diff:*)\n  - Bash(gh pr view:*)\n---\n",
        "a scoped Edit, the form that grants a scoped write": "---\ndescription: x\nallowed-tools: Read, Edit(docs/**)\n---\n",
        "a scoped Write, inert in Claude Code but not refused": "---\ndescription: x\nallowed-tools: Write(docs/**)\n---\n",
        "a Bash prefix scope with spaces": "---\ndescription: x\nallowed-tools: [\"Bash(git diff *)\"]\n---\n",
        "a lower-case name, which matches no tool": "---\ndescription: x\nallowed-tools: bash\n---\n",
        "an Edit scoped from ./": "---\ndescription: x\nallowed-tools: Edit(./docs/**)\n---\n",
        "an Edit scoped from the project root": "---\ndescription: x\nallowed-tools: Edit(/docs/**)\n---\n",
        "an empty Bash prefix, which matches no command": "---\ndescription: x\nallowed-tools: Bash(:*)\n---\n",
        "no frontmatter and a form feed in the body": "# Cmd\n\nA\x0cB\n",
        "a body that does not start at the first line": "\n---\nmodel: opus\n---\n",
    }
    wrong: dict[str, list[str]] = {}
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "command.md"
        for label, text in mutations.items():
            path.write_text(text, encoding="utf-8")
            if len(got := _command_problems(path)) != 1:
                wrong[label] = got
        f190 = list(mutations)[list(mutations).index("an unscoped Bash"):list(mutations).index("an Edit climbing out") + 1]
        for label in f190:
            path.write_text(mutations[label], encoding="utf-8")
            if not any("(F190)" in p for p in _command_problems(path)):
                wrong[f"{label} (not by the F190 rule)"] = _command_problems(path)
        for label, text in controls.items():
            path.write_text(text, encoding="utf-8")
            if got := _command_problems(path):
                wrong[label] = got
    assert not wrong, f"command frontmatter misjudged: {wrong}"


def test_the_command_selector_reads_what_claude_code_loads() -> None:
    """F104 adversary — a nested command and an upper-case `.MD` are loaded, so both are checked."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for rel in ("plain.md", "UPPER.MD", "sub/nested.md", "notes.txt", "dir.md/inner.txt"):
            (root / rel).parent.mkdir(parents=True, exist_ok=True)
            (root / rel).write_text("x", encoding="utf-8")
        got = {p.relative_to(root).as_posix() for p in _command_files(root)}
    assert got == {"UPPER.MD", "plain.md", "sub/nested.md"}, got

# ── F176: the reviewer is shown the front door ───────────────────────────────

# What names the front door, as a reader sees it: the path with or without backticks, a ./ or
# ../ prefix, or as a link target; "reference page" in any case, emphasis or hyphenation.
_FRONT_DOOR = {
    "README.md": re.compile(r"(?:^|[\s`(\[<])(?:\.{1,2}/)*README\.md\b"),
    "docs/architecture/overview.md": re.compile(r"(?:^|[\s`(\[<])(?:\.{1,2}/)*docs/architecture/overview\.md\b"),
    "a reference page": re.compile(r"reference[\s*_-]+pages?\b", re.I),
}


def _rendered_lines(text: str) -> tuple[list[str], list[str]]:
    """*text*'s lines with what renders nothing blanked, and what could not be read (F174's
    `blank_unrendered`, plus indented code)."""
    lines, problems = blank_unrendered(text.replace("\r\n", "\n"))
    return blank_indented_code(lines), problems


def _front_door_problems(fix_text: str, reviewer_text: str) -> list[str]:
    """Why the reviewer's brief can still leave out the front door, or [] (F176).

    `/fix`'s one Phase 7 section, a level-2 heading as rendered (ATX or setext), running to the
    next heading of level 1 or 2, and `pr-reviewer.md` below its frontmatter, each name
    `README.md`, `docs/architecture/overview.md` and a reference page, as rendered: fenced and
    indented code and comments do not count, a reflowed line or a link target does.

    Out of what a text check can decide: whether the sentence instructs or negates, and text a
    renderer hides but a model reads raw (a link title, an image alt, an HTML element); naming
    them is what the check sees, carrying them is the instruction's to hold. Not recognised as
    code, so a name there still counts: tab-indented code, indented code inside a list item,
    and a paragraph opening with two spaces, which reads as a list continuation. None occurs
    in either file today; a false pass needs the names hidden in code on purpose.
    """
    problems: list[str] = []
    lines, unreadable = _rendered_lines(fix_text)
    problems += [f"fix.md: {u}" for u in unreadable]
    headings = rendered_headings(lines)
    phase7 = [(i, h) for i, h in enumerate(headings) if h[1] == 2 and re.match(r"Phase 7\b", heading_text(h[2]))]
    if len(phase7) != 1:
        problems.append(f"fix.md has {len(phase7)} '## Phase 7' sections as rendered; one")
        section = ""
    else:
        k, (first, _, _) = phase7[0]
        end = next((h[0] for h in headings[k + 1:] if h[1] <= 2), len(lines))
        section = "\n".join(lines[first:end])
    body_lines, unreadable = _rendered_lines(strip_frontmatter(reviewer_text))
    problems += [f"pr-reviewer.md: {u}" for u in unreadable]
    for name, text in (("fix.md Phase 7", section), ("pr-reviewer.md", "\n".join(body_lines))):
        flat = " ".join(text.split())
        missing = [token for token, pattern in _FRONT_DOOR.items() if not pattern.search(flat)]
        if missing:
            problems.append(f"{name} does not name {missing}")
    return problems


def test_the_reviewer_is_always_shown_the_front_door() -> None:
    """F176 — the lesson of 2026-10-01: a brief that lists only the changed files never puts a
    stale README claim in front of the reviewer. `/fix` Phase 7 must name the front door in the
    brief, and `pr-reviewer` must open it, whether or not the diff touched it."""
    fix = (_arch.CLAUDE_DIR / "commands" / "fix.md").read_text(encoding="utf-8")
    reviewer = (_arch.CLAUDE_DIR / "agents" / "pr-reviewer.md").read_text(encoding="utf-8")
    assert not (problems := _front_door_problems(fix, reviewer)), problems


_GOOD_BRIEF = ("The brief names `README.md`, `docs/architecture/overview.md` and the reference page of each "
               "public name the diff adds or changes, whether or not they changed.")
_REVIEWER_OK = "---\nname: pr-reviewer\n---\n\nOpen " + _GOOD_BRIEF + "\n"


def _fix(phase7: str, before: str = "", after: str = "## Phase 8 — Record\n\nx\n") -> str:
    return f"# /fix\n\n{before}## Phase 7 — Review\n\n{phase7}\n\n{after}"


@pytest.mark.parametrize("fix,reviewer", [
    (_fix(_GOOD_BRIEF), _REVIEWER_OK),
    (_fix(_GOOD_BRIEF.replace("`README.md`", "README.md")), _REVIEWER_OK),
    (_fix(_GOOD_BRIEF.replace("`README.md`", "[README](../../README.md)")
          .replace("`docs/architecture/overview.md`", "[overview](../../docs/architecture/overview.md)")), _REVIEWER_OK),
    (_fix(_GOOD_BRIEF.replace("`docs/", "`./docs/")), _REVIEWER_OK),
    (_fix(_GOOD_BRIEF.replace("reference page", "Reference-page")), _REVIEWER_OK),
    (_fix(_GOOD_BRIEF.replace("reference page", "**reference** page")), _REVIEWER_OK),
    (_fix(_GOOD_BRIEF).replace("## Phase 7 — Review", "  ## Phase 7 — Review"), _REVIEWER_OK),
    (_fix(_GOOD_BRIEF).replace("## Phase 7 — Review", "##  Phase 7"), _REVIEWER_OK),
    (_fix(_GOOD_BRIEF).replace("## Phase 7 — Review", "Phase 7 — Review\n----------------"), _REVIEWER_OK),
    (_fix("```\n## not a heading\n```\n\n" + _GOOD_BRIEF), _REVIEWER_OK),
    (_fix(_GOOD_BRIEF), "---\n---\n\nOpen " + _GOOD_BRIEF + "\n\n---\n\nmore\n"),
    (_fix(_GOOD_BRIEF), _REVIEWER_OK.replace("\n", "\r\n")),
    (_fix(_GOOD_BRIEF), "---\nname: r\n--- \n\nOpen " + _GOOD_BRIEF + "\n"),
], ids=["plain", "no backticks", "links", "dot-slash", "capital hyphen", "emphasis", "indented heading",
        "two spaces", "setext", "fenced hash line", "empty frontmatter", "crlf", "closing fence trailing space"])
def test_the_front_door_check_accepts_equivalent_wordings(fix: str, reviewer: str) -> None:
    """F176 — check-adversary's false alarms, each now accepted."""
    assert _front_door_problems(fix, reviewer) == []


@pytest.mark.parametrize("fix,reviewer,fragment", [
    (_fix("nothing", before="```\n## Phase 7\n\n" + _GOOD_BRIEF + "\n```\n\n"), _REVIEWER_OK, "fix.md Phase 7"),
    (_fix("nothing", before="<!--\n## Phase 7\n\n" + _GOOD_BRIEF + "\n-->\n\n"), _REVIEWER_OK, "fix.md Phase 7"),
    ("# f\n\n## Phase 6\n\n<!--\n\n## Phase 7 — Review\n\n" + _GOOD_BRIEF + "\n\n## Phase 8\n\n-->\n",
     _REVIEWER_OK, "0 '## Phase 7' sections"),
    ("# f\n\n## Phase 6\n\n```\n\n## Phase 7 — Review\n\n" + _GOOD_BRIEF + "\n\n## Phase 8\n\n```\n",
     _REVIEWER_OK, "0 '## Phase 7' sections"),
    (_fix("nothing", after="# Appendix\n\n" + _GOOD_BRIEF + "\n"), _REVIEWER_OK, "fix.md Phase 7"),
    (_fix("nothing", after="Appendix\n--------\n\n" + _GOOD_BRIEF + "\n"), _REVIEWER_OK, "fix.md Phase 7"),
    (_fix("nothing", after=" ## Phase 8\n\n" + _GOOD_BRIEF + "\n"), _REVIEWER_OK, "fix.md Phase 7"),
    (_fix(_GOOD_BRIEF) + "\n## Phase 7 — again\n\nnothing\n", _REVIEWER_OK, "2 '## Phase 7' sections"),
    (_fix("Para.\n\n    " + _GOOD_BRIEF), _REVIEWER_OK, "fix.md Phase 7"),
    (_fix(_GOOD_BRIEF), "---\r\nname: r\r\ndescription: " + _GOOD_BRIEF + "\r\n---\r\n\r\nbody\r\n", "pr-reviewer.md"),
    (_fix(_GOOD_BRIEF), "---\nname: r\ndescription: " + _GOOD_BRIEF + "\n--- \n\nbody\n", "pr-reviewer.md"),
    (_fix(_GOOD_BRIEF), "\ufeff---\nname: r\ndescription: " + _GOOD_BRIEF + "\n---\n\nbody\n", "pr-reviewer.md"),
    (_fix(_GOOD_BRIEF), "---\nname: r\ndescription: " + _GOOD_BRIEF + "\n---", "pr-reviewer.md"),
    (_fix("```\n" + _GOOD_BRIEF), _REVIEWER_OK, "never closed"),
    (_fix(_GOOD_BRIEF), "--- \nname: r\ndescription: " + _GOOD_BRIEF + "\n---\n\nbody\n", "pr-reviewer.md"),
    (_fix(_GOOD_BRIEF), "---\nname: r\ndescription: " + _GOOD_BRIEF + "\n", "pr-reviewer.md"),
], ids=["fake heading in fence", "fake heading in comment", "section in comment", "section in fence",
        "h1 ends section", "setext ends section", "indented h2 ends section", "two sections", "indented code",
        "crlf frontmatter", "fence trailing space", "bom frontmatter", "frontmatter at eof", "unclosed fence",
        "opening fence trailing space", "unclosed frontmatter"])
def test_the_front_door_check_reports_each_bypass(fix: str, reviewer: str, fragment: str) -> None:
    """F176 — check-adversary's bypasses, each reported by the branch it targets."""
    problems = _front_door_problems(fix, reviewer)
    assert any(fragment in p for p in problems), problems
