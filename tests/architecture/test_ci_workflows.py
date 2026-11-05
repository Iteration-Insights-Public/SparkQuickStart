"""
test_ci_workflows.py — the shape of .github/workflows/ that the release path depends on.

Contains: assertions on workflow triggers, conditions, permissions, installs and pins,
    read from the parsed YAML; plus a run of the path classifier's own bash.
Does NOT contain: any claim about how GitHub Actions evaluates them — that cannot be run
    here, and the first run on main after a change is its only live check.

Parsed, not read as text (F48). The first version of every check here was a regex over the
workflow text, and every one was bypassed at least once in review: YAML's grammar — quoted
and explicit keys, flow collections, block scalars, anchors and tags, line breaks — is
wider than any set of patterns. Shell commands inside `run:` values are still read as text,
because they are bash, not YAML; but they are taken from the parsed value, never from a
line of the file.

Two PyYAML behaviours are handled on purpose: a bare `on:` key loads as the boolean `True`
(YAML 1.1), and a duplicate key silently keeps the last value — so the loader here rejects
duplicates, which would otherwise let a second `uses:` in one step hide the first.
"""
from __future__ import annotations

import re
import tomllib
from pathlib import Path
from typing import Any

import _arch
import _yaml
import yaml

_WORKFLOWS = _arch.ROOT / ".github" / "workflows"
_TESTS = _WORKFLOWS / "pr-tests-and-automerge.yaml"
_BUILDS = _WORKFLOWS / "platform-builds.yaml"
_SYNC = _WORKFLOWS / "sync-release-branches.yaml"


# The strict loader lives in _yaml.py, shared with the agent frontmatter checks (F103).
_StrictLoader = _yaml.StrictLoader


def _load(text: str) -> tuple[dict, None] | tuple[None, str]:
    """The workflow as a mapping, or why it is not one — a file this module cannot parse is
    reported by every check, never skipped.

    PyYAML implements YAML 1.1, GitHub a 1.2 parser; where they differ this fails closed.
    Known cases: a tab before a trailing `#` comment is valid 1.2 and a 1.1 scanner error, and
    a `<<:` merge key fails the duplicate-key pre-scan — both are reported as unparseable
    rather than read.
    """
    doc, error = _yaml.load_mapping(text)
    if doc is None:
        return None, error
    # PyYAML loads a bare `on:` as True and a quoted `"on":` as "on" — two keys to it, one to
    # GitHub. With both present a check would read one and miss the other (F48 review).
    if "on" in doc and True in doc:
        return None, "both `on:` and `\"on\":` at the top level — one key to GitHub, two to PyYAML"
    return doc, None


def _on(doc: dict) -> Any:
    """The `on:` triggers. PyYAML (YAML 1.1) loads a bare `on` key as the boolean True."""
    return doc["on"] if "on" in doc else doc.get(True)


def _jobs(doc: dict) -> dict:
    jobs = doc.get("jobs")
    return jobs if isinstance(jobs, dict) else {}


def _norm(value: Any) -> str:
    """An expression with its whitespace collapsed — a folded `if: >` ends in a newline."""
    return " ".join(str(value).split()) if value is not None else ""


def _conjuncts(expr: Any) -> list[str]:
    """The `&&` conjuncts of an `if:`, with a `${{ }}` wrapper and each conjunct's outer
    parentheses removed — GitHub accepts both spellings, and the first version reported them."""
    text = _norm(expr)
    wrapped = re.fullmatch(r"\$\{\{\s*(.*?)\s*\}\}", text)
    text = wrapped.group(1) if wrapped else text
    parts = []
    for part in text.split("&&"):
        part = part.strip()
        while part.startswith("(") and part.endswith(")"):
            part = part[1:-1].strip()
        parts.append(part)
    return parts


def _is_pure_conjunction(expr: Any) -> bool:
    """No `||` and no negation: `A && B && x || true` is always true, which a conjunct check
    alone would pass (F36's lesson, found again by F48's probe)."""
    text = _norm(expr)
    return "||" not in text and not re.search(r"!(?!=)", text)


def _steps(job: Any) -> list[dict]:
    steps = job.get("steps") if isinstance(job, dict) else None
    return [s for s in steps if isinstance(s, dict)] if isinstance(steps, list) else []


def _walk(node: Any, _seen: set[int] | None = None):
    """Every mapping in a parsed document, depth first; an alias cycle is visited once."""
    seen = _seen if _seen is not None else set()
    if isinstance(node, (dict, list)):
        if id(node) in seen:
            return
        seen.add(id(node))
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _walk(value, seen)
    elif isinstance(node, list):
        for item in node:
            yield from _walk(item, seen)


def _strings(node: Any):
    """Every string scalar value in a parsed document."""
    for mapping in _walk(node):
        for value in mapping.values():
            if isinstance(value, str):
                yield value
            elif isinstance(value, list):
                yield from (v for v in value if isinstance(v, str))


_SCRIPT_KEYS = ("run", "script", "bash", "inlineScript")


def _scripts(node: Any) -> list[str]:
    """Every shell script in a parsed document: GitHub's `run:` and Azure's `script:`/`bash:`."""
    return [m[k] for m in _walk(node) for k in _SCRIPT_KEYS if isinstance(m.get(k), str)]


def _workflow_name(text: str) -> str:
    doc, error = _load(text)
    assert doc is not None, error
    name = doc.get("name")
    assert isinstance(name, str), "workflow has no top-level name"
    return name


def _workflow_files() -> dict[str, str]:
    return {p.name: p.read_text(encoding="utf-8") for p in sorted(_WORKFLOWS.glob("*.y*ml"))}


# --- F32 / F42: the release sync follows a green test run ---------------------------------


_TESTED_SHA = "${{github.event.workflow_run.head_sha}}"


def _sync_problems(sync: str, tests_name: str) -> list[str]:
    """Every way the sync workflow can build a release branch from an untested tree."""
    doc, error = _load(sync)
    if doc is None:
        return [error]
    problems = []
    on = _on(doc)
    triggers = set(on) if isinstance(on, (dict, list)) else {on}
    if "push" in triggers:
        problems.append("the sync triggers on push, so it runs whether or not the tests passed")
    run = on.get("workflow_run") if isinstance(on, dict) else None
    run = run if isinstance(run, dict) else {}
    if run.get("workflows") != [tests_name]:
        problems.append(f"workflow_run must name the test workflow exactly — {tests_name!r} — "
                        f"found {run.get('workflows')!r}")
    # `workflows:` is a glob, not a literal (F42): an unescaped `+` in the name made the
    # filter match nothing, and the sync never fired. GitHub documents the special set only
    # as "characters like * ** + ? !", so this is an allowlist rather than a denylist.
    if not re.fullmatch(r"[A-Za-z0-9 -]+", tests_name):
        problems.append(f"the test workflow's name {tests_name!r} is not letters, digits, spaces and "
                        "hyphens only, so workflow_run's glob filter may not match it literally")
    if run.get("types") != ["completed"]:
        problems.append(f"workflow_run types must be [completed], found {run.get('types')!r}")
    # The test workflow also runs on pushes to master; a green run there must not rebuild the
    # release branches from master.
    if run.get("branches") != ["main"]:
        problems.append(f"workflow_run branches must be [main], found {run.get('branches')!r}")

    jobs = _jobs(doc)
    if not jobs:
        return [*problems, "the sync has no jobs this check can read"]
    for name, job in jobs.items():
        job = job if isinstance(job, dict) else {}
        condition = _conjuncts(job.get("if"))
        if not _is_pure_conjunction(job.get("if")):
            problems.append(f"{name}: if: is not a pure && conjunction, so it can hold without its checks")
        if "github.event.workflow_run.conclusion == 'success'" not in condition:
            problems.append(f"{name}: if: does not require conclusion == 'success'; completed is not passed")
        if "github.event.workflow_run.event == 'push'" not in condition:
            problems.append(f"{name}: if: does not require event == 'push'; a green PR run would sync a PR head")
        env = job.get("env") if isinstance(job.get("env"), dict) else {}
        # Bound at job level, and never rebound — a step-level env overrides the job's.
        bindings = [m["env"]["SOURCE_SHA"] for m in _walk(job)
                    if isinstance(m.get("env"), dict) and "SOURCE_SHA" in m["env"]]
        if (re.sub(r"\s+", "", str(env.get("SOURCE_SHA"))) != _TESTED_SHA
                or any(re.sub(r"\s+", "", str(b)) != _TESTED_SHA for b in bindings)):
            problems.append(f"{name}: SOURCE_SHA is not bound to github.event.workflow_run.head_sha everywhere")
        checkouts = [s for s in _steps(job) if str(s.get("uses", "")).startswith("actions/checkout@")]
        withs = [s.get("with") if isinstance(s.get("with"), dict) else {} for s in checkouts]
        refs = [re.sub(r"\s+", "", str(w.get("ref"))) for w in withs]
        if not refs or any(ref != _TESTED_SHA for ref in refs) or any("repository" in w for w in withs):
            problems.append(f"{name}: actions/checkout does not check out this repository at workflow_run.head_sha")
        scripts = _scripts(job)
        resets = [m for script in scripts for m in re.findall(r"git checkout -B\s+(\S+)\s+(\S+)", script)]
        if not resets or any(target.strip("'\"") != "$SOURCE_SHA" for _, target in resets):
            problems.append(f"{name}: the release branch is not reset to $SOURCE_SHA: {resets}")
        # Any other command that moves HEAD could replace the tested tree after the reset.
        movers = [m.group(0) for script in scripts
                  for m in re.finditer(r"git\s+(?:reset|switch|restore|pull|merge|rebase|cherry-pick)(?=\s|$)[^\n]*"
                                       r"|git\s+checkout\s+(?!-B\s)[^\n]*", script)]
        if movers:
            problems.append(f"{name}: a git command other than the reset moves HEAD: {movers}")
    # Under workflow_run, GITHUB_SHA / github.sha is main's tip when the sync starts, not the
    # commit the tests passed. Comments are not in the parsed document, so explaining the
    # rule in one stays legal.
    if any(re.search(r"GITHUB_SHA|github\.(?:workflow_)?sha\b|github\s*\[\s*['\"](?:workflow_)?sha['\"]\s*\]", s)
           for s in _strings(doc)):
        problems.append("GITHUB_SHA / github.sha is main's current tip under workflow_run, not the tested commit")
    return problems


def test_release_sync_runs_only_after_a_green_test_run() -> None:
    """F32 — the release branches are regenerated only from a SHA the test workflow passed.

    The sync used to trigger on `push: main`, independent of the tests. At a11dc49 both
    platform-filtered builds failed on main and the sync for the same SHA still succeeded
    and force-pushed release/fabric and release/databricks — and `release.yaml` publishes a
    wheel from those branches without running a test.

    The workflow name is asserted verbatim because `workflow_run` matches on it: rename the
    test workflow and the sync stops firing, with no error anywhere. And it must contain no
    glob-special character, because the filter is a glob (F42).
    """
    tests_name = _workflow_name(_TESTS.read_text(encoding="utf-8"))
    problems = _sync_problems(_SYNC.read_text(encoding="utf-8"), tests_name)
    assert not problems, f"the release sync can publish an untested tree: {problems}"


def test_every_release_sync_check_can_fail() -> None:
    """F32 — anti-vacuity, one mutation of the real workflow per check, each reported alone."""
    sync = _SYNC.read_text(encoding="utf-8")
    tests_name = _workflow_name(_TESTS.read_text(encoding="utf-8"))
    assert _sync_problems(sync, tests_name) == []
    mutations = {
        "reset to main": ('git checkout -B "release/$PLATFORM" "$SOURCE_SHA"',
                          'git checkout -B "release/$PLATFORM" main'),
        "checkout ref main": ("ref: ${{ github.event.workflow_run.head_sha }}", "ref: main"),
        "SOURCE_SHA main": ("SOURCE_SHA: ${{ github.event.workflow_run.head_sha }}",
                            "SOURCE_SHA: main"),
        "GITHUB_SHA in commit": ('from main@$SOURCE_SHA"', 'from main@$GITHUB_SHA"'),
        "push trigger": ("on:\n  workflow_run:", "on:\n  push:\n    branches: [ main ]\n  workflow_run:"),
        "renamed workflow": ("tests and wheel build\"]", "tests and wheel-build\"]"),
        "not completed": ("types: [completed]", "types: [requested]"),
        "conclusion replaced by true": ("github.event.workflow_run.conclusion == 'success' &&",
                                        "true &&"),
        "event check dropped": (" &&\n      github.event.workflow_run.event == 'push'", ""),
        "a duplicate key": ("    runs-on: ubuntu-latest\n", "    runs-on: ubuntu-latest\n    runs-on: x\n"),
        "push under a quoted \"on\"": ("\njobs:\n", '\n"on":\n  push:\n    branches: [main]\n\njobs:\n'),
        # The probe's bypass: both checks present as conjuncts, yet always true.
        "&& '' || true after the checks": ("github.event.workflow_run.event == 'push'\n",
                                           "github.event.workflow_run.event == 'push' && '' || true\n"),
        "branches filter widened": ("    branches: [main]\n", "    branches: ['**']\n"),
        "SOURCE_SHA rebound in a step": ("      - name: Configure git identity\n",
                                         "      - name: Configure git identity\n        env:\n          SOURCE_SHA: main\n"),
        "HEAD moved after the reset": ('git checkout -B "release/$PLATFORM" "$SOURCE_SHA"',
                                       'git checkout -B "release/$PLATFORM" "$SOURCE_SHA" && git reset --hard origin/main'),
        "main's tip by index": ('from main@$SOURCE_SHA"', "from main@${{ github['sha'] }}\""),
        "checkout of a fork": ("          fetch-depth: 0\n", "          fetch-depth: 0\n          repository: someone/fork\n"),
    }
    for char in "+,()[]*?!":
        bad = f"{tests_name} {char} x"
        if len(_sync_problems(sync.replace(tests_name, bad), bad)) != 1:
            raise AssertionError(f"{char!r} in the test workflow's name is not reported alone")
    silent = []
    for label, (old, new) in mutations.items():
        assert sync.count(old) == 1, f"mutation {label!r}: its anchor is not in the workflow once"
        if len(_sync_problems(sync.replace(old, new), tests_name)) != 1:
            silent.append(label)
    assert not silent, f"these mutations were not reported by exactly one check: {silent}"
    commented = sync.replace("    env:\n", "    # never GITHUB_SHA or github.sha here\n    env:\n", 1)
    assert commented != sync and _sync_problems(commented, tests_name) == []
    wrapped = sync.replace("    if: >\n      github.event.workflow_run.conclusion == 'success' &&\n      github.event.workflow_run.event == 'push'\n",
                           "    if: ${{ (github.event.workflow_run.conclusion == 'success') && github.event.workflow_run.event == 'push' }}\n")
    assert wrapped != sync and _sync_problems(wrapped, tests_name) == [], "a ${{ }}-wrapped condition was reported"


# --- F33: every push to main is verified --------------------------------------------------


_CANONICAL_GROUPS = {
    "pr-tests-${{ github.event_name == 'pull_request' && github.ref || github.sha }}",
    "pr-tests-${{ github.event_name == 'pull_request' && github.ref || github.run_id }}",
}


def _concurrency_problems(tests: str) -> list[str]:
    """Every way the test workflow's concurrency can cancel a run on main (F33).

    GitHub cancels a *pending* run whenever a newer one joins the same group, even with
    `cancel-in-progress: false` — so turning cancellation off is not enough. A push gets a
    group of its own; only pull requests share one, by ref. Matched exactly: swapping the
    operands gives every push to main one shared group again.
    """
    doc, error = _load(tests)
    if doc is None:
        return [error]
    problems = []
    top = doc.get("concurrency")
    if top is None:
        problems.append("no top-level concurrency: pull requests no longer supersede their own older runs")
    else:
        group = top.get("group") if isinstance(top, dict) else top
        if re.sub(r"\s+", "", str(group)) not in {re.sub(r"\s+", "", g) for g in _CANONICAL_GROUPS}:
            problems.append(f"the concurrency group {group!r} is not the per-commit-on-push form "
                            f"{sorted(_CANONICAL_GROUPS)[1]!r}")
        if not (isinstance(top, dict) and top.get("cancel-in-progress") is True):
            problems.append("cancel-in-progress is not true: a newer PR commit no longer cancels "
                            "the run it supersedes")
    overriding = [name for name, job in _jobs(doc).items() if isinstance(job, dict) and "concurrency" in job]
    if overriding:
        problems.append(f"job-level concurrency overrides the workflow's: {overriding}")
    return problems


def test_runs_on_main_are_never_cancelled() -> None:
    """F33 — every commit on main is verified.

    The group was `pr-tests-${{ github.ref }}` with `cancel-in-progress: true`, one group for
    every push to main: 9 of 26 runs on main were cancelled and those commits never verified.
    """
    problems = _concurrency_problems(_TESTS.read_text(encoding="utf-8"))
    assert not problems, f"a run on main can be cancelled: {problems}"


def test_every_concurrency_check_can_fail() -> None:
    """F33 — anti-vacuity: one mutation per check, each reported alone."""
    tests = _TESTS.read_text(encoding="utf-8")
    assert _concurrency_problems(tests) == []
    m = re.search(r"^concurrency:\n  group: (.+)\n  cancel-in-progress: true\n", tests, re.M)
    assert m is not None, "the concurrency block is not in the shape these mutations anchor on"
    block, expr = m.group(0), m.group(1)
    mutations = {
        "operands swapped": tests.replace(expr, expr.replace("github.ref || github.sha",
                                                             "github.sha || github.ref")),
        "one group per ref, as before F33": tests.replace(expr, "pr-tests-${{ github.ref }}"),
        "cancel-in-progress off": tests.replace(block, block.replace("true", "false")),
        "block deleted": tests.replace(block, ""),
        "one-line shorthand": tests.replace(block, f"concurrency: {expr}\n"),
        "job-level group added": tests.replace(
            "  test:\n", "  test:\n    concurrency: pr-tests-${{ github.ref }}\n", 1),
    }
    wrong = {label: _concurrency_problems(text) for label, text in mutations.items()
             if text == tests or len(_concurrency_problems(text)) != 1}
    assert not wrong, f"these mutations were not reported by exactly one check: {wrong}"


# --- F35: gates resolve their inputs from the lock ----------------------------------------


_PYPROJECT = _arch.ROOT / "pyproject.toml"
_LOCK = _arch.ROOT / "requirements.txt"
_GATE_TOOLS = ("mypy", "ruff")
# Installers that are tooling, not gate inputs: they decide nothing a gate reports.
_TOOLING = {"pip", "build"}
_INSTALLER = re.compile(r"(?:\bpython3?\s+-m\s+|\buv\s+)?\bpip3?\s+install\b")


def _ci_files() -> dict[str, str]:
    """Every file that installs something a CI gate runs on."""
    files = [*sorted(_WORKFLOWS.glob("*.y*ml")), _arch.ROOT / "Dockerfile"]
    return {_arch.rel(p): p.read_text(encoding="utf-8") for p in files if p.is_file()}


def _install_commands(text: str) -> list[str]:
    """Every installer invocation in shell text, continuations joined and comments stripped.

    Whole-line and trailing comments go first, so a `-c requirements.txt` written in a
    comment cannot satisfy the check. Then backslash continuations are joined, and each
    `&&` / `;` segment holding an installer is one command.
    """
    lines = []
    for line in text.splitlines():
        if line.lstrip().startswith("#"):
            continue
        lines.append(re.sub(r"\s+#.*$", "", line))
    joined = re.sub(r"\\\s*\n\s*", " ", "\n".join(lines))
    commands = []
    for segment in re.split(r"&&|;|\n", joined):
        m = _INSTALLER.search(segment)
        if m:
            commands.append(segment[m.end():].strip().strip("'\""))
    return commands


def _shell_of(name: str, text: str) -> tuple[list[str], str | None]:
    """The shell text of a CI file: a Dockerfile is shell as written; a YAML file's shell is
    every parsed string value, never its raw lines."""
    if not name.endswith((".yml", ".yaml")):
        return [text], None
    doc, error = _load(text)
    # Every string value, not a list of script keys: Azure's `pwsh:`/`powershell:` and any
    # future key would otherwise be skipped (F48 review).
    return ([], error) if doc is None else (list(_strings(doc)), None)


def _classify_install(args: str, locked: dict[str, str], extras_free: bool) -> str | None:
    """Why one installer invocation resolves outside the lock, or None when it cannot."""
    tokens = [tok.strip("'\"") for tok in args.split()]
    if "-r" in tokens and tokens[tokens.index("-r") + 1:][:1] == ["requirements.txt"]:
        return None
    constrained = "-c" in tokens and tokens[tokens.index("-c") + 1:][:1] == ["requirements.txt"]
    targets = [tok for i, tok in enumerate(tokens)
               if not tok.startswith("-") and tokens[i - 1:i] != ["-c"]]
    if not targets:
        return f"`{args}` installs nothing this check can identify"
    for target in targets:
        project = re.fullmatch(r"(\.|/app|\S*/)(\[[\w,]+\])?", target)
        if project:
            if project.group(2) and not constrained:
                return f"`{args}` installs extras {project.group(2)} without -c requirements.txt"
            if not project.group(2) and not (extras_free or constrained):
                return f"`{args}` installs the project's dependencies without -c requirements.txt"
            continue
        name = re.match(r"[A-Za-z0-9_.-]+", target)
        base = name.group(0) if name else target
        if base in _GATE_TOOLS:
            if target != f"{base}=={locked.get(base)}":
                return f"`{args}` installs {target!r}; a gate tool must be {base}=={locked.get(base)}"
            continue
        if base in _TOOLING and target == base:
            continue
        return f"`{args}` installs {target!r}, which is neither the lock, the project, a pinned gate tool nor tooling"
    return None


def _gate_tool_problems(dev: object, locked: dict[str, str]) -> list[str]:
    """Each gate tool in the `[dev]` extra is declared once, as `tool==<the lock's version>`.

    Entries are read as PEP 508 requirements, so a name is compared after PEP 503
    normalisation (`MyPy`, `mypy == 2.3.1`) and a different package sharing a prefix
    (`mypy-extensions`) is not mistaken for the tool. A marker, an extra or a URL on a gate
    tool is refused: each makes what is installed depend on more than the pin.
    """
    from packaging.requirements import InvalidRequirement, Requirement
    from packaging.utils import canonicalize_name

    if not isinstance(dev, list) or not all(isinstance(entry, str) for entry in dev):
        return [f"[project.optional-dependencies].dev is {dev!r}; it must be a list of strings"]
    problems, requirements = [], []
    for entry in dev:
        try:
            requirements.append(Requirement(entry))
        except InvalidRequirement as exc:
            problems.append(f"[dev] entry {entry!r} is not a PEP 508 requirement: {exc}")
    for tool in _GATE_TOOLS:
        found = [r for r in requirements if canonicalize_name(r.name) == tool]
        if not found:
            problems.append(f"[dev] does not declare {tool}; a gate tool must be pinned there exactly")
            continue
        if len(found) > 1:
            problems.append(f"[dev] declares {tool} {len(found)} times: {[str(r) for r in found]}")
            continue
        req = found[0]
        specs = list(req.specifier)
        if req.marker or req.extras or req.url or len(specs) != 1 or specs[0].operator != "==" \
                or "*" in specs[0].version:
            problems.append(f"[dev] declares {str(req)!r}; a gate tool must be pinned exactly, "
                            "with no marker, extra, URL or second specifier")
        elif locked.get(tool) != specs[0].version:
            problems.append(f"[dev] pins {tool}=={specs[0].version} but the lock has {locked.get(tool)}")
    return problems


def _gate_input_problems(pyproject: str, lock: str, ci: dict[str, str]) -> list[str]:
    """Every way a CI gate can resolve a gate tool or a locked dependency off the lock (F35).

    A gate's verdict must not change on unchanged code because a release landed between two
    runs. Every installer in every CI file is classified against an allowlist, and a form it
    does not recognise is reported. Carve-out: the lock deliberately excludes `[runtime]`
    (pandas, pyarrow), so those still resolve from ranges under `-c`.

    pyproject is parsed, not matched (F101): the regex reader read a `dev` list in another
    table or inside a multi-line string, and an empty `dependencies` anywhere, as the
    project's. `[project].dependencies` counts as empty only when it is absent or `[]` and
    not listed in `dynamic`.
    """
    try:
        project = tomllib.loads(pyproject).get("project")
    except tomllib.TOMLDecodeError as exc:
        return [f"pyproject.toml is not valid TOML: {exc}"]
    project = project if isinstance(project, dict) else {}
    locked = dict(re.findall(r"^([A-Za-z0-9_.-]+)==(\S+)$", lock, re.M))
    extras = project.get("optional-dependencies")
    problems = _gate_tool_problems(extras.get("dev") if isinstance(extras, dict) else None, locked)
    extras_free = project.get("dependencies", []) == [] and "dependencies" not in project.get("dynamic", [])
    for name, text in ci.items():
        scripts, error = _shell_of(name, text)
        if error:
            problems.append(f"{name}: {error}")
        for script in scripts:
            for args in _install_commands(script):
                why = _classify_install(args, locked, extras_free)
                if why is not None:
                    problems.append(f"{name}: {why}")
    return problems


def test_gates_resolve_their_inputs_from_the_lock() -> None:
    """F35 — a gate's verdict changes only when the code or the lock does."""
    ci = _ci_files()
    assert len(ci) >= 5, f"expected the workflows and Dockerfile: {sorted(ci)}"
    found = sum(len(_install_commands(s)) for n, t in ci.items() for s in _shell_of(n, t)[0])
    assert found >= 8, f"the installer scan found {found} installs; it is broken, not clean"
    problems = _gate_input_problems(_PYPROJECT.read_text(encoding="utf-8"),
                                    _LOCK.read_text(encoding="utf-8"), ci)
    assert not problems, f"a gate resolves its inputs outside the lock: {problems}"


def test_every_gate_input_check_can_fail() -> None:
    """F35 — anti-vacuity, for the selector as well as the judgement."""
    pyproject = _PYPROJECT.read_text(encoding="utf-8")
    lock = _LOCK.read_text(encoding="utf-8")
    ci = _ci_files()
    assert _gate_input_problems(pyproject, lock, ci) == []
    wf = _arch.rel(_TESTS)
    mypy_pin = re.search(r'"mypy==[^"]+"', pyproject)
    assert mypy_pin is not None

    def with_step(command: str) -> dict[str, str]:
        body = "\n".join(f"          {line}" for line in command.split("\n"))
        text = ci[wf].replace("      - name: Run ruff\n",
                              f"      - name: probe\n        run: |\n{body}\n\n      - name: Run ruff\n", 1)
        assert text != ci[wf], "the probe step's anchor is gone"
        return ci | {wf: text}

    mutations = {
        "mypy back to a range": (pyproject.replace(mypy_pin.group(0), '"mypy>=2.3,<3"'), lock, ci),
        "mypy pin off the lock": (pyproject.replace(mypy_pin.group(0), '"mypy==2.3.0"'), lock, ci),
        "unpinned ruff": (pyproject, lock, with_step("pip install ruff")),
        "ruff off the lock": (pyproject, lock, with_step("pip install 'ruff==9.9.9'")),
        "pip3 extras, unconstrained": (pyproject, lock, with_step("pip3 install -e '.[dev]'")),
        "python -m pip extras": (pyproject, lock, with_step("python -m pip install '.[dev,spark]'")),
        "continuation hides the extras": (pyproject, lock, with_step("pip install --quiet \\\n  -e '.[dev]'")),
        "-c only in a comment": (pyproject, lock, with_step("pip install -e '.[dev]'  # -c requirements.txt")),
        "an unknown package": (pyproject, lock, with_step("pip install requests")),
        "a workflow that is not YAML": (pyproject, lock, ci | {wf: ci[wf] + "\n  broken: [\n"}),
    }
    # F101: the reader, not only the judgement. Each decoy below satisfies a regex over the
    # text while the real [project] keys say otherwise; only a parser sees which is which.
    dev_block = re.search(r"^dev = \[.*?^\]\n", pyproject, re.M | re.S)
    assert dev_block is not None, "the [dev] extra's anchor is gone"
    unpinned = pyproject.replace(mypy_pin.group(0), '"mypy>=2.3,<3"')
    mutations |= {
        "a pinned dev list in another table": (
            "[tool.decoy]\n" + dev_block.group(0) + "\n" + unpinned, lock, ci),
        "a pinned dev list inside a multi-line string": (
            'decoy = """\n' + dev_block.group(0) + '"""\n\n' + unpinned, lock, ci),
        "an empty dependencies list in another table": (
            "[tool.decoy]\ndependencies = []\n\n"
            + pyproject.replace("dependencies = []", 'dependencies = ["requests"]', 1),
            lock, {"Dockerfile": "RUN pip install .\n"}),
    }
    # F101's adversary: forms a parsed reader must judge by what pip would install.
    real_dev = dev_block.group(0)
    # One bare install alone: the real CI files hold three, and a dependencies mutation would
    # break each of them, so the count would be three, not one.
    install_project = {"Dockerfile": "RUN pip install .\n"}

    def with_dev(entries: str) -> str:
        return pyproject.replace(real_dev, f"dev = [{entries}]\n", 1)

    with_deps = pyproject.replace("dependencies = []", 'dependencies = ["requests>=2"]', 1)
    mutations |= {
        "a gate tool declared twice": (with_dev('"ruff==0.16.4", "mypy==2.3.1", "mypy>=3"'), lock, ci),
        "a gate tool missing from dev": (with_dev('"ruff==0.16.4"'), lock, ci),
        "dev's bracket indented, the tool in the next extra": (
            pyproject.replace(real_dev, 'dev = [\n  "ruff==0.16.4",\n  ]\nlint = [\n  "mypy==2.3.1",\n]\n', 1),
            lock, ci),
        "a marker on a gate tool": (with_dev('"ruff==0.16.4", "mypy==2.3.1; python_version < \'3\'"'), lock, ci),
        "an extra on a gate tool": (with_dev('"ruff==0.16.4", "mypy[reports]==2.3.1"'), lock, ci),
        "a second specifier on a gate tool": (with_dev('"ruff==0.16.4", "mypy>=2,==2.3.1"'), lock, ci),
        "a wildcard pin": (with_dev('"ruff==0.16.4", "mypy==2.3.*"'), lock, ci),
        "pyproject that is not valid TOML": (pyproject + "\n[project]\nname = 'again'\n", lock, ci),
        "an extra named dependencies, real dependencies set": (
            with_deps.replace("[project.optional-dependencies]\n", "[project.optional-dependencies]\ndependencies = []\n", 1),
            lock, install_project),
        "dev that is a string, not a list": (with_dev('"x"').replace('dev = ["x"]', 'dev = "mypy==2.3.1"', 1), lock, ci),
        "an entry that is not a requirement": (with_dev('"ruff==0.16.4", "mypy==2.3.1", "bad=1"'), lock, ci),
        "dependencies declared dynamic": (
            pyproject.replace("dependencies = []", 'dependencies = []\ndynamic = ["dependencies"]', 1),
            lock, install_project),
    }
    wrong = {label: _gate_input_problems(*args) for label, args in mutations.items()
             if len(_gate_input_problems(*args)) != 1}
    assert not wrong, f"these mutations were not reported by exactly one check: {wrong}"
    controls = {
        "optional-dependencies as an inline table in [project]": pyproject[
            :pyproject.index("[project.optional-dependencies]\n")].replace(
            "dependencies = []", 'dependencies = []\noptional-dependencies = { dev = ["ruff==0.16.4", "mypy==2.3.1"] }', 1)
            + pyproject[pyproject.index("[project.optional-dependencies]\n"):].replace(
                "[project.optional-dependencies]\n" + real_dev, "[tool.moved]\n", 1),
        "dev as a dotted key in [project]": pyproject.replace(real_dev, "", 1).replace(
            "dependencies = []", 'dependencies = []\noptional-dependencies.dev = ["ruff==0.16.4", "mypy==2.3.1"]', 1
        ).replace("[project.optional-dependencies]\n", "[tool.moved]\n", 1),
        "a one-line dev with literal strings": with_dev("'ruff==0.16.4', 'mypy==2.3.1'"),
        "spaces and case in a pin": with_dev('"Ruff == 0.16.4", "MyPy==2.3.1"'),
        "a package sharing the tool's prefix": with_dev('"mypy-extensions==1.1.0", "ruff==0.16.4", "mypy==2.3.1"'),
    }
    noisy = {label: got for label, text in controls.items() if (got := _gate_input_problems(text, lock, ci))}
    assert not noisy, f"these valid spellings were reported: {noisy}"
    no_deps = pyproject.replace("dependencies = []\n", "", 1)
    assert _gate_input_problems(no_deps, lock, ci) == [], \
        "an absent [project].dependencies is empty under PEP 621, so a bare install needs no lock"


# --- F38: the jobs that install the lock cache pip ----------------------------------------


_HEAVY_INSTALL = re.compile(r"(?:^|\s)['\"]?(?:\.|/app|\S*/)\[[^\]]+\]|-r\s+requirements\.txt")


def _pip_cache_problems(workflows: dict[str, str]) -> list[str]:
    """Every job that installs the lock or the project's extras without a pip cache keyed on
    the lock (F38). Every job in every workflow is walked; a heavy job with no setup-python
    step is reported, not skipped. A folded `>` path list is one pattern to setup-python and
    matches nothing, so the paths must be separate lines.
    """
    problems = []
    for file, text in workflows.items():
        doc, error = _load(text)
        if doc is None:
            problems.append(f"{file}: {error}")
            continue
        jobs = _jobs(doc)
        if not jobs:
            problems.append(f"{file}: no jobs found under jobs:; this check cannot vouch for any")
            continue
        for name, job in jobs.items():
            if not any(_HEAVY_INSTALL.search(args) for s in _scripts(job) for args in _install_commands(s)):
                continue
            setup = next((s for s in _steps(job) if str(s.get("uses", "")).startswith("actions/setup-python@")), None)
            if setup is None:
                problems.append(f"{file}:{name}: installs the lock or extras with no setup-python step")
                continue
            with_ = setup.get("with") if isinstance(setup.get("with"), dict) else {}
            if with_.get("cache") != "pip":
                problems.append(f"{file}:{name}: setup-python has no `cache: pip`")
            paths = [p.strip() for p in str(with_.get("cache-dependency-path", "")).splitlines()]
            if "requirements.txt" not in paths:
                problems.append(f"{file}:{name}: the pip cache is not keyed on requirements.txt")
    return problems


def test_extras_installs_are_cached_on_the_lock() -> None:
    """F38 — pyspark (~300 MB) is not downloaded again by every job on every run."""
    problems = _pip_cache_problems(_workflow_files())
    assert not problems, f"an extras install is not cached: {problems}"


def test_every_pip_cache_check_can_fail() -> None:
    """F38 — anti-vacuity, selector mutations first; positive controls for the valid forms."""
    files = _workflow_files()
    assert _pip_cache_problems(files) == []
    wf, rel = _TESTS.name, "release.yaml"
    tests = files[wf]
    assert "\n  guard-trunk:\n" in tests

    def job(install: str) -> str:
        return f"\n  extra:\n    runs-on: ubuntu-latest\n    steps:\n      - run: {install}\n"

    typecheck_cache = re.search(r"(  typecheck:\n(?:.*\n)*?)          cache: pip\n", tests)
    assert typecheck_cache is not None
    mutations = {
        "extras as ./[dev], new job": files | {wf: tests.replace("\n  guard-trunk:\n", job("pip install -e './[dev]' -c requirements.txt") + "\n  guard-trunk:\n", 1)},
        "extras with a space, quoted": files | {wf: tests.replace("\n  guard-trunk:\n", job("pip install -e \".[dev, spark]\" -c requirements.txt") + "\n  guard-trunk:\n", 1)},
        "the lock via -r, new job": files | {wf: tests.replace("\n  guard-trunk:\n", job("pip install -r requirements.txt") + "\n  guard-trunk:\n", 1)},
        "extras job in another workflow": files | {rel: files[rel] + job("pip install -e '.[dev]' -c requirements.txt")},
        "cache removed from typecheck": files | {wf: tests.replace(typecheck_cache.group(0), typecheck_cache.group(1), 1)},
        "cache keyed off the lock": files | {wf: tests.replace("            requirements.txt\n", "            setup.cfg\n", 1)},
        "folded > scalar": files | {wf: tests.replace("cache-dependency-path: |\n", "cache-dependency-path: >\n", 1)},
        "no jobs at all": files | {rel: re.sub(r"\njobs:\n(?:.*\n?)*", "\njobs:\n", files[rel])},
    }
    wrong = {label: _pip_cache_problems(f) for label, f in mutations.items()
             if f == files or len(_pip_cache_problems(f)) != 1}
    assert not wrong, f"these mutations were not reported by exactly one check: {wrong}"
    controls = {
        "cache: 'pip' quoted": tests.replace("          cache: pip\n", "          cache: 'pip'\n", 1),
        "id between uses and with": re.sub(
            r"(        uses: actions/setup-python@[^\n]*\n)(        with:\n          python-version: '3.11'\n          # pyspark)",
            r"\1        id: py\n\2", tests, count=1),
    }
    noisy = {label: _pip_cache_problems(files | {wf: text}) for label, text in controls.items()
             if text == tests or _pip_cache_problems(files | {wf: text})}
    assert not noisy, f"these valid forms were reported: {noisy}"


# --- F39: Delta from the image, offline, and a skip is red --------------------------------


_DOCKERFILE = _arch.ROOT / "Dockerfile"
_DOCKER_FLAGS = {"--rm", "-i", "-t", "-it", "-d", "--init", "--privileged"}


def _docker_run_options(line: str) -> str:
    """The options of a `docker run`, i.e. only what precedes the image reference.

    Anything after the image is the container's command, which docker never applies — so
    `--network none` written there must not count (F39 review).
    """
    tokens = line[line.index("docker run") + len("docker run"):].split()
    taken, i = [], 0
    while i < len(tokens) and tokens[i].startswith("-"):
        step = 1 if tokens[i] in _DOCKER_FLAGS or "=" in tokens[i] else 2
        taken.extend(tokens[i:i + step])
        i += step
    return " ".join(taken)


def _delta_offline_problems(dockerfile: str, tests: str) -> list[str]:
    """Every way CI's test job can lose Delta — or any other capability — silently (F39).

    The image resolves the Delta JARs at build time, in a layer keyed on the lock rather than
    the source; the tests run with no network, so a capability that still needs one shows;
    and a skip fails the run, so losing it is red.
    """
    problems = []
    lines = [ln for ln in dockerfile.splitlines() if not ln.lstrip().startswith("#")]
    lock_at = next((i for i, ln in enumerate(lines) if re.search(r"-r\s+requirements\.txt", ln)), None)
    source_at = next((i for i, ln in enumerate(lines) if re.match(r"^\s*(ADD|COPY)\s+\./?\s", ln)), None)
    warm_at = next((i for i, ln in enumerate(lines)
                    if re.match(r"^\s*RUN\b", ln) and "configure_spark_with_delta_pip(" in ln
                    and ".getOrCreate()" in ln and "||" not in ln), None)
    if warm_at is None:
        problems.append("the Dockerfile has no RUN step that resolves the Delta JARs via "
                        "configure_spark_with_delta_pip")
    elif lock_at is None or source_at is None or not lock_at < warm_at < source_at:
        problems.append(f"the Delta warm-up (line {warm_at}) must sit after the lock install "
                        f"({lock_at}) and before the source is added ({source_at})")
    doc, error = _load(tests)
    if doc is None:
        return [*problems, error]
    shell = "\n".join(_scripts(_jobs(doc).get("test")))
    runs = [ln for ln in re.sub(r"\\\s*\n\s*", " ", shell).splitlines() if re.search(r"\bdocker run\b", ln)]
    if len(runs) != 1:
        problems.append(f"expected exactly one `docker run` in the test job, found {len(runs)}")
    else:
        options = _docker_run_options(runs[0])
        if not re.search(r"(?:^|\s)--network(?:=|\s+)none(?:\s|$)", options):
            problems.append("the test job's `docker run` has network access, so a capability that "
                            "needs Maven at test time goes unnoticed")
        if not re.search(r"(?:^|\s)-e\s+SQS_FAIL_ON_SKIP=1(?:\s|$)", options):
            problems.append("the test job does not set SQS_FAIL_ON_SKIP=1, so a lost capability "
                            "skips its tests on a green run")
    return problems


def test_ci_runs_delta_offline_and_fails_on_skips() -> None:
    """F39 — Delta comes from the image, and losing it is a red build."""
    problems = _delta_offline_problems(_DOCKERFILE.read_text(encoding="utf-8"),
                                       _TESTS.read_text(encoding="utf-8"))
    assert not problems, f"CI can lose Delta silently: {problems}"


def test_every_delta_offline_check_can_fail() -> None:
    """F39 — anti-vacuity, selector mutations first."""
    docker = _DOCKERFILE.read_text(encoding="utf-8")
    tests = _TESTS.read_text(encoding="utf-8")
    assert _delta_offline_problems(docker, tests) == []
    warm = next(ln for ln in docker.splitlines() if "configure_spark_with_delta_pip" in ln)
    run = re.search(r"docker run [^\n]*", tests)
    assert run is not None
    mutations = {
        "warm-up removed": (docker.replace(warm + "\n", ""), tests),
        "warm-up swallows failure": (docker.replace(warm, warm + " || true"), tests),
        "warm-up before the lock": (docker.replace(warm + "\n", "").replace(
            "COPY requirements.txt", warm + "\nCOPY requirements.txt", 1), tests),
        "options after the image": (docker, tests.replace(
            "--network none -e SQS_FAIL_ON_SKIP=1 local/pyspark-de-utils-test:latest",
            "local/pyspark-de-utils-test:latest --network none -e SQS_FAIL_ON_SKIP=1", 1)),
        "warm-up after the source": (docker.replace(warm + "\n", "") + "\n" + warm + "\n", tests),
        "a second docker run": (docker, tests.replace(run.group(0), run.group(0) + "\n          docker run --rm x", 1)),
        "network back on": (docker, tests.replace("--network none ", "", 1)),
        "skips allowed": (docker, tests.replace("-e SQS_FAIL_ON_SKIP=1 ", "", 1)),
    }
    # Options after the image lose both, so that one is expected twice.
    expected = dict.fromkeys(mutations, 1) | {"options after the image": 2}
    wrong = {label: _delta_offline_problems(*args) for label, args in mutations.items()
             if args == (docker, tests) or len(_delta_offline_problems(*args)) != expected[label]}
    assert not wrong, f"these mutations were not reported as expected {expected}: {wrong}"
    control = tests.replace("--network none ", "--network=none ", 1)
    assert control != tests and _delta_offline_problems(docker, control) == [], \
        "--network=none is a valid spelling and was reported"


# The container's command: three pytest runs, `&&`-chained so any failure fails the step. Unit
# is the coverage gate (addopts' --cov and fail_under; coverage answers to the unit suite alone,
# test-classification.md), integration and architecture take --no-cov. Unit and integration run
# under xdist with `--dist loadfile`; architecture is serial because its ExampleCoverage warning
# class, defined in a test module, cannot be unserialized by xdist.
_CONTAINER_RUNS = (
    ("/app/tests/unit", {"-n auto", "--dist loadfile", "-rs"}),
    ("/app/tests/integration", {"-n auto", "--dist loadfile", "--no-cov", "-rs"}),
    ("/app/tests/architecture", {"--no-cov", "-rs"}),
)


def _container_pytest_problems(tests: str) -> list[str]:
    """Every way the test job's container can run the wrong suites, or hide a failure."""
    doc, error = _load(tests)
    if doc is None:
        return [error]
    shell = "\n".join(_scripts(_jobs(doc).get("test")))
    runs = [ln for ln in re.sub(r"\\\s*\n\s*", " ", shell).splitlines() if re.search(r"\bdocker run\b", ln)]
    if len(runs) != 1:
        return [f"expected exactly one `docker run` in the test job, found {len(runs)}"]
    quoted = re.search(r"-lc '([^']*)'\s*$", runs[0])
    if quoted is None:
        return ["the test job's `docker run` has no `-lc '<command>'` to inspect"]
    command = quoted.group(1)
    if re.search(r";|\|\||(?<!&)&(?!&)|\bset \+e\b", command):
        return [("the container command joins its runs with something other than `&&`, so a failing "
                 f"run can be followed by a passing one: {command!r}")]
    parts = [p.strip() for p in command.split("&&")]
    if len(parts) != len(_CONTAINER_RUNS):
        return [f"expected {len(_CONTAINER_RUNS)} pytest runs in the container, found {len(parts)}"]
    problems = []
    for part, (path, want) in zip(parts, _CONTAINER_RUNS, strict=True):
        head = "python3 -m pytest "
        if not part.startswith(head):
            problems.append(f"run for {path} does not start with `{head}`: {part!r}")
            continue
        rest = part[len(head):]
        if not rest.startswith(path + " ") and rest != path:
            problems.append(f"run for {path} does not target it first: {part!r}")
            continue
        flags = rest[len(path):]
        option = r"-n \S+|--dist \S+|--?[A-Za-z][\w-]*(?:=\S+)?"
        got = set(re.findall(option, flags))
        # Anything the options do not account for is a stray argument, such as a second
        # path: `/app/tests/unit /app/tests/integration` would put integration under the gate.
        leftover = re.sub(option, "", flags).split()
        if got != want or leftover:
            problems.append(f"run for {path} has options {sorted(got)} and extra arguments {leftover}, "
                            f"expected {sorted(want)} and none")
    return problems


def test_the_container_runs_the_three_suites_and_fails_on_any() -> None:
    """The unit suite is the coverage gate, integration and architecture are not, none is dropped."""
    problems = _container_pytest_problems(_TESTS.read_text(encoding="utf-8"))
    assert not problems, f"the test job's container command is wrong: {problems}"


def test_every_container_pytest_check_can_fail() -> None:
    """Anti-vacuity: each mutation is reported by exactly one check."""
    tests = _TESTS.read_text(encoding="utf-8")
    assert _container_pytest_problems(tests) == []
    unit = "python3 -m pytest /app/tests/unit -n auto --dist loadfile -rs"
    integration = "python3 -m pytest /app/tests/integration -n auto --dist loadfile --no-cov -rs"
    architecture = "python3 -m pytest /app/tests/architecture --no-cov -rs"
    assert all(x in tests for x in (unit, integration, architecture))
    mutations = {
        "unit run loses the coverage gate": tests.replace(unit, unit + " --no-cov", 1),
        "unit run drops the gate by plugin": tests.replace(unit, unit + " -p no:cov", 1),
        "integration run measures coverage": tests.replace(integration, integration.replace(" --no-cov", ""), 1),
        "unit run serial": tests.replace(unit, unit.replace(" -n auto", ""), 1),
        "unit run distributes by test": tests.replace(unit, unit.replace("loadfile", "load"), 1),
        "architecture run under xdist": tests.replace(architecture, architecture + " -n auto", 1),
        "architecture run dropped": tests.replace(" && " + architecture, "", 1),
        "integration run dropped": tests.replace(" && " + integration, "", 1),
        "second suite in the unit run": tests.replace(unit, unit.replace("/app/tests/unit", "/app/tests/unit /app/tests/integration"), 1),
        "whole tree again": tests.replace(unit, "python3 -m pytest /app/tests -rs", 1),
        "skips not listed": tests.replace(integration, integration.replace(" -rs", ""), 1),
        "failure swallowed by a semicolon": tests.replace(unit + " && ", unit + " ; ", 1),
        "failure swallowed by ||": tests.replace(unit + " && ", unit + " || true && ", 1),
        "a run in the background": tests.replace(unit + " && ", unit + " & ", 1),
    }
    wrong = {label: _container_pytest_problems(text) for label, text in mutations.items()
             if text == tests or len(_container_pytest_problems(text)) != 1}
    assert not wrong, f"these mutations were not reported by exactly one check: {wrong}"


# --- F40: docs-only pull requests skip the heavy suites -----------------------------------


_HEAVY_JOBS = ("test", "typecheck")
_GATE_ON_CHANGES = "needs.changes.outputs.code == 'true'"


def _needs(job: dict) -> list[str]:
    needs = job.get("needs")
    return [needs] if isinstance(needs, str) else list(needs) if isinstance(needs, list) else []


def _classify_script(tests: str) -> str | None:
    """The `run:` of the `changes` job's `classify` step, exactly as bash will see it."""
    doc, _ = _load(tests)
    changes = _jobs(doc).get("changes") if doc else None
    step = next((s for s in _steps(changes) if s.get("id") == "classify"), None)
    return step.get("run") if step and isinstance(step.get("run"), str) else None


def _path_filter_problems(tests: str) -> list[str]:
    """Every way a docs-only PR can still pay for the heavy jobs, or a code change skip them (F40).

    Structure only; what the classifier decides is proven by running it
    (`test_the_path_classifier_decides_as_documented`).
    """
    doc, error = _load(tests)
    if doc is None:
        return [error]
    jobs = _jobs(doc)
    problems = []
    outputs = (jobs.get("changes") or {}).get("outputs") if isinstance(jobs.get("changes"), dict) else None
    if not isinstance(outputs, dict) or re.sub(r"\s+", "", str(outputs.get("code"))) != "${{steps.classify.outputs.code}}":
        problems.append("no `changes` job exporting steps.classify.outputs.code as `code`")
    for name in _HEAVY_JOBS:
        job = jobs.get(name)
        if not isinstance(job, dict):
            problems.append(f"heavy job {name!r} not found")
            continue
        if "changes" not in _needs(job) or _norm(job.get("if")) != _GATE_ON_CHANGES:
            problems.append(f"{name} is not gated exactly on `{_GATE_ON_CHANGES}` with needs: changes")
    arch = jobs.get("architecture")
    runs = [_norm(s.get("run")) for s in _steps(arch)]
    if "pytest tests/architecture --no-cov" not in runs:
        problems.append("no `architecture` job running exactly `pytest tests/architecture --no-cov`")
    elif arch.get("if") or arch.get("needs"):
        problems.append("the architecture job is gated; a docs-only PR must still run it — it reads .claude/**")
    return problems


def test_docs_only_prs_skip_the_heavy_jobs() -> None:
    """F40 — the two heavy suites (test, typecheck) run when code changed; the architecture suite always runs."""
    problems = _path_filter_problems(_TESTS.read_text(encoding="utf-8"))
    assert not problems, f"the path filter is not wired as intended: {problems}"


def test_every_path_filter_check_can_fail() -> None:
    """F40 — anti-vacuity, selector mutations first."""
    tests = _TESTS.read_text(encoding="utf-8")
    assert _path_filter_problems(tests) == []
    gate = f"    if: {_GATE_ON_CHANGES}\n"
    assert tests.count(gate) == len(_HEAVY_JOBS)
    mutations = {
        "a heavy job ungated": tests.replace(gate, "", 1),
        "a heavy job on another output": tests.replace(gate, gate.replace("code", "docs"), 1),
        "changes job output removed": tests.replace("code: ${{ steps.classify.outputs.code }}", "x: y", 1),
        "needs: changes removed from a heavy job": re.sub(
            r"(\n  typecheck:\n    name: [^\n]*\n)    needs: changes\n", r"\1", tests, count=1),
        "a heavy job renamed": tests.replace("\n  typecheck:\n", "\n  type-check:\n", 1),
        "architecture gated by if only": tests.replace("\n  architecture:\n", f"\n  architecture:\n{gate}", 1),
        "architecture gated by needs only": tests.replace("\n  architecture:\n", "\n  architecture:\n    needs: changes\n", 1),
        "architecture suite narrowed": tests.replace(
            "run: pytest tests/architecture --no-cov", "run: pytest tests/architecture/test_layering.py --no-cov", 1),
        "architecture job gone": tests.replace("pytest tests/architecture --no-cov", "pytest tests/unit", 1),
    }
    wrong = {label: _path_filter_problems(text) for label, text in mutations.items()
             if text == tests or len(_path_filter_problems(text)) != 1}
    assert not wrong, f"these mutations were not reported by exactly one check: {wrong}"


def _git_env() -> dict[str, str]:
    import os

    return {**os.environ, "GIT_CONFIG_GLOBAL": os.devnull, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
            "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"}


def _run_classifier(script: str, event: str, changed: list[str], base_files: tuple[str, ...] = (),
                    deleted: tuple[str, ...] = (), renamed: tuple[tuple[str, str], ...] = (),
                    base_override: str | None = None, release: str = "synced",
                    unverified_code: bool = False) -> str:
    """Run the classify step against a throwaway repo.

    The base commit holds `base_files`; the PR commit writes `changed`, deletes `deleted`
    and `git mv`s each `renamed` pair — so a rename is a real rename, which git's default
    rename detection would otherwise collapse to its destination path (F40 review).

    For a push (F102), `release` models the release branches' last sync subjects:
    "synced" — both name the base commit as verified; "none" — no release refs;
    "mismatch" — they name different commits. `unverified_code` adds a code commit
    between the verified base and the change, the case where main went red and a docs
    merge followed.
    """
    import os
    import subprocess
    import tempfile

    with tempfile.TemporaryDirectory() as repo:
        def git(*args: str) -> str:
            return subprocess.run(["git", "-C", repo, *args], check=True, capture_output=True,
                                  text=True, env=_git_env()).stdout
        git("init", "-q")
        (Path(repo) / "seed").write_text("x")
        for rel in base_files:
            path = Path(repo) / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"{rel}\n" * 20)
        git("add", "-A")
        git("commit", "-q", "-m", "base")
        base = git("rev-parse", "HEAD").strip()
        tree = git("rev-parse", "HEAD^{tree}").strip()
        for platform in ("fabric", "databricks"):
            if release == "none" or (release == "mismatch" and platform == "databricks"):
                continue
            synced = subprocess.run(["git", "-C", repo, "commit-tree", tree, "-m",
                                     f"chore: sync release/{platform} from main@{base}"],
                                    check=True, capture_output=True, text=True, env=_git_env()).stdout.strip()
            git("update-ref", f"refs/remotes/origin/release/{platform}", synced)
        if release == "mismatch":
            other = subprocess.run(["git", "-C", repo, "commit-tree", tree, "-m",
                                    f"chore: sync release/databricks from main@{'1' * 40}"],
                                   check=True, capture_output=True, text=True, env=_git_env()).stdout.strip()
            git("update-ref", "refs/remotes/origin/release/databricks", other)
        if unverified_code:
            (Path(repo) / "src").mkdir(exist_ok=True)
            (Path(repo) / "src" / "unverified.py").write_text("red = True\n")
            git("add", "-A")
            git("commit", "-q", "-m", "an unverified code commit")
            base = git("rev-parse", "HEAD").strip()  # what GitHub's `before` would be
        for rel in changed:
            path = Path(repo) / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(rel)
        for rel in deleted:
            git("rm", "-q", rel)
        for src, dst in renamed:
            (Path(repo) / dst).parent.mkdir(parents=True, exist_ok=True)
            git("mv", src, dst)
        git("add", "-A")
        git("commit", "-q", "--allow-empty", "-m", "change")
        head = git("rev-parse", "HEAD").strip()
        out = Path(repo) / "github_output"
        subprocess.run(["bash", "-euo", "pipefail", "-c", script], cwd=repo, check=True, capture_output=True,
                       env={**os.environ, "EVENT": event, "BASE_SHA": base_override or base, "HEAD_SHA": head,
                            "GITHUB_OUTPUT": str(out)})
        found = re.findall(r"^code=(\w+)$", out.read_text(), re.M)
        assert len(found) == 1, f"the classifier wrote {found!r}, expected one code= line"
        return found[0]


def test_the_path_classifier_decides_as_documented() -> None:
    """F40 — run the real classify script, not a reading of it.

    Docs-only means every changed path is under .claude/ or docs/, or is a Markdown file;
    anything else is code. A push is compared tree to tree against its last verified base
    (F102); an unusable base, a failed diff or an empty diff is code.
    """
    script = _classify_script(_TESTS.read_text(encoding="utf-8"))
    assert script, "no classify step found in the changes job"
    cases = {
        # F102: a push is classified too; docs-only is not code, code is.
        ("push", ("docs/x.md",)): "false",
        ("push", ("src/spark_quick_start/x.py",)): "true",
        # F102's adversary: tests/architecture shares the test job's pytest process with the
        # unit suite (shadowed imports, nested conftests, basename collisions), so it is code.
        ("pull_request", ("tests/architecture/test_x.py", ".claude/backlog.md")): "true",
        ("pull_request", ("tests/architecture/sub/conftest.py",)): "true",
        ("pull_request", (".claude/backlog.md", "docs/USER_GUIDE.md", "readme.MD")): "false",
        ("pull_request", (".github/copilot-instructions.md",)): "false",
        ("pull_request", ("src/spark_quick_start/x.py",)): "true",
        ("pull_request", ("tests/unit/test_x.py",)): "true",
        ("pull_request", (".github/workflows/x.yaml",)): "true",
        ("pull_request", ("requirements.txt",)): "true",
        ("pull_request", (".claude/backlog.md", "Dockerfile")): "true",
        ("pull_request", ()): "true",
    }
    wrong = {f"{event} {paths}": (got, want) for (event, paths), want in cases.items()
             if (got := _run_classifier(script, event, list(paths))) != want}
    mod = "src/spark_quick_start/x.py"
    moves = {
        "module renamed into docs/": (_run_classifier(script, "pull_request", [], base_files=(mod,),
                                                      renamed=((mod, "docs/x.py"),)), "true"),
        "test renamed to Markdown": (_run_classifier(script, "pull_request", [], base_files=("tests/unit/test_x.py",),
                                                     renamed=(("tests/unit/test_x.py", "docs/test_x.md"),)), "true"),
        "module deleted": (_run_classifier(script, "pull_request", [], base_files=(mod,), deleted=(mod,)), "true"),
        "doc renamed within docs/": (_run_classifier(script, "pull_request", [], base_files=("docs/a.md",),
                                                     renamed=(("docs/a.md", "docs/b.md"),)), "false"),
    }
    wrong |= {label: pair for label, pair in moves.items() if pair[0] != pair[1]}
    # F102: a base the classifier cannot use is code, never docs.
    for label, base in (("zero base", "0" * 40), ("unknown base", "1" * 40)):
        got = _run_classifier(script, "pull_request", ["docs/x.md"], base_override=base)
        if got != "true":
            wrong[label] = (got, "true")
    # F102 review: a push is diffed against the last VERIFIED commit, never `before`.
    pushes = {
        "docs after an unverified code commit": (dict(unverified_code=True), "true"),
        "no release refs": (dict(release="none"), "true"),
        "release branches disagree": (dict(release="mismatch"), "true"),
    }
    for label, (kwargs, want) in pushes.items():
        got = _run_classifier(script, "push", ["docs/x.md"], **kwargs)
        if got != want:
            wrong[label] = (got, want)
    assert not wrong, f"classified wrongly (got, want): {wrong}"


# --- F43: the test workflow is read-only --------------------------------------------------


def _writes(permissions: Any) -> str | None:
    """'write' if a permissions value grants write, 'unreadable' if it is not a form GitHub
    documents, None when it is read-only."""
    if permissions == {}:
        return None
    if isinstance(permissions, str):
        return {"read-all": None, "write-all": "write"}.get(permissions, "unreadable")
    if isinstance(permissions, dict):
        levels = list(permissions.values())
        if not all(isinstance(v, str) and v in ("read", "write", "none") for v in levels):
            return "unreadable"
        return "write" if "write" in levels else None
    # A bare `permissions:` loads as None; whether GitHub reads that as none or as unset (the
    # repository default, which may be write) is not established, so it fails closed.
    return "unreadable"


def _read_only_problems(tests: str) -> list[str]:
    """Every way the test workflow could write to the repository (F43).

    It cannot merge, push or tag, so nothing it does can bypass itself: a merge made with
    `GITHUB_TOKEN` starts no workflow run, so a merging job here would skip main's tests and,
    since F32, the release sync. Every job declares its permissions — a job without them gets
    the repository default, which may be write — and none holds write.
    """
    doc, error = _load(tests)
    if doc is None:
        return [error]
    problems = []
    if "permissions" in doc and (why := _writes(doc["permissions"])):
        problems.append(f"the workflow-level permissions are {why}")
    jobs = _jobs(doc)
    if not jobs:
        return [*problems, "no jobs found under jobs:; this check cannot vouch for any"]
    for name, job in jobs.items():
        if not isinstance(job, dict) or "permissions" not in job:
            problems.append(f"{name} declares no permissions, so it gets the repository default")
        elif why := _writes(job["permissions"]):
            problems.append(f"{name}'s permissions are {why}")
    return problems


def test_the_test_workflow_is_read_only() -> None:
    """F43 — the workflow that verifies main cannot change it."""
    problems = _read_only_problems(_TESTS.read_text(encoding="utf-8"))
    assert not problems, f"the test workflow can write: {problems}"


def test_every_read_only_check_can_fail() -> None:
    """F43 — anti-vacuity: the selector, every spelling of write, and valid forms kept clean."""
    tests = _TESTS.read_text(encoding="utf-8")
    assert _read_only_problems(tests) == []
    anchor = "\n  guard-trunk:\n"
    assert anchor in tests

    def job(perms: str, key: str = "  extra:") -> str:
        return tests.replace(anchor, f"\n{key}\n    runs-on: ubuntu-latest\n{perms}    steps:\n      - run: 'true'\n" + anchor, 1)

    mutations = {
        "pull-requests: write": job("    permissions:\n      contents: read\n      pull-requests: write\n"),
        "write quoted": job("    permissions:\n      contents: 'write'\n"),
        "write after a blank line": job("    permissions:\n      contents: read\n\n      actions: write\n"),
        "id-token: write": job("    permissions:\n      id-token: write\n"),
        "flow {contents: write}": job("    permissions: {contents: write}\n"),
        "write-all": job("    permissions: write-all\n"),
        "write-all, key with a comment": job("    permissions: write-all\n", key="  extra:  # publishes"),
        "no permissions": job(""),
        "unreadable permissions": job("    permissions: some-new-form\n"),
        "workflow-level write": tests.replace("\njobs:\n", "\npermissions:\n  contents: write\n\njobs:\n", 1),
        "no jobs at all": re.sub(r"\njobs:\n(?:.*\n?)*", "\njobs:\n", tests),
        "write behind an anchor": job("    permissions: &p\n      contents: write\n"),
        "permissions left null": job("    permissions:\n"),
        "a scope holding a list": job("    permissions:\n      contents: [write]\n"),
        "a duplicate permissions key": job("    permissions:\n      contents: read\n    permissions:\n      contents: write\n"),
        "write hidden by a later read": job("    permissions:\n      contents: write\n    permissions:\n      contents: read\n"),
    }
    wrong = {label: _read_only_problems(text) for label, text in mutations.items()
             if text == tests or len(_read_only_problems(text)) != 1}
    assert not wrong, f"these mutations were not reported by exactly one check: {wrong}"
    controls = {
        "read-all": job("    permissions: read-all\n"),
        "flow { contents: read }": job("    permissions: { contents: read }\n"),
        "trailing comment": job("    permissions:\n      contents: read  # clone only\n"),
        "no permissions at all": job("    permissions: {}\n"),
    }
    noisy = {label: _read_only_problems(text) for label, text in controls.items() if _read_only_problems(text)}
    assert not noisy, f"these valid forms were reported: {noisy}"


# --- F37: third-party actions are pinned to a commit --------------------------------------


_ACTION_REF = re.compile(r"^([\w.-]+)/([\w.-]+)((?:/[\w.-]+)*)@(\S+)$")


def _uses_nodes(text: str) -> list[tuple[Any, str]]:
    """Every `uses:` where GitHub reads one — on a job, or on a step — as (value, the rest of
    the physical line after the value). Found through the composed node tree, so the
    trailing `# vX.Y.Z` comment is read from exactly the line that value ends on, not from
    anywhere else the same string appears (F48 review)."""
    root = yaml.compose(text, Loader=_StrictLoader)
    lines = text.splitlines()

    def child(node: Any, key: str) -> Any:
        if isinstance(node, yaml.MappingNode):
            for k, v in node.value:
                if isinstance(k, yaml.ScalarNode) and k.value == key:
                    return v
        return None

    found = []
    jobs = child(root, "jobs")
    for _, job in jobs.value if isinstance(jobs, yaml.MappingNode) else []:
        holders = [job]
        steps = child(job, "steps")
        holders += steps.value if isinstance(steps, yaml.SequenceNode) else []
        for holder in holders:
            node = child(holder, "uses")
            if node is None:
                continue
            value = node.value if isinstance(node, yaml.ScalarNode) else node
            end = node.end_mark
            rest = lines[end.line][end.column:] if end.line < len(lines) else ""
            found.append((value, rest))
    return found


def _pin_problems(workflows: dict[str, str]) -> list[str]:
    """Every action reference that can change under this repository without a commit here (F37).

    A tag is mutable: whoever controls the action's repository can move `@v2` to new code, and
    it runs with the job's token. So every action outside `actions/` is a full 40-hex commit
    SHA with its version as a trailing `# vX.Y.Z` comment on the same line; a `docker://`
    image carries its sha256 digest; a local `./` action needs nothing — unless an
    `actions/checkout` fetched another repository for it to run, which is reported.
    """
    problems = []
    for name, text in workflows.items():
        doc, error = _load(text)
        if doc is None:
            problems.append(f"{name}: {error}")
            continue
        for step in (s for j in _jobs(doc).values() for s in _steps(j)):
            with_ = step.get("with") if isinstance(step.get("with"), dict) else {}
            if str(step.get("uses", "")).startswith("actions/checkout@") and "repository" in with_:
                problems.append(f"{name}: actions/checkout fetches {with_['repository']!r}; code from another "
                                "repository then runs as a local ./ action at whatever ref it names")
        for value, rest in _uses_nodes(text):
            if not isinstance(value, str):
                problems.append(f"{name}: `uses:` holds a {type(value).__name__}, not a string reference")
                continue
            if value.startswith("./"):
                continue
            if value.startswith("docker://"):
                if not re.search(r"@sha256:[0-9a-f]{64}$", value):
                    problems.append(f"{name}: {value!r} is a docker image without a sha256 digest")
                continue
            ref = _ACTION_REF.match(value)
            if ref is None or ".." in value:
                problems.append(f"{name}: {value!r} is a reference this check cannot read")
                continue
            if ref.group(1) == "actions":
                continue
            if not re.fullmatch(r"[0-9a-f]{40}", ref.group(4)):
                problems.append(f"{name}: {value!r} is not pinned to a full commit SHA")
            elif not re.fullmatch(r"[ \t]+#[ \t]*v\d+(?:\.\d+)*(?:-[\w.]+)?[ \t]*", rest):
                problems.append(f"{name}: {value!r} has no `# vX.Y.Z` comment on its own line")
    return problems


def test_third_party_actions_are_pinned_to_a_commit() -> None:
    """F37 — no action outside `actions/` runs code this repository did not pin."""
    problems = _pin_problems(_workflow_files())
    assert not problems, (
        "every action outside actions/ is pinned to a commit SHA with a `# vX.Y.Z` comment — "
        f"see _pin_problems and .claude/standards/tooling-and-gates.md (F37): {problems}"
    )


def test_every_pin_check_can_fail() -> None:
    """F37 — anti-vacuity: every spelling that once got past a regex, now judged on the parse."""
    files = _workflow_files()
    assert _pin_problems(files) == []
    rel = "release.yaml"
    line = next(ln for ln in files[rel].splitlines() if "softprops/action-gh-release@" in ln)
    sha = "3bb12739c298aeb8a4eeaf626c5b8d85266b0e65"

    def with_uses(value: str) -> dict[str, str]:
        return files | {rel: files[rel].replace(line, f"        uses: {value}", 1)}

    def with_line(text: str) -> dict[str, str]:
        return files | {rel: files[rel].replace(line, text, 1)}

    def with_step_after(step: str) -> dict[str, str]:
        # Appended as the job's last step, so it cannot capture an existing step's keys.
        return files | {rel: files[rel].rstrip("\n") + "\n" + step + "\n"}

    mutations = {
        "a tag": with_uses("softprops/action-gh-release@v2"),
        "a tag, quoted": with_uses("'softprops/action-gh-release@v2'"),
        "a short SHA": with_uses("softprops/action-gh-release@3bb1273  # v2.6.2"),
        "a SHA, no version comment": with_uses(f"softprops/action-gh-release@{sha}"),
        "a SHA, comment not a version": with_uses(f"softprops/action-gh-release@{sha}  # latest"),
        "a sub-path action on a tag": with_uses("owner/repo/sub/dir@v1"),
        "docker image without digest": with_uses("docker://alpine:3.20"),
        "a templated reference": with_uses("${{ matrix.action }}"),
        "actions/.. owner escape": with_uses("actions/../evil/repo@v1"),
        # Every form that got past the regex versions (F37's probe and review).
        "a flow-style step": with_line("      - { uses: owner/repo@v1 }"),
        "quoted key": with_line('      - "uses": owner/repo@v1'),
        "JSON-style step": with_line('      - {"uses": "owner/repo@v1"}'),
        "escaped key": with_line('      - "u\\x73es": owner/repo@v1'),
        "explicit ? key": with_line("      - ? uses # k\n        : owner/repo@v1"),
        "# inside a multi-line string": with_line('      - name: "x\n          #"\n        uses: owner/repo@v1'),
        "Unicode line separator": with_line("        uses: owner/repo@v1 # v1 "),
        "uses after a step-opening if: >": with_line(
            "      - if: >\n          github.event_name == 'push'\n        uses: owner/repo@v1"),
        "uses after a step-opening name: >-": with_line("      - name: >-\n          x\n        uses: owner/repo@v1"),
        "anchor on the key": with_line("      - &s uses: owner/repo@v1"),
        "tag on the key": with_line("      - !!str uses: owner/repo@v1"),
        "anchor before an open quote": with_line('      - name: &a "x\n          #"\n        uses: owner/repo@v1'),
        "a second uses in the same step": with_line(line + "\n        uses: owner/repo@v1"),
        # Bad value first: plain PyYAML keeps the last duplicate, so without the strict
        # loader this unpinned reference would vanish behind the pinned one.
        "an unpinned uses hidden by a later pinned one": with_line("        uses: owner/repo@v1\n" + line),
        "a reusable workflow on a tag": files | {rel: files[rel].rstrip("\n")
                                                 + "\n  reuse:\n    uses: owner/repo/.github/workflows/x.yml@v1\n"},
        # F48's probe: the version comment found elsewhere, and code fetched from another repo.
        "version comment only on another line": files | {rel: files[rel].replace(
            line, f"        uses: evil/repo@{sha}", 1) + f"\n# evil/repo@{sha}  # v1.0.0\n"},
        "version comment on a different owner": files | {rel: files[rel].replace(
            line, f"        uses: evil/repo@{sha}", 1).rstrip("\n")
            + f"\n      - uses: notevil/repo@{sha}  # v1.0.0\n"},
        "version comment inside a run block": files | {rel: files[rel].replace(
            line, f"        uses: evil/repo@{sha}", 1).rstrip("\n")
            + f"\n      - run: |\n          uses: evil/repo@{sha}  # v1.0.0\n"},
        "checkout of another repository": with_step_after(
            "      - uses: actions/checkout@v4\n        with:\n          repository: evil/repo\n          path: evil"),
    }
    wrong = {label: _pin_problems(f) for label, f in mutations.items()
             if f == files or len(_pin_problems(f)) != 1}
    assert not wrong, f"these mutations were not reported by exactly one check: {wrong}"
    controls = {
        "first-party on a tag": with_uses("actions/checkout@v4"),
        "pinned, quoted, commented": with_uses(f"'softprops/action-gh-release@{sha}'  # v2.6.2"),
        "a local action": with_uses("./.github/actions/setup"),
        "docker image by digest": with_uses("docker://alpine@sha256:" + "a" * 64),
        "uses: inside a run string": with_step_after("      - run: 'echo this uses: nothing'"),
        "a key named re-uses": with_step_after("      - run: x\n        with:\n          re-uses: 1"),
        "uses: inside a block scalar": with_step_after("      - run: |\n          uses: foo"),
        "a pre-release version": with_uses(f"softprops/action-gh-release@{sha}  # v2.6.2-rc.1"),
        "uses as data under with:": with_step_after("      - run: x\n        env:\n          uses: anything"),
    }
    noisy = {label: _pin_problems(f) for label, f in controls.items() if _pin_problems(f)}
    assert not noisy, f"these valid forms were reported: {noisy}"


def test_the_lock_keeps_its_header() -> None:
    """F48 — `uv pip compile` drops requirements.txt's hand-written header on regeneration.

    The header is where the file says it is derived, how to regenerate it, why `[runtime]`
    is excluded and that it is Python-3.11-specific. It was lost once; this makes a second
    loss red instead of silent.
    """
    head = _LOCK.read_text(encoding="utf-8").splitlines()[:3]
    assert head and head[0].startswith("# requirements.txt — DERIVED LOCK"), (
        "requirements.txt lost its header — uv drops it on regeneration; restore it from git "
        f"history before committing. First lines: {head}"
    )


_README = "README.md"



def _readme_problems(pyproject: str, root_entries: set[str]) -> list[str]:
    """Every way the project readme can be other than `README.md`, spelled exactly (F52).

    F45: pyproject said `README.md` while the file was `readme.MD`; macOS resolves the two as
    one file, the Linux runners that build the wheel do not, and the wheel shipped an empty
    long description. F45 pointed pyproject at the real name; the owner then chose
    `README.md` as the name (F52).

    The TOML is parsed, not matched: the first version read `readme = "..."` lines with a
    regex, and F52's adversary got past it seven ways — a `readme` in another table, in a
    multi-line string, or `[project].readme` written as an inline table, dotted keys or a
    `[project.readme]` sub-table naming another file. PEP 621 allows the string form or a
    table with `file` (or `text`); only the file `README.md` is accepted.

    Root entries are compared as names, so the check is case-sensitive on any filesystem; a
    readme variant is any entry whose stem is `readme` in any case (`README`, `readme.MD`,
    `README.rst`), which is what collides on a case-insensitive checkout or shadows it.
    """
    problems = []
    try:
        project = tomllib.loads(pyproject).get("project")
    except tomllib.TOMLDecodeError as exc:
        return [f"pyproject.toml is not valid TOML: {exc}"]
    readme = project.get("readme") if isinstance(project, dict) else None
    declared = readme if isinstance(readme, str) else readme.get("file") if isinstance(readme, dict) else None
    if declared != _README or (isinstance(readme, dict) and "text" in readme):
        problems.append(f"[project].readme is {readme!r}; it must name the file exactly {_README!r}")
    if _README not in root_entries:
        problems.append(f"no file named exactly {_README!r} at the repository root")
    others = sorted(e for e in root_entries if e.split(".")[0].lower() == "readme" and e != _README)
    if others:
        problems.append(f"other readme files at the root, which case-insensitive checkouts collide on: {others}")
    return problems


def test_the_project_readme_is_README_md() -> None:
    """F52 — one readme, `README.md`, and pyproject names it."""
    problems = _readme_problems(_PYPROJECT.read_text(encoding="utf-8"),
                                {e.name for e in _arch.ROOT.iterdir()})
    assert not problems, f"the project readme is not as declared: {problems}"


def test_every_readme_check_can_fail() -> None:
    """F52 — anti-vacuity: each check reported alone; F52's adversary's bypasses are mutations,
    its false alarms controls."""
    P = '[project]\nname = "x"\n'
    good = P + 'readme = "README.md"\n'
    entries = {"README.md", "pyproject.toml"}
    assert _readme_problems(good, entries) == []
    mutations = {
        "pyproject names readme.MD": (P + 'readme = "readme.MD"\n', entries),
        "pyproject declares no readme": (P, entries),
        "only readme.MD at the root": (good, {"readme.MD", "pyproject.toml"}),
        "a second readme beside it": (good, entries | {"readme.MD"}),
        # The adversary's bypasses of the regex version:
        "readme only in another table": (P + '[tool.x]\nreadme = "README.md"\n', entries),
        "no [project] table": ('[other]\nreadme = "README.md"\n', entries),
        "inline table naming readme.MD": (P + 'readme = {file = "readme.MD", content-type = "text/markdown"}\n'
                                          '[tool.x]\nreadme = "README.md"\n', entries),
        "dotted keys naming readme.MD": (P + 'readme.file = "readme.MD"\n[tool.x]\nreadme = "README.md"\n', entries),
        "sub-table naming readme.MD": (P + '[project.readme]\nfile = "readme.MD"\n[tool.x]\nreadme = "README.md"\n', entries),
        "only inside a multi-line string": (P + 'description = """\nreadme = "README.md"\n"""\n', entries),
        "inline text, not a file": (P + 'readme = {text = "hi", content-type = "text/plain"}\n', entries),
        "not TOML at all": ("readme = [\n", entries),
    }
    # readme.MD alone both misses README.md and is a stray readme, so it is reported twice.
    expected = dict.fromkeys(mutations, 1) | {"only readme.MD at the root": 2}
    wrong = {label: _readme_problems(*args) for label, args in mutations.items()
             if len(_readme_problems(*args)) != expected[label]}
    assert not wrong, f"these mutations were not reported as expected {expected}: {wrong}"
    controls = {
        "single quotes": P + "readme = 'README.md'\n",
        "inline table": P + 'readme = {file = "README.md", content-type = "text/markdown"}\n',
        "dotted keys": P + 'readme.file = "README.md"\nreadme.content-type = "text/markdown"\n',
        "sub-table": P + '[project.readme]\nfile = "README.md"\ncontent-type = "text/markdown"\n',
        "indented key": P + '  readme = "README.md"\n',
        "quoted key": P + '"readme" = "README.md"\n',
        "multi-line string value": P + 'readme = """README.md"""\n',
        "unicode escape": P + 'readme = "README\\u002Emd"\n',
        "an unrelated readme key elsewhere": good + '[tool.x]\nreadme = "other"\n',
    }
    noisy = {label: _readme_problems(text, entries) for label, text in controls.items() if _readme_problems(text, entries)}
    assert not noisy, f"these valid forms were reported: {noisy}"
    for name in ("readme-tools.py", "READMEs", ".readme"):
        assert _readme_problems(good, entries | {name}) == [], f"{name!r} is not a readme variant"


def test_a_force_push_cannot_hide_a_code_revert() -> None:
    """F102's adversary (B5): a force-push cannot hide a code revert.

    History: P; X = P + a code change (the verified, synced tip); N = P + a doc change,
    force-pushed over X. A three-dot `X...N` diff runs from the merge-base P and lists only
    the doc. The classifier requires the verified commit to be an ancestor of the head, so
    N — which does not descend from X — is code.
    """
    import os
    import subprocess
    import tempfile

    script = _classify_script(_TESTS.read_text(encoding="utf-8"))
    assert script, "no classify step found in the changes job"
    env = {**os.environ, "GIT_CONFIG_GLOBAL": os.devnull, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
           "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"}
    with tempfile.TemporaryDirectory() as repo:
        def git(*args: str) -> str:
            return subprocess.run(["git", "-C", repo, *args], check=True, capture_output=True,
                                  text=True, env=env).stdout.strip()
        git("init", "-q")
        (Path(repo) / "seed").write_text("x")
        git("add", "-A")
        git("commit", "-q", "-m", "P")
        base = git("rev-parse", "HEAD")
        (Path(repo) / "src").mkdir()
        (Path(repo) / "src" / "a.py").write_text("x = 1\n")
        git("add", "-A")
        git("commit", "-q", "-m", "X")
        old_tip = git("rev-parse", "HEAD")
        git("checkout", "-q", base)
        (Path(repo) / "docs").mkdir()
        (Path(repo) / "docs" / "d.md").write_text("doc\n")
        git("add", "-A")
        git("commit", "-q", "-m", "N")
        new_tip = git("rev-parse", "HEAD")
        # The old tip X was the last verified, synced commit; N does not descend from it.
        for platform in ("fabric", "databricks"):
            synced = git("commit-tree", git("rev-parse", "HEAD^{tree}"), "-m",
                         f"chore: sync release/{platform} from main@{old_tip}")
            git("update-ref", f"refs/remotes/origin/release/{platform}", synced)
        out = Path(repo) / "github_output"
        subprocess.run(["bash", "-euo", "pipefail", "-c", script], cwd=repo, check=True, capture_output=True,
                       env={**env, "EVENT": "push", "BASE_SHA": old_tip, "HEAD_SHA": new_tip,
                            "GITHUB_OUTPUT": str(out)})
        found = re.findall(r"^code=(\w+)$", out.read_text(), re.M)
    assert found == ["true"], f"a force-push that reverts code was classified {found}"


# --- F135: the platform-filtered builds run on demand only ---------------------------------


_PLATFORMS = ("fabric", "databricks")
_AUTOMATIC_PATTERNS = (r"filter_platform", r"platform-builds", r"platform_builds", r"\bgh workflow run\b",
                       r"/dispatches\b", r"workflow-dispatch")
_BUILD_TOP_KEYS = {"name", "on", "permissions", "jobs", "concurrency", "run-name"}
_BUILD_JOB_KEYS = {"name", "if", "runs-on", "permissions", "steps"}


def _unquoted(text: str) -> str:
    """A shell string with what bash removes before it runs removed: a backslash-newline, and
    backslashes and quotes, so `filter_platform_rel\\ease.sh` and `'filter_platform'_release.sh`
    read as the name they run, and runs of whitespace as one space."""
    return " ".join(re.sub(r"\\\n|[\\'\"]", "", text).split())


def _gate_values(value: Any) -> set[str] | None:
    """The input values an `if:` admits, when it is a disjunction of `inputs.platform == '<v>'`
    comparisons in any order or operand order; None for any other form, which is reported.

    A `${{ }}` with anything around it, a block scalar's newline included, is a non-empty string
    to GitHub and always true, so it is refused here rather than unwrapped."""
    if not isinstance(value, str):
        return None
    text = value
    if "${{" in text:
        wrapped = re.fullmatch(r"\$\{\{(.*)\}\}", text, re.S)
        if not wrapped:
            return None
        text = wrapped.group(1)
    text = " ".join(text.split())
    if "&&" in text or "!" in text:
        return None
    values = set()
    for part in text.split("||"):
        part = part.strip()
        while part.startswith("(") and part.endswith(")"):
            part = part[1:-1].strip()
        match = re.fullmatch(r"(\S.*?)\s*==\s*(\S.*)", part)
        if not match:
            return None
        sides = {s.strip().replace("github.event.inputs.", "inputs.").replace("inputs['platform']", "inputs.platform")
                 for s in match.groups()}
        literal = [s for s in sides if re.fullmatch(r"'[^']*'", s)]
        if len(sides) != 2 or "inputs.platform" not in sides or len(literal) != 1:
            return None
        values.add(literal[0][1:-1])
    return values


def _command(run: Any) -> str:
    """A step's command with the spellings that run the same thing read as one."""
    text = _norm(run).replace('"', "'")
    text = re.sub(r"^python -m pytest\b", "pytest", text)
    return re.sub(r"\bbash \./scripts/", "bash scripts/", text)


def _on_demand_problems(tests: str, builds: str | None) -> list[str]:
    """Every way the test workflow can run the platform-filtered builds, or the builds workflow
    not run them as asked (F135). It reads those two files only: the sync workflow, which runs
    the filter to build release branches, `release.yaml` and the Dockerfile
    are not read.

    The owner's decision (2026-09-29): they leave the automatic test workflow and run only from
    `platform-builds.yaml`, started by hand with a `platform` input of `both`, `fabric` or
    `databricks`. The test workflow may not name the filter script or the builds workflow in
    any string, dispatch a workflow, or call a local workflow or action — a text check cannot
    follow a call, so it refuses one. The builds workflow is ours and pinned to one layout: each
    platform its own job gated on the input, with checkout, setup-python, the filter, the locked
    install and pytest as its only steps, and no key that could skip, redirect or neuter them.
    """
    problems = []
    doc, error = _load(tests)
    if doc is None:
        problems.append(f"test workflow: {error}")
    else:
        for text in {_unquoted(s) for s in _strings(doc)}:
            for pattern in _AUTOMATIC_PATTERNS:
                if re.search(pattern, text):
                    problems.append(f"the test workflow names `{pattern}`: {text[:80]!r}")
        for mapping in _walk(doc):
            uses = mapping.get("uses")
            if isinstance(uses, str) and uses.startswith("./"):
                problems.append(f"the test workflow calls the local {uses!r}, which this check cannot read")
    if builds is None:
        return [*problems, "platform-builds.yaml not found"]
    bdoc, error = _load(builds)
    if bdoc is None:
        return [*problems, f"platform-builds.yaml: {error}"]
    extra = set(bdoc) - _BUILD_TOP_KEYS - {True}
    if extra:
        problems.append(f"platform-builds.yaml has top-level keys it must not: {sorted(map(str, extra))}")
    on = _on(bdoc)
    dispatch = on.get("workflow_dispatch") if isinstance(on, dict) else None
    if not isinstance(on, dict) or set(on) != {"workflow_dispatch"}:
        problems.append("platform-builds.yaml must be started by workflow_dispatch and nothing else")
    inputs = dispatch.get("inputs") if isinstance(dispatch, dict) else None
    platform = inputs.get("platform") if isinstance(inputs, dict) else None
    if not isinstance(platform, dict) or platform.get("type") != "choice" \
            or not isinstance(platform.get("options"), list) \
            or sorted(map(str, platform["options"])) != sorted(("both", *_PLATFORMS)) \
            or platform.get("required") is not True or platform.get("default") != "both":
        problems.append("no required `platform` choice input offering exactly both, fabric and databricks, "
                        "defaulting to both")
    problems += [f"platform-builds.yaml: {p}" for p in _read_only_problems(builds)]
    jobs = _jobs(bdoc)
    for platform_name in _PLATFORMS:
        name = f"filtered-{platform_name}"
        job = jobs.get(name)
        if not isinstance(job, dict):
            problems.append(f"job {name} not found")
            continue
        if _gate_values(job.get("if")) != {"both", platform_name}:
            problems.append(f"{name} is not gated on `inputs.platform == 'both' || inputs.platform == '{platform_name}'`")
        if set(job) - _BUILD_JOB_KEYS:
            problems.append(f"{name} has keys it must not: {sorted(map(str, set(job) - _BUILD_JOB_KEYS))}")
        if job.get("runs-on") != "ubuntu-latest":
            problems.append(f"{name} does not run on ubuntu-latest")
        steps = job.get("steps") if isinstance(job.get("steps"), list) else []
        layout = [("uses", "actions/checkout@"), ("uses", "actions/setup-python@"),
                  ("run", f"bash scripts/filter_platform_release.sh {platform_name}"),
                  ("run", "pip install -e '.[dev,spark,runtime]' -c requirements.txt"),
                  ("run", "pytest -q")]
        if len(steps) != len(layout):
            problems.append(f"{name} does not have exactly the steps checkout, setup-python, filter, install, pytest")
            continue
        for step, (kind, want) in zip(steps, layout, strict=True):
            if not isinstance(step, dict):
                problems.append(f"{name} has a step that is not a mapping")
                continue
            if kind == "uses":
                ok = str(step.get("uses", "")).startswith(want) and set(step) <= {"name", "uses", "with"}
                if want == "actions/checkout@":
                    ok = ok and "with" not in step
                if not ok:
                    problems.append(f"{name} does not use {want.rstrip('@')} as that step, without a ref or extra keys")
            elif _command(step.get("run")) != want or set(step) - {"name", "run", "if"} \
                    or ("if" in step and _norm(step["if"]) not in ("success()", "${{ success() }}")):
                problems.append(f"{name} does not run exactly `{want}` as that step, unconditionally")
    return problems


def test_platform_builds_run_on_demand_only() -> None:
    """F135 — the filtered builds are started by hand, and do what they did in the test workflow."""
    problems = _on_demand_problems(_TESTS.read_text(encoding="utf-8"),
                                   _BUILDS.read_text(encoding="utf-8") if _BUILDS.exists() else None)
    assert not problems, f"the platform-filtered builds are not on demand as decided: {problems}"


def test_every_on_demand_check_can_fail() -> None:
    """F135 — anti-vacuity: each form the adversary found is reported; each valid spelling is not.

    Four valid forms are reported on purpose, because the check pins one readable layout rather
    than evaluate expressions or shell: a `contains(fromJSON(...))` gate, the three commands in
    one `run:` block, the test workflow naming the script in an `echo` or calling it with
    `--list`, and a builds workflow whose only `permissions:` is workflow-level."""
    tests = _TESTS.read_text(encoding="utf-8")
    builds = _BUILDS.read_text(encoding="utf-8")
    assert _on_demand_problems(tests, builds) == []
    anchor = "\n  lint:\n"
    assert anchor in tests

    def auto(body: str) -> str:
        job = ("\n  extra:\n    runs-on: ubuntu-latest\n    permissions:\n      contents: read\n"
               f"    steps:\n{body}")
        return tests.replace(anchor, job + anchor, 1)

    run_step = "      - run: {}\n".format
    fabric_gate = "    if: inputs.platform == 'both' || inputs.platform == 'fabric'\n"
    pytest_step = "      - name: Run filtered test suite\n        run: pytest -q\n"
    filter_step = "      - name: Filter checkout for fabric\n        run: bash scripts/filter_platform_release.sh fabric\n"
    install_step = "      - name: Install filtered package\n"
    fabric_job = "\n  filtered-fabric:\n    name: Verify platform-filtered build (fabric)\n"
    for fragment in (fabric_gate, pytest_step, filter_step, install_step, fabric_job):
        assert fragment in builds, fragment

    def job_key(line: str) -> str:
        return builds.replace(fabric_gate, fabric_gate + line, 1)

    automatic = {
        "gh workflow run": auto(run_step("gh workflow run platform-builds.yaml -f platform=both")),
        "gh workflow run by display name, spaced": auto(run_step(
            "gh workflow  run 'Platform-filtered builds' -f platform=both")),
        "curl dispatch": auto(run_step("curl -X POST https://api.github.com/repos/o/r/actions/workflows/x.yaml/dispatches")),
        "dispatch action": auto("      - uses: benc-uk/workflow-dispatch@v1\n        with:\n          workflow: x\n"),
        "backslash in the name": auto(run_step("bash scripts/filter_platform_rel\\\\ease.sh fabric")),
        "line continuation": auto("      - run: |\n          bash scripts/filter_platform_\\\n          release.sh fabric\n"),
        "name in env": auto("      - run: bash \"scripts/$F\" fabric\n        env:\n          F: filter_platform_release.sh\n"),
        "glob": auto(run_step("bash scripts/filter_platform_rel*.sh fabric")),
        "split quoting": auto(run_step("bash scripts/'filter_platform'_release.sh fabric")),
        "concatenated in python": auto(run_step(
            "python -c \"import subprocess; subprocess.run(['bash', 'scripts/filter_platform_release' + '.sh', 'fabric'])\"")),
        "shell override": auto("      - run: pytest -q\n        shell: bash scripts/filter_platform_release.sh fabric {0}\n"),
        "local reusable workflow": tests.replace(
            anchor, "\n  extra:\n    permissions:\n      contents: read\n    uses: ./.github/workflows/filtered.yaml\n" + anchor, 1),
        "local action": auto("      - uses: ./.github/actions/filtered-build\n"),
    }
    neutered = {
        "gate in a literal block": builds.replace(
            fabric_gate, "    if: |\n      ${{ inputs.platform == 'both' || inputs.platform == 'fabric' }}\n", 1),
        "gate in a folded block": builds.replace(
            fabric_gate, "    if: >\n      ${{ inputs.platform == 'both' || inputs.platform == 'fabric' }}\n", 1),
        "gate with a trailing space": builds.replace(
            fabric_gate, "    if: \"${{ inputs.platform == 'both' || inputs.platform == 'fabric' }} \"\n", 1),
        "gate with a leading space": builds.replace(
            fabric_gate, "    if: \" ${{ inputs.platform == 'both' || inputs.platform == 'fabric' }}\"\n", 1),
        "gate on the wrong platform": builds.replace(fabric_gate, fabric_gate.replace("'fabric'", "'databricks'"), 1),
        "gate that is always true": builds.replace(fabric_gate, "    if: always()\n", 1),
        "shell on the pytest step": builds.replace(pytest_step, pytest_step + "        shell: sh -c 'exit 0' {0}\n", 1),
        "job defaults": job_key("    defaults:\n      run:\n        shell: bash -c true {0}\n"),
        "workflow defaults": builds.replace("\njobs:\n", "\ndefaults:\n  run:\n    shell: bash -c true {0}\n\njobs:\n", 1),
        "job env": job_key("    env:\n      PYTEST_ADDOPTS: --collect-only\n"),
        "workflow env": builds.replace("\njobs:\n", "\nenv:\n  PYTEST_ADDOPTS: --collect-only\n\njobs:\n", 1),
        "step env": builds.replace(pytest_step, pytest_step + "        env:\n          PYTEST_ADDOPTS: --collect-only\n", 1),
        "tree restored after the filter": builds.replace(install_step, "      - run: git checkout -- . && git clean -fd\n" + install_step, 1),
        "script emptied before the filter": builds.replace(filter_step, "      - run: echo true > scripts/filter_platform_release.sh\n" + filter_step, 1),
        "tests removed before pytest": builds.replace(pytest_step, "      - run: rm -rf tests\n" + pytest_step, 1),
        "checkout of another ref": builds.replace("        uses: actions/checkout@v4\n",
                                                  "        uses: actions/checkout@v4\n        with:\n          ref: release/fabric\n", 1),
        "input not required": builds.replace("        required: true\n", "        required: false\n", 1),
        "input default outside the options": builds.replace("        default: both\n", "        default: none\n", 1),
        "input not a choice": builds.replace("        type: choice\n", "        type: string\n", 1),
        "a step skipped by a capitalised if": builds.replace(pytest_step, pytest_step + "        If: false\n", 1),
        "a step skipped by if": builds.replace(pytest_step, pytest_step + "        if: false\n", 1),
        "job continues on error": job_key("    continue-on-error: 0\n"),
        "step continues on error": builds.replace(pytest_step, pytest_step + "        continue-on-error: true\n", 1),
        "job times out at once": job_key("    timeout-minutes: 0\n"),
        "job on another runner": builds.replace("    runs-on: ubuntu-latest\n", "    runs-on: nonexistent\n", 1),
        "job with a matrix": job_key("    strategy:\n      matrix:\n        x: []\n"),
        "job in a container": job_key("    container: alpine\n"),
        "job needs another": job_key("    needs: filtered-databricks\n"),
        "step in another directory": builds.replace(pytest_step, pytest_step + "        working-directory: docs\n", 1),
        "pytest on part of the suite": builds.replace("        run: pytest -q\n", "        run: pytest -q tests/architecture\n", 1),
        "filter for the other platform": builds.replace(
            "run: bash scripts/filter_platform_release.sh fabric", "run: bash scripts/filter_platform_release.sh databricks", 1),
        "install unlocked": builds.replace("-c requirements.txt\n", "\n", 1),
        "a push trigger": builds.replace("\non:\n", "\non:\n  push:\n", 1),
        "a job removed": builds.replace(fabric_job, "\n  filtered-fabrik:\n    name: Verify platform-filtered build (fabric)\n", 1),
        "a job that writes": job_key("    permissions:\n      contents: write\n").replace(
            "    permissions:\n      contents: write\n    runs-on: ubuntu-latest\n    permissions:\n      contents: read\n",
            "    runs-on: ubuntu-latest\n    permissions:\n      contents: write\n", 1),
    }
    # Each is reported once, by the check it targets; `gh workflow run platform-builds.yaml`
    # names two refused tokens, so it is reported twice.
    expected = dict.fromkeys([*automatic, *neutered], 1) | {"gh workflow run": 2}
    wrong = {label: _on_demand_problems(text, builds) for label, text in automatic.items()
             if text == tests or len(_on_demand_problems(text, builds)) != expected[label]}
    wrong |= {label: _on_demand_problems(tests, text) for label, text in neutered.items()
              if text == builds or len(_on_demand_problems(tests, text)) != expected[label]}
    assert not wrong, f"these forms were not reported as expected {expected}: {wrong}"
    assert _on_demand_problems(tests, None) == ["platform-builds.yaml not found"]
    job = ("\n  test-platform-filtered:\n    runs-on: ubuntu-latest\n    permissions:\n      contents: read\n"
           "    steps:\n      - run: bash scripts/filter_platform_release.sh ${{ matrix.platform }}\n")
    assert _on_demand_problems(tests.replace(anchor, job + anchor, 1), builds), "the old job was not reported"

    gate = "    if: {}\n".format
    controls = {
        "disjuncts reversed": builds.replace(fabric_gate, gate("inputs.platform == 'fabric' || inputs.platform == 'both'"), 1),
        "no spaces around ==": builds.replace(fabric_gate, gate("inputs.platform=='both' || inputs.platform=='fabric'"), 1),
        "operands swapped": builds.replace(fabric_gate, gate("\"'both' == inputs.platform || 'fabric' == inputs.platform\""), 1),
        "index syntax": builds.replace(fabric_gate, gate("inputs['platform'] == 'both' || inputs['platform'] == 'fabric'"), 1),
        "parenthesised": builds.replace(fabric_gate, gate("(inputs.platform == 'both') || (inputs.platform == 'fabric')"), 1),
        "wrapped": builds.replace(fabric_gate, gate("${{ inputs.platform == 'both' || inputs.platform == 'fabric' }}"), 1),
        "github.event.inputs": builds.replace(
            fabric_gate, gate("github.event.inputs.platform == 'both' || github.event.inputs.platform == 'fabric'"), 1),
        "python -m pytest": builds.replace("        run: pytest -q\n", "        run: python -m pytest -q\n", 1),
        "./scripts": builds.replace("run: bash scripts/filter_platform_release.sh fabric",
                                    "run: bash ./scripts/filter_platform_release.sh fabric", 1),
        "double-quoted extras": builds.replace("pip install -e '.[dev,spark,runtime]'", 'pip install -e ".[dev,spark,runtime]"', 1),
        "if: success()": builds.replace(pytest_step, pytest_step + "        if: success()\n", 1),
        "checkout pinned to a SHA": builds.replace("uses: actions/checkout@v4", "uses: actions/checkout@" + "a" * 40, 1),
        "quoted on": builds.replace("\non:\n", '\n"on":\n', 1),
        "options reordered": builds.replace("[both, fabric, databricks]", "[fabric, databricks, both]", 1),
    }
    noisy = {label: _on_demand_problems(tests, text) for label, text in controls.items()
             if text == builds or _on_demand_problems(tests, text)}
    assert not noisy, f"these valid forms were reported: {noisy}"
    reported_on_purpose = {
        "contains(fromJSON)": builds.replace(fabric_gate, gate("contains(fromJSON('[\"both\",\"fabric\"]'), inputs.platform)"), 1),
        "one run block": builds.replace(install_step, "", 1),
    }
    assert all(_on_demand_problems(tests, text) for text in reported_on_purpose.values())
    assert _on_demand_problems(auto(run_step("echo see filter_platform_release.sh")), builds)
    quiet = auto("      - name: Summarise this workflow run\n        run: echo done\n")
    assert quiet != tests and _on_demand_problems(quiet, builds) == [], "a step name was reported"
