"""
test_instruction_integrity.py — the instruction system checks itself.

Contains: CLAUDE.md payload and index assertions, rule-enforcement declarations,
    backlog well-formedness, and a dangling-path check across .claude/**.
Does NOT contain: any claim about source code — the sibling modules own those.

This module exists because the failure it catches is the one the old system had:
an index and a directory that quietly disagree, and a document pointing at a path
nobody ever created.
"""
from __future__ import annotations

import itertools
import re
import subprocess
import sys
import tomllib
from collections import Counter

import _arch
import pytest
from _markdown import (
    FENCE_OPEN,
    blank_indented_code,
    blank_unrendered,
    heading_text,
    rendered_headings,
    strip_frontmatter,
)

_CLAUDE_MD = _arch.ROOT / "CLAUDE.md"
_RULES = _arch.CLAUDE_DIR / "rules"
_STANDARDS = _arch.CLAUDE_DIR / "standards"
_BACKLOG = _arch.CLAUDE_DIR / "backlog.md"
_STATUSES = {"open", "planned", "in-progress", "landed", "wontfix"}


def test_claude_md_has_no_at_imports() -> None:
    """Review Focus 1 — the always-on payload is this one file.

    A single re-added @ line silently restores the problem the rewrite removed:
    fourteen files resident in every session, and nothing salient.
    """
    offenders = [
        f"CLAUDE.md:{i}: {line.strip()}"
        for i, line in enumerate(_CLAUDE_MD.read_text(encoding="utf-8").splitlines(), start=1)
        if line.lstrip().startswith("@.claude/")
    ]
    assert not offenders, (
        "CLAUDE.md @-imports a file. Depth is reachable by path, not resident: "
        f"{offenders}"
    )


def test_no_document_claims_the_rules_arrive_by_at_import() -> None:
    """F20 — the sibling of test_claude_md_has_no_at_imports, from the other side.

    That assertion guards the mechanism: CLAUDE.md must not `@`-import a rule.
    This one guards documents that *describe* the mechanism. `review-pr.md` opened
    by telling every reviewer "Those rules and standards are already in your
    context — CLAUDE.md pulls every file in .claude/rules/ and
    .claude/standards/ in by @ import." All fourteen imports had been removed.

    **Scoped to the mechanism, deliberately, after the first version of this test
    was scoped to residency and got it backwards.** Whether the rules are resident
    is not this repo's to assert: the eight `.claude/rules/` files have been
    observed arriving as project instructions with no `@` import anywhere and no
    settings file, while the seven `.claude/standards/` files have not, and
    nothing in the tree causes either. So "the rules are in your context" may well
    be true and "nothing is preloaded" is at least partly false — the first
    version asserted the second, pinned it into the tree, and its phrase set also
    matched the *negation*, so it failed anyone stating the truth plainly. Only
    the `@`-import claim is falsifiable from inside the repo, and that is what
    this checks.

    Normalised before matching — backticks and emphasis stripped, whitespace
    collapsed across the whole document — because these files are hard-wrapped
    near 100 columns and the first version matched per raw line. The deleted
    sentence read ``in by `@` import.``, so that version's own phrase
    "by @ import" never matched the defect it was written for; two of its four
    phrases hit, and only because the wrap happened to fall after "in". An edit
    that shifts the wrap is the ordinary shape of an edit, not an exotic one.

    Sentences carrying a negation are skipped, for the reason backlog.md is
    exempted below: a document must be able to say the claim is false without
    tripping the check that the claim is false. Same ruling as the AST-over-text
    choice in testing.md — a rule's enforcement must not punish the prose that
    explains it.
    """
    # The falsifiable shape: a document saying CLAUDE.md pulls or imports the rule
    # or standard directories in by @ import. Matched against normalised text, so
    # backticks and line wrapping cannot hide it.
    claim = re.compile(
        r"(pulls|imports|@-imports)[^.]{0,80}\.claude/(rules|standards)|"
        r"\.claude/(rules|standards)[^.]{0,80}(in )?by @ import"
    )
    negation = re.compile(r"\b(not|never|no longer|stopped|false|removed|does not)\b", re.I)

    docs = [_CLAUDE_MD]
    for subdir in _INSTRUCTION_DOCS:
        docs.extend(sorted((_arch.CLAUDE_DIR / subdir).rglob("*.md")))
    # backlog.md is deliberately out of scope, unlike in the sibling assertions
    # that append it: an entry's Evidence field must be able to quote the false
    # claim it reports, and a check that forbade that would force the backlog to
    # describe defects in euphemism.

    # Anti-vacuity: rglob over a renamed directory yields nothing without error, so
    # without these two, renaming .claude/commands/ would drop the scan to
    # CLAUDE.md alone and it would still pass green. This module has already
    # shipped one assertion of exactly that shape.
    assert len(docs) >= 25, (
        f"this check reached only {len(docs)} instruction documents; it is meant to walk "
        f"CLAUDE.md plus all of {list(_INSTRUCTION_DOCS)}. A renamed directory makes rglob "
        "return nothing silently."
    )
    review_pr = _arch.CLAUDE_DIR / "commands" / "review-pr.md"
    assert review_pr in docs, (
        f"{_arch.rel(review_pr)} — the document F20 was about — is not in scope"
    )

    offenders = []
    for doc in docs:
        flat = re.sub(
            r"\s+", " ", doc.read_text(encoding="utf-8").replace("`", "").replace("*", "")
        )
        offenders.extend(
            f"{_arch.rel(doc)}: {sentence.strip()[:120]}"
            for sentence in re.split(r"(?<=[.!?]) ", flat)
            if claim.search(sentence) and not negation.search(sentence)
        )
    assert not offenders, (
        "a .claude document says CLAUDE.md @-imports the rules or standards. It does "
        "not — see test_claude_md_has_no_at_imports. What the harness supplies on its "
        "own is not this repo's to promise either way; say the files are reachable by "
        f"path and must be opened: {offenders}"
    )


def test_the_rules_table_matches_the_rules_directory() -> None:
    """Review Focus 2 — a rule not in the index is documentation nobody reads."""
    text = _CLAUDE_MD.read_text(encoding="utf-8")
    indexed = set(re.findall(r"\.claude/rules/([a-z-]+)\.md", text))
    on_disk = {p.stem for p in _RULES.glob("*.md")}
    assert indexed == on_disk, (
        "CLAUDE.md's rules table and .claude/rules/ disagree. "
        f"Only in the table: {sorted(indexed - on_disk)}. "
        f"Only on disk: {sorted(on_disk - indexed)}."
    )


def test_the_standards_table_matches_the_standards_directory() -> None:
    """I4 — the rules half of this symmetry had a test; the standards half did not.

    A `.claude/standards/*.md` added without an index row is exactly as invisible
    as an unindexed rule, and nothing caught it: this is the same failure mode as
    test_the_rules_table_matches_the_rules_directory, one directory over.
    """
    text = _CLAUDE_MD.read_text(encoding="utf-8")
    indexed = set(re.findall(r"\.claude/standards/([a-z-]+)\.md", text))
    on_disk = {p.stem for p in _STANDARDS.glob("*.md")}
    assert indexed == on_disk, (
        "CLAUDE.md's standards table and .claude/standards/ disagree. "
        f"Only in the table: {sorted(indexed - on_disk)}. "
        f"Only on disk: {sorted(on_disk - indexed)}."
    )


def test_every_rule_declares_its_frontmatter() -> None:
    """Every rule file carries `name` matching its filename, and a description.

    The seven original rule files all do; data-contract.md shipped without one
    because CLAUDE.md's `@` imports strip frontmatter, so the convention was
    invisible to whoever wrote the new file. This is the check that makes it
    visible.
    """
    problems = []
    for path in sorted(_RULES.glob("*.md")):
        lines = path.read_text(encoding="utf-8").splitlines()
        if not lines or lines[0].strip() != "---":
            problems.append(f"{_arch.rel(path)} has no frontmatter block")
            continue
        try:
            close = lines.index("---", 1)
        except ValueError:
            problems.append(f"{_arch.rel(path)} frontmatter is never closed")
            continue
        block = lines[1:close]
        names = [line for line in block if line.startswith("name:")]
        if not names:
            problems.append(f"{_arch.rel(path)} frontmatter has no `name:`")
        elif names[0].split(":", 1)[1].strip() != path.stem:
            problems.append(
                f"{_arch.rel(path)} frontmatter name {names[0]!r} does not match its filename stem"
            )
        if not any(line.startswith("description:") for line in block):
            problems.append(f"{_arch.rel(path)} frontmatter has no `description:`")
    assert not problems, f"rule-file frontmatter is inconsistent: {problems}"


def test_every_rule_declares_its_enforcement() -> None:
    """No rule is allowed to be silently advisory."""
    undeclared = []
    for path in sorted(_RULES.glob("*.md")):
        head = path.read_text(encoding="utf-8")[:600]
        if "Enforced by:" not in head:
            undeclared.append(_arch.rel(path))
    assert not undeclared, (
        "a rule file does not declare its enforcing test. Write "
        "'Enforced by: <path>' or 'Enforced by: none — judgement.' with the reason: "
        f"{undeclared}"
    )


def test_every_declared_enforcing_test_exists() -> None:
    """A declaration pointing at nothing is worse than none.

    Resolved case-sensitively (M2, 2026-09-25 review) — `Path.exists()` gives a
    different verdict on macOS than on the Linux CI runner for the same path.
    """
    missing: list[str] = []
    for path in sorted(_RULES.glob("*.md")):
        missing.extend(
            f"{_arch.rel(path)} -> {target}"
            for target in re.findall(r"^Enforced by: (tests/\S+)", path.read_text(encoding="utf-8"), re.M)
            if not _arch.resolves_case_sensitively(_arch.ROOT, target)
        )
    assert not missing, f"a rule declares an enforcing test that does not exist: {missing}"


# Instruction documents name paths relative to several different roots, all of
# them legitimate: the repo root, the package, the package's orchestration
# subtree (`transformers/_pipeline.py`), the test root (`unit/test_x.py`), and
# the unit suite (`fabric/`). A single-root resolver reports fourteen real paths
# as dangling — measured — and a test that cries wolf fourteen times gets
# disabled. Resolve against each root before calling anything dangling.
_DOC_ROOTS = (
    "",
    "src/spark_quick_start",
    "src/spark_quick_start/orchestration",
    "tests",
    "tests/unit",
)

# `.claude/notes/` is gitignored working scratch, not instruction text, and it
# carries genuinely stale references from before the test-suite split. Scanning
# it would make this test a maintenance tax on a scratchpad.
#
# `agents` is in scope too (I5, 2026-09-25 review): two agent files reference
# `examples/databricks` and `examples/fabric`, neither of which exists, and
# nothing was scanning `.claude/agents/` at all.
# `skills` joins with F60: a skill is an instruction document like any other, and a path
# named in one must exist.
_INSTRUCTION_DOCS = ("rules", "standards", "workflow", "commands", "agents", "skills")


def _nested_claude_problems(paths: list[str]) -> list[str]:
    """Every `.claude/` below the repository root among *paths* (F60).

    This repo's choice: every scan above walks `.claude/<subdir>` at the root only, so a
    nested `src/.claude/agents/x.md` would escape the dangling-path, @-import and citation
    checks — and Claude Code documents loading a nested `.claude/skills/` for work in that
    subtree. One `.claude/`, at the root.
    """
    return sorted({f"{p.split('/.claude/')[0]}/.claude/ is a nested Claude configuration no check reads"
                   for p in paths if "/.claude/" in f"/{p}" and not p.startswith(".claude/")})


def test_no_nested_claude_configuration() -> None:
    """F60 — skills, agents, rules and commands live only in the root .claude/."""
    import subprocess

    listed = subprocess.run(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
                            cwd=_arch.ROOT, capture_output=True, check=True).stdout.decode("utf-8").split("\0")
    assert any(p.startswith(".claude/agents/") for p in listed), "git listed no agent; the scan is broken"
    problems = _nested_claude_problems([p for p in listed if p])
    assert not problems, f"nested .claude/ directories: {problems}"
    # Anti-vacuity: the root one passes, nested ones are each reported.
    assert _nested_claude_problems([".claude/skills/a/SKILL.md", "CLAUDE.md"]) == []
    assert len(_nested_claude_problems(["src/.claude/skills/x/SKILL.md", "docs/.claude/agents/y.md"])) == 2

# Both of these dangling references are extensionless directory names, which the
# suffix gate below would otherwise discard (see `known_suffixes`) — measured: a
# candidate ending in neither a known extension nor "/" was being silently
# skipped, which is precisely why these two survived undetected.
# A path can also just be a bare word pair with a slash and no extension at all —
# `try/except` in plan-vs-direct.md is English, not a path. Measured over every
# in-scope document (including `agents`): admitting extensionless candidates
# finds exactly the two real dangling references above and introduces exactly
# this one false positive. Denylist it precisely rather than keeping the whole
# suffix gate, which would also hide any *future* extensionless dangling path.
def _names_a_repo_path(candidate: str) -> bool:
    """Is *candidate* a claim about a file in this repo, rather than a path-shaped token?

    F21's fix, and the reason it is a predicate rather than another denylist entry. The
    old check asked whether a path-shaped token resolved, and too many things are
    path-shaped without claiming anything about this tree: a git branch name
    (fix/F14-..., release/fabric), a file inside an installed dependency
    (delta/__init__.py), prose like try/except. Each was worked around by de-backticking
    — twice inside the entry documenting the problem — and every fix keyed to one shape
    left the next one to be found by hand.

    A claim about this repo begins with a directory this repo has, under one of the roots
    documents legitimately write relative to. `src/`, `tests/`, `.claude/` and `docs/`
    qualify at the top level; `adapters/` and `transformers/` qualify because they exist
    under `src/spark_quick_start` and its `orchestration` subpackage, which is how rule
    files name them. Nothing named `fix`, `release`, `delta` or `try` is a directory
    anywhere, so all four are excluded by construction and not by name.

    The trade this accepts, stated because it is a real one: a dangling reference whose
    first segment is *also* absent — `nonexistent_top_level/foo.py` — is no longer
    reported. That is the rarer error by a wide margin, and it is the price of not
    reporting four kinds of non-path every time someone writes a branch name.
    """
    head = candidate.split("/", 1)[0]
    if not head or head in {".", ".."}:
        return False
    return any((_arch.ROOT / root / head).is_dir() for root in _DOC_ROOTS)


_KNOWN_NON_PATHS = frozenset({"try/except"})

# An illustrative path in a worked example is not a claim that the file exists.
# `scratchpad/` is the session-scratch prefix the workflow docs use by
# convention; it is never in the repo.
_ILLUSTRATIVE_PREFIXES = ("scratchpad/",)

# Documents also name a path on its own line inside a fenced block — that is how
# design-feature.md states its output location. The inline-backtick pattern alone
# misses it, which made this assertion green while the very reference it was
# written to catch (docs/design/) sat in the tree. Measured: adding this finds
# that one path and introduces no false positives across the in-scope documents.
_WHOLE_LINE_PATH = re.compile(r"^\s*((?:[\w.-]+/)+[\w.-]*)\s*$", re.MULTILINE)

# A path can also be a bare list item. update-docs.md lists six of them, all
# dangling, and none was caught until a reviewer read the file — the leading
# "- " breaks _WHOLE_LINE_PATH's anchor because a hyphen is not whitespace.
_LIST_ITEM_PATH = re.compile(r"^\s*[-*+]\s+((?:[\w.-]+/)+[\w.-]*)\s*$", re.MULTILINE)

# CLAUDE.md's rules and standards tables link their files rather than quoting
# them. The rules half is back-stopped by the table-vs-directory assertion; the
# standards half is not, so without this a deleted standard goes unreported.
_MD_LINK_PATH = re.compile(r"\]\(((?:[\w.-]+/)+[\w.-]*)\)")


def test_no_claude_document_references_a_path_that_does_not_exist() -> None:
    """Review Focus 3 — live today: design-feature.md writes to docs/design/.

    Scope is the instruction documents, not `.claude/notes/`. Each candidate is
    resolved against every root in _DOC_ROOTS, because rule files legitimately
    name paths relative to the package and the test tree as well as the repo.

    Four patterns feed the candidate set, because Markdown says "here is a path"
    four different ways and each rendering hid a real dangling path until someone
    went looking by hand:

    - an inline `` `path` `` span (the original form);
    - a path that occupies an entire line by itself (how design-feature.md
      states `docs/design/` inside a fenced block — the inline-only version is
      blind to this and passed clean while that exact reference sat in the tree);
    - a bare list item, `- path` / `* path` / `+ path` (how update-docs.md lists
      six paths, all dangling, invisible to the whole-line pattern because a
      leading hyphen is not whitespace and breaks its anchor);
    - a Markdown link, `[text](path)` (how CLAUDE.md's rules and standards
      tables reference their files — the rules half is back-stopped by
      test_the_rules_table_matches_the_rules_directory, the standards half is
      not, so without this a deleted standard goes unreported).

    Extensionless candidates (a bare directory name like `examples/databricks`)
    are admitted too — see `_KNOWN_NON_PATHS` above for why that is safe: it
    finds the two real dangling references in the adapter-engineer agents and
    denylists the one measured false positive rather than discarding every
    extensionless path, which is what let those two survive undetected.

    **What this check does not cover, stated because the enumeration above reads as
    complete and is not** (F17). Two renderings are invisible to it: a path embedded
    mid-line in prose without backticks, and one inside a fenced block that is not alone on
    its line. Neither has a pattern here, and a fifth pattern for mid-line prose was
    measured and rejected — it flagged ordinary sentences containing a slash. Pattern-
    matching prose for paths has a long tail, so treat this as best-effort: three rounds of
    additions each found a rendering the previous round missed (inline, then whole-line,
    then bulleted and linked), and the fourth was found by a reviewer reading files by hand.

    Since F21 the question asked is narrower and the limit is different in kind: a candidate
    is only checked when its first segment names a real directory under one of the roots
    below. So a dangling reference whose *top-level* segment is also absent —
    `nonexistent_top_level/foo.py` — is not reported at all. That is deliberate, and the
    price of not reporting four kinds of non-path (branch names, dependency paths, prose
    like try/except) every time one appears in a document.

    Resolution is deliberately case-sensitive (`_arch.resolves_case_sensitively`
    rather than `Path.exists()`), because `Path.exists()` gives a different
    verdict on macOS (case-insensitive) than on the Linux CI runner
    (case-sensitive) for the same candidate — the filesystem version of the
    "different results from the same command in two shells" trap
    tooling-and-gates.md warns about for interpreters.
    """
    inline_pattern = re.compile(r"`((?:[\w.-]+/)+[\w.-]*)`")
    docs = [_CLAUDE_MD]
    for subdir in _INSTRUCTION_DOCS:
        docs.extend(sorted((_arch.CLAUDE_DIR / subdir).rglob("*.md")))
    backlog = _arch.CLAUDE_DIR / "backlog.md"
    if backlog.exists():
        docs.append(backlog)

    dangling = []
    for doc in docs:
        text = doc.read_text(encoding="utf-8")
        candidates = (
            inline_pattern.findall(text)
            + _WHOLE_LINE_PATH.findall(text)
            + _LIST_ITEM_PATH.findall(text)
            + _MD_LINK_PATH.findall(text)
        )
        for match in candidates:
            if match in _KNOWN_NON_PATHS:
                continue
            if match.startswith(("http", *_ILLUSTRATIVE_PREFIXES)):
                continue
            if "*" in match or "<" in match:
                continue  # a glob or a placeholder, not a path
            if not _names_a_repo_path(match):
                continue  # path-shaped, but not a claim about this repo — F21
            if any(_arch.resolves_case_sensitively(_arch.ROOT / root, match) for root in _DOC_ROOTS):
                continue
            dangling.append(f"{_arch.rel(doc)} -> {match}")
    assert not dangling, (
        "a .claude document references a path that is not in the tree. An "
        "instruction pointing nowhere is an instruction that cannot be followed: "
        f"{dangling}"
    )


def test_every_backlog_entry_is_well_formed() -> None:
    """Six fields, one of five statuses. The loop reads this file."""
    text = _BACKLOG.read_text(encoding="utf-8")
    problems: list[str] = []
    for block in text.split("\n## ")[1:]:
        entry_id = block.split(" ", 1)[0]
        problems.extend(
            f"{entry_id} is missing {field}"
            for field in ("**Severity:**", "**Status:**", "**Evidence:**", "**Fix:**",
                          "**Verification:**", "**Log:**")
            if field not in block
        )
        status = re.search(r"\*\*Status:\*\*\s*(\S+)", block)
        if status and status.group(1) not in _STATUSES:
            problems.append(f"{entry_id} has status {status.group(1)!r}; allowed: {sorted(_STATUSES)}")
    assert not problems, f"malformed backlog entry: {problems}"


def test_every_backlog_id_is_unique_and_matches_the_table() -> None:
    """The fields-per-entry check never looked at identity — this is what would have.

    F13 was duplicated: a fix wave appended the corrected `wontfix` version and left the
    stale `open` original in place, so the file carried two `## F13` sections with the
    *same* title. `test_every_backlog_entry_is_well_formed` passed anyway, because it
    checks that each block has six fields and a valid status, never whether the id it
    is attached to is unique.

    Two checks, not one, because the second is strictly stronger: a duplicate section
    id is one way the table and the bodies can disagree, but a table row with no
    section (or a section with no table row) is another, and only the second check
    catches those.
    """
    text = _BACKLOG.read_text(encoding="utf-8")
    section_ids = re.findall(r"^## (F\d+)", text, re.M)
    table_ids = re.findall(r"^\| (F\d+) \|", text, re.M)

    duplicates = [
        f"{entry_id} (x{count})" for entry_id, count in Counter(section_ids).items() if count > 1
    ]
    assert not duplicates, (
        "a backlog id has more than one '## F<n>' section — delete the stale one: "
        f"{duplicates}"
    )

    table_set, body_set = set(table_ids), set(section_ids)
    only_in_table = sorted(table_set - body_set)
    only_in_body = sorted(body_set - table_set)
    assert not only_in_table and not only_in_body, (
        "the backlog's summary table and its '## F<n>' section bodies disagree on which "
        f"ids exist. Only in the table (add the missing section): {only_in_table}. "
        f"Only in the body (add the missing table row): {only_in_body}."
    )


def test_every_backlog_verification_target_exists() -> None:
    """Review Focus 4 — a wrong path sends /fix to write a test that already has a home.

    Resolved case-sensitively (M2, 2026-09-25 review) — see `_arch.resolves_case_sensitively`.
    """
    text = _BACKLOG.read_text(encoding="utf-8")
    missing = [
        target
        for target in sorted(set(re.findall(r"`(tests/[\w/]+\.py)", text)))
        if not _arch.resolves_case_sensitively(_arch.ROOT, target)
    ]
    assert not missing, f"the backlog names a test file that does not exist: {missing}"


def test_the_architecture_suite_fails_on_coverage_without_no_cov() -> None:
    """F18 — the trap itself, asserted instead of the prose describing it.

    This replaces an assertion that checked whether the strings "--no-cov" and
    "fail_under" appeared somewhere in verification.md. That could not detect a
    documentation defect — the trap could have been described backwards and it would have
    passed — and it would false-fail on any rewording of that paragraph. Substring presence
    in prose is not a checkable property, and an assertion that cannot fail for the reason
    it names is worse than no assertion, because the count says it is covered.

    The trap is real and it *is* checkable: `--cov=src` is global in pyproject's addopts
    and `fail_under = 85` applies, while the architecture suite reads source as text and
    imports almost nothing. So running it alone without `--no-cov` exits non-zero on
    coverage with every test passing — which reads as a broken suite the first time someone
    runs it, and is why verification.md documents the flag.

    Asserted on the distinction that matters: tests pass, coverage fails. A plain non-zero
    exit would also match a genuinely broken suite.
    """
    # One module, not the whole directory. `tests/architecture` contains *this* test, so a
    # subprocess over the directory re-enters it and recurses until the machine gives up —
    # which is what a first version did. test_scaffolding.py is three assertions with no
    # engine and no imports of the package under test, so it triggers the same coverage
    # floor for the same reason while being unable to re-enter.
    completed = subprocess.run(
        [
            sys.executable, "-m", "pytest",
            "tests/architecture/test_scaffolding.py", "-q", "-p", "no:cacheprovider",
        ],
        cwd=_arch.ROOT, capture_output=True, text=True, check=False, timeout=180,
    )
    output = completed.stdout + completed.stderr

    assert completed.returncode != 0, (
        "running the architecture suite without --no-cov is expected to fail on coverage; "
        "if it now passes, either fail_under or the addopts changed and verification.md's "
        f"guidance is stale. Output tail: {output[-400:]}"
    )
    assert "FAILED" not in output and " failed" not in output, (
        "the failure must be a coverage failure, not a test failure — that distinction is "
        f"the whole point of the documented flag. Output tail: {output[-400:]}"
    )
    assert "Coverage failure" in output or "fail_under" in output or "Required test" in output, (
        "expected coverage to be the stated reason for the non-zero exit. If pytest-cov's "
        f"wording changed, update this assertion rather than weakening it. Output tail: {output[-600:]}"
    )


def test_the_path_check_distinguishes_a_repo_claim_from_a_path_shaped_token() -> None:
    """F21 — six false positives in one session, each a different shape.

    The dangling-path assertion asks "is this a path that does not exist?". Too many
    things answer yes without being a claim about this repo: a git branch name
    (`fix/F14-...`), a file inside an installed dependency (delta/__init__.py), a
    sentence saying a file is *absent*, and prose like try/except. Each one was worked
    around by de-backticking, twice inside the very entry documenting the problem, and a
    convention enforced by folklore is a convention already drifting.

    The question it should ask is "is this a claim about a file in this repo?" — and a
    claim's first segment names a real directory under one of the roots documents write
    relative to. That is a positive test rather than a growing denylist of shapes, and it
    is why the four cases below are excluded by construction rather than by name.

    Asserted on the predicate directly, because the suite-level assertion passing tells
    you nothing about *why* — it passed before this change too, on de-backticked prose.
    """
    # Not repo claims: no such directory under any doc root.
    for token in ("fix/F14-measure-ms-double-rounding", "release/fabric",
                  "delta/__init__.py", "try/except", "io.delta/spark"):
        assert not _names_a_repo_path(token), f"{token!r} must not be read as a repo path"

    # Repo claims, and each resolves — these are the ones the check exists for.
    for token in ("src/spark_quick_start/__init__.py", ".claude/rules/layering.md",
                  "tests/architecture/_arch.py", "scripts/filter_platform_release.sh"):
        assert _names_a_repo_path(token), f"{token!r} must be read as a repo path"

    # A repo claim that does not resolve is exactly what must still be caught.
    assert _names_a_repo_path("src/spark_quick_start/no_such_module.py")
    assert not any(
        _arch.resolves_case_sensitively(_arch.ROOT / root, "src/spark_quick_start/no_such_module.py")
        for root in _DOC_ROOTS
    )


def _claude_md_sections() -> dict[int, int]:
    """Section number → how many ordered items that section has.

    An item is a top-level "N." line inside the section. §7's numbered agreement is the
    only section that is cited item-by-item today, but the parse is general so a new
    numbered section is covered without an edit here.
    """
    text = _CLAUDE_MD.read_text(encoding="utf-8")
    sections: dict[int, int] = {}
    current: int | None = None
    for line in text.splitlines():
        heading = re.match(r"^## (\d+)\.", line)
        if heading:
            current = int(heading.group(1))
            sections[current] = 0
            continue
        if current is not None and re.match(r"^\d+\.\s", line):
            sections[current] += 1
    return sections


def _rule_item_counts() -> dict[str, int]:
    """Rule file stem → how many top-level numbered rules it has."""
    counts = {}
    for path in sorted(_RULES.glob("*.md")):
        counts[path.stem] = len(
            re.findall(r"^\d+\.\s", path.read_text(encoding="utf-8"), re.M)
        )
    return counts


def test_every_citation_of_a_section_or_a_rule_resolves() -> None:
    """F22 — the tree cites CLAUDE.md sections and numbered rules, and nothing checked them.

    `.claude/notes/lessons.md` told every session to append a correction "see CLAUDE.md
    §5.4". §5 is the Standards table and has no ordered list, so there is no §5.4; the real
    reference is §7.4, and the working agreement had moved from §5 to §7 in a rewrite that
    left the pointer behind. A file every session is instructed to read at start carried a
    reference to nothing, inside the very sentence telling the reader to record lessons.

    `rule N` citations are the same class and were made the dominant form by F3 — roughly
    fifteen of them across the agent definitions and the standards, each keyed to a
    bullet's *position* in a list. Reordering one bullet in `platform-boundary.md` or
    `spark-delta-cost.md` silently redirects every citation past it. Ordinals are paired
    with their subject in prose so a human can spot a mismatch; this is what makes it
    machine-detectable.

    Two shapes, one assertion, because they fail the same way — a pointer that resolves to
    nothing, in a document whose only job is to be followed.
    """
    sections = _claude_md_sections()
    assert len(sections) >= 8, f"expected CLAUDE.md's numbered sections, found {sorted(sections)}"
    assert sections.get(7, 0) >= 10, (
        "§7's working agreement is the section cited item-by-item; a parse that finds no "
        f"items there is broken, not a clean result. Found {sections.get(7)}"
    )
    rules = _rule_item_counts()
    assert len(rules) >= 7, f"expected the rule files, found {sorted(rules)}"

    # backlog.md is out of scope, as it is for the dangling-path check and for the same
    # reason: an entry's Evidence must be able to quote the wrong citation it reports, and
    # F22's own entry quotes §5.4. Enforcement must not punish the prose that explains it.
    docs = [_CLAUDE_MD]
    for subdir in _INSTRUCTION_DOCS:
        docs.extend(sorted((_arch.CLAUDE_DIR / subdir).rglob("*.md")))
    docs.extend(sorted((_arch.CLAUDE_DIR / "notes").glob("*.md")))

    bad = []
    for doc in docs:
        text = doc.read_text(encoding="utf-8")
        # Scoped to citations that name CLAUDE.md. A bare §N cites the *containing*
        # document — plan-vs-direct.md numbers its own sections from §0, and CLAUDE.md's
        # own HTML comment records that its §3 was that template's §0. A first version of
        # this assertion matched every §N and reported three such references as dangling,
        # which is the over-broad matching that has cost this suite three rounds elsewhere.
        for m in re.finditer(r"CLAUDE\.md`?\s*§(\d+)(?:\.(\d+))?", text):
            section, item = int(m.group(1)), m.group(2)
            if section not in sections:
                bad.append(f"{_arch.rel(doc)}: §{section} — no such section")
            elif item is not None and int(item) > sections[section]:
                bad.append(
                    f"{_arch.rel(doc)}: §{section}.{item} — §{section} has "
                    f"{sections[section]} numbered item(s)"
                )
        for m in re.finditer(r"`?([a-z-]+)\.md`?\s+rule (\d+)", text):
            stem, n = m.group(1), int(m.group(2))
            if stem in rules and n > rules[stem]:
                bad.append(f"{_arch.rel(doc)}: {stem} rule {n} — that file has {rules[stem]}")
    assert not bad, (
        "a .claude document cites a section or rule number that does not exist. A pointer "
        f"to nothing is worse than no pointer, because it reads as verified: {bad}"
    )


_LESSONS = _arch.CLAUDE_DIR / "notes" / "lessons.md"
_VOCAB_MARKER = "**Attribution vocabulary:**"
_STRICT_LESSON = re.compile(r"^### (?P<date>[0-9]{4}-[0-9]{2}-[0-9]{2}) — \S.*$")
_LOG_LINE = re.compile(r"^- \d{4}-\d{2}-\d{2} — .*$", re.M)
_CORRECTED = re.compile(r"\bcorrected:", re.I)
_ATTRIBUTION = re.compile(r"requested by (?P<req>.+?), applied by (?P<app>.+?)\.?$")


def _attribution_vocabulary() -> list[re.Pattern[str]]:
    """F31 — the one list of who may request or apply a correction, read from lessons.md.

    Parsed rather than copied, so a new source of corrections is one edit to the
    document that defines it, and the test cannot drift from the definition. A token
    with a `<placeholder>` — `gate:<command>`, `agent:<name>` — matches its literal
    prefix followed by any non-empty value.
    """
    lines = [
        line for line in _LESSONS.read_text(encoding="utf-8").splitlines()
        if line.startswith(_VOCAB_MARKER)
    ]
    assert len(lines) == 1, (
        f"lessons.md must define the attribution vocabulary on exactly one line starting "
        f"{_VOCAB_MARKER!r}; found {len(lines)}"
    )
    tokens = re.findall(r"`([^`]+)`", lines[0])
    assert len(tokens) >= 3, f"the vocabulary line parsed to {tokens}; the parse is broken"
    return [
        re.compile(re.sub(r"<[^>]+>", r"\\S.*", re.escape(token).replace(r"\<", "<").replace(r"\>", ">")))
        for token in tokens
    ]


def _attribution_problem(value: str, vocabulary: list[re.Pattern[str]]) -> str | None:
    """Why `value` is not an admissible requester/applier, or None when it is."""
    value = value.strip().strip("`")
    if not any(p.fullmatch(value) for p in vocabulary):
        return f"{value!r} is not in the attribution vocabulary"
    return None


def _correction_lines(text: str) -> list[str]:
    """The dated Log lines that record a correction — the half of (b) that selects.

    Case-insensitive on `corrected:`, so a sentence-cased `Corrected:` is held to the
    shape rather than silently leaving the set (F31 review).
    """
    return [line for line in _LOG_LINE.findall(text) if _CORRECTED.search(line)]


def _correction_line_problem(line: str, vocabulary: list[re.Pattern[str]]) -> str | None:
    """Why a `corrected:` Log line is malformed, or None when it names both parties."""
    m = _ATTRIBUTION.search(line)
    if m is None:
        return "does not end in 'requested by <v>, applied by <v>'"
    return (_attribution_problem(m.group("req"), vocabulary)
            or _attribution_problem(m.group("app"), vocabulary))


def _lesson_problems(text: str, vocabulary: list[re.Pattern[str]]) -> list[str]:
    """Every way the lesson entries break their shape (F31, F174), or [] when none does.

    The entries live in lessons-archive.md since F193; lessons.md indexes them.

    An entry is any rendered heading whose text, markup aside, starts with a digit: at any
    level, indented, quoted, listed or underlined, so a lesson written in another shape is
    reported rather than skipped; a selector that narrows its own scope stays green over what
    it no longer reads. Each must be `### YYYY-MM-DD — <title>` with an ASCII, real date not
    in the future; it runs to the next heading of any kind; it holds exactly one
    `**Requested by:**` and one `**Applied by:**` line, each with its value on that line and in
    the vocabulary lessons.md defines; and the dates do not rise down the file, which says
    "newest first". Fenced code and HTML comments are not read: they render nothing.

    Stated limit: a heading that starts with a word, not a digit, is not an entry here.
    """
    import datetime as _dt
    lines, problems = blank_unrendered(text)
    headings = rendered_headings(lines)
    entries = [(i, h) for i, h in enumerate(headings) if re.match(r"[\W_]*\d", heading_text(h[2]))]
    if not entries:
        return [*problems, "found no dated lesson entries"]
    dates: list[_dt.date] = []
    for i, (first, level, line) in entries:
        shown = line.strip()
        m = _STRICT_LESSON.match(line)
        if level != 3:
            problems.append(f"{shown!r}: a lesson is a level-3 heading, this one is level {level}")
        elif m is None:
            problems.append(f"{shown!r}: not the '### YYYY-MM-DD — <title>' shape")
        if m is not None:
            try:
                day = _dt.date.fromisoformat(m.group("date"))
            except ValueError:
                problems.append(f"{shown!r}: {m.group('date')} is not a date")
            else:
                # A day's grace: a lesson dated in the author's zone may be ahead of UTC.
                if day > _dt.datetime.now(_dt.UTC).date() + _dt.timedelta(days=1):
                    problems.append(f"{shown!r}: {day} is in the future")
                dates.append(day)
        last = headings[i + 1][0] if i + 1 < len(headings) else len(lines)
        entry = "\n".join(lines[first:last])
        for field in ("Requested by", "Applied by"):
            found = re.findall(rf"^\*\*{field}:\*\*[ \t]*(.*)$", entry, re.M)
            if not found:
                problems.append(f"{shown!r}: missing **{field}:**")
            elif len(found) > 1:
                problems.append(f"{shown!r}: **{field}:** appears {len(found)} times")
            elif not found[0].strip():
                problems.append(f"{shown!r}: **{field}:** has no value on its line")
            elif (why := _attribution_problem(found[0], vocabulary)) is not None:
                problems.append(f"{shown!r}: {field} — {why}")
    for earlier, later in itertools.pairwise(dates):
        if later > earlier:
            problems.append(f"entries are not newest first: {later} follows {earlier}")
            break
    return problems


def test_every_lesson_names_who_requested_and_who_applied_it() -> None:
    """F31 (a) — a lesson records where the correction came from, not only what it was.

    The trigger used to be "a correction from the user". Read strictly it would have
    rejected most of the lessons on file, because a review, a gate or the author caught
    them; and none said who, so a lesson could not be weighed by its source. Every entry
    now carries both fields, drawn from the vocabulary lessons.md defines. The walk is
    `_lesson_problems`, which also holds the heading shape and the order (F174).
    """
    problems = [p for p in _lesson_problems(_LESSON_ARCHIVE.read_text(encoding="utf-8"), _attribution_vocabulary())
                if "Requested by" in p or "Applied by" in p]
    assert not problems, f"lesson without a well-formed attribution: {problems}"


def test_every_logged_correction_names_who_requested_and_who_applied_it() -> None:
    """F31 (b) — every correction lands on its finding's Log line, attributed.

    Scoped to dated Log lines containing `corrected:`, so the backlog preamble and an
    entry's Fix field may describe the shape without being held to it.

    Stated limit: this catches a correction logged in the wrong shape. It cannot catch
    one that was never logged, and nothing in the tree can.
    """
    vocabulary = _attribution_vocabulary()
    problems = [
        f"{line[:80]!r}: {why}"
        for line in _correction_lines(_BACKLOG.read_text(encoding="utf-8"))
        if (why := _correction_line_problem(line, vocabulary)) is not None
    ]
    assert not problems, f"backlog correction without a well-formed attribution: {problems}"


def test_the_attribution_check_can_fail() -> None:
    """F31 — anti-vacuity. (b) had zero lines in scope when it landed, so it passed on
    nothing; this proves both of its halves work — the selection of lines as well as the
    judgement of each. The first version tested only the judgement, so a broken selector
    would have left (b) green over an empty set (F31 review)."""
    vocabulary = _attribution_vocabulary()
    good = [
        "- 2026-09-27 — corrected: x — requested by pr-reviewer, applied by self.",
        "- 2026-09-27 — corrected: x — requested by gate:pytest tests/architecture --no-cov, applied by self.",
        "- 2026-09-27 — corrected: x — requested by user, applied by agent:test-engineer.",
    ]
    bad = [
        "- 2026-09-27 — corrected: x.",
        "- 2026-09-27 — corrected: x — requested by the review, applied by self.",
        "- 2026-09-27 — corrected: x — requested by gate:, applied by self.",
        "- 2026-09-27 — Corrected: x — requested by user, applied by me.",
    ]
    block = "\n".join(
        ["**Log:**", "- 2026-09-27 — landed, no correction.", *good, *bad,
         "`corrected: <what> — requested by <v>, applied by <v>` in a Fix field."]
    )
    assert _correction_lines(block) == good + bad
    assert [_correction_line_problem(line, vocabulary) for line in good] == [None] * len(good)
    assert all(_correction_line_problem(line, vocabulary) is not None for line in bad)


# ── F193: lessons.md is an index of lessons-archive.md ──────────────────────

_LESSON_ARCHIVE = _arch.CLAUDE_DIR / "notes" / "lessons-archive.md"
_INDEX_ROW = re.compile(r"^\| (?P<date>\d{4}-\d{2}-\d{2}) \| (?P<rule>.*?) \| (?P<held>.*?) \| "
                        r"\[(?P<title>[^\]]+)\]\(lessons-archive\.md#(?P<anchor>[^)]+)\) \|$")


def _slug(heading: str) -> str:
    """GitHub's anchor for a Markdown heading: lower case, punctuation dropped, spaces to '-'."""
    text = re.sub(r"[^\w\- ]", "", heading.strip().lower())
    return text.replace(" ", "-")


def _archive_entries(text: str) -> list[tuple[str, str, str]]:
    """(date, heading text, the Rule field unwrapped) for each `### YYYY-MM-DD — …` entry."""
    entries = []
    for block in re.split(r"(?m)^(?=### \d{4}-\d{2}-\d{2} — )", text)[1:]:
        heading = block.splitlines()[0][4:]
        rule = re.search(r"^\*\*Rule:\*\*(.*?)(?=^\*\*|\Z)", block, re.M | re.S)
        entries.append((heading[:10], heading, " ".join(rule.group(1).split()) if rule else ""))
    return entries


def _index_problems(index: str, archive: str) -> list[str]:
    """Why lessons.md's index and lessons-archive.md's entries disagree, or [] (F193).

    One index row per archive entry, in the same order (newest first): its date, its Rule
    copied verbatim (wrapped lines joined, `|` escaped as `\\|`), and a link to the entry's
    anchor. An entry without a row, a row without an entry, or a paraphrased rule is reported.
    """
    rows = [m for line in index.splitlines() if (m := _INDEX_ROW.match(line))]
    stray = [line for line in index.splitlines()
             if line.startswith("| 20") and not _INDEX_ROW.match(line)]
    problems = [f"index row not in the '| date | rule | held by | [entry](lessons-archive.md#…) |' "
                f"shape: {line[:80]!r}" for line in stray]
    entries = _archive_entries(archive)
    if not entries:
        return [*problems, "lessons-archive.md has no entries"]
    if len(rows) != len(entries):
        problems.append(f"the index has {len(rows)} rows for {len(entries)} archive entries")
    for row, (date, heading, rule) in zip(rows, entries):
        where = f"{heading[:60]!r}"
        if row["date"] != date or row["anchor"] != _slug(heading):
            problems.append(f"{where}: the index row at this position is {row['title'][:60]!r}")
            continue
        if not rule:
            problems.append(f"{where}: the archive entry has no **Rule:**")
        elif row["rule"].replace("\\|", "|") != rule:
            problems.append(f"{where}: the index rule is not the archive's Rule verbatim")
    return problems


def test_the_lessons_index_matches_the_archive() -> None:
    """F193 — lessons.md is read at every session's start, so it holds one row per lesson;
    the entries live in lessons-archive.md, and the row's rule is the entry's, verbatim."""
    problems = _index_problems(_LESSONS.read_text(encoding="utf-8"),
                               _LESSON_ARCHIVE.read_text(encoding="utf-8") if _LESSON_ARCHIVE.is_file() else "")
    assert not problems, problems


def test_the_lessons_index_check_can_fail() -> None:
    """F193 — anti-vacuity: each disagreement is reported, the agreeing pair is not."""
    archive = ("# Archive\n\n### 2026-10-02 — Second\n\n**Rule:** Do B,\nalways.\n**Requested by:** self\n\n"
               "### 2026-10-01 — First | pipe\n\n**Rule:** Do A | then C.\n**Requested by:** self\n")
    good = ("| Date | Rule | Held by | Entry |\n|---|---|---|---|\n"
            "| 2026-10-02 | Do B, always. |  | [Second](lessons-archive.md#2026-10-02--second) |\n"
            "| 2026-10-01 | Do A \\| then C. | x | [First \\| pipe](lessons-archive.md#2026-10-01--first--pipe) |\n")
    assert _index_problems(good, archive) == [], _index_problems(good, archive)
    second, first = good.splitlines()[2], good.splitlines()[3]
    mutations = {
        "a paraphrased rule": good.replace("Do B, always.", "Do B."),
        "a missing row": good.replace(first + "\n", ""),
        "rows out of order": good.replace(second, "TMP").replace(first, second).replace("TMP", first),
        "a row in another shape": good.replace("| 2026-10-02 | Do B", "| 2026-10-02 |Do B"),
        "an extra row": good + first + "\n",
    }
    wrong = {k: v for k, v in mutations.items() if not _index_problems(v, archive)}
    assert not wrong, f"these were not reported: {sorted(wrong)}"


# ── F193: todo.md is rotated, not grown ─────────────────────────────────────

_TODO = _arch.CLAUDE_DIR / "notes" / "todo.md"
_TODO_BUDGET = 250


def test_todo_md_stays_within_its_budget() -> None:
    """F193 — todo.md had grown to 645 lines of closed sections; /wrap-up now moves them to
    a dated, tracked todo-archive, and this keeps the rotation from being forgotten."""
    lines = len(_TODO.read_text(encoding="utf-8").splitlines())
    assert lines <= _TODO_BUDGET, (
        f"todo.md is {lines} lines, over its {_TODO_BUDGET}; move closed sections to "
        ".claude/notes/todo-archive-<date>.md, as /wrap-up Step 4 says")


def test_every_dated_lesson_is_a_level_3_heading_and_the_file_runs_newest_first() -> None:
    """F174 — a lesson written at another heading level, or out of order, is reported.

    Both lessons of 2026-10-01 opened with `##`, and the old selector read only `###`, so
    the attribution check walked past them in silence: a check that narrows its own scope
    and stays green. lessons.md also says "newest first" while its entries ran oldest first.
    """
    problems = _lesson_problems(_LESSON_ARCHIVE.read_text(encoding="utf-8"), _attribution_vocabulary())
    assert not problems, f"lessons-archive.md: {problems}"


_GOOD_LESSON = "{h} {d} — A title\n\nbody\n**Requested by:** `user`\n**Applied by:** `self`\n"


def test_the_lesson_shape_check_can_fail() -> None:
    """F174 — anti-vacuity: each near-miss is reported, and a well-formed file is not."""
    vocabulary = _attribution_vocabulary()

    def lessons(*entries: tuple[str, str]) -> str:
        return "# Lessons\n\n## Entries\n\n" + "\n".join(_GOOD_LESSON.format(h=h, d=d) for h, d in entries)

    assert _lesson_problems(lessons(("###", "2026-10-02"), ("###", "2026-10-01")), vocabulary) == []
    assert _lesson_problems(lessons(("###", "2026-10-01"), ("###", "2026-10-01")), vocabulary) == []
    for level in ("#", "##", "####", "#####", "######"):
        found = _lesson_problems(lessons(("###", "2026-10-02"), (level, "2026-10-01")), vocabulary)
        assert len(found) == 1 and "level" in found[0], (level, found)
    out_of_order = _lesson_problems(lessons(("###", "2026-09-26"), ("###", "2026-10-01")), vocabulary)
    assert len(out_of_order) == 1 and "newest first" in out_of_order[0], out_of_order
    hyphen = _lesson_problems(lessons(("###", "2026-10-02")).replace(" — ", " - "), vocabulary)
    assert len(hyphen) == 1 and "shape" in hyphen[0], hyphen
    unattributed = _lesson_problems(lessons(("###", "2026-10-02")).replace("`self`", "me"), vocabulary)
    assert len(unattributed) == 1 and "Applied by" in unattributed[0], unattributed
    assert _lesson_problems("# Lessons\n\nno entries\n", vocabulary) == ["found no dated lesson entries"]


_A = "**Requested by:** `user`\n**Applied by:** `self`\n"


@pytest.mark.parametrize("entry,fragment", [
    (" ### 2026-10-05 — indented\n" + _A, "not the"),
    ("   ### 2026-10-01 — no attribution\n\nbody\n", "missing **Requested by:**"),
    ("2026-10-01 — setext two\n---\n\n" + _A, "level 2"),
    ("2026-10-01 — setext one\n===\n\n" + _A, "level 1"),
    ("> ### 2026-10-01 — quoted\n", "not the"),
    ("- ### 2026-10-01 — listed\n", "not the"),
    ("### 2026-10-01a — x\n" + _A, "not the"),
    ("### 2026-10-01_x — x\n" + _A, "not the"),
    ("### 2026-10-011 — x\n" + _A, "not the"),
    ("### 2026/10/01 — x\n" + _A, "not the"),
    ("### 2026.10.01 — x\n" + _A, "not the"),
    ("### **2026-10-01** — x\n" + _A, "not the"),
    ("### `2026-10-01` — x\n" + _A, "not the"),
    ("### [2026-10-01] — x\n" + _A, "not the"),
    ("### \uff12\uff10\uff12\uff16-\uff11\uff10-\uff10\uff12 — fw\n" + _A, "not the"),
    ("### 2026-13-45 — not a date\n" + _A, "is not a date"),
    ("### 2026-10-01 — \n" + _A, "not the"),
    ("### 2026-10-01 — hidden\n<!--\n" + _A + "-->\n", "missing **Requested by:**"),
    ("### 2026-10-01 — split\n**Requested by:**\n\n`user`\n**Applied by:** `self`\n", "no value"),
    ("### 2026-10-01 — twice\n" + _A + "**Requested by:** me\n", "appears 2 times"),
    ("### 2026-10-01 — borrows\n\n## Appendix\n" + _A, "missing **Requested by:**"),
    ("### 2026-10-01 — a\n" + _A + "````md\n```python\n````\n### 2026-09-30 — b\n", "missing **Requested by:**"),
    ("### 2026-10-01 — fenced attribution\n```\n" + _A + "```\n", "missing **Requested by:**"),
    ("### 2026-10-01 — a `<!--` in code\n" + _A + "### 2026-09-30 — c\n", "— c': missing **Requested by:**"),
    ("### 2026-10-01 — unclosed\n" + _A + "```\n### 2026-09-30 — d\n", "never closed"),
    ("### 2026-10-01 — a\n" + _A + "````\n```\n````\n### 2026-09-30 — b\n", "— b': missing **Requested by:**"),
    ("### 2026-10-01 — a\nnote `<!--`\n" + _A + "### 2026-09-30 — c\n\n### 2026-09-29 — z `-->`\n" + _A,
     "— c': missing **Requested by:**"),
    ("### 2026-10-01 — a\n" + _A + "```inline``` is code, not a fence\n### 2026-09-30 — e\n",
     "— e': missing **Requested by:**"),
], ids=lambda e: e.split("\n")[0][:40] if "\n" in e else e)
def test_the_lesson_shape_check_reports_each_near_miss(entry: str, fragment: str) -> None:
    """F174 — check-adversary's bypasses and the review's, each reported by the check it targets.

    Placed after a good entry dated 2026-10-02, so a skipped entry would leave the file clean;
    each case names the problem it must raise, so a deleted branch is caught by its own case."""
    text = "# Lessons\n\n## Entries\n\n### 2026-10-02 — good\n\n" + _A + "\n" + entry
    problems = _lesson_problems(text, _attribution_vocabulary())
    assert any(fragment in p for p in problems), (entry, problems)


def test_a_lesson_dated_in_the_future_is_reported() -> None:
    """F174 review — first in the file, so only the future-date branch can report it."""
    text = "# Lessons\n\n### 9999-12-31 — the future\n" + _A + "\n### 2026-10-02 — good\n" + _A
    assert any("in the future" in p for p in _lesson_problems(text, _attribution_vocabulary()))


@pytest.mark.parametrize("text", [
    "# Lessons\n\n```\n### 2026-01-01 — example\n```\n\n### 2026-10-02 — good\n" + _A,
    "# Lessons\n\n~~~\n### 2026-01-01 — example\n~~~\n\n### 2026-10-02 — good\n" + _A,
    "# Lessons\n<!--\n## 2026-01-01 draft\n-->\n### 2026-10-02 — good\n" + _A,
    "# Lessons\n\n### 2026-10-02 — real\n\nexample:\n```\n### 2026-10-01 — quoted\n```\n" + _A,
    "\ufeff# Lessons\n\n### 2026-10-02 — good ###\r\n" + _A.replace("\n", "\r\n"),
], ids=["backtick fence", "tilde fence", "html comment", "fence inside an entry", "bom, crlf, closing hashes"])
def test_the_lesson_shape_check_leaves_valid_files_alone(text: str) -> None:
    """F174 — check-adversary's false alarms: what renders nothing is not an entry."""
    assert _lesson_problems(text, _attribution_vocabulary()) == []


# ── F175: the mypy baseline's size is stated once ────────────────────────────

_BASELINE_FILE = _arch.ROOT / "mypy-baseline.txt"
# A number an author could give as the baseline's size: two or more digits, value checked by the
# caller. Not part of a word, an F-id, an issue (#34), a section (§8), a line reference (:57), a
# version (3.11), a date (2026-09-26) or a percentage (80%).
_SIZE_LIKE_NUMBER = re.compile(
    r"(?<![^\W_]|[#§:./,\-\u2011])(?:\d{1,3}(?:,\d{3})+|\d{2,})(?![^\W_]|%|[.,/:]\d|[-\u2011]\d)")
_SECTION_8_CLAIM = re.compile(r"\*\*(?P<errors>\d[\d,]*) errors\*\* across (?P<entries>\d[\d,]*) baseline lines\b")
_BASELINE_WORD = re.compile(r"base[\s\-\u2011]?line", re.I)


def _baseline_total() -> tuple[int, int, int | None]:
    """(errors, entries, the `# Total:` header's number or None) the baseline file records,
    read from its lines rather than from mypy."""
    text = _BASELINE_FILE.read_text(encoding="utf-8")
    entries = [line for line in text.splitlines() if line.strip() and not line.lstrip().startswith("#")]
    header = re.search(r"^# Total: (\d+)\s*$", text, re.M)
    return (sum(int(line.split(" ", 1)[0]) for line in entries), len(entries),
            int(header.group(1)) if header else None)


_LIST_ITEM = re.compile(r"^ {0,3}(?:[-*+]|\d+[.)])[ \t]")


def _blocks(text: str) -> list[str]:
    """The units a reader takes as one statement: a paragraph, one list item with its
    continuation lines, one table row, one line of fenced code. A number and the word baseline
    count together across a wrapped line, never across two items, rows or code lines."""
    blocks: list[str] = []
    current: list[str] = []
    fence: str | None = None

    def close() -> None:
        if current:
            blocks.append("\n".join(current))
            current.clear()

    for line in text.splitlines():
        if (m := FENCE_OPEN.match(line)) is not None and (fence is None or m.group("run")[0] == fence):
            close()
            fence = None if fence is not None else m.group("run")[0]
            continue
        if fence is not None or line.lstrip().startswith("|"):
            close()
            blocks.append(line)
        elif not line.strip():
            close()
        elif _LIST_ITEM.match(line):
            close()
            current.append(line)
        else:
            current.append(line)
    close()
    return blocks


def _baseline_count_claims(text: str) -> list[str]:
    """Each block of *text* that names the baseline and holds a number of 50 or more (F175).

    A block is what `_blocks` returns: a paragraph, one list item, one table row, one line of
    fenced code. In these documents "baseline" is the mypy baseline, and its size is in the
    hundreds, so any such number beside the word is read as a statement of it, however it is
    phrased: before or after the noun, with any adjective or emphasis, across a wrapped line.

    Stated limits. Missed: a number in words or below 50; a number and the word in different
    blocks (a heading over the paragraph that gives the number, a table header over a data
    row, a list item continued after a blank line, a pronoun pointing back); prose after a
    fence `_blocks` misreads, whose lines become blocks of their own. Reported though no size
    is stated: any other number of 50 or more beside the word, such as a year written alone or
    a threshold; the remedy is to move it to its own sentence. The test asserts that the
    baseline still holds 50 or more errors, the day it does not, the threshold is revisited.
    """
    claims = []
    for block in _blocks(text):
        if not _BASELINE_WORD.search(block):
            continue
        numbers = [int(m.group(0).replace(",", "")) for m in _SIZE_LIKE_NUMBER.finditer(block)]
        if any(n >= 50 for n in numbers):
            claims.append(" ".join(block.split()))
    return claims


def _section(text: str, number: int) -> tuple[str, str]:
    """(the `## N.` section of a markdown file, the rest of the file)."""
    m = re.search(rf"^## {number}\..*?(?=^## |\Z)", text, re.M | re.S)
    if m is None:
        return "", text
    return m.group(0), text[:m.start()] + text[m.end():]


def _instruction_docs() -> list:
    """CLAUDE.md and every .md under .claude/, except history: the backlog, where a past number
    in a Log line is a record, not a claim, and the notes, which are gitignored scratch, so a
    local run and CI would read different files. `lessons.md` is the exception kept: it is
    tracked, and CLAUDE.md §7.4 makes it reading at every session's start; so is its archive
    (F193). The todo archives are tracked too, but they are history, as the backlog is."""
    backlog, notes = _arch.CLAUDE_DIR / "backlog.md", _arch.CLAUDE_DIR / "notes"
    return [_CLAUDE_MD, *(d for d in sorted(_arch.CLAUDE_DIR.rglob("*.md"))
                         if d != backlog and (notes not in d.parents or d in (_LESSONS, _LESSON_ARCHIVE)))]


def _baseline_size_problems(docs: dict[str, str], errors: int, entries: int, header: int | None) -> list[str]:
    """Why the instructions' statements of the baseline's size are wrong, or [] (F175)."""
    problems = []
    if header != errors:
        problems.append(f"mypy-baseline.txt: '# Total: {header}' but its counts sum to {errors}")
    for name, text in docs.items():
        if name == "CLAUDE.md":
            section_8, text = _section(text, 8)
            found = list(_SECTION_8_CLAIM.finditer(section_8))
            if len(found) != 1:
                problems.append(f"CLAUDE.md §8 states '**N errors** across M baseline lines' {len(found)} "
                                "times; once")
            else:
                stated = (int(found[0].group("errors").replace(",", "")),
                          int(found[0].group("entries").replace(",", "")))
                if stated != (errors, entries):
                    problems.append(f"CLAUDE.md §8 says {stated[0]} errors across {stated[1]} baseline "
                                    f"lines; the baseline file records {errors} across {entries}")
        problems += [f"{name}: {claim[:100]!r} — state the size only in CLAUDE.md §8, or point at "
                     "scripts/mypy_baseline.py" for claim in _baseline_count_claims(text)]
    return problems


def test_the_baseline_size_is_stated_only_in_claude_md_section_8_and_matches_the_file() -> None:
    """F175 — four instruction files stated the baseline's size, as 201 and as 255.

    `python scripts/mypy_baseline.py` is the authority (CLAUDE.md §8 says so); one statement of
    the number summarises it, in §8, and it must equal what the baseline file records.
    """
    docs = {_arch.rel(d) if d != _CLAUDE_MD else "CLAUDE.md": d.read_text(encoding="utf-8")
            for d in _instruction_docs()}
    assert _LESSONS in _instruction_docs()
    errors, entries, header = _baseline_total()
    assert errors >= 50, (
        f"the baseline now holds {errors} errors, below the 50 _baseline_count_claims treats as a "
        "possible statement of its size; lower the threshold, or the check misses a stale number")
    assert not (problems := _baseline_size_problems(docs, errors, entries, header)), problems


_S8 = "## 8. Known debt\n\n| mypy baseline — **201 errors** across 132 baseline lines; 4 of the 201 fixed, F15, issue #34 |\n"
_CLAUDE_OK = "# Claude\n\n## 1. What\n\nText.\n\n" + _S8


def _size_problems(claude: str = _CLAUDE_OK, other: str = "", header: int | None = 201) -> list[str]:
    return _baseline_size_problems({"CLAUDE.md": claude, ".claude/x.md": other}, 201, 132, header)


@pytest.mark.parametrize("other", [
    "The 255-error baseline is expected.", "baseline: 255", "errors in the baseline: 255",
    "| mypy baseline | 255 |", "the 255\nerrors in the baseline", "carries a 255-error\n  baseline. Keep it clean.",
    "255 known errors in the baseline", "the 255-entry baseline", "the baseline holds 255 findings",
    "`255` errors in the baseline", "[255](mypy-baseline.txt) errors, the baseline", "<b>255</b> baseline errors",
    "the 255‑error baseline", "255+ errors in the baseline", "the base-line holds 255 errors",
    "the base line holds 255 errors", "baseline total = 1,255", "the baseline holds _255_ errors",
], ids=lambda o: o.replace("\n", " ")[:40])
def test_the_baseline_size_check_reports_each_statement_outside_section_8(other: str) -> None:
    """F175 — check-adversary's phrasings, each a statement of the size in another document."""
    assert any(p.startswith(".claude/x.md:") for p in _size_problems(other=other)), other


@pytest.mark.parametrize("claude,fragment", [
    (_CLAUDE_OK.replace("**201 errors**", "**255 errors**"), "says 255 errors"),
    (_CLAUDE_OK.replace("**201 errors**", "**2010 errors**"), "says 2010 errors"),
    (_CLAUDE_OK.replace("across 132", "across 1132"), "across 1132"),
    (_CLAUDE_OK.replace(_S8, "## 8. Known debt\n\nNone.\n").replace("Text.", "The mypy baseline: **201 errors** across 132 baseline lines."),
     "0 times"),
    (_CLAUDE_OK.replace("Text.", "The mypy baseline holds 255 known errors."), "CLAUDE.md: "),
], ids=["wrong number", "superstring number", "superstring entries", "moved out of section 8", "second statement"])
def test_the_baseline_size_check_holds_section_8_to_the_file(claude: str, fragment: str) -> None:
    """F175 — the one statement is in §8, is the canonical phrase, and its numbers are the file's."""
    problems = _size_problems(claude=claude)
    assert any(fragment in p for p in problems), problems


def test_the_baseline_size_check_compares_the_files_own_header() -> None:
    assert any("# Total: 255" in p for p in _size_problems(header=255))


@pytest.mark.parametrize("other", [
    "exit code 1 errors out when the baseline is stale", "2026-09-26 errors: baseline regenerated",
    "the baseline catches new errors; step 3 errors are fatal", "if 0 errors remain, delete the baseline",
    "see §8; issue #34; F15 — the baseline", "it clears ~80% of the baseline", "line :57 of the baseline",
    "python 3.11 and the baseline", "- coverage gate fail_under=85\n- the baseline is the authority",
    "| coverage | 85 |\n| baseline | scripts/mypy_baseline.py |",
    "```\npytest  # fail_under=85\npython scripts/mypy_baseline.py\n```",
], ids=lambda o: o.replace("\n", " ")[:40])
def test_the_baseline_size_check_leaves_other_numbers_alone(other: str) -> None:
    """F175 — check-adversary's false alarms, and the measured ones: list items, table rows and
    code lines are separate statements."""
    assert _size_problems(other=other) == []


# ── F171: the Python floor is one number, stated where authors look ─────────


_PYPROJECT = _arch.ROOT / "pyproject.toml"
_API_AND_TYPING = _arch.CLAUDE_DIR / "standards" / "api-and-typing.md"


# Files that would win over pyproject.toml: ruff reads ruff.toml and .ruff.toml before it, and
# mypy reads mypy.ini and .mypy.ini before it. Neither reads tox.ini, and mypy reaches setup.cfg
# only when pyproject.toml has no [tool.mypy], which it has (F171 review).
_CONFIG_OVERRIDES = ("ruff.toml", ".ruff.toml", "mypy.ini", ".mypy.ini")
_SPEC_CLAUSE = re.compile(r"^(?P<op>>=|<=|==|!=|~=|>|<)\s*(?P<v>\d+(?:\.\d+)*)$")
_PYTHON_VERSION_MENTION = re.compile(r"\bPython\s+(\d+)\.(\d+)\b", re.I)


def _version(text: str) -> tuple[int, ...]:
    parts = [int(p) for p in text.split(".")]
    while len(parts) > 1 and parts[-1] == 0:
        parts.pop()
    return tuple(parts)


def _floor(requires: object) -> tuple[int, int] | None:
    """`requires-python`'s floor, (3, N), or None when it is not one readable lower bound.

    Clauses in any order; exactly one `>=` or `~=` lower bound of the form 3.N (a trailing .0
    is the same version); `<` and `<=` upper bounds above it; `!=` exclusions allowed. An
    exclusive `>` lower bound names no floor, and is not read as one.
    """
    if not isinstance(requires, str):
        return None
    clauses = [c.strip() for c in requires.split(",")]
    parsed = [_SPEC_CLAUSE.match(c) for c in clauses]
    if not clauses or not all(parsed):
        return None
    lower = [_version(m.group("v")) for m in parsed if m and m.group("op") in (">=", "~=")]
    if len(lower) != 1 or len(lower[0]) != 2 or lower[0][0] != 3:
        return None
    if any(m and m.group("op") in (">", "==") for m in parsed):
        return None
    if any(m and m.group("op") in ("<", "<=") and _version(m.group("v")) <= lower[0] for m in parsed):
        return None
    return lower[0][0], lower[0][1]


def _python_floor_problems(pyproject: dict, standard: str, root_files: set[str],
                           nested_ruff: list[str] | None = None) -> list[str]:
    """Why the floor's statements disagree, or [] (F171).

    The floor is `requires-python`'s one lower bound, 3.N. mypy's `python_version` must be
    `"3.N"`, ruff's `target-version` `"py3N"`; no ruff or mypy configuration file may sit beside
    pyproject.toml to override them, and no ruff configuration below it (*nested_ruff*: ruff
    takes the configuration closest to a file, so one in tests/ would retarget all of tests/); the classifiers name 3.N and nothing older. The typing
    standard, read as rendered (comments and fenced code removed, emphasis and line wraps
    flattened), says "The floor is Python 3.N" once and names no other "Python X.Y". A
    `requires-python` this cannot read is a problem, not a pass.
    """
    problems: list[str] = []
    requires = pyproject.get("project", {}).get("requires-python")
    floor = _floor(requires)
    if floor is None:
        return [f"requires-python {requires!r} is not one '>=3.N' floor with upper bounds above it"]
    major, minor = floor
    tool = pyproject.get("tool", {})
    if (mypy := tool.get("mypy", {}).get("python_version")) != f"{major}.{minor}":
        problems.append(f"[tool.mypy] python_version is {mypy!r}; requires-python says {major}.{minor}")
    if (ruff := tool.get("ruff", {}).get("target-version")) != f"py{major}{minor}":
        problems.append(f"[tool.ruff] target-version is {ruff!r}; requires-python says py{major}{minor}")
    problems += [f"{name} overrides pyproject.toml's ruff or mypy settings; keep them in pyproject.toml"
                 for name in _CONFIG_OVERRIDES if name in root_files]
    problems += [f"{path} is a ruff configuration below the root; it overrides target-version for its "
                 "subtree" for path in nested_ruff or []]
    classifiers = [re.fullmatch(r"Programming Language :: Python :: (\d+)\.(\d+)", c)
                   for c in pyproject.get("project", {}).get("classifiers", [])]
    named = sorted((int(m.group(1)), int(m.group(2))) for m in classifiers if m)
    if not named or named[0] != floor:
        problems.append(f"the classifiers name {named}; the oldest must be the floor {major}.{minor}")
    lines, unreadable = blank_unrendered(standard)
    rendered = " ".join(re.sub(r"[*_`]", "", "\n".join(lines)).split())
    problems += [f"api-and-typing.md: {u}" for u in unreadable]
    stated = re.findall(r"The floor is Python (\d+)\.(\d+)\b", rendered)
    if stated != [(str(major), str(minor))]:
        problems.append(f"api-and-typing.md must say 'The floor is Python {major}.{minor}' once; it says "
                        f"{['.'.join(v) for v in stated] or 'nothing'}")
    others = sorted({f"{x}.{y}" for x, y in _PYTHON_VERSION_MENTION.findall(rendered)
                     if (int(x), int(y)) != floor})
    if others:
        problems.append(f"api-and-typing.md names Python {others}; only the floor may be written "
                        "'Python X.Y', a newer feature's version as a bare number")
    return problems


def test_the_python_floor_is_one_version_everywhere_it_is_stated() -> None:
    """F171 — `requires-python`, mypy's `python_version` and ruff's `target-version` name one
    floor, and the typing standard says it, so an author knows which syntax is in bounds."""
    pyproject = tomllib.loads(_PYPROJECT.read_text(encoding="utf-8"))
    root_files = {p.name for p in _arch.ROOT.iterdir()}
    nested = [_arch.rel(p) for tree in ("src", "tests") for p in sorted((_arch.ROOT / tree).rglob("*"))
              if p.name in ("ruff.toml", ".ruff.toml")
              or (p.name == "pyproject.toml" and "[tool.ruff" in p.read_text(encoding="utf-8"))]
    problems = _python_floor_problems(pyproject, _API_AND_TYPING.read_text(encoding="utf-8"), root_files, nested)
    assert not problems, problems
    # F191: the owner set the floor to 3.11 on 2026-10-02, the version CI runs.
    floor = _floor(pyproject["project"]["requires-python"])
    assert floor == (3, 11), f"the floor is {floor}; the owner set it to 3.11 (F191)"


_FLOOR_PYPROJECT = {
    "project": {"requires-python": ">=3.10", "classifiers": [
        "Programming Language :: Python :: 3", "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11"]},
    "tool": {"mypy": {"python_version": "3.10"}, "ruff": {"target-version": "py310"}},
}
_FLOOR_STANDARD = "# API\n\n- The floor is Python 3.10: no `ExceptionGroup` (3.11).\n"


def _floor_case(requires=None, mypy=None, ruff=None, standard=_FLOOR_STANDARD, root=(), classifiers=None,
                nested=None):
    import copy
    py = copy.deepcopy(_FLOOR_PYPROJECT)
    if requires is not None:
        py["project"]["requires-python"] = requires
    if mypy is not None:
        py["tool"]["mypy"]["python_version"] = mypy
    if ruff is not None:
        py["tool"]["ruff"]["target-version"] = ruff
    if classifiers is not None:
        py["project"]["classifiers"] = classifiers
    return _python_floor_problems(py, standard, set(root), nested)


def test_the_python_floor_check_passes_a_consistent_project() -> None:
    assert _floor_case() == []


@pytest.mark.parametrize("kwargs", [
    {"requires": ">=3.10.0"}, {"requires": "<4,>=3.10"}, {"requires": ">=3.10,<=3.13"},
    {"requires": ">=3.10,!=3.11.1"}, {"requires": "~=3.10"}, {"requires": " >= 3.10 "},
    {"standard": "- The floor is\n  Python 3.10.\n"}, {"standard": "- The floor is **Python 3.10**.\n"},
    {"standard": "- The floor is  Python 3.10.\n"}, {"root": ["setup.cfg", "tox.ini"]},
], ids=lambda k: str(k)[:40])
def test_the_python_floor_check_accepts_equivalent_spellings(kwargs) -> None:
    """F171 — check-adversary's false alarms: valid PEP 440 forms and rendered-same sentences."""
    assert _floor_case(**kwargs) == [], kwargs


@pytest.mark.parametrize("kwargs,fragment", [
    ({"requires": ">=3.10,<3.10"}, "requires-python"), ({"requires": ">=3.10,<3.9"}, "requires-python"),
    ({"requires": ">=3.10,<3"}, "requires-python"), ({"requires": ">=3.10,<..."}, "requires-python"),
    ({"requires": ">3.9"}, "requires-python"), ({"requires": ">=3.10,>3.9"}, "requires-python"),
    ({"requires": ">=3.10,==3.10"}, "requires-python"), ({"requires": ">=3.10.*"}, "requires-python"),
    ({"requires": "==3.10.*"}, "requires-python"), ({"requires": ">=3.10; extra"}, "requires-python"),
    ({"requires": ["3.10"]}, "requires-python"), ({"requires": ">=4.0"}, "requires-python"),
    ({"mypy": 3.1}, "python_version"), ({"mypy": "3.11"}, "python_version"),
    ({"ruff": "py311"}, "target-version"), ({"ruff": "PY310"}, "target-version"),
    ({"root": ["ruff.toml"]}, "ruff.toml overrides"), ({"root": ["mypy.ini"]}, "mypy.ini overrides"),
    ({"root": [".ruff.toml"]}, ".ruff.toml overrides"), ({"root": [".mypy.ini"]}, ".mypy.ini overrides"),
    ({"nested": ["tests/ruff.toml"]}, "below the root"),
    ({"classifiers": ["Programming Language :: Python :: 3.9", "Programming Language :: Python :: 3.10"]},
     "classifiers"),
    ({"standard": _FLOOR_STANDARD + "the floor is Python 3.9\n"}, "names Python ['3.9']"),
    ({"standard": _FLOOR_STANDARD + "Python 3.9 is the minimum supported version.\n"}, "names Python"),
    ({"standard": _FLOOR_STANDARD + "The floor is Python 4.0\n"}, "once"),
    ({"standard": "<!-- The floor is Python 3.10 -->\nWe target Python 3.8.\n"}, "once"),
    ({"standard": "```\nThe floor is Python 3.10\n```\n"}, "once"),
    ({"standard": "nothing here"}, "once"),
], ids=lambda k: str(k)[:44])
def test_the_python_floor_check_reports_each_disagreement(kwargs, fragment) -> None:
    """F171 — check-adversary's bypasses, each reported by the branch it targets."""
    problems = _floor_case(**kwargs)
    assert any(fragment in p for p in problems), (kwargs, problems)


# ── F172: the testing standard agrees with the test-classification rule ─────

_TESTING_STANDARD = _arch.CLAUDE_DIR / "standards" / "testing.md"


_NUMBER_WORDS = {"single": 1, "one": 1, "two": 2, "pair": 2, "both": 2, "dual": 2, "twin": 2,
                 "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 9,
                 "ten": 10, "eleven": 11, "twelve": 12}
# A negation within three words before the session or the fixture: "no SparkSession", "never
# create a SparkSession", "must not use the spark fixture"; not one elsewhere in the sentence.
_NEGATED_SPARK = re.compile(
    r"\b(?:no|never|not|without|forbidden|neither|nor)\b(?:\W+\w+){0,3}?\W+"
    r"(?:Spark[\s-]?Sessions?\b|spark\s+fixture\b|fixture\s+spark\b)", re.I)


def _standard_as_read(standard: str) -> tuple[str, list[str]]:
    """The standard's text as a reader sees it, flattened, and what could not be read: frontmatter,
    fenced and indented code, comments, link destinations, reference definitions and HTML tags
    removed, emphasis and backticks dropped (underscores kept: they are part of names)."""
    lines, problems = blank_unrendered(strip_frontmatter(standard))
    text = "\n".join(blank_indented_code(lines))
    text = re.sub(r"^ {0,3}\[[^\]]+\]:.*$", "", text, flags=re.M)
    text = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = re.sub(r"<[^>]+>", " ", text)
    return " ".join(re.sub(r"[*`]", "", text).split()), problems


def _suite_problems(standard: str, suites: list[str]) -> list[str]:
    """Why the testing standard disagrees with the suites on disk and the rule, or [] (F172).

    Read as `_standard_as_read` gives it: the standard names each suite's directory,
    `tests/<suite>/`, as a path of its own (not inside a URL or a longer name, and not one of its
    subdirectories); every count of suites in the plural form "N suites", with the number in
    digits or a word, the nearer of the two words before "suites" ("the three suites", "a pair of
    suites"), is the number on disk; and no sentence that mentions a unit test pairs `SparkSession` or the `spark`
    fixture with a negation, which the test-classification rule's sanctioning of a real
    SparkSession in a unit would contradict.

    Stated limits. Not read: a count in the singular or in other words ("a single directory per
    suite", "the suite count is 2"), a prohibition without those names ("unit tests start no
    engine") or with the negation more than three words before them, and meaning: a true sentence such as "two suites block merge" is read as a count,
    and the remedy is to say which two. A suite directory with an underscore is matched as written.
    """
    rendered, problems = _standard_as_read(standard)
    problems = [f"testing.md: {p}" for p in problems]
    problems += [f"testing.md does not name `tests/{suite}/`" for suite in suites
                 if not re.search(rf"(?<![\w/:.@-])tests/{re.escape(suite)}(?:/(?![\w-]+/)|(?![\w/-]))", rendered)]
    for m in re.finditer(r"\bsuites\b", rendered, re.I):
        before = re.findall(r"[\w-]+", rendered[max(0, m.start() - 60):m.start()])[-2:]
        for word in reversed(before):
            count = _NUMBER_WORDS.get(word.lower()) or (int(word) if word.isdigit() and len(word) < 4 else None)
            if count is not None:
                if count != len(suites):
                    problems.append(f"testing.md counts {word} suites; the tree has {len(suites)}")
                break
    for sentence in re.split(r"(?<=[.!?|])\s+", rendered):
        if re.search(r"\bunit\b", sentence, re.I) and _NEGATED_SPARK.search(sentence):
            problems.append(f"testing.md: {sentence[:90]!r} forbids a SparkSession in unit tests; "
                            "test-classification.md sanctions one (Spark is an engine, not a unit of ours)")
    return problems


def test_the_testing_standard_names_the_suites_the_tree_has() -> None:
    """F172 — testing.md said "Two suites" and "no SparkSession" in unit tests after
    test-classification.md defined three suites and sanctioned a real SparkSession in a unit."""
    suites = sorted(d.name for d in (_arch.ROOT / "tests").iterdir()
                    if d.is_dir() and any(d.rglob("test_*.py")))
    assert suites == ["architecture", "integration", "unit"], suites
    assert not (problems := _suite_problems(_TESTING_STANDARD.read_text(encoding="utf-8"), suites)), problems


_SUITES = ["architecture", "integration", "unit"]
_NAMED = "Three suites: `tests/unit/`, `tests/integration/` and `tests/architecture/`."


@pytest.mark.parametrize("text", [
    _NAMED, _NAMED + " One suite per directory.", _NAMED + " Run one suite at a time.",
    _NAMED + " Each of the three suites has a conftest.", _NAMED + " Two of the three suites run Spark.",
    _NAMED + " Since 2025 suites are split by directory.", _NAMED + " The fast suite is -m 'not spark'.",
    _NAMED + " Architecture tests start no SparkSession.",
    "Three suites: tests/unit/test_x.py, `tests/integration/`, tests/architecture/.",
    _NAMED + " A unit test may take the `spark` fixture.",
    _NAMED + " A unit test may take the `spark` fixture when the unit needs an engine: Spark is an engine, "
             "not a unit of ours.",
    _NAMED + " Unit tests do not need Fabric or Databricks, and a unit test may take the spark fixture.",
], ids=lambda t: t.replace(_NAMED, "+")[:40])
def test_the_testing_standard_check_accepts_valid_wordings(text: str) -> None:
    """F172 — check-adversary's false alarms that the design can tell apart, each accepted."""
    assert _suite_problems(text, _SUITES) == [], text


@pytest.mark.parametrize("text,fragment", [
    ("[a](tests/unit/) [b](tests/integration/) [c](tests/architecture/)", "does not name"),
    ("[1]: tests/unit/\n[2]: tests/integration/\n[3]: tests/architecture/\n", "does not name"),
    ('<a href="tests/unit/">x</a> `tests/integration/` `tests/architecture/`', "`tests/unit/`"),
    ("Para.\n\n    tests/unit/ tests/integration/ tests/architecture/\n", "does not name"),
    ("---\ndescription: tests/unit/ tests/integration/ tests/architecture/\n---\n\nbody\n", "does not name"),
    ("https://x/tests/unit/ `tests/integration/` `tests/architecture/`", "`tests/unit/`"),
    ("mytests/unit/ `tests/integration/` `tests/architecture/`", "`tests/unit/`"),
    ("`tests/unit/fabric/` `tests/integration/` `tests/architecture/`", "`tests/unit/`"),
    ("```\ntests/unit/ tests/integration/ tests/architecture/\n```\n", "does not name"),
    (_NAMED + " Two suites, really.", "counts Two"), (_NAMED + " Two test suites.", "counts Two"),
    (_NAMED + " Two separate suites.", "counts Two"), (_NAMED + " A pair of suites.", "counts pair"),
    (_NAMED + " Both suites run in CI.", "counts Both"), (_NAMED + " Seven suites.", "counts Seven"),
    (_NAMED + " Ten suites.", "counts Ten"), (_NAMED + " 2 suites.", "counts 2"),
    (_NAMED + " Unit tests use the mock adapter, no SparkSession.", "forbids"),
    (_NAMED + " Unit tests: no `SparkSession`.", "forbids"), (_NAMED + " Unit tests run without a Spark session.", "forbids"),
    (_NAMED + " Unit tests never create a SparkSession.", "forbids"),
    (_NAMED + " Unit tests, by design, start no SparkSession at all.", "forbids"),
    (_NAMED + " A unit test must not use the spark fixture.", "forbids"),
    (_NAMED + " Unit tests: no SparkSessions.", "forbids"),
    (_NAMED + " Unit tests use the mock adapter; no SparkSession.", "forbids"),
], ids=lambda t: t.replace(_NAMED, "+").replace("\n", " ")[:40] if isinstance(t, str) else t)
def test_the_testing_standard_check_reports_each_disagreement(text: str, fragment: str) -> None:
    """F172 — check-adversary's bypasses that the design reads, each reported."""
    problems = _suite_problems(text, _SUITES)
    assert any(fragment in p for p in problems), (text, problems)


# ── F173: the project's hooks name scripts that exist ───────────────────────

_SETTINGS = _arch.CLAUDE_DIR / "settings.json"
_HOOK_COMMAND = re.compile(r'python3 "\$\{CLAUDE_PROJECT_DIR\}/(?P<path>\.claude/hooks/[\w-]+\.py)"')
# The events Claude Code knows (its settings schema); a misspelt one never fires (F173 adversary).
_HOOK_EVENTS = frozenset({
    "PreToolUse", "PostToolUse", "PostToolUseFailure", "PostToolBatch", "Notification",
    "UserPromptSubmit", "UserPromptExpansion", "SessionStart", "SessionEnd", "Stop", "StopFailure",
    "SubagentStart", "SubagentStop", "PreCompact", "PostCompact", "PreModelSwitch", "PostModelSwitch",
    "PermissionRequest", "PermissionDenied", "Setup", "TeammateIdle", "TaskCreated", "TaskCompleted",
    "Elicitation", "ElicitationResult", "ConfigChange", "WorktreeCreate", "WorktreeRemove",
    "InstructionsLoaded", "CwdChanged", "FileChanged", "DirectoryAdded", "MessageDisplay"})
# Keys a hook may carry here; `args`, `shell`, `async`, `once` and `if` change how, whether or when
# it runs, so a hook carrying one is not the hook the contract describes.
_HOOK_KEYS = frozenset({"type", "command", "timeout", "statusMessage"})
# `env` keys that change which python3 a hook command runs, or what it imports (F194 adversary).
_HOOK_ENV = frozenset({"PATH", "PYTHONPATH", "PYTHONHOME", "PYTHONSTARTUP", "CLAUDE_PROJECT_DIR"})
# Each required hook: its event, the triggers it must cover, the script it must run, and whether
# those triggers are exactly the ones it may fire on. SessionStart carries two (F173, F194): the
# state restore on compaction alone, the git history on startup and resume alone.
_REQUIRED_HOOKS = (
    ("PreCompact", frozenset({"manual", "auto"}), ".claude/hooks/pre_compact.py", False),
    ("SessionStart", frozenset({"compact"}), ".claude/hooks/post_compact.py", True),
    ("SessionStart", frozenset({"startup", "resume"}), ".claude/hooks/session_start.py", True),
)


def _covers(matcher: object) -> set[str] | None:
    """The triggers a matcher selects: None for every one (absent, empty or "*"), else its
    `|`-separated names; an unreadable matcher selects nothing. A regex matcher such as
    `^(startup|resume)$` would select the same triggers on the platform; it is refused here on
    purpose, so the settings name their triggers one way (F194 adversary)."""
    if matcher is None or matcher in ("", "*"):
        return None
    return set(matcher.split("|")) if isinstance(matcher, str) else set()


def _hook_problems(settings: object, existing: set[str]) -> list[str]:
    """Why a project hook could fail, misfire or not run, unseen, or [] (F173).

    Every hook is a command hook running exactly `python3 "${CLAUDE_PROJECT_DIR}/.claude/hooks/
    <name>.py"` for a script that exists, reads and parses, with a timeout of 1 to 60 seconds and
    no key beyond type, command, timeout and statusMessage. Each event is one Claude Code knows,
    and its value a list of groups, each a dict with a list of hooks. The required hooks are
    present: `PreCompact` covering the manual and auto triggers and running pre_compact.py;
    `SessionStart` for exactly `compact`, running post_compact.py (F173); and `SessionStart` for
    exactly `startup` and `resume`, running session_start.py (F194). A group counts only when
    every hook in it passes. A required script runs alone in its group, and under its own event
    only; its triggers there, summed over its groups, are exactly the required ones where the
    requirement is exact (F194 adversary: a second script in a group, or a required script under
    another event, went unread). A group holds only `matcher` and `hooks`; `statusMessage` is a
    string; `disableAllHooks` is not set, and `env` does not set what the hooks' interpreter
    resolves through. Anything unreadable is reported, never skipped.
    """
    if not isinstance(settings, dict):
        return ["settings.json is not a JSON object"]
    problems: list[str] = []
    if settings.get("disableAllHooks"):
        problems.append("settings.json sets disableAllHooks, which turns every hook off")
    env = settings.get("env")
    if isinstance(env, dict):
        problems += [f"settings.json env sets {key}, which changes the python3 the hooks run"
                     for key in sorted(_HOOK_ENV & set(env))]
    hooks = settings.get("hooks")
    if not isinstance(hooks, dict):
        return [*problems, "settings.json has no hooks object"]
    required = {script: event for event, _, script, _ in _REQUIRED_HOOKS}
    covered: dict[tuple[str, str], set[str] | None] = {}
    for event, groups in hooks.items():
        if event not in _HOOK_EVENTS:
            problems.append(f"{event!r} is not a hook event Claude Code knows; it never fires")
            continue
        if not isinstance(groups, list):
            problems.append(f"{event}: the groups are not a list")
            continue
        for g, group in enumerate(groups):
            where = f"{event}[{g}]"
            if not isinstance(group, dict) or not isinstance(group.get("hooks"), list) or not group["hooks"]:
                problems.append(f"{where}: not a group with a non-empty list of hooks")
                continue
            problems += [f"{where}: key {k!r} is not a group key" for k in sorted(set(group) - {"matcher", "hooks"})]
            matcher = group.get("matcher")
            if matcher is not None and not isinstance(matcher, str):
                problems.append(f"{where}: matcher {matcher!r} is not a string")
                continue
            ok, scripts = True, set()
            for hook in group["hooks"]:
                found = _one_hook_problems(hook, existing, where)
                problems += found
                ok = ok and not found
                if not found:
                    scripts.add(_HOOK_COMMAND.fullmatch(hook["command"]).group("path"))
            for script in sorted(scripts & set(required)):
                if len(scripts) > 1:
                    problems.append(f"{where}: runs {script} with another script; a required hook runs alone")
                if event != required[script]:
                    problems.append(f"{where}: runs {script} under {event}; it belongs to {required[script]}")
            if ok and len(scripts) == 1:
                key = (event, next(iter(scripts)))
                triggers = _covers(matcher)
                previous = covered.get(key, set())
                covered[key] = None if triggers is None or previous is None else previous | triggers
    for event, triggers, script, exact in _REQUIRED_HOOKS:
        got = covered.get((event, script), set())
        # An exact requirement fires on its triggers and nothing else; PreCompact on every trigger.
        ok = got == triggers if exact else got is None or triggers <= got
        if not ok:
            problems.append(f"settings.json has no well-formed {event} hook for {sorted(triggers)} "
                            f"running {script}")
    return problems


def _one_hook_problems(hook: object, existing: set[str], where: str) -> list[str]:
    import ast
    if not isinstance(hook, dict):
        return [f"{where}: hook {hook!r} is not an object"]
    problems = [f"{where}: key {k!r} changes how or whether the hook runs" for k in sorted(set(hook) - _HOOK_KEYS)]
    command = hook.get("command")
    m = _HOOK_COMMAND.fullmatch(command) if isinstance(command, str) and hook.get("type") == "command" else None
    if m is None:
        return [*problems, f'{where}: {command!r} is not python3 "${{CLAUDE_PROJECT_DIR}}/.claude/hooks/<name>.py"']
    path = m.group("path")
    if path not in existing:
        problems.append(f"{where}: {path} does not exist")
    else:
        try:
            ast.parse((_arch.ROOT / path).read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, ValueError, SyntaxError) as exc:
            problems.append(f"{where}: {path} cannot be read or parsed: {type(exc).__name__}")
    if "statusMessage" in hook and not isinstance(hook["statusMessage"], str):
        problems.append(f"{where}: statusMessage {hook['statusMessage']!r} is not a string")
    timeout = hook.get("timeout")
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 1 <= timeout <= 60:
        problems.append(f"{where}: timeout {timeout!r}; give one between 1 and 60 seconds")
    return problems


def test_the_project_hooks_name_scripts_that_exist() -> None:
    """F173 — the compaction hooks: each runs a script in .claude/hooks/ that exists and parses,
    through `${CLAUDE_PROJECT_DIR}`, with a timeout; a hook that points nowhere fails silently."""
    import json
    settings = json.loads(_SETTINGS.read_text(encoding="utf-8"))
    existing = {_arch.rel(p) for p in (_arch.CLAUDE_DIR / "hooks").glob("*.py")}
    assert not (problems := _hook_problems(settings, existing)), problems


_H = {"type": "command", "command": 'python3 "${CLAUDE_PROJECT_DIR}/.claude/hooks/pre_compact.py"', "timeout": 10}
_P = {"type": "command", "command": 'python3 "${CLAUDE_PROJECT_DIR}/.claude/hooks/post_compact.py"', "timeout": 10}
_S = {"type": "command", "command": 'python3 "${CLAUDE_PROJECT_DIR}/.claude/hooks/session_start.py"', "timeout": 10}
_EXISTING = {".claude/hooks/pre_compact.py", ".claude/hooks/post_compact.py", ".claude/hooks/session_start.py"}
_START = {"matcher": "startup|resume", "hooks": [_S]}


def _settings(pre: object = None, post: object = None, start: object = None, **top: object) -> dict:
    """Valid settings with one part replaced: *post* the compact group, *start* the
    startup/resume group (both in SessionStart; `start=[]` leaves the second out)."""
    session = [{"matcher": "compact", "hooks": [_P]}] if post is None else post
    if isinstance(session, list):
        session = [*session, *([_START] if start is None else start)]
    hooks = {"PreCompact": [{"hooks": [_H]}] if pre is None else pre, "SessionStart": session}
    return {"hooks": hooks, **top}


@pytest.mark.parametrize("settings", [
    _settings(), _settings(pre=[{"matcher": "", "hooks": [_H]}]), _settings(pre=[{"matcher": "*", "hooks": [_H]}]),
    _settings(pre=[{"matcher": "manual|auto", "hooks": [_H]}]),
    _settings(pre=[{"matcher": "manual", "hooks": [_H]}, {"matcher": "auto", "hooks": [_H]}]),
    _settings(post=[{"matcher": "compact", "hooks": [{**_P, "statusMessage": "Restoring state"}]}]),
    _settings(start=[{"matcher": "resume|startup", "hooks": [_S]}]),
    _settings(start=[{"matcher": "startup", "hooks": [_S]}, {"matcher": "resume", "hooks": [_S]}]),
    _settings(post=[], start=[_START, {"matcher": "compact", "hooks": [_P]}]),
    _settings(start=[_START, {"matcher": "startup", "hooks": [_S]}]),
    _settings(env={"TZ": "UTC"}),
], ids=["plain", "empty matcher", "star", "alternation", "two groups", "status message",
        "startup reversed", "startup two groups", "startup first", "startup twice", "harmless env"])
def test_the_hook_check_accepts_valid_settings(settings: dict) -> None:
    """F173, F194 — the adversary's false alarms: matchers that cover every PreCompact trigger,
    and the SessionStart groups split or ordered differently."""
    assert _hook_problems(settings, _EXISTING) == []


@pytest.mark.parametrize("settings,fragment", [
    (_settings(pre=[{}]), "PreCompact[0]"), (_settings(pre=[None]), "PreCompact[0]"),
    (_settings(pre="x"), "not a list"), (_settings(pre={"hooks": [_H]}), "not a list"),
    (_settings(pre=[{"hooks": []}, {"matcher": "manual", "hooks": [_H]}]), "no well-formed PreCompact"),
    (_settings(post=[{"matcher": "compact", "hooks": []}]), "no well-formed SessionStart"),
    (_settings(post=[{"matcher": "compact|startup", "hooks": [_P]}]), "no well-formed SessionStart"),
    (_settings(post=[{"matcher": "Compact", "hooks": [_P]}]), "no well-formed SessionStart"),
    (_settings(pre=[{"hooks": [{**_H, "args": ["--x"]}]}]), "'args'"),
    (_settings(pre=[{"hooks": [{**_H, "shell": "powershell"}]}]), "'shell'"),
    (_settings(post=[{"matcher": "compact", "hooks": [{**_P, "async": True}]}]), "'async'"),
    (_settings(post=[{"matcher": "compact", "hooks": [{**_P, "once": True}]}]), "'once'"),
    (_settings(pre=[{"hooks": [{**_H, "if": "false"}]}]), "'if'"),
    (_settings(disableAllHooks=True), "disableAllHooks"),
    ({"hooks": {**_settings()["hooks"], "Stop": [{"hooks": []}]}}, "Stop[0]"),
    ({"hooks": {**_settings()["hooks"], "PreCompat": [{"hooks": [_H]}]}}, "'PreCompat'"),
    (_settings(pre=[{"hooks": [_P]}], post=[{"matcher": "compact", "hooks": [_H]}]), "no well-formed"),
    ([], "not a JSON object"), (_settings(post=[{"matcher": ["compact"], "hooks": [_P]}]), "matcher"),
    (_settings(pre=[{"hooks": None}]), "PreCompact[0]"), (_settings(pre=[{"hooks": [None]}]), "is not an object"),
    (_settings(pre=[{"hooks": ["x"]}]), "is not an object"), (_settings(pre=[{"hooks": [{**_H, "command": 5}]}]), "is not python3"),
    (_settings(pre=[{"hooks": [{**_H, "command": _H["command"] + "\n"}]}]), "is not python3"),
    (_settings(pre=[{"hooks": [{**_H, "timeout": True}]}]), "timeout"),
    (_settings(pre=[{"hooks": [{**_H, "timeout": float("inf")}]}]), "timeout"),
    (_settings(pre=[{"hooks": [{**_H, "command": 'python3 "${CLAUDE_PROJECT_DIR}/.claude/hooks/missing.py"'}]}]),
     "does not exist"),
    # F194 — the startup/resume group.
    (_settings(start=[]), "session_start.py"),
    (_settings(start=[{"matcher": "startup|resume", "hooks": [_P]}]), "session_start.py"),
    (_settings(start=[{"matcher": "startup", "hooks": [_S]}]), "session_start.py"),
    (_settings(start=[{"matcher": "resume", "hooks": [_S]}]), "session_start.py"),
    (_settings(start=[{"hooks": [_S]}]), "session_start.py"),
    (_settings(start=[{"matcher": "*", "hooks": [_S]}]), "session_start.py"),
    (_settings(start=[{"matcher": "startup|resume|clear", "hooks": [_S]}]), "session_start.py"),
    (_settings(start=[{"matcher": "Startup|resume", "hooks": [_S]}]), "session_start.py"),
    (_settings(start=[{"matcher": "startup|resume", "hooks": [_S, _P]}]), "session_start.py"),
    (_settings(start=[{"matcher": "startup|resume", "hooks": [{**_S, "timeout": 0}]}]), "timeout"),
    (_settings(post=[], start=[{"matcher": "startup|resume|compact", "hooks": [_S]}]), "post_compact.py"),
    (_settings(post=[], start=[{"matcher": "startup|resume|compact", "hooks": [_S]}]), "session_start.py"),
    (_settings(post=[], start=[{"matcher": "startup|resume|compact", "hooks": [_P]}]), "post_compact.py"),
    (_settings(post=[], start=[{"matcher": "startup|resume|compact", "hooks": [_P, _S]}]), "post_compact.py"),
    (_settings(post=[{"matcher": "compact", "hooks": [_S]}], start=[{"matcher": "startup|resume", "hooks": [_P]}]),
     "no well-formed SessionStart"),
    (_settings(start=[{"matcher": "startup|resume", "hooks": [_S]}, {"matcher": "clear", "hooks": [_S]}]),
     "session_start.py"),
    # F194's adversary: a required script beside another, or under another event; env; group keys.
    (_settings(start=[_START, {"hooks": [_S, _P]}]), "with another script"),
    (_settings(start=[_START, {"matcher": "clear", "hooks": [_S, _P]}]), "with another script"),
    (_settings(start=[_START, {"matcher": "clear", "hooks": [_S, _H]}]), "with another script"),
    (_settings(start=[_START, {"matcher": "compact", "hooks": [_P, _S]}]), "with another script"),
    (_settings(pre=[{"hooks": [_H]}, {"hooks": [_S]}]), "belongs to SessionStart"),
    ({"hooks": {**_settings()["hooks"], "Stop": [{"hooks": [_S]}]}}, "belongs to SessionStart"),
    (_settings(start=[_START, {"hooks": [_H]}]), "belongs to PreCompact"),
    ({"hooks": {**_settings()["hooks"], "PostCompact": [{"hooks": [_P]}]}}, "belongs to SessionStart"),
    (_settings(env={"PATH": "/nowhere"}), "env sets PATH"),
    (_settings(env={"PYTHONPATH": "/tmp/x"}), "env sets PYTHONPATH"),
    (_settings(env={"CLAUDE_PROJECT_DIR": "/tmp/x"}), "env sets CLAUDE_PROJECT_DIR"),
    (_settings(start=[{"matcher": "startup|resume", "hooks": [_S], "disabled": True}]), "'disabled'"),
    (_settings(start=[{**_START, "if": "false"}]), "'if'"),
    (_settings(post=[{"matcher": "compact", "hooks": [{**_P, "statusMessage": {"x": 1}}]}]), "statusMessage"),
    (_settings(start=[{"matcher": "^(startup|resume)$", "hooks": [_S]}]), "session_start.py"),
], ids=lambda x: str(x)[:50] if isinstance(x, str) else None)
def test_the_hook_check_reports_each_bypass(settings: object, fragment: str) -> None:
    """F173 — check-adversary's bypasses and crashes, each now reported."""
    problems = _hook_problems(settings, _EXISTING)
    assert any(fragment in p for p in problems), problems


def test_the_hooks_import_only_the_standard_library() -> None:
    """F173 — the hook scripts run with whatever python3 is on PATH, outside the project's
    virtualenv, so they import nothing beyond the standard library."""
    import ast
    import sys
    for script in sorted((_arch.CLAUDE_DIR / "hooks").glob("*.py")):
        tree = ast.parse(script.read_text(encoding="utf-8"))
        names = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
        names |= {n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
        assert names <= set(sys.stdlib_module_names) | {"__future__"}, (_arch.rel(script), names)


# ── F177: the rules carry constraints, not their histories ───────────────────

# Every rule file is read in full by every session (F23). These ceilings hold each one to its
# constraints, an "Enforced by" line and a sentence of why per constraint; an incident's story
# belongs to its backlog entry or decision record. A rule that needs more is a decision, made by
# raising the number here in its own change.
# Measured at F177's landing, 2026-10-02: 48 lines, 2585 bytes, 15777 together; a small margin.
_RULE_MAX_LINES = 52
_RULE_MAX_BYTES = 2800
_RULES_MAX_BYTES = 16500
_AT_IMPORT = re.compile(r"(?<![\w@])@(?:\.{0,2}/|~/)?[\w./-]+\.(?:md|markdown|txt)\b")


def _rule_files() -> list:
    """Every Markdown file under .claude/rules/, at any depth: a session reads them all."""
    return sorted(p for p in _RULES.rglob("*") if p.suffix.lower() in (".md", ".markdown"))


def _rule_weight_problems(rules: dict[str, str]) -> list[str]:
    """Which rule files, or their sum, exceed the ceilings, or [] (F177).

    Weight is what a session receives: the body below the frontmatter, which is stripped before
    a rule is loaded, measured in UTF-8 bytes (a closer bound on tokens than characters, which
    count an ideogram or an emoji as one) and in lines, counted at `\n`. A rule may not `@`-import
    another file, which would load weight this count cannot see.

    Stated limits: a rule loaded only for some paths (a `paths:` frontmatter) is counted as
    always loaded; HTML comments, which a loader may strip, are counted.
    """
    problems = []
    total = 0
    for name, text in rules.items():
        body = strip_frontmatter(text)
        size = len(body.encode("utf-8"))
        lines = body.count("\n") + (0 if body.endswith("\n") or not body else 1)
        total += size
        if lines > _RULE_MAX_LINES:
            problems.append(f"{name}: {lines} lines, over the {_RULE_MAX_LINES} ceiling")
        if size > _RULE_MAX_BYTES:
            problems.append(f"{name}: {size} bytes, over the {_RULE_MAX_BYTES} ceiling")
        problems += [f"{name}: {m.group(0)!r} would import a file this count cannot see"
                     for m in _AT_IMPORT.finditer(body)]
    if total > _RULES_MAX_BYTES:
        problems.append(f"the rules hold {total} bytes together, over the {_RULES_MAX_BYTES} ceiling")
    return problems


def test_the_rules_stay_within_their_ceilings() -> None:
    """F177 — the rules carried their incident histories in the body of the constraint."""
    rules, unreadable = {}, []
    for path in _rule_files():
        try:
            rules[_arch.rel(path)] = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            unreadable.append(f"{_arch.rel(path)}: {type(exc).__name__}")
    assert len(rules) >= 8, rules.keys()
    assert not (problems := unreadable + _rule_weight_problems(rules)), problems


@pytest.mark.parametrize("rules,fragment", [
    ({"r.md": "---\nname: r\n---\n" + "x\n" * (_RULE_MAX_LINES + 1)}, f"{_RULE_MAX_LINES + 1} lines"),
    ({"r.md": "x" * (_RULE_MAX_BYTES + 1)}, f"{_RULE_MAX_BYTES + 1} bytes"),
    ({"r.md": "\u6f22" * (_RULE_MAX_BYTES // 3 + 1)}, "bytes, over"),
    ({"r.md": "\U0001F600" * (_RULE_MAX_BYTES // 4 + 1)}, "bytes, over"),
    ({f"r{n}.md": "x" * (_RULES_MAX_BYTES // 8 + 1) for n in range(8)}, "together"),
    ({"r.md": "See @docs/big.md for the rest.\n"}, "import"),
    ({"r.md": "Import @../x.markdown here.\n"}, "import"),
], ids=["lines", "bytes", "ideograms", "emoji", "total", "at-import", "relative at-import"])
def test_the_rule_ceiling_check_reports_each_overweight_form(rules: dict, fragment: str) -> None:
    """F177 — check-adversary's bypasses, each reported."""
    problems = _rule_weight_problems(rules)
    assert any(fragment in p for p in problems), problems


@pytest.mark.parametrize("rules", [
    {"r.md": "---\nname: r\ndescription: " + "d" * 400 + "\n---\n" + "x\n" * _RULE_MAX_LINES},
    {"r.md": "a\x0cb\x1cc\u2028d\n" * 50},
    {"r.md": "`@runtime_checkable` is a decorator.\n"},
    {"r.md": "email someone@example.md is not an import.\n"},
], ids=["frontmatter not counted", "rare separators are not lines", "decorator", "email"])
def test_the_rule_ceiling_check_leaves_valid_rules_alone(rules: dict) -> None:
    """F177 — check-adversary's false alarms, each accepted."""
    assert _rule_weight_problems(rules) == []


def test_the_rule_glob_reads_every_depth_and_suffix(tmp_path, monkeypatch) -> None:
    """F177 — a rule in a subdirectory, or spelled .MD or .markdown, is still a rule."""
    import test_instruction_integrity as module
    for name in ("a.md", "sub/b.md", "C.MD", "d.markdown", "e.txt"):
        (tmp_path / name).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / name).write_text("x")
    monkeypatch.setattr(module, "_RULES", tmp_path)
    assert sorted(p.relative_to(tmp_path).as_posix() for p in _rule_files()) == ["C.MD", "a.md", "d.markdown", "sub/b.md"]
