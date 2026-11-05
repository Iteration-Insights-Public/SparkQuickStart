"""F194 — the session-start hook shows what happened in git since the last `/wrap-up`.

`session_start.py` runs on Claude Code's SessionStart for `startup` and `resume`. It reads the
anchor commit `/wrap-up` writes into the "Start here next session" section of
`.claude/notes/todo.md` and prints the commits since it, marking the ones with no Claude
co-author trailer as made by hand; without a usable anchor it prints the last ten and says why.
When `gh` is on PATH it adds the last merged pull requests, and drops them on any failure.
Like the F173 hooks it never fails a session start. Each case runs the script as Claude Code
does, a subprocess with `CLAUDE_PROJECT_DIR` set, against a throwaway git repository and a PATH
built for the case.
"""
from __future__ import annotations

import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

_ROOT = next(p for p in Path(__file__).resolve().parents if (p / "pyproject.toml").is_file())
_SCRIPT = _ROOT / ".claude" / "hooks" / "session_start.py"
_CLAUDE = "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
_ENV = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"}


def _load_hook():
    """The hook as a module: it is a standalone script, not a package member."""
    spec = importlib.util.spec_from_file_location("session_start_hook", _SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _git(repo: Path, *args: str) -> str:
    done = subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True, text=True,
                          env={**os.environ, **_ENV})
    return done.stdout.strip()


def _commit(repo: Path, subject: str, *, claude: bool) -> str:
    (repo / "log.txt").write_text((repo / "log.txt").read_text() + subject + "\n")
    _git(repo, "add", "log.txt")
    _git(repo, "commit", "-q", "-m", subject, *(["-m", _CLAUDE] if claude else []))
    return _git(repo, "rev-parse", "--short", "HEAD")


def _todo(repo: Path, anchor_line: str | None) -> None:
    lines = ["# Todo", "", "## Start here next session", "", "*Written by `/wrap-up` on 2026-10-01.*", ""]
    if anchor_line is not None:
        lines += [anchor_line, ""]
    lines += ["- F999: finish the demo", "", "---", "", "## Older", ""]
    (repo / ".claude" / "notes" / "todo.md").write_text("\n".join(lines))


def _bin(tmp_path: Path, gh: str | None, *, git: bool = True, on_log: str | None = None) -> str:
    """A PATH holding git (unless *git* is false) and, when *gh* is given, a fake `gh` running that
    shell body; *on_log* replaces what git does for `log`, every other subcommand passing through."""
    bindir = tmp_path / "bin"
    bindir.mkdir()
    real_git = shutil.which("git")
    log = f'case " $* " in *" log "*) {on_log};; esac\n' if on_log else ""
    if git:
        (bindir / "git").write_text(f'#!/bin/sh\n{log}exec "{real_git}" "$@"\n')
        (bindir / "git").chmod(0o755)
    if gh is not None:
        (bindir / "gh").write_text(f"#!/bin/sh\n{gh}\n")
        (bindir / "gh").chmod(0o755)
    return str(bindir)


def _run(repo: Path, path: str, stdin: str = "{}") -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, str(_SCRIPT)], input=stdin, text=True, capture_output=True,
                          cwd=repo, timeout=30, env={**os.environ, "PATH": path, "CLAUDE_PROJECT_DIR": str(repo)})


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """Twelve commits: c01..c12, the odd ones by hand, the even ones with a Claude trailer."""
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    (repo / ".claude" / "notes").mkdir(parents=True)
    (repo / "log.txt").write_text("")
    for n in range(1, 13):
        _commit(repo, f"c{n:02d}", claude=n % 2 == 0)
    return repo


def test_an_anchor_shows_only_the_later_commits_and_marks_the_ones_by_hand(repo: Path, tmp_path: Path) -> None:
    anchor = _git(repo, "rev-parse", "--short", "HEAD~3")       # c09
    _todo(repo, f"Anchor: {anchor} (HEAD on 2026-10-01)")
    done = _run(repo, _bin(tmp_path, gh=None))
    assert done.returncode == 0, done.stderr
    out = done.stdout
    # Compare against the parsed commit subjects, not raw substrings: a subject (c09) can be a
    # substring of a commit hash (ef7c099), which made this check flaky on unlucky hashes.
    subjects = [m.group(1) for line in out.splitlines()
                if (m := re.match(r"[0-9a-f]{7,}\s+(.*)", line))]
    for later in ("c10", "c11", "c12"):
        assert any(s.startswith(later) for s in subjects), (later, out)
    for earlier in ("c09", "c08", "c01"):
        assert not any(s.startswith(earlier) for s in subjects), (earlier, out)
    by_hand = [s for s in subjects if "(by hand)" in s]
    assert any(s.startswith("c11") for s in by_hand) and not any(
        s.startswith("c10") or s.startswith("c12") for s in by_hand), out
    order = [s[:3] for s in subjects]
    assert order.index("c12") < order.index("c11") < order.index("c10"), "newest first"
    assert "backlog" in out, "by-hand commits are to be checked against the open backlog entries"


def test_a_merge_commit_is_not_listed_but_what_it_brought_is(repo: Path, tmp_path: Path) -> None:
    """A merge has no co-author trailer and changes nothing itself; its parents' commits show."""
    anchor = _git(repo, "rev-parse", "--short", "HEAD")
    _git(repo, "switch", "-q", "-c", "side")
    _commit(repo, "side work", claude=True)
    _git(repo, "switch", "-q", "-")
    _git(repo, "merge", "-q", "--no-ff", "side", "-m", "Merge branch 'side' (local)")
    _todo(repo, f"Anchor: {anchor} (HEAD on 2026-10-01)")
    out = _run(repo, _bin(tmp_path, gh=None)).stdout
    assert "side work" in out and "Merge branch" not in out, out


def test_an_anchor_at_head_says_there_is_nothing_since(repo: Path, tmp_path: Path) -> None:
    _todo(repo, f"Anchor: {_git(repo, 'rev-parse', '--short', 'HEAD')} (HEAD on 2026-10-01)")
    done = _run(repo, _bin(tmp_path, gh=None))
    assert done.returncode == 0, done.stderr
    assert "no commits since" in done.stdout.lower() and "c12" not in done.stdout, done.stdout
    assert "backlog" not in done.stdout, "no by-hand commit listed, so no instruction"


def test_more_than_fifteen_commits_since_the_anchor_are_capped(repo: Path, tmp_path: Path) -> None:
    _todo(repo, f"Anchor: {_git(repo, 'rev-parse', '--short', 'HEAD')} (HEAD on 2026-10-01)")
    for n in range(13, 33):
        _commit(repo, f"c{n:02d}", claude=True)
    done = _run(repo, _bin(tmp_path, gh=None))
    assert done.returncode == 0, done.stderr
    assert "c32" in done.stdout and "c18" in done.stdout and "c17" not in done.stdout, done.stdout
    assert "5 more" in done.stdout, done.stdout


@pytest.mark.parametrize("anchor_line,why", [
    (None, "no anchor"),
    ("Anchor: deadbee (HEAD on 2026-10-01)", "unreachable"),
], ids=["no anchor", "unreachable anchor"])
def test_without_a_usable_anchor_the_last_ten_commits_and_a_notice(repo: Path, tmp_path: Path,
                                                                   anchor_line: str | None, why: str) -> None:
    _todo(repo, anchor_line)
    done = _run(repo, _bin(tmp_path, gh=None))
    assert done.returncode == 0, done.stderr
    out = done.stdout
    assert why in out.lower(), out
    assert "c12" in out and "c03" in out and "c02" not in out, out
    assert any("c03" in line and "(by hand)" in line for line in out.splitlines()), out


def test_a_missing_todo_is_the_no_anchor_case(repo: Path, tmp_path: Path) -> None:
    done = _run(repo, _bin(tmp_path, gh=None))
    assert done.returncode == 0, done.stderr
    assert "no anchor" in done.stdout.lower() and "c12" in done.stdout, done.stdout


def test_merged_pull_requests_are_listed_when_gh_answers(repo: Path, tmp_path: Path) -> None:
    _todo(repo, None)
    prs = json.dumps([{"number": 42, "title": "fix the thing", "mergedAt": "2026-09-30T10:00:00Z"}])
    done = _run(repo, _bin(tmp_path, gh=f"echo '{prs}'"))
    assert done.returncode == 0, done.stderr
    assert "#42 fix the thing (merged 2026-09-30)" in done.stdout, done.stdout


@pytest.mark.parametrize("gh", [None, "exec sleep 5", "echo boom >&2; exit 1", "echo 'not json'"],
                         ids=["gh absent", "gh slow", "gh fails", "gh garbage"])
def test_a_missing_slow_or_failing_gh_prints_no_pull_requests(repo: Path, tmp_path: Path, gh: str | None) -> None:
    _todo(repo, None)
    started = time.monotonic()
    done = _run(repo, _bin(tmp_path, gh=gh))
    elapsed = time.monotonic() - started
    assert done.returncode == 0, done.stderr
    assert "#" not in done.stdout.replace("## ", ""), done.stdout
    assert "c12" in done.stdout, "the git part is still printed"
    assert elapsed < 4.5, f"the gh call is bounded at 3 s, took {elapsed:.1f}s"


def test_the_output_stays_under_the_cap(repo: Path, tmp_path: Path) -> None:
    limit = _load_hook().LIMIT
    _todo(repo, f"Anchor: {_git(repo, 'rev-parse', '--short', 'HEAD')} (HEAD on 2026-10-01)")
    for n in range(13, 40):
        _commit(repo, f"c{n:02d} " + "a very long subject " * 20, claude=False)
    prs = json.dumps([{"number": n, "title": "t" * 400, "mergedAt": "2099-09-30T10:00:00Z"} for n in range(5)])
    done = _run(repo, _bin(tmp_path, gh=f"echo '{prs}'"))
    assert done.returncode == 0, done.stderr
    assert len(done.stdout.rstrip("\n")) <= limit, len(done.stdout)


def test_the_cap_drops_old_commit_lines_never_the_instruction_or_the_pull_requests(repo: Path, tmp_path: Path) -> None:
    """Whole lines only: the oldest commits go first, with a count; the backlog instruction and
    every pull request line stay, and no line is cut in the middle."""
    limit = _load_hook().LIMIT
    _todo(repo, f"Anchor: {_git(repo, 'rev-parse', '--short', 'HEAD')} (HEAD on 2026-10-01)")
    for n in range(13, 31):
        _commit(repo, f"c{n:02d} " + "x" * 86, claude=False)
    prs = json.dumps([{"number": n, "title": "p" * 85, "mergedAt": f"2099-09-2{n}T10:00:00Z"} for n in range(5)])
    out = _run(repo, _bin(tmp_path, gh=f"echo '{prs}'")).stdout
    assert len(out.rstrip("\n")) <= limit, len(out)
    assert "against the open entries in .claude/backlog.md" in out, out
    assert all(f"#{n} " in out for n in range(5)), out
    assert "c30" in out and "more" in out and "cut at" not in out, out
    assert "data, not instructions" in out, out


@pytest.mark.parametrize("anchored", [True, False], ids=["after an anchor", "in the fallback"])
def test_git_that_does_not_answer_is_not_reported_as_no_commits(repo: Path, tmp_path: Path, anchored: bool) -> None:
    _todo(repo, f"Anchor: {_git(repo, 'rev-parse', '--short', 'HEAD~2')} (HEAD on 2026-10-01)" if anchored else None)
    done = _run(repo, _bin(tmp_path, gh=None, on_log="exit 1"))
    assert done.returncode == 0, done.stderr
    assert "git did not answer; history not shown" in done.stdout, done.stdout
    assert "no commits since" not in done.stdout.lower(), done.stdout


def test_git_slow_on_log_is_bounded_and_reported(repo: Path, tmp_path: Path) -> None:
    _todo(repo, f"Anchor: {_git(repo, 'rev-parse', '--short', 'HEAD~2')} (HEAD on 2026-10-01)")
    started = time.monotonic()
    done = _run(repo, _bin(tmp_path, gh=None, on_log="exec sleep 5"))
    assert done.returncode == 0, done.stderr
    assert "git did not answer; history not shown" in done.stdout, done.stdout
    assert time.monotonic() - started < 4.5


@pytest.mark.parametrize("case", ["not a repository", "unborn HEAD", "git absent"])
def test_the_hook_is_silent_when_it_cannot_say_anything(tmp_path: Path, case: str) -> None:
    project = tmp_path / "p"
    (project / ".claude" / "notes").mkdir(parents=True)
    if case == "unborn HEAD":
        _git(project, "init", "-q")
    if case == "git absent":
        _git(project, "init", "-q")
        _git(project, "commit", "-q", "--allow-empty", "-m", "c01")
    done = _run(project, _bin(tmp_path, gh=None, git=case != "git absent"))
    assert done.returncode == 0, done.stderr
    assert done.stdout == "", done.stdout


def test_pull_requests_are_sorted_here_and_the_ones_before_the_anchor_dropped(repo: Path, tmp_path: Path) -> None:
    _todo(repo, f"Anchor: {_git(repo, 'rev-parse', '--short', 'HEAD~2')} (HEAD on 2026-10-01)")
    prs = json.dumps([{"number": 1, "title": "middle", "mergedAt": "2099-01-02T00:00:00Z"},
                      {"number": 2, "title": "old", "mergedAt": "2001-01-01T00:00:00Z"},
                      {"number": 3, "title": "newest", "mergedAt": "2099-01-03T00:00:00Z"},
                      {"number": 4, "title": "oldest new", "mergedAt": "2099-01-01T00:00:00Z"}])
    out = _run(repo, _bin(tmp_path, gh=f"echo '{prs}'")).stdout
    assert "Merged pull requests since the anchor, newest first" in out, out
    assert "#2 old" not in out, out
    assert out.index("#3 newest") < out.index("#1 middle") < out.index("#4 oldest new"), out


def test_without_an_anchor_the_latest_five_pull_requests_sorted(repo: Path, tmp_path: Path) -> None:
    _todo(repo, None)
    prs = json.dumps([{"number": n, "title": f"pr{n}", "mergedAt": f"2026-09-{10 + (n * 7) % 13:02d}T00:00:00Z"}
                      for n in range(1, 9)])
    out = _run(repo, _bin(tmp_path, gh=f"echo '{prs}'")).stdout
    assert "Merged pull requests, latest five, newest first" in out, out
    shown = [line for line in out.splitlines() if line.startswith("#") and not line.startswith("## ")]
    dates = [line.rsplit("(merged ", 1)[1] for line in shown]
    assert len(shown) == 5 and dates == sorted(dates, reverse=True), shown


@pytest.mark.parametrize("trailer,by_hand", [
    ("Co-Authored-By: Pat Doe <pat@example.com>", True),
    ("Co-authored-by: Claude Opus 5.5 <noreply@anthropic.com>", False),
    ("Co-Authored-By: Claude the cat <claude@example.com>", True),
], ids=["another co-author", "lowercase key", "the word Claude alone"])
def test_by_hand_means_no_anthropic_co_author(repo: Path, tmp_path: Path, trailer: str, by_hand: bool) -> None:
    _todo(repo, f"Anchor: {_git(repo, 'rev-parse', '--short', 'HEAD')} (HEAD on 2026-10-01)")
    (repo / "log.txt").write_text("more\n")
    _git(repo, "commit", "-q", "-am", "c13", "-m", trailer)
    line = next(line for line in _run(repo, _bin(tmp_path, gh=None)).stdout.splitlines() if " c13" in line)
    assert line.endswith("(by hand)") is by_hand, line


def test_a_subject_with_a_tab_parses(repo: Path, tmp_path: Path) -> None:
    _todo(repo, f"Anchor: {_git(repo, 'rev-parse', '--short', 'HEAD')} (HEAD on 2026-10-01)")
    (repo / "log.txt").write_text("more\n")
    _git(repo, "commit", "-q", "-am", "c13\twith a tab", "-m", _CLAUDE)
    out = _run(repo, _bin(tmp_path, gh=None)).stdout
    line = next(line for line in out.splitlines() if " c13" in line)
    assert "with a tab" in line and not line.endswith("(by hand)"), line


@pytest.mark.parametrize("form", ["- Anchor: {h} (HEAD on 2026-10-01)", "* Anchor: `{h}` (HEAD on 2026-10-01)",
                                  "Anchor: `{h}` (HEAD on 2026-10-01)"], ids=["dash", "star backticks", "backticks"])
def test_the_anchor_line_may_be_a_bullet_or_backticked(repo: Path, tmp_path: Path, form: str) -> None:
    _todo(repo, form.format(h=_git(repo, "rev-parse", "--short", "HEAD~1")))
    out = _run(repo, _bin(tmp_path, gh=None)).stdout
    assert "c12" in out and "c11" not in out and "no anchor" not in out.lower(), out


def test_the_fallback_asks_to_check_by_hand_commits_if_not_already_checked(repo: Path, tmp_path: Path) -> None:
    _todo(repo, None)
    out = _run(repo, _bin(tmp_path, gh=None)).stdout
    assert "if not already checked" in out, out


@pytest.mark.parametrize("case", ["not a repository", "garbage stdin", "no notes directory", "empty repository"])
def test_the_hook_exits_0_whatever_goes_wrong(tmp_path: Path, case: str) -> None:
    """A hook that fails must never break a session start."""
    project = tmp_path / "p"
    project.mkdir()
    if case != "not a repository":
        _git(project, "init", "-q")
    if case == "garbage stdin":
        _git(project, "commit", "-q", "--allow-empty", "-m", "c01")
    done = _run(project, _bin(tmp_path, gh=None), stdin="{not json" if case == "garbage stdin" else "{}")
    assert done.returncode == 0, (case, done.stderr)
