"""F173 — the compaction hooks keep the working state across a context compaction.

`pre_compact.py` snapshots the git state into a gitignored note before a compaction;
`post_compact.py` prints, after it, the state and the repo's own pointers, which enter the
model's context. Neither may ever block or fail a compaction: whatever goes wrong, they exit 0.
Each is run here as Claude Code runs it, a subprocess with `CLAUDE_PROJECT_DIR` set and the
hook's JSON on stdin, against a throwaway git repository.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

_ROOT = next(p for p in Path(__file__).resolve().parents if (p / "pyproject.toml").is_file())
_HOOKS = _ROOT / ".claude" / "hooks"
_LIMIT = int(re.search(r"^LIMIT = (\d+)", (_HOOKS / "post_compact.py").read_text(), re.M).group(1))


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True,
                   env={**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
                        "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"})


def _run(script: str, project: Path, payload: object = None, cwd: Path | None = None) -> subprocess.CompletedProcess:
    stdin = payload if isinstance(payload, str) else json.dumps(payload or {})
    return subprocess.run([sys.executable, str(_HOOKS / script)], input=stdin, text=True,
                          capture_output=True, cwd=cwd or project, timeout=30,
                          env={**os.environ, "CLAUDE_PROJECT_DIR": str(project)})


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A repository on branch `fix/F999-demo` with a stash, an uncommitted change, a todo
    with its "Start here" section and a backlog with one open and one landed row."""
    _git(tmp_path, "init", "-q", "-b", "main")
    (tmp_path / ".claude" / "notes").mkdir(parents=True)
    (tmp_path / "a.txt").write_text("one\n")
    _git(tmp_path, "add", "a.txt")
    _git(tmp_path, "commit", "-q", "-m", "init")
    _git(tmp_path, "switch", "-q", "-c", "fix/F999-demo")
    (tmp_path / "a.txt").write_text("stashed\n")
    _git(tmp_path, "stash", "push", "-q", "-m", "owner notebook outputs")
    (tmp_path / "a.txt").write_text("dirty\n")
    (tmp_path / ".claude" / "notes" / "todo.md").write_text(
        "# Todo\n\n## Start here next session\n\n- F999: finish the demo\n\n---\n\n## Older\n\nold text\n")
    (tmp_path / ".claude" / "backlog.md").write_text(
        "| id | title | kind | status | branch |\n|---|---|---|---|---|\n"
        "| F998 | landed thing | bug | landed | fix/F998 |\n| F999 | the demo is unfinished | task | open | — |\n"
        "| F997 | planned thing | task | planned | — |\n")
    return tmp_path


def test_the_pre_compact_hook_snapshots_the_git_state(repo: Path) -> None:
    done = _run("pre_compact.py", repo, {"trigger": "auto", "transcript_path": "/t/x.jsonl"})
    assert done.returncode == 0, done.stderr
    note = (repo / ".claude" / "notes" / "compaction-state.md").read_text()
    for expected in ("fix/F999-demo", "a.txt", "owner notebook outputs", "auto"):
        assert expected in note, (expected, note)


def test_the_post_compact_hook_prints_the_state_and_the_pointers(repo: Path) -> None:
    (repo / ".claude" / "notes" / "compaction-state.md").write_text(
        "# Working state before a compaction\n\nWritten at 2026-10-02.\n\nBranch: fix/F999-demo\n")
    done = _run("post_compact.py", repo, {"source": "compact", "transcript_path": "/t/x.jsonl"})
    assert done.returncode == 0, done.stderr
    out = done.stdout
    for expected in ("fix/F999-demo", "a.txt", "owner notebook outputs", "F999: finish the demo",
                     "F999", "F997", "/t/x.jsonl", "before the compaction"):
        assert expected in out, (expected, out)
    assert "F998" not in out, "a landed row is not open work"
    assert "old text" not in out, "only the Start here section of todo.md"
    assert len(out) <= _LIMIT, len(out)


def test_the_post_compact_block_stays_under_its_limit_with_a_long_backlog(repo: Path) -> None:
    rows = "".join(f"| F{n} | {'a long title ' * 8} | task | open | — |\n" for n in range(100, 400))
    (repo / ".claude" / "backlog.md").write_text("| id | title | kind | status | branch |\n|---|---|---|---|---|\n" + rows)
    (repo / "big.txt").write_text("x")
    for n in range(200):
        (repo / f"untracked_{n}.txt").write_text("x")
    done = _run("post_compact.py", repo, {})
    assert done.returncode == 0, done.stderr
    assert len(done.stdout) <= _LIMIT, len(done.stdout)
    assert "more" in done.stdout, "a capped list says so"


def test_the_post_compact_block_is_cut_at_the_scripts_limit(repo: Path) -> None:
    """Every section capped and still too long: the cut itself is what holds the limit."""
    long = "- " + "x" * 300
    (repo / ".claude" / "notes" / "todo.md").write_text("## Start here next session\n\n" + "\n".join([long] * 10) + "\n")
    (repo / ".claude" / "notes" / "compaction-state.md").write_text("# s\n\n" + "\n".join([long] * 12))
    done = _run("post_compact.py", repo, {})
    assert done.returncode == 0, done.stderr
    assert len(done.stdout.rstrip("\n")) <= _LIMIT and "(cut at the hook's limit)" in done.stdout


def test_a_killed_pre_compact_run_leaves_no_stale_snapshot(repo: Path) -> None:
    """The old note goes first, before the hook reads its input: a run killed while it waits
    (here, on an stdin that never closes) leaves no note, never the last session's."""
    import time
    note = repo / ".claude" / "notes" / "compaction-state.md"
    note.write_text("# from the last session\n")
    proc = subprocess.Popen([sys.executable, str(_HOOKS / "pre_compact.py")], stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=repo,
                            env={**os.environ, "CLAUDE_PROJECT_DIR": str(repo)})
    try:
        deadline = time.monotonic() + 10
        while note.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
    finally:
        proc.kill()
        proc.communicate()
    assert not note.exists()


@pytest.mark.parametrize("script", ["pre_compact.py", "post_compact.py"])
@pytest.mark.parametrize("case", ["not a repository", "garbage stdin", "no notes directory"])
def test_a_hook_exits_0_whatever_goes_wrong(tmp_path: Path, script: str, case: str) -> None:
    """A hook that fails must never block or break a compaction (exit 2 would block PreCompact)."""
    payload: object = "{not json" if case == "garbage stdin" else {}
    if case != "not a repository":
        _git(tmp_path, "init", "-q")
    done = _run(script, tmp_path, payload)
    assert done.returncode == 0, (case, done.stderr)
