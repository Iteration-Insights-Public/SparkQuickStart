#!/usr/bin/env python3
"""Gate mypy on *new* errors only, against a committed baseline.

`[tool.mypy] strict = true` has been declared for a long time while nothing ran
it, so the tree carries a large existing error count. Fixing all of it up front
is a real piece of work (see the issue linked from pyproject.toml), and leaving
mypy unenforced until then means new type errors keep landing unnoticed. This
script is the middle path: the existing errors are recorded once, and CI fails
only when a *new* one appears.

Errors are keyed by (file, error-code, message) with a count, deliberately
ignoring line numbers — otherwise every unrelated edit above an error would
shift its line and read as "new". The trade-off is that adding a second,
identical error to a file that already has one is detected (the count rises),
but moving one around inside the same file is not.

Usage:
    python scripts/mypy_baseline.py            # check; non-zero exit on new errors
    python scripts/mypy_baseline.py --update   # rewrite the baseline from current state

The baseline is meant to shrink. When you fix errors, this script tells you the
count dropped and asks you to re-run with --update, so the file tracks reality
rather than silently permitting regressions back up to the old number.
"""
from __future__ import annotations

import argparse
import collections
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
BASELINE = REPO_ROOT / "mypy-baseline.txt"

# Scope: the whole shipped package. Deliberately not narrowed to the "clean"
# modules — a baseline covers legacy code just as well, and narrowing would
# leave those files with no gate at all.
TARGETS = ["src/spark_quick_start"]

ERROR_RE = re.compile(r"^(?P<file>[^:]+):\d+:(?:\d+:)? error: (?P<msg>.*?)(?:\s+\[(?P<code>[a-z-]+)\])?$")


def run_mypy() -> str:
    proc = subprocess.run(
        [sys.executable, "-m", "mypy", *TARGETS],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0 and not proc.stdout.strip():
        sys.stderr.write(proc.stderr)
        raise SystemExit(f"mypy failed to run (exit {proc.returncode})")
    return proc.stdout


def parse(output: str) -> collections.Counter[str]:
    counts: collections.Counter[str] = collections.Counter()
    for line in output.splitlines():
        m = ERROR_RE.match(line.strip())
        if m:
            counts[f"{m.group('file')}\t{m.group('code') or '-'}\t{m.group('msg')}"] += 1
    return counts


def env_fingerprint() -> str:
    """Identify the environment, because mypy's output depends on it.

    Which third-party packages are importable changes what mypy can resolve: with
    delta-spark installed, DeltaTable calls are typed and some `# type: ignore`
    comments are load-bearing; without it the same calls degrade to Any and those
    comments become "unused". A baseline recorded in one environment therefore
    reports phantom "new errors" in another. CI installs `.[dev,spark,runtime]`
    (note: no `local`, so no delta-spark), and that is what the committed
    baseline must be generated against.
    """
    import importlib.util

    try:
        from mypy.version import __version__ as mypy_version
    except Exception:  # pragma: no cover — mypy always present in practice
        mypy_version = "?"
    pkgs = " ".join(
        f"{name}={int(importlib.util.find_spec(name) is not None)}"
        for name in ("pyspark", "pandas", "pyarrow", "delta")
    )
    return f"mypy={mypy_version} {pkgs}"


def load_baseline() -> tuple[collections.Counter[str], str]:
    counts: collections.Counter[str] = collections.Counter()
    recorded_env = ""
    if not BASELINE.exists():
        return counts, recorded_env
    for line in BASELINE.read_text().splitlines():
        if line.startswith("# Env: "):
            recorded_env = line[len("# Env: "):].strip()
            continue
        if not line.strip() or line.startswith("#"):
            continue
        count, _, key = line.partition(" ")
        counts[key] = int(count)
    return counts, recorded_env


def write_baseline(counts: collections.Counter[str]) -> None:
    total = sum(counts.values())
    header = (
        "# mypy baseline — pre-existing errors, recorded so CI can fail on NEW ones.\n"
        "# Regenerate with: python scripts/mypy_baseline.py --update\n"
        "# Format: <count> <file>\\t<error-code>\\t<message>\n"
        "#\n"
        "# Generated against the environment below. mypy's output depends on which\n"
        "# packages are importable, so regenerate under `pip install -e\n"
        "# '.[dev,spark,runtime]' -c requirements.txt` — the same extras and locked\n"
        "# versions CI installs — or the diff will be dominated by environment noise\n"
        "# rather than real changes.\n"
        f"# Env: {env_fingerprint()}\n"
        f"# Total: {total}\n"
    )
    body = "".join(f"{n} {k}\n" for k, n in sorted(counts.items()))
    BASELINE.write_text(header + body)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--update", action="store_true", help="rewrite the baseline from current state")
    args = ap.parse_args()

    current = parse(run_mypy())

    if args.update:
        write_baseline(current)
        print(f"baseline updated: {sum(current.values())} errors across {len(current)} distinct entries")
        return 0

    baseline, recorded_env = load_baseline()
    current_env = env_fingerprint()
    env_mismatch = bool(recorded_env) and recorded_env != current_env

    new = {k: current[k] - baseline.get(k, 0) for k in current if current[k] > baseline.get(k, 0)}
    fixed = {k: baseline[k] - current.get(k, 0) for k in baseline if baseline[k] > current.get(k, 0)}

    if env_mismatch and (new or fixed):
        print(
            "WARNING: environment differs from the one the baseline was recorded in.\n"
            f"  baseline: {recorded_env}\n"
            f"  current:  {current_env}\n"
            "mypy resolves different types depending on which packages are installed,\n"
            "so some differences below are probably environmental rather than real.\n"
            "To reproduce CI exactly:  pip install -e '.[dev,spark,runtime]' -c requirements.txt\n",
            file=sys.stderr,
        )

    if new:
        print(f"NEW mypy errors: {sum(new.values())}\n", file=sys.stderr)
        for key, n in sorted(new.items()):
            file_, code, msg = key.split("\t")
            print(f"  {file_} [{code}] {msg}" + (f"  (x{n})" if n > 1 else ""), file=sys.stderr)
        print(
            "\nFix them, or — if the error is genuinely acceptable — add a targeted\n"
            "`# type: ignore[code]` with a comment saying why. Do not run --update to\n"
            "paper over a new error; the baseline is for pre-existing debt only.\n"
            "\nNote: `# type: ignore` must sit on the line mypy reports the error against.\n"
            "For a multi-line import that is the `from ... import (` line, not the name\n"
            "inside the parentheses.",
            file=sys.stderr,
        )
        return 1

    total_now, total_base = sum(current.values()), sum(baseline.values())
    if fixed:
        print(f"No new mypy errors. {sum(fixed.values())} baseline error(s) look fixed — nice.")
        print(f"Run `python scripts/mypy_baseline.py --update` to shrink the baseline "
              f"({total_base} -> {total_now}).")
    else:
        print(f"No new mypy errors ({total_now} baseline errors unchanged).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
