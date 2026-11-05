---
name: tooling-and-gates
description: The commands that verify a change, and the policy governing ruff ignores and the mypy baseline.
---

# Tooling and gates

```bash
source .venv/bin/activate             # ALWAYS first — see the note below

pytest tests/unit -m 'not spark' --no-cov  # inner loop — no engine, seconds
pytest tests/unit -n 6 --dist loadfile                # coverage gate (fail_under=85)
pytest tests/integration -n 6 --dist loadfile --no-cov # composition suite
pytest tests/architecture --no-cov    # boundary rules, serial; --no-cov because --cov=src is global
pytest --no-cov                       # everything in one serial process — optional, see below
docker compose up                     # the same three runs in a container, as CI
ruff check src tests                  # pinned ruff==0.16.4, explicit select list
python scripts/mypy_baseline.py       # fails only on NEW mypy errors
python scripts/mypy_baseline.py --update   # after genuinely fixing some
scripts/verify_trunk_complete.sh      # trunk still has every platform-specific path
python scripts/check_mermaid.py       # static mermaid rules; --list names every diagram
```

- **The three gate runs are parallel (two of them) and split by suite.** Each xdist worker is its
  own process with its own JVM and `SparkSession` (about 1.5 GB each); `--dist loadfile` keeps a
  module on one worker, so a module-scoped fixture behaves as in a serial run. `-n 6` locally:
  measured, 4 to 6 workers took the unit and integration suites from 219 s to 199 s on 12 cores,
  and more workers add JVMs for little gain. CI runs `-n auto`, which scales with the runner's
  CPUs (2 on the private `ubuntu-latest` today, more on a larger runner). Only the unit run
  carries the coverage gate: coverage answers to the unit suite alone
  (`.claude/rules/test-classification.md`). The architecture suite stays serial because its
  `ExampleCoverage` warning class, defined in a test module, cannot be unserialized by xdist.
  **Given up:** the former `pytest --no-cov -q` ran all three suites in one process, which caught an
  assertion that passes only because another file had already been imported. Parallel runs by file
  no longer exercise that across files. A serial `pytest --no-cov` remains available, and is the
  way to chase an order dependence.
- **Run everything from `.venv`.** A bare `pytest` in an unactivated shell picks up
  whatever interpreter is on PATH — on a machine with Anaconda that is PySpark 4.1.1
  against the project's 3.5.3, a major version apart. The visible symptom is a Delta test
  that skips in `.venv` and fails in the other environment, because the skip guard checks
  whether the `delta` Python package is importable rather than whether Spark can load the
  data source. Different results from the same command in two shells is the tell.
- `ruff` rules are pinned explicitly in `pyproject.toml`; the `ignore` list is a
  *deferred* list with real hit counts, each line independently removable. Never add an
  entry without a count and a reason; never raise an existing count.
- `mypy` is `strict = true` but the tree carries a baseline of known errors, counted by
  `python scripts/mypy_baseline.py` and summarised in CLAUDE.md §8. New code must be clean; do
  not regenerate the baseline to hide new errors.
- `pytest tests/architecture` **needs `--no-cov` when run alone.** `--cov=src` lives in
  `addopts`, so the directory by itself reports near-zero coverage and trips
  `fail_under = 85`. The failure says "coverage failure", not "your tests failed" — read the
  last line before assuming the suite is broken. The full `pytest` run is unaffected.
- **Workflow files are checked as parsed YAML, and third-party actions are pinned.**
  `tests/architecture/test_ci_workflows.py` loads `.github/workflows/` with PyYAML (a `[dev]`
  dependency, test-only) through a loader that rejects duplicate keys. Every `uses:` outside
  `actions/` is a 40-hex commit SHA with a `# vX.Y.Z` comment beside it; a `docker://` image
  carries its `@sha256:` digest. PyYAML is YAML 1.1 and GitHub's parser 1.2: where they
  differ the check fails closed — a tab before a trailing `#` comment, for one, is reported
  as unparseable. A failure naming one of these is the rule, not a broken test (F37, F48).
- **Markdown is read by a CommonMark parser.** `tests/architecture/_docs.py` parses
  README.md and docs/ with markdown-it-py (a `[dev]` dependency, tables and strikethrough
  enabled), so a link counts only where GitHub renders one (F56); `scripts/check_mermaid.py`
  finds the mermaid diagrams in every tracked Markdown file with the same configuration (F57)
  and so runs from the dev venv too. A `.venv` made before it was
  added fails to collect `test_documentation.py`, and pytest then aborts the whole session
  without naming the cause: reinstall with `pip install -e '.[dev,spark,runtime]' -c requirements.txt`.
