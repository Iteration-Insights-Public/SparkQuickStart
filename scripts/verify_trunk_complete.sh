#!/usr/bin/env bash
# Asserts that this checkout is the complete, platform-neutral trunk — that
# nothing platform-specific has gone missing from it.
#
# Why this exists: release/fabric and release/databricks are build outputs,
# regenerated from main by sync-release-branches.yaml. In August 2026 a feature
# branch was cut from release/databricks instead of from main, so that branch
# carried the databricks *filter* commit; merging it deleted fabric_adapter.py,
# local_adapter.py, local_notebookutils/ and spark_logging/fabric_persistence.py
# from the trunk. LoadOrchestrator(platform="fabric") — the default — then raised
# ModuleNotFoundError, and the sync job regenerated release/fabric from the
# broken trunk, leaving it with no FabricAdapter at all.
#
# Nothing caught it at the time: filter_platform_release.sh skips paths that are
# already absent, and the tests that would have failed (tests/fabric/) were
# deleted by the same commit. Every check passed on a broken trunk.
#
# This guards the *outcome* rather than one cause, so it also catches a bad
# rebase, a careless `git rm`, or any other route to the same damage.
#
# The expected path set is not duplicated here: it is the union of both
# platforms' exclusion lists, read from filter_platform_release.sh itself, which
# stays the single source of truth. Anything one platform strips out must, by
# definition, be present on the trunk both are built from.
#
# Usage: scripts/verify_trunk_complete.sh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

FILTER_SCRIPT="scripts/filter_platform_release.sh"

missing=()

while IFS= read -r path; do
  [ -n "$path" ] || continue
  if [ ! -e "$path" ]; then
    missing+=("$path")
  fi
done < <(
  {
    bash "$FILTER_SCRIPT" fabric --list
    bash "$FILTER_SCRIPT" databricks --list
  } | sort -u
)

# The [local] extra is stripped by _strip_pyproject_local_extra.py during
# filtering, so its absence is the same signal as a missing adapter file.
if ! grep -qE '^local = \[' pyproject.toml; then
  missing+=("pyproject.toml: [project.optional-dependencies] 'local' extra")
fi

if [ ${#missing[@]} -gt 0 ]; then
  cat >&2 <<'EOF'
ERROR: this checkout is not the complete platform-neutral trunk.

The paths listed below are stripped when building a platform release, which
means they must be present on main. Their absence means a platform-filtered
tree has been merged into the trunk.

EOF
  for path in "${missing[@]}"; do
    echo "  missing: $path" >&2
  done
  cat >&2 <<'EOF'

Most likely cause: this branch was cut from release/fabric or
release/databricks. Those are CI build outputs, never a base for new work.

To fix: rebase onto main, or restore the deleted paths, e.g.
    git checkout <last-good-commit> -- <path>
EOF
  exit 1
fi

echo "trunk is complete: all platform-specific paths present"
