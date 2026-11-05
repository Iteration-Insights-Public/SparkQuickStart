#!/usr/bin/env bash
# Deletes the source/test paths that don't belong in a platform-specific
# release build. Single source of truth for what "fabric-only" and
# "databricks-only" mean — called both by the platform-filtered builds
# (.github/workflows/platform-builds.yaml, run by hand since F135) and by the
# release-branch sync job (.github/workflows/sync-release-branches.yaml), so
# the two can never disagree about the exclusion list.
#
# Usage: scripts/filter_platform_release.sh <fabric|databricks> [--list]
# Run from the repo root, against a checkout you're OK mutating in place
# (this script deletes files).
#
# --list prints the exclusion paths, one per line, and deletes nothing. That
# keeps the lists below the single source of truth for verify_trunk_complete.sh
# too: the union of both platforms' exclusions is exactly the set of
# platform-specific paths that must exist on the trunk.
set -euo pipefail

PLATFORM="${1:-}"
MODE="${2:-delete}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

case "$PLATFORM" in
  fabric)
    EXCLUDE_PATHS=(
      "src/spark_quick_start/adapters/databricks_adapter.py"
      "src/spark_quick_start/adapters/local_adapter.py"
      "src/spark_quick_start/local_notebookutils"
      "src/spark_quick_start/spark_logging/databricks_persistence.py"
      # NOTE: spark_logging/local_persistence.py is NOT excluded here.
      # LocalFilePersistenceAdapter is platform-neutral, not local-only:
      # OrchestratorLogger.from_logging_context() uses it on every platform
      # whenever log_file_dir is a locally-mounted path (/Volumes/...,
      # /dbfs/..., /tmp/...) — unlike LocalAdapter/local_notebookutils,
      # which really are local-dev/CI-only.
      "tests/unit/databricks"
      "tests/unit/local"
      "tests/integration/local"
      # The architecture suite asserts properties of the complete trunk (that
      # every filtered path exists, that no instruction document names a
      # missing path). Both are false by construction on a platform-filtered
      # checkout, which has just deleted paths on purpose — so the suite has
      # no meaning here and is stripped, not merely expected to fail.
      "tests/architecture"
    )
    ;;
  databricks)
    EXCLUDE_PATHS=(
      "src/spark_quick_start/adapters/fabric_adapter.py"
      "src/spark_quick_start/adapters/local_adapter.py"
      "src/spark_quick_start/local_notebookutils"
      "src/spark_quick_start/spark_logging/fabric_persistence.py"
      # See the fabric case above — local_persistence.py stays.
      "tests/unit/fabric"
      "tests/unit/local"
      "tests/integration/local"
      # See the fabric case above: the architecture suite describes the
      # complete trunk and is meaningless on a filtered checkout.
      "tests/architecture"
    )
    ;;
  *)
    echo "usage: $0 <fabric|databricks> [--list]" >&2
    exit 1
    ;;
esac

if [ "$MODE" = "--list" ]; then
  printf '%s\n' "${EXCLUDE_PATHS[@]}"
  exit 0
fi

if [ "$MODE" != "delete" ]; then
  echo "usage: $0 <fabric|databricks> [--list]" >&2
  exit 1
fi

for path in "${EXCLUDE_PATHS[@]}"; do
  if [ -e "$path" ]; then
    rm -rf "$path"
    echo "removed: $path"
  fi
done

# The 'local' extra (delta-spark/nbformat, for local_adapter.py dev/CI use)
# no longer applies once local_adapter.py itself is gone from this checkout.
python3 "$REPO_ROOT/scripts/_strip_pyproject_local_extra.py"

echo "filtered for platform: $PLATFORM"
