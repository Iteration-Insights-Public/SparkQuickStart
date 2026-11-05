#!/usr/bin/env bash
# One round of the demo: changes the local IoT table, uploads it, and loads it into bronze.
#
# Usage: reference_design/databricks/scripts/demo_changes.sh [--new N] [--update M] [deploy.sh options]
#
#   --new N       new devices to add (default 1000)
#   --update M    existing devices to give a new firmware_version (default 100)
#   Every other argument goes to deploy.sh as it is, such as -p <profile> or the catalog and the
#   service principal; see deploy.sh for those and for the settings it reads from the environment.
#
# The round:
#   1. data/generate_iot_device_sample.py --append adds N devices and updates M existing ones,
#      all with a modified_at after the table's current max(modified_at), the last watermark.
#   2. deploy.sh --landing-only --tables IoT_DeviceRegistry --load-iot uploads the table to the
#      raw volume and runs load_iot, which seeds the landing table and loads it, on one cluster.
#
# Expect in control.load_runs for IoT_DeviceRegistry: inserted_count N + M (the new devices and
# the new versions of the updated ones), updated_count M (the closed versions), and
# hwm_value_before equal to the previous run's hwm_value_after.
#
# Needs the bundle deployed and data/IoT_DeviceRegistry_delta loaded once (deploy.sh --demo), and
# a Python with pyspark and delta-spark for the generator.
set -euo pipefail

usage() {
  echo "usage: $0 [--new N] [--update M] [deploy.sh options]" >&2
  exit 1
}

NEW=1000
UPDATE=100
DEPLOY_ARGS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --new)
      [ $# -ge 2 ] || usage
      NEW="$2"
      shift 2
      ;;
    --update)
      [ $# -ge 2 ] || usage
      UPDATE="$2"
      shift 2
      ;;
    --landing-only | --tables | --load | --load-iot | --demo)
      echo "$1: demo_changes.sh chooses deploy.sh's mode itself" >&2
      usage
      ;;
    *)
      DEPLOY_ARGS+=("$1")
      shift
      ;;
  esac
done
case "$NEW$UPDATE" in
  *[!0-9]*) echo "--new and --update take whole numbers" >&2; usage ;;
esac

SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPTS_DIR/../../.." && pwd)"
TABLE="$REPO_ROOT/data/IoT_DeviceRegistry_delta"
if [ ! -d "$TABLE" ]; then
  echo "no $TABLE: run deploy.sh --demo first, after making it with" >&2
  echo "  python data/generate_iot_device_sample.py --rows 100000 --format delta --partitions 4 --seed 1" >&2
  exit 1
fi

# The first of python3 and python that runs, as deploy.sh picks it.
PYTHON=""
for candidate in python3 python; do
  if command -v "$candidate" > /dev/null 2>&1 && "$candidate" -c "import sys" > /dev/null 2>&1; then
    PYTHON="$candidate"
    break
  fi
done
if [ -z "$PYTHON" ]; then
  echo "no working python3 or python on PATH" >&2
  exit 1
fi

echo "== 1. $NEW new and $UPDATE updated devices in $TABLE"
"$PYTHON" "$REPO_ROOT/data/generate_iot_device_sample.py" \
  --rows "$NEW" --update "$UPDATE" --format delta --append --output "$TABLE"

echo "== 2. upload and load_iot"
"$SCRIPTS_DIR/deploy.sh" --landing-only --tables IoT_DeviceRegistry --load-iot \
  ${DEPLOY_ARGS[@]+"${DEPLOY_ARGS[@]}"}

echo "== expect in control.load_runs for IoT_DeviceRegistry:" \
  "inserted_count $((NEW + UPDATE)), updated_count $UPDATE"
