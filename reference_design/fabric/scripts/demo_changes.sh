#!/usr/bin/env bash
# One round of the demo, all of it in Fabric: changes the IoT data and loads it into bronze.
#
# Usage: reference_design/fabric/scripts/demo_changes.sh [--new N] [--update M] [<workspace>]
#
#   --new N       new devices to add (default 1000)
#   --update M    existing devices to give a new firmware_version (default 100)
#   <workspace>   as for deploy.sh; may come from FABRIC_WORKSPACE instead
#
# The round is nb_orchestrate with job=iot_changes, in one session:
#   1. iot_generate (mode=changes) changes M devices and adds N in Files/raw/IoT_DeviceRegistry,
#      all with a modified_at after the table's max(modified_at), the last watermark.
#   2. seed_landing seeds landing.iot_deviceregistry from it, and load_entity loads it.
# Nothing is uploaded and no local Spark runs; the same round is nb_orchestrate's job=iot_changes
# run from the workspace.
#
# Expect in control.load_runs for IoT_DeviceRegistry: inserted_count N + M (the new devices and
# the new versions of the updated ones), updated_count M (the closed versions), and
# hwm_value_before equal to the previous run's hwm_value_after.
#
# Needs the items published and the first load run once (deploy.sh --demo).
set -euo pipefail

usage() {
  echo "usage: $0 [--new N] [--update M] [<workspace>]" >&2
  exit 1
}

NEW=1000
UPDATE=100
POSITIONAL=()
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
    -*)
      usage
      ;;
    *)
      POSITIONAL+=("$1")
      shift
      ;;
  esac
done
case "$NEW$UPDATE" in
  *[!0-9]*) echo "--new and --update take whole numbers" >&2; usage ;;
esac
case ${#POSITIONAL[@]} in
  1) WORKSPACE="${POSITIONAL[0]}" ;;
  0)
    WORKSPACE="${FABRIC_WORKSPACE:-}"
    [ -n "$WORKSPACE" ] || { echo "the workspace is not set: pass it as an argument, or set FABRIC_WORKSPACE" >&2; usage; }
    ;;
  *) usage ;;
esac

SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_fabric.sh
. "$SCRIPTS_DIR/_fabric.sh"
check_signed_in "$WORKSPACE"

run_notebook nb_orchestrate -P "job:string=iot_changes,new:int=$NEW,update:int=$UPDATE"

echo "== expect in control.load_runs for IoT_DeviceRegistry:" \
  "inserted_count $((NEW + UPDATE)), updated_count $UPDATE"
