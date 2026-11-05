#!/usr/bin/env bash
# Publishes the Fabric items, uploads the landing data, seeds the landing tables, and optionally
# runs the loads. With --landing-only it only uploads and seeds, leaving everything else as it is.
#
# Usage: reference_design/fabric/scripts/deploy.sh [options] [<workspace>]
#
#   <workspace>                  an existing Fabric workspace on a capacity, by name, such as
#                                dev_<you>; may come from FABRIC_WORKSPACE instead
#   --landing-only               upload and seed only: no publish, so the notebooks, Environment,
#                                wheel, bronze tables and load_runs stay as they are
#   --wheel-only                 build the wheel and publish the Environment with it, nothing
#                                else: the step after each Update in a workspace whose items come
#                                from Git integration. Not with any other option
#   --tables <a,b>               upload and seed only these AdventureWorks landing folders, such
#                                as Person_Person
#   --load                       also run nb_orchestrate job=load_bronze once landing is seeded
#   --load-iot                   also run nb_orchestrate job=load_iot, which seeds the IoT landing
#                                table itself from the data iot_generate made
#   --demo                       run nb_orchestrate job=demo instead of seed_landing: one session
#                                makes the IoT data when it is absent, seeds every landing table
#                                and runs all five loads. Not with --tables, --load or --load-iot
#
# The IoT data is made in Fabric by the iot_generate notebook, never uploaded: demo makes the
# first 100000 devices, and demo_changes.sh runs a round of changes.
#
# Every run is as the signed-in user: no service principal, no workspace identity.
#
# Prerequisites, each one a failure we hit on the first live run:
#   - The workspace exists on a Fabric capacity (F SKU or trial), not a Power BI Premium Per User
#     one; this script never creates it.
#   - The Fabric CLI, signed in as yourself:  pip install ms-fabric-cli; fab auth login
#     (choose the interactive browser login, not a service principal).
#   - The Azure CLI, signed in as the same user:  az login. azcopy and fabric-cicd take its
#     token. This script stops when fab and az are signed in as different principals: the
#     upload then fails with ArtifactNotFound or 403, which do not name the cause.
#   - azcopy: fab cp uploads single files only, so the Delta folders go up with azcopy, on
#     OneLake's dfs endpoint (the blob endpoint encodes the path's slashes and fails with 404).
#   - fabric-cicd and azure-identity in the python that runs this script:
#       pip install fabric-cicd azure-identity
#   - Python with pip, which builds the library's wheel: the first of python3 and python that
#     runs (on Windows, python3 is often the Microsoft Store placeholder).
#   - bash: on Windows, Git Bash, not the bash.exe that starts WSL.
set -euo pipefail

usage() {
  echo "usage: $0 [--landing-only] [--tables <a,b>] [--load] [--load-iot] [--demo] [<workspace>]" >&2
  echo "       $0 --wheel-only [<workspace>]" >&2
  echo "  <workspace> may be set as FABRIC_WORKSPACE instead" >&2
  exit 1
}

LOAD=""
LOAD_IOT=""
DEMO=""
LANDING_ONLY=""
WHEEL_ONLY=""
TABLES=""
POSITIONAL=()
while [ $# -gt 0 ]; do
  case "$1" in
    --landing-only)
      LANDING_ONLY="yes"
      shift
      ;;
    --wheel-only)
      WHEEL_ONLY="yes"
      shift
      ;;
    --tables)
      [ $# -ge 2 ] && [ -n "$2" ] || usage
      TABLES="$2"
      shift 2
      ;;
    --load-iot)
      LOAD_IOT="yes"
      shift
      ;;
    --demo)
      DEMO="yes"
      shift
      ;;
    --load)
      LOAD="yes"
      shift
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
if [ -n "$DEMO" ] && { [ -n "$TABLES" ] || [ -n "$LOAD" ] || [ -n "$LOAD_IOT" ]; }; then
  echo "--demo seeds every landing table and runs every load: not with --tables, --load or --load-iot" >&2
  usage
fi

if [ -n "$WHEEL_ONLY" ] && { [ -n "$LANDING_ONLY" ] || [ -n "$TABLES" ] || [ -n "$LOAD" ] || [ -n "$LOAD_IOT" ] || [ -n "$DEMO" ]; }; then
  echo "--wheel-only publishes the Environment and nothing else: not with another option" >&2
  usage
fi

case ${#POSITIONAL[@]} in
  1) WORKSPACE="${POSITIONAL[0]}" ;;
  0)
    WORKSPACE="${FABRIC_WORKSPACE:-}"
    if [ -z "$WORKSPACE" ]; then
      echo "the workspace is not set: pass it as an argument, or set FABRIC_WORKSPACE" >&2
      usage
    fi
    ;;
  *) usage ;;
esac

DESIGN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$DESIGN_DIR/../.." && pwd)"
cd "$REPO_ROOT"
# shellcheck source=_fabric.sh
. "$DESIGN_DIR/scripts/_fabric.sh"

find_python
check_signed_in "$WORKSPACE"

# Builds the wheel from this checkout and publishes the items of the given types with it;
# fabric-cicd waits for the Environment's publish and raises on failure, and the state it left is
# read here too, so a run never starts on the last published wheel by accident.
publish() {
  local workspace_id state
  workspace_id="$(fab get "$WS_PATH" -q id 2> /dev/null | tr -d '"[:space:]')"
  rm -rf build/fabric_wheel
  "$PYTHON" -m pip wheel --no-deps --wheel-dir build/fabric_wheel .
  "$PYTHON" "$DESIGN_DIR/scripts/publish_items.py" --workspace-id "$workspace_id" \
    --wheel "$(ls build/fabric_wheel/*.whl)" --item-types "$1"
  state="$(fab get "$WS_PATH/env_quickstart.Environment" -q properties.publishDetails.state 2> /dev/null | tr -d '"[:space:]')"
  if [ "$state" != "Success" ]; then
    echo "env_quickstart's publish state is '${state:-unknown}', not Success; see the Environment in the workspace" >&2
    exit 1
  fi
}

# --wheel-only: the Environment alone. In a workspace connected to Git, the notebooks and the
# lakehouse come from Update; the wheel never does.
if [ -n "$WHEEL_ONLY" ]; then
  if ! fab_exists "$WS_PATH/env_quickstart.Environment"; then
    echo "env_quickstart not found in $WORKSPACE; publish the items first (Update from Git, or deploy.sh)" >&2
    exit 1
  fi
  publish Environment
  echo "env_quickstart published with the wheel from this checkout"
  exit 0
fi

if ! command -v azcopy > /dev/null 2>&1; then
  echo "azcopy is not on PATH; the landing folders go up with it (see reference_design/fabric/README.md)" >&2
  exit 1
fi

# --landing-only: no publish, so the notebooks, Environment, wheel and every non-landing table
# stay as they are; the items must have been published before.
if [ -z "$LANDING_ONLY" ]; then
  publish Lakehouse,Environment,Notebook
fi
if ! fab_exists "$LH_PATH"; then
  echo "lh_quickstart not found in $WORKSPACE; run once without --landing-only to publish it" >&2
  exit 1
fi

# OneLake's dfs endpoint, which azcopy treats as ADLS Gen2: folders, and no encoded slashes.
ONELAKE="https://onelake.dfs.fabric.microsoft.com/$("$PYTHON" -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1]))' "$WORKSPACE")/lh_quickstart.Lakehouse/Files"
export AZCOPY_AUTO_LOGIN_TYPE=AZCLI
AZCOPY_ARGS=(--recursive --trusted-microsoft-suffixes=onelake.dfs.fabric.microsoft.com)

# Removed first, then copied: a copy keeps files the local folder no longer has, and a stale
# _delta_log entry from an older, regenerated table would be read as the latest version.
upload() {
  local source="$1" files_path="$2"
  if fab_exists "$LH_PATH/Files/$files_path"; then
    echo "removing Files/$files_path"
    azcopy remove "$ONELAKE/$files_path" "${AZCOPY_ARGS[@]}"
  fi
  echo "uploading $source to Files/$files_path"
  azcopy copy "$source/*" "$ONELAKE/$files_path/" "${AZCOPY_ARGS[@]}"
}

lower() { echo "$1" | tr '[:upper:]' '[:lower:]'; }

# --tables: landing folder names, case-insensitive, separated by commas. Kept as one
# space-delimited string of distinct lower-case names (no associative arrays: bash 3).
WANTED=" "
WANTED_COUNT=0
for name in $(echo "$TABLES" | tr ',' ' '); do
  name="$(lower "$name")"
  case "$WANTED" in
    *" $name "*) ;;
    *) WANTED="$WANTED$name "; WANTED_COUNT=$((WANTED_COUNT + 1)) ;;
  esac
done

case "$WANTED" in
  *" iot_deviceregistry "*)
    echo "--tables IoT_DeviceRegistry: the IoT data is made in Fabric, not uploaded; run demo_changes.sh for a round" >&2
    exit 1
    ;;
esac

shopt -s nullglob
folders=()
for folder in "$REPO_ROOT"/data/Person_* "$REPO_ROOT"/data/Sales_*; do
  if [ ! -d "$folder" ]; then
    continue
  fi
  if [ "$WANTED_COUNT" -gt 0 ]; then
    case "$WANTED" in
      *" $(lower "$(basename "$folder")") "*) ;;
      *) continue ;;
    esac
  fi
  folders+=("$folder")
done
if [ ${#folders[@]} -eq 0 ]; then
  echo "no data/Person_* or data/Sales_* folder under $REPO_ROOT${TABLES:+ matching --tables $TABLES}" >&2
  exit 1
fi
if [ "$WANTED_COUNT" -gt 0 ] && [ ${#folders[@]} -ne "$WANTED_COUNT" ]; then
  echo "--tables $TABLES: not every name matches a local folder; found:" >&2
  for folder in "${folders[@]}"; do echo "  $(basename "$folder")" >&2; done
  exit 1
fi

upload "$DESIGN_DIR/contexts" contexts
for folder in "${folders[@]}"; do
  upload "$folder" "raw/$(basename "$folder")"
done

if [ -n "$DEMO" ]; then
  # The demo makes the IoT data and seeds every landing table itself, in the session that runs
  # the loads.
  run_notebook nb_orchestrate -P job:string=demo
  exit 0
fi

names=()
for folder in "${folders[@]}"; do names+=("$(basename "$folder")"); done
SEED_ARGS=()
if [ -n "$TABLES" ]; then
  # Semicolons: fab job run -P separates parameters with commas.
  SEED_ARGS=(-P "tables:string=$(IFS=';'; echo "${names[*]}")")
fi
run_notebook seed_landing ${SEED_ARGS[@]+"${SEED_ARGS[@]}"}

if [ -n "$LOAD" ]; then
  run_notebook nb_orchestrate -P job:string=load_bronze
fi
if [ -n "$LOAD_IOT" ]; then
  run_notebook nb_orchestrate -P job:string=load_iot
fi
