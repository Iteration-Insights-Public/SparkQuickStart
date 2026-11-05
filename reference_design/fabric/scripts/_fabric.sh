# Shared by deploy.sh and demo_changes.sh; sourced, not run. Defines:
#   find_python               sets PYTHON to the first of python3 and python that runs
#   fab_exists <path>         succeeds when fab reports the path exists
#   check_signed_in <ws>      fab and az on PATH and one principal, <ws> visible;
#                             sets WS_PATH and LH_PATH
#   run_notebook <nb> [-P ..] runs <nb> in WS_PATH with fab job run, and fails unless it completed

# The first of python3 and python that runs. On Windows python3 is often the Microsoft Store
# placeholder, which exists on PATH but only prints an install hint and exits 49.
find_python() {
  PYTHON=""
  local candidate
  for candidate in python3 python; do
    if command -v "$candidate" > /dev/null 2>&1 && "$candidate" -c "import sys" > /dev/null 2>&1; then
      PYTHON="$candidate"
      return 0
    fi
  done
  echo "no working python3 or python on PATH; the wheel build and this script need one" >&2
  exit 1
}

# fab exists exits 0 either way and prints "* true" or "* false" between CRLF line ends, so the
# answer is read as a word, not compared as the whole output.
fab_exists() {
  fab exists "$1" 2> /dev/null | tr -d '\r' | grep -w true > /dev/null
}

check_signed_in() {
  local workspace="$1" tool fab_principal az_principal
  for tool in fab az; do
    if ! command -v "$tool" > /dev/null 2>&1; then
      echo "$tool is not on PATH; see the prerequisites in reference_design/fabric/README.md" >&2
      exit 1
    fi
  done

  # fab and az must be one principal: fab checks the workspace and runs the notebooks, while
  # azcopy and fabric-cicd act as the az user. A workspace one of them created can be invisible
  # to the other.
  fab_principal="$(fab auth status 2> /dev/null | sed -n 's/^ *Principal ID: *//p' | tr -d '[:space:]')"
  az_principal="$(az ad signed-in-user show --query id -o tsv 2> /dev/null | tr -d '[:space:]' || true)"
  if [ -z "$fab_principal" ] || [ -z "$az_principal" ] || [ "$fab_principal" != "$az_principal" ]; then
    echo "fab and az are not signed in as the same user: fab '${fab_principal:-none}', az '${az_principal:-none}'." >&2
    echo "  Sign both in as yourself: fab auth login (interactive browser), az login." >&2
    exit 1
  fi

  WS_PATH="$workspace.Workspace"
  LH_PATH="$WS_PATH/lh_quickstart.Lakehouse"
  if ! fab_exists "$WS_PATH"; then
    echo "workspace $workspace not found, or not visible to you; create it on a Fabric capacity first" >&2
    exit 1
  fi
}

# fab job run waits, and exits 1 on a failed run but 0 on a timeout or a cancelled one, so
# success is read from its own "completed" line rather than from the exit code alone.
run_notebook() {
  local notebook="$1" log
  shift
  log="$(mktemp)"
  echo "running $notebook $*"
  fab job run "$WS_PATH/$notebook.Notebook" "$@" --timeout 10800 2>&1 | tee "$log"
  if ! grep -q "Job instance '[^']*' completed" "$log"; then
    rm -f "$log"
    echo "$notebook did not complete; see its run in the workspace's Monitor hub" >&2
    exit 1
  fi
  rm -f "$log"
}
