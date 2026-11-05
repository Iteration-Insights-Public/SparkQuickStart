#!/usr/bin/env bash
# Deploys the bundle, uploads the landing data, seeds the landing tables, and optionally runs
# the loads. With --landing-only it only uploads and seeds, leaving everything else as it is.
#
# Usage: reference_design/databricks/scripts/deploy.sh [options] [<catalog> <run-as-sp-application-id>]
#
#   <catalog>                    an existing Unity Catalog catalog, such as main
#   <run-as-sp-application-id>   the service principal's application ID (a UUID, not its name)
#                                Both may come from the environment instead (below).
#   -p, --profile <name>         the databricks CLI profile to deploy with
#   --var <name=value>           any other bundle variable, such as node_type=Standard_D4ds_v4;
#                                repeatable
#   --landing-only               upload and seed only: no bundle deploy, so the jobs, schemas,
#                                wheel, bronze tables and load_runs stay as they are
#   --tables <a,b>               upload and seed only these landing folders, such as
#                                IoT_DeviceRegistry (a local data/<name>_delta folder lands as
#                                <name>)
#   --load                       also run load_bronze once the landing tables are seeded
#   --load-iot                   also run load_iot, which seeds the IoT landing table itself;
#                                with --tables IoT_DeviceRegistry alone, seed_landing is skipped
#   --demo                       run the demo job instead of seed_landing: one cluster seeds
#                                every landing table and runs all five loads; needs
#                                data/IoT_DeviceRegistry_delta. Not with --tables, --load or
#                                --load-iot
#
# Settings from the environment, each overridden by its argument when both are given. The
# names are the Databricks CLI's own, so `databricks bundle run` reads them too:
#   DATABRICKS_CONFIG_PROFILE    the CLI profile, which holds the workspace host (-p)
#   BUNDLE_VAR_catalog           the catalog (<catalog>)
#   BUNDLE_VAR_run_as_sp         the service principal's application ID (<run-as-sp-...>)
#   BUNDLE_VAR_<name>            any other bundle variable in databricks.yml, such as
#                                BUNDLE_VAR_node_type or BUNDLE_VAR_schema_prefix (--var)
#
# Prerequisites, each one a failure we hit on a deployment:
#   - The catalog exists and has managed storage (its own, or the metastore's root);
#     the bundle creates schemas in it, never the catalog.
#   - The service principal is in the workspace and has USE CATALOG on the catalog:
#       databricks grants update catalog <catalog> --json \
#         '{"changes":[{"principal":"<sp-application-id>","add":["USE_CATALOG"]}]}'
#     The bundle grants everything inside the schemas it creates.
#   - The deployer has the Service Principal: User role on the service principal
#     (Settings > Identity and access > Service principals > <sp> > Permissions);
#     without it, creating the jobs fails with 403.
#   - The service principal may create clusters: the "Allow unrestricted cluster creation"
#     entitlement (Service principals > <sp> > Configurations), or a cluster policy it can use;
#     without it, the job fails with PERMISSION_DENIED before the cluster starts.
#   - The Azure subscription has core quota in the workspace's region for the node_type's
#     VM family (4 cores); without it, the cluster fails with AZURE_QUOTA_EXCEEDED_EXCEPTION.
#       az vm list-usage --location <region> --subscription <subscription-id> -o table
#   - The deployer is logged in to the workspace as themselves:
#       databricks auth login --host https://<workspace>.azuredatabricks.net
#     ARM_CLIENT_ID, ARM_CLIENT_SECRET and ARM_TENANT_ID in the environment take precedence
#     over a profile, so with a profile this script unsets them.
#   - Python with pip, which builds the library's wheel: the first of python3 and python that
#     runs (on Windows, python3 is often the Microsoft Store placeholder).
#   - bash: on Windows, Git Bash, not the bash.exe that starts WSL.
set -euo pipefail

usage() {
  echo "usage: $0 [-p|--profile <name>] [--var <name=value>]... [--landing-only] [--tables <a,b>]" \
    "[--load] [--load-iot] [--demo] [<catalog> <run-as-sp-application-id>]" >&2
  echo "  <catalog> and <run-as-sp-application-id> may be set as BUNDLE_VAR_catalog and" \
    "BUNDLE_VAR_run_as_sp instead; the profile as DATABRICKS_CONFIG_PROFILE" >&2
  exit 1
}

PROFILE_ARGS=()
EXTRA_VARS=()
LOAD=""
LOAD_IOT=""
DEMO=""
LANDING_ONLY=""
TABLES=""
POSITIONAL=()
while [ $# -gt 0 ]; do
  case "$1" in
    --landing-only)
      LANDING_ONLY="yes"
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
    -p | --profile)
      [ $# -ge 2 ] || usage
      PROFILE_ARGS=(--profile "$2")
      shift 2
      ;;
    --var)
      [ $# -ge 2 ] || usage
      case "$2" in *=*) ;; *) usage ;; esac
      EXTRA_VARS+=(--var "$2")
      shift 2
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

case ${#POSITIONAL[@]} in
  2)
    CATALOG="${POSITIONAL[0]}"
    RUN_AS_SP="${POSITIONAL[1]}"
    ;;
  0)
    CATALOG="${BUNDLE_VAR_catalog:-}"
    RUN_AS_SP="${BUNDLE_VAR_run_as_sp:-}"
    if [ -z "$CATALOG" ] || [ -z "$RUN_AS_SP" ]; then
      echo "the catalog and the run-as service principal are not set: pass both as arguments," \
        "or set BUNDLE_VAR_catalog and BUNDLE_VAR_run_as_sp" >&2
      usage
    fi
    ;;
  *)
    usage
    ;;
esac

# A profile from the environment is passed explicitly too, so every command below uses it
# and the ARM_* variables are cleared for it as for -p.
if [ ${#PROFILE_ARGS[@]} -eq 0 ] && [ -n "${DATABRICKS_CONFIG_PROFILE:-}" ]; then
  PROFILE_ARGS=(--profile "$DATABRICKS_CONFIG_PROFILE")
fi
if [ ${#PROFILE_ARGS[@]} -gt 0 ]; then
  unset ARM_CLIENT_ID ARM_CLIENT_SECRET ARM_TENANT_ID
fi

BUNDLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$BUNDLE_DIR/../.." && pwd)"
cd "$BUNDLE_DIR"

# Checked before anything is deployed: the demo's last task loads the IoT table.
if [ -n "$DEMO" ] && [ ! -d "$REPO_ROOT/data/IoT_DeviceRegistry_delta" ]; then
  echo "--demo loads the IoT table too, and data/IoT_DeviceRegistry_delta is missing; make it with:" >&2
  echo "  python data/generate_iot_device_sample.py --rows 100000 --format delta --partitions 4 --seed 1" >&2
  exit 1
fi

# The first of python3 and python that runs. On Windows python3 is often the Microsoft Store
# placeholder, which exists on PATH but only prints an install hint and exits 49.
PYTHON=""
for candidate in python3 python; do
  if command -v "$candidate" > /dev/null 2>&1 && "$candidate" -c "import sys" > /dev/null 2>&1; then
    PYTHON="$candidate"
    break
  fi
done
if [ -z "$PYTHON" ]; then
  echo "no working python3 or python on PATH; the wheel build and this script need one" >&2
  exit 1
fi

# The caller's own python, from --var python=... or BUNDLE_VAR_python, is kept, and this one
# left out: a --var here would override the environment's.
PYTHON_VAR=(--var "python=$PYTHON")
if [ -n "${BUNDLE_VAR_python:-}" ]; then
  PYTHON_VAR=()
fi
for arg in ${EXTRA_VARS[@]+"${EXTRA_VARS[@]}"}; do
  case "$arg" in python=*) PYTHON_VAR=() ;; esac
done

# "${arr[@]+...}" keeps an empty array from tripping set -u on older bash.
BUNDLE_ARGS=(--target dev ${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"}
  --var "catalog=$CATALOG" --var "run_as_sp=$RUN_AS_SP" ${PYTHON_VAR[@]+"${PYTHON_VAR[@]}"}
  ${EXTRA_VARS[@]+"${EXTRA_VARS[@]}"})

# --landing-only: no deploy, so the jobs, schemas, wheel and every non-landing table stay as
# they are; the bundle must have been deployed before.
if [ -z "$LANDING_ONLY" ]; then
  databricks bundle deploy "${BUNDLE_ARGS[@]}"
fi

# The landing volume's path, as the bundle resolved it. Read from the schema, not the volume:
# the volume's schema_name is a reference to the schema, which validate leaves unresolved.
RAW_DIR="$(databricks bundle validate "${BUNDLE_ARGS[@]}" --output json | "$PYTHON" -c '
import json, sys
resources = json.load(sys.stdin)["resources"]
schema, volume = resources["schemas"]["landing"], resources["volumes"]["raw"]
print("/Volumes/{}/{}/{}".format(schema["catalog_name"], schema["name"], volume["name"]))
')"

# The landing folder name: the local name without a trailing _delta, so
# data/IoT_DeviceRegistry_delta lands as IoT_DeviceRegistry and seeds table iot_deviceregistry.
raw_name() {
  local name
  name="$(basename "$1")"
  echo "${name%_delta}"
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

shopt -s nullglob
folders=()
# The IoT table by its exact name: data/ may hold larger variants beside it
# (IoT_DeviceRegistry_delta_1M, _1B) that must not be uploaded.
for folder in "$REPO_ROOT"/data/Person_* "$REPO_ROOT"/data/Sales_* "$REPO_ROOT"/data/IoT_DeviceRegistry_delta; do
  if [ ! -d "$folder" ]; then
    continue
  fi
  if [ "$WANTED_COUNT" -gt 0 ]; then
    case "$WANTED" in
      *" $(lower "$(raw_name "$folder")") "*) ;;
      *) continue ;;
    esac
  fi
  folders+=("$folder")
done
if [ ${#folders[@]} -eq 0 ]; then
  echo "no data/Person_*, data/Sales_* or data/IoT_DeviceRegistry_delta folder under $REPO_ROOT${TABLES:+ matching --tables $TABLES}" >&2
  exit 1
fi
if [ "$WANTED_COUNT" -gt 0 ] && [ ${#folders[@]} -ne "$WANTED_COUNT" ]; then
  echo "--tables $TABLES: not every name matches a local folder; found:" >&2
  for folder in "${folders[@]}"; do echo "  $(raw_name "$folder")" >&2; done
  exit 1
fi

for folder in "${folders[@]}"; do
  target="dbfs:$RAW_DIR/$(raw_name "$folder")"
  # Removed first: cp --overwrite keeps files the local folder no longer has, and a stale
  # _delta_log entry from an older, regenerated table would be read as the latest version.
  if databricks fs ls ${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"} "$target" > /dev/null 2>&1; then
    echo "removing $target"
    databricks fs rm -r ${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"} "$target"
  fi
  echo "uploading $(basename "$folder") to $target"
  databricks fs cp --recursive --overwrite ${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"} "$folder" "$target"
done

if [ -n "$DEMO" ]; then
  # The demo job seeds every landing table itself, on the cluster that runs the loads.
  databricks bundle run demo "${BUNDLE_ARGS[@]}"
  exit 0
fi

# load_iot seeds the IoT landing table itself, so an IoT-only round skips seed_landing and its
# cluster.
names=()
for folder in "${folders[@]}"; do names+=("$(raw_name "$folder")"); done
if [ -n "$LOAD_IOT" ] && [ -z "$LOAD" ] && [ ${#names[@]} -eq 1 ] && [ "${names[0]}" = "IoT_DeviceRegistry" ]; then
  echo "skipping seed_landing: load_iot seeds IoT_DeviceRegistry"
else
  SEED_ARGS=()
  if [ -n "$TABLES" ]; then
    # Semicolons: bundle run --params separates parameters with commas.
    SEED_ARGS=(--params "tables=$(IFS=';'; echo "${names[*]}")")
  fi
  databricks bundle run seed_landing "${BUNDLE_ARGS[@]}" ${SEED_ARGS[@]+"${SEED_ARGS[@]}"}
fi

if [ -n "$LOAD" ]; then
  databricks bundle run load_bronze "${BUNDLE_ARGS[@]}"
fi
if [ -n "$LOAD_IOT" ]; then
  databricks bundle run load_iot "${BUNDLE_ARGS[@]}"
fi
