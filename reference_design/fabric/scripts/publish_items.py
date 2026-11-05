"""Publish the reference design's Fabric items into a workspace, as the az-logged-in user.

The Fabric form of `databricks bundle deploy`, run by `deploy.sh`. Copies `workspace/` to
`build/fabric_items/`, puts the wheel `deploy.sh` built into the Environment's
`Libraries/CustomLibraries/` there, so the wheel is never committed, and publishes the copy with
`fabric-cicd`. `fabric-cicd` publishes the Lakehouse, then the Environment, then the notebooks;
replaces each item's logical ID in the notebooks' metadata with the deployed item's ID, so no
`parameter.yml` is needed; and waits for the Environment's publish at the end, raising if it
failed.

Usage: python publish_items.py --workspace-id <id> --wheel <path to .whl> [--item-types <a,b>]

`--item-types Environment` publishes the Environment alone: the wheel step of a workspace whose
other items come from Git integration, which never publishes an Environment and never carries
the wheel.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

DESIGN_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = DESIGN_DIR.parent.parent
ITEMS_DIR = DESIGN_DIR / "workspace"
BUILD_DIR = REPO_ROOT / "build" / "fabric_items"
ITEM_TYPES = ("Lakehouse", "Environment", "Notebook")


def stage(wheel: Path) -> Path:
    """Copy the items to BUILD_DIR, with `wheel` as the Environment's only custom library."""
    shutil.rmtree(BUILD_DIR, ignore_errors=True)
    # Bytecode a local import or compile left beside a notebook would be published as a part of it.
    shutil.copytree(ITEMS_DIR, BUILD_DIR, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    libraries = BUILD_DIR / "env_quickstart.Environment" / "Libraries" / "CustomLibraries"
    libraries.mkdir(parents=True)
    shutil.copy2(wheel, libraries / wheel.name)
    return BUILD_DIR


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument("--wheel", required=True, type=Path)
    parser.add_argument("--item-types", default=",".join(ITEM_TYPES),
                        help=f"comma-separated, of {', '.join(ITEM_TYPES)}; default all")
    args = parser.parse_args()
    item_types = [t for t in args.item_types.split(",") if t]
    unknown = sorted(set(item_types) - set(ITEM_TYPES))
    if not item_types or unknown:
        print(f"publish_items: --item-types takes {', '.join(ITEM_TYPES)}; got {args.item_types!r}", file=sys.stderr)
        return 1
    if not args.wheel.is_file():
        print(f"publish_items: no wheel at {args.wheel}", file=sys.stderr)
        return 1
    try:
        from azure.identity import AzureCliCredential
        from fabric_cicd import FabricWorkspace, publish_all_items
    except ImportError as exc:
        print(
            f"publish_items: {exc.name} is not installed; run: pip install fabric-cicd azure-identity",
            file=sys.stderr,
        )
        return 1

    workspace = FabricWorkspace(
        workspace_id=args.workspace_id,
        repository_directory=str(stage(args.wheel)),
        item_type_in_scope=item_types,
        token_credential=AzureCliCredential(),
    )
    publish_all_items(workspace)
    return 0


if __name__ == "__main__":
    sys.exit(main())
