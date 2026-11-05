"""Remove the `local` optional-dependency group (and its leading comment
block) from pyproject.toml in place.

Used by filter_platform_release.sh: once local_adapter.py/local_notebookutils
are deleted from a platform-filtered checkout, the `local` extra (delta-spark,
nbformat — installed only to develop/test LocalAdapter) no longer applies.

Plain line-scanning rather than a TOML writer, so comments and formatting
elsewhere in the file are left untouched.
"""
from __future__ import annotations

import sys
from pathlib import Path

PYPROJECT = Path(__file__).resolve().parent.parent / "pyproject.toml"


def strip_local_extra(text: str) -> str:
    lines = text.splitlines(keepends=True)
    start = next((i for i, ln in enumerate(lines) if ln.startswith("local = [")), None)
    if start is None:
        return text  # already stripped or never present — no-op

    # Walk backward over the contiguous leading comment block.
    block_start = start
    while block_start > 0 and lines[block_start - 1].lstrip().startswith("#"):
        block_start -= 1

    # Walk forward to the closing "]" of the local = [...] array.
    end = start
    while end < len(lines) and lines[end].rstrip() != "]":
        end += 1
    if end == len(lines):
        raise ValueError("pyproject.toml: unterminated 'local = [' array")

    return "".join(lines[:block_start] + lines[end + 1:])


def main() -> None:
    text = PYPROJECT.read_text()
    stripped = strip_local_extra(text)
    if stripped != text:
        PYPROJECT.write_text(stripped)
        print(f"stripped 'local' extra from {PYPROJECT}")
    else:
        print(f"no 'local' extra found in {PYPROJECT} — nothing to do")


if __name__ == "__main__":
    main()
    sys.exit(0)
