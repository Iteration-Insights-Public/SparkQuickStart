"""
test_release_filter.py — executable form of .claude/rules/release-and-branching.md.

Contains: assertions that filter_platform_release.sh lists every platform-specific
    path and nothing that does not exist.
Does NOT contain: branch-policy checks — a branch's parent is not visible from the tree.
"""
from __future__ import annotations

import _arch

_SCRIPT = _arch.ROOT / "scripts" / "filter_platform_release.sh"

# Anything matching one of these is platform-specific and must be filtered.
#
# The third element of each tuple exempts a file that matches the suffix but is
# platform-*neutral* — the ABC that the per-platform implementations subclass. That is also
# where to add a genuinely neutral module if one appears; extending the suffix or the
# directory would be the wrong lever.
#
# `spark_logging/` needs no exemption: its ABC is `persistence_adapter.py`, which does not
# end in `_persistence.py`, so the suffix never matches it and the entry that used to sit
# here was dead — it read as load-bearing and was not (F19 items 10 and 11).
_PLATFORM_PATTERNS = (
    ("src/spark_quick_start/adapters/", "_adapter.py", {"platform_adapter.py"}),
    ("src/spark_quick_start/spark_logging/", "_persistence.py", set()),
)


def _script_text() -> str:
    return _SCRIPT.read_text(encoding="utf-8")


def test_every_filtered_path_exists() -> None:
    """A stale entry makes verify_trunk_complete.sh assert against a path nobody ships.

    Resolved case-sensitively (M2, 2026-09-25 review) — see `_arch.resolves_case_sensitively`.
    """
    text = _script_text()
    quoted = [
        line.strip().strip('"')
        for line in text.splitlines()
        if line.strip().startswith('"') and line.strip().endswith('"')
    ]
    missing = [entry for entry in quoted if not _arch.resolves_case_sensitively(_arch.ROOT, entry)]
    assert not missing, (
        "filter_platform_release.sh lists a path that is not in the tree. It is the "
        "single source of truth for what fabric-only and databricks-only mean, and "
        f"verify_trunk_complete.sh reads its lists: {missing}"
    )


def test_every_platform_specific_module_is_filtered() -> None:
    """A new platform file not added to the script makes verify_trunk_complete.sh lie."""
    text = _script_text()
    unlisted = []
    for directory, suffix, exempt in _PLATFORM_PATTERNS:
        for path in sorted((_arch.ROOT / directory).glob(f"*{suffix}")):
            if path.name in exempt:
                continue
            # local_persistence.py is deliberately not filtered — it is
            # platform-neutral and used whenever log_file_dir is a mounted path.
            if path.name == "local_persistence.py":
                continue
            if _arch.rel(path) not in text:
                unlisted.append(_arch.rel(path))
    assert not unlisted, (
        "a platform-specific module is not listed in filter_platform_release.sh. "
        "Add it in the same change that creates it, or verify_trunk_complete.sh is "
        f"lying (rule: release-and-branching): {unlisted}"
    )


def test_every_platform_scoped_test_directory_is_filtered() -> None:
    """Platform test directories are stripped by exact path."""
    text = _script_text()
    unlisted = [
        f"tests/{suite}/{platform}"
        for suite in ("unit", "integration")
        for platform in ("fabric", "databricks", "local")
        if (_arch.ROOT / "tests" / suite / platform).is_dir()
        and f"tests/{suite}/{platform}" not in text
    ]
    assert not unlisted, (
        "a platform-scoped test directory is not stripped by "
        f"filter_platform_release.sh (rule: test-classification, Mechanics): {unlisted}"
    )
