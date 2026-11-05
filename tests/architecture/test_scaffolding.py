"""
test_scaffolding.py — the architecture suite can find the tree it audits.

Contains: smoke assertions on _arch's anchors and the directory-stamped marker.
Does NOT contain: any boundary rule — those live in the sibling modules.
"""
from __future__ import annotations

import _arch


def test_repo_root_is_found_by_marker_not_by_counting() -> None:
    """_arch.ROOT is the directory holding pyproject.toml."""
    assert (_arch.ROOT / "pyproject.toml").is_file()
    assert (_arch.ROOT / "src" / "spark_quick_start").is_dir()


def test_the_suite_is_stamped_architecture(request) -> None:
    """tests/conftest.py stamps the marker from the directory, never by hand."""
    assert request.node.get_closest_marker("architecture") is not None
