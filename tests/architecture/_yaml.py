"""
_yaml.py — a strict YAML reader for the architecture checks.

Contains: `load_mapping`, which parses YAML with PyYAML's SafeLoader but refuses a mapping
    that repeats a key, and returns the document or why it is not a mapping.
Does NOT contain: any assertion, or any rule of a particular file format — GitHub's `on:`
    quirk stays in test_ci_workflows.py, which wraps this.

PyYAML silently keeps the last of two equal keys; the platforms these files are written for
(GitHub Actions, Claude Code) refuse or disagree, so a check reading the last one would vouch
for a value nobody else reads (F48, F103). PyYAML is a [dev] dependency, test-only.
"""
from __future__ import annotations

from typing import Any

import yaml


class StrictLoader(yaml.SafeLoader):
    """SafeLoader that refuses a mapping with the same key twice, or a key no dict can hold."""


def _no_duplicate_keys(loader: StrictLoader, node: yaml.MappingNode, deep: bool = False) -> dict:
    seen: set[Any] = set()
    for key_node, _ in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in seen
            seen.add(key)  # a set is "in" a set without hashing, but cannot be added (!!set)
        except TypeError:
            # A YAML complex key (`? [a]`, `? {a: 1}`, `!!set`) constructs to a list, dict or
            # set: no Python mapping holds it, and a check must report the file, not crash (F136).
            raise yaml.constructor.ConstructorError(
                None, None, f"unhashable key of type {type(key).__name__}", key_node.start_mark
            ) from None
        if duplicate:
            raise yaml.constructor.ConstructorError(
                None, None, f"duplicate key {key!r}", key_node.start_mark)
    return yaml.SafeLoader.construct_mapping(loader, node, deep)


StrictLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _no_duplicate_keys)


def load_mapping(text: str) -> tuple[dict, None] | tuple[None, str]:
    """*text* as a YAML mapping, or why it is not one.

    Only YAML's standard tags construct (no !!python/object), plus the duplicate-key refusal.
    """
    try:
        doc = yaml.load(text, Loader=StrictLoader)
    except yaml.YAMLError as exc:
        return None, f"not valid YAML: {str(exc).splitlines()[0]}"
    if not isinstance(doc, dict):
        return None, "not a YAML mapping at the top level"
    return doc, None
