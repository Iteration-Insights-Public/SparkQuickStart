"""
test_yaml_loader.py — the strict YAML reader the architecture checks share, tested on its own.

Contains: `_yaml.load_mapping` driven with inputs the loader must reject, report as
    unreadable, or still accept; no repo file is read.
Does NOT contain: any check of a particular file format — those live in the tests that wrap it.

A helper that every check trusts to say "this file is unreadable" has to say it for every
input, not crash on some (F136).
"""
from __future__ import annotations

import _yaml
import pytest

# Keys YAML 1.1 allows that a Python dict cannot hold: each constructs to a list, a dict or a set.
_UNHASHABLE = {
    "a flow-sequence key": "? [a]\n: 1\n",
    "a block-sequence key": "? - a\n  - b\n: 1\n",
    "a mapping key": "? {a: 1}\n: 1\n",
    "a block-mapping key": "? a: 1\n  b: 2\n: 1\n",
    "an unhashable nested in a sequence key": "? [a, [b]]\n: 1\n",
    "a mapping nested in a sequence key": "? [a, {b: 1}]\n: 1\n",
    "a !!set key": "? !!set {a, b}\n: 1\n",
    "an !!omap key": "? !!omap [a: 1]\n: 1\n",
    "an unhashable key after valid ones": "ok: 1\nfine: 2\n? [a]\n: 3\n",
    "an unhashable key in a nested mapping": "outer:\n  ? [a]\n  : 1\n",
    "an unhashable key twice": "? [a]\n: 1\n? [a]\n: 2\n",
}

# Explicit or non-string keys that hash, and must keep loading.
_HASHABLE = {
    "an explicit scalar key": ("? a\n: 1\n", {"a": 1}),
    "an integer key": ("1: x\n", {1: "x"}),
    "a bare on key": ("on: x\n", {True: "x"}),
    "a null key": ("? \n: 1\n", {None: 1}),
    "a date key": ("2001-01-01: x\n", None),
    "a binary key": ("? !!binary aGk=\n: 1\n", {b"hi": 1}),
}


@pytest.mark.parametrize("label", _UNHASHABLE)
def test_an_unhashable_key_is_reported_not_raised(label: str) -> None:
    doc, error = _yaml.load_mapping(_UNHASHABLE[label])
    assert doc is None, label
    assert error is not None and error.startswith("not valid YAML: "), (label, error)
    assert "unhashable" in error, (label, error)


@pytest.mark.parametrize("label", _HASHABLE)
def test_a_hashable_complex_key_is_still_accepted(label: str) -> None:
    text, expected = _HASHABLE[label]
    doc, error = _yaml.load_mapping(text)
    assert error is None, (label, error)
    assert doc is not None
    if expected is not None:
        assert doc == expected


def test_a_duplicate_key_is_still_reported() -> None:
    for text in ("a: 1\na: 2\n", "? a\n: 1\na: 2\n", "outer:\n  k: 1\n  k: 2\n", "1: x\n1: y\n"):
        doc, error = _yaml.load_mapping(text)
        assert doc is None and error is not None, text
        assert "duplicate key" in error, (text, error)


def test_the_unhashable_error_names_the_key_type() -> None:
    _, error = _yaml.load_mapping("? [a]\n: 1\n")
    assert error is not None and "list" in error
