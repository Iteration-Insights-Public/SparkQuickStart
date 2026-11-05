"""_frames.py — assertion helper for tests that pass a MagicMock DataFrame through a transformer.

Imported as ``from _frames import only_partition_probe``: pytest puts each test directory on
``sys.path`` and the tests have no ``__init__.py``. Not a test module, so it is neither
collected nor classified.
"""
from __future__ import annotations

from typing import Any


def only_partition_probe(frame: Any) -> None:
    """Assert the only thing that read *frame* was the base hooks' partition probe.

    A frame that goes through a transformer's ``__call__`` or ``produce()`` gets
    ``df.rdd.getNumPartitions()`` from ``_safe_num_partitions`` (``capture_partitions`` defaults
    to True), so ``assert not frame.method_calls`` cannot hold there. Anything else recorded
    (``filter``, ``count``, ``columns`` access via a call, ...) means the step read data the test
    claims it only carried.
    """
    seen = {call[0] for call in frame.method_calls}
    assert seen <= {"rdd.getNumPartitions"}, frame.method_calls
