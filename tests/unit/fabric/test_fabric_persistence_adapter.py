"""FabricPersistenceAdapter contract check (spark_logging), relocated from
tests/test_spark_logging.py's TestAdapterContract during the platform-scoped
test split.
"""
from __future__ import annotations

from spark_quick_start.spark_logging import FabricPersistenceAdapter


def test_fabric_has_required_methods():
    a = FabricPersistenceAdapter()
    assert callable(a.mkdir)
    assert callable(a.write)
    assert callable(a.copy)
    assert callable(a.notebook_exit)
