"""DatabricksAdapter.get_secret reads a secret scope through dbutils (F221)."""
from __future__ import annotations

from types import SimpleNamespace


def test_get_secret_calls_dbutils_secrets(monkeypatch):
    from spark_quick_start.adapters import databricks_adapter

    calls = []

    def get(*, scope, key):
        calls.append((scope, key))
        return "s3cr3t"

    monkeypatch.setattr(databricks_adapter, "_get_dbutils", lambda: SimpleNamespace(secrets=SimpleNamespace(get=get)))
    assert databricks_adapter.DatabricksAdapter().get_secret("bc-scope", "bc-secret") == "s3cr3t"
    assert calls == [("bc-scope", "bc-secret")]
