"""FabricAdapter.get_secret reads Key Vault through notebookutils, imported lazily (F221)."""
from __future__ import annotations

import sys
from types import SimpleNamespace


def test_get_secret_calls_notebookutils_credentials(monkeypatch):
    from spark_quick_start.adapters.fabric_adapter import FabricAdapter

    calls = []

    def get_secret(vault, name):
        calls.append((vault, name))
        return "s3cr3t"

    monkeypatch.setitem(sys.modules, "notebookutils", SimpleNamespace(credentials=SimpleNamespace(getSecret=get_secret)))
    assert FabricAdapter().get_secret("https://v.vault.azure.net/", "bc-secret") == "s3cr3t"
    assert calls == [("https://v.vault.azure.net/", "bc-secret")]
