"""PlatformAdapter.get_secret: the default refusal, MockPlatformAdapter and LocalAdapter (F221)."""
from __future__ import annotations

import pytest

from spark_quick_start.adapters import LocalAdapter, MockPlatformAdapter, PlatformAdapter


class _Bare(PlatformAdapter):
    def run_notebook(self, name, timeout, args):  # type: ignore[no-untyped-def]
        return ""

    def run_notebooks_parallel(self, dag):  # type: ignore[no-untyped-def]
        return {}

    def delete_path(self, path):  # type: ignore[no-untyped-def]
        return None

    def get_current_notebook_name(self):  # type: ignore[no-untyped-def]
        return None


def test_the_default_refuses_naming_the_class():
    with pytest.raises(NotImplementedError, match="_Bare does not implement get_secret"):
        _Bare().get_secret("scope", "key")


@pytest.mark.parametrize("cls", [MockPlatformAdapter, LocalAdapter])
def test_returns_the_mapped_secret(cls):
    adapter = cls(secrets={("vault", "bc-secret"): "s3cr3t"})
    assert adapter.get_secret("vault", "bc-secret") == "s3cr3t"


@pytest.mark.parametrize("cls", [MockPlatformAdapter, LocalAdapter])
def test_a_missing_secret_raises_keyerror_without_the_values(cls):
    adapter = cls(secrets={("vault", "other"): "s3cr3t"})
    with pytest.raises(KeyError) as raised:
        adapter.get_secret("vault", "bc-secret")
    assert "s3cr3t" not in str(raised.value)


@pytest.mark.parametrize("cls", [MockPlatformAdapter, LocalAdapter])
def test_no_secrets_given_raises_keyerror(cls):
    with pytest.raises(KeyError):
        cls().get_secret("vault", "bc-secret")


def test_mock_records_the_scope_and_key_never_the_value():
    adapter = MockPlatformAdapter(secrets={("vault", "bc-secret"): "s3cr3t"})
    adapter.get_secret("vault", "bc-secret")
    assert adapter.get_secret_calls == [("vault", "bc-secret")]
    assert "s3cr3t" not in repr(vars(adapter).get("get_secret_calls"))


def test_local_reads_no_environment(monkeypatch):
    monkeypatch.setenv("BC_SECRET", "from-env")
    monkeypatch.setenv("VAULT_BC_SECRET", "from-env")
    with pytest.raises(KeyError):
        LocalAdapter().get_secret("vault", "bc-secret")


def test_the_secrets_mapping_is_copied():
    given = {("vault", "k"): "one"}
    adapter = MockPlatformAdapter(secrets=given)
    given[("vault", "k")] = "two"
    assert adapter.get_secret("vault", "k") == "one"
