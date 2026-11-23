import importlib

import pytest


def test_stub_raises_clear_error():
    mod = importlib.import_module("v2_discovery")
    assert mod.IS_STUB is True
    from _private_stub import PrivateModuleNotIncluded
    with pytest.raises(PrivateModuleNotIncluded, match="v2_discovery.blitz_discover"):
        mod.blitz_discover(None, {}, "x")


def test_v2_cache_roundtrip(tmp_path):
    from v2_cache import cache_stats, load_cache, save_cache
    path = str(tmp_path / "cache.json")
    assert load_cache(path) == {}
    save_cache({"example.com": {"mx": True}}, path)
    assert load_cache(path) == {"example.com": {"mx": True}}
    assert cache_stats(load_cache(path)) == {"entries": 1}


def test_forge_imports_in_clean_env():
    forge = importlib.import_module("forge")
    assert callable(forge.main)


def test_light_client_without_keys_raises_cleanly(monkeypatch):
    import llm_router
    monkeypatch.delenv("KIMI_API_KEY", raising=False)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "your-anthropic-key")
    monkeypatch.delenv("FORGE_FORCE_CLAUDE", raising=False)
    with pytest.raises(RuntimeError, match="no LLM key set"):
        llm_router.get_light_client()
