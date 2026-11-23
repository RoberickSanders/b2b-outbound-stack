"""Minimal public implementation of the private `v2_cache` module.

A flat JSON file cache. The production version adds TTLs and per-vendor
namespaces; this one is enough for dry runs and local experiments.
"""
import json
import os

V2_CACHE_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                             "v2_pipeline_cache.json")
IS_STUB = False


def load_cache(path=V2_CACHE_FILE):
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def save_cache(cache, path=V2_CACHE_FILE):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cache, f)
    os.replace(tmp, path)


def cache_stats(cache):
    return {"entries": len(cache or {})}
