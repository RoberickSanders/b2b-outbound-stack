"""
Cache system for the lead pipeline.
Handles persistent caching of API results, classification data, and catch-all domain checks.
"""

import os
import json
import copy
import hashlib
import threading

_cache_lock = threading.Lock()


def load_cache(cache_file):
    """Load cache from disk. Returns dict."""
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def save_cache(cache, cache_file):
    """Atomically save cache to disk."""
    with _cache_lock:
        try:
            snapshot = copy.deepcopy(cache)
            temp = cache_file + ".tmp"
            with open(temp, "w", encoding="utf-8") as f:
                json.dump(snapshot, f, ensure_ascii=False)
            os.replace(temp, cache_file)
        except (IOError, RuntimeError):
            pass


def cache_key(*parts):
    """Generate a cache key from parts."""
    raw = "|".join(str(p) for p in parts)
    return hashlib.md5(raw.encode()).hexdigest()


def load_pipeline_cache(cache_file, cache_version):
    """Load pipeline cache with version migration."""
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                cache = json.load(f)
            stored_version = cache.get("_cache_version", 1)
            if stored_version < cache_version:
                print(f"  Cache version {stored_version} -> {cache_version}: clearing contact data...")
                keys_to_remove = []
                for key in cache:
                    if key.startswith("_"):
                        continue
                    val = cache[key]
                    results = val.get("results", []) if isinstance(val, dict) else []
                    if isinstance(results, list):
                        for r in results:
                            if isinstance(r, dict) and (r.get("source") or r.get("name") or r.get("email")):
                                keys_to_remove.append(key)
                                break
                for key in keys_to_remove:
                    del cache[key]
                cache["_cache_version"] = cache_version
                save_cache(cache, cache_file)
                print(f"    Cleared {len(keys_to_remove)} stale contact entries")
            return cache
        except (json.JSONDecodeError, IOError):
            return {"_cache_version": cache_version}
    return {"_cache_version": cache_version}


def load_classification_cache(cache_file):
    """Load persistent classification cache (domain -> classification result)."""
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def save_classification_cache(cache, cache_file):
    """Save classification cache to disk."""
    try:
        temp = cache_file + ".tmp"
        with open(temp, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
        os.replace(temp, cache_file)
    except (IOError, RuntimeError):
        pass
