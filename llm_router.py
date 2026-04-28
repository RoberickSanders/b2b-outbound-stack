"""
llm_router.py — Central LLM client routing for the Forge.

Routes:
  - HEAVY tasks (copy drafting, humanizing, grading, reply drafting) → Claude Sonnet 4
  - LIGHT tasks (parsing, classification, title filter, owner extraction) → Kimi K2.6

Environment:
  ANTHROPIC_API_KEY  — required for heavy (Sonnet) tasks
  KIMI_API_KEY       — optional; if set, light tasks route to Kimi
  FORGE_FORCE_CLAUDE — override: "true" routes everything back to Claude (for A/B testing)

Why this module exists:
  Before 2026-04-20, every Haiku call site did `anthropic.Anthropic(api_key=...)` with
  model="claude-haiku-4-5-20251001". That's 20+ files duplicating the same setup. Adding
  Kimi as a cheaper alternative for light tasks meant touching every file.

  Instead, every call site now uses get_light_client() or get_heavy_client() + the model
  name from CLIENT_META["model"]. Swap out the whole fleet from one file.

Fingerprint differences (why heavy stays on Claude):
  Kimi K2.6 passed our A/B test on cost (~8x cheaper) and latency (~40% faster) but
  produced first-pass copy with different AI "tells": em dashes, wrong spintax syntax
  ({a/b} instead of {a|b}), markdown bold, signatures, and made-up stats. Our humanizer
  skill is tuned for Claude's tells. Until the humanizer learns Kimi's patterns, the
  quality-sensitive copy pipeline stays on Sonnet.
"""

import os
from anthropic import Anthropic

# Auto-load workspace .env so standalone tests and scripts that import this
# module pick up KIMI_API_KEY + ANTHROPIC_API_KEY without needing config.py first.
try:
    from dotenv import load_dotenv
    _WORKSPACE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    # override=True so .env wins over Claude Code's empty-string shell defaults
    # (per CLAUDE.md: Claude Code's shell sets ANTHROPIC_API_KEY="" by default)
    load_dotenv(os.path.join(_WORKSPACE_ROOT, ".env"), override=True)
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"), override=True)
except ImportError:
    pass


# ============================================================
# ENDPOINTS + MODELS
# ============================================================

KIMI_BASE_URL = "https://api.kimi.com/coding"
KIMI_MODEL = "kimi-for-coding"  # routes to Kimi K2.6 under the hood

CLAUDE_HEAVY_MODEL = "claude-sonnet-4-20250514"
CLAUDE_LIGHT_MODEL = "claude-haiku-4-5-20251001"


# ============================================================
# CLIENT GETTERS
# ============================================================

def get_heavy_client():
    """Returns (client, model_name) for heavy tasks.

    Always Claude Sonnet 4. Used for:
      - Copy drafting
      - Copy humanizing
      - Copy grading
      - Reply drafting (Bryce-style)
      - Anything with an 18-point rubric downstream

    Returns:
      (Anthropic client, model_name: str)
    """
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    return Anthropic(api_key=key), CLAUDE_HEAVY_MODEL


def get_light_client():
    """Returns (client, model_name) for light tasks.

    Kimi K2.6 via Moonshot's Anthropic-compatible endpoint if KIMI_API_KEY is set,
    otherwise falls back to Claude Haiku. Used for:
      - Forge natural-language parser (query → intent JSON)
      - Niche classification
      - Owner name extraction from Google snippets
      - Title red-flag filter
      - Keyword generation
      - Second contact finder prompts

    Returns:
      (Anthropic client, model_name: str)

    Rationale:
      Kimi K2.6 via the Anthropic-compatible endpoint is a drop-in replacement at the
      SDK level. No code changes needed at call sites — they just receive the client
      and model name. ~8x cheaper per token than Haiku, comparable quality on
      classification-grade tasks.

    Override:
      Set FORGE_FORCE_CLAUDE=true to route light tasks back to Claude Haiku for
      A/B testing or if Kimi has an outage.
    """
    if os.environ.get("FORGE_FORCE_CLAUDE", "").lower() == "true":
        return Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", "")), CLAUDE_LIGHT_MODEL

    kimi_key = os.environ.get("KIMI_API_KEY", "")
    if kimi_key:
        return Anthropic(api_key=kimi_key, base_url=KIMI_BASE_URL), KIMI_MODEL

    # Fallback to Claude Haiku if KIMI_API_KEY not set
    return Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", "")), CLAUDE_LIGHT_MODEL


def get_haiku_client():
    """Legacy alias for get_light_client(). Kept for backward compatibility.

    New code should call get_light_client() directly since it's more accurate
    about what this is (light tasks, not necessarily Haiku).
    """
    return get_light_client()


# ============================================================
# CONVENIENCE: pre-built messages wrapper
# ============================================================

def light_complete(system: str, user: str, max_tokens: int = 500, **kwargs) -> str:
    """One-shot wrapper for the most common light-task call pattern.

    Usage:
        from llm_router import light_complete
        result = light_complete(
            system="Extract owner name from this snippet...",
            user=snippet_text,
            max_tokens=100,
        )

    Returns just the text content. For structured / multi-turn, use get_light_client()
    directly.
    """
    client, model = get_light_client()
    resp = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": user}],
        **kwargs,
    )
    return resp.content[0].text


def heavy_complete(system: str, user: str, max_tokens: int = 2000, **kwargs) -> str:
    """One-shot wrapper for heavy-task calls. Always Claude Sonnet 4."""
    client, model = get_heavy_client()
    resp = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": user}],
        **kwargs,
    )
    return resp.content[0].text


# ============================================================
# DIAGNOSTICS
# ============================================================

def route_status() -> dict:
    """Returns the current routing configuration. Useful for doctor.py health checks."""
    kimi_key = os.environ.get("KIMI_API_KEY", "")
    anth_key = os.environ.get("ANTHROPIC_API_KEY", "")
    forced = os.environ.get("FORGE_FORCE_CLAUDE", "").lower() == "true"
    if forced:
        light_model = f"{CLAUDE_LIGHT_MODEL} (forced via FORGE_FORCE_CLAUDE)"
    elif kimi_key:
        light_model = f"{KIMI_MODEL} @ {KIMI_BASE_URL}"
    else:
        light_model = f"{CLAUDE_LIGHT_MODEL} (KIMI_API_KEY not set, fallback)"
    return {
        "heavy_model": CLAUDE_HEAVY_MODEL,
        "heavy_key_set": bool(anth_key),
        "light_model": light_model,
        "light_key_set": bool(kimi_key) or bool(anth_key),
    }


if __name__ == "__main__":
    print("LLM Router Status:")
    for k, v in route_status().items():
        print(f"  {k}: {v}")
