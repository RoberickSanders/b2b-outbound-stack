#!/usr/bin/env python3
"""
treg_client.py — thin, budget-aware client for treg (https://treg.to) used by forge_enrich.

Auth: env TREG_TOKEN (loaded from /home/box/.secrets/treg_token or $TREG_TOKEN_FILE if unset).
The token is sent only in the X-Treg-Token header and is never printed, logged or written.

Every call goes through TregClient.call(), which enforces:
  - a per-call price cap (X-Treg-Route-Max-Cost, default $0.02)
  - a per-run budget cap (refuses to start a call that could push spend over the budget)
  - a ledger of (lead, step, tool, provider, cost_usd) for per-lead cost logging
"""
import os
import json
import threading
import requests

os.environ.setdefault("TREG_TELEMETRY", "0")
TREG_BASE = os.environ.get("TREG_BASE_URL", "https://treg.to/call/")
FIT_GATE_TOOL = "openrouter.ai-judge.decide"


def fit_gate_model():
    """Judge model id for the fit gate, set by env FIT_GATE_MODEL. Empty -> judge disabled."""
    return os.environ.get("FIT_GATE_MODEL", "").strip()


def load_treg_token():
    tok = os.environ.get("TREG_TOKEN", "").strip()
    if tok:
        return tok
    path = os.path.expanduser(os.environ.get("TREG_TOKEN_FILE", "/home/box/.secrets/treg_token"))
    try:
        with open(path) as f:
            tok = f.read().strip()
        if tok:
            os.environ["TREG_TOKEN"] = tok  # process env only, never persisted
    except OSError:
        tok = ""
    return tok


class BudgetExceeded(Exception):
    pass


class TregClient:
    def __init__(self, budget_usd=0.50, per_call_cap_usd=0.02, dry_run=False, timeout=90):
        self.token = load_treg_token()
        self.budget = float(budget_usd)
        self.cap = float(per_call_cap_usd)
        self.dry_run = dry_run
        self.timeout = timeout
        self.spent = 0.0
        self.reserved = 0.0
        self.ledger = []
        self._lock = threading.Lock()

    @property
    def available(self):
        return bool(self.token)

    def remaining(self):
        with self._lock:
            return self.budget - self.spent - self.reserved

    def call(self, tool, body, lead="", step="", cap_usd=None, extra_headers=None):
        """POST a JSON body to a treg tool. Returns (json_or_None, info dict).
        info = {status, cost_usd, provider, call_id, error}. Raises BudgetExceeded."""
        cap = min(self.cap, cap_usd if cap_usd is not None else self.cap)
        if self.dry_run:
            return None, {"status": 0, "cost_usd": 0.0, "provider": "dry-run", "error": "dry_run"}
        if not self.token:
            return None, {"status": 0, "cost_usd": 0.0, "provider": "", "error": "no_token"}
        with self._lock:
            if self.spent + self.reserved + cap > self.budget + 1e-9:
                raise BudgetExceeded(f"budget ${self.budget:.2f} would be exceeded (spent ${self.spent:.4f})")
            self.reserved += cap
        headers = {"X-Treg-Token": self.token, "Content-Type": "application/json",
                   "X-Treg-Route-Max-Cost": f"{cap:.4f}", "X-Treg-Max-Cost-Usd": f"{cap:.4f}"}
        if extra_headers:
            headers.update(extra_headers)
        info = {"status": 0, "cost_usd": 0.0, "provider": "", "call_id": "", "error": ""}
        data = None
        try:
            r = requests.post(TREG_BASE + tool, headers=headers, json=body, timeout=self.timeout)
            info["status"] = r.status_code
            info["cost_usd"] = int(r.headers.get("X-Treg-Cost-Micro", "0") or 0) / 1e6
            info["provider"] = r.headers.get("X-Treg-Served-By", "") or tool
            info["call_id"] = r.headers.get("X-Treg-Call-Id", "")
            try:
                data = r.json()
            except ValueError:
                data = None
            if r.status_code >= 400:
                info["error"] = (json.dumps(data)[:200] if data else r.text[:200])
                if isinstance(data, dict) and data.get("_treg", {}).get("charged_micro"):
                    info["cost_usd"] = data["_treg"]["charged_micro"] / 1e6
        except requests.RequestException as e:
            info["error"] = type(e).__name__
        finally:
            with self._lock:
                self.reserved -= cap
                self.spent += info["cost_usd"]
                self.ledger.append({"lead": lead, "step": step, "tool": tool,
                                    "provider": info["provider"], "cost_usd": info["cost_usd"],
                                    "status": info["status"]})
        return data, info

    def lead_cost(self, lead):
        with self._lock:
            return sum(x["cost_usd"] for x in self.ledger if x["lead"] == lead)

    def lead_providers(self, lead):
        with self._lock:
            return sorted({f'{x["step"]}:{x["provider"]}' for x in self.ledger
                           if x["lead"] == lead and x["status"] and x["status"] < 400})

    # ---------------- fit gate judge ----------------
    def judge(self, state, questions, lead=""):
        """Call the judge model (env FIT_GATE_MODEL) via treg's OpenRouter route. Returns answers dict or None
        (None immediately if FIT_GATE_MODEL is unset)."""
        model = fit_gate_model()
        if not model:
            return None
        data, info = self.call(FIT_GATE_TOOL, {"model": model, "state": state, "questions": questions},
                               lead=lead, step="fit_gate", cap_usd=0.005)
        if not data or info["status"] != 200:
            return None
        if not str(data.get("model", "")).startswith(model):
            return None
        return data.get("answers")


def esc(s):
    return str(s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
