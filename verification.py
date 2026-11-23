"""
Email verification and catch-all domain detection.
Uses MillionVerifier API for verification, with persistent caching for catch-all results.
"""

import time
import random
import threading
import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import (
    MILLIONVERIFIER_API_KEY, MILLIONVERIFIER_ENDPOINT,
    BOUNCEBAN_API_KEY, BOUNCEBAN_ENDPOINT,
    MV_RATE_DELAY, VERIFY_WORKERS, CATCH_ALL_WORKERS, CATCH_ALL_RATE_DELAY,
)
from cache import cache_key, save_cache


# ============================================================
# EMAIL VERIFICATION (MillionVerifier)
# ============================================================

_mv_rate_lock = threading.Lock()
_mv_last_call = 0.0


def verify_email_mv(email):
    """Verify a single email via MillionVerifier API.
    Returns True (valid), False (invalid), or None (error/unknown — needs retry)."""
    global _mv_last_call
    if not MILLIONVERIFIER_API_KEY:
        return True

    with _mv_rate_lock:
        elapsed = time.time() - _mv_last_call
        if elapsed < MV_RATE_DELAY:
            time.sleep(MV_RATE_DELAY - elapsed)
        _mv_last_call = time.time()

    try:
        url = f"{MILLIONVERIFIER_ENDPOINT}?api={MILLIONVERIFIER_API_KEY}&email={email}"
        resp = requests.get(url, timeout=15)
        if resp.status_code == 429:
            time.sleep(2.0)
            return None
        resp.raise_for_status()
        data = resp.json()
        result = data.get("result", "unknown").lower()
        if result in ["ok", "catch_all"]:
            return True
        elif result in ["invalid", "disposable"]:
            return False
        else:
            return None
    except Exception:
        return None


def verify_email_bounceban(email):
    """Verify a single email via BounceBan's waterfall API.
    Returns: 'deliverable', 'undeliverable', 'risky', 'unknown', or None on error."""
    if not BOUNCEBAN_API_KEY:
        return None
    try:
        resp = requests.get(
            BOUNCEBAN_ENDPOINT,
            params={"email": email, "timeout": 80},
            headers={"Authorization": BOUNCEBAN_API_KEY},
            timeout=90,
        )
        if resp.status_code == 408:
            # Timeout — retry once (free retry within 30 min)
            time.sleep(2)
            resp = requests.get(
                BOUNCEBAN_ENDPOINT,
                params={"email": email, "timeout": 80},
                headers={"Authorization": BOUNCEBAN_API_KEY},
                timeout=90,
            )
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == "success":
                return data.get("result", "unknown")
        return None
    except Exception:
        return None


def bounceban_verify_contacts(contacts, dry_run=False):
    """Second-pass verification via BounceBan on MV-valid emails.
    Only runs on emails that passed MillionVerifier to catch edge cases."""
    if not BOUNCEBAN_API_KEY or dry_run:
        return contacts

    # Only verify emails that MV marked as valid
    to_verify = [c for c in contacts if c.get("verified") and c.get("email")]
    if not to_verify:
        return contacts

    print(f"\n  [7c/8] BounceBan double-verification ({len(to_verify)} emails)...")

    rejected = 0
    risky = 0
    done = 0
    _bb_lock = threading.Lock()

    def _bb_verify(contact):
        result = verify_email_bounceban(contact["email"])
        return contact, result

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(_bb_verify, c): c for c in to_verify}
        for future in as_completed(futures):
            contact, result = future.result()
            done += 1

            if result == "undeliverable":
                contact["verified"] = False
                contact["bb_result"] = "undeliverable"
                with _bb_lock:
                    rejected += 1
            elif result == "risky":
                contact["bb_result"] = "risky"
                with _bb_lock:
                    risky += 1
            elif result == "deliverable":
                contact["bb_result"] = "deliverable"
            else:
                contact["bb_result"] = result or "unknown"

            if done % 25 == 0:
                print(f"    [{done}/{len(to_verify)}] BounceBan -- {rejected} rejected, {risky} risky so far")

    # Remove contacts that BounceBan flagged as undeliverable
    before = len(contacts)
    contacts = [c for c in contacts if c.get("bb_result") != "undeliverable"]
    final_rejected = before - len(contacts)

    print(f"    BounceBan complete: {final_rejected} rejected, {risky} risky (kept with flag)")
    return contacts


def verify_contacts(contacts, dry_run=False, double_verify=False):
    """Verify ALL emails via MillionVerifier, regardless of source.
    If double_verify=True, also runs BounceBan on MV-valid emails.
    No email is trusted without MV verification.
    Uses parallel threads for faster verification."""
    needs_verification = [c for c in contacts if c.get("email")]
    no_email = [c for c in contacts if not c.get("email")]

    if dry_run:
        print(f"    [DRY RUN] Would verify {len(needs_verification)} emails via MillionVerifier.")
        return contacts

    if not needs_verification:
        return contacts

    print(f"\n  Verifying ALL {len(needs_verification)} emails via MillionVerifier ({VERIFY_WORKERS} threads)...")

    verified_contacts = []
    rejected = 0
    uncertain = 0
    _verify_lock = threading.Lock()

    def _verify_one(contact):
        result = verify_email_mv(contact["email"])
        if result is None:
            time.sleep(1.0)
            result = verify_email_mv(contact["email"])
        return contact, result

    done = 0
    with ThreadPoolExecutor(max_workers=VERIFY_WORKERS) as executor:
        futures = {executor.submit(_verify_one, c): c for c in needs_verification}
        for future in as_completed(futures):
            contact, is_valid = future.result()
            done += 1
            if is_valid is True:
                contact["verified"] = True
                with _verify_lock:
                    verified_contacts.append(contact)
            elif is_valid is False:
                with _verify_lock:
                    rejected += 1
            else:
                contact["verified"] = False
                with _verify_lock:
                    verified_contacts.append(contact)
                    uncertain += 1

            if done % 50 == 0:
                print(f"    [{done}/{len(needs_verification)}] Verified -- {rejected} rejected so far")

    print(f"  Verification complete: {len(verified_contacts)} valid, {rejected} rejected, {uncertain} uncertain")

    all_contacts = verified_contacts + no_email

    # Double verification with BounceBan — EARLY EXIT on clear results
    # Only send to BB when MV result is uncertain or domain is catch-all.
    # MV valid + not catch-all = skip BB (saves 30-50% BB credits).
    if double_verify and BOUNCEBAN_API_KEY:
        needs_bb = [c for c in all_contacts if c.get("verified") and c.get("email") and
                    (c.get("catch_all") or c.get("mv_result") in ("risky", "uncertain", "unknown"))]
        skip_bb = [c for c in all_contacts if c not in needs_bb]

        if needs_bb:
            print(f"\n  BB early exit: {len(skip_bb)} clear (skipped), {len(needs_bb)} need double-check")
            bb_verified = bounceban_verify_contacts(needs_bb + [c for c in all_contacts if not c.get("verified") or not c.get("email")], dry_run=dry_run)
            # Merge back: BB-checked contacts + skipped contacts
            all_contacts = [c for c in bb_verified if c.get("email")] + [c for c in skip_bb]
        else:
            print(f"\n  BB early exit: all {len(all_contacts)} clear from MV — skipping BounceBan entirely")

    return all_contacts


# ============================================================
# CATCH-ALL DOMAIN DETECTION
# ============================================================

_catch_all_rate_lock = threading.Lock()
_catch_all_last_call = 0.0


def check_catch_all(domain, cache=None, cache_file=None):
    """Check if a domain is catch-all by verifying a random nonexistent email.
    Results are cached in pipeline_cache.json to avoid re-checking across runs."""
    global _catch_all_last_call
    if not MILLIONVERIFIER_API_KEY:
        return False

    if cache is not None:
        key = cache_key("catch_all", domain)
        if key in cache:
            return cache[key].get("is_catch_all", False)

    with _catch_all_rate_lock:
        elapsed = time.time() - _catch_all_last_call
        if elapsed < CATCH_ALL_RATE_DELAY:
            time.sleep(CATCH_ALL_RATE_DELAY - elapsed)
        _catch_all_last_call = time.time()

    random_local = f"zz_test_{random.randint(10000,99999)}"
    test_email = f"{random_local}@{domain}"
    try:
        url = f"{MILLIONVERIFIER_ENDPOINT}?api={MILLIONVERIFIER_API_KEY}&email={test_email}"
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            result = resp.json().get("result", "").lower()
            is_catch_all = result in ["ok", "catch_all"]
            if cache is not None and cache_file:
                cache[cache_key("catch_all", domain)] = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "is_catch_all": is_catch_all,
                }
                save_cache(cache, cache_file)
            return is_catch_all
    except Exception:
        pass
    return False


def flag_catch_all_domains(contacts, cache=None, cache_file=None):
    """Check domains for catch-all status and flag contacts from those domains.
    Uses parallel threads and persistent caching to avoid redundant checks."""
    domains_to_check = set()
    for c in contacts:
        domain = c.get("domain", "")
        if domain and c.get("type") == "personal":
            domains_to_check.add(domain)

    if not domains_to_check:
        return contacts, {}

    catch_all_domains = {}

    uncached = []
    for domain in domains_to_check:
        if cache is not None:
            key = cache_key("catch_all", domain)
            if key in cache:
                catch_all_domains[domain] = cache[key].get("is_catch_all", False)
                continue
        uncached.append(domain)

    cached_count = len(domains_to_check) - len(uncached)
    if cached_count:
        print(f"    Catch-all cache: {cached_count} domains cached, {len(uncached)} new to check")

    if uncached:
        checked = 0
        check_lock = threading.Lock()

        def _check_one(domain):
            return domain, check_catch_all(domain, cache=cache, cache_file=cache_file)

        with ThreadPoolExecutor(max_workers=CATCH_ALL_WORKERS) as executor:
            futures = {executor.submit(_check_one, d): d for d in uncached}
            for future in as_completed(futures):
                domain, is_catch_all = future.result()
                catch_all_domains[domain] = is_catch_all
                with check_lock:
                    checked += 1
                    if checked % 20 == 0:
                        print(f"    [{checked}/{len(uncached)}] Catch-all checks...")

    flagged = 0
    for c in contacts:
        domain = c.get("domain", "")
        if catch_all_domains.get(domain, False):
            c["catch_all"] = True
            flagged += 1
        else:
            c["catch_all"] = False

    return contacts, catch_all_domains


# ============================================================
# CREDIT CHECKING
# ============================================================

def check_hunter_credits():
    """Check remaining Hunter API credits."""
    from config import HUNTER_API_KEY
    if not HUNTER_API_KEY:
        return None
    try:
        r = requests.get(f"https://api.hunter.io/v2/account?api_key={HUNTER_API_KEY}", timeout=10)
        if r.status_code == 200:
            data = r.json().get("data", {})
            searches = data.get("requests", {}).get("searches", {})
            return int(searches.get("used", 0)), int(searches.get("available", 0))
    except Exception:
        pass
    return None


def check_mv_credits():
    """Check remaining MillionVerifier credits."""
    if not MILLIONVERIFIER_API_KEY:
        return None
    try:
        url = f"{MILLIONVERIFIER_ENDPOINT}?api={MILLIONVERIFIER_API_KEY}&email=test@test.com"
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            return data.get("credits", None)
    except Exception:
        pass
    return None


def print_credit_status():
    """Print current API credit status."""
    from config import HUNTER_API_KEY, ICYPEAS_API_KEY

    print(f"\n  API Credit Status:")

    hunter = check_hunter_credits()
    if hunter:
        used, available = hunter
        remaining = available - used
        print(f"    Hunter: {remaining} remaining ({used}/{available} used)")
        if remaining < 100:
            print(f"    WARNING: Hunter credits low! Resets on next billing cycle.")
    elif HUNTER_API_KEY:
        print(f"    Hunter: Unable to check")
    else:
        print(f"    Hunter: No API key")

    mv = check_mv_credits()
    if mv is not None:
        print(f"    MillionVerifier: {mv:,} credits remaining")
        if mv < 1000:
            print(f"    WARNING: MillionVerifier credits low!")
    elif MILLIONVERIFIER_API_KEY:
        print(f"    MillionVerifier: Unable to check")
    else:
        print(f"    MillionVerifier: No API key")

    if ICYPEAS_API_KEY:
        print(f"    Icypeas: Configured (credit check not available via API)")
    else:
        print(f"    Icypeas: No API key")
    print()
