"""
CSV export, Smartlead formatting, global deduplication, fuzzy dedup, and run history.
"""

import os
import re
import csv
import json
from datetime import datetime

from config import OUTPUT_DIR, GLOBAL_DEDUP_FILE, EXCLUDED_TITLES


def _output_path(filename):
    return os.path.join(OUTPUT_DIR, filename)


# ============================================================
# CONTACT SCORING
# ============================================================

def score_contact(title):
    """Score a contact by title. Returns (score, priority)."""
    t = title.lower() if title else ""
    score = 0
    priority = "low"
    if any(k in t for k in [
        "owner", "founder", "co-founder", "president",
        "principal", "managing partner", "partner", "managing director"
    ]):
        score += 5
        priority = "owner"
    elif "ceo" in t:
        score += 4
        priority = "executive"
    elif any(k in t for k in ["cto", "chief technology officer", "chief information officer", "coo", "chief operating officer"]):
        score += 4
        priority = "executive"
    elif any(k in t for k in ["vp", "vice president"]):
        score += 3
        priority = "buyer"
    elif any(k in t for k in [
        "director of operations", "director of facilities", "director of engineering",
        "director of maintenance", "director of safety", "director of security",
        "director of property", "director of construction",
        "head of operations", "head of facilities", "head of engineering",
        "general manager", "property manager", "facilities manager",
        "operations manager", "maintenance manager", "safety manager",
        "regional manager", "area manager", "district manager",
    ]):
        score += 2
        priority = "buyer"
    elif "director" in t and t.strip() != "director":
        score += 1
        priority = "buyer"
    elif "director" in t:
        score += 0
        priority = "low"
    elif any(k in t for k in ["manager", "lead"]):
        score += 1
        priority = "buyer"
    return score, priority


def is_bad_title(title, ctx=None):
    """Check if a title should be excluded from results."""
    if not title:
        return False
    t = title.lower()
    if any(bad in t for bad in EXCLUDED_TITLES):
        return True
    if ctx and ctx.excluded_titles:
        if any(bad in t for bad in ctx.excluded_titles):
            return True
    return False


# ============================================================
# CSV EXPORT
# ============================================================

def export_contacts(contacts, filename="contacts_final.csv", ctx=None):
    """Export contacts to CSV with scoring and deduplication."""
    filename = _output_path(filename)

    seen_emails = set()
    unique = []
    for c in contacts:
        email = (c.get("email") or "").lower()
        if email and email in seen_emails:
            continue
        if email:
            seen_emails.add(email)
        unique.append(c)

    for c in unique:
        score, priority = score_contact(c.get("title", ""))
        c["score"] = score
        c["priority"] = priority

    with open(filename, "w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "company", "domain", "name", "title", "email", "phone", "type",
            "priority", "score", "source", "verified", "catch_all",
            "signal_score", "top_signal", "top_signal_detail", "signal_summary",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for c in sorted(unique, key=lambda x: x.get("score", 0), reverse=True):
            writer.writerow(c)

    dm_filename = _output_path(filename.replace("contacts_final", "decision_makers").replace(
        os.path.dirname(filename) + os.sep, ""))
    if "/" in dm_filename or "\\" in dm_filename:
        dm_filename = filename.replace("contacts_final", "decision_makers")

    decision_makers = [c for c in unique if c.get("priority") in ("owner", "executive", "buyer")]
    with open(dm_filename, "w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "company", "domain", "name", "title", "email", "phone", "type",
            "priority", "score", "source", "verified",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for c in sorted(decision_makers, key=lambda x: x.get("score", 0), reverse=True):
            writer.writerow(c)

    print(f"  Exported {len(unique)} contacts -> {filename}")
    print(f"  Exported {len(decision_makers)} decision makers -> {dm_filename}")

    return unique


def export_smartlead(contacts, filename="smartlead_import.csv", ctx=None):
    """Export contacts in Smartlead-ready format."""
    filename = _output_path(filename)

    rows = []
    generic_rows = []
    skipped_unverified = 0
    skipped_catch_all = 0
    skipped_not_validated = 0

    for c in contacts:
        if not c.get("email"):
            continue
        if c.get("verified") is False:
            skipped_unverified += 1
            continue
        if c.get("catch_all") and c.get("type") != "generic":
            skipped_catch_all += 1
            continue
        if c.get("role_validated") is False:
            skipped_not_validated += 1
            continue

        name = c.get("name", "") or ""
        parts = name.strip().split()
        first_name = parts[0] if parts else ""
        last_name = parts[-1] if len(parts) > 1 else ""

        row = {
            "email": c["email"],
            "first_name": first_name,
            "last_name": last_name,
            "company_name": c.get("company", ""),
            "phone": c.get("phone", ""),
            "title": c.get("title", ""),
            "website": c.get("domain", ""),
            "custom1": c.get("priority", ""),
            "custom2": c.get("top_signal", ""),
            "custom3": c.get("top_signal_detail", ""),
        }

        if c.get("type") == "personal" and c.get("priority") in ("owner", "executive", "buyer"):
            title = (c.get("title", "") or "").lower()
            if title and is_bad_title(title, ctx):
                continue
            if title.strip() == "director":
                continue
            rows.append(row)
        elif c.get("type") == "generic":
            generic_rows.append(row)

    all_rows = rows + generic_rows

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f,
                                fieldnames=["email", "first_name", "last_name",
                                            "company_name", "phone", "title", "website",
                                            "custom1", "custom2", "custom3"])
        writer.writeheader()
        writer.writerows(all_rows)

    skips = []
    if skipped_unverified:
        skips.append(f"{skipped_unverified} unverified")
    if skipped_catch_all:
        skips.append(f"{skipped_catch_all} catch-all")
    if skipped_not_validated:
        skips.append(f"{skipped_not_validated} not_validated")
    skip_msg = f" ({', '.join(skips)} skipped)" if skips else ""
    tier2_msg = f" ({len(generic_rows)} generic/role-based)" if generic_rows else ""
    print(f"  Exported {len(rows)} personal + {len(generic_rows)} generic contacts -> {filename}{tier2_msg}{skip_msg}")
    return rows + generic_rows


# ============================================================
# COMPANY CSV EXPORT
# ============================================================

def export_companies(companies, filename="companies.csv"):
    """Export companies to CSV."""
    filename = _output_path(filename)
    with open(filename, "w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "company", "domain", "website", "category",
            "rating", "reviews", "tech_stack", "description",
            "linkedin_url", "twitter_url", "facebook_url",
            "address", "phone",
            "signal_score", "top_signal", "top_signal_detail", "signal_summary",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for c in companies:
            writer.writerow(c)
    print(f"  Exported {len(companies)} companies -> {filename}")


def export_domains(companies, filename="domains.csv"):
    """Export unique domains to CSV."""
    filename = _output_path(filename)
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["domain"])
        seen = set()
        count = 0
        for c in companies:
            d = c.get("domain", "")
            if d and d not in seen:
                seen.add(d)
                writer.writerow([d])
                count += 1
    print(f"  Exported {count} unique domains -> {filename}")


# ============================================================
# GLOBAL DEDUPLICATION
# ============================================================

def load_global_dedup():
    seen = set()
    if os.path.exists(GLOBAL_DEDUP_FILE):
        try:
            with open(GLOBAL_DEDUP_FILE, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    email = row.get("email", "").lower().strip()
                    if email:
                        seen.add(email)
        except (IOError, csv.Error):
            pass
    return seen


def save_global_dedup(contacts, client_name=""):
    file_exists = os.path.exists(GLOBAL_DEDUP_FILE)
    try:
        with open(GLOBAL_DEDUP_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["email", "company", "client", "date"])
            if not file_exists:
                writer.writeheader()
            for c in contacts:
                if c.get("email"):
                    writer.writerow({
                        "email": c["email"].lower(),
                        "company": c.get("company", ""),
                        "client": client_name,
                        "date": datetime.now().strftime("%Y-%m-%d"),
                    })
    except IOError:
        pass


def dedup_against_global(contacts):
    seen = load_global_dedup()
    if not seen:
        return contacts, 0
    before = len(contacts)
    filtered = [c for c in contacts if c.get("email", "").lower() not in seen]
    removed = before - len(filtered)
    return filtered, removed


# ============================================================
# FUZZY COMPANY DEDUPLICATION
# ============================================================

def normalize_company_name(name):
    """Normalize company name for fuzzy dedup."""
    name = name.lower().strip()
    name = re.sub(r'\b(llc|inc|corp|ltd|co|company|group|the)\b', '', name)
    name = re.sub(r'[^a-z0-9\s]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name


# ============================================================
# RUN HISTORY
# ============================================================

def log_run(client_name, stats_dict):
    if not client_name:
        return
    history_file = os.path.join(OUTPUT_DIR, "runs.json")
    history = []
    if os.path.exists(history_file):
        try:
            with open(history_file, "r", encoding="utf-8") as f:
                history = json.load(f)
        except (json.JSONDecodeError, IOError):
            history = []

    history.append({
        "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        **stats_dict,
    })

    try:
        with open(history_file, "w", encoding="utf-8") as f:
            json.dump(history, f, indent=2, ensure_ascii=False)
    except IOError:
        pass
