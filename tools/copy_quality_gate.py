"""
paf_copy_gate.py — Automated ship-gate for auto-drafted CLIENT_A campaigns.

Runs the humanizer-scan + 18-point WRITING_RULES rubric locally, without
invoking Claude. Replaces the human-driven ship gate for pipeline-launched
campaigns (post_forge_launcher, signal_campaign_launcher, cannabis_launch).

Enforces the same rules the /new-campaign slash command enforces:
  - humanizer: zero fabrication/AI-pattern flags
  - 18-point rubric: 14+/18 required to ship (A grade)
  - format: 1B "I'll cut the fluff...", 1C "2 things // checking in", 3B {{company_name}}
  - word count: <= 75 per email
  - spintax + merge fields + HTML breaks

Usage:
    from paf_copy_gate import grade_sequence
    result = grade_sequence(sequence_payload)
    if not result["ship_ok"]:
        print(f"BLOCKED: score={result['score']}/18, flags={result['humanizer_flags']}")
        return
"""

import re


# ============================================================================
# Humanizer patterns -- any match = fabrication/AI tell
# ============================================================================
HUMANIZER_PATTERNS = {
    # Fabricated frequency claims (the operator caught these 2026-04-23)
    "frequency_most": r"\bmost\b(?! recent)",          # "most recent" is OK
    "frequency_usually": r"\busually\b",
    "frequency_often": r"\boften\b",
    "frequency_typically": r"\btypically\b",
    "frequency_frequently": r"\bfrequently\b",
    # Fabricated market/carrier claims
    "market_tightened": r"\b(has|have) tightened\b",
    "carrier_now_ask": r"\bcarriers? now ask\b",
    "single_most": r"\bsingle (item|most)\b",
    "more_than_any": r"\bmore than any other\b",
    "number_one": r"\b(number one|the #1)\b",
    # AI vocabulary (anthropic humanizer skill patterns)
    "ai_vocab": r"\b(crucial|pivotal|delve|tapestry|testament|showcase|vibrant|boasts|stands as|serves as|fostering|garner|intricate|underscore|underscoring|highlighting|symbolizing)\b",
    # Significance inflation
    "significance": r"\b(is a testament to|is a reminder that|reflects broader|setting the stage for|shaping the)\b",
    # Promotional
    "promotional": r"\b(in the heart of|nestled|must-visit|world-class|unparalleled|groundbreaking|breathtaking|stunning|exemplifies|commitment to)\b",
    # Chatbot artifacts
    "chatbot": r"\b(hope this helps|of course|certainly|great question|happy to help|let me know if)\b",
    # Em dashes (always banned)
    "em_dash": r"—",
    # Negative parallelisms
    "not_only_but": r"\bnot only.*?but\b",
    # Email 2 banned phrases
    "following_up": r"\b(following up|circling back|touching base)\b",
    # Division-of-labor closers
    "division_labor": r"you handle\b.*?we handle\b|\bwe handle\b.*?\byou handle\b",
}


# ============================================================================
# Nowoslawski subject pattern detection
# Allows rule 16 (format_compliance) to accept Nowoslawski-style variants
# in addition to the classic CLIENT_A formats. The Nowoslawski framework treats
# subject + first-line as ONE preview unit (colleague_internal,
# vendor_scheduling, customer_inquiry, competitor_intel, classic_question_intro).
# Source of truth: NOWOSLAWSKI_SUBJECT_PATTERNS in paf_copy_banks.py.
# ============================================================================
def _flatten_spintax(s):
    """Return the spintax options inside the first {a|b|c} token, lowercased.
    If no spintax, returns [the_whole_string_lowercased]."""
    if not s:
        return []
    m = re.search(r"\{([^{}]+)\}", s)
    if not m:
        return [s.strip().lower()]
    return [t.strip().lower() for t in m.group(1).split("|") if t.strip()]


def _nowoslawski_keywords():
    """Build the keyword set from all Nowoslawski subject_template_options.
    Imports lazily so paf_copy_gate stays standalone if paf_copy_banks is missing."""
    try:
        from paf_copy_banks import NOWOSLAWSKI_SUBJECT_PATTERNS
    except ImportError:
        return set()
    keywords = set()
    for pattern in NOWOSLAWSKI_SUBJECT_PATTERNS.values():
        for opt in pattern.get("subject_template_options", []):
            for kw in _flatten_spintax(opt):
                # Skip merge-field-only templates like "{{company_name}}"
                if "{{" in kw or "}}" in kw:
                    continue
                if kw:
                    keywords.add(kw)
    return keywords


# Cache the keyword set on first call (paf_copy_banks doesn't change at runtime)
_NOWOSLAWSKI_KW_CACHE = None


def _is_nowoslawski_subject(subject):
    """Return True if any of the subject's spintax options matches a Nowoslawski
    pattern keyword. Designed to be permissive — same keyword in different
    Nowoslawski patterns all count as 'Nowoslawski-styled'."""
    global _NOWOSLAWSKI_KW_CACHE
    if _NOWOSLAWSKI_KW_CACHE is None:
        _NOWOSLAWSKI_KW_CACHE = _nowoslawski_keywords()
    if not subject or not _NOWOSLAWSKI_KW_CACHE:
        return False
    for opt in _flatten_spintax(subject):
        if opt in _NOWOSLAWSKI_KW_CACHE:
            return True
    return False


# ============================================================================
# 18-point rubric checks
# Each returns (bool pass, description)
# ============================================================================
def rubric_checks(seq):
    """Run all 18 rubric checks. Returns list of (rule_name, passed, note)."""
    checks = []

    # Flatten all variants
    all_variants = []
    for s in seq.get("sequences", []):
        for v in s.get("seq_variants", []) or []:
            all_variants.append({"seq": s.get("seq_number"), **v})

    seq1 = [v for v in all_variants if v["seq"] == 1]
    seq2 = [v for v in all_variants if v["seq"] == 2]
    seq3 = [v for v in all_variants if v["seq"] == 3]

    # Strip HTML + spintax helper
    def plain(body):
        body = re.sub(r"<[^>]+>", " ", body or "")
        body = re.sub(r"\{[^}]*\}", "X", body)
        body = re.sub(r"\s+", " ", body).strip()
        return body

    def word_count(body):
        return len(plain(body).split())

    # 1. <= 75 words per email
    word_fails = [(v["seq"], v["variant_label"], word_count(v["email_body"]))
                  for v in all_variants if word_count(v["email_body"]) > 75]
    checks.append(("1_word_count_75", not word_fails,
                   f"{len(word_fails)} variants over limit" if word_fails else "all <= 75"))

    # 2. Speaks "you/your" (not about "[industry]" in general)
    has_you = all("you" in plain(v["email_body"]).lower() or "your" in plain(v["email_body"]).lower()
                  for v in all_variants)
    checks.append(("2_addresses_you", has_you, "speaks to you/your" if has_you else "some variants lack direct address"))

    # 3. Specific industry language (at least NFPA or state code reference)
    specific_ok = all(
        re.search(r"NFPA|CMS|CDHS|MED|AHJ|state fire marshal|Denver Fire|ESFR|commodity class|NEMA|GFCI", plain(v["email_body"]))
        for v in seq1
    )
    checks.append(("3_specific_industry", specific_ok, "NFPA/code references present" if specific_ok else "missing specifics"))

    # 4. Subject lines 2-8 words — use the FIRST spintax option as representative
    def subj_word_count(s):
        if not s:
            return 0
        # If subject is pure spintax like "{a|b|c}", extract the first option
        m = re.match(r"^\s*\{([^{}|]+)(?:\|[^{}]+)?\}\s*$", s)
        if m:
            return len(m.group(1).split())
        # Otherwise strip spintax and count the remaining text
        cleaned = re.sub(r"\{[^}]*\}", " ", s)
        return len(cleaned.split())
    bad_subjects = [(v["seq"], v["variant_label"], v["subject"])
                    for v in seq1
                    if v.get("subject") and not (2 <= subj_word_count(v["subject"]) <= 8)]
    checks.append(("4_subject_length", not bad_subjects, f"{len(bad_subjects)} subjects out of range" if bad_subjects else "all subjects OK"))

    # 5. No banned openers ("Most [industry] struggle...")
    banned_openers = re.compile(r"^(most|many|all|every)\s+(property|facility|manufacturer|owner)", re.IGNORECASE)
    bad_openers = any(banned_openers.search(plain(v["email_body"])[:100]) for v in seq1)
    checks.append(("5_no_banned_opener", not bad_openers, "no banned openers" if not bad_openers else "banned opener found"))

    # 6. No vague benefits (all reference specific items)
    vague = any("so nothing hits you" in plain(v["email_body"]).lower()
                or "full compliance process" in plain(v["email_body"]).lower()
                or "carrier-grade" in plain(v["email_body"]).lower()
                for v in all_variants)
    checks.append(("6_no_vague_benefits", not vague, "benefits specific" if not vague else "vague benefit language found"))

    # 7. Value/time-based CTA
    has_cta = all(re.search(r"\?|call|minutes|walkthrough|audit|sample|reply", plain(v["email_body"]), re.IGNORECASE)
                  for v in all_variants)
    checks.append(("7_has_cta", has_cta, "every variant has CTA"))

    # 8. Personal CTA ("I built you..." / "Send me...")
    # At least one Email 2 should have a personal reciprocation
    personal_cta_email2 = any(
        re.search(r"\b(send me|want me to|i'll tell you|photograph|photo of)\b", plain(v["email_body"]), re.IGNORECASE)
        for v in seq2
    )
    checks.append(("8_personal_cta", personal_cta_email2, "E2 has personal CTA" if personal_cta_email2 else "E2 lacks personal CTA"))

    # 9. Email 2 doesn't re-intro + no following up
    e2_clean = all(not re.search(r"\b(following up|circling back|touching base|nice to meet|introducing myself)\b",
                                  plain(v["email_body"]), re.IGNORECASE)
                   for v in seq2)
    checks.append(("9_email2_clean", e2_clean, "E2 clean" if e2_clean else "E2 has banned phrases"))

    # 10. Variants meaningfully different (unique subject lines + unique openers)
    # Accept 3+ variants (1C is optional — the operator dropped it as weak 2026-04-23)
    seq1_subjects = set(v.get("subject", "") for v in seq1)
    seq1_openers = set(plain(v["email_body"])[:80] for v in seq1)
    variants_unique = len(seq1) >= 3 and len(seq1_subjects) >= 3 and len(seq1_openers) >= 3
    checks.append(("10_variants_unique", variants_unique, f"{len(seq1)} variants, {len(seq1_subjects)} unique subjects, {len(seq1_openers)} unique openers"))

    # 11. No fabricated case studies (no made-up numbers, %, $ amounts, company names)
    # Check for suspicious patterns like "helped 50 companies", "increased revenue by 30%", etc.
    fabricated = []
    for v in all_variants:
        body = plain(v["email_body"])
        # Revenue/savings claims without attribution
        if re.search(r"\b(saved|increased|boosted|grew|generated)\s+\$?\d+", body, re.IGNORECASE):
            fabricated.append((v["seq"], v["variant_label"]))
        # "we helped X companies" type
        if re.search(r"\bwe (helped|served|worked with) \d+", body, re.IGNORECASE):
            fabricated.append((v["seq"], v["variant_label"]))
    checks.append(("11_no_fabricated_stats", not fabricated, f"no fabricated stats" if not fabricated else f"{len(fabricated)} fabrications"))

    # 12. No em dashes
    em = any("—" in (v.get("email_body") or "") for v in all_variants)
    checks.append(("12_no_em_dashes", not em, "no em dashes" if not em else "em dash found"))

    # 13. Spintax present
    has_spintax = all(re.search(r"\{[^{}]+\|[^{}]+\}", v["email_body"] or "")
                      for v in seq1)
    checks.append(("13_has_spintax", has_spintax, "spintax in all E1"))

    # 14. Merge fields present
    has_firstname = all("{{first_name}}" in (v.get("email_body") or "")
                        for v in all_variants)
    has_companyname_3b = any("{{company_name}}" in (v.get("email_body") or "")
                              for v in seq3 if v["variant_label"] == "B")
    merge_ok = has_firstname and has_companyname_3b
    checks.append(("14_merge_fields", merge_ok, "first_name + company_name merges present"))

    # 15. HTML paragraph breaks
    html_ok = all("<div>" in (v.get("email_body") or "") or "<br>" in (v.get("email_body") or "")
                  for v in all_variants)
    checks.append(("15_html_breaks", html_ok, "HTML breaks present"))

    # 16. Format compliance — accepts EITHER CLIENT_A-classic OR Nowoslawski-styled
    # variants. CLIENT_A-classic = the "I'll cut the fluff" / "2 things // checking in"
    # template family. Nowoslawski = colleague_internal / vendor_scheduling /
    # customer_inquiry / competitor_intel / classic_question_intro patterns
    # added to paf_copy_banks.NOWOSLAWSKI_SUBJECT_PATTERNS.
    #
    # 1B: required. Pass if either format detected.
    b1 = next((v for v in seq1 if v["variant_label"] == "B"), None)
    if b1:
        classic_b1 = "I'll cut the fluff and get right to it" in (b1.get("email_body") or "")
        nowo_b1 = _is_nowoslawski_subject(b1.get("subject") or "")
        b1_ok = classic_b1 or nowo_b1
        b1_format = "classic" if classic_b1 else ("nowoslawski" if nowo_b1 else "INVALID")
    else:
        b1_ok = False
        b1_format = "missing"

    # 1C: OPTIONAL. If present, accept classic "2 things // checking in" OR Nowoslawski.
    c1 = next((v for v in seq1 if v["variant_label"] == "C"), None)
    if c1:
        classic_c1 = (c1.get("subject", "").strip() == "2 things // checking in"
                      and "I know you're busy - 2 quick things:" in (c1.get("email_body") or ""))
        nowo_c1 = _is_nowoslawski_subject(c1.get("subject") or "")
        c1_ok = classic_c1 or nowo_c1
        c1_format = "classic" if classic_c1 else ("nowoslawski" if nowo_c1 else "INVALID")
    else:
        c1_ok = True
        c1_format = "n/a"

    # 1D: OPTIONAL. Always free-form on subject. Accept any subject if present.
    d1 = next((v for v in seq1 if v["variant_label"] == "D"), None)
    if d1:
        nowo_d1 = _is_nowoslawski_subject(d1.get("subject") or "")
        d1_format = "nowoslawski" if nowo_d1 else "freeform"
    else:
        d1_format = "n/a"

    # 3B: {{company_name}} required UNIVERSALLY (both formats — breakup-style closer)
    b3 = next((v for v in seq3 if v["variant_label"] == "B"), None)
    b3_ok = b3 and "{{company_name}}" in (b3.get("email_body") or "")
    format_ok = b1_ok and c1_ok and b3_ok
    checks.append(("16_format_compliance", format_ok,
                   f"1B={b1_format} 1C={c1_format} 1D={d1_format} 3B={'OK' if b3_ok else 'missing'}"))

    # 17. PQS-angled (regulatory pain / compliance fear)
    has_pqs = all(
        re.search(r"inspect|citation|violation|compliance|liability|renewal|audit|survey", plain(v["email_body"]), re.IGNORECASE)
        for v in seq1
    )
    checks.append(("17_pqs_angled", has_pqs, "regulatory angle present" if has_pqs else "PQS angle weak"))

    # 18. Cialdini + LF8 (Authority via NFPA/state code + Reciprocation via free audit)
    has_authority = all(re.search(r"NFPA|CMS|CDHS|MED|state", plain(v["email_body"]))
                        for v in seq1)
    has_reciprocation = any(re.search(r"free|no cost|no pitch|send me", plain(v["email_body"]), re.IGNORECASE)
                            for v in seq2)
    cialdini_ok = has_authority and has_reciprocation
    checks.append(("18_cialdini_lf8", cialdini_ok, f"authority={bool(has_authority)} reciprocation={bool(has_reciprocation)}"))

    return checks


# ============================================================================
# Humanizer scan
# ============================================================================
def humanizer_scan(seq):
    """Scan entire sequence for AI/fabrication patterns. Returns list of hits."""
    hits = []
    for s in seq.get("sequences", []):
        for v in s.get("seq_variants", []) or []:
            body = (v.get("email_body") or "").lower()
            for name, pat in HUMANIZER_PATTERNS.items():
                for m in re.finditer(pat, body):
                    start = max(0, m.start() - 20)
                    end = min(len(body), m.end() + 20)
                    hits.append({
                        "seq": s.get("seq_number"),
                        "variant": v.get("variant_label"),
                        "pattern": name,
                        "context": f"...{body[start:end]}...",
                    })
    return hits


# ============================================================================
# Main gate
# ============================================================================
def grade_sequence(seq, min_score=14):
    """Run humanizer + 18-point rubric. Returns full result dict.

    Ship gate:
      - humanizer_flags must be 0
      - score must be >= min_score (default 14/18 = A grade)
    """
    humanizer_flags = humanizer_scan(seq)
    rubric = rubric_checks(seq)
    passed = [c for c in rubric if c[1]]
    failed = [c for c in rubric if not c[1]]
    score = len(passed)
    ship_ok = (len(humanizer_flags) == 0) and (score >= min_score)
    return {
        "ship_ok": ship_ok,
        "score": score,
        "total": len(rubric),
        "min_required": min_score,
        "passed": [c[0] for c in passed],
        "failed": [{"rule": c[0], "note": c[2]} for c in failed],
        "humanizer_flags": humanizer_flags,
    }


# ============================================================================
# CLI usage
# ============================================================================
if __name__ == "__main__":
    import argparse, json, sys
    ap = argparse.ArgumentParser(description="Grade a CLIENT_A sequence JSON against ship gate.")
    ap.add_argument("file", help="Path to sequence JSON (matches Smartlead payload format)")
    ap.add_argument("--min-score", type=int, default=14)
    args = ap.parse_args()
    with open(args.file) as f:
        seq = json.load(f)
    result = grade_sequence(seq, min_score=args.min_score)
    print(json.dumps(result, indent=2))
    sys.exit(0 if result["ship_ok"] else 1)
