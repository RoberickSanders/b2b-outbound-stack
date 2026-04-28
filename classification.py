"""
AI-powered company classification.
Keyword scoring + Haiku fallback for niche-agnostic classification.
"""

import re
import json
import os
import time
import csv
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import (
    ANTHROPIC_API_KEY, CLASSIFIER_HAIKU_MODEL,
    CLASSIFIER_CONFIDENCE_THRESHOLD, CLASSIFIER_SCRAPE_TIMEOUT,
    CLASSIFIER_SCRAPE_WORKERS, CLASSIFIER_HAIKU_BATCH_SIZE,
    CLASSIFIER_SCRAPE_PAGES, TECH_SIGNATURES, OUTPUT_DIR,
)
from cache import save_classification_cache

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False


def _output_path(filename):
    return os.path.join(OUTPUT_DIR, filename)


# ============================================================
# WEBSITE SCRAPING FOR CLASSIFICATION
# ============================================================

def scrape_for_classification(domain):
    """Scrape homepage + key inner pages for classification signals."""
    norm_domain = domain.lower().replace("https://", "").replace("http://", "").rstrip("/")
    norm_domain = norm_domain.split("/")[0].split("?")[0]

    result = {
        "domain": norm_domain,
        "title": "",
        "meta": "",
        "body": "",
        "pages_scraped": 0,
        "error": None,
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36"
    }

    all_text_parts = []

    for path in CLASSIFIER_SCRAPE_PAGES:
        for scheme in ["https://", "http://"]:
            url = f"{scheme}{domain}{path}" if path != "/" else f"{scheme}{domain}"
            try:
                resp = requests.get(
                    url, timeout=CLASSIFIER_SCRAPE_TIMEOUT, headers=headers,
                    allow_redirects=True, verify=False,
                )
                if resp.status_code != 200:
                    continue
                resp.raise_for_status()

                if HAS_BS4:
                    soup = BeautifulSoup(resp.text, "html.parser")

                    if path == "/" and soup.title and not result["title"]:
                        result["title"] = soup.title.get_text(strip=True)[:200]

                    if path == "/" and not result["meta"]:
                        meta_tag = soup.find("meta", attrs={"name": re.compile("description", re.I)})
                        if meta_tag and meta_tag.get("content"):
                            result["meta"] = meta_tag["content"][:500]

                    for tag in soup(["script", "style", "nav", "footer", "header", "noscript"]):
                        tag.decompose()
                    page_text = soup.get_text(separator=" ", strip=True)
                else:
                    html = resp.text
                    if path == "/" and not result["title"]:
                        title_match = re.search(r'<title[^>]*>(.*?)</title>', html, re.IGNORECASE | re.DOTALL)
                        if title_match:
                            result["title"] = title_match.group(1).strip()[:200]
                    if path == "/" and not result["meta"]:
                        meta_match = re.search(
                            r'<meta\s+[^>]*name=["\']description["\'][^>]*content=["\']([^"\']+)',
                            html, re.IGNORECASE
                        )
                        if meta_match:
                            result["meta"] = meta_match.group(1)[:500]
                    page_text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
                    page_text = re.sub(r'<style[^>]*>.*?</style>', '', page_text, flags=re.DOTALL | re.IGNORECASE)
                    page_text = re.sub(r'<[^>]+>', ' ', page_text)
                    page_text = re.sub(r'\s+', ' ', page_text).strip()

                words = page_text.split()[:300]
                all_text_parts.append(" ".join(words))
                result["pages_scraped"] += 1
                break

            except Exception as e:
                if path == "/":
                    result["error"] = str(e)[:100]
                continue

        if result["pages_scraped"] >= 3:
            break

    result["body"] = " ".join(all_text_parts)[:3000]
    return result


# ============================================================
# KEYWORD CLASSIFICATION
# ============================================================

def _keyword_matches(keyword, text):
    """Check if a keyword matches in text using word boundary matching for short keywords."""
    kw = keyword.lower().strip()
    if not kw:
        return False
    if " " in kw:
        return kw in text
    if len(kw) <= 4:
        return bool(re.search(r'\b' + re.escape(kw) + r'\b', text))
    return kw in text


def classify_by_keywords(scraped, positive_keywords, negative_keywords):
    """Score a company based on ICP-driven keyword matching."""
    text = f"{scraped['title']} {scraped['meta']} {scraped['body']}".lower()

    pos_hits = 0
    neg_hits = 0
    matched = []

    for kw in positive_keywords:
        if _keyword_matches(kw, text):
            pos_hits += 1
            matched.append(f"+{kw}")

    for kw in negative_keywords:
        if _keyword_matches(kw, text):
            neg_hits += 1
            matched.append(f"-{kw}")

    if pos_hits >= 3 and neg_hits == 0:
        confidence = min(95, 60 + (pos_hits * 5))
        classification = "MATCH"
    elif pos_hits >= 2 and neg_hits == 0:
        confidence = 55 + (pos_hits * 5)
        classification = "MATCH"
    elif pos_hits >= 1 and neg_hits == 0:
        confidence = 40 + (pos_hits * 10)
        classification = "AMBIGUOUS"
    elif pos_hits == 0 and neg_hits >= 2:
        confidence = max(5, 30 - (neg_hits * 10))
        classification = "NOT_MATCH"
    elif pos_hits == 0 and neg_hits == 1:
        confidence = 25
        classification = "NOT_MATCH"
    elif pos_hits > 0 and neg_hits > 0:
        if pos_hits > neg_hits * 2:
            confidence = 50 + (pos_hits - neg_hits) * 5
            classification = "AMBIGUOUS"
        elif neg_hits > pos_hits:
            confidence = 30
            classification = "NOT_MATCH"
        else:
            confidence = 45
            classification = "AMBIGUOUS"
    elif pos_hits == 0 and neg_hits == 0:
        if scraped["pages_scraped"] == 0:
            confidence = 60
            classification = "AMBIGUOUS"
        else:
            confidence = 55
            classification = "AMBIGUOUS"
    else:
        confidence = 40
        classification = "AMBIGUOUS"

    return pos_hits, neg_hits, confidence, matched, classification


# ============================================================
# HAIKU CLASSIFICATION
# ============================================================

def classify_batch_with_haiku(batch, icp, api_key):
    """Send ambiguous companies to Haiku for AI classification."""
    if not api_key:
        return {}

    try:
        from anthropic import Anthropic
        client = Anthropic(api_key=api_key)
    except ImportError:
        return {}

    client_summary = icp.get("client_summary", "")
    target_description = icp.get("target_description", "")
    pos_keywords = icp.get("classifier_positive_keywords", [])
    neg_keywords = icp.get("classifier_negative_keywords", [])
    pos_list = ", ".join(pos_keywords[:15]) if pos_keywords else "target business"
    neg_list = ", ".join(neg_keywords[:20]) if neg_keywords else "unrelated businesses"

    batch_text = "\n".join([
        f"- {c['company_name']} | {c['domain']} | Title: {c.get('title', '')[:100]} | "
        f"Meta: {c.get('meta', '')[:150]} | Body: {c.get('body', '')[:200]}"
        for c in batch
    ])

    prompt = f"""Classify companies for a lead generation campaign.

Looking for: {client_summary}
Target type: {target_description}

The TARGET company type has these characteristics: {pos_list}
These are NOT target companies: {neg_list}

Classify each company as MATCH or NOT_MATCH:
- MATCH = the company IS the target type described above
- NOT_MATCH = the company is NOT the target type (wrong industry, wrong business model)

Rules:
- Look at what the company DOES and OFFERS, not just keywords in the name
- A company that mentions target keywords but isn't actually that type of business = NOT_MATCH
- Example: A dental office that mentions "managed IT services" is still a dental office = NOT_MATCH
- Example: A law firm that mentions "cybersecurity" is still a law firm = NOT_MATCH
- Focus on the company's PRIMARY business, not peripheral services they may use
- When in doubt, prefer MATCH — false negatives are worse than false positives for lead gen

Respond ONLY with valid JSON array. No markdown, no backticks.
[{{"d":"domain.com","c":"MATCH","cf":75,"r":"reason"}},{{"d":"other.com","c":"NOT_MATCH","cf":15,"r":"reason"}}]

"c" = MATCH or NOT_MATCH
"cf" = confidence 0-100
"r" = brief reason

Companies:
{batch_text}"""

    try:
        msg = client.messages.create(
            model=CLASSIFIER_HAIKU_MODEL,
            max_tokens=3000,
            messages=[{"role": "user", "content": prompt}],
        )

        text = msg.content[0].text.strip()
        if text.startswith("```"):
            text = re.sub(r"^```json?\n?", "", text)
            text = re.sub(r"\n?```$", "", text)

        parsed = json.loads(text)
        results = {}
        for r in parsed:
            results[r["d"]] = {
                "classification": r["c"],
                "confidence": int(r.get("cf", 50)),
                "reason": r.get("r", ""),
            }
        return results

    except Exception as e:
        print(f"    Haiku classification error: {e}")
        return {}


# ============================================================
# FULL CLASSIFICATION PIPELINE
# ============================================================

def classify_companies(companies, icp, threshold=None, classification_cache=None, classification_cache_file=None):
    """Full company classification pipeline."""
    if threshold is None:
        threshold = CLASSIFIER_CONFIDENCE_THRESHOLD

    if classification_cache is None:
        classification_cache = {}

    pos_keywords = [k.lower() for k in icp.get("classifier_positive_keywords", [])]
    neg_keywords = [k.lower() for k in icp.get("classifier_negative_keywords", [])]

    if not pos_keywords:
        pos_keywords = [k.lower() for k in icp.get("positive_keywords", [])]
    if not neg_keywords:
        neg_keywords = [k.lower() for k in icp.get("negative_keywords", [])]

    if not pos_keywords and not neg_keywords:
        print("    No classifier keywords available — skipping classification")
        return companies, [], []

    domain_to_company = {}
    for c in companies:
        domain_to_company[c["domain"]] = c

    cached_results = {}
    domains_to_scrape = []
    for domain in domain_to_company:
        if domain in classification_cache:
            cached = classification_cache[domain]
            cached_results[domain] = cached
        else:
            domains_to_scrape.append(domain)

    if cached_results:
        print(f"    Classification cache: {len(cached_results)} cached, {len(domains_to_scrape)} to classify")

    scraped = {}
    if domains_to_scrape:
        print(f"    Scraping {len(domains_to_scrape)} company websites for classification...")
        with ThreadPoolExecutor(max_workers=CLASSIFIER_SCRAPE_WORKERS) as executor:
            futures = {executor.submit(scrape_for_classification, d): d for d in domains_to_scrape}
            done = 0
            for future in as_completed(futures):
                done += 1
                d = futures[future]
                try:
                    result = future.result()
                    scraped[d] = result
                except Exception:
                    scraped[d] = {"domain": d, "title": "", "meta": "", "body": "", "pages_scraped": 0, "error": "exception"}
                if done % 50 == 0:
                    print(f"      [{done}/{len(domains_to_scrape)}] Scraped")

    classifications = {}
    ambiguous = []

    for domain in domains_to_scrape:
        data = scraped.get(domain, {})
        pos, neg, confidence, matched, classification = classify_by_keywords(data, pos_keywords, neg_keywords)

        if classification == "AMBIGUOUS":
            ambiguous.append({
                "domain": domain,
                "company_name": domain_to_company[domain]["company"],
                "title": data.get("title", ""),
                "meta": data.get("meta", ""),
                "body": data.get("body", ""),
                "pos": pos,
                "neg": neg,
                "keyword_confidence": confidence,
            })
        else:
            classifications[domain] = {
                "classification": classification,
                "confidence": confidence,
                "reason": f"keyword score +{pos}/-{neg}",
                "method": "keywords",
                "matches": ", ".join(matched[:8]),
            }

    kw_match = sum(1 for v in classifications.values() if v["classification"] == "MATCH")
    kw_not = sum(1 for v in classifications.values() if v["classification"] == "NOT_MATCH")
    print(f"    Keyword scoring: {kw_match} MATCH, {kw_not} NOT_MATCH, {len(ambiguous)} ambiguous")

    if ambiguous and ANTHROPIC_API_KEY:
        print(f"    Sending {len(ambiguous)} ambiguous companies to Haiku...")
        batches = [ambiguous[i:i + CLASSIFIER_HAIKU_BATCH_SIZE]
                   for i in range(0, len(ambiguous), CLASSIFIER_HAIKU_BATCH_SIZE)]

        for i, batch in enumerate(batches):
            print(f"      Batch {i+1}/{len(batches)}...")
            try:
                haiku_results = classify_batch_with_haiku(batch, icp, ANTHROPIC_API_KEY)
                for domain, result in haiku_results.items():
                    classifications[domain] = {
                        "classification": result["classification"],
                        "confidence": result["confidence"],
                        "reason": result["reason"],
                        "method": "haiku",
                        "matches": "",
                    }
            except Exception as e:
                print(f"      ERROR: {e}")
                for c in batch:
                    classifications[c["domain"]] = {
                        "classification": "AMBIGUOUS",
                        "confidence": c.get("keyword_confidence", 35),
                        "reason": f"haiku error: {str(e)[:50]}",
                        "method": "haiku_error",
                        "matches": "",
                    }
            time.sleep(0.5)
    elif ambiguous:
        print(f"    No ANTHROPIC_API_KEY — {len(ambiguous)} ambiguous companies scored by keywords only")
        for c in ambiguous:
            classifications[c["domain"]] = {
                "classification": "AMBIGUOUS",
                "confidence": c.get("keyword_confidence", 35),
                "reason": "no api key for haiku",
                "method": "keywords_only",
                "matches": "",
            }

    for domain, cached in cached_results.items():
        classifications[domain] = cached

    for domain, result in classifications.items():
        if domain not in cached_results:
            classification_cache[domain] = result

    passed = []
    rejected = []
    classification_log = []

    for company in companies:
        domain = company["domain"]
        cls = classifications.get(domain, {})
        confidence = cls.get("confidence", 35)
        classification = cls.get("classification", "AMBIGUOUS")

        log_entry = {
            "domain": domain,
            "company": company["company"],
            "classification": classification,
            "confidence": confidence,
            "method": cls.get("method", "unknown"),
            "reason": cls.get("reason", ""),
            "matches": cls.get("matches", ""),
        }
        classification_log.append(log_entry)

        if classification == "MATCH" and confidence >= threshold - 10:
            passed.append(company)
        elif classification == "AMBIGUOUS" and confidence >= threshold:
            passed.append(company)
        else:
            rejected.append(company)

    if classification_cache_file:
        save_classification_cache(classification_cache, classification_cache_file)

    return passed, rejected, classification_log


def export_classification_log(log, filename="classification_log.csv"):
    """Export the full classification log for review."""
    filename = os.path.join(OUTPUT_DIR, filename)
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["domain", "company", "classification", "confidence",
                           "method", "reason", "matches"]
        )
        writer.writeheader()
        for entry in sorted(log, key=lambda x: x.get("confidence", 0), reverse=True):
            writer.writerow(entry)
    print(f"  Exported classification log -> {filename}")


# ============================================================
# FIRMOGRAPHICS ENRICHMENT
# ============================================================

def detect_tech_stack(html):
    """Detect technologies from website HTML source."""
    if not html:
        return []
    html_lower = html.lower()
    detected = []
    for tech, signatures in TECH_SIGNATURES.items():
        if any(sig.lower() in html_lower for sig in signatures):
            detected.append(tech)
    return sorted(detected)


def extract_company_meta(html, url):
    """Extract company metadata from HTML."""
    meta = {
        "description": "",
        "social_links": {},
    }

    if not html:
        return meta

    desc_match = re.search(
        r'<meta\s+(?:name=["\']description["\']|property=["\']og:description["\'])\s+content=["\']([^"\']{10,300})["\']',
        html, re.IGNORECASE
    )
    if not desc_match:
        desc_match = re.search(
            r'content=["\']([^"\']{10,300})["\']\s+(?:name=["\']description["\']|property=["\']og:description["\'])',
            html, re.IGNORECASE
        )
    if desc_match:
        meta["description"] = desc_match.group(1).strip()

    social_patterns = {
        "linkedin": re.compile(r'href=["\']?(https?://(?:www\.)?linkedin\.com/company/[^"\'\s>]+)', re.I),
        "twitter": re.compile(r'href=["\']?(https?://(?:www\.)?(?:twitter|x)\.com/[^"\'\s>]+)', re.I),
        "facebook": re.compile(r'href=["\']?(https?://(?:www\.)?facebook\.com/[^"\'\s>]+)', re.I),
    }
    for platform, pattern in social_patterns.items():
        match = pattern.search(html)
        if match:
            meta["social_links"][platform] = match.group(1).rstrip("/")

    return meta


def enrich_firmographics(company_data, cache, cache_file):
    """Enrich a company with firmographic data: tech stack, description, social links."""
    from cache import cache_key, save_cache
    from config import EMAIL_SCRAPE_TIMEOUT

    domain = company_data.get("domain", "")
    website = company_data.get("website", "")
    key = cache_key("firmographics", domain)

    if key in cache:
        return cache[key].get("results", {})

    if not website:
        return {}

    url = website.strip()
    if not url.startswith("http"):
        url = "http://" + url

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "text/html",
    }

    try:
        resp = requests.get(url, headers=headers, timeout=EMAIL_SCRAPE_TIMEOUT, allow_redirects=True)
        resp.raise_for_status()
        html = resp.text
    except Exception:
        from datetime import datetime, timezone
        cache[key] = {"timestamp": datetime.now(timezone.utc).isoformat(), "results": {}}
        save_cache(cache, cache_file)
        return {}

    tech_stack = detect_tech_stack(html)
    meta = extract_company_meta(html, url)

    result = {
        "tech_stack": tech_stack,
        "description": meta.get("description", ""),
        "linkedin_url": meta.get("social_links", {}).get("linkedin", ""),
        "twitter_url": meta.get("social_links", {}).get("twitter", ""),
        "facebook_url": meta.get("social_links", {}).get("facebook", ""),
    }

    from datetime import datetime, timezone
    cache[key] = {"timestamp": datetime.now(timezone.utc).isoformat(), "results": result}
    save_cache(cache, cache_file)
    return result


# ============================================================
# POST-ENRICHMENT VALIDATION
# ============================================================

def validate_contacts_post_enrichment(contacts, icp, ctx):
    """Post-enrichment validation: flag companies where contacts don't match ICP roles."""
    target_roles = icp.get("target_roles", [])
    buyer_keywords = [k.lower() for k in icp.get("buyer_keywords", [])]

    if not target_roles and not buyer_keywords:
        for c in contacts:
            c["role_validated"] = True
        return contacts

    by_domain = {}
    for c in contacts:
        d = c.get("domain", "")
        if d not in by_domain:
            by_domain[d] = []
        by_domain[d].append(c)

    flagged_domains = 0
    for domain, domain_contacts in by_domain.items():
        has_relevant_title = False
        for c in domain_contacts:
            title = (c.get("title", "") or "").lower()
            if not title:
                continue
            if _title_matches_roles(title, target_roles):
                has_relevant_title = True
                break
            if buyer_keywords and any(kw in title for kw in buyer_keywords):
                has_relevant_title = True
                break

        for c in domain_contacts:
            c["role_validated"] = has_relevant_title or not c.get("title")

        if not has_relevant_title and any(c.get("title") for c in domain_contacts):
            flagged_domains += 1

    if flagged_domains:
        print(f"    Post-enrichment validation: {flagged_domains} domains flagged (contacts don't match ICP roles)")

    return contacts


def _title_matches_roles(title, target_roles):
    """Check if a job title matches any of the target roles (fuzzy)."""
    if not target_roles or not title:
        return True
    title_lower = title.lower()
    for role in target_roles:
        role_lower = role.lower()
        role_words = role_lower.split()
        if all(w in title_lower for w in role_words):
            return True
        if role_lower in title_lower or title_lower in role_lower:
            return True
    return False
