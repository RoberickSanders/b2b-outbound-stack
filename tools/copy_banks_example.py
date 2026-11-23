#!/usr/bin/env python3.13
"""
copy_banks_example.py — example skeleton (NOT production copy).

Production copy_banks files live OUTSIDE this public repo. Each agency
maintains its own private copy_banks per client. This file shows the
STRUCTURE so you can implement your own.

Real production banks are typically organized:
  - subject_pattern_X (Nowoslawski-style colleague_internal, vendor_scheduling, etc.)
  - openers (greeting + first-line variations)
  - body_blocks (insertion phrases per niche/intent)
  - cta_phrases (single-ask variants)
  - signature_templates (per-sender)

When the copy quality gate (copy_quality_gate.py) runs, it pulls from these
banks to assemble candidate sequences.
"""

# Skeleton structure — replace each list with real production copy
# tuned for your specific client's voice + niche.

SUBJECT_PATTERNS = {
    "colleague_internal": [
        "[Subject pattern 1 placeholder]",
        "[Subject pattern 2 placeholder]",
    ],
    "vendor_scheduling": [
        "[Subject pattern 3 placeholder]",
    ],
    "customer_inquiry": [
        "[Subject pattern 4 placeholder]",
    ],
}

OPENERS = [
    "[Opener 1 placeholder]",
    "[Opener 2 placeholder]",
]

BODY_BLOCKS = {
    "compliance_angle": ["[Body block placeholder]"],
    "performance_angle": ["[Body block placeholder]"],
    "social_proof": ["[Body block placeholder]"],
}

CTAS = [
    "[CTA 1 placeholder]",
    "[CTA 2 placeholder]",
]

SIGNATURE_TEMPLATES = {
    "default": "{first_name} {last_name} / {title} / {company}",
}


def get_bank(name: str) -> dict:
    """Public interface — return a structured copy bank dict by name."""
    raise NotImplementedError(
        "Production copy banks are agency-private. Implement get_bank() "
        "with your own production copy."
    )
