#!/usr/bin/env python3
"""
verify_title_redflags.py: bad-title regex filter.

Drops contacts whose job title means they cannot buy (interns, assistants,
students, retirees, front-desk roles, etc.). Used by forge.py and
tools/verify_combine.py as one vote in the 2-of-3 consensus check.

Usage:
    python3 tools/verify_title_redflags.py "Executive Assistant to the CEO"
"""

import re
import sys

BAD_TITLE_PATTERNS = [
    r"\bintern(ship)?\b",
    r"\bstudent\b",
    r"\bretired\b",
    r"\bvolunteer\b",
    r"\b(executive |administrative |personal )?assistant\b",
    r"\breceptionist\b",
    r"\bfront desk\b",
    r"\bcashier\b",
    r"\bteller\b",
    r"\btrainee\b",
    r"\bapprentice\b",
    r"\bseasonal\b",
    r"\bformer\b",
    r"\bseeking\b|\bopen to work\b",
]

# Titles containing these are kept even if a red-flag word also appears
# ("Assistant General Manager" is a real buyer in hospitality).
ALLOW_OVERRIDES = [
    r"\bassistant (general )?manager\b",
    r"\bassistant (vice )?president\b",
    r"\bassistant director\b",
]


def is_bad_title(title):
    if not title:
        return False
    t = title.lower()
    if any(re.search(p, t) for p in ALLOW_OVERRIDES):
        return False
    return any(re.search(p, t) for p in BAD_TITLE_PATTERNS)


if __name__ == "__main__":
    for arg in sys.argv[1:]:
        print(f"{'DROP' if is_bad_title(arg) else 'KEEP'}\t{arg}")
