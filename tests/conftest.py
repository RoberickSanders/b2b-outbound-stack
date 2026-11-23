import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for p in (os.path.join(ROOT, "tools"), ROOT):
    if p not in sys.path:
        sys.path.insert(0, p)
STUBS = os.path.join(ROOT, "stubs")
if STUBS not in sys.path:
    sys.path.append(STUBS)
