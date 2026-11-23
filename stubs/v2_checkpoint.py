"""Stand-in for the private `v2_checkpoint` module: Per-step checkpoints so long runs can resume."""
from _private_stub import install

_FILTER_ONLY_FLAGS = frozenset()

install(__name__, purpose='Per-step checkpoints so long runs can resume.', constants={'_FILTER_ONLY_FLAGS': _FILTER_ONLY_FLAGS})
