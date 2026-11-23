"""
Shared helper for the stand-in modules in stubs/.

The production pipeline has a set of discovery, enrichment, scoring and
export modules that stay private (they hold vendor-specific request code
and tuned client logic). These stand-ins keep the same import surface so
the public repo imports cleanly, `python forge.py "<brief>" --dry-run` and
`python doctor.py` run, and the tests can exercise the pure logic.

Any real call into a stand-in raises PrivateModuleNotIncluded with a clear
message. To run the full pipeline, put your own implementation of the
module in the repo root (or tools/). The real module is found first,
because stubs/ is appended to the END of sys.path.
"""

import sys


class PrivateModuleNotIncluded(NotImplementedError):
    pass


def install(module_name, constants=None, purpose=""):
    mod = sys.modules[module_name]
    mod.__doc__ = f"Stand-in for private module {module_name!r}. {purpose}".strip()
    mod.IS_STUB = True
    for k, v in (constants or {}).items():
        setattr(mod, k, v)

    def __getattr__(name):
        if name.startswith("__"):
            raise AttributeError(name)

        def _not_included(*args, **kwargs):
            raise PrivateModuleNotIncluded(
                f"{module_name}.{name}() is part of the private production pipeline "
                f"and is not in the public repo. {purpose} "
                f"See stubs/README.md for how to plug in your own implementation."
            )
        _not_included.__name__ = name
        return _not_included

    mod.__getattr__ = __getattr__
