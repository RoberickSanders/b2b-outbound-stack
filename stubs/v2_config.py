"""Stand-in for the private `v2_config` module: Run configuration object for the discovery pipeline."""
import os as _os
from _private_stub import install

SCRIPT_DIR = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
PROJECTS_DIR = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), 'projects')
AIARK_API_KEY = _os.environ.get('AIARK_API_KEY', '')
COMPANY_FIELDS = ['company', 'domain', 'website', 'phone', 'city', 'state', 'industry']

install(__name__, purpose='Run configuration object for the discovery pipeline.', constants={'SCRIPT_DIR': SCRIPT_DIR, 'PROJECTS_DIR': PROJECTS_DIR, 'AIARK_API_KEY': AIARK_API_KEY, 'COMPANY_FIELDS': COMPANY_FIELDS})
