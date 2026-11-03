"""
VPN Analysis Configuration
===========================
All VPN analysis uses crawl_analysis_new exclusively.
No data from crawl_analysis_last is used here.

Header data lives in header_data_new (normal) and header_data_vpn (VPN).
Scripts / js_calls / html_elements / cookies join via crawl_sessions.id.

Intersection rule: only eTLDs present in BOTH normal and VPN location
for a given country pair are included in comparative analysis.
"""

import sys
import os
_HERE = os.path.abspath(os.path.dirname(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "../.."))
for _p in [_HERE, _ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from dotenv import load_dotenv
if os.path.exists(os.path.join(_ROOT, ".env")):
    load_dotenv(os.path.join(_ROOT, ".env"))

import os
import json
import warnings
import pymysql
import pandas as pd
import numpy as np
import traceback
from collections import defaultdict

# Suppress all pandas UserWarnings globally across the VPN analysis
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
pd.options.mode.chained_assignment = None  # suppress SettingWithCopyWarning

DB_CONFIG_VPN = {
    "host":     os.getenv("host"),
    "user":     os.getenv("user"),
    "password": os.getenv("password"),
    "database": "crawl_analysis_new",
    "charset":  os.getenv("charset", "utf8mb4"),
}

# Header analysis pairs: (country in header_data_new, country in header_data_vpn)
# Both tables use plain country names — the table itself distinguishes normal vs VPN.
# 4 countries available in header tables.
HEADER_PAIRS = [
    ("INDIA",   "INDIA"),
    ("GERMANY", "GERMANY"),
    ("UAE",     "UAE"),
    ("USA",     "USA"),
]

# Session-based analysis pairs: (normal location, VPN location in crawl_sessions)
# crawl_sessions.location uses -VPN suffix to distinguish VPN visits.
# 5 countries available (adds ALGERIA).
SESSION_PAIRS = [
    ("INDIA",   "INDIA-VPN"),
    ("GERMANY", "GERMANY-VPN"),
    ("UAE",     "UAE-VPN"),
    ("USA",     "USA-VPN"),
    ("ALGERIA", "ALGERIA-VPN"),
]

# Alias used by scripts that don't distinguish — defaults to session pairs
VPN_LOCATION_PAIRS = SESSION_PAIRS

# Only global websites are in crawl_analysis_new
CATEGORY = "global"

# Security headers list (same as main analysis)
SECURITY_HEADERS = json.loads(
    open(os.path.join(_ROOT, "src/headers/SECURITY_HEADERS.json")).read()
)

# Output root
VPN_RESULTS_BASE = "./results/vpn"

# No consistency tiers for VPN — single visit per eTLD
# All results are implicitly Tier B (meaningful, single observation)

# HTML attributes relevant to security/privacy/availability analysis
# (excludes purely functional attributes like class, id, data-*)
SECURITY_RELEVANT_ATTRS = {
    # Visibility / availability — geo-blocking signal
    "hidden", "aria-hidden", "aria-disabled", "aria-live",
    # Security attributes on elements
    "integrity", "crossorigin", "referrerpolicy", "nonce",
    "sandbox", "allow", "allowfullscreen",           # iframe security
    # Content blocking signals
    "loading",                                        # lazy = deferred, eager = immediate
    "type",                                           # script type=module
    # Link security
    "rel", "href",
}

# Inline style properties that indicate hidden/blocked content
BLOCKING_STYLE_PATTERNS = [
    "display:none", "display: none",
    "visibility:hidden", "visibility: hidden",
    "opacity:0", "opacity: 0",
    "height:0", "width:0",
    "overflow:hidden", "overflow: hidden",
]
