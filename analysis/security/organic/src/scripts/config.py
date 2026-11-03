"""
Shared config for scripts analysis.
The Cloudflare Radar bearer token must be set as CF_RADAR_API in .env.
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

# Re-export shared DB / dataset config from headers config
from src.headers.config import (
    DB_CONFIG, DB_CONFIG_NEW,
    DATASETS, VPN_DATASET, INCLUDE_VPN,
    RESULTS_BASE, WORKERS, QUICK_PEEK,
)

# Cloudflare Radar API — token stored as CF_RADAR_API in .env
CF_RADAR_API  = os.getenv("CF_RADAR_API", "")
CF_RADAR_BASE = "https://api.cloudflare.com/client/v4/radar"

# Output root for scripts analysis
SCRIPTS_RESULTS_BASE = "./results/scripts/"

# TLD-based fallback category heuristics
TLD_CATEGORY_MAP = {
    "gov": "Government",
    "edu": "Education",
    "mil": "Government",
    "ngo": "Non-profit",
    "org": "Non-profit/Organisation",
    "ac":  "Education",
    "sch": "Education",
}

# Minimum eTLD count for a third-party source to appear in cross-dataset deviation analysis
MIN_ETLD_PRESENCE = 3
