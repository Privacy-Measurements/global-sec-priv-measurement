"""
VPN analysis cache helpers.
Shared by all VPN scripts to skip DB queries when results already exist.

Usage in main():
    from src.vpn.cache import results_exist, load_country_results

    if not FORCE_RECOMPUTE and results_exist(out_dir, EXPECTED_FILES):
        data = load_country_results(out_dir, EXPECTED_FILES)
        # use data instead of re-querying
    else:
        # run full analysis, save CSVs
"""

import os
import json
import warnings
import pandas as pd

warnings.filterwarnings("ignore")

# ── Per-script expected output files ─────────────────────────────────────────

HEADER_FILES   = ["header_delta.csv", "header_adoption_comparison.csv"]
SCRIPT_FILES   = ["script_fp_tp_delta.csv", "script_domain_delta.csv"]
JS_FILES       = ["js_category_delta.csv", "js_normal_adoption.csv", "js_vpn_adoption.csv"]
IFRAME_FILES   = ["iframe_rate_delta.csv", "iframe_origin_delta.csv"]
HTML_FILES     = ["element_count_delta.csv"]
ATTRIBUTION_FILES = ["script_call_attribution.csv"]


def results_exist(out_dir: str, filenames: list[str]) -> bool:
    """Return True if all expected CSV files exist and are non-empty."""
    for fname in filenames:
        path = os.path.join(out_dir, fname)
        if not os.path.exists(path) or os.path.getsize(path) < 10:
            return False
    return True


def load_csv(out_dir: str, fname: str) -> pd.DataFrame:
    path = os.path.join(out_dir, fname)
    if os.path.exists(path) and os.path.getsize(path) > 1:
        try:
            return pd.read_csv(path)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


def load_json(path: str) -> dict:
    if os.path.exists(path):
        try:
            return json.load(open(path))
        except Exception:
            return {}
    return {}


def load_country_results(out_dir: str, filenames: list[str]) -> dict[str, pd.DataFrame]:
    """Load all expected CSVs from out_dir into a dict keyed by filename stem."""
    return {
        os.path.splitext(f)[0]: load_csv(out_dir, f)
        for f in filenames
    }
