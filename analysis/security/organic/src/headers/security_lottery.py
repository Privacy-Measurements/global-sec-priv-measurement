"""
Security Lottery Analysis — Roth et al. (USENIX Sec 2022)
Covers: CSP (XSS + frame-ancestors + TLS upgrade), XFO, HSTS, Set-Cookie
Unit of analysis: etld x location
Header source: har_requests.response_headers WHERE is_main_frame = 1  (CSP/XFO/HSTS/Set-Cookie)
Cookie attribute source: cookies table (PageGraph), filtered to same-site/same-origin callers
  - cookies.storage_value: raw Set-Cookie-style string with name=value; Secure; HttpOnly; SameSite=...
  - har_cookies table is empty (HAR ingestion not yet run) — not used
"""

import sys
import os
_HERE = os.path.abspath(os.path.dirname(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "../.."))
for _p in [_HERE, _ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from config import *

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns

from collections import Counter

# Roth et al. classifiers live in roth/
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from roth.parser import parse_csp, parse_xfo, parse_hsts, parse_cookie, classify_cookie
from roth.enums import (
    FA, XSS, TLS, XFO, HSTSAge, HSTSSub, HSTSPreload,
    CookieSecure, CookieHttpOnly, CookieSameSite,
)

import sys as _sys
_utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
if _utils_path not in _sys.path:
    _sys.path.insert(0, _utils_path)
from src.utils.consistency import (
    ConsistencyTagger, load_visit_map, filter_sufficient_visits,
    TIER_A, TIER_B, get_header_normaliser, MIN_VISITS,
)

# Default output dir (overridden per-category in main())
OUTPUT_DIR = "./results/headers/security_lottery"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Enum label maps ───────────────────────────────────────────────────────────

FA_LABELS    = {FA.FA_UNSAFE.value: "Unsafe", FA.CONSTRAINED.value: "Constrained",
                FA.SELF.value: "Self", FA.INVALID.value: "None/Invalid"}
XSS_LABELS   = {XSS.XSS_UNSAFE.value: "Unsafe", XSS.XSS_SAFE.value: "Safe"}
TLS_LABELS   = {TLS.TLS_UNSAFE.value: "Unsafe", TLS.ENABLED.value: "Enabled"}
XFO_LABELS   = {XFO.UNSAFE.value: "Unsafe", XFO.SELF.value: "SameOrigin", XFO.NONE.value: "Deny"}
HSTS_AGE_LABELS = {HSTSAge.DISABLE.value: "Disabled (0)",
                   HSTSAge.AGE_UNSAFE.value: "Absent/Invalid",
                   HSTSAge.LOW.value: "Low (<1yr)",
                   HSTSAge.BIG.value: "≥1yr"}
SS_LABELS    = {CookieSameSite.NONE.value: "None/Absent",
                CookieSameSite.LAX.value: "Lax",
                CookieSameSite.STRICT.value: "Strict"}

# ── DB fetch ──────────────────────────────────────────────────────────────────

# Main-frame response headers for CSP / XFO / HSTS / Set-Cookie
HEADER_QUERY = """
    SELECT
        cs.etld,
        cs.location,
        cs.category,
        cs.id   AS session_id,
        har.id  AS har_req_id,
        har.response_headers
    FROM crawl_sessions cs
    JOIN har_requests har ON har.session_id = cs.id
    WHERE cs.category = %s
      AND cs.etld_url_rel != 'cross-site'
      AND har.is_main_frame = 1
      AND har.party_type = 'first-party'
      AND har.status < 400
      AND har.response_headers IS NOT NULL
"""

# Cookie attributes from PageGraph cookies table.
# Filtered to same-site / same-origin callers (first-party cookies set during the page visit).
# storage_value is the raw Set-Cookie string: "name=value; Secure; HttpOnly; SameSite=Lax; ..."
COOKIE_QUERY = """
    SELECT
        cs.etld,
        cs.location,
        cs.category,
        ck.storage_key   AS cookie_name,
        ck.storage_value,
        ck.etld_caller_url_rel
    FROM cookies ck
    JOIN crawl_sessions cs ON cs.id = ck.session_id
    WHERE cs.category = %s
      AND ck.etld_caller_url_rel IN ('same-site', 'same-origin')
      AND ck.storage_value IS NOT NULL
"""

def fetch_headers(db_config: dict, category: str) -> pd.DataFrame:
    conn = pymysql.connect(**db_config)
    df = pd.read_sql(HEADER_QUERY, conn, params=(category,))
    conn.close()
    return df

def fetch_cookies(db_config: dict, category: str) -> pd.DataFrame:
    conn = pymysql.connect(**db_config)
    df = pd.read_sql(COOKIE_QUERY, conn, params=(category,))
    conn.close()
    return df


# ── Header normalisation (mirrors security_lottery_check.py logic) ────────────

def normalise_headers(raw: str) -> dict:
    """Lowercase-key flat dict from the JSON response_headers blob."""
    try:
        d = json.loads(raw) if isinstance(raw, str) else raw
        return {k.lower().strip(): str(v).strip() for k, v in d.items()}
    except Exception:
        return {}

def extract_lottery_headers(headers: dict) -> dict:
    """Return only SECURITY_LOTTERY headers, handling multi-value CSP concat."""
    out = {}
    csp, csp_ro = "", ""
    for h, v in headers.items():
        if h not in SECURITY_LOTTERY:
            continue
        if h == "content-security-policy":
            csp = (csp + ", " + v.strip()) if csp else v.strip()
            out[h] = csp
        elif h == "content-security-policy-report-only":
            csp_ro = (csp_ro + ", " + v.strip()) if csp_ro else v.strip()
            out[h] = csp_ro
        elif h == "strict-transport-security":
            # First occurrence wins (RFC behaviour)
            if h not in out:
                out[h] = v.strip()
        elif h == "x-frame-options":
            out[h] = v.strip()
        elif h == "set-cookie":
            # Flat dict collapses multiples to last value; parse what we have
            out[h] = v.strip()
    return out


# ── Per-unit classification ───────────────────────────────────────────────────

def classify_unit_headers(unit_sessions: list[dict], etld: str, tagger=None) -> dict:
    """
    Aggregate all sessions for one etld x location.
    For each SECURITY_LOTTERY header apply Roth classification.
    Multi-session conflict: take best (max) classification across sessions,
    consistent with the paper's approach of using the most protective policy seen.
    Returns dict of classification results keyed by header.
    """
    import tldextract as tldex
    origin = tldex.extract(etld).fqdn or etld

    # Accumulate per-header best classifications across sessions
    csp_best   = None   # dict with FA/XSS/TLS keys
    hsts_best  = None   # (age_class, sub_class, preload_class)
    xfo_best   = None   # int
    # Set-Cookie: collect all cookie dicts across sessions then aggregate
    all_cookie_classes: list[dict] = []  # list of per-cookie class dicts
    inconsistent: set[str] = set()

    seen: dict[str, list] = {h: [] for h in SECURITY_LOTTERY}

    for sess in unit_sessions:
        lottery = extract_lottery_headers(sess["headers"])
        for h in SECURITY_LOTTERY:
            if h in lottery:
                seen[h].append(lottery[h])

    # CSP — use report-only as fallback only if no enforced CSP
    csp_values = seen.get("content-security-policy", [])
    csp_ro_values = seen.get("content-security-policy-report-only", [])
    effective_csp = csp_values if csp_values else csp_ro_values
    if effective_csp:
        # Combine sessions: take max (most protective) classification
        for v in effective_csp:
            result = parse_csp(origin, v)
            if csp_best is None:
                csp_best = result.copy()
            else:
                for k in csp_best:
                    csp_best[k] = max(csp_best[k], result.get(k, 0))
        if len(set(effective_csp)) > 1:
            inconsistent.add("content-security-policy")
    else:
        csp_best = {"FA": FA.FA_UNSAFE.value, "XSS": XSS.XSS_UNSAFE.value,
                    "TLS": TLS.TLS_UNSAFE.value}

    # HSTS — first-wins per session, then take max across sessions
    hsts_values = seen.get("strict-transport-security", [])
    if hsts_values:
        for v in hsts_values:
            result = parse_hsts(v)["HSTS"]  # (age, sub, preload)
            if hsts_best is None:
                hsts_best = list(result)
            else:
                hsts_best = [max(hsts_best[i], result[i]) for i in range(3)]
        if len(set(hsts_values)) > 1:
            inconsistent.add("strict-transport-security")
    else:
        hsts_best = [HSTSAge.AGE_UNSAFE.value, HSTSSub.SUB_UNSAFE.value, 0]

    # XFO — take max across sessions
    xfo_values = seen.get("x-frame-options", [])
    if xfo_values:
        for v in xfo_values:
            result = parse_xfo(v)["XFO"]
            xfo_best = result if xfo_best is None else max(xfo_best, result)
        if len(set(xfo_values)) > 1:
            inconsistent.add("x-frame-options")
    else:
        xfo_best = XFO.UNSAFE.value

    # Set-Cookie — parse all session values, aggregate per-cookie classes
    cookie_values = seen.get("set-cookie", [])
    for v in cookie_values:
        parsed = parse_cookie(v)  # {cookie_id: {secure, httponly, samesite}}
        all_cookie_classes.extend(parsed.values())

    existing_result = {
        "csp":         csp_best,
        "hsts":        hsts_best,
        "xfo":         xfo_best,
        "cookies":     all_cookie_classes,
        "inconsistent": inconsistent,
        "has_csp":     bool(csp_values),
        "has_csp_ro":  bool(csp_ro_values),
        "has_hsts":    bool(hsts_values),
        "has_xfo":     bool(xfo_values),
        "has_cookies": bool(cookie_values),
    }

    if tagger:
        loc = unit_sessions[0].get("location", "") if unit_sessions else ""
        consistency = {}
        for h in SECURITY_LOTTERY:
            session_values = {
                sess["session_id"]: sess["headers"].get(h, "")
                for sess in unit_sessions
                if h in sess["headers"]
            }
            if session_values:
                normalise_fn = get_header_normaliser(h)
                tier, resolved = tagger.tag_value(etld, loc, session_values, normalise_fn)
                consistency[h] = tier
        return {**existing_result, "consistency_tiers": consistency}
    else:
        return {**existing_result, "consistency_tiers": {}}


# ── Build analysis frame ──────────────────────────────────────────────────────

def build_lottery_frame(raw_df: pd.DataFrame, tagger=None) -> list[dict]:
    units = []
    for (etld, loc, cat), grp in raw_df.groupby(["etld", "location", "category"]):
        sessions = [
            {
                "headers":    normalise_headers(row["response_headers"]),
                "session_id": row["session_id"],
                "location":   loc,
            }
            for _, row in grp.iterrows()
        ]
        classification = classify_unit_headers(sessions, etld, tagger=tagger)
        units.append({
            "etld": etld, "location": loc, "category": cat,
            "session_count": len(grp),
            **classification,
        })
    return units


# ── Cookie attribute analysis from har_cookies ────────────────────────────────

def _parse_cookie_row(row) -> dict | None:
    """
    Parse Secure/HttpOnly/SameSite from cookies.storage_value (raw Set-Cookie string).
    parse_cookie() returns {cookie_id: classified_dict}; we take the first entry.
    Returns a classify_cookie() result dict, or None on failure.
    """
    try:
        sv = row.get("storage_value") or ""
        if not sv:
            return None
        parsed = parse_cookie(sv)
        if not parsed:
            return None
        return next(iter(parsed.values()))
    except Exception:
        return None


def build_cookie_frame(cookie_df: pd.DataFrame) -> list[dict]:
    """
    Build per-cookie classification records from the PageGraph cookies table.
    Uses storage_value (raw Set-Cookie string) for Secure/HttpOnly/SameSite attributes.
    Handles empty table gracefully.
    """
    if cookie_df.empty:
        return []

    records = []
    for _, row in cookie_df.iterrows():
        classes = _parse_cookie_row(row)
        if classes is None:
            continue
        records.append({
            "etld": row["etld"],
            "location": row["location"],
            "category": row["category"],
            "name": row.get("cookie_name"),
            "party_type": row.get("etld_caller_url_rel"),
            **classes,
        })
    return records


# ── Summarise ─────────────────────────────────────────────────────────────────

def summarise_lottery(units: list[dict]) -> dict:
    total = len(units)

    def dist(key, label_map):
        c: Counter = Counter()
        for u in units:
            val = u[key]
            c[label_map.get(val, str(val))] += 1
        return c

    # CSP sub-dimensions
    csp_xss  = Counter(XSS_LABELS.get(u["csp"]["XSS"], str(u["csp"]["XSS"])) for u in units)
    csp_fa   = Counter(FA_LABELS.get(u["csp"]["FA"],  str(u["csp"]["FA"]))  for u in units)
    csp_tls  = Counter(TLS_LABELS.get(u["csp"]["TLS"], str(u["csp"]["TLS"])) for u in units)

    # HSTS sub-dimensions
    hsts_age  = Counter(HSTS_AGE_LABELS.get(u["hsts"][0], str(u["hsts"][0])) for u in units)
    hsts_sub  = Counter({
        "includeSubDomains": sum(1 for u in units if u["hsts"][1]),
        "No includeSubDomains": sum(1 for u in units if not u["hsts"][1]),
    })
    hsts_pre  = Counter({
        "Preload-eligible": sum(1 for u in units if u["hsts"][2]),
        "Not eligible": sum(1 for u in units if not u["hsts"][2]),
    })

    xfo_dist  = Counter(XFO_LABELS.get(u["xfo"], str(u["xfo"])) for u in units)

    # Cookie summary (from Set-Cookie via har_requests — best-effort)
    all_cookies = [c for u in units for c in u["cookies"]]
    n_cookies = len(all_cookies)
    cookie_secure   = Counter({
        "Secure": sum(1 for c in all_cookies if c["secure"] == CookieSecure.SECURE_SAFE.value),
        "Not Secure": sum(1 for c in all_cookies if c["secure"] != CookieSecure.SECURE_SAFE.value),
    })
    cookie_httponly = Counter({
        "HttpOnly": sum(1 for c in all_cookies if c["httponly"] == CookieHttpOnly.HTTP_SAFE.value),
        "Not HttpOnly": sum(1 for c in all_cookies if c["httponly"] != CookieHttpOnly.HTTP_SAFE.value),
    })
    cookie_ss = Counter(SS_LABELS.get(c["samesite"], str(c["samesite"])) for c in all_cookies)

    return {
        "total": total,
        "csp_xss": csp_xss,   "csp_fa": csp_fa,   "csp_tls": csp_tls,
        "has_csp":  sum(1 for u in units if u["has_csp"]),
        "has_csp_ro": sum(1 for u in units if u["has_csp_ro"]),
        "hsts_age": hsts_age, "hsts_sub": hsts_sub, "hsts_pre": hsts_pre,
        "has_hsts": sum(1 for u in units if u["has_hsts"]),
        "xfo_dist": xfo_dist,
        "has_xfo":  sum(1 for u in units if u["has_xfo"]),
        "n_cookies": n_cookies,
        "cookie_secure": cookie_secure,
        "cookie_httponly": cookie_httponly,
        "cookie_samesite": cookie_ss,
        "has_cookies": sum(1 for u in units if u["has_cookies"]),
        "inconsistent_counts": Counter(
            h for u in units for h in u["inconsistent"]
        ),
    }


def summarise_har_cookies(cookie_records: list[dict]) -> dict:
    """Summary over structured cookies table data (PageGraph, same-site/same-origin callers)."""
    if not cookie_records:
        return {"available": False}

    total = len(cookie_records)
    same_origin = [r for r in cookie_records if r.get("party_type") == "same-origin"]
    same_site   = [r for r in cookie_records if r.get("party_type") == "same-site"]

    def attr_rates(records):
        n = len(records)
        if n == 0:
            return {}
        return {
            "secure_rate":   sum(1 for r in records if r["secure"] == CookieSecure.SECURE_SAFE.value) / n,
            "httponly_rate": sum(1 for r in records if r["httponly"] == CookieHttpOnly.HTTP_SAFE.value) / n,
            "ss_none":   sum(1 for r in records if r["samesite"] == CookieSameSite.NONE.value) / n,
            "ss_lax":    sum(1 for r in records if r["samesite"] == CookieSameSite.LAX.value) / n,
            "ss_strict": sum(1 for r in records if r["samesite"] == CookieSameSite.STRICT.value) / n,
            "n": n,
        }

    return {
        "available": True,
        "total": total,
        "same_origin": attr_rates(same_origin),
        "same_site":   attr_rates(same_site),
        "samesite_dist": Counter(
            SS_LABELS.get(r["samesite"], str(r["samesite"])) for r in cookie_records
        ),
        "secure_dist": Counter(
            "Secure" if r["secure"] == CookieSecure.SECURE_SAFE.value else "Not Secure"
            for r in cookie_records
        ),
        "httponly_dist": Counter(
            "HttpOnly" if r["httponly"] == CookieHttpOnly.HTTP_SAFE.value else "Not HttpOnly"
            for r in cookie_records
        ),
        "by_location": {
            loc: {
                "total": len(grp := [r for r in cookie_records if r["location"] == loc]),
                **attr_rates(grp),
            }
            for loc in sorted({r["location"] for r in cookie_records})
        },
    }


# ── Per-dataset breakdown helpers ─────────────────────────────────────────────

def by_dataset(units: list[dict], key_fn, label_map: dict) -> dict:
    """
    Returns {(category, location): Counter of label -> count}
    for use in grouped bar / stacked bar charts.
    """
    result = {}
    for (cat, loc), grp in _group(units):
        result[(cat, loc)] = Counter(
            label_map.get(key_fn(u), str(key_fn(u))) for u in grp
        )
    return result


def _group(units):
    from itertools import groupby
    key = lambda u: (u["category"], u["location"])
    data = sorted(units, key=key)
    return [(k, list(g)) for k, g in groupby(data, key=key)]


# ── Plots ─────────────────────────────────────────────────────────────────────

def _grouped_stacked_bar(dataset_counters: dict, label_order: list[str],
                          colors: list, title: str, fname: str):
    """
    dataset_counters: {(cat, loc): Counter}
    Produces one stacked bar per (cat, loc) group.
    """
    groups = sorted(dataset_counters.keys())
    x_labels = [f"{c}\n{l}" for c, l in groups]
    totals = [sum(dataset_counters[g].values()) for g in groups]

    fig, ax = plt.subplots(figsize=(max(8, len(groups) * 1.1), 5))
    bottoms = [0.0] * len(groups)
    for label, color in zip(label_order, colors):
        vals = [dataset_counters[g].get(label, 0) / totals[i] * 100
                if totals[i] else 0
                for i, g in enumerate(groups)]
        ax.bar(range(len(groups)), vals, bottom=bottoms,
               label=label, color=color, edgecolor="white", linewidth=0.4)
        bottoms = [b + v for b, v in zip(bottoms, vals)]
    ax.set_xticks(range(len(groups)))
    ax.set_xticklabels(x_labels, fontsize=8)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax.set_ylabel("% of etld × location units")
    ax.set_title(title)
    ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=8)
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/{fname}.pdf", bbox_inches="tight")
    plt.savefig(f"{OUTPUT_DIR}/{fname}.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved {fname}.pdf/png")


def plot_csp(units: list[dict]):
    xss_order   = ["Safe", "Unsafe"]
    fa_order    = ["None/Invalid", "Self", "Constrained", "Unsafe"]
    tls_order   = ["Enabled", "Unsafe"]
    xss_colors  = ["mediumseagreen", "tomato"]
    fa_colors   = ["mediumseagreen", "steelblue", "sandybrown", "tomato"]
    tls_colors  = ["mediumseagreen", "tomato"]

    _grouped_stacked_bar(
        by_dataset(units, lambda u: u["csp"]["XSS"], XSS_LABELS),
        xss_order, xss_colors, "CSP — XSS Protection Classification", "csp_xss")
    _grouped_stacked_bar(
        by_dataset(units, lambda u: u["csp"]["FA"], FA_LABELS),
        fa_order, fa_colors, "CSP — Frame-Ancestors Classification", "csp_fa")
    _grouped_stacked_bar(
        by_dataset(units, lambda u: u["csp"]["TLS"], TLS_LABELS),
        tls_order, tls_colors, "CSP — TLS Upgrade Classification", "csp_tls")


def plot_xfo(units: list[dict]):
    order  = ["Deny", "SameOrigin", "Unsafe"]
    colors = ["mediumseagreen", "steelblue", "tomato"]
    _grouped_stacked_bar(
        by_dataset(units, lambda u: u["xfo"], XFO_LABELS),
        order, colors, "X-Frame-Options Classification", "xfo_lottery")


def plot_hsts(units: list[dict]):
    age_order  = ["≥1yr", "Low (<1yr)", "Disabled (0)", "Absent/Invalid"]
    age_colors = ["mediumseagreen", "sandybrown", "tomato", "lightgrey"]
    _grouped_stacked_bar(
        by_dataset(units, lambda u: u["hsts"][0], HSTS_AGE_LABELS),
        age_order, age_colors, "HSTS — max-age Classification", "hsts_age_lottery")


def plot_cookies_header(units: list[dict]):
    """Plots from Set-Cookie via har_requests (best-effort, last-value-per-response)."""
    all_cookies = [c for u in units for c in u["cookies"]]
    if not all_cookies:
        print("  No Set-Cookie data from har_requests, skipping header-based cookie plots.")
        return

    # One figure, three subplots: Secure / HttpOnly / SameSite
    fig, axes = plt.subplots(1, 3, figsize=(14, 4))

    for ax, (title, counter) in zip(axes, [
        ("Secure Flag",  Counter(
            "Secure" if c["secure"] == CookieSecure.SECURE_SAFE.value else "Not Secure"
            for c in all_cookies)),
        ("HttpOnly Flag", Counter(
            "HttpOnly" if c["httponly"] == CookieHttpOnly.HTTP_SAFE.value else "Not HttpOnly"
            for c in all_cookies)),
        ("SameSite",     Counter(
            SS_LABELS.get(c["samesite"], str(c["samesite"])) for c in all_cookies)),
    ]):
        labels, vals = zip(*counter.most_common()) if counter else ([], [])
        total = sum(vals)
        colors = {"Secure": "mediumseagreen", "Not Secure": "tomato",
                  "HttpOnly": "mediumseagreen", "Not HttpOnly": "tomato",
                  "Strict": "mediumseagreen", "Lax": "steelblue",
                  "None/Absent": "tomato"}.get
        bar_colors = [colors(l, "lightgrey") for l in labels]
        ax.bar(labels, [v / total * 100 for v in vals], color=bar_colors, edgecolor="white")
        ax.yaxis.set_major_formatter(mtick.PercentFormatter())
        ax.set_title(title)
        ax.set_ylabel("% of cookies")
        ax.tick_params(axis="x", labelsize=8)
    fig.suptitle("Cookie Security Attributes (Set-Cookie via HAR response headers)", y=1.02)
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/cookies_header_attributes.pdf", bbox_inches="tight")
    plt.savefig(f"{OUTPUT_DIR}/cookies_header_attributes.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  Saved cookies_header_attributes.pdf/png")


def plot_cookies_har(cookie_summary: dict):
    """Plots from PageGraph cookies table (same-origin vs same-site breakdown)."""
    if not cookie_summary.get("available"):
        print("  No cookies data from PageGraph cookies table — skipping structured cookie plots.")
        return

    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    attr_keys = [
        ("Secure Flag",   "secure_rate",   "secure_dist",   ["Secure", "Not Secure"],
         ["mediumseagreen", "tomato"]),
        ("HttpOnly Flag", "httponly_rate",  "httponly_dist",  ["HttpOnly", "Not HttpOnly"],
         ["mediumseagreen", "tomato"]),
        ("SameSite",      None,             "samesite_dist",  ["Strict", "Lax", "None/Absent"],
         ["mediumseagreen", "steelblue", "tomato"]),
    ]
    for ax, (title, rate_key, dist_key, order, colors) in zip(axes, attr_keys):
        dist = cookie_summary[dist_key]
        total = sum(dist.values())
        vals = [dist.get(l, 0) / total * 100 if total else 0 for l in order]
        ax.bar(order, vals, color=colors, edgecolor="white")
        ax.yaxis.set_major_formatter(mtick.PercentFormatter())
        ax.set_title(title)
        ax.set_ylabel("% of cookies")
        ax.tick_params(axis="x", labelsize=8)

        # Overlay same-origin vs same-site bars for Secure/HttpOnly
        so = cookie_summary.get("same_origin", {})
        ss = cookie_summary.get("same_site", {})
        if so and ss and rate_key:
            for i, (label, rates, hatch) in enumerate([
                ("same-origin", so, "//"), ("same-site", ss, "\\\\")
            ]):
                rate = rates.get(rate_key, 0) * 100
                ax.bar(i * 0.3 - 0.15, rate, width=0.2,
                       color="white", edgecolor="black",
                       hatch=hatch, label=label, zorder=3)
        if rate_key:
            ax.legend(fontsize=7)

    fig.suptitle(f"Cookie Security Attributes (PageGraph cookies table, n={cookie_summary['total']:,})", y=1.02)
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/cookies_pg_attributes.pdf", bbox_inches="tight")
    plt.savefig(f"{OUTPUT_DIR}/cookies_pg_attributes.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  Saved cookies_pg_attributes.pdf/png")


def plot_cookies_by_location(cookie_summary: dict):
    """Per-location breakdown of cookie security attributes from PageGraph cookies table."""
    if not cookie_summary.get("available"):
        return
    by_loc = cookie_summary.get("by_location", {})
    if not by_loc:
        return

    locs = sorted(by_loc.keys())
    attrs = [
        ("Secure rate",   "secure_rate",   "mediumseagreen"),
        ("HttpOnly rate", "httponly_rate",  "steelblue"),
        ("SameSite=Strict rate", "ss_strict", "sandybrown"),
        ("SameSite=Lax rate",    "ss_lax",    "khaki"),
    ]
    x = range(len(locs))
    width = 0.8 / len(attrs)

    fig, ax = plt.subplots(figsize=(max(10, len(locs) * 1.2), 5))
    for i, (label, key, color) in enumerate(attrs):
        vals = [by_loc[loc].get(key, 0) * 100 for loc in locs]
        offset = (i - len(attrs) / 2) * width + width / 2
        ax.bar([xi + offset for xi in x], vals, width=width * 0.9,
               label=label, color=color, edgecolor="white")
    ax.set_xticks(list(x))
    ax.set_xticklabels(locs, rotation=30, ha="right", fontsize=8)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax.set_ylabel("% of cookies")
    ax.set_title("Cookie Security Attributes by Location (PageGraph cookies table)")
    ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=8)
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/cookies_pg_by_location.pdf", bbox_inches="tight")
    plt.savefig(f"{OUTPUT_DIR}/cookies_pg_by_location.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  Saved cookies_pg_by_location.pdf/png")


def plot_combined_lottery(units: list[dict]):
    """
    Heatmap: for each etld × location, show the combined lottery score
    (fraction of security dimensions that are 'safe') grouped by dataset × location.
    Dimensions: CSP-XSS, CSP-FA (not FA_UNSAFE), CSP-TLS, XFO (not UNSAFE), HSTS-age (BIG).
    """
    rows = []
    for u in units:
        score = {
            "CSP XSS safe":      int(u["csp"]["XSS"] == XSS.XSS_SAFE.value),
            "CSP FA constrained":int(u["csp"]["FA"]  != FA.FA_UNSAFE.value),
            "CSP TLS upgrade":   int(u["csp"]["TLS"] == TLS.ENABLED.value),
            "XFO not unsafe":    int(u["xfo"]         != XFO.UNSAFE.value),
            "HSTS ≥1yr":         int(u["hsts"][0]     == HSTSAge.BIG.value),
        }
        rows.append({"category": u["category"], "location": u["location"], **score})

    df = pd.DataFrame(rows)
    dims = ["CSP XSS safe", "CSP FA constrained", "CSP TLS upgrade",
            "XFO not unsafe", "HSTS ≥1yr"]
    group_means = df.groupby(["category", "location"])[dims].mean()

    fig, ax = plt.subplots(figsize=(9, max(4, len(group_means) * 0.6)))
    sns.heatmap(group_means * 100, ax=ax, annot=True, fmt=".1f",
                cmap="RdYlGn", vmin=0, vmax=100,
                linewidths=0.5, cbar_kws={"label": "% safe"})
    ax.set_xlabel("Security Dimension")
    ax.set_ylabel("Dataset × Location")
    ax.set_title("Security Lottery — % of Sites Meeting Each Criterion")
    ax.set_xticklabels(ax.get_xticklabels(), rotation=30, ha="right", fontsize=8)
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/lottery_heatmap.pdf", bbox_inches="tight")
    plt.savefig(f"{OUTPUT_DIR}/lottery_heatmap.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  Saved lottery_heatmap.pdf/png")


# ── LaTeX ─────────────────────────────────────────────────────────────────────

def _pct(num, denom):
    return f"{num/denom*100:.1f}\\%" if denom else "---"


def lottery_summary_to_latex(s: dict, units: list[dict]) -> str:
    total = s["total"]
    lines = [
        r"\begin{table}[ht]", r"\centering", r"\small",
        r"\begin{tabular}{lrr}", r"\toprule",
        r"Security Lottery Dimension & Count & \% \\", r"\midrule",
        r"\multicolumn{3}{l}{\textit{Content Security Policy}} \\",
    ]
    for label, count in s["csp_xss"].most_common():
        lines.append(f"\\quad CSP XSS: {label} & {count} & {_pct(count, total)} \\\\")
    for label, count in s["csp_fa"].most_common():
        lines.append(f"\\quad CSP FA: {label} & {count} & {_pct(count, total)} \\\\")
    for label, count in s["csp_tls"].most_common():
        lines.append(f"\\quad CSP TLS: {label} & {count} & {_pct(count, total)} \\\\")
    lines.append(r"\midrule")
    lines.append(r"\multicolumn{3}{l}{\textit{X-Frame-Options}} \\")
    for label, count in s["xfo_dist"].most_common():
        lines.append(f"\\quad XFO: {label} & {count} & {_pct(count, total)} \\\\")
    lines.append(r"\midrule")
    lines.append(r"\multicolumn{3}{l}{\textit{Strict Transport Security}} \\")
    for label, count in s["hsts_age"].most_common():
        lines.append(f"\\quad HSTS age: {label} & {count} & {_pct(count, total)} \\\\")
    n_hsts = s["has_hsts"]
    lines.append(f"\\quad includeSubDomains & {s['hsts_sub']['includeSubDomains']} & "
                 f"{_pct(s['hsts_sub']['includeSubDomains'], n_hsts)} \\\\")
    lines.append(f"\\quad Preload-eligible & {s['hsts_pre']['Preload-eligible']} & "
                 f"{_pct(s['hsts_pre']['Preload-eligible'], n_hsts)} \\\\")
    lines.append(r"\midrule")
    lines.append(r"\multicolumn{3}{l}{\textit{Set-Cookie (via har\_requests, best-effort)}} \\")
    n_c = s["n_cookies"]
    for label, count in s["cookie_secure"].most_common():
        lines.append(f"\\quad {label} & {count} & {_pct(count, n_c)} \\\\")
    for label, count in s["cookie_httponly"].most_common():
        lines.append(f"\\quad {label} & {count} & {_pct(count, n_c)} \\\\")
    for label, count in s["cookie_samesite"].most_common():
        lines.append(f"\\quad SameSite={label} & {count} & {_pct(count, n_c)} \\\\")
    lines += [
        r"\bottomrule", r"\end{tabular}",
        r"\caption{Security Lottery classification (Roth et al.) across all \textit{etld} $\times$ \textit{location} units.}",
        r"\label{tab:security_lottery}", r"\end{table}",
    ]
    return "\n".join(lines)


def har_cookies_to_latex(s: dict) -> str:
    if not s.get("available"):
        return (r"\begin{table}[ht]\centering" + "\n"
                r"\caption{No cookie attribute data available from PageGraph cookies table.}" + "\n"
                r"\label{tab:pg_cookies}\end{table}")

    total = s["total"]
    so, ss = s.get("same_origin", {}), s.get("same_site", {})
    lines = [
        r"\begin{table}[ht]", r"\centering", r"\small",
        r"\begin{tabular}{lrrrr}", r"\toprule",
        r"Attribute & All & \% & same-origin \% & same-site \% \\", r"\midrule",
    ]
    for label, dist_key, rate_key in [
        ("Secure",   "secure_dist",   "secure_rate"),
        ("HttpOnly", "httponly_dist", "httponly_rate"),
    ]:
        safe_val = s[dist_key].get(label, 0)
        lines.append(
            f"{label} & {safe_val} & {_pct(safe_val, total)} "
            f"& {so.get(rate_key, 0)*100:.1f}\\% "
            f"& {ss.get(rate_key, 0)*100:.1f}\\% \\\\"
        )
    for ss_label in ["Strict", "Lax", "None/Absent"]:
        c = s["samesite_dist"].get(ss_label, 0)
        lines.append(
            f"SameSite={ss_label} & {c} & {_pct(c, total)} "
            f"& --- & --- \\\\"
        )
    lines += [
        r"\bottomrule", r"\end{tabular}",
        r"\caption{Cookie security attributes from PageGraph \texttt{cookies} table "
        r"(same-origin vs same-site callers, $n=" + str(total) + r"$ cookies).}",
        r"\label{tab:pg_cookies}", r"\end{table}",
    ]
    return "\n".join(lines)


# ── Console summary ───────────────────────────────────────────────────────────

def print_summary(s: dict, cookie_summary: dict):
    total = s["total"]
    print(f"\n{'='*70}\nSECURITY LOTTERY ANALYSIS\n{'='*70}")
    print(f"Total etld × location units: {total:,}")

    print(f"\n{'─'*70}\nCSP (n={total:,})")
    for label, c in s["csp_xss"].most_common():
        print(f"  XSS  {label:<20} {c:6d}  ({_pct(c, total)})")
    for label, c in s["csp_fa"].most_common():
        print(f"  FA   {label:<20} {c:6d}  ({_pct(c, total)})")
    for label, c in s["csp_tls"].most_common():
        print(f"  TLS  {label:<20} {c:6d}  ({_pct(c, total)})")

    print(f"\n{'─'*70}\nXFO (n={total:,})")
    for label, c in s["xfo_dist"].most_common():
        print(f"  {label:<25} {c:6d}  ({_pct(c, total)})")

    n_hsts = s["has_hsts"]
    print(f"\n{'─'*70}\nHSTS (has HSTS: {n_hsts:,} / {total:,})")
    for label, c in s["hsts_age"].most_common():
        print(f"  max-age {label:<20} {c:6d}  ({_pct(c, total)})")
    print(f"  includeSubDomains    {s['hsts_sub']['includeSubDomains']:6d}  ({_pct(s['hsts_sub']['includeSubDomains'], n_hsts)} of HSTS sites)")
    print(f"  Preload-eligible     {s['hsts_pre']['Preload-eligible']:6d}  ({_pct(s['hsts_pre']['Preload-eligible'], n_hsts)} of HSTS sites)")

    n_c = s["n_cookies"]
    print(f"\n{'─'*70}\nSet-Cookie via har_requests (best-effort, {n_c:,} cookies observed)")
    for label, c in s["cookie_secure"].most_common():
        print(f"  {label:<25} {c:6d}  ({_pct(c, n_c)})")
    for label, c in s["cookie_httponly"].most_common():
        print(f"  {label:<25} {c:6d}  ({_pct(c, n_c)})")
    for label, c in s["cookie_samesite"].most_common():
        print(f"  SameSite {label:<18} {c:6d}  ({_pct(c, n_c)})")

    if s["inconsistent_counts"]:
        print(f"\n{'─'*70}\nCross-session inconsistencies:")
        for h, c in s["inconsistent_counts"].most_common():
            print(f"  {h:<45} {c:6d} units")

    if cookie_summary.get("available"):
        total_hc = cookie_summary["total"]
        print(f"\n{'─'*70}\nPageGraph cookies table ({total_hc:,} cookies, same-site/same-origin)")
        for label, c in cookie_summary["secure_dist"].most_common():
            print(f"  {label:<25} {c:6d}  ({_pct(c, total_hc)})")
        for label, c in cookie_summary["httponly_dist"].most_common():
            print(f"  {label:<25} {c:6d}  ({_pct(c, total_hc)})")
        for label, c in cookie_summary["samesite_dist"].most_common():
            print(f"  SameSite {label:<18} {c:6d}  ({_pct(c, total_hc)})")
        so = cookie_summary.get("same_origin", {})
        ss = cookie_summary.get("same_site",   {})
        if so:
            print(f"\n  same-origin ({so.get('n',0):,} cookies): "
                  f"Secure={so.get('secure_rate',0)*100:.1f}%  "
                  f"HttpOnly={so.get('httponly_rate',0)*100:.1f}%  "
                  f"SS-Strict={so.get('ss_strict',0)*100:.1f}%")
        if ss:
            print(f"  same-site   ({ss.get('n',0):,} cookies): "
                  f"Secure={ss.get('secure_rate',0)*100:.1f}%  "
                  f"HttpOnly={ss.get('httponly_rate',0)*100:.1f}%  "
                  f"SS-Strict={ss.get('ss_strict',0)*100:.1f}%")
        by_loc = cookie_summary.get("by_location", {})
        if by_loc:
            print(f"\n  By location (Secure% / HttpOnly% / SS-Strict%):")
            for loc, lr in sorted(by_loc.items()):
                n = lr.get("n", lr.get("total", 0))
                print(f"    {loc:<20} n={n:6,}  "
                      f"Secure={lr.get('secure_rate',0)*100:5.1f}%  "
                      f"HttpOnly={lr.get('httponly_rate',0)*100:5.1f}%  "
                      f"SS-Strict={lr.get('ss_strict',0)*100:5.1f}%")
    else:
        print(f"\n  No cookie attribute data from PageGraph cookies table.")


# ── Main ──────────────────────────────────────────────────────────────────────

def _run_for_category(category: str, db_config: dict):
    """Run the full Security Lottery pipeline for a single dataset category."""
    global OUTPUT_DIR
    OUTPUT_DIR = os.path.join(RESULTS_BASE, "security_lottery", category)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"Fetching response headers ({category})...")
    raw = fetch_headers(db_config, category)
    if raw.empty:
        print(f"  No data for {category} — skipping.")
        return
    print(f"  {len(raw):,} rows")

    print("Loading visit map for consistency tagging...")
    visit_map = load_visit_map(db_config, category)
    tagger = ConsistencyTagger(visit_map)
    raw, excluded = filter_sufficient_visits(raw, visit_map, etld_col="etld", loc_col="location")
    if not excluded.empty:
        print(f"  Excluded {excluded['etld'].nunique()} eTLDs with insufficient visits")

    print("Building Security Lottery classification frame...")
    units = build_lottery_frame(raw, tagger=tagger)
    print(f"  {len(units):,} etld × location units")

    # Cookie attributes from PageGraph cookies table
    print("Fetching cookie attributes from PageGraph cookies table...")
    try:
        cookie_df = fetch_cookies(db_config, category)
        cookie_df, _ = filter_sufficient_visits(cookie_df, visit_map, etld_col="etld", loc_col="location")
        cookie_records = build_cookie_frame(cookie_df)
        print(f"  {len(cookie_records):,} cookie attribute records")
    except Exception:
        traceback.print_exc()
        cookie_records = []
    cookie_summary = summarise_har_cookies(cookie_records)

    # Filter to Tier A (stable) units — units where every SECURITY_LOTTERY header
    # present in that unit is consistent across all visits.
    def _is_stable_unit(unit: dict) -> bool:
        tiers = unit.get("consistency_tiers", {})
        if not tiers:
            return False
        return all(t == TIER_A for t in tiers.values())

    units_stable = [u for u in units if _is_stable_unit(u)]

    summary        = summarise_lottery(units)
    summary_stable = summarise_lottery(units_stable) if units_stable else None
    print_summary(summary, cookie_summary)
    if units_stable:
        print(f"\n  Tier A (stable) units: {len(units_stable):,} "
              f"of {len(units):,} ({len(units_stable)/len(units)*100:.1f}%)")

    print("\nGenerating plots...")
    # Tier A+B plots
    plot_csp(units)
    plot_xfo(units)
    plot_hsts(units)
    plot_cookies_header(units)
    plot_cookies_har(cookie_summary)
    plot_cookies_by_location(cookie_summary)
    plot_combined_lottery(units)

    # Tier A (stable) plots — saved in stable/ subdirectory
    if units_stable and len(units_stable) >= 10:
        _prev = OUTPUT_DIR
        OUTPUT_DIR = os.path.join(_prev, "stable")
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        plot_csp(units_stable)
        plot_xfo(units_stable)
        plot_hsts(units_stable)
        plot_combined_lottery(units_stable)
        OUTPUT_DIR = _prev

    print("Writing LaTeX tables...")
    with open(f"{OUTPUT_DIR}/table_lottery_summary.tex", "w") as f:
        f.write(lottery_summary_to_latex(summary, units))
    with open(f"{OUTPUT_DIR}/table_pg_cookies.tex", "w") as f:
        f.write(har_cookies_to_latex(cookie_summary))
    # Tier A stable table
    if summary_stable:
        with open(f"{OUTPUT_DIR}/table_lottery_summary_stable.tex", "w") as f:
            f.write(lottery_summary_to_latex(summary_stable, units_stable))
        print("  Saved table_lottery_summary.tex, table_lottery_summary_stable.tex, table_pg_cookies.tex")
    else:
        print("  Saved table_lottery_summary.tex, table_pg_cookies.tex")

    print("Saving raw results...")
    with open(f"{OUTPUT_DIR}/lottery_units.json", "w") as f:
        json.dump(units, f, default=lambda o: list(o) if isinstance(o, set) else str(o))

    # Save tier summary per header
    tier_counts = {}
    for unit in units:
        for h, tier in unit.get("consistency_tiers", {}).items():
            if h not in tier_counts:
                tier_counts[h] = {}
            tier_counts[h][tier] = tier_counts[h].get(tier, 0) + 1
    with open(f"{OUTPUT_DIR}/lottery_tier_distribution.json", "w") as f:
        json.dump(tier_counts, f, indent=2)
    print("  Saved lottery_tier_distribution.json")

    if cookie_records:
        with open(f"{OUTPUT_DIR}/pg_cookie_records.json", "w") as f:
            json.dump(cookie_records, f, default=str)

    print(f"  Done. Outputs in: {OUTPUT_DIR}")


def main():
    active_datasets = list(DATASETS)
    if INCLUDE_VPN:
        active_datasets.append(VPN_DATASET)

    for category, db_config in active_datasets:
        print(f"\n{'='*60}")
        print(f"Dataset: {category}")
        print(f"{'='*60}")
        _run_for_category(category, db_config)

    print(f"\nDone. All outputs under: {RESULTS_BASE}/security_lottery/<category>/")


if __name__ == "__main__":
    main()
