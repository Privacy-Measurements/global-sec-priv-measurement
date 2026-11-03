"""
Fill tables/security_lottery.tex with real values from security_lottery results.

Sources:
  results/headers/security_lottery/<category>/lottery_units.json
      — per-unit Roth classifications (HSTS, CSP, XFO, cookies from har)
  results/headers/security_lottery/<category>/pg_cookie_records.json
      — per-cookie PageGraph records with secure/httponly/samesite fields

Denominators (consistent for all tables):
  HSTS table : units where has_hsts = True  (among sites that deploy HSTS)
  CSP  table : units where has_csp  = True  (among sites that deploy CSP)
  XFO  table : units where has_xfo  = True  (among sites that deploy XFO)
  Cookie table: total cookie records per (category, location)
               (each record = one cookie instance)

Roth enum values (from src/headers/roth/enums.py):
  HSTSAge:   DISABLE=-1, AGE_UNSAFE=0, LOW=1, BIG=2
  HSTSSub:   SUB_UNSAFE=0, SAFE=1
  HSTSPreload: 0=absent, 1=present
  XSS:       XSS_UNSAFE=0, XSS_SAFE=1
  TLS:       TLS_UNSAFE=0, ENABLED=1
  FA:        FA_UNSAFE=0, CONSTRAINED=1, SELF=2, INVALID=3
  XFO:       UNSAFE=0, SELF=1, NONE=2
  CookieSameSite: NONE=0, LAX=1, STRICT=2
  CookieSecure:   SECURE_UNSAFE=0, SECURE_SAFE=1
  CookieHttpOnly: HTTP_UNSAFE=0, HTTP_SAFE=1

Datasets: D1=global, D2=country_coded, D3=country_specific
Locations: ALGERIA, GERMANY, INDIA, UAE, USA
"""

import sys, os
_HERE = os.path.abspath(os.path.dirname(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "../.."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import re, json
import numpy as np
import pandas as pd
from collections import defaultdict

TEMPLATE = os.path.join(_ROOT, "tables", "security_lottery.tex")
OUTPUT   = os.path.join(_ROOT, "tables", "security_lottery_filled.tex")
RESULTS  = os.path.join(_ROOT, "results", "headers", "security_lottery")

DATASETS  = {"D1": "global", "D2": "country_coded", "D3": "country_specific"}
LOCATIONS = ["ALGERIA", "GERMANY", "INDIA", "UAE", "USA"]


# ── Data loading ──────────────────────────────────────────────────────────────

def load_units(category: str) -> list[dict]:
    path = os.path.join(RESULTS, category, "lottery_units.json")
    if not os.path.exists(path):
        print(f"  MISSING: {path}")
        return []
    return json.load(open(path))


def load_cookie_records(category: str) -> list[dict]:
    path = os.path.join(RESULTS, category, "pg_cookie_records.json")
    if not os.path.exists(path):
        print(f"  MISSING: {path}")
        return []
    return json.load(open(path))


# ── Per-location rate computation ─────────────────────────────────────────────

def rates_by_location(
        units: list[dict],
        filter_fn,        # selects denominator units
        count_fn,         # counts numerator within filtered units
) -> dict[str, float]:
    """
    Returns {location: rate} where
      rate = count_fn(filtered) / len(filtered)
    for units filtered by filter_fn.
    """
    result = {}
    for loc in LOCATIONS:
        loc_units = [u for u in units if u["location"] == loc]
        filtered  = [u for u in loc_units if filter_fn(u)]
        n = len(filtered)
        result[loc] = count_fn(filtered) / n if n > 0 else 0.0
    return result


def cookie_rates_by_location(
        records: list[dict],
        count_fn,
) -> dict[str, float]:
    """Same but for cookie records (denominator = total cookies per location)."""
    result = {}
    for loc in LOCATIONS:
        loc_recs = [r for r in records if r["location"] == loc]
        n = len(loc_recs)
        result[loc] = count_fn(loc_recs) / n if n > 0 else 0.0
    return result


# ── Table row definitions ─────────────────────────────────────────────────────
# Each row: (display_name, filter_fn, count_fn, use_cookies)

def make_hsts_rows():
    # Denominator: sites that served HSTS at least once (has_hsts=True).
    # For policy classification, a single occurrence suffices — we classify
    # the best (max) policy the site has shown across sessions.
    hsts_filter = lambda u: u.get("has_hsts", False)
    return [
        # "Does not include preload" = hsts[2] == 0
        ("Does not include \\textit{preload}",
         hsts_filter,
         lambda us: sum(1 for u in us if u["hsts"][2] == 0)),
        # "Missing includeSubDomains" = hsts[1] == 0 (SUB_UNSAFE)
        ("Missing \\textit{includeSubDomains}",
         hsts_filter,
         lambda us: sum(1 for u in us if u["hsts"][1] == 0)),
        # "Non-standard max-age" = hsts[0] == 1 (LOW: 0 < age < 1yr)
        ("Non-standard \\textit{max-age}",
         hsts_filter,
         lambda us: sum(1 for u in us if u["hsts"][0] == 1)),
        # "Disabled" = hsts[0] == -1 (DISABLE: max-age=0)
        ("Disabled",
         hsts_filter,
         lambda us: sum(1 for u in us if u["hsts"][0] == -1)),
    ]


def make_csp_rows():
    # Denominator: sites that served CSP at least once (has_csp=True).
    csp_filter = lambda u: u.get("has_csp", False)
    return [
        # "Prone to XSS" = XSS == 0 (XSS_UNSAFE)
        ("Prone to XSS",
         csp_filter,
         lambda us: sum(1 for u in us if u["csp"]["XSS"] == 0)),
        # "Prone to TLS attacks" = TLS == 0 (TLS_UNSAFE)
        ("Prone to TLS attacks",
         csp_filter,
         lambda us: sum(1 for u in us if u["csp"]["TLS"] == 0)),
        # "Prone to framing attacks" = FA == 0 (FA_UNSAFE)
        ("Prone to framing attacks",
         csp_filter,
         lambda us: sum(1 for u in us if u["csp"]["FA"] == 0)),
        # "Invalid Policy" = FA == 3 (INVALID)
        ("Invalid Policy",
         csp_filter,
         lambda us: sum(1 for u in us if u["csp"]["FA"] == 3)),
    ]


def make_xfo_rows():
    # Denominator: sites that served XFO at least once (has_xfo=True).
    xfo_filter = lambda u: u.get("has_xfo", False)
    return [
        # SELF = xfo == 1
        ("SELF",   xfo_filter, lambda us: sum(1 for u in us if u["xfo"] == 1)),
        # NONE = xfo == 2 (deny framing entirely)
        ("NONE",   xfo_filter, lambda us: sum(1 for u in us if u["xfo"] == 2)),
        # UNSAFE = xfo == 0
        ("UNSAFE", xfo_filter, lambda us: sum(1 for u in us if u["xfo"] == 0)),
    ]


def make_cookie_rows():
    # Denominator: total cookie records per location
    # CookieSameSite: NONE=0, LAX=1, STRICT=2
    # CookieSecure: SECURE_UNSAFE=0, SECURE_SAFE=1
    # CookieHttpOnly: HTTP_UNSAFE=0, HTTP_SAFE=1
    return [
        ("SameSite: Strict",
         lambda rs: sum(1 for r in rs if r["samesite"] == 2)),
        ("SameSite: Lax",
         lambda rs: sum(1 for r in rs if r["samesite"] == 1)),
        ("SameSite: None",
         lambda rs: sum(1 for r in rs if r["samesite"] == 0)),
        ("{\\textit{secure} set to \\textit{false}}",
         lambda rs: sum(1 for r in rs if r["secure"] == 0)),
        ("\\textit{httpOnly} set to \\textit{false}",
         lambda rs: sum(1 for r in rs if r["httponly"] == 0)),
    ]


# ── Rate → cell string ────────────────────────────────────────────────────────

def fmt(rate: float) -> str:
    return f"{rate * 100:.2f}\\%"


def make_row_cells(
        loc_rates: dict[str, float],
) -> str:
    """Build the 5-location + mean + std cell string."""
    vals = [loc_rates.get(loc, 0.0) for loc in LOCATIONS]
    mean = np.mean(vals)
    std  = np.std(vals, ddof=1) if len(vals) > 1 else 0.0
    return " & ".join(fmt(v) for v in vals) + f" & {fmt(mean)} & {fmt(std)}"


# ── Build per-table body ──────────────────────────────────────────────────────

def build_table_body(
        row_defs: list,
        all_units: dict[str, list],       # {category: [units]}
        all_cookies: dict[str, list],     # {category: [records]} (for cookie table)
        use_cookies: bool = False,
) -> str:
    lines = []
    for row_def in row_defs:
        if use_cookies:
            display, count_fn = row_def
        else:
            display, filter_fn, count_fn = row_def

        first = True
        for dataset_label in ("D1", "D2", "D3"):
            category = DATASETS[dataset_label]
            if use_cookies:
                records = all_cookies.get(category, [])
                loc_rates = cookie_rates_by_location(records, count_fn)
            else:
                units = all_units.get(category, [])
                loc_rates = rates_by_location(units, filter_fn, count_fn)

            cells = make_row_cells(loc_rates)

            if first:
                lines.append(
                    f"        \\multirow{{3}}{{*}}{{{display}}} "
                    f"& {dataset_label} & {cells} \\\\"
                )
                first = False
            else:
                lines.append(f"        ~ & {dataset_label} & {cells} \\\\")

        lines.append("        \\hline")

    # Remove trailing \hline
    if lines and lines[-1].strip() == "\\hline":
        lines.pop()
    return "\n".join(lines)


# ── Template rewriting ────────────────────────────────────────────────────────

_BODY_RE = re.compile(
    r"(\\midrule\s*\n)(.*?)(\s*\\bottomrule)",
    re.DOTALL,
)


def fill_table_in_content(content: str, body: str) -> tuple[str, bool]:
    def _replacer(m):
        return m.group(1) + body + "\n" + m.group(3)
    new, n = _BODY_RE.subn(_replacer, content, count=1)
    return new, n > 0


# ── Console summary ───────────────────────────────────────────────────────────

def print_summary(label: str, row_defs: list, all_units: dict,
                  all_cookies: dict, use_cookies: bool):
    print(f"\n  [{label}]")
    for row_def in row_defs:
        display = row_def[0] if use_cookies else row_def[0]
        # Show global/ALGERIA value as spot check
        category = "global"
        if use_cookies:
            _, count_fn = row_def
            records = all_cookies.get(category, [])
            loc_rates = cookie_rates_by_location(records, count_fn)
        else:
            _, filter_fn, count_fn = row_def
            units = all_units.get(category, [])
            loc_rates = rates_by_location(units, filter_fn, count_fn)
        alg = loc_rates.get("ALGERIA", 0)
        usa = loc_rates.get("USA", 0)
        # Strip latex commands for display
        clean = re.sub(r'\\[a-zA-Z]+\{([^}]*)\}', r'\1', display).replace('\\', '')
        print(f"    {clean[:40]:<40}  ALGERIA={alg*100:.2f}%  USA={usa*100:.2f}%")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Loading lottery units and cookie records...")
    all_units   = {}
    all_cookies = {}
    for label, category in DATASETS.items():
        units = load_units(category)
        all_units[category] = units
        print(f"  [{label}] {category}: {len(units):,} units")
        cookies = load_cookie_records(category)
        all_cookies[category] = cookies
        print(f"         cookies: {len(cookies):,} records")

    # Define row structures
    hsts_rows   = make_hsts_rows()
    csp_rows    = make_csp_rows()
    xfo_rows    = make_xfo_rows()
    cookie_rows = make_cookie_rows()

    print("\nSpot-check values (global/ALGERIA):")
    print_summary("HSTS",   hsts_rows,   all_units, all_cookies, False)
    print_summary("CSP",    csp_rows,    all_units, all_cookies, False)
    print_summary("XFO",    xfo_rows,    all_units, all_cookies, False)
    print_summary("Cookie", cookie_rows, all_units, all_cookies, True)

    # Build bodies for each of the 4 tables
    hsts_body   = build_table_body(hsts_rows,   all_units, all_cookies, False)
    csp_body    = build_table_body(csp_rows,    all_units, all_cookies, False)
    xfo_body    = build_table_body(xfo_rows,    all_units, all_cookies, False)
    cookie_body = build_table_body(cookie_rows, all_units, all_cookies, True)

    bodies = [hsts_body, csp_body, xfo_body, cookie_body]

    # Read template and replace bodies in order (one per \midrule...\bottomrule block)
    with open(TEMPLATE) as f:
        content = f.read()

    # Split on table boundaries, fill each body in order, then rejoin.
    # We process the string left-to-right, replacing one block at a time
    # by advancing a cursor past each filled block.
    filled = content
    cursor = 0
    for i, body in enumerate(bodies):
        # Search from cursor onwards
        segment = filled[cursor:]
        m = _BODY_RE.search(segment)
        if not m:
            print(f"\n  WARNING: could not find body block for table {i+1}")
            continue
        replacement = m.group(1) + body + "\n" + m.group(3)
        abs_start = cursor + m.start()
        abs_end   = cursor + m.end()
        filled = filled[:abs_start] + replacement + filled[abs_end:]
        # Advance cursor past the replacement so next search starts after it
        cursor = abs_start + len(replacement)
        print(f"\n  Table {i+1} filled successfully")

    with open(OUTPUT, "w") as f:
        f.write(filled)
    print(f"\nSaved: {OUTPUT}")


if __name__ == "__main__":
    main()
