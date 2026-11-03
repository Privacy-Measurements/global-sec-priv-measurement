"""
Fill tables/headers.tex with real adoption rates from analysis results.

Source: results/headers/header_deployment/<category>/adoption_rates_stable.csv
        (Tier A — stable across all visits, status=200 only)

Denominator: eTLDs with at least one successful (status=200) main-frame request
             per location — already enforced by the har_requests status=200 filter
             added to header_deployment.py FETCH_QUERY.

Datasets:  D1 = global, D2 = country_coded, D3 = country_specific
Locations: ALGERIA, GERMANY, INDIA, UAE, USA (5 columns in table)
Mean/Std:  computed from the 5 location values per row.

Missing headers (not in analysis results) are silently skipped — the
original dummy values are preserved in the output for those rows.
"""

import sys
import os
_HERE = os.path.abspath(os.path.dirname(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "../.."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import re
import numpy as np
import pandas as pd

# ── Configuration ─────────────────────────────────────────────────────────────

TEMPLATE  = os.path.join(_ROOT, "tables", "headers.tex")
OUTPUT    = os.path.join(_ROOT, "tables", "headers_filled.tex")
RESULTS   = os.path.join(_ROOT, "results", "headers", "header_deployment")

# Dataset label → category folder name
DATASETS = {
    "D1": "global",
    "D2": "country_coded",
    "D3": "country_specific",
}

# Table column order (must match tex column order)
LOCATIONS = ["ALGERIA", "GERMANY", "INDIA", "UAE", "USA"]

# Map: adoption_rates_stable.csv column suffix → table location name
# Columns in CSV are like "global|ALGERIA", "global|GERMANY", etc.
# We strip the "category|" prefix and match to LOCATIONS.

# Map: header display name in tex → lowercase key in CSV index
HEADER_MAP = {
    "X-Content-Type-Options":              "x-content-type-options",
    "Strict-Transport-Security":           "strict-transport-security",
    "X-Frame-Options":                     "x-frame-options",
    "Set-Cookie":                          "set-cookie",
    "X-XSS-Protection":                    "x-xss-protection",
    "Content-Security-Policy":             "content-security-policy",
    "Report-To":                           "report-to",
    "Permissions-Policy":                  "permissions-policy",
    "Access-Control-Allow-Origin":         "access-control-allow-origin",
    "X-Permitted-Cross-Domain-Policies":   "x-permitted-cross-domain-policies",
    "Cross-Origin-Resource-Policy":        "cross-origin-resource-policy",
    "Content-Security-Policy-Report-Only": "content-security-policy-report-only",
    "Access-Control-Allow-Methods":        "access-control-allow-methods",
    "Cross-Origin-Opener-Policy":          "cross-origin-opener-policy",
    "Access-Control-Allow-Credentials":    "access-control-allow-credentials",
    "Reporting-Endpoints":                 "reporting-endpoints",
    "Access-Control-Allow-Headers":        "access-control-allow-headers",
    "Cross-Origin-Embedder-Policy":        "cross-origin-embedder-policy",
    "Expect-CT":                           "expect-ct",
    "Access-Control-Expose-Headers":       "access-control-expose-headers",
    "Timing-Allow-Origin":                 "timing-allow-origin",
    "Access-Control-Max-Age":              "access-control-max-age",
    "Origin-Agent-Cluster":                "origin-agent-cluster",
    # Add any further headers here
}


# ── Load adoption rates ───────────────────────────────────────────────────────

def load_stable_rates(category: str) -> dict[str, dict[str, float]] | None:
    """
    Load adoption_rates_stable.csv for a dataset category.
    Returns {header_key: {location: rate_0_to_1}} or None if file missing.

    The CSV index is the header name (lowercase), columns are "category|LOCATION".
    """
    path = os.path.join(RESULTS, category, "adoption_rates_stable.csv")
    if not os.path.exists(path):
        print(f"  WARNING: missing {path}")
        return None

    df = pd.read_csv(path, index_col=0)

    # Build lookup: header_key -> {location -> rate}
    result: dict[str, dict[str, float]] = {}
    for header_key in df.index:
        rates = {}
        for col in df.columns:
            # col format: "global|ALGERIA" or "country_specific|GERMANY" etc.
            parts = col.split("|")
            if len(parts) == 2:
                loc = parts[1].upper()
            else:
                loc = col.upper()
            if loc in LOCATIONS:
                val = df.loc[header_key, col]
                rates[loc] = float(val) if pd.notna(val) else 0.0
        if rates:
            result[header_key.lower()] = rates

    return result


# ── Format helpers ────────────────────────────────────────────────────────────

def fmt(rate: float) -> str:
    """Format a 0–1 rate as 'XX.XX%'."""
    return f"{rate * 100:.2f}\\%"


def build_row_values(
        header_display: str,
        dataset_label: str,
        all_rates: dict[str, dict[str, dict[str, float]]],
) -> str | None:
    """
    Build the 7 cell values (5 locations + mean + std) for one data row.
    Returns None if no data available for this header × dataset combination.
    """
    category    = DATASETS[dataset_label]
    header_key  = HEADER_MAP.get(header_display)
    if header_key is None:
        return None

    rates_for_cat = all_rates.get(category)
    if rates_for_cat is None:
        return None

    rates_for_hdr = rates_for_cat.get(header_key)
    if rates_for_hdr is None:
        return None

    vals = []
    for loc in LOCATIONS:
        v = rates_for_hdr.get(loc)
        if v is None:
            return None   # missing location — skip this row
        vals.append(v)

    mean = np.mean(vals)
    std  = np.std(vals, ddof=1)   # sample std to match typical reporting

    cells = " & ".join(fmt(v) for v in vals)
    return f"{cells} & {fmt(mean)} & {fmt(std)}"


# ── LaTeX rewriting ───────────────────────────────────────────────────────────

# Regex to match a data row:
# "~ & D1 & 40.45\% & ... \\" or "D1 & 40.45\% & ..."
# The row structure from the template:
#   "\multirow{3}{*}{HeaderName} & D1 & v1 & v2 & v3 & v4 & v5 & mean & std \\"
#   "~ & D2 & ..."
#   "~ & D3 & ..."

_MULTIROW_RE = re.compile(
    r"(\\multirow\{3\}\{\*\}\{([^}]+)\}\s*&\s*)(D[123])"
    r"(\s*&\s*)(.*?)(\\\\)"
)
_TILDE_ROW_RE = re.compile(
    r"(\s*~\s*&\s*)(D[123])"
    r"(\s*&\s*)(.*?)(\\\\)"
)


def fill_table(template_path: str, output_path: str,
               all_rates: dict) -> None:
    with open(template_path, "r") as f:
        lines = f.readlines()

    out_lines = []
    current_header: str | None = None

    for line in lines:
        # Match \multirow line — captures header display name and D1/D2/D3
        m = _MULTIROW_RE.search(line)
        if m:
            current_header = m.group(2).strip()
            dataset_label  = m.group(3)
            new_vals = build_row_values(current_header, dataset_label, all_rates)
            if new_vals is not None:
                # Replace from the dataset label up to the "\\" terminator,
                # preserving anything after it verbatim (e.g. a trailing
                # "\hline"/"\midrule" on the last row of a \multirow group).
                trailing = line[m.end(6):]
                new_line = (line[:m.start(3)]
                            + dataset_label
                            + m.group(4)      # " & "
                            + new_vals
                            + " " + m.group(6)  # "\\"
                            + trailing)
                if not trailing.endswith("\n"):
                    new_line += "\n"
                out_lines.append(new_line)
                print(f"  Filled: {current_header} / {dataset_label}")
            else:
                out_lines.append(line)
                print(f"  Kept dummy: {current_header} / {dataset_label}")
            continue

        # Match ~ & D2/D3 continuation rows
        m2 = _TILDE_ROW_RE.search(line)
        if m2 and current_header is not None:
            dataset_label = m2.group(2)
            new_vals = build_row_values(current_header, dataset_label, all_rates)
            if new_vals is not None:
                trailing = line[m2.end(5):]
                new_line = (line[:m2.start(2)]
                            + dataset_label
                            + m2.group(3)       # " & "
                            + new_vals
                            + " " + m2.group(5)  # "\\"
                            + trailing)
                if not trailing.endswith("\n"):
                    new_line += "\n"
                out_lines.append(new_line)
                print(f"  Filled: {current_header} / {dataset_label}")
            else:
                out_lines.append(line)
                print(f"  Kept dummy: {current_header} / {dataset_label}")
            continue

        out_lines.append(line)

    with open(output_path, "w") as f:
        f.writelines(out_lines)

    print(f"\nWritten: {output_path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Loading adoption rates (Tier A — stable, status=200)...")
    all_rates: dict[str, dict[str, dict[str, float]] | None] = {}
    for label, category in DATASETS.items():
        print(f"  [{label}] {category}")
        all_rates[category] = load_stable_rates(category)

    missing = [c for c, r in all_rates.items() if r is None]
    if missing:
        print(f"\nWARNING: No data for: {missing}")
        print("Run header_deployment.py for those datasets first.")
        if all(r is None for r in all_rates.values()):
            print("No data available at all — aborting.")
            return

    print(f"\nFilling table from: {TEMPLATE}")
    fill_table(TEMPLATE, OUTPUT, all_rates)

    print("\nDone.")
    print(f"  Input : {TEMPLATE}")
    print(f"  Output: {OUTPUT}")


if __name__ == "__main__":
    main()
