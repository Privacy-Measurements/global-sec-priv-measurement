"""
Fill tables/third-party_scripts.tex with real third-party script inclusion
rates from analysis results.

Source: results/scripts/<category>/tp_source_table.csv
        Columns: domain, top_fqdn, location, category, etld_count, session_count, tier

Metric: etld_count / total_etlds_for_(category, location)
        = fraction of websites in that (dataset, location) bucket including
          this script domain at least once.

Total eTLD denominator: read from results/headers/header_deployment/<category>/
        deployment_by_etld_location.csv — counts eTLDs per (category, location)
        with at least one successful (status=200) main-frame request.
        Falls back to the maximum etld_count seen for that (category, location)
        in tp_source_table.csv.

Ordering: descending by cumulative etld_count summed across all datasets
          and all locations (matches caption: "ordered in descending order of
          their occurrences across the cumulative sum").

Script display label: top_fqdn column (most common full subdomain seen for
          that registered domain, e.g. "www.googletagmanager.com").

The script:
  1. Loads data for D1 (global), D2 (country_coded), D3 (country_specific)
  2. Ranks domains by cumulative sum
  3. Matches domains to existing rows in the template (by top_fqdn or
     registered domain)
  4. Fills or regenerates the table body
  5. Writes tables/third-party_scripts_filled.tex
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

TEMPLATE = os.path.join(_ROOT, "tables", "third-party_scripts.tex")
OUTPUT   = os.path.join(_ROOT, "tables", "third-party_scripts_filled.tex")

SCRIPTS_RESULTS  = os.path.join(_ROOT, "results", "scripts")
HEADERS_RESULTS  = os.path.join(_ROOT, "results", "headers", "header_deployment")

DATASETS = {"D1": "global", "D2": "country_coded", "D3": "country_specific"}
LOCATIONS = ["ALGERIA", "GERMANY", "INDIA", "UAE", "USA"]

# Top-N domains to include (matches number of rows in template)
TOP_N = 20


# ── Data loading ──────────────────────────────────────────────────────────────

def load_source_table(category: str) -> pd.DataFrame:
    path = os.path.join(SCRIPTS_RESULTS, category, "tp_source_table.csv")
    if not os.path.exists(path):
        print(f"  MISSING: {path}")
        return pd.DataFrame()
    return pd.read_csv(path)


def load_etld_totals(category: str) -> dict[str, int]:
    """
    Returns {location: total_etld_count} for this category.
    Primary source: deployment_by_etld_location.csv (status=200 filtered).
    Fallback: max etld_count across all domains in tp_source_table.
    """
    deploy_path = os.path.join(HEADERS_RESULTS, category,
                               "deployment_by_etld_location.csv")
    if os.path.exists(deploy_path):
        df = pd.read_csv(deploy_path)
        if "location" in df.columns:
            return df.groupby("location")["etld"].nunique().to_dict()

    # Fallback
    st = load_source_table(category)
    if st.empty or "location" not in st.columns:
        return {}
    return st.groupby("location")["etld_count"].max().to_dict()


# ── Rank domains by cumulative presence ───────────────────────────────────────

def build_ranked_domains(
        all_source_tables: dict[str, pd.DataFrame],
        top_n: int = TOP_N,
) -> pd.DataFrame:
    """
    Combine source tables across all datasets and locations.
    Rank by descending cumulative etld_count (sum over all D × L combinations).
    Returns DataFrame with top_n rows: domain, top_fqdn, cumulative_count.
    """
    frames = []
    for category, df in all_source_tables.items():
        if not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    if "top_fqdn" not in combined.columns:
        combined["top_fqdn"] = combined["domain"]

    # Most common top_fqdn per registered domain across all datasets
    fqdn_map = (combined.groupby(["domain", "top_fqdn"])
                        .size().reset_index(name="n")
                        .sort_values("n", ascending=False)
                        .groupby("domain")["top_fqdn"].first()
                        .to_dict())

    ranked = (combined.groupby("domain")["etld_count"]
                      .sum()
                      .reset_index(name="cumulative_count")
                      .sort_values("cumulative_count", ascending=False)
                      .head(top_n))
    ranked["top_fqdn"] = ranked["domain"].map(fqdn_map).fillna(ranked["domain"])
    return ranked.reset_index(drop=True)


# ── Rate computation ──────────────────────────────────────────────────────────

def get_rate(
        domain: str,
        dataset_label: str,
        location: str,
        source_tables: dict[str, pd.DataFrame],
        etld_totals: dict[str, dict[str, int]],
) -> float | None:
    """
    Returns etld_count / total_etlds for (domain, dataset, location).
    Returns None if data unavailable.
    """
    category = DATASETS[dataset_label]
    df = source_tables.get(category)
    if df is None or df.empty:
        return None

    row = df[(df["domain"] == domain) & (df["location"] == location)]
    if row.empty:
        return 0.0  # domain not seen in this (category, location) → 0%

    count = row["etld_count"].iloc[0]
    total = etld_totals.get(category, {}).get(location)
    if not total:
        return None
    return count / total


def fmt(rate: float) -> str:
    """Format rate as percentage — trim trailing zeros for compactness."""
    pct = rate * 100
    s = f"{pct:.2f}"
    s = s.rstrip("0").rstrip(".")
    return s + "\\%"


def fmt_stat(val: float) -> str:
    """Format mean/std as percentage with 2 decimal places."""
    return f"{val * 100:.2f}\\%"


# ── Table body generation ─────────────────────────────────────────────────────

def build_table_body(
        ranked: pd.DataFrame,
        source_tables: dict[str, pd.DataFrame],
        all_totals: dict[str, dict[str, int]],
) -> str:
    """
    Generate the full table body (rows only, no header/footer boilerplate).
    Returns a string of LaTeX rows.
    """
    lines = []
    for _, r in ranked.iterrows():
        domain    = r["domain"]
        fqdn      = r["top_fqdn"]
        first_row = True

        for dataset_label in ("D1", "D2", "D3"):
            cells = []
            valid_rates = []
            for loc in LOCATIONS:
                rate = get_rate(domain, dataset_label, loc,
                                source_tables, all_totals)
                cells.append(fmt(rate) if rate is not None else "---")
                if rate is not None:
                    valid_rates.append(rate)

            # Mean and Std. Dev across the 5 locations
            if valid_rates:
                mean_val = np.mean(valid_rates)
                std_val  = np.std(valid_rates, ddof=1) if len(valid_rates) > 1 else 0.0
                mean_str = fmt_stat(mean_val)
                std_str  = fmt_stat(std_val)
            else:
                mean_str = "---"
                std_str  = "---"

            row_vals = " & ".join(cells) + f" & {mean_str} & {std_str}"
            if first_row:
                fqdn_escaped = fqdn.replace("_", r"\_").replace("%", r"\%")
                lines.append(
                    f"        \\multirow{{3}}{{*}}{{{fqdn_escaped}}} "
                    f"& {dataset_label} & {row_vals} \\\\"
                )
                first_row = False
            else:
                lines.append(f"        ~ & {dataset_label} & {row_vals} \\\\")

            if dataset_label != "D3":
                pass  # no \hline between D1/D2/D3 rows within same domain
        lines.append("        \\hline")

    # Remove trailing \hline
    if lines and lines[-1].strip() == "\\hline":
        lines.pop()

    return "\n".join(lines)


# ── Template rewriting ────────────────────────────────────────────────────────

_BODY_RE = re.compile(
    r"(\\midrule\s*\n)"         # after \midrule
    r"(.*?)"                     # existing body
    r"(\s*\\bottomrule)",        # before \bottomrule
    re.DOTALL,
)


def fill_table(template_path: str, output_path: str, body: str) -> None:
    with open(template_path, "r") as f:
        content = f.read()

    def _replacer(m):
        return m.group(1) + body + "\n" + m.group(3)

    new_content, n = _BODY_RE.subn(_replacer, content)
    if n == 0:
        print("  WARNING: could not locate table body — writing body only")
        new_content = content  # leave unchanged, write separately

    with open(output_path, "w") as f:
        f.write(new_content)
    print(f"  Written: {output_path}")


# ── Console summary ───────────────────────────────────────────────────────────

def print_summary(ranked: pd.DataFrame, source_tables: dict, all_totals: dict):
    print(f"\n  Top {len(ranked)} domains by cumulative eTLD presence:")
    print(f"  {'FQDN':<45} {'Cumulative':>12}")
    print("  " + "─" * 60)
    for _, r in ranked.iterrows():
        print(f"  {r['top_fqdn']:<45} {r['cumulative_count']:>12,}")
    print()
    for label, category in DATASETS.items():
        totals = all_totals.get(category, {})
        if totals:
            print(f"  [{label}] {category} denominators: "
                  + ", ".join(f"{l}={v}" for l, v in sorted(totals.items())
                              if l in LOCATIONS))


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Loading tp_source_table.csv for each dataset...")
    source_tables: dict[str, pd.DataFrame] = {}
    all_totals:    dict[str, dict[str, int]] = {}

    for label, category in DATASETS.items():
        print(f"  [{label}] {category}")
        df = load_source_table(category)
        source_tables[category] = df
        all_totals[category]    = load_etld_totals(category)
        if df.empty:
            print(f"    No data — will show 0% or --- for all {label} cells")
        else:
            print(f"    {len(df):,} domain × location rows loaded")

    print("\nRanking domains by cumulative eTLD presence...")
    ranked = build_ranked_domains(source_tables, top_n=TOP_N)
    if ranked.empty:
        print("ERROR: No data available. Run script_analysis.py first.")
        return

    print_summary(ranked, source_tables, all_totals)

    print(f"\nBuilding table body ({len(ranked)} domains)...")
    body = build_table_body(ranked, source_tables, all_totals)

    print(f"Filling template: {TEMPLATE}")
    fill_table(TEMPLATE, OUTPUT, body)

    print(f"\nDone.")
    print(f"  Input : {TEMPLATE}")
    print(f"  Output: {OUTPUT}")


if __name__ == "__main__":
    main()
