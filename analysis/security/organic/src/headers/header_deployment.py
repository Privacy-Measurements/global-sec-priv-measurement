"""
Component 1 — Security Header Deployment
Unit of analysis: etld x location
Source: har_requests.response_headers WHERE is_main_frame = 1
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

from src.utils.consistency import (
    ConsistencyTagger, load_visit_map, filter_sufficient_visits,
    TIER_A, TIER_B, TIER_ABSENT,
    get_header_normaliser,
)

# ── DB helpers ────────────────────────────────────────────────────────────────

FETCH_QUERY = """
    SELECT
        cs.etld,
        cs.location,
        cs.category,
        cs.id AS session_id,
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

def fetch_sessions(db_config: dict, category: str) -> pd.DataFrame:
    conn = pymysql.connect(**db_config)
    df = pd.read_sql(FETCH_QUERY, conn, params=(category,))
    conn.close()
    return df


# ── Header extraction ─────────────────────────────────────────────────────────

def extract_present_headers(response_headers_json: str) -> set:
    """Return lowercase set of security header names present in a response."""
    try:
        headers = json.loads(response_headers_json) if isinstance(response_headers_json, str) else response_headers_json
        lowered = {k.lower() for k in headers.keys()}
        return lowered & set(SECURITY_HEADERS)
    except Exception:
        return set()


# ── Multi-session conflict resolution ────────────────────────────────────────

def resolve_sessions(group: pd.DataFrame,
                     tagger: ConsistencyTagger) -> dict:
    """
    For a single etld x location group (multiple sessions), resolve which
    security headers are deployed and at which consistency tier.

    Tier A ("stable"):     header present in ALL visits for this etld × location.
    Tier B ("meaningful"): header present in AT LEAST ONE visit.
    Absent:                never observed.

    Returns:
        {
          "tiers":         {header: tier_label},
          "session_count": int,
          "visit_count":   int,
        }
    """
    etld     = group["etld"].iloc[0]
    location = group["location"].iloc[0]
    session_count = len(group)
    per_session_headers = {
        row["session_id"]: extract_present_headers(row["response_headers"])
        for _, row in group.iterrows()
    }

    tiers = {}
    for h in SECURITY_HEADERS:
        observed_sids = {sid for sid, headers in per_session_headers.items()
                         if h in headers}
        tiers[h] = tagger.tag_binary(etld, location, observed_sids)

    return {
        "tiers":         tiers,
        "session_count": session_count,
        "visit_count":   tagger.visit_count(etld, location),
    }


# ── Build analysis frame ──────────────────────────────────────────────────────

def build_deployment_frame(raw_df: pd.DataFrame,
                           tagger: ConsistencyTagger) -> pd.DataFrame:
    """
    Aggregate raw HAR rows to one row per etld x location.
    For each security header adds:
      <header>          — 1 if Tier A or Tier B (deployed in ≥1 visit), else 0
      <header>__tier    — "stable" | "meaningful" | "absent"
    Also adds visit_count and flags insufficient_visits pairs.
    """
    records = []
    for (etld, location, category), group in raw_df.groupby(
            ["etld", "location", "category"]):
        resolved = resolve_sessions(group, tagger)
        row = {
            "etld":                etld,
            "location":            location,
            "category":            category,
            "session_count":       resolved["session_count"],
            "visit_count":         resolved["visit_count"],
            "insufficient_visits": resolved["visit_count"] < MIN_VISITS,
        }
        for h in SECURITY_HEADERS:
            tier = resolved["tiers"][h]
            row[h]              = int(tier in (TIER_A, TIER_B))
            row[f"{h}__tier"]   = tier
        records.append(row)
    return pd.DataFrame(records)


# ── Statistics ────────────────────────────────────────────────────────────────

def compute_adoption_rates(df: pd.DataFrame,
                           tier: str | None = None) -> pd.DataFrame:
    """
    Returns a DataFrame: index = header name,
    columns = (dataset, location) multi-index with adoption rate [0-1].

    tier: if "stable" or "meaningful", only count headers at that tier or above.
          None = count any deployed header (Tier A or B).
    """
    rows = []
    for header in SECURITY_HEADERS:
        row = {"header": header}
        tier_col = f"{header}__tier"
        for (cat, loc), grp in df.groupby(["category", "location"]):
            if tier == TIER_A:
                rate = (grp[tier_col] == TIER_A).mean()
            else:
                rate = grp[header].mean()
            row[f"{cat}|{loc}"] = rate
        rows.append(row)
    return pd.DataFrame(rows).set_index("header")


def compute_header_counts(df: pd.DataFrame) -> pd.Series:
    """Total security headers deployed per etld x location."""
    return df[SECURITY_HEADERS].sum(axis=1)


def zero_header_rate(df: pd.DataFrame) -> pd.DataFrame:
    """% of etld x location with zero security headers, per dataset x location."""
    df = df.copy()
    df["_count"] = df[SECURITY_HEADERS].sum(axis=1)
    rows = []
    for (cat, loc), grp in df.groupby(["category", "location"]):
        rows.append({
            "category": cat,
            "location": loc,
            "total_sites": len(grp),
            "zero_header_sites": (grp["_count"] == 0).sum(),
            "zero_header_pct": (grp["_count"] == 0).mean() * 100,
        })
    return pd.DataFrame(rows)


def compute_inconsistency_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Count of etld × location pairs where header is Tier B (meaningful but not stable).
    These are headers observed at least once but not in all visits.
    """
    rows = []
    for h in SECURITY_HEADERS:
        tier_col = f"{h}__tier"
        if tier_col in df.columns:
            n_b = (df[tier_col] == TIER_B).sum()
            n_a = (df[tier_col] == TIER_A).sum()
            if n_b > 0:
                rows.append({"header": h, "tier_b_pairs": n_b, "tier_a_pairs": n_a})
    if not rows:
        return pd.Series(dtype=int, name="tier_b_pairs")
    return (pd.DataFrame(rows)
              .set_index("header")["tier_b_pairs"]
              .sort_values(ascending=False))


# ── Per-dataset plots & tables ────────────────────────────────────────────────

def plot_adoption_rates(adoption_df: pd.DataFrame, out_dir: str,
                        suffix: str = "", title_suffix: str = ""):
    cols = list(adoption_df.columns)
    n_headers = len(adoption_df)
    n_cols = len(cols)

    x = range(n_headers)
    width = 0.8 / n_cols
    fig, ax = plt.subplots(figsize=(max(14, n_headers * 0.6), 6))

    for i, col in enumerate(cols):
        offset = (i - n_cols / 2) * width + width / 2
        ax.bar([xi + offset for xi in x], adoption_df[col] * 100,
               width=width * 0.9, label=col)

    ax.set_xticks(list(x))
    ax.set_xticklabels(adoption_df.index, rotation=45, ha="right", fontsize=8)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax.set_ylabel("Adoption Rate (%)")
    ax.set_title(f"Security Header Adoption Rate by Location{title_suffix}")
    ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=7)
    plt.tight_layout()
    plt.savefig(f"{out_dir}/adoption_rates{suffix}.pdf", bbox_inches="tight")
    plt.savefig(f"{out_dir}/adoption_rates{suffix}.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved adoption_rates{suffix}.pdf/png")


def plot_codeployment_heatmap(df: pd.DataFrame, out_dir: str):
    binary = df[SECURITY_HEADERS].astype(float)
    deployed_headers = [h for h in SECURITY_HEADERS if binary[h].sum() > 0]
    if len(deployed_headers) < 2:
        print("  Skipping co-deployment heatmap (insufficient data)")
        return
    corr = binary[deployed_headers].corr(method="pearson")
    fig, ax = plt.subplots(figsize=(max(10, len(deployed_headers) * 0.55),
                                    max(8, len(deployed_headers) * 0.5)))
    sns.heatmap(corr, ax=ax, annot=False, fmt=".2f", cmap="coolwarm",
                vmin=-1, vmax=1, linewidths=0.3, square=True,
                xticklabels=deployed_headers, yticklabels=deployed_headers)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right", fontsize=7)
    ax.set_yticklabels(ax.get_yticklabels(), rotation=0, fontsize=7)
    ax.set_title("Security Header Co-deployment Correlation (Pearson)")
    plt.tight_layout()
    plt.savefig(f"{out_dir}/codeployment_heatmap.pdf", bbox_inches="tight")
    plt.savefig(f"{out_dir}/codeployment_heatmap.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved codeployment_heatmap.pdf/png")


def plot_header_count_distribution(df: pd.DataFrame, out_dir: str):
    locations = sorted(df["location"].unique())
    fig, axes = plt.subplots(1, len(locations),
                             figsize=(6 * len(locations), 4), sharey=False)
    if len(locations) == 1:
        axes = [axes]
    max_count = int(df[SECURITY_HEADERS].sum(axis=1).max())
    bins = range(0, max_count + 2)
    for ax, loc in zip(axes, locations):
        counts = df[df["location"] == loc][SECURITY_HEADERS].sum(axis=1)
        ax.hist(counts, bins=bins, edgecolor="white", color="steelblue", align="left")
        ax.set_xlabel("# Security Headers Deployed")
        ax.set_ylabel("# Sites (etld × location)")
        ax.set_title(loc)
        ax.set_xticks(list(bins)[:-1])
    fig.suptitle("Distribution of Security Header Count per Site", y=1.02)
    plt.tight_layout()
    plt.savefig(f"{out_dir}/header_count_distribution.pdf", bbox_inches="tight")
    plt.savefig(f"{out_dir}/header_count_distribution.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved header_count_distribution.pdf/png")


# ── LaTeX output ──────────────────────────────────────────────────────────────

def adoption_to_latex(adoption_df: pd.DataFrame) -> str:
    cols = list(adoption_df.columns)
    col_spec = "l" + "r" * len(cols)
    header_row = " & ".join(["Header"] + [c.replace("_", r"\_").replace("|", " / ") for c in cols]) + r" \\"
    lines = [
        r"\begin{table}[ht]",
        r"\centering",
        r"\small",
        r"\begin{tabular}{" + col_spec + "}",
        r"\toprule",
        header_row,
        r"\midrule",
    ]
    for h, row in adoption_df.iterrows():
        vals = " & ".join([h.replace("-", r"\mbox{-}")] + [f"{v*100:.1f}\\%" for v in row])
        lines.append(vals + r" \\")
    lines += [
        r"\bottomrule",
        r"\end{tabular}",
        r"\caption{Security header adoption rate per location (\% of \textit{etld} $\times$ \textit{location} pairs).}",
        r"\label{tab:header_adoption}",
        r"\end{table}",
    ]
    return "\n".join(lines)


def zero_header_to_latex(zero_df: pd.DataFrame) -> str:
    lines = [
        r"\begin{table}[ht]",
        r"\centering",
        r"\small",
        r"\begin{tabular}{llrrr}",
        r"\toprule",
        r"Category & Location & Total Sites & Zero-Header Sites & \% \\" ,
        r"\midrule",
    ]
    for _, row in zero_df.iterrows():
        lines.append(
            f"{row['category']} & {row['location']} & {int(row['total_sites'])} "
            f"& {int(row['zero_header_sites'])} & {row['zero_header_pct']:.1f}\\% \\\\"
        )
    lines += [
        r"\bottomrule",
        r"\end{tabular}",
        r"\caption{Sites deploying zero security headers, by location.}",
        r"\label{tab:zero_headers}",
        r"\end{table}",
    ]
    return "\n".join(lines)


# ── Console summary ───────────────────────────────────────────────────────────

def print_summary(df: pd.DataFrame, adoption_df: pd.DataFrame, zero_df: pd.DataFrame,
                  inc_summary: pd.Series, category: str):
    total = len(df)
    print(f"\n{'='*70}")
    print(f"COMPONENT 1 — SECURITY HEADER DEPLOYMENT: {category.upper()}")
    print(f"{'='*70}")
    print(f"Total etld × location pairs analysed: {total:,}")
    for loc, grp in df.groupby("location"):
        print(f"    {loc}: {len(grp):,} sites")

    print(f"\n{'─'*70}")
    print(f"ADOPTION RATES (% of etld × location pairs):")
    print(f"{'─'*70}")
    col_w = max(len(c) for c in adoption_df.columns) + 2
    header_line = f"{'Header':<40}" + "".join(f"{c:>{col_w}}" for c in adoption_df.columns)
    print(header_line)
    print("─" * len(header_line))
    for h, row in adoption_df.iterrows():
        vals = "".join(f"{v*100:>{col_w-1}.1f}% " for v in row)
        print(f"{h:<40}{vals}")

    print(f"\n{'─'*70}")
    print(f"ZERO-HEADER SITES:")
    print(f"{'─'*70}")
    print(zero_df.to_string(index=False))

    if not inc_summary.empty:
        print(f"\n{'─'*70}")
        print(f"MULTI-SESSION INCONSISTENCIES:")
        print(f"{'─'*70}")
        print(inc_summary.to_string())

    print(f"\n{'─'*70}")
    print(f"TOP 10 MOST DEPLOYED HEADERS:")
    print(f"{'─'*70}")
    overall = df[SECURITY_HEADERS].mean().sort_values(ascending=False).head(10)
    for h, rate in overall.items():
        print(f"  {h:<45} {rate*100:5.1f}%")
    print()


# ── Cross-dataset comparison ──────────────────────────────────────────────────

def plot_cross_dataset_adoption(all_dfs: dict[str, pd.DataFrame], out_dir: str):
    """
    Heatmap: rows = headers, columns = datasets (aggregated across locations).
    Cell = adoption rate %.
    """
    rows = {}
    for cat, df in all_dfs.items():
        rows[cat] = df[SECURITY_HEADERS].mean()
    heat = pd.DataFrame(rows) * 100  # headers × datasets

    # Sort headers by mean adoption descending
    heat = heat.loc[heat.mean(axis=1).sort_values(ascending=False).index]

    fig, ax = plt.subplots(figsize=(max(6, len(all_dfs) * 1.5), max(8, len(heat) * 0.45)))
    sns.heatmap(heat, ax=ax, annot=True, fmt=".1f", cmap="YlOrRd",
                vmin=0, vmax=100, linewidths=0.4,
                cbar_kws={"label": "Adoption rate (%)"})
    ax.set_xticklabels(ax.get_xticklabels(), rotation=30, ha="right", fontsize=9)
    ax.set_yticklabels(ax.get_yticklabels(), rotation=0, fontsize=8)
    ax.set_title("Security Header Adoption Rate Across Datasets (% of etld × location)")
    plt.tight_layout()
    plt.savefig(f"{out_dir}/cross_dataset_adoption_heatmap.pdf", bbox_inches="tight")
    plt.savefig(f"{out_dir}/cross_dataset_adoption_heatmap.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved cross_dataset_adoption_heatmap.pdf/png")


def plot_cross_dataset_zero_headers(all_dfs: dict[str, pd.DataFrame], out_dir: str):
    """Grouped bar: % of sites with zero security headers per dataset × location."""
    records = []
    for cat, df in all_dfs.items():
        df = df.copy()
        df["_count"] = df[SECURITY_HEADERS].sum(axis=1)
        for loc, grp in df.groupby("location"):
            records.append({
                "dataset": cat,
                "location": loc,
                "zero_pct": (grp["_count"] == 0).mean() * 100,
            })
    plot_df = pd.DataFrame(records)

    datasets = sorted(plot_df["dataset"].unique())
    locations = sorted(plot_df["location"].unique())
    x = range(len(locations))
    width = 0.8 / len(datasets)

    fig, ax = plt.subplots(figsize=(max(10, len(locations) * 1.2), 5))
    for i, cat in enumerate(datasets):
        sub = plot_df[plot_df["dataset"] == cat].set_index("location")
        vals = [sub.loc[loc, "zero_pct"] if loc in sub.index else 0 for loc in locations]
        offset = (i - len(datasets) / 2) * width + width / 2
        ax.bar([xi + offset for xi in x], vals, width=width * 0.9, label=cat, edgecolor="white")
    ax.set_xticks(list(x))
    ax.set_xticklabels(locations, rotation=30, ha="right", fontsize=8)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax.set_ylabel("% of sites with zero security headers")
    ax.set_title("Sites with Zero Security Headers by Dataset × Location")
    ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=8)
    plt.tight_layout()
    plt.savefig(f"{out_dir}/cross_dataset_zero_headers.pdf", bbox_inches="tight")
    plt.savefig(f"{out_dir}/cross_dataset_zero_headers.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved cross_dataset_zero_headers.pdf/png")


def plot_cross_dataset_top_headers(all_dfs: dict[str, pd.DataFrame], out_dir: str, top_n: int = 10):
    """
    Grouped bar for top-N headers by mean adoption across all datasets.
    One bar group per header, one bar per dataset.
    """
    # Determine top-N headers by overall mean across all datasets
    combined = pd.concat(list(all_dfs.values()), ignore_index=True)
    top_headers = combined[SECURITY_HEADERS].mean().sort_values(ascending=False).head(top_n).index.tolist()

    datasets = sorted(all_dfs.keys())
    x = range(len(top_headers))
    width = 0.8 / len(datasets)

    fig, ax = plt.subplots(figsize=(max(12, top_n * 0.9), 5))
    for i, cat in enumerate(datasets):
        df = all_dfs[cat]
        vals = [df[h].mean() * 100 for h in top_headers]
        offset = (i - len(datasets) / 2) * width + width / 2
        ax.bar([xi + offset for xi in x], vals, width=width * 0.9, label=cat, edgecolor="white")
    ax.set_xticks(list(x))
    ax.set_xticklabels([h.replace("-", "-\n") for h in top_headers], rotation=0, ha="center", fontsize=7)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax.set_ylabel("Adoption Rate (%)")
    ax.set_title(f"Top {top_n} Security Headers — Adoption Rate by Dataset")
    ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=8)
    plt.tight_layout()
    plt.savefig(f"{out_dir}/cross_dataset_top_headers.pdf", bbox_inches="tight")
    plt.savefig(f"{out_dir}/cross_dataset_top_headers.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved cross_dataset_top_headers.pdf/png")


def cross_dataset_adoption_to_latex(all_dfs: dict[str, pd.DataFrame]) -> str:
    """Table: headers × datasets, cell = adoption rate %."""
    datasets = sorted(all_dfs.keys())
    col_spec = "l" + "r" * len(datasets)
    header_row = " & ".join(["Header"] + [d.replace("_", r"\_") for d in datasets]) + r" \\"
    lines = [
        r"\begin{table}[ht]",
        r"\centering",
        r"\small",
        r"\begin{tabular}{" + col_spec + "}",
        r"\toprule",
        header_row,
        r"\midrule",
    ]
    # Sort headers by mean adoption descending
    combined = pd.concat(list(all_dfs.values()), ignore_index=True)
    sorted_headers = combined[SECURITY_HEADERS].mean().sort_values(ascending=False).index
    for h in sorted_headers:
        vals = [f"{all_dfs[d][h].mean()*100:.1f}\\%" for d in datasets]
        lines.append(h.replace("-", r"\mbox{-}") + " & " + " & ".join(vals) + r" \\")
    lines += [
        r"\bottomrule",
        r"\end{tabular}",
        r"\caption{Security header adoption rate by dataset (\% of \textit{etld} $\times$ \textit{location} pairs, aggregated across locations).}",
        r"\label{tab:cross_dataset_adoption}",
        r"\end{table}",
    ]
    return "\n".join(lines)


def cross_dataset_zero_to_latex(all_dfs: dict[str, pd.DataFrame]) -> str:
    """Table: dataset × location → zero-header %."""
    records = []
    for cat, df in all_dfs.items():
        df = df.copy()
        df["_count"] = df[SECURITY_HEADERS].sum(axis=1)
        for loc, grp in df.groupby("location"):
            records.append({
                "Dataset": cat,
                "Location": loc,
                "Total": len(grp),
                "Zero-header": int((grp["_count"] == 0).sum()),
                "\\%": f"{(grp['_count'] == 0).mean()*100:.1f}\\%",
            })
    plot_df = pd.DataFrame(records).sort_values(["Dataset", "Location"])
    lines = [
        r"\begin{table}[ht]",
        r"\centering",
        r"\small",
        r"\begin{tabular}{llrrr}",
        r"\toprule",
        r"Dataset & Location & Total & Zero-header sites & \% \\",
        r"\midrule",
    ]
    prev_cat = None
    for _, row in plot_df.iterrows():
        cat = row["Dataset"].replace("_", r"\_")
        if cat != prev_cat and prev_cat is not None:
            lines.append(r"\midrule")
        prev_cat = cat
        lines.append(f"{cat} & {row['Location']} & {row['Total']} & {row['Zero-header']} & {row['\\%']} \\\\")
    lines += [
        r"\bottomrule",
        r"\end{tabular}",
        r"\caption{Sites with zero security headers by dataset and location.}",
        r"\label{tab:cross_dataset_zero}",
        r"\end{table}",
    ]
    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    active_datasets = list(DATASETS)
    if INCLUDE_VPN:
        active_datasets.append(VPN_DATASET)

    # Filter to datasets that actually have data (skip categories not yet crawled)
    available = []
    all_deployment_dfs: dict[str, pd.DataFrame] = {}

    for category, db_config in active_datasets:
        out_dir = os.path.join(RESULTS_BASE, "header_deployment", category)
        os.makedirs(out_dir, exist_ok=True)

        print(f"\n{'='*60}")
        print(f"Dataset: {category}")
        print(f"{'='*60}")
        print(f"Fetching data...")
        raw = fetch_sessions(db_config, category)
        if raw.empty:
            print(f"  No data for {category} — skipping.")
            continue
        print(f"  {len(raw):,} HAR main-frame rows")

        print("Loading visit map for consistency tagging...")
        visit_map = load_visit_map(db_config, category)
        tagger    = ConsistencyTagger(visit_map)
        insuff    = (visit_map.groupby(["etld", "location"])["access_id"]
                              .nunique() < MIN_VISITS).sum()
        print(f"  {insuff} etld × location pairs have < {MIN_VISITS} visits "
              f"(excluded from Tier A)")

        print("Building deployment frame (etld × location)...")
        df = build_deployment_frame(raw, tagger)
        print(f"  {len(df):,} etld × location pairs  "
              f"(sufficient: {(~df['insufficient_visits']).sum():,})")

        df.to_csv(f"{out_dir}/deployment_by_etld_location.csv", index=False)
        print(f"  Saved deployment_by_etld_location.csv")

        # Produce adoption rates for both tiers
        adoption_df    = compute_adoption_rates(df)            # Tier A+B combined
        adoption_df_a  = compute_adoption_rates(df, TIER_A)   # Tier A only (stable)
        zero_df        = zero_header_rate(df)
        inc_summary    = compute_inconsistency_summary(df)

        print_summary(df, adoption_df, zero_df, inc_summary, category)

        print("Generating plots...")
        plot_adoption_rates(adoption_df,   out_dir)
        plot_adoption_rates(adoption_df_a, out_dir, suffix="_stable",
                            title_suffix=" — Tier A (Stable, all visits)")
        plot_codeployment_heatmap(df, out_dir)
        plot_header_count_distribution(df, out_dir)

        print("Writing LaTeX tables...")
        with open(f"{out_dir}/table_adoption_rates.tex", "w") as f:
            f.write(adoption_to_latex(adoption_df))
        with open(f"{out_dir}/table_adoption_rates_stable.tex", "w") as f:
            f.write(adoption_to_latex(adoption_df_a))
        with open(f"{out_dir}/table_zero_headers.tex", "w") as f:
            f.write(zero_header_to_latex(zero_df))
        print(f"  Saved table_adoption_rates.tex/stable, table_zero_headers.tex")

        adoption_df.to_csv(f"{out_dir}/adoption_rates.csv")
        adoption_df_a.to_csv(f"{out_dir}/adoption_rates_stable.csv")

        # Save tier distribution per header
        tier_cols = [f"{h}__tier" for h in SECURITY_HEADERS if f"{h}__tier" in df.columns]
        tier_summary = pd.DataFrame({
            h: df[f"{h}__tier"].value_counts()
            for h in SECURITY_HEADERS if f"{h}__tier" in df.columns
        }).T
        tier_summary.to_csv(f"{out_dir}/tier_distribution.csv")

        all_deployment_dfs[category] = df
        available.append(category)

    # Cross-dataset comparison (only when ≥2 datasets available)
    if len(all_deployment_dfs) >= 2:
        cross_dir = os.path.join(RESULTS_BASE, "header_deployment", "cross_dataset")
        os.makedirs(cross_dir, exist_ok=True)
        print(f"\n{'='*60}")
        print(f"Cross-dataset comparison ({', '.join(available)})")
        print(f"{'='*60}")
        print("Generating cross-dataset plots...")
        plot_cross_dataset_adoption(all_deployment_dfs, cross_dir)
        plot_cross_dataset_zero_headers(all_deployment_dfs, cross_dir)
        plot_cross_dataset_top_headers(all_deployment_dfs, cross_dir)
        print("Writing cross-dataset LaTeX tables...")
        with open(f"{cross_dir}/table_cross_dataset_adoption.tex", "w") as f:
            f.write(cross_dataset_adoption_to_latex(all_deployment_dfs))
        with open(f"{cross_dir}/table_cross_dataset_zero.tex", "w") as f:
            f.write(cross_dataset_zero_to_latex(all_deployment_dfs))
        print(f"  Saved table_cross_dataset_adoption.tex, table_cross_dataset_zero.tex")
        print(f"\nCross-dataset outputs in: {cross_dir}")
    else:
        print(f"\nOnly one dataset available ({available[0] if available else 'none'}) — skipping cross-dataset comparison.")

    print(f"\nDone. Per-dataset outputs in: {RESULTS_BASE}/header_deployment/<category>/")


if __name__ == "__main__":
    main()
