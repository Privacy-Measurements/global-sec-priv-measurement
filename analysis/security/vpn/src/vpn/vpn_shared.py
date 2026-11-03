"""
Shared utilities for VPN comparative analysis.

Core concept: for each country pair (e.g. INDIA / INDIA-VPN), we work only
on the intersection of eTLDs present in both normal and VPN crawls.
All comparison functions produce per-eTLD deltas and aggregate statistics.
"""

import sys
import os
_HERE = os.path.abspath(os.path.dirname(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "../.."))
for _p in [_HERE, _ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from src.vpn.config import (
    DB_CONFIG_VPN, VPN_LOCATION_PAIRS,
    HEADER_PAIRS, SESSION_PAIRS, CATEGORY,
)

import json
import traceback
import warnings
from collections import defaultdict

import pymysql
import pandas as pd
import numpy as np
from scipy.spatial.distance import jensenshannon
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns


# ── Intersection helpers ──────────────────────────────────────────────────────

def build_session_intersections(db_config: dict = DB_CONFIG_VPN) -> dict[tuple, set[str]]:
    """
    For tables joining via crawl_sessions (scripts, js_calls, html_elements,
    iframes): return shared eTLDs between normal and VPN visits.

    Uses SESSION_PAIRS — crawl_sessions.location distinguishes normal vs VPN
    via the -VPN suffix (e.g. INDIA vs INDIA-VPN).
    5 countries: INDIA, GERMANY, UAE, USA, ALGERIA.
    """
    conn = pymysql.connect(**db_config)
    intersections = {}
    for normal_loc, vpn_loc in SESSION_PAIRS:
        norm_df = pd.read_sql(
            "SELECT DISTINCT etld FROM crawl_sessions "
            "WHERE category = %s AND location = %s",
            conn, params=(CATEGORY, normal_loc)
        )
        vpn_df = pd.read_sql(
            "SELECT DISTINCT etld FROM crawl_sessions "
            "WHERE category = %s AND location = %s",
            conn, params=(CATEGORY, vpn_loc)
        )
        shared = set(norm_df["etld"]) & set(vpn_df["etld"])
        intersections[(normal_loc, vpn_loc)] = shared
        print(f"  {normal_loc} ↔ {vpn_loc}: "
              f"{len(norm_df)} normal, {len(vpn_df)} VPN, "
              f"{len(shared)} shared eTLDs")
    conn.close()
    return intersections


# ── Global intersection (single denominator across all country pairs) ─────────

def build_global_intersection(
        db_config: dict = DB_CONFIG_VPN,
        pairs: list[tuple] | None = None,
        source: str = "session",
) -> set[str]:
    """
    Return the set of eTLDs present in ALL location pairs simultaneously.
    This gives a single common denominator for cross-country comparison,
    ensuring adoption rates are directly comparable across countries.

    source: "session" uses crawl_sessions (SESSION_PAIRS, 5 countries)
            "header"  uses header_data_new/vpn (HEADER_PAIRS, 4 countries)
    pairs:  override the default pair list if provided
    """
    if pairs is None:
        pairs = HEADER_PAIRS if source == "header" else SESSION_PAIRS

    conn = pymysql.connect(**db_config)
    global_set: set[str] | None = None

    for normal_loc, vpn_loc in pairs:
        if source == "header":
            norm_etlds = set(pd.read_sql(
                "SELECT DISTINCT etld FROM header_data_new "
                "WHERE category = %s AND country = %s",
                conn, params=(CATEGORY, normal_loc)
            )["etld"])
            vpn_etlds = set(pd.read_sql(
                "SELECT DISTINCT etld FROM header_data_vpn "
                "WHERE category = %s AND country = %s",
                conn, params=(CATEGORY, vpn_loc)
            )["etld"])
        else:
            norm_etlds = set(pd.read_sql(
                "SELECT DISTINCT etld FROM crawl_sessions "
                "WHERE category = %s AND location = %s",
                conn, params=(CATEGORY, normal_loc)
            )["etld"])
            vpn_etlds = set(pd.read_sql(
                "SELECT DISTINCT etld FROM crawl_sessions "
                "WHERE category = %s AND location = %s",
                conn, params=(CATEGORY, vpn_loc)
            )["etld"])

        pair_shared = norm_etlds & vpn_etlds
        global_set = pair_shared if global_set is None else (global_set & pair_shared)

    conn.close()
    global_set = global_set or set()
    print(f"  Global intersection ({source}, {len(pairs)} pairs): "
          f"{len(global_set)} eTLDs")
    return global_set


# ── Per-eTLD outlier analysis ─────────────────────────────────────────────────

def compute_etld_outliers(
        etld_data: dict[str, dict[str, pd.DataFrame]],
        api_categories: list[str] | None = None,
        jsd_threshold: float = 0.2,
        count_ratio_threshold: float = 2.0,
) -> pd.DataFrame:
    """
    For each eTLD in the global intersection, identify anomalous behaviour
    across location pairs.

    Parameters
    ----------
    etld_data : {country_label: {"normal": df, "vpn": df}}
        Each df has columns: etld, call_method, api_category, call_count.
        Built from the aggregated JS call DataFrames.
    api_categories : list of category names to include in JSD vector
    jsd_threshold : JSD ≥ this value flags structural behavioural difference
    count_ratio_threshold : VPN/normal call count ratio ≥ this flags volume anomaly

    Returns
    -------
    DataFrame with columns:
        etld, country, normal_calls, vpn_calls, count_ratio,
        jsd, is_count_outlier, is_jsd_outlier, is_outlier
    Sorted by JSD descending.
    """
    from src.js_calls.config import API_CATEGORIES as _API_CATS
    if api_categories is None:
        api_categories = list(_API_CATS.keys())

    rows = []
    for country, sides in etld_data.items():
        norm_df = sides.get("normal", pd.DataFrame())
        vpn_df  = sides.get("vpn",    pd.DataFrame())
        if norm_df.empty and vpn_df.empty:
            continue

        # All eTLDs in this country pair
        etlds = set()
        if not norm_df.empty and "etld" in norm_df.columns:
            etlds |= set(norm_df["etld"].unique())
        if not vpn_df.empty and "etld" in vpn_df.columns:
            etlds |= set(vpn_df["etld"].unique())

        for etld in etlds:
            # Total call counts
            n_calls = norm_df[norm_df["etld"] == etld]["call_count"].sum() \
                      if not norm_df.empty else 0
            v_calls = vpn_df[vpn_df["etld"] == etld]["call_count"].sum() \
                      if not vpn_df.empty else 0

            # Count ratio (VPN / normal) — treat zero-normal as infinity
            if n_calls == 0 and v_calls == 0:
                continue
            ratio = (v_calls / n_calls) if n_calls > 0 else float("inf")

            # Category distribution vectors
            def _cat_vec(df, etld):
                sub = df[df["etld"] == etld] if not df.empty else pd.DataFrame()
                vec = np.zeros(len(api_categories))
                if sub.empty:
                    return vec
                for i, cat in enumerate(api_categories):
                    vec[i] = sub[sub["api_category"] == cat]["call_count"].sum()
                total = vec.sum()
                return vec / total if total > 0 else vec

            n_vec = _cat_vec(norm_df, etld)
            v_vec = _cat_vec(vpn_df, etld)

            # JSD between normal and VPN category distributions
            # Only meaningful if both have calls
            if n_calls > 0 and v_calls > 0:
                jsd = float(jensenshannon(n_vec, v_vec) ** 2)
            else:
                jsd = 1.0  # one side is zero — maximally different

            is_count  = (ratio >= count_ratio_threshold or
                         (n_calls > 0 and v_calls == 0) or
                         ratio <= 1 / count_ratio_threshold)
            is_jsd    = jsd >= jsd_threshold

            rows.append({
                "etld":             etld,
                "country":          country,
                "normal_calls":     int(n_calls),
                "vpn_calls":        int(v_calls),
                "count_ratio":      round(ratio, 3) if ratio != float("inf") else None,
                "jsd":              round(jsd, 4),
                "is_count_outlier": is_count,
                "is_jsd_outlier":   is_jsd,
                "is_outlier":       is_count or is_jsd,
            })

    if not rows:
        return pd.DataFrame()
    return (pd.DataFrame(rows)
              .sort_values("jsd", ascending=False)
              .reset_index(drop=True))


def plot_outlier_scatter(
        outlier_df: pd.DataFrame,
        country: str,
        out_dir: str,
        top_n: int = 20,
):
    """
    Log-scale scatter: x = normal call count, y = VPN call count.
    Log axes compress the wide dynamic range. Diagonal = no change.
    Outliers highlighted; top JSD outliers annotated by eTLD.
    Background hexbin density for non-outlier mass.
    """
    if outlier_df.empty:
        return

    sub = outlier_df[outlier_df["country"] == country].copy()
    sub = sub[sub["count_ratio"].notna() &
              (sub["normal_calls"] > 0) & (sub["vpn_calls"] > 0)]
    if sub.empty:
        return

    fig, ax = plt.subplots(figsize=(8, 7))

    # Background hexbin of all points (density)
    hb = ax.hexbin(
        np.log10(sub["normal_calls"] + 1),
        np.log10(sub["vpn_calls"] + 1),
        gridsize=30, cmap="Blues", mincnt=1, alpha=0.4,
    )
    plt.colorbar(hb, ax=ax, label="Site density")

    # Highlight JSD outliers on top
    jsd_out = sub[sub["is_jsd_outlier"]]
    ax.scatter(
        np.log10(jsd_out["normal_calls"] + 1),
        np.log10(jsd_out["vpn_calls"] + 1),
        s=50, color="tomato", zorder=5, label=f"JSD outlier (n={len(jsd_out)})",
    )
    # Count-only outliers
    cnt_out = sub[sub["is_count_outlier"] & ~sub["is_jsd_outlier"]]
    if not cnt_out.empty:
        ax.scatter(
            np.log10(cnt_out["normal_calls"] + 1),
            np.log10(cnt_out["vpn_calls"] + 1),
            s=30, color="sandybrown", zorder=4,
            label=f"Count outlier only (n={len(cnt_out)})",
        )

    # Diagonal (no change line) in log space
    lim = max(
        np.log10(sub["normal_calls"].max() + 1),
        np.log10(sub["vpn_calls"].max() + 1),
    ) * 1.05
    ax.plot([0, lim], [0, lim], color="grey", linestyle="--",
            linewidth=0.8, label="No change")

    # Annotate top JSD outliers only (keep readable)
    for _, r in jsd_out.nlargest(min(top_n, len(jsd_out)), "jsd").iterrows():
        ax.annotate(
            r["etld"].replace("www.", ""),
            (np.log10(r["normal_calls"] + 1), np.log10(r["vpn_calls"] + 1)),
            fontsize=5, alpha=0.85, color="darkred",
            xytext=(4, 2), textcoords="offset points",
        )

    ax.set_xlabel("log₁₀(Normal JS calls + 1)")
    ax.set_ylabel("log₁₀(VPN JS calls + 1)")
    ax.set_title(
        f"JS Call Volume: Normal vs VPN — {country}\n"
        f"({len(jsd_out)} JSD outliers, {len(cnt_out)} count outliers "
        f"of {len(sub)} eTLDs)"
    )
    ax.legend(fontsize=8)
    plt.tight_layout()
    save_fig(f"{out_dir}/outlier_scatter_{country.lower()}")


def plot_outlier_ranked(
        outlier_df: pd.DataFrame,
        out_dir: str,
        top_n: int = 20,
):
    """
    Ranked dot plot: top-N outlier eTLDs by JSD, one panel per country.
    Each row = one eTLD. Left dot = normal calls, right dot = VPN calls
    on a log scale, connected by a line coloured by direction of change.
    More interpretable than a scatter for identifying specific sites.
    """
    if outlier_df.empty:
        return

    countries = sorted(outlier_df["country"].unique())
    n_cols = min(len(countries), 3)
    n_rows = (len(countries) + n_cols - 1) // n_cols
    fig, axes = plt.subplots(n_rows, n_cols,
                             figsize=(7 * n_cols, max(6, top_n * 0.35) * n_rows),
                             squeeze=False)

    for idx, country in enumerate(countries):
        ax = axes[idx // n_cols][idx % n_cols]
        sub = outlier_df[
            (outlier_df["country"] == country) &
            outlier_df["is_outlier"] &
            outlier_df["count_ratio"].notna() &
            (outlier_df["normal_calls"] > 0) &
            (outlier_df["vpn_calls"] > 0)
        ].nlargest(top_n, "jsd")

        if sub.empty:
            ax.set_visible(False)
            continue

        y = range(len(sub))
        norm_log = np.log10(sub["normal_calls"].values + 1)
        vpn_log  = np.log10(sub["vpn_calls"].values + 1)

        for i, (n_v, v_v) in enumerate(zip(norm_log, vpn_log)):
            color = "tomato" if v_v > n_v else "steelblue"
            ax.plot([n_v, v_v], [i, i], color=color, linewidth=1.5, alpha=0.7)
        ax.scatter(norm_log, list(y), color="steelblue", s=30,
                   zorder=5, label="Normal")
        ax.scatter(vpn_log,  list(y), color="tomato",    s=30,
                   zorder=5, label="VPN")

        ax.set_yticks(list(y))
        ax.set_yticklabels(
            [r["etld"].replace("www.", "")[:35] for _, r in sub.iterrows()],
            fontsize=6,
        )
        ax.set_xlabel("log₁₀(calls + 1)")
        ax.set_title(f"{country} — top {len(sub)} by JSD")
        if idx == 0:
            ax.legend(fontsize=7, loc="lower right")

    # Hide unused panels
    for idx in range(len(countries), n_rows * n_cols):
        axes[idx // n_cols][idx % n_cols].set_visible(False)

    fig.suptitle(
        "JS Call Volume Change: Normal vs VPN\n"
        "(red line = more over VPN, blue = fewer; ranked by JSD)",
        y=1.01,
    )
    plt.tight_layout()
    save_fig(f"{out_dir}/outlier_ranked_dotplot")


def plot_outlier_jsd_distribution(
        outlier_df: pd.DataFrame,
        out_dir: str,
        jsd_threshold: float = 0.2,
):
    """
    Box plot: JSD distribution per country, with threshold line.
    Shows how much JS call behaviour differs between normal and VPN across sites.
    """
    if outlier_df.empty:
        return
    import matplotlib.pyplot as plt

    countries = sorted(outlier_df["country"].unique())
    data = [outlier_df[outlier_df["country"] == c]["jsd"].dropna().values
            for c in countries]
    fig, ax = plt.subplots(figsize=(max(6, len(countries) * 1.2), 5))
    ax.boxplot(data, labels=countries, patch_artist=True,
               boxprops=dict(facecolor="steelblue", alpha=0.5))
    ax.axhline(jsd_threshold, color="tomato", linestyle="--",
               linewidth=0.8, label=f"JSD threshold ({jsd_threshold})")
    ax.set_ylabel("Jensen-Shannon Divergence (category distribution)")
    ax.set_title("Per-site JS Call Category Divergence: Normal vs VPN")
    ax.legend(fontsize=8)
    ax.tick_params(axis="x", rotation=20)
    plt.tight_layout()
    save_fig(f"{out_dir}/outlier_jsd_distribution")


def outliers_to_latex(
        outlier_df: pd.DataFrame,
        top_n: int = 20,
) -> str:
    """LaTeX table of top outlier eTLDs ranked by JSD."""
    if outlier_df.empty:
        return ""
    top = outlier_df[outlier_df["is_outlier"]].head(top_n)
    if top.empty:
        return ""
    lines = [
        r"\begin{table}[ht]", r"\centering", r"\small",
        r"\begin{tabular}{llrrrcc}", r"\toprule",
        r"eTLD & Country & Normal calls & VPN calls & Ratio & JSD & Type \\",
        r"\midrule",
    ]
    for _, row in top.iterrows():
        ratio_str = f"{row['count_ratio']:.2f}" if row["count_ratio"] else "—"
        flags = []
        if row["is_count_outlier"]: flags.append("count")
        if row["is_jsd_outlier"]:   flags.append("JSD")
        lines.append(
            f"\\texttt{{{row['etld'].replace('_', r'\_')}}} & "
            f"{row['country']} & "
            f"{row['normal_calls']:,} & {row['vpn_calls']:,} & "
            f"{ratio_str} & {row['jsd']:.3f} & "
            f"{', '.join(flags)} \\\\"
        )
    lines += [
        r"\bottomrule", r"\end{tabular}",
        r"\caption{Top outlier eTLDs by JS call behavioural divergence "
        r"(JSD) between normal and VPN access.}",
        r"\label{tab:vpn_js_outliers}", r"\end{table}",
    ]
    return "\n".join(lines)


# ── Shared thresholds (used across utils, js_call_analysis, attribution) ─────

JSD_THRESHOLD         = 0.2   # adjusted JSD ≥ this → true VPN outlier
COUNT_RATIO_THRESHOLD = 2.0   # VPN/normal call count ratio ≥ this → count outlier


# ── Shared plot helpers ───────────────────────────────────────────────────────

# Human-readable label for the normal side of each pair
COUNTRY_LABEL = {
    "INDIA":   "India",
    "GERMANY": "Germany",
    "UAE":     "UAE",
    "USA":     "USA",
    "ALGERIA": "Algeria",
}


def save_fig(path_base: str):
    plt.savefig(f"{path_base}.pdf", bbox_inches="tight")
    plt.savefig(f"{path_base}.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved {os.path.basename(path_base)}.pdf/png")


def plot_delta_heatmap(
        all_comparison: dict[str, pd.DataFrame],
        metric_col: str,
        title: str,
        out_path: str,
        fmt: str = ".1f",
        cmap: str = "RdYlGn",
        center: float = 0,
):
    """
    Heatmap: rows = headers, cols = country pairs, cell = metric_col value.
    all_comparison: {country_label: adoption_rate_comparison_df}
    """
    countries = list(all_comparison.keys())
    if not countries:
        return
    headers = all_comparison[countries[0]]["header"].tolist()
    heat = pd.DataFrame(index=headers, columns=countries, data=0.0)
    for country, comp_df in all_comparison.items():
        for _, row in comp_df.iterrows():
            heat.loc[row["header"], country] = row[metric_col]
    heat = heat.astype(float)
    # Sort by mean absolute value
    heat = heat.loc[heat.abs().mean(axis=1).sort_values(ascending=False).index]

    fig, ax = plt.subplots(figsize=(max(5, len(countries) * 1.8),
                                    max(8, len(heat) * 0.45)))
    sns.heatmap(heat, ax=ax, annot=True, fmt=fmt, cmap=cmap,
                center=center, linewidths=0.4,
                cbar_kws={"label": metric_col})
    ax.set_title(title)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=30, ha="right", fontsize=9)
    ax.set_yticklabels(ax.get_yticklabels(), rotation=0, fontsize=8)
    plt.tight_layout()
    save_fig(out_path)


def _pct(n, d):
    return f"{n/d*100:.1f}\\%" if d else "---"


# ── Baseline geographic variance correction ───────────────────────────────────

def subtract_baseline_variance(
        outlier_df: pd.DataFrame,
        baseline_csv: str | None = None,
) -> pd.DataFrame:
    """
    Correct VPN outlier JSD scores for natural geographic baseline variance
    at the eTLD × country level (not just per eTLD globally).

    For each (eTLD, country) VPN pair, the relevant baseline is the max JSD
    observed for that eTLD in any normal-crawl location pair that involves
    the same country. This avoids over-subtracting: a site that varies
    naturally between INDIA and GERMANY should not penalise the INDIA-UAE
    VPN comparison.

    Formula:
        baseline_jsd(etld, country) =
            max JSD of any normal-crawl pair (location_a, location_b)
            for this eTLD where location_a == country OR location_b == country

        adjusted_jsd = max(0, VPN_JSD − baseline_jsd)

    Falls back to global max per eTLD when no country-specific baseline exists.

    Parameters
    ----------
    outlier_df  : DataFrame from js_call_outliers.csv; must have 'etld', 'country',
                  'jsd', 'is_outlier' columns.
    baseline_csv: path to results/location_outliers/global/location_jsd_outliers.csv
                  (output of the project's location_outlier_analysis.py — not
                  included in this bundle). If None or missing, returns
                  outlier_df unchanged with a warning.

    Returns
    -------
    outlier_df with added columns:
        baseline_jsd     — country-specific baseline JSD for this eTLD
        adjusted_jsd     — max(0, jsd − baseline_jsd)
        is_true_outlier  — adjusted_jsd ≥ JSD_THRESHOLD
    """
    if baseline_csv is None or not os.path.exists(baseline_csv or ""):
        print("  WARNING: baseline_csv not available — "
              "VPN JSD scores are NOT corrected for geographic baseline variance. "
              "Results should be interpreted as upper bounds on VPN-specific effects.")
        result = outlier_df.copy()
        result["baseline_jsd"]    = None
        result["adjusted_jsd"]    = result["jsd"]
        result["is_true_outlier"] = result["is_outlier"]
        return result

    baseline = pd.read_csv(baseline_csv)

    # Build: (etld, country) → max baseline JSD involving that country
    # An eTLD's baseline for country C = max JSD across all pairs where
    # location_a == C or location_b == C in the normal crawl.
    rows = []
    for _, r in baseline.iterrows():
        for loc in (r["location_a"], r["location_b"]):
            rows.append({
                "etld":    r["etld"],
                "country": loc,          # country label in normal crawl
                "jsd":     r["jsd"],
            })
    if not rows:
        # Baseline file exists but is empty — treat as unavailable
        print("  WARNING: baseline file is empty — no correction applied.")
        result = outlier_df.copy()
        result["baseline_jsd"]    = 0.0
        result["adjusted_jsd"]    = result["jsd"]
        result["is_true_outlier"] = result["is_outlier"]
        return result

    baseline_long = pd.DataFrame(rows)

    # Max baseline JSD per (etld, country)
    baseline_by_country = (
        baseline_long
        .groupby(["etld", "country"])["jsd"]
        .max()
        .reset_index(name="baseline_jsd")
    )

    # The outlier_df uses COUNTRY_LABEL values (e.g. "India") while the
    # baseline uses crawl_sessions.location values (e.g. "INDIA").
    # Map INDIA → India etc., then keep only (etld, country_label, baseline_jsd).
    baseline_lookup = (
        baseline_by_country
        .assign(country=baseline_by_country["country"].map(
            lambda c: COUNTRY_LABEL.get(c, c)   # "INDIA" → "India"
        ))
        [["etld", "country", "baseline_jsd"]]
        .drop_duplicates()
    )

    # Merge on (etld, country) — left join adds baseline_jsd column (NaN where no match)
    result = outlier_df.merge(baseline_lookup, on=["etld", "country"], how="left")

    # After merge, baseline_jsd may not exist if suffixes were applied due to
    # a pre-existing baseline_jsd column in outlier_df. Resolve that case.
    if "baseline_jsd" not in result.columns:
        if "baseline_jsd_y" in result.columns:
            result = result.rename(columns={"baseline_jsd_y": "baseline_jsd"})
            result.drop(columns=[c for c in result.columns if c == "baseline_jsd_x"],
                        inplace=True, errors="ignore")
        else:
            result["baseline_jsd"] = 0.0

    # Fallback: eTLDs with no country-specific baseline → global max per eTLD
    global_max = (
        baseline_long.groupby("etld")["jsd"].max()
        .reset_index(name="baseline_jsd_global")
    )
    result = result.merge(global_max, on="etld", how="left")
    result["baseline_jsd"] = result["baseline_jsd"].fillna(
        result["baseline_jsd_global"]
    ).fillna(0.0)
    result.drop(columns=["baseline_jsd_global"], inplace=True, errors="ignore")

    result["adjusted_jsd"]    = (result["jsd"] - result["baseline_jsd"]).clip(lower=0)
    result["is_true_outlier"] = result["adjusted_jsd"] >= JSD_THRESHOLD

    n_raw  = int(result["is_outlier"].sum())
    n_true = int(result["is_true_outlier"].sum())
    n_country_specific = int(
        result[result["baseline_jsd"] > 0]["etld"].nunique()
    )
    print(f"  Baseline subtraction (eTLD × country level):")
    print(f"    {n_raw} raw outliers → {n_true} true VPN outliers")
    print(f"    Country-specific baseline applied to {n_country_specific} eTLDs; "
          f"global fallback for the rest")
    return result
