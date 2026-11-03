"""
VPN JS Call Analysis
=====================
Compares security/privacy-critical JS API call distribution between
normal and VPN access. Uses SSCursor streaming to avoid sort-buffer OOM.

Covers:
  1. Overall FP/TP call rate delta per country pair
  2. Per API-category adoption delta (fingerprinting, eval, defensive, etc.)
  3. Deviation in security/privacy-critical calls (fingerprinting, eval)
     between normal and VPN — key signal for VPN-triggered tracking changes
  4. Cross-country: systematic vs country-specific API call changes
"""

import sys
import os
_HERE = os.path.abspath(os.path.dirname(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "../.."))
for _p in [_HERE, _ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from src.vpn.config import (
    DB_CONFIG_VPN, VPN_LOCATION_PAIRS, CATEGORY, VPN_RESULTS_BASE,
)
from src.vpn.vpn_shared import (
    build_session_intersections, build_global_intersection,
    compute_etld_outliers, plot_outlier_scatter, plot_outlier_ranked,
    plot_outlier_jsd_distribution, outliers_to_latex,
    COUNTRY_LABEL, save_fig, _pct, plot_delta_heatmap,
    JSD_THRESHOLD, COUNT_RATIO_THRESHOLD, subtract_baseline_variance,
)
from src.js_calls.config import (
    API_CATEGORIES, THIRD_PARTY_ONLY_CATEGORIES,
)

import json
import traceback
from collections import defaultdict

import pymysql
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns

OUTPUT_DIR = VPN_RESULTS_BASE
FORCE_RECOMPUTE = False  # set True to bypass cache and re-run DB queries

from src.vpn.cache import results_exist, load_country_results, JS_FILES, load_csv


def _safe_round(v, n: int = 4):
    """Round v to n decimal places, returning None for None/NaN."""
    try:
        return round(float(v), n) if v is not None else None
    except (TypeError, ValueError):
        return None

# ── Build method lookup (reuse from js_call_analysis) ────────────────────────

_METHOD_TO_CAT: dict[str, str] = {}
for _cat, _methods in API_CATEGORIES.items():
    for _m in _methods:
        _METHOD_TO_CAT[_m.lower()] = _cat

def classify_method(cm: str) -> str | None:
    if not cm:
        return None
    cm = cm.lower()
    if cm in _METHOD_TO_CAT:
        return _METHOD_TO_CAT[cm]
    for prefix, cat in _METHOD_TO_CAT.items():
        if cm.startswith(prefix):
            return cat
    return None

_TAXONOMY_METHODS = list({m for methods in API_CATEGORIES.values() for m in methods})


# ── DB fetch (SSCursor streaming) ─────────────────────────────────────────────

def fetch_js_calls_pair(
        db_config: dict,
        normal_loc: str,
        vpn_loc: str,
        etlds: set[str],
        chunk_size: int = 200_000,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Stream js_calls for both locations, aggregate in Python.
    Returns (normal_agg_df, vpn_agg_df) each with columns:
        etld, location, call_method, api_category, etld_caller_url_rel,
        call_count (weighted by occurrences).
    """
    if not etlds:
        return pd.DataFrame(), pd.DataFrame()

    ph_methods = ", ".join(["%s"] * len(_TAXONOMY_METHODS))
    prefixes   = [f"{p}.%" for p in {m.split(".")[0] for m in _TAXONOMY_METHODS}]
    ph_prefix  = " OR ".join("jc.call_method LIKE %s" for _ in prefixes)
    ph_etlds   = ", ".join(["%s"] * len(etlds))

    query = f"""
        SELECT
            cs.etld,
            cs.location,
            jc.call_method,
            COALESCE(jc.etld_caller_url_rel, 'unknown') AS etld_caller_url_rel,
            jc.context_origin
        FROM crawl_sessions cs
        JOIN js_calls jc ON jc.session_id = cs.id
        WHERE cs.category = %s
          AND cs.etld_url_rel != 'cross-site'
          AND cs.location IN (%s, %s)
          AND cs.etld IN ({ph_etlds})
          AND jc.call_method IS NOT NULL
          AND (
              jc.call_method IN ({ph_methods})
              OR {ph_prefix}
          )
        ORDER BY cs.location, cs.etld
    """
    params = (
        [CATEGORY, normal_loc, vpn_loc] +
        list(etlds) + _TAXONOMY_METHODS + prefixes
    )

    AGG_KEY = ("etld", "location", "call_method", "etld_caller_url_rel")
    counts: dict[tuple, int] = defaultdict(int)

    conn = pymysql.connect(**db_config)
    try:
        cur = conn.cursor(pymysql.cursors.SSCursor)
        cur.execute(query, params)
        cols    = [d[0] for d in cur.description]
        col_idx = {c: i for i, c in enumerate(cols)}
        total   = 0
        while True:
            rows = cur.fetchmany(chunk_size)
            if not rows:
                break
            total += len(rows)
            for row in rows:
                key = tuple(row[col_idx[c]] for c in AGG_KEY)
                counts[key] += 1
            if total % 1_000_000 == 0:
                print(f"    streamed {total:,} rows...")
        cur.close()
    finally:
        conn.close()

    if not counts:
        return pd.DataFrame(), pd.DataFrame()

    records = []
    for key, cnt in counts.items():
        row = dict(zip(AGG_KEY, key))
        row["call_count"]   = cnt
        row["api_category"] = classify_method(row["call_method"])
        records.append(row)

    df = pd.DataFrame(records)
    return (
        df[df["location"] == normal_loc].copy(),
        df[df["location"] == vpn_loc].copy(),
    )


# ── Analysis ──────────────────────────────────────────────────────────────────

def compute_category_adoption(df: pd.DataFrame, etlds: set[str]) -> pd.DataFrame:
    """
    Per API category: % of eTLDs making ≥1 call, split by party.
    Respects THIRD_PARTY_ONLY_CATEGORIES context rule.
    """
    classified = df[df["api_category"].notna()].copy()
    n_etlds    = len(etlds)
    rows = []
    for (cat, party), grp in classified.groupby(["api_category", "etld_caller_url_rel"]):
        if cat in THIRD_PARTY_ONLY_CATEGORIES and party != "cross-site":
            continue
        adopters = grp["etld"].nunique()
        rows.append({
            "api_category":  cat,
            "party":         party,
            "etld_adopters": adopters,
            "adoption_rate": adopters / n_etlds,
            "total_calls":   grp["call_count"].sum(),
        })
    return pd.DataFrame(rows)


def compute_adoption_delta(
        norm_adop: pd.DataFrame,
        vpn_adop:  pd.DataFrame,
) -> pd.DataFrame:
    """
    Per (api_category, party): adoption rate delta VPN − normal.
    """
    if norm_adop.empty and vpn_adop.empty:
        return pd.DataFrame()
    merged = pd.merge(
        norm_adop.rename(columns={"adoption_rate": "normal_rate",
                                   "etld_adopters": "normal_adopters"}),
        vpn_adop.rename(columns={"adoption_rate": "vpn_rate",
                                  "etld_adopters": "vpn_adopters"}),
        on=["api_category", "party"], how="outer",
    ).fillna(0)
    merged["delta_pp"] = (merged["vpn_rate"] - merged["normal_rate"]) * 100
    return merged.sort_values("delta_pp")


# ── Plots ─────────────────────────────────────────────────────────────────────

def plot_category_delta_heatmap(
        all_delta: dict[str, pd.DataFrame],
        party: str,
        out_dir: str,
):
    """
    Heatmap: API category × country, cell = adoption rate delta (pp).
    Positive = more prevalent over VPN, negative = less.
    """
    categories = list(API_CATEGORIES.keys())
    countries  = list(all_delta.keys())
    heat = pd.DataFrame(index=categories, columns=countries, data=0.0)
    for c, delta_df in all_delta.items():
        sub = delta_df[delta_df["party"] == party]
        for _, row in sub.iterrows():
            if row["api_category"] in categories:
                heat.loc[row["api_category"], c] = row["delta_pp"]
    heat = heat.astype(float)
    heat = heat.loc[heat.abs().max(axis=1).sort_values(ascending=False).index]
    # Drop all-zero rows
    heat = heat.loc[(heat.abs() > 0.1).any(axis=1)]
    if heat.empty:
        return

    fig, ax = plt.subplots(figsize=(max(5, len(countries) * 1.8),
                                    max(5, len(heat) * 0.5)))
    sns.heatmap(heat, ax=ax, annot=True, fmt=".1f",
                cmap="RdYlGn", center=0, linewidths=0.4,
                cbar_kws={"label": "Adoption rate delta (pp)"})
    ax.set_title(f"JS API Category Adoption Delta: Normal → VPN "
                 f"({'3rd-party' if party == 'cross-site' else '1st-party'})")
    ax.set_xticklabels(ax.get_xticklabels(), rotation=30, ha="right", fontsize=9)
    ax.set_yticklabels(ax.get_yticklabels(), rotation=0, fontsize=8)
    plt.tight_layout()
    suffix = "tp" if party == "cross-site" else "fp"
    save_fig(f"{out_dir}/js_category_delta_{suffix}")


def plot_fp_tp_call_rate_delta(
        all_norm: dict[str, pd.DataFrame],
        all_vpn:  dict[str, pd.DataFrame],
        etlds_map: dict[str, int],
        out_dir: str,
):
    """Grouped bar: overall TP call share normal vs VPN per country."""
    countries = list(all_norm.keys())
    x = range(len(countries))
    width = 0.35

    def _tp_rate(df):
        total = df["call_count"].sum()
        tp    = df[df["etld_caller_url_rel"] == "cross-site"]["call_count"].sum()
        return tp / total * 100 if total else 0

    norm_rates = [_tp_rate(all_norm[c]) for c in countries]
    vpn_rates  = [_tp_rate(all_vpn[c])  for c in countries]

    fig, ax = plt.subplots(figsize=(max(6, len(countries) * 1.5), 5))
    ax.bar([xi - width/2 for xi in x], norm_rates, width,
           label="Normal", color="steelblue", edgecolor="white")
    ax.bar([xi + width/2 for xi in x], vpn_rates,  width,
           label="VPN",    color="tomato",    edgecolor="white")
    ax.set_xticks(list(x))
    ax.set_xticklabels(countries, rotation=20, ha="right")
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax.set_ylabel("% of JS calls from 3rd-party scripts")
    ax.set_title("Third-party JS Call Share: Normal vs VPN")
    ax.legend()
    plt.tight_layout()
    save_fig(f"{out_dir}/js_fp_tp_delta")


# ── LaTeX ─────────────────────────────────────────────────────────────────────

def category_delta_to_latex(all_delta: dict[str, pd.DataFrame]) -> str:
    countries = list(all_delta.keys())
    categories = list(API_CATEGORIES.keys())
    col_spec = "l" + "r" * len(countries)
    header = " & ".join(["API Category"] + countries) + r" \\"
    lines = [
        r"\begin{table}[ht]", r"\centering", r"\small",
        r"\begin{tabular}{" + col_spec + "}", r"\toprule",
        header, r"\midrule",
        r"\multicolumn{" + str(1+len(countries)) + r"}{l}"
        r"{\textit{3rd-party calls only}} \\",
    ]
    for cat in categories:
        vals = []
        for c in countries:
            delta_df = all_delta[c]
            sub = delta_df[
                (delta_df["api_category"] == cat) &
                (delta_df["party"] == "cross-site")
            ]
            if sub.empty:
                vals.append("---")
            else:
                d = sub.iloc[0]["delta_pp"]
                sign = "+" if d > 0 else ""
                vals.append(f"{sign}{d:.1f}pp")
        lines.append(
            cat.replace("_", r"\_") + " & " + " & ".join(vals) + r" \\"
        )
    lines += [
        r"\bottomrule", r"\end{tabular}",
        r"\caption{JS API category adoption rate delta (VPN $-$ Normal, pp).}",
        r"\label{tab:vpn_js_delta}", r"\end{table}",
    ]
    return "\n".join(lines)


# ── Console summary ───────────────────────────────────────────────────────────

def print_summary(
        normal_loc: str, vpn_loc: str,
        delta_df: pd.DataFrame,
):
    print(f"\n{'='*70}")
    print(f"JS CALL ANALYSIS: {normal_loc} ↔ {vpn_loc}")
    print(f"{'='*70}")
    if delta_df.empty:
        print("  No data.")
        return
    tp = delta_df[delta_df["party"] == "cross-site"].sort_values(
        "delta_pp", key=abs, ascending=False)
    print("  Top API category changes (3rd-party, by |Δpp|):")
    for _, row in tp.head(8).iterrows():
        sign = "+" if row["delta_pp"] > 0 else ""
        print(f"    {row['api_category']:<30} "
              f"normal={row['normal_rate']*100:.1f}%  "
              f"vpn={row['vpn_rate']*100:.1f}%  "
              f"Δ={sign}{row['delta_pp']:.1f}pp")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    global OUTPUT_DIR
    os.makedirs(VPN_RESULTS_BASE, exist_ok=True)
    cross_dir = os.path.join(VPN_RESULTS_BASE, "cross_country")
    os.makedirs(cross_dir, exist_ok=True)

    # Build global intersection — single denominator across all 5 country pairs
    print("Building global eTLD intersection (all 5 country pairs)...")
    global_etlds = build_global_intersection(DB_CONFIG_VPN, source="session")

    # Also build per-pair intersections for fetching (we only fetch data for
    # eTLDs in the global set, ensuring the same sites are compared everywhere)
    print("Building per-pair session intersections...")
    intersections = build_session_intersections(DB_CONFIG_VPN)

    all_delta:   dict[str, pd.DataFrame] = {}
    all_norm:    dict[str, pd.DataFrame] = {}
    all_vpn_:    dict[str, pd.DataFrame] = {}
    etld_data:   dict[str, dict]         = {}  # for outlier analysis

    for normal_loc, vpn_loc in VPN_LOCATION_PAIRS:
        country_label = COUNTRY_LABEL.get(normal_loc, normal_loc)
        # Restrict to global intersection — same denominator across all countries
        etlds = intersections.get((normal_loc, vpn_loc), set()) & global_etlds
        if not etlds:
            print(f"  No shared eTLDs for {normal_loc} ↔ {vpn_loc}, skipping.")
            continue

        OUTPUT_DIR = os.path.join(VPN_RESULTS_BASE, normal_loc.lower())
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        if not FORCE_RECOMPUTE and results_exist(OUTPUT_DIR, JS_FILES):
            print(f"\nLoading cached JS results: {normal_loc} ↔ {vpn_loc}")
            cached    = load_country_results(OUTPUT_DIR, JS_FILES)
            delta_df  = cached.get("js_category_delta",  pd.DataFrame())
            norm_adop = cached.get("js_normal_adoption", pd.DataFrame())
            vpn_adop  = cached.get("js_vpn_adoption",    pd.DataFrame())
            # Reconstruct raw DFs as empty — not needed for plots/outliers when cached
            normal_df = pd.DataFrame()
            vpn_df    = pd.DataFrame()
        else:
            print(f"\nStreaming JS calls: {normal_loc} ↔ {vpn_loc} "
                  f"({len(etlds)} eTLDs, global intersection)")
            normal_df, vpn_df = fetch_js_calls_pair(
                DB_CONFIG_VPN, normal_loc, vpn_loc, etlds)
            if normal_df.empty and vpn_df.empty:
                print("  No data, skipping.")
                continue
            norm_adop = compute_category_adoption(normal_df, global_etlds)
            vpn_adop  = compute_category_adoption(vpn_df,   global_etlds)
            delta_df  = compute_adoption_delta(norm_adop, vpn_adop)
            delta_df.to_csv(f"{OUTPUT_DIR}/js_category_delta.csv",    index=False)
            norm_adop.to_csv(f"{OUTPUT_DIR}/js_normal_adoption.csv",  index=False)
            vpn_adop.to_csv(f"{OUTPUT_DIR}/js_vpn_adoption.csv",      index=False)

        print_summary(normal_loc, vpn_loc, delta_df)
        plot_category_delta_heatmap({country_label: delta_df}, "cross-site", OUTPUT_DIR)
        plot_category_delta_heatmap({country_label: delta_df}, "same-site",  OUTPUT_DIR)

        all_delta[country_label] = delta_df
        all_norm[country_label]  = normal_df
        all_vpn_[country_label]  = vpn_df
        etld_data[country_label] = {"normal": normal_df, "vpn": vpn_df}

    if len(all_delta) >= 2:
        OUTPUT_DIR = cross_dir

        # Aggregate plots with global denominator
        plot_fp_tp_call_rate_delta(all_norm, all_vpn_,
                                   {c: len(global_etlds) for c in all_norm},
                                   cross_dir)
        plot_category_delta_heatmap(all_delta, "cross-site", cross_dir)
        with open(f"{cross_dir}/table_js_category_delta.tex", "w") as f:
            f.write(category_delta_to_latex(all_delta))
        print(f"  Saved table_js_category_delta.tex")

        # Per-eTLD outlier analysis
        # Use cached outliers CSV if available and no raw data was fetched
        outliers_path = os.path.join(cross_dir, "js_call_outliers.csv")
        raw_available = any(not v["normal"].empty for v in etld_data.values())
        if not FORCE_RECOMPUTE and not raw_available and os.path.exists(outliers_path):
            print("\nLoading cached outliers from js_call_outliers.csv...")
            outlier_df = pd.read_csv(outliers_path)
        else:
            print("\nComputing per-eTLD JS call outliers...")
            outlier_df = compute_etld_outliers(
                etld_data,
                jsd_threshold=JSD_THRESHOLD,
                count_ratio_threshold=COUNT_RATIO_THRESHOLD,
            )
        if not outlier_df.empty:
            n_outliers = outlier_df["is_outlier"].sum()
            print(f"  {n_outliers} raw outlier eTLD×country pairs "
                  f"(JSD≥0.2 or count ratio≥2×)")

            # Apply baseline geographic variance correction if available
            baseline_csv = "./results/location_outliers/global/location_jsd_outliers.csv"
            outlier_df = subtract_baseline_variance(outlier_df, baseline_csv)
            n_true = int(outlier_df["is_true_outlier"].sum())
            print(f"  {n_true} true VPN outliers after baseline correction")

            outlier_df.to_csv(f"{cross_dir}/js_call_outliers.csv", index=False)

            # Log-scale scatter per country (density hexbin + annotated outliers)
            for country in outlier_df["country"].unique():
                plot_outlier_scatter(outlier_df, country, cross_dir)

            # Ranked dot plot — all countries in one figure (most interpretable)
            plot_outlier_ranked(outlier_df, cross_dir, top_n=20)

            # JSD distribution box plot across countries
            plot_outlier_jsd_distribution(outlier_df, cross_dir, jsd_threshold=JSD_THRESHOLD)

            # LaTeX table
            with open(f"{cross_dir}/table_js_outliers.tex", "w") as f:
                f.write(outliers_to_latex(outlier_df, top_n=20))

            # JSON summary of outlier statistics per country
            outlier_summary = {}
            for country, grp in outlier_df.groupby("country"):
                is_out      = grp[grp["is_outlier"]]
                is_true_out = grp[grp["is_true_outlier"]] \
                              if "is_true_outlier" in grp.columns \
                              else is_out
                jsd_col = "adjusted_jsd" if "adjusted_jsd" in grp.columns else "jsd"
                outlier_summary[country] = {
                    "total_etlds":             len(grp),
                    "n_raw_outliers":          int(is_out["is_outlier"].sum()),
                    "n_true_outliers":         int(is_true_out["is_true_outlier"].sum())
                                               if "is_true_outlier" in grp.columns else None,
                    "n_jsd_outliers":          int(is_out["is_jsd_outlier"].sum()),
                    "n_count_outliers":        int(is_out["is_count_outlier"].sum()),
                    "median_jsd_raw":          round(float(grp["jsd"].median()), 4),
                    "mean_jsd_raw":            round(float(grp["jsd"].mean()), 4),
                    "median_jsd_adjusted":     round(float(grp[jsd_col].median()), 4),
                    "baseline_correction_applied": "adjusted_jsd" in grp.columns,
                    "top_true_outlier_etlds": [
                        {
                            "etld":          r["etld"],
                            "jsd_raw":       round(float(r["jsd"]), 4),
                            "jsd_adjusted":  round(float(r[jsd_col]), 4),
                            "baseline_jsd":  _safe_round(r.get("baseline_jsd")),
                            "count_ratio":   r["count_ratio"],
                            "normal_calls":  int(r["normal_calls"]),
                            "vpn_calls":     int(r["vpn_calls"]),
                        }
                        for _, r in is_true_out.nlargest(10, jsd_col).iterrows()
                    ],
                }
            with open(f"{cross_dir}/js_outlier_summary.json", "w") as f:
                json.dump(outlier_summary, f, indent=2, default=str)
            print(f"  Saved js_call_outliers.csv, js_outlier_summary.json, "
                  f"table_js_outliers.tex")

    print(f"\nDone. Outputs in: {VPN_RESULTS_BASE}")


if __name__ == "__main__":
    main()
