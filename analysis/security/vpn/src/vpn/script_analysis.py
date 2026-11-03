"""
VPN Script Analysis
====================
Compares first/third-party script distribution between normal and VPN access.

Covers:
  1. FP/TP ratio delta per country pair
  2. Third-party domains appearing only in VPN (surveillance signal)
  3. Third-party domains disappearing over VPN (geo-blocking signal)
  4. Top sources with largest presence delta
  5. Cross-country: which script domain changes are systematic

Source: scripts table in crawl_analysis_new, joined via crawl_sessions.
        Intersection eTLDs only (shared across normal and VPN location).
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
from src.vpn.cache import results_exist, load_country_results, SCRIPT_FILES
from src.vpn.vpn_shared import (
    build_session_intersections, build_global_intersection, COUNTRY_LABEL, save_fig, _pct,
)

import json
import traceback
from collections import defaultdict

import pymysql
import pandas as pd
import numpy as np
import tldextract
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns

OUTPUT_DIR = VPN_RESULTS_BASE
FORCE_RECOMPUTE = False  # set True to bypass cache and re-run DB queries


# ── DB fetch ──────────────────────────────────────────────────────────────────

SCRIPTS_QUERY = """
    SELECT
        cs.etld,
        cs.location,
        s.script_src,
        s.etld_script_src_rel,
        s.frame_main
    FROM crawl_sessions cs
    JOIN scripts s ON s.session_id = cs.id
    WHERE cs.category = %s
          AND cs.etld_url_rel != 'cross-site'
      AND cs.location IN (%s, %s)
      AND cs.etld IN ({placeholders})
      AND s.script_src IS NOT NULL
      AND s.script_src != ''
"""

def fetch_scripts_pair(
        db_config: dict,
        normal_loc: str,
        vpn_loc: str,
        etlds: set[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not etlds:
        return pd.DataFrame(), pd.DataFrame()
    ph   = ", ".join(["%s"] * len(etlds))
    conn = pymysql.connect(**db_config)
    try:
        df = pd.read_sql(
            SCRIPTS_QUERY.format(placeholders=ph),
            conn,
            params=[CATEGORY, normal_loc, vpn_loc] + list(etlds),
        )
    finally:
        conn.close()
    return (
        df[df["location"] == normal_loc].copy(),
        df[df["location"] == vpn_loc].copy(),
    )


# ── Analysis ──────────────────────────────────────────────────────────────────

def _reg_domain(url: str) -> str:
    try:
        ext = tldextract.extract(url)
        return ext.registered_domain.lower() if ext.registered_domain else ""
    except Exception:
        return ""


def compute_fp_tp_delta(
        normal_df: pd.DataFrame,
        vpn_df: pd.DataFrame,
        etlds: set[str],
) -> pd.DataFrame:
    """Per-etld: FP/TP counts and share in normal vs VPN."""
    rows = []
    for etld in etlds:
        nd = normal_df[normal_df["etld"] == etld]
        vd = vpn_df[vpn_df["etld"] == etld]
        for side, df in [("normal", nd), ("vpn", vd)]:
            total = len(df)
            tp    = (df["etld_script_src_rel"] == "cross-site").sum()
            rows.append({
                "etld": etld, "side": side,
                "total": total, "tp": tp, "fp": total - tp,
                "tp_rate": tp / total if total else 0,
            })
    return pd.DataFrame(rows)


def compute_domain_delta(
        normal_df: pd.DataFrame,
        vpn_df: pd.DataFrame,
        etlds: set[str],
) -> pd.DataFrame:
    """
    Per third-party domain: eTLD presence count in normal vs VPN.
    Classifies each domain as: vpn_only / normal_only / both / changed_count.
    """
    def tp_domains_per_etld(df):
        tp = df[df["etld_script_src_rel"] == "cross-site"].copy()
        tp["domain"] = tp["script_src"].apply(_reg_domain)
        tp = tp[tp["domain"] != ""]
        return tp.groupby("domain")["etld"].apply(set).to_dict()

    norm_map = tp_domains_per_etld(normal_df)
    vpn_map  = tp_domains_per_etld(vpn_df)
    all_domains = set(norm_map) | set(vpn_map)

    rows = []
    for dom in all_domains:
        n_etlds = len(norm_map.get(dom, set()) & etlds)
        v_etlds = len(vpn_map.get(dom,  set()) & etlds)
        if n_etlds == 0 and v_etlds == 0:
            continue
        if   n_etlds == 0: status = "vpn_only"
        elif v_etlds == 0: status = "normal_only"
        else:              status = "both"
        rows.append({
            "domain":         dom,
            "normal_etlds":   n_etlds,
            "vpn_etlds":      v_etlds,
            "etld_delta":     v_etlds - n_etlds,
            "status":         status,
        })
    return pd.DataFrame(rows).sort_values("etld_delta")


# ── Plots ─────────────────────────────────────────────────────────────────────

def plot_fp_tp_delta(
        all_fp_tp: dict[str, pd.DataFrame],
        out_dir: str,
):
    """Grouped bar: TP rate normal vs VPN per country."""
    countries = list(all_fp_tp.keys())
    x = range(len(countries))
    width = 0.35
    norm_rates = [
        all_fp_tp[c][all_fp_tp[c]["side"] == "normal"]["tp_rate"].mean() * 100
        for c in countries
    ]
    vpn_rates = [
        all_fp_tp[c][all_fp_tp[c]["side"] == "vpn"]["tp_rate"].mean() * 100
        for c in countries
    ]
    fig, ax = plt.subplots(figsize=(max(6, len(countries) * 1.5), 5))
    ax.bar([xi - width/2 for xi in x], norm_rates, width,
           label="Normal", color="steelblue", edgecolor="white")
    ax.bar([xi + width/2 for xi in x], vpn_rates,  width,
           label="VPN",    color="tomato",    edgecolor="white")
    ax.set_xticks(list(x))
    ax.set_xticklabels(countries, rotation=20, ha="right")
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax.set_ylabel("Mean 3rd-party script rate per site (%)")
    ax.set_title("Third-party Script Share: Normal vs VPN")
    ax.legend()
    plt.tight_layout()
    save_fig(f"{out_dir}/script_fp_tp_delta")


def plot_domain_delta(
        domain_df: pd.DataFrame,
        country_label: str,
        out_dir: str,
        top_n: int = 20,
):
    """
    Two-panel horizontal bar:
    Left: top domains lost over VPN (normal_only / large negative delta)
    Right: top domains gained over VPN (vpn_only / large positive delta)
    """
    if domain_df.empty:
        return
    lost   = domain_df[domain_df["etld_delta"] < 0].nsmallest(top_n, "etld_delta")
    gained = domain_df[domain_df["etld_delta"] > 0].nlargest(top_n, "etld_delta")

    fig, axes = plt.subplots(1, 2, figsize=(16, max(5, top_n * 0.4)))
    for ax, sub, title, color in [
        (axes[0], lost,   f"Scripts LOST over VPN — {country_label}",   "tomato"),
        (axes[1], gained, f"Scripts GAINED over VPN — {country_label}", "mediumseagreen"),
    ]:
        if sub.empty:
            ax.text(0.5, 0.5, "None", ha="center", va="center",
                    transform=ax.transAxes, color="grey")
            ax.set_title(title)
            continue
        ax.barh(sub["domain"][::-1], sub["etld_delta"].abs()[::-1],
                color=color, edgecolor="white")
        ax.set_xlabel("# eTLDs affected")
        ax.set_title(title)
        ax.tick_params(axis="y", labelsize=7)
    plt.tight_layout()
    save_fig(f"{out_dir}/script_domain_delta")


def plot_cross_country_domain_changes(
        all_domain: dict[str, pd.DataFrame],
        out_dir: str,
        top_n: int = 20,
):
    """
    Heatmap: top domains by |delta| × countries, cell = etld_delta.
    """
    all_df = pd.concat([
        df.assign(country=c) for c, df in all_domain.items()
    ], ignore_index=True)
    if all_df.empty:
        return
    top_domains = (all_df.groupby("domain")["etld_delta"]
                         .apply(lambda s: s.abs().sum())
                         .sort_values(ascending=False)
                         .head(top_n).index.tolist())
    countries = list(all_domain.keys())
    heat = pd.DataFrame(index=top_domains, columns=countries, data=0)
    for c, df in all_domain.items():
        agg = df.set_index("domain")["etld_delta"]
        for dom in top_domains:
            heat.loc[dom, c] = int(agg.get(dom, 0))
    heat = heat.astype(float)

    fig, ax = plt.subplots(figsize=(max(5, len(countries) * 1.8),
                                    max(7, top_n * 0.45)))
    sns.heatmap(heat, ax=ax, annot=True, fmt=".0f",
                cmap="RdYlGn", center=0, linewidths=0.4,
                cbar_kws={"label": "Site Count"})
    # ax.set_title(f"Top {top_n} scripts domains added / missing on visited sites over VPN traffic (compared to eTLD Count Difference)")
    ax.set_xticklabels(ax.get_xticklabels(), rotation=30, ha="right", fontsize=9)
    ax.set_yticklabels(ax.get_yticklabels(), rotation=0, fontsize=8)
    plt.tight_layout()
    save_fig(f"{out_dir}/script_domain_delta_cross_country")


# ── LaTeX ─────────────────────────────────────────────────────────────────────

def domain_delta_to_latex(
        all_domain: dict[str, pd.DataFrame],
        change_type: str = "vpn_only",
        top_n: int = 15,
) -> str:
    countries = list(all_domain.keys())
    lines = [
        r"\begin{table}[ht]", r"\centering", r"\small",
        r"\begin{tabular}{l" + "rr" * len(countries) + "}", r"\toprule",
        "Domain & " + " & ".join(
            f"\\multicolumn{{2}}{{c}}{{{c}}}" for c in countries
        ) + r" \\",
        "& " + " & ".join(["Normal eTLDs", "VPN eTLDs"] * len(countries)) + r" \\",
        r"\midrule",
    ]
    # Find top domains of this type across all countries
    all_df = pd.concat([
        df[df["status"] == change_type].assign(country=c)
        for c, df in all_domain.items()
    ], ignore_index=True)
    if all_df.empty:
        return ""
    top = (all_df.groupby("domain")["etld_delta"].apply(abs)
                 .groupby("domain").sum()
                 .sort_values(ascending=False).head(top_n).index)
    for dom in top:
        vals = []
        for c in countries:
            df = all_domain[c]
            row = df[df["domain"] == dom]
            if row.empty:
                vals.append("0 & 0")
            else:
                r = row.iloc[0]
                vals.append(f"{int(r['normal_etlds'])} & {int(r['vpn_etlds'])}")
        lines.append(
            dom.replace("_", r"\_") + " & " + " & ".join(vals) + r" \\"
        )
    caption = {
        "vpn_only":    "Third-party script domains appearing ONLY over VPN.",
        "normal_only": "Third-party script domains disappearing over VPN (geo-blocked).",
    }.get(change_type, "Script domain changes over VPN.")
    lines += [
        r"\bottomrule", r"\end{tabular}",
        r"\caption{" + caption + r"}",
        r"\label{tab:vpn_script_" + change_type + r"}", r"\end{table}",
    ]
    return "\n".join(lines)


# ── Console summary ───────────────────────────────────────────────────────────

def print_summary(
        normal_loc: str, vpn_loc: str,
        fp_tp_df: pd.DataFrame,
        domain_df: pd.DataFrame,
):
    print(f"\n{'='*70}")
    print(f"SCRIPT ANALYSIS: {normal_loc} ↔ {vpn_loc}")
    print(f"{'='*70}")
    if not fp_tp_df.empty:
        norm_tp = fp_tp_df[fp_tp_df["side"] == "normal"]["tp_rate"].mean() * 100
        vpn_tp  = fp_tp_df[fp_tp_df["side"] == "vpn"]["tp_rate"].mean()   * 100
        print(f"  Mean TP rate — normal: {norm_tp:.1f}%  VPN: {vpn_tp:.1f}%  "
              f"Δ={vpn_tp-norm_tp:+.1f}pp")
    if not domain_df.empty:
        vpn_only    = (domain_df["status"] == "vpn_only").sum()
        normal_only = (domain_df["status"] == "normal_only").sum()
        print(f"  Domains VPN-only (new over VPN): {vpn_only}")
        print(f"  Domains normal-only (lost over VPN): {normal_only}")
        print(f"\n  Top 5 VPN-only domains:")
        for _, r in domain_df[domain_df["status"] == "vpn_only"].nlargest(5, "vpn_etlds").iterrows():
            print(f"    {r['domain']:<40} {r['vpn_etlds']:4} eTLDs")
        print(f"\n  Top 5 lost-over-VPN domains:")
        for _, r in domain_df[domain_df["status"] == "normal_only"].nlargest(5, "normal_etlds").iterrows():
            print(f"    {r['domain']:<40} {r['normal_etlds']:4} eTLDs")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    global OUTPUT_DIR
    os.makedirs(VPN_RESULTS_BASE, exist_ok=True)
    cross_dir = os.path.join(VPN_RESULTS_BASE, "cross_country")
    os.makedirs(cross_dir, exist_ok=True)

    print("Building session-level eTLD intersections...")
    intersections = build_session_intersections(DB_CONFIG_VPN)
    print("Building global eTLD intersection (all 5 country pairs)...")
    global_etlds = build_global_intersection(DB_CONFIG_VPN, source="session")

    all_fp_tp:  dict[str, pd.DataFrame] = {}
    all_domain: dict[str, pd.DataFrame] = {}

    for normal_loc, vpn_loc in VPN_LOCATION_PAIRS:
        country_label = COUNTRY_LABEL.get(normal_loc, normal_loc)
        etlds = intersections.get((normal_loc, vpn_loc), set()) & global_etlds
        if not etlds:
            print(f"  No shared eTLDs for {normal_loc} ↔ {vpn_loc}, skipping.")
            continue

        OUTPUT_DIR = os.path.join(VPN_RESULTS_BASE, normal_loc.lower())
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        if not FORCE_RECOMPUTE and results_exist(OUTPUT_DIR, SCRIPT_FILES):
            print(f"\nLoading cached results: {normal_loc} ↔ {vpn_loc}")
            cached = load_country_results(OUTPUT_DIR, SCRIPT_FILES)
            fp_tp_df  = cached.get("script_fp_tp_delta",  pd.DataFrame())
            domain_df = cached.get("script_domain_delta", pd.DataFrame())
        else:
            print(f"\nFetching scripts: {normal_loc} ↔ {vpn_loc} ({len(etlds)} eTLDs)")
            normal_df, vpn_df = fetch_scripts_pair(
                DB_CONFIG_VPN, normal_loc, vpn_loc, etlds)
            fp_tp_df   = compute_fp_tp_delta(normal_df, vpn_df, etlds)
            domain_df  = compute_domain_delta(normal_df, vpn_df, etlds)
            fp_tp_df.to_csv(f"{OUTPUT_DIR}/script_fp_tp_delta.csv",  index=False)
            domain_df.to_csv(f"{OUTPUT_DIR}/script_domain_delta.csv", index=False)

        print_summary(normal_loc, vpn_loc, fp_tp_df, domain_df)
        plot_domain_delta(domain_df, country_label, OUTPUT_DIR)

        all_fp_tp[country_label]  = fp_tp_df
        all_domain[country_label] = domain_df

    if len(all_fp_tp) >= 2:
        OUTPUT_DIR = cross_dir
        plot_fp_tp_delta(all_fp_tp, cross_dir)
        plot_cross_country_domain_changes(all_domain, cross_dir)

        with open(f"{cross_dir}/table_vpn_only_scripts.tex", "w") as f:
            f.write(domain_delta_to_latex(all_domain, "vpn_only"))
        with open(f"{cross_dir}/table_normal_only_scripts.tex", "w") as f:
            f.write(domain_delta_to_latex(all_domain, "normal_only"))

    print(f"\nDone. Outputs in: {VPN_RESULTS_BASE}")


if __name__ == "__main__":
    main()
