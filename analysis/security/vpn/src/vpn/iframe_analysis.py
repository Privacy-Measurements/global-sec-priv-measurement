"""
VPN Iframe Analysis
====================
Compares third-party iframe origins between normal and VPN access.

Source: scripts table WHERE frame_main=0, using DISTINCT (session_id, frame_id, frame_origin)
        as the iframe instance proxy (same approach as main iframe_analysis.py).
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
from src.vpn.cache import results_exist, load_country_results, IFRAME_FILES
from src.vpn.vpn_shared import (
    build_session_intersections, build_global_intersection, COUNTRY_LABEL, save_fig,
)
from src.scripts.domain_utils import _registered_domain

import json
import traceback
from urllib.parse import urlparse

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

IFRAME_QUERY = """
    SELECT
        cs.etld        AS page_etld,
        cs.location,
        s.session_id,
        s.frame_id,
        s.frame_origin
    FROM crawl_sessions cs
    JOIN (
        SELECT DISTINCT session_id, frame_id, frame_origin
        FROM scripts
        WHERE frame_main = 0
          AND frame_id IS NOT NULL
    ) s ON s.session_id = cs.id
    WHERE cs.category = %s
          AND cs.etld_url_rel != 'cross-site'
      AND cs.location IN (%s, %s)
      AND cs.etld IN ({placeholders})
"""

def fetch_iframes_pair(
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
            IFRAME_QUERY.format(placeholders=ph),
            conn,
            params=[CATEGORY, normal_loc, vpn_loc] + list(etlds),
        )
    finally:
        conn.close()

    # Enrich
    df["frame_domain"] = df["frame_origin"].apply(
        lambda o: _registered_domain(o) if pd.notna(o) and o else ""
    )
    df["page_domain"] = df["page_etld"].apply(
        lambda e: _registered_domain(f"https://{e}") if e else ""
    )
    df["is_tp"] = (
        df["frame_domain"].ne("") &
        (df["frame_domain"] != df["page_domain"])
    )
    return (
        df[df["location"] == normal_loc].copy(),
        df[df["location"] == vpn_loc].copy(),
    )


# ── Analysis ──────────────────────────────────────────────────────────────────

def compute_iframe_origin_delta(
        normal_df: pd.DataFrame,
        vpn_df: pd.DataFrame,
        etlds: set[str],
) -> pd.DataFrame:
    """
    Per third-party iframe origin: eTLD presence in normal vs VPN.
    Status: vpn_only / normal_only / both.
    """
    def tp_origins(df):
        tp = df[df["is_tp"] & df["frame_domain"].ne("")]
        return tp.groupby("frame_domain")["page_etld"].apply(set).to_dict()

    norm_map = tp_origins(normal_df)
    vpn_map  = tp_origins(vpn_df)
    all_origins = set(norm_map) | set(vpn_map)

    rows = []
    for origin in all_origins:
        n_etlds = len(norm_map.get(origin, set()) & etlds)
        v_etlds = len(vpn_map.get(origin,  set()) & etlds)
        if n_etlds == 0 and v_etlds == 0:
            continue
        if   n_etlds == 0: status = "vpn_only"
        elif v_etlds == 0: status = "normal_only"
        else:              status = "both"
        rows.append({
            "frame_domain":  origin,
            "normal_etlds":  n_etlds,
            "vpn_etlds":     v_etlds,
            "etld_delta":    v_etlds - n_etlds,
            "status":        status,
        })
    return pd.DataFrame(rows).sort_values("etld_delta")


def compute_tp_iframe_rate_delta(
        normal_df: pd.DataFrame,
        vpn_df: pd.DataFrame,
        etlds: set[str],
) -> pd.DataFrame:
    """Per-etld: tp iframe count in normal vs VPN."""
    rows = []
    for etld in etlds:
        for side, df in [("normal", normal_df), ("vpn", vpn_df)]:
            sub = df[df["page_etld"] == etld]
            tp  = sub[sub["is_tp"]]
            rows.append({
                "etld": etld, "side": side,
                "total_iframes": len(sub),
                "tp_iframes":    len(tp),
                "unique_tp_origins": tp["frame_domain"].nunique(),
            })
    return pd.DataFrame(rows)


# ── Plots ─────────────────────────────────────────────────────────────────────

def plot_tp_iframe_rate_delta(
        all_rate: dict[str, pd.DataFrame],
        out_dir: str,
):
    """Grouped bar: mean tp iframe count per site, normal vs VPN."""
    countries = list(all_rate.keys())
    x = range(len(countries))
    width = 0.35
    norm = [all_rate[c][all_rate[c]["side"] == "normal"]["tp_iframes"].mean()
            for c in countries]
    vpn  = [all_rate[c][all_rate[c]["side"] == "vpn"]["tp_iframes"].mean()
            for c in countries]
    fig, ax = plt.subplots(figsize=(max(6, len(countries) * 1.5), 5))
    ax.bar([xi - width/2 for xi in x], norm, width,
           label="Normal", color="steelblue", edgecolor="white")
    ax.bar([xi + width/2 for xi in x], vpn,  width,
           label="VPN",    color="tomato",    edgecolor="white")
    ax.set_xticks(list(x))
    ax.set_xticklabels(countries, rotation=20, ha="right")
    ax.set_ylabel("Mean 3rd-party iframes per site")
    ax.set_title("Third-party Iframe Count: Normal vs VPN")
    ax.legend()
    plt.tight_layout()
    save_fig(f"{out_dir}/iframe_tp_rate_delta")


def plot_origin_delta_heatmap(
        all_origin: dict[str, pd.DataFrame],
        out_dir: str,
        top_n: int = 20,
):
    """Heatmap: top iframe origin changes × countries."""
    all_df = pd.concat([
        df.assign(country=c) for c, df in all_origin.items()
    ], ignore_index=True)
    if all_df.empty:
        return
    top_origins = (
        all_df.groupby("frame_domain")["etld_delta"]
        .apply(lambda s: s.abs().sum())
        .sort_values(ascending=False)
        .head(top_n).index.tolist()
    )
    countries = list(all_origin.keys())
    heat = pd.DataFrame(index=top_origins, columns=countries, data=0)
    for c, df in all_origin.items():
        agg = df.set_index("frame_domain")["etld_delta"]
        for orig in top_origins:
            heat.loc[orig, c] = int(agg.get(orig, 0))
    heat = heat.astype(float)
    heat = heat.loc[(heat.abs() > 0).any(axis=1)]
    if heat.empty:
        return

    fig, ax = plt.subplots(figsize=(max(5, len(countries) * 1.8),
                                    max(6, len(heat) * 0.45)))
    sns.heatmap(heat, ax=ax, annot=True, fmt=".0f",
                cmap="RdYlGn", center=0, linewidths=0.4,
                cbar_kws={"label": "Site Count"})
    # ax.set_title(f"Top {top_n} Iframe Origin Changes Over VPN")
    ax.set_xticklabels(ax.get_xticklabels(), rotation=30, ha="right", fontsize=9)
    ax.set_yticklabels(ax.get_yticklabels(), rotation=0, fontsize=8)
    plt.tight_layout()
    save_fig(f"{out_dir}/iframe_origin_delta_heatmap")


# ── Console summary ───────────────────────────────────────────────────────────

def print_summary(normal_loc: str, vpn_loc: str,
                  rate_df: pd.DataFrame, origin_df: pd.DataFrame):
    print(f"\n{'='*70}")
    print(f"IFRAME ANALYSIS: {normal_loc} ↔ {vpn_loc}")
    print(f"{'='*70}")
    if not rate_df.empty:
        nm = rate_df[rate_df["side"] == "normal"]["tp_iframes"].mean()
        vm = rate_df[rate_df["side"] == "vpn"]["tp_iframes"].mean()
        print(f"  Mean TP iframes/site — normal: {nm:.2f}  VPN: {vm:.2f}  "
              f"Δ={vm-nm:+.2f}")
    if not origin_df.empty:
        vpn_only    = (origin_df["status"] == "vpn_only").sum()
        normal_only = (origin_df["status"] == "normal_only").sum()
        print(f"  VPN-only iframe origins: {vpn_only}")
        print(f"  Lost-over-VPN iframe origins: {normal_only}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    global OUTPUT_DIR
    os.makedirs(VPN_RESULTS_BASE, exist_ok=True)
    cross_dir = os.path.join(VPN_RESULTS_BASE, "cross_country")
    os.makedirs(cross_dir, exist_ok=True)

    intersections = build_session_intersections(DB_CONFIG_VPN)
    print("Building global eTLD intersection (all 5 country pairs)...")
    global_etlds = build_global_intersection(DB_CONFIG_VPN, source="session")

    all_rate:   dict[str, pd.DataFrame] = {}
    all_origin: dict[str, pd.DataFrame] = {}

    for normal_loc, vpn_loc in VPN_LOCATION_PAIRS:
        country_label = COUNTRY_LABEL.get(normal_loc, normal_loc)
        etlds = intersections.get((normal_loc, vpn_loc), set()) & global_etlds
        if not etlds:
            continue

        OUTPUT_DIR = os.path.join(VPN_RESULTS_BASE, normal_loc.lower())
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        if not FORCE_RECOMPUTE and results_exist(OUTPUT_DIR, IFRAME_FILES):
            print(f"\nLoading cached results: {normal_loc} ↔ {vpn_loc}")
            cached    = load_country_results(OUTPUT_DIR, IFRAME_FILES)
            rate_df   = cached.get("iframe_rate_delta",   pd.DataFrame())
            origin_df = cached.get("iframe_origin_delta", pd.DataFrame())
        else:
            print(f"\nFetching iframes: {normal_loc} ↔ {vpn_loc} ({len(etlds)} eTLDs)")
            normal_df, vpn_df = fetch_iframes_pair(
                DB_CONFIG_VPN, normal_loc, vpn_loc, etlds)
            rate_df   = compute_tp_iframe_rate_delta(normal_df, vpn_df, etlds)
            origin_df = compute_iframe_origin_delta(normal_df, vpn_df, etlds)
            rate_df.to_csv(f"{OUTPUT_DIR}/iframe_rate_delta.csv",   index=False)
            origin_df.to_csv(f"{OUTPUT_DIR}/iframe_origin_delta.csv", index=False)

        print_summary(normal_loc, vpn_loc, rate_df, origin_df)

        all_rate[country_label]   = rate_df
        all_origin[country_label] = origin_df

    if len(all_rate) >= 2:
        OUTPUT_DIR = cross_dir
        plot_tp_iframe_rate_delta(all_rate, cross_dir)
        plot_origin_delta_heatmap(all_origin, cross_dir)

    print(f"\nDone. Outputs in: {VPN_RESULTS_BASE}")


if __name__ == "__main__":
    main()
