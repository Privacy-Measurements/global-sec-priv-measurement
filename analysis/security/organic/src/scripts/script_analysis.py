"""
Third-party script analysis
Unit of analysis: etld × location
Source: scripts table, etld_script_src_rel = 'cross-site' for third-party

Components:
  1. Third-party script distribution per dataset
  2. Deviation in TOP_N third-party sources across datasets
  3. Profile of third-party scripts (owner, category, tracker/FP flags)
  4. First-party vs third-party distribution across datasets
  5. Non-uniform third-party source presence across datasets
  6. Third-party script distribution across website categories (CF Radar)
"""

import sys
import os
_HERE = os.path.abspath(os.path.dirname(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "../.."))
for _p in [_HERE, _ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from src.scripts.config import (
    DB_CONFIG, DB_CONFIG_NEW,
    DATASETS, VPN_DATASET, INCLUDE_VPN,
    SCRIPTS_RESULTS_BASE, WORKERS, MIN_ETLD_PRESENCE,
)
from src.scripts.domain_categoriser import categorise_bulk, get_rank
from src.utils.consistency import (
    ConsistencyTagger, load_visit_map, filter_sufficient_visits,
    TIER_A, TIER_B, TIER_ABSENT,
)

import json
import traceback
from collections import Counter, defaultdict

import pymysql
import pandas as pd
import numpy as np
import tldextract
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns

from src.headers.source_profiler_lists import (
    _load_disconnect, _load_tracker_set, _load_fingerprinting_domains,
)

OUTPUT_DIR = SCRIPTS_RESULTS_BASE  # overridden per category in main()

TOP_N = 20  # top-N third-party sources for deviation analysis

# ── DB fetch ──────────────────────────────────────────────────────────────────

SCRIPTS_QUERY = """
    SELECT
        cs.etld,
        cs.location,
        cs.category,
        cs.id          AS session_id,
        s.script_src,
        s.etld_script_src_rel,
        s.script_type,
        s.frame_main
    FROM crawl_sessions cs
    JOIN scripts s ON s.session_id = cs.id
    WHERE cs.category = %s
      AND cs.category NOT LIKE '%%validation'
      AND cs.etld_url_rel != 'cross-site'
      AND s.script_src IS NOT NULL
      AND s.script_src != ''
"""

def fetch_scripts(db_config: dict, category: str) -> pd.DataFrame:
    conn = pymysql.connect(**db_config)
    df = pd.read_sql(SCRIPTS_QUERY, conn, params=(category,))
    conn.close()
    return df


# ── Domain extraction ─────────────────────────────────────────────────────────

def script_domain(script_src: str) -> str:
    """Registered domain (eTLD+1) from script_src URL."""
    try:
        ext = tldextract.extract(script_src)
        return ext.registered_domain.lower() if ext.registered_domain else ""
    except Exception:
        return ""


# ── Build per-unit frame ──────────────────────────────────────────────────────

def build_unit_frame(raw: pd.DataFrame, tagger=None) -> pd.DataFrame:
    """
    Aggregate to etld × location level.
    For each unit: count of unique third-party script domains,
    count of total scripts (any party), count of first-party scripts,
    set of unique third-party domains observed (majority across sessions).

    If tagger is provided, also computes stable_tp_domains and
    meaningful_tp_domains counts using the consistency tagger.
    """
    raw = raw.copy()
    raw["domain"] = raw["script_src"].apply(script_domain)

    records = []
    for (etld, loc, cat), grp in raw.groupby(["etld", "location", "category"]):
        sessions = grp["session_id"].nunique()
        total    = len(grp)
        fp       = grp[grp["etld_script_src_rel"].isin(["same-site", "same-origin"])]
        tp       = grp[grp["etld_script_src_rel"] == "cross-site"]

        # Unique third-party domains seen in majority of sessions
        tp_domain_sessions = (
            tp[tp["domain"] != ""]
            .groupby("domain")["session_id"].nunique()
        )
        majority_tp = set(
            tp_domain_sessions[tp_domain_sessions >= max(1, sessions / 2)].index
        )

        # Per-domain session sets for consistency tagging
        dom_sessions = (
            tp[tp["domain"] != ""]
            .groupby("domain")["session_id"].apply(set).to_dict()
        )

        stable_count = 0
        meaningful_count = 0
        for dom in majority_tp:
            tier = (
                tagger.tag_binary(etld, loc, dom_sessions.get(dom, set()))
                if tagger else TIER_B
            )
            if tier == TIER_A:
                stable_count += 1
            else:
                meaningful_count += 1

        records.append({
            "etld":        etld,
            "location":    loc,
            "category":    cat,
            "session_count":       sessions,
            "total_scripts":       total,
            "fp_scripts":          len(fp),
            "tp_scripts":          len(tp),
            "unique_tp_domains":   len(majority_tp),
            "tp_domain_set":       majority_tp,
            "stable_tp_domains":   stable_count,
            "meaningful_tp_domains": meaningful_count,
        })
    return pd.DataFrame(records)


# ── Third-party source frequency table ───────────────────────────────────────

def _fqdn_from_url(url: str) -> str:
    """Extract FQDN (subdomain + domain) from a script src URL."""
    try:
        from urllib.parse import urlparse
        host = urlparse(url).hostname or ""
        return host.lower()
    except Exception:
        return ""


def build_tp_source_table(raw: pd.DataFrame, tagger=None) -> pd.DataFrame:
    """
    For each third-party domain: how many unique eTLDs include it (per location).
    Returns DataFrame: domain, top_fqdn, location, category, etld_count,
                       session_count, tier.

    top_fqdn: the most commonly seen full hostname (subdomain) for this
              registered domain, e.g. "www.googletagmanager.com" for
              registered domain "googletagmanager.com". Used for table display.

    tier is TIER_A if for every etld that includes this domain at this location,
    the domain appeared in all that etld's visits; TIER_B otherwise.
    """
    tp = raw[raw["etld_script_src_rel"] == "cross-site"].copy()
    tp["domain"] = tp["script_src"].apply(script_domain)
    tp["fqdn"]   = tp["script_src"].apply(_fqdn_from_url)
    tp = tp[tp["domain"] != ""]

    # Pre-compute most common FQDN per registered domain (across all locations)
    top_fqdn_map = (tp.groupby(["domain", "fqdn"])
                      .size().reset_index(name="n")
                      .sort_values("n", ascending=False)
                      .groupby("domain")["fqdn"].first()
                      .to_dict())

    rows = []
    for (domain, loc), grp in tp.groupby(["domain", "location"]):
        all_stable = True
        for etld_val, etld_grp in grp.groupby("etld"):
            dom_sids = set(etld_grp["session_id"].unique())
            if tagger and tagger.tag_binary(etld_val, loc, dom_sids) != TIER_A:
                all_stable = False
                break
        tier = TIER_A if (tagger and all_stable) else TIER_B

        rows.append({
            "domain":        domain,
            "top_fqdn":      top_fqdn_map.get(domain, domain),
            "location":      loc,
            "category":      grp["category"].iloc[0],
            "etld_count":    grp["etld"].nunique(),
            "session_count": grp["session_id"].nunique(),
            "tier":          tier,
        })
    return pd.DataFrame(rows)


# ── Privacy list profiling ────────────────────────────────────────────────────

def profile_domains(domains: list[str]) -> pd.DataFrame:
    """
    Enrich a list of third-party domains with:
      owner (disconnect.me), category (CF Radar + fallback),
      is_tracker, is_fingerprinter.
    """
    resource_to_entity, _ = _load_disconnect()
    tracker_set            = _load_tracker_set()
    fp_domains             = _load_fingerprinting_domains()
    categories             = categorise_bulk(domains, verbose=True)

    rows = []
    for d in domains:
        ext = tldextract.extract(d)
        reg = ext.registered_domain.lower() if ext.registered_domain else d.lower()
        fqdn = ext.fqdn.lower() if ext.fqdn else d.lower()

        entity = resource_to_entity.get(fqdn) or resource_to_entity.get(reg)
        is_tracker = reg in tracker_set or fqdn in tracker_set
        is_fp      = reg in fp_domains  or fqdn in fp_domains
        cat        = categories.get(d, "Other/Unknown")

        rows.append({
            "domain":           d,
            "owner":            entity or "—",
            "cf_category":      cat,
            "is_tracker":       is_tracker,
            "is_fingerprinter": is_fp,
            "cf_rank":          get_rank(d),
        })
    return pd.DataFrame(rows)


# ── Cross-dataset deviation ───────────────────────────────────────────────────

def compute_cross_dataset_deviation(
    source_tables: dict[str, pd.DataFrame],
    top_n: int = TOP_N,
) -> pd.DataFrame:
    """
    For each third-party domain: compute per-dataset prevalence
    (fraction of eTLDs that include it), then flag high-deviation sources.

    Returns DataFrame with columns:
      domain, <cat>_prev, ..., max_prev, min_prev, spread, is_significant
    """
    datasets = sorted(source_tables.keys())

    # Build domain → {dataset: prevalence} map (aggregated across locations)
    prev: dict[str, dict[str, float]] = defaultdict(dict)
    etld_counts: dict[str, int] = {}

    for cat, st in source_tables.items():
        if st.empty:
            continue
        total_etlds = st.groupby("location")["etld_count"].sum().sum()  # rough total
        # Aggregate across locations: a domain's prevalence = max across locations
        # (conservative: present if seen in any location)
        for domain, grp in st.groupby("domain"):
            # Fraction of eTLDs (summed across locations) that include this domain
            etld_sum = grp["etld_count"].sum()
            if etld_sum < MIN_ETLD_PRESENCE:
                continue
            prev[domain][cat] = grp["etld_count"].max()  # peak location

    if not prev:
        return pd.DataFrame()

    rows = []
    for domain, dp in prev.items():
        if len(dp) < 1:
            continue
        vals = {f"{cat}_etld_count": dp.get(cat, 0) for cat in datasets}
        counts = [dp.get(cat, 0) for cat in datasets]
        rows.append({
            "domain":  domain,
            **vals,
            "max_count": max(counts),
            "min_count": min(counts),
            "spread":    max(counts) - min(counts),
        })

    df = pd.DataFrame(rows).sort_values("spread", ascending=False)

    # "Significant" = appears heavily in ≥1 dataset but barely/not in ≥1 other
    # Threshold: spread > 50% of max_count and max_count >= MIN_ETLD_PRESENCE
    df["is_significant"] = (
        (df["spread"] >= df["max_count"] * 0.5) &
        (df["max_count"] >= MIN_ETLD_PRESENCE)
    )
    return df


# ── Summary statistics ────────────────────────────────────────────────────────

def summarise_distribution(unit_df: pd.DataFrame) -> dict:
    """Per-location summary: tp rate, fp rate, unique tp domains distribution."""
    summary = {}
    for (cat, loc), grp in unit_df.groupby(["category", "location"]):
        n = len(grp)
        tp_rate = grp["tp_scripts"].sum() / grp["total_scripts"].sum() if grp["total_scripts"].sum() else 0
        fp_rate = grp["fp_scripts"].sum() / grp["total_scripts"].sum() if grp["total_scripts"].sum() else 0
        summary[(cat, loc)] = {
            "n_sites":         n,
            "total_scripts":   int(grp["total_scripts"].sum()),
            "tp_rate":         tp_rate,
            "fp_rate":         fp_rate,
            "median_unique_tp": grp["unique_tp_domains"].median(),
            "mean_unique_tp":  grp["unique_tp_domains"].mean(),
        }
    return summary


# ── Plots ─────────────────────────────────────────────────────────────────────

def _save(fname: str):
    plt.savefig(f"{OUTPUT_DIR}/{fname}.pdf", bbox_inches="tight")
    plt.savefig(f"{OUTPUT_DIR}/{fname}.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved {fname}.pdf/png")


def plot_fp_tp_distribution(unit_df: pd.DataFrame):
    """Stacked bar: first-party vs third-party script share per location."""
    data = []
    for (cat, loc), grp in unit_df.groupby(["category", "location"]):
        total = grp["total_scripts"].sum()
        if total == 0:
            continue
        data.append({
            "label":   loc,
            "1st-party": grp["fp_scripts"].sum() / total * 100,
            "3rd-party": grp["tp_scripts"].sum() / total * 100,
        })
    if not data:
        return
    df = pd.DataFrame(data).set_index("label")
    fig, ax = plt.subplots(figsize=(max(8, len(df) * 1.1), 5))
    df.plot(kind="bar", stacked=True, ax=ax,
            color=["steelblue", "tomato"], edgecolor="white")
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax.set_ylabel("% of scripts")
    ax.set_xlabel("")
    ax.set_title("First-party vs Third-party Script Distribution by Location")
    ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left")
    ax.tick_params(axis="x", rotation=30)
    plt.tight_layout()
    _save("fp_tp_distribution")


def plot_unique_tp_domains_dist(unit_df: pd.DataFrame):
    """Box plot: distribution of unique third-party domain count per site."""
    locations = sorted(unit_df["location"].unique())
    fig, ax = plt.subplots(figsize=(max(8, len(locations) * 1.2), 5))
    data = [unit_df[unit_df["location"] == loc]["unique_tp_domains"].dropna().values
            for loc in locations]
    ax.boxplot(data, labels=locations, patch_artist=True,
               boxprops=dict(facecolor="steelblue", alpha=0.6))
    ax.set_xlabel("Location")
    ax.set_ylabel("Unique 3rd-party domains per site")
    ax.set_title("Distribution of Unique Third-party Script Domains per Site")
    ax.tick_params(axis="x", rotation=30)
    plt.tight_layout()
    _save("unique_tp_domains_dist")


def plot_top_tp_sources(source_table: pd.DataFrame, top_n: int = TOP_N):
    """Horizontal bar: top-N third-party domains by eTLD count, per location."""
    if source_table.empty:
        return
    locations = sorted(source_table["location"].unique())
    fig, axes = plt.subplots(1, len(locations),
                             figsize=(7 * len(locations), max(6, top_n * 0.4)),
                             sharey=False)
    if len(locations) == 1:
        axes = [axes]
    for ax, loc in zip(axes, locations):
        sub = (source_table[source_table["location"] == loc]
               .nlargest(top_n, "etld_count"))
        if sub.empty:
            ax.set_visible(False)
            continue
        ax.barh(sub["domain"][::-1], sub["etld_count"][::-1],
                color="steelblue", edgecolor="white")
        ax.set_xlabel("# eTLDs including this script domain")
        ax.set_title(loc)
        ax.tick_params(axis="y", labelsize=7)
    fig.suptitle(f"Top {top_n} Third-party Script Domains by eTLD Presence", y=1.02)
    plt.tight_layout()
    _save("top_tp_sources")


def plot_tp_profile_breakdown(profile_df: pd.DataFrame, source_table: pd.DataFrame):
    """
    Stacked bar: category breakdown of top-N third-party domains
    (weighted by total eTLD count).
    """
    if profile_df.empty or source_table.empty:
        return
    merged = source_table.merge(profile_df[["domain", "cf_category", "is_tracker", "is_fingerprinter"]],
                                on="domain", how="left")
    merged["cf_category"] = merged["cf_category"].fillna("Other/Unknown")

    # Aggregate eTLD counts by category
    cat_counts = merged.groupby("cf_category")["etld_count"].sum().sort_values(ascending=False)

    fig, ax = plt.subplots(figsize=(10, 5))
    cat_counts.plot(kind="bar", ax=ax, color="steelblue", edgecolor="white")
    ax.set_ylabel("Total eTLD inclusions")
    ax.set_xlabel("Domain category")
    ax.set_title("Third-party Script Category Distribution (by eTLD inclusion count)")
    ax.tick_params(axis="x", rotation=40)
    plt.tight_layout()
    _save("tp_category_breakdown")


def plot_tracker_fp_summary(profile_df: pd.DataFrame, source_table: pd.DataFrame):
    """Bar: tracker / fingerprinter share among top-N third-party domains."""
    if profile_df.empty or source_table.empty:
        return
    merged = source_table.merge(profile_df[["domain", "is_tracker", "is_fingerprinter"]],
                                on="domain", how="left")
    total_etlds = merged["etld_count"].sum()
    tracker_etlds = merged[merged["is_tracker"] == True]["etld_count"].sum()
    fp_etlds      = merged[merged["is_fingerprinter"] == True]["etld_count"].sum()

    fig, ax = plt.subplots(figsize=(6, 4))
    labels = ["Tracker", "Fingerprinter", "Neither"]
    vals   = [
        tracker_etlds / total_etlds * 100 if total_etlds else 0,
        fp_etlds      / total_etlds * 100 if total_etlds else 0,
        (total_etlds - tracker_etlds) / total_etlds * 100 if total_etlds else 0,
    ]
    ax.bar(labels, vals, color=["tomato", "sandybrown", "steelblue"], edgecolor="white")
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax.set_ylabel("% of eTLD inclusions")
    ax.set_title("Tracker / Fingerprinter Share of Third-party Script Inclusions")
    plt.tight_layout()
    _save("tracker_fp_summary")


def plot_cross_dataset_deviation(dev_df: pd.DataFrame, datasets: list[str], top_n: int = TOP_N):
    """
    Heatmap of top-N most significant cross-dataset deviations.
    Rows = domains, columns = datasets, cell = eTLD count.
    """
    if dev_df.empty:
        return
    sig = dev_df[dev_df["is_significant"]].head(top_n)
    if sig.empty:
        sig = dev_df.head(top_n)
    cols = [f"{d}_etld_count" for d in datasets if f"{d}_etld_count" in sig.columns]
    heat = sig.set_index("domain")[cols].rename(
        columns={f"{d}_etld_count": d for d in datasets}
    )
    fig, ax = plt.subplots(figsize=(max(6, len(datasets) * 1.8), max(6, len(heat) * 0.45)))
    sns.heatmap(heat, ax=ax, annot=True, fmt=".0f", cmap="YlOrRd",
                linewidths=0.4, cbar_kws={"label": "eTLD count"})
    ax.set_xticklabels(ax.get_xticklabels(), rotation=30, ha="right", fontsize=9)
    ax.set_yticklabels(ax.get_yticklabels(), rotation=0, fontsize=8)
    ax.set_title(f"Top {top_n} Non-uniform Third-party Sources Across Datasets")
    plt.tight_layout()
    _save("cross_dataset_deviation_heatmap")


def plot_website_category_tp(unit_df: pd.DataFrame, etld_category_map: dict[str, str]):
    """
    Box plot: unique third-party domain count per site,
    grouped by website category (from CF Radar on the site's own eTLD).
    """
    df = unit_df.copy()
    df["site_category"] = df["etld"].map(etld_category_map).fillna("Other/Unknown")
    top_cats = df["site_category"].value_counts().head(12).index
    df = df[df["site_category"].isin(top_cats)]
    if df.empty:
        return
    cats = df.groupby("site_category")["unique_tp_domains"].median().sort_values(ascending=False).index
    data = [df[df["site_category"] == c]["unique_tp_domains"].dropna().values for c in cats]
    fig, ax = plt.subplots(figsize=(max(10, len(cats) * 1.2), 5))
    ax.boxplot(data, labels=list(cats), patch_artist=True,
               boxprops=dict(facecolor="steelblue", alpha=0.6))
    ax.set_ylabel("Unique 3rd-party domains per site")
    ax.set_title("Third-party Script Distribution by Website Category")
    ax.tick_params(axis="x", rotation=40)
    plt.tight_layout()
    _save("tp_by_website_category")


# ── Cross-dataset plots ───────────────────────────────────────────────────────

def plot_cross_dataset_fp_tp(all_unit_dfs: dict[str, pd.DataFrame], out_dir: str):
    """Grouped bar: first/third-party script share per dataset × location."""
    records = []
    for cat, udf in all_unit_dfs.items():
        for loc, grp in udf.groupby("location"):
            total = grp["total_scripts"].sum()
            if total == 0:
                continue
            records.append({
                "dataset":   cat,
                "location":  loc,
                "tp_pct":    grp["tp_scripts"].sum() / total * 100,
                "fp_pct":    grp["fp_scripts"].sum() / total * 100,
            })
    if not records:
        return
    df = pd.DataFrame(records)
    datasets  = sorted(df["dataset"].unique())
    locations = sorted(df["location"].unique())
    x = range(len(locations))
    width = 0.8 / len(datasets)

    fig, ax = plt.subplots(figsize=(max(10, len(locations) * 1.2), 5))
    for i, dset in enumerate(datasets):
        sub = df[df["dataset"] == dset].set_index("location")
        vals = [sub.loc[loc, "tp_pct"] if loc in sub.index else 0 for loc in locations]
        offset = (i - len(datasets) / 2) * width + width / 2
        ax.bar([xi + offset for xi in x], vals, width=width * 0.9,
               label=dset, edgecolor="white")
    ax.set_xticks(list(x))
    ax.set_xticklabels(locations, rotation=30, ha="right", fontsize=8)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax.set_ylabel("% third-party scripts")
    ax.set_title("Third-party Script Share by Dataset × Location")
    ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=8)
    plt.tight_layout()
    plt.savefig(f"{out_dir}/cross_dataset_tp_share.pdf", bbox_inches="tight")
    plt.savefig(f"{out_dir}/cross_dataset_tp_share.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  Saved cross_dataset_tp_share.pdf/png")


def plot_cross_dataset_unique_tp(all_unit_dfs: dict[str, pd.DataFrame], out_dir: str):
    """Box plot: unique tp domain count per site, side-by-side per dataset."""
    datasets = sorted(all_unit_dfs.keys())
    fig, axes = plt.subplots(1, len(datasets),
                             figsize=(5 * len(datasets), 5), sharey=True)
    if len(datasets) == 1:
        axes = [axes]
    for ax, cat in zip(axes, datasets):
        udf = all_unit_dfs[cat]
        locations = sorted(udf["location"].unique())
        data = [udf[udf["location"] == loc]["unique_tp_domains"].dropna().values
                for loc in locations]
        ax.boxplot(data, labels=locations, patch_artist=True,
                   boxprops=dict(facecolor="steelblue", alpha=0.6))
        ax.set_title(cat)
        ax.set_xlabel("Location")
        ax.tick_params(axis="x", rotation=30)
    axes[0].set_ylabel("Unique 3rd-party domains per site")
    fig.suptitle("Unique Third-party Script Domains per Site by Dataset", y=1.02)
    plt.tight_layout()
    plt.savefig(f"{out_dir}/cross_dataset_unique_tp.pdf", bbox_inches="tight")
    plt.savefig(f"{out_dir}/cross_dataset_unique_tp.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  Saved cross_dataset_unique_tp.pdf/png")


def plot_cross_dataset_top_sources_heatmap(
    all_source_tables: dict[str, pd.DataFrame],
    out_dir: str,
    top_n: int = TOP_N,
):
    """
    Heatmap: top-N sources (by overall eTLD count) × datasets.
    Cell = eTLD count in that dataset.
    """
    # Determine top-N globally
    all_st = pd.concat(list(all_source_tables.values()), ignore_index=True)
    if all_st.empty:
        return
    top_domains = (all_st.groupby("domain")["etld_count"].sum()
                   .sort_values(ascending=False).head(top_n).index.tolist())

    datasets = sorted(all_source_tables.keys())
    heat_data = {}
    for cat, st in all_source_tables.items():
        if st.empty:
            heat_data[cat] = {d: 0 for d in top_domains}
            continue
        agg = st.groupby("domain")["etld_count"].sum()
        heat_data[cat] = {d: int(agg.get(d, 0)) for d in top_domains}

    heat = pd.DataFrame(heat_data, index=top_domains)
    fig, ax = plt.subplots(figsize=(max(6, len(datasets) * 1.8), max(8, top_n * 0.45)))
    sns.heatmap(heat, ax=ax, annot=True, fmt="d", cmap="YlOrRd",
                linewidths=0.4, cbar_kws={"label": "eTLD count"})
    ax.set_xticklabels(ax.get_xticklabels(), rotation=30, ha="right", fontsize=9)
    ax.set_yticklabels(ax.get_yticklabels(), rotation=0, fontsize=8)
    ax.set_title(f"Top {top_n} Third-party Script Domains — eTLD Count per Dataset")
    plt.tight_layout()
    plt.savefig(f"{out_dir}/cross_dataset_top_sources_heatmap.pdf", bbox_inches="tight")
    plt.savefig(f"{out_dir}/cross_dataset_top_sources_heatmap.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  Saved cross_dataset_top_sources_heatmap.pdf/png")


# ── LaTeX ─────────────────────────────────────────────────────────────────────

def _pct(n, d):
    return f"{n/d*100:.1f}\\%" if d else "---"


def distribution_to_latex(summary: dict) -> str:
    groups = sorted(summary.keys())
    lines = [
        r"\begin{table}[ht]", r"\centering", r"\small",
        r"\begin{tabular}{llrrrr}", r"\toprule",
        r"Category & Location & Sites & Total scripts & 3rd-party \% & Median unique 3rd-party \\",
        r"\midrule",
    ]
    prev_cat = None
    for (cat, loc) in groups:
        s = summary[(cat, loc)]
        if cat != prev_cat and prev_cat is not None:
            lines.append(r"\midrule")
        prev_cat = cat
        lines.append(
            f"{cat.replace('_', r'\_')} & {loc} & {s['n_sites']:,} & "
            f"{s['total_scripts']:,} & {s['tp_rate']*100:.1f}\\% & "
            f"{s['median_unique_tp']:.1f} \\\\"
        )
    lines += [
        r"\bottomrule", r"\end{tabular}",
        r"\caption{Third-party script distribution per dataset and location.}",
        r"\label{tab:tp_distribution}", r"\end{table}",
    ]
    return "\n".join(lines)


def top_sources_to_latex(source_table: pd.DataFrame, profile_df: pd.DataFrame,
                         top_n: int = TOP_N) -> str:
    if source_table.empty:
        return ""
    merged = (source_table.groupby("domain")["etld_count"].sum()
              .reset_index().nlargest(top_n, "etld_count")
              .merge(profile_df[["domain", "owner", "cf_category",
                                  "is_tracker", "is_fingerprinter"]],
                     on="domain", how="left"))
    lines = [
        r"\begin{table}[ht]", r"\centering", r"\small",
        r"\begin{tabular}{lrllcc}", r"\toprule",
        r"Domain & eTLD count & Owner & Category & Tracker & FP \\",
        r"\midrule",
    ]
    for _, row in merged.iterrows():
        lines.append(
            f"{row['domain'].replace('_', r'\_')} & {int(row['etld_count'])} & "
            f"{str(row.get('owner','—')).replace('_', r'\_')[:25]} & "
            f"{str(row.get('cf_category','—')).replace('_', r'\_')[:20]} & "
            f"{'\\checkmark' if row.get('is_tracker') else ''} & "
            f"{'\\checkmark' if row.get('is_fingerprinter') else ''} \\\\"
        )
    lines += [
        r"\bottomrule", r"\end{tabular}",
        r"\caption{Top third-party script domains by eTLD presence.}",
        r"\label{tab:top_tp_sources}", r"\end{table}",
    ]
    return "\n".join(lines)


def deviation_to_latex(dev_df: pd.DataFrame, datasets: list[str], top_n: int = TOP_N) -> str:
    if dev_df.empty:
        return ""
    sig = dev_df[dev_df["is_significant"]].head(top_n)
    if sig.empty:
        sig = dev_df.head(top_n)
    count_cols = [f"{d}_etld_count" for d in datasets if f"{d}_etld_count" in sig.columns]
    col_spec = "l" + "r" * len(count_cols) + "r"
    header = " & ".join(["Domain"] + [d.replace("_", r"\_") for d in datasets] + ["Spread"]) + r" \\"
    lines = [
        r"\begin{table}[ht]", r"\centering", r"\small",
        r"\begin{tabular}{" + col_spec + "}", r"\toprule",
        header, r"\midrule",
    ]
    for _, row in sig.iterrows():
        vals = [str(int(row.get(c, 0))) for c in count_cols]
        lines.append(
            f"{row['domain'].replace('_', r'\_')} & "
            + " & ".join(vals)
            + f" & {int(row['spread'])} \\\\"
        )
    lines += [
        r"\bottomrule", r"\end{tabular}",
        r"\caption{Most non-uniformly distributed third-party script domains across datasets.}",
        r"\label{tab:tp_deviation}", r"\end{table}",
    ]
    return "\n".join(lines)


# ── Console summary ───────────────────────────────────────────────────────────

def print_summary(summary: dict, source_table: pd.DataFrame, category: str):
    print(f"\n{'='*70}")
    print(f"THIRD-PARTY SCRIPT ANALYSIS: {category.upper()}")
    print(f"{'='*70}")
    for (cat, loc), s in sorted(summary.items()):
        print(f"  {loc:<20} sites={s['n_sites']:,}  "
              f"total_scripts={s['total_scripts']:,}  "
              f"3rd-party={s['tp_rate']*100:.1f}%  "
              f"median_unique_tp={s['median_unique_tp']:.1f}")
    if not source_table.empty:
        print(f"\n  Top 10 third-party domains (by eTLD count, across all locations):")
        top = (source_table.groupby("domain")["etld_count"].sum()
               .sort_values(ascending=False).head(10))
        for domain, cnt in top.items():
            print(f"    {domain:<45} {cnt:6,} eTLDs")


# ── Per-category pipeline ─────────────────────────────────────────────────────

def _run_for_category(category: str, db_config: dict):
    global OUTPUT_DIR
    OUTPUT_DIR = os.path.join(SCRIPTS_RESULTS_BASE, category)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"Fetching scripts ({category})...")
    raw = fetch_scripts(db_config, category)
    if raw.empty:
        print(f"  No data for {category} — skipping.")
        return None, None, None

    print(f"  {len(raw):,} script rows")

    print("Loading visit map for consistency tagging...")
    visit_map = load_visit_map(db_config, category)
    tagger = ConsistencyTagger(visit_map)
    raw, excluded = filter_sufficient_visits(raw, visit_map, etld_col="etld", loc_col="location")
    if not excluded.empty:
        print(f"  Excluded {excluded['etld'].nunique()} eTLDs with insufficient visits")

    print("Building unit frame (etld × location)...")
    unit_df = build_unit_frame(raw, tagger)
    print(f"  {len(unit_df):,} etld × location units")

    print("Building third-party source frequency table...")
    source_table = build_tp_source_table(raw, tagger)

    # Profile unique third-party domains
    all_tp_domains = source_table["domain"].unique().tolist()
    print(f"Profiling {len(all_tp_domains)} unique third-party domains...")
    profile_df = profile_domains(all_tp_domains)

    # Site-level categorisation for analysis point 6
    all_etlds = unit_df["etld"].unique().tolist()
    print(f"Categorising {len(all_etlds)} site eTLDs for website category analysis...")
    etld_category_map = categorise_bulk(all_etlds, verbose=True)

    summary = summarise_distribution(unit_df)
    print_summary(summary, source_table, category)

    print("\nGenerating plots...")
    plot_fp_tp_distribution(unit_df)
    plot_unique_tp_domains_dist(unit_df)
    plot_top_tp_sources(source_table)
    plot_tp_profile_breakdown(profile_df, source_table)
    plot_tracker_fp_summary(profile_df, source_table)
    plot_website_category_tp(unit_df, etld_category_map)

    print("Writing LaTeX tables...")
    with open(f"{OUTPUT_DIR}/table_tp_distribution.tex", "w") as f:
        f.write(distribution_to_latex(summary))
    with open(f"{OUTPUT_DIR}/table_top_tp_sources.tex", "w") as f:
        f.write(top_sources_to_latex(source_table, profile_df))
    print("  Saved table_tp_distribution.tex, table_top_tp_sources.tex")

    print("Saving raw results...")
    unit_df.drop(columns=["tp_domain_set"]).to_csv(f"{OUTPUT_DIR}/unit_frame.csv", index=False)
    source_table.to_csv(f"{OUTPUT_DIR}/tp_source_table.csv", index=False)
    if "tier" in source_table.columns:
        tier_counts = source_table.groupby(["tier", "location"])["domain"].count()
        tier_counts.to_csv(f"{OUTPUT_DIR}/tp_source_tier_distribution.csv")
    profile_df.to_csv(f"{OUTPUT_DIR}/tp_profile.csv", index=False)
    with open(f"{OUTPUT_DIR}/etld_categories.json", "w") as f:
        json.dump(etld_category_map, f, indent=2)
    print(f"  Done. Outputs in: {OUTPUT_DIR}")

    return unit_df, source_table, profile_df


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    active_datasets = list(DATASETS)
    if INCLUDE_VPN:
        active_datasets.append(VPN_DATASET)

    all_unit_dfs:    dict[str, pd.DataFrame] = {}
    all_source_tables: dict[str, pd.DataFrame] = {}
    all_profiles:    dict[str, pd.DataFrame] = {}

    for category, db_config in active_datasets:
        print(f"\n{'='*60}")
        print(f"Dataset: {category}")
        print(f"{'='*60}")
        unit_df, source_table, profile_df = _run_for_category(category, db_config)
        if unit_df is not None:
            all_unit_dfs[category]      = unit_df
            all_source_tables[category] = source_table
            all_profiles[category]      = profile_df

    # Cross-dataset analysis (≥2 datasets)
    if len(all_unit_dfs) >= 2:
        cross_dir = os.path.join(SCRIPTS_RESULTS_BASE, "cross_dataset")
        os.makedirs(cross_dir, exist_ok=True)
        datasets = sorted(all_unit_dfs.keys())

        print(f"\n{'='*60}")
        print(f"Cross-dataset analysis ({', '.join(datasets)})")
        print(f"{'='*60}")

        print("Computing cross-dataset source deviation...")
        dev_df = compute_cross_dataset_deviation(all_source_tables)

        print("Generating cross-dataset plots...")
        plot_cross_dataset_fp_tp(all_unit_dfs, cross_dir)
        plot_cross_dataset_unique_tp(all_unit_dfs, cross_dir)
        plot_cross_dataset_top_sources_heatmap(all_source_tables, cross_dir)

        if not dev_df.empty:
            # Save deviation plot using the cross_dir by temporarily setting OUTPUT_DIR
            global OUTPUT_DIR
            OUTPUT_DIR = cross_dir
            plot_cross_dataset_deviation(dev_df, datasets)

        print("Writing cross-dataset LaTeX tables...")
        if not dev_df.empty:
            with open(f"{cross_dir}/table_tp_deviation.tex", "w") as f:
                f.write(deviation_to_latex(dev_df, datasets))
            dev_df.to_csv(f"{cross_dir}/tp_deviation.csv", index=False)
            print("  Saved table_tp_deviation.tex, tp_deviation.csv")

        print(f"\nCross-dataset outputs in: {cross_dir}")
    else:
        avail = list(all_unit_dfs.keys())
        print(f"\nOnly one dataset available ({avail[0] if avail else 'none'}) — "
              f"skipping cross-dataset analysis.")

    print(f"\nDone. All outputs under: {SCRIPTS_RESULTS_BASE}/<category>/")


if __name__ == "__main__":
    main()
